#!/usr/bin/env python3
"""
End-to-end sample program for a print-audit pipeline:
1) Sync encrypted print stream files from a data server (SFTP)
2) Decrypt them locally (Fernet)
3) Merge split stream parts into a single file per task group
4) Rename merged outputs with a timestamp derived from the server-side mtime
   (server time is treated as UTC, converted to UTC+8).

This script integrates the demo logic from:
- download_decrypt.py
- mergeprint.py
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import shutil
import subprocess
import stat
import sys
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from glob import glob
from shutil import which
from typing import Dict, Iterable, List, Optional, Tuple


# --- Defaults (same as the original demo scripts) ---
DEFAULT_VPS_CONFIG = {
    "hostname": "192.168.0.100",
    "port": 22,
    "username": "root",
    "password": "password",
}

DEFAULT_REMOTE_PATH = "/root/abc/uploads"
DEFAULT_LOCAL_DOWNLOAD_DIR = "./downloads"
DEFAULT_LOCAL_DECRYPT_DIR = "./decrypted"
DEFAULT_LOCAL_MERGE_DIR = "./merge"
DEFAULT_LOCAL_CONVERTED_DIR = "./converted"
DEFAULT_LOG_DIR = "./logs"

DEFAULT_DOWNLOAD_HISTORY = "downloaded_files.txt"
DEFAULT_DECRYPT_HISTORY = "decrypted_files.txt"
DEFAULT_REMOTE_META = "remote_file_meta.json"  # filename -> {"mtime": int, "size": int}

# Fernet key: prefer env var; this placeholder value is only for the demo.
DEFAULT_FERNET_KEY_ENV = "FERNET_KEY"
DEFAULT_FERNET_KEY = b"fernetkey"


UTC = timezone.utc
UTC_PLUS_8 = timezone(timedelta(hours=8))


def setup_logging(log_dir: str) -> logging.Logger:
    os.makedirs(log_dir, exist_ok=True)
    log_filename = os.path.join(log_dir, datetime.now().strftime("log_%Y%m%d_%H%M%S.log"))

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.FileHandler(log_filename, encoding="utf-8"), logging.StreamHandler()],
    )
    return logging.getLogger("printer_audit")


def load_history_set(filepath: str) -> set:
    if os.path.exists(filepath):
        with open(filepath, "r", encoding="utf-8") as f:
            return set(line.strip() for line in f if line.strip())
    return set()


def append_history(filepath: str, filename: str) -> None:
    with open(filepath, "a", encoding="utf-8") as f:
        f.write(filename + "\n")


def _load_json(path: str, default):
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def _save_json_atomic(path: str, data) -> None:
    tmp_path = path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=True, indent=2, sort_keys=True)
    os.replace(tmp_path, path)


def _import_paramiko(logger: logging.Logger):
    try:
        import paramiko  # type: ignore

        return paramiko
    except Exception as e:
        logger.error("Missing dependency: paramiko (%s). Install it to enable SFTP sync.", e)
        return None


def _import_fernet(logger: logging.Logger):
    try:
        from cryptography.fernet import Fernet  # type: ignore

        return Fernet
    except Exception as e:
        logger.error("Missing dependency: cryptography (%s). Install it to enable decryption.", e)
        return None


def _get_fernet_key(logger: logging.Logger, key_from_arg: Optional[str]) -> bytes:
    # Priority: CLI arg -> env -> default placeholder.
    if key_from_arg:
        return key_from_arg.encode("utf-8")
    env_val = os.environ.get(DEFAULT_FERNET_KEY_ENV)
    if env_val:
        return env_val.encode("utf-8")

    # Keep compatibility with the original demo script; warn because it's not a valid Fernet key.
    logger.warning(
        "Using placeholder Fernet key (%r). Set --fernet-key or env %s for real runs.",
        DEFAULT_FERNET_KEY,
        DEFAULT_FERNET_KEY_ENV,
    )
    return DEFAULT_FERNET_KEY


def ensure_dirs(*dirs: str) -> None:
    for d in dirs:
        os.makedirs(d, exist_ok=True)


def get_remote_file_list(sftp, remote_path: str, logger: logging.Logger):
    try:
        files = sftp.listdir_attr(remote_path)
        return [f for f in files if not stat.S_ISDIR(f.st_mode)]
    except Exception as e:
        logger.error("Failed to list remote files: %s", e)
        return []


def get_remote_status(sftp, remote_path: str, logger: logging.Logger) -> Tuple[int, int]:
    file_list = get_remote_file_list(sftp, remote_path, logger)
    total_size = sum(f.st_size for f in file_list)
    return len(file_list), total_size


def wait_for_remote_stable(
    sftp,
    remote_path: str,
    logger: logging.Logger,
    interval_sec: int,
    max_rounds: int,
) -> List:
    """
    Wait until the remote folder has a stable (count, total_size) snapshot across two samples.
    Returns the last file list when stable.
    """
    rounds = 0
    while True:
        rounds += 1
        count1, size1 = get_remote_status(sftp, remote_path, logger)
        logger.info("Snapshot 1 -> Files: %s, Total Size: %s bytes", count1, size1)

        logger.info("Waiting %s seconds for stability check...", interval_sec)
        time.sleep(interval_sec)

        count2, size2 = get_remote_status(sftp, remote_path, logger)
        logger.info("Snapshot 2 -> Files: %s, Total Size: %s bytes", count2, size2)

        if count1 == count2 and size1 == size2:
            files = get_remote_file_list(sftp, remote_path, logger)
            if not files:
                logger.warning("Remote folder is empty.")
            else:
                logger.info("Remote folder is STABLE.")
            return files

        logger.info("Remote folder is still changing (%s -> %s)...", size1, size2)
        if max_rounds > 0 and rounds >= max_rounds:
            logger.warning("Stability check exceeded max_rounds=%s; proceeding with current list.", max_rounds)
            return get_remote_file_list(sftp, remote_path, logger)


def download_if_needed(
    sftp,
    remote_path: str,
    local_download_dir: str,
    filename: str,
    downloaded_set: set,
    download_history_path: str,
    logger: logging.Logger,
) -> bool:
    if filename in downloaded_set:
        local_file_full = os.path.join(local_download_dir, filename)
        if os.path.exists(local_file_full):
            logger.info("Skipping Download (Already recorded): %s", filename)
            return True
        logger.warning("History says downloaded but local file missing; re-downloading: %s", filename)

    remote_file_full = os.path.join(remote_path, filename).replace("\\", "/")
    local_file_full = os.path.join(local_download_dir, filename)
    logger.info("Downloading: %s", filename)
    try:
        sftp.get(remote_file_full, local_file_full)
        append_history(download_history_path, filename)
        downloaded_set.add(filename)
        logger.info("Download complete: %s", filename)
        return True
    except Exception as e:
        logger.error("Download failed for %s: %s", filename, e)
        return False


def decrypt_if_needed(
    cipher,
    local_download_dir: str,
    local_decrypt_dir: str,
    filename: str,
    decrypted_set: set,
    decrypt_history_path: str,
    logger: logging.Logger,
) -> bool:
    if filename in decrypted_set:
        target_path = os.path.join(local_decrypt_dir, filename)
        if os.path.exists(target_path):
            logger.info("Skipping Decryption (Already recorded): %s", filename)
            return True
        logger.warning("History says decrypted but local file missing; re-decrypting: %s", filename)

    source_path = os.path.join(local_download_dir, filename)
    target_path = os.path.join(local_decrypt_dir, filename)
    try:
        with open(source_path, "rb") as f:
            encrypted_data = f.read()
        decrypted_data = cipher.decrypt(encrypted_data)
        with open(target_path, "wb") as f:
            f.write(decrypted_data)

        append_history(decrypt_history_path, filename)
        decrypted_set.add(filename)
        logger.info("Successfully decrypted: %s", filename)
        return True
    except Exception as e:
        logger.error("Decryption failed for %s: %s", filename, e)
        return False


def load_remote_meta(meta_path: str) -> Dict[str, Dict[str, int]]:
    data = _load_json(meta_path, default={})
    if isinstance(data, dict):
        return data  # filename -> {"mtime": int, "size": int}
    return {}


def update_remote_meta(
    meta: Dict[str, Dict[str, int]],
    filename: str,
    mtime: int,
    size: int,
) -> None:
    meta[filename] = {"mtime": int(mtime), "size": int(size)}


def beijing_timestamp_from_utc_epoch(epoch_sec: int, fmt: str) -> str:
    dt_utc = datetime.fromtimestamp(epoch_sec, tz=UTC)
    dt_bj = dt_utc.astimezone(UTC_PLUS_8)
    return dt_bj.strftime(fmt)


@dataclass(frozen=True)
class MergePart:
    seq: int
    name: str
    path: str


def group_decrypted_parts(src_dir: str) -> Dict[str, List[MergePart]]:
    # Same rule as mergeprint.py:
    # (Sequence)(Hostname)_ctrl_(JobName)_(ID)-(Type)
    # Example: 46wpa_ctrl_job13_10013-2
    pattern = re.compile(r"^(\d+)([a-zA-Z0-9]+)_ctrl_([a-zA-Z0-9]+)_\d+-(\d+)$")
    groups: Dict[str, List[MergePart]] = defaultdict(list)

    for filename in os.listdir(src_dir):
        m = pattern.match(filename)
        if not m:
            continue
        seq = int(m.group(1))
        host = m.group(2)
        job = m.group(3)
        file_type = m.group(4)
        group_key = f"{host}-{job}-{file_type}"
        groups[group_key].append(MergePart(seq=seq, name=filename, path=os.path.join(src_dir, filename)))

    # Sort each group by sequence.
    for k in list(groups.keys()):
        groups[k].sort(key=lambda p: p.seq)
    return dict(groups)


def merge_groups(
    src_dir: str,
    dst_dir: str,
    remote_meta: Dict[str, Dict[str, int]],
    logger: logging.Logger,
    timestamp_fmt: str,
    mtime_policy: str,
) -> List[str]:
    ensure_dirs(dst_dir)

    if not os.path.exists(src_dir):
        logger.error("Source directory not found: %s", src_dir)
        return []

    groups = group_decrypted_parts(src_dir)
    logger.info("Parsing complete. Found %s unique task groups.", len(groups))

    outputs: List[str] = []
    for group_key in sorted(groups.keys()):
        parts = groups[group_key]
        base_output_path = os.path.join(dst_dir, group_key)

        # Compute server-side mtime for this stream by aggregating part mtimes (default: max).
        mtimes = []
        for p in parts:
            meta = remote_meta.get(p.name)
            if meta and "mtime" in meta:
                mtimes.append(int(meta["mtime"]))

        out_path = base_output_path
        if mtimes:
            epoch = max(mtimes) if mtime_policy == "max" else min(mtimes)
            ts = beijing_timestamp_from_utc_epoch(epoch, timestamp_fmt)
            out_path = f"{base_output_path}_{ts}"
        else:
            logger.warning("No remote mtime metadata for group %s; output will not be timestamped.", group_key)

        # Avoid overwriting if already exists.
        final_path = out_path
        if os.path.exists(final_path):
            i = 1
            while os.path.exists(f"{out_path}_{i}"):
                i += 1
            final_path = f"{out_path}_{i}"

        logger.info("Merging into: %s", final_path)
        with open(final_path, "wb") as outfile:
            for p in parts:
                logger.info("  Appending: %s (Seq: %s)", p.name, p.seq)
                try:
                    with open(p.path, "rb") as infile:
                        shutil.copyfileobj(infile, outfile)
                except Exception as e:
                    logger.error("  Failed to read %s: %s", p.name, e)

        logger.info("Group %s merged successfully.", group_key)
        outputs.append(final_path)

    return outputs


def detect_stream_kind(path: str) -> str:
    """
    Best-effort identification for common print stream/container types.
    Returns: "pdf" | "ps" | "pjl_or_pcl" | "unknown"
    """
    try:
        with open(path, "rb") as f:
            head = f.read(8192)
    except Exception:
        return "unknown"

    if head.startswith(b"%PDF-"):
        return "pdf"
    if head.startswith(b"%!PS"):
        return "ps"

    # PJL / PCL often contains a UEL marker or PJL header.
    if b"\x1b%-12345X" in head or b"@PJL" in head:
        return "pjl_or_pcl"

    # Many PCL streams contain ESC (0x1b) commands; this is not definitive.
    if b"\x1b" in head:
        return "pjl_or_pcl"

    return "unknown"


def render_to_images_or_pdf(
    input_path: str,
    out_dir: str,
    out_basename: str,
    out_format: str,
    dpi: int,
    tool: str,
    logger: logging.Logger,
) -> None:
    """
    Render a raw print stream into PDF or page images using external interpreters.
    This is a best-effort helper for audit viewing; it typically cannot reconstruct
    the original source document (text/searchability may be lost).
    """
    os.makedirs(out_dir, exist_ok=True)

    kind = detect_stream_kind(input_path)
    if tool == "auto":
        # Prefer Ghostscript for PS/PDF; otherwise prefer GhostPDL PCL interpreter.
        if kind in ("pdf", "ps"):
            candidates = ["gs", "gpcl6", "pcl6"]
        else:
            candidates = ["gpcl6", "pcl6", "gs"]

        tool = ""
        for c in candidates:
            if which(c) is not None:
                tool = c
                break
        if not tool:
            raise FileNotFoundError(f"No renderer found in PATH (tried: {', '.join(candidates)})")

    if out_format not in ("png", "pdf"):
        raise ValueError("out_format must be png or pdf")

    if tool == "gs":
        if which("gs") is None:
            raise FileNotFoundError("gs not found in PATH")
        if out_format == "pdf":
            out_path = os.path.join(out_dir, f"{out_basename}.pdf")
            cmd = ["gs", "-dSAFER", "-dBATCH", "-dNOPAUSE", "-sDEVICE=pdfwrite", "-o", out_path, input_path]
        else:
            out_pat = os.path.join(out_dir, f"{out_basename}-%03d.png")
            cmd = ["gs", "-dSAFER", "-dBATCH", "-dNOPAUSE", "-sDEVICE=png16m", "-r{}".format(dpi), "-o", out_pat, input_path]
    elif tool in ("gpcl6", "pcl6"):
        # GhostPDL PCL/PCLXL interpreter. On some systems the binary is `pcl6`.
        bin_name = "gpcl6" if tool == "gpcl6" else "pcl6"
        if which(bin_name) is None:
            raise FileNotFoundError(f"{bin_name} not found in PATH")
        if out_format == "pdf":
            out_path = os.path.join(out_dir, f"{out_basename}.pdf")
            cmd = [bin_name, "-dSAFER", "-sDEVICE=pdfwrite", "-o", out_path, input_path]
        else:
            out_pat = os.path.join(out_dir, f"{out_basename}-%03d.png")
            cmd = [bin_name, "-dSAFER", "-sDEVICE=png16m", "-r{}".format(dpi), "-o", out_pat, input_path]
    else:
        raise ValueError("tool must be auto|gs|gpcl6|pcl6")

    logger.info("Rendering (%s -> %s) via %s: %s", input_path, out_format, tool, " ".join(cmd))
    p = subprocess.run(cmd, capture_output=True, text=True)
    if p.returncode != 0:
        # Surface stderr for troubleshooting (common: missing GhostPDL resources / fonts).
        raise RuntimeError(f"Render failed (rc={p.returncode}). stderr:\n{p.stderr.strip()}")


def _sanitize_basename(name: str) -> str:
    # Keep filenames portable for audit artifacts.
    return re.sub(r"[^A-Za-z0-9._-]+", "_", name)


def try_convert_stream(
    input_path: str,
    converted_dir: str,
    out_format: str,
    dpi: int,
    tool: str,
    logger: logging.Logger,
) -> bool:
    """
    Best-effort conversion for audit viewing. Returns True if we produced output files.
    """
    os.makedirs(converted_dir, exist_ok=True)
    base = _sanitize_basename(os.path.basename(input_path))

    # Auto: choose an order, but still fall back to other interpreters.
    kind = detect_stream_kind(input_path)
    if tool == "auto":
        if kind in ("pdf", "ps"):
            attempts = ["gs", "gpcl6", "pcl6"]
        else:
            attempts = ["gpcl6", "pcl6", "gs"]
    else:
        attempts = [tool]

    last_err: Optional[Exception] = None
    for t in attempts:
        try:
            render_to_images_or_pdf(
                input_path=input_path,
                out_dir=converted_dir,
                out_basename=base,
                out_format=out_format,
                dpi=dpi,
                tool=t,
                logger=logger,
            )
        except Exception as e:
            last_err = e
            logger.warning("Convert attempt failed via %s for %s: %s", t, input_path, e)
            continue

        # Verify outputs exist.
        if out_format == "pdf":
            out_path = os.path.join(converted_dir, f"{base}.pdf")
            if os.path.exists(out_path) and os.path.getsize(out_path) > 0:
                logger.info("Converted OK: %s", out_path)
                return True
        else:
            outs = sorted(glob(os.path.join(converted_dir, f"{base}-*.png")))
            if outs:
                logger.info("Converted OK: %s pages (%s ...)", len(outs), outs[0])
                return True

        last_err = RuntimeError("renderer returned success but no output files were found")

    if last_err is not None:
        logger.error("Conversion failed for %s (all attempts). Last error: %s", input_path, last_err)
    else:
        logger.error("Conversion failed for %s (no attempts).", input_path)
    return False


def render_path(
    src_path: str,
    out_dir: str,
    out_format: str,
    dpi: int,
    tool: str,
    logger: logging.Logger,
) -> None:
    if os.path.isdir(src_path):
        for name in sorted(os.listdir(src_path)):
            p = os.path.join(src_path, name)
            if os.path.isdir(p):
                continue
            try_convert_stream(
                input_path=p,
                converted_dir=out_dir,
                out_format=out_format,
                dpi=dpi,
                tool=tool,
                logger=logger,
            )
    else:
        try_convert_stream(
            input_path=src_path,
            converted_dir=out_dir,
            out_format=out_format,
            dpi=dpi,
            tool=tool,
            logger=logger,
        )


def run_sync_decrypt(args, logger: logging.Logger) -> Dict[str, Dict[str, int]]:
    paramiko = _import_paramiko(logger)
    Fernet = _import_fernet(logger)
    if paramiko is None or Fernet is None:
        raise SystemExit(2)

    key = _get_fernet_key(logger, args.fernet_key)
    try:
        cipher = Fernet(key)
    except Exception as e:
        logger.error("Invalid Fernet key (%s). Provide a valid key via --fernet-key or env %s.", e, DEFAULT_FERNET_KEY_ENV)
        raise SystemExit(2)

    ensure_dirs(args.download_dir, args.decrypt_dir)

    downloaded_set = load_history_set(args.download_history)
    decrypted_set = load_history_set(args.decrypt_history)
    remote_meta = load_remote_meta(args.remote_meta)

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        logger.info("Connecting to data server: %s:%s ...", args.hostname, args.port)
        ssh.connect(
            hostname=args.hostname,
            port=args.port,
            username=args.username,
            password=args.password,
            timeout=args.connect_timeout,
        )
        sftp = ssh.open_sftp()
        logger.info("SSH connection established.")

        current_files = wait_for_remote_stable(
            sftp=sftp,
            remote_path=args.remote_path,
            logger=logger,
            interval_sec=args.stability_interval,
            max_rounds=args.stability_max_rounds,
        )

        for fattr in current_files:
            filename = fattr.filename
            # Record metadata even if file was downloaded before, so merge can use it.
            update_remote_meta(remote_meta, filename, mtime=fattr.st_mtime, size=fattr.st_size)

            if not download_if_needed(
                sftp=sftp,
                remote_path=args.remote_path,
                local_download_dir=args.download_dir,
                filename=filename,
                downloaded_set=downloaded_set,
                download_history_path=args.download_history,
                logger=logger,
            ):
                continue

            decrypt_if_needed(
                cipher=cipher,
                local_download_dir=args.download_dir,
                local_decrypt_dir=args.decrypt_dir,
                filename=filename,
                decrypted_set=decrypted_set,
                decrypt_history_path=args.decrypt_history,
                logger=logger,
            )

    finally:
        try:
            ssh.close()
        except Exception:
            pass
        logger.info("SSH session closed.")

    _save_json_atomic(args.remote_meta, remote_meta)
    return remote_meta


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Print audit pipeline demo (sync/decrypt/merge).")
    sub = p.add_subparsers(dest="cmd", required=False)

    def add_common_conn_flags(sp: argparse.ArgumentParser) -> None:
        sp.add_argument("--hostname", default=DEFAULT_VPS_CONFIG["hostname"])
        sp.add_argument("--port", type=int, default=DEFAULT_VPS_CONFIG["port"])
        sp.add_argument("--username", default=DEFAULT_VPS_CONFIG["username"])
        sp.add_argument("--password", default=DEFAULT_VPS_CONFIG["password"])
        sp.add_argument("--connect-timeout", type=int, default=20)
        sp.add_argument("--remote-path", default=DEFAULT_REMOTE_PATH)
        sp.add_argument("--stability-interval", type=int, default=60)
        sp.add_argument(
            "--stability-max-rounds",
            type=int,
            default=0,
            help="0 means no limit; otherwise stop waiting after N rounds and proceed.",
        )

    def add_common_paths(sp: argparse.ArgumentParser) -> None:
        sp.add_argument("--download-dir", default=DEFAULT_LOCAL_DOWNLOAD_DIR)
        sp.add_argument("--decrypt-dir", default=DEFAULT_LOCAL_DECRYPT_DIR)
        sp.add_argument("--merge-dir", default=DEFAULT_LOCAL_MERGE_DIR)
        sp.add_argument("--converted-dir", default=DEFAULT_LOCAL_CONVERTED_DIR)
        sp.add_argument("--log-dir", default=DEFAULT_LOG_DIR)
        sp.add_argument("--download-history", default=DEFAULT_DOWNLOAD_HISTORY)
        sp.add_argument("--decrypt-history", default=DEFAULT_DECRYPT_HISTORY)
        sp.add_argument("--remote-meta", default=DEFAULT_REMOTE_META)

    sp_run = sub.add_parser("run", help="sync+decrypt then merge (default)")
    add_common_conn_flags(sp_run)
    add_common_paths(sp_run)
    sp_run.add_argument("--fernet-key", default=None, help=f"Fernet key (or use env {DEFAULT_FERNET_KEY_ENV}).")
    sp_run.add_argument("--timestamp-format", default="%Y%m%d%H%M%S", help="Merged output timestamp format.")
    sp_run.add_argument("--mtime-policy", choices=["max", "min"], default="max", help="How to aggregate part mtimes.")
    sp_run.add_argument(
        "--auto-convert",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="After merging, automatically try converting merged streams to audit-friendly files in --converted-dir.",
    )
    sp_run.add_argument("--convert-format", choices=["png", "pdf"], default="png")
    sp_run.add_argument("--convert-dpi", type=int, default=300)
    sp_run.add_argument("--convert-tool", choices=["auto", "gs", "gpcl6", "pcl6"], default="auto")

    sp_sync = sub.add_parser("sync", help="only sync+decrypt")
    add_common_conn_flags(sp_sync)
    add_common_paths(sp_sync)
    sp_sync.add_argument("--fernet-key", default=None, help=f"Fernet key (or use env {DEFAULT_FERNET_KEY_ENV}).")

    sp_merge = sub.add_parser("merge", help="only merge decrypted parts")
    add_common_paths(sp_merge)
    sp_merge.add_argument("--timestamp-format", default="%Y%m%d%H%M%S", help="Merged output timestamp format.")
    sp_merge.add_argument("--mtime-policy", choices=["max", "min"], default="max", help="How to aggregate part mtimes.")
    sp_merge.add_argument(
        "--auto-convert",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="After merging, automatically try converting merged streams to audit-friendly files in --converted-dir.",
    )
    sp_merge.add_argument("--convert-format", choices=["png", "pdf"], default="png")
    sp_merge.add_argument("--convert-dpi", type=int, default=300)
    sp_merge.add_argument("--convert-tool", choices=["auto", "gs", "gpcl6", "pcl6"], default="auto")

    sp_render = sub.add_parser("render", help="render a merged/raw stream to PDF or page images for audit viewing")
    sp_render.add_argument("--src", required=True, help="Input file path or directory (e.g. merge/).")
    sp_render.add_argument("--out-dir", default=DEFAULT_LOCAL_CONVERTED_DIR, help="Output directory for rendered files.")
    sp_render.add_argument("--format", choices=["png", "pdf"], default="png", help="Output format.")
    sp_render.add_argument("--dpi", type=int, default=300, help="DPI used when rendering to images.")
    sp_render.add_argument("--tool", choices=["auto", "gs", "gpcl6", "pcl6"], default="auto", help="Renderer backend.")
    sp_render.add_argument("--log-dir", default=DEFAULT_LOG_DIR)

    # For convenience: `python printer_audit.py` acts like `run`.
    p.set_defaults(cmd="run")
    return p


def main(argv: Optional[List[str]] = None) -> int:
    # If no subcommand is provided, behave as `run` (matching the original intention),
    # so flags like `--hostname ...` also work without explicitly typing `run`.
    if argv is None:
        argv = sys.argv[1:]
    if not argv:
        argv = ["run"]
    elif argv[0].startswith("-") and argv[0] not in ("-h", "--help"):
        argv = ["run"] + argv

    args = build_arg_parser().parse_args(argv)
    logger = setup_logging(args.log_dir)

    if args.cmd == "sync":
        run_sync_decrypt(args, logger)
        return 0

    if args.cmd == "merge":
        remote_meta = load_remote_meta(args.remote_meta)
        merged = merge_groups(
            src_dir=args.decrypt_dir,
            dst_dir=args.merge_dir,
            remote_meta=remote_meta,
            logger=logger,
            timestamp_fmt=args.timestamp_format,
            mtime_policy=args.mtime_policy,
        )
        if args.auto_convert:
            for p in merged:
                try_convert_stream(
                    input_path=p,
                    converted_dir=args.converted_dir,
                    out_format=args.convert_format,
                    dpi=args.convert_dpi,
                    tool=args.convert_tool,
                    logger=logger,
                )
        return 0

    if args.cmd == "render":
        render_path(
            src_path=args.src,
            out_dir=args.out_dir,
            out_format=args.format,
            dpi=args.dpi,
            tool=args.tool,
            logger=logger,
        )
        return 0

    if args.cmd == "run":
        remote_meta = run_sync_decrypt(args, logger)
        merged = merge_groups(
            src_dir=args.decrypt_dir,
            dst_dir=args.merge_dir,
            remote_meta=remote_meta,
            logger=logger,
            timestamp_fmt=args.timestamp_format,
            mtime_policy=args.mtime_policy,
        )
        if args.auto_convert:
            for p in merged:
                try_convert_stream(
                    input_path=p,
                    converted_dir=args.converted_dir,
                    out_format=args.convert_format,
                    dpi=args.convert_dpi,
                    tool=args.convert_tool,
                    logger=logger,
                )
        return 0

    logger.error("Unknown cmd: %s", args.cmd)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
