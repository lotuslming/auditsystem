#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
打印机数据审计同步系统 - Ubuntu版本
功能：SSH同步、解密、合并、unirast转PDF
"""

import os
import json
import subprocess
import time
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from cryptography.fernet import Fernet
import paramiko
from collections import defaultdict
import re
import struct

class PrintAuditSync:
    def __init__(self, config):
        self.config = config
        self.local_record_file = config['local_record_file']
        self.temp_decrypt_dir = config['temp_decrypt_dir']
        self.output_dir = config['output_dir']
        self.merge_dir = config['merge_dir']
        self.fernet_key = config['fernet_key']
        
        # 创建必要的目录
        for dir_path in [self.temp_decrypt_dir, self.output_dir, self.merge_dir]:
            Path(dir_path).mkdir(parents=True, exist_ok=True)
        
        # 加载已同步记录
        self.synced_files = self._load_sync_record()
        
        # 检查转换工具
        self._check_conversion_tools()
        
    def _check_conversion_tools(self):
        """检查系统中可用的转换工具"""
        self.available_tools = {}
        
        tools = {
            'cupsfilter': 'cupsfilter',
            'gs': 'gs',
            'convert': 'convert',  # ImageMagick
            'pdftoppm': 'pdftoppm',
            'rasterview': 'rasterview'
        }
        
        for tool_name, command in tools.items():
            try:
                result = subprocess.run(
                    ['which', command],
                    capture_output=True,
                    text=True
                )
                if result.returncode == 0:
                    self.available_tools[tool_name] = result.stdout.strip()
                    print(f"✓ 发现工具: {tool_name} -> {result.stdout.strip()}")
            except Exception:
                pass
        
        if not self.available_tools:
            print("警告: 未找到任何转换工具，请安装以下软件包之一:")
            print("  sudo apt-get install cups-filters")
            print("  sudo apt-get install ghostscript")
            print("  sudo apt-get install imagemagick")
    
    def _load_sync_record(self):
        """加载本地同步记录"""
        if os.path.exists(self.local_record_file):
            with open(self.local_record_file, 'r') as f:
                return json.load(f)
        return {}
    
    def _save_sync_record(self):
        """保存同步记录"""
        with open(self.local_record_file, 'w') as f:
            json.dump(self.synced_files, f, indent=2)
    
    def sync_from_server(self):
        """使用SSH/SCP从服务端增量同步数据"""
        print("\n" + "="*60)
        print("步骤1: 从服务端同步数据")
        print("="*60)
        
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        try:
            # 连接服务器
            print(f"正在连接服务器 {self.config['server_host']}...")
            ssh.connect(
                hostname=self.config['server_host'],
                port=self.config['server_port'],
                username=self.config['server_user'],
                password=self.config.get('server_password'),
                key_filename=self.config.get('server_key_file'),
                timeout=30
            )
            print("✓ 服务器连接成功")
            
            # 获取服务端文件列表
            sftp = ssh.open_sftp()
            remote_dir = self.config['remote_dir']
            
            print(f"正在扫描远程目录: {remote_dir}")
            remote_files = sftp.listdir(remote_dir)
            new_files = []
            
            # 确保本地同步目录存在
            Path(self.config['local_sync_dir']).mkdir(parents=True, exist_ok=True)
            
            for filename in remote_files:
                remote_path = f"{remote_dir}/{filename}"
                
                try:
                    stat = sftp.stat(remote_path)
                    file_mtime = stat.st_mtime
                    
                    # 检查是否已同步
                    if filename not in self.synced_files or \
                       self.synced_files[filename]['mtime'] < file_mtime:
                        
                        local_path = os.path.join(self.config['local_sync_dir'], filename)
                        print(f"  下载: {filename} ({stat.st_size} bytes)")
                        sftp.get(remote_path, local_path)
                        
                        self.synced_files[filename] = {
                            'mtime': file_mtime,
                            'sync_time': time.time(),
                            'local_path': local_path,
                            'size': stat.st_size
                        }
                        new_files.append((filename, file_mtime))
                        print(f"  ✓ 已同步: {filename}")
                except Exception as e:
                    print(f"  ✗ 跳过文件 {filename}: {e}")
            
            sftp.close()
            ssh.close()
            
            self._save_sync_record()
            print(f"\n同步完成: 新增 {len(new_files)} 个文件，总计 {len(self.synced_files)} 个文件")
            return new_files
            
        except Exception as e:
            print(f"✗ 同步失败: {e}")
            return []
    
    def decrypt_files(self):
        """使用Fernet解密文件"""
        print("\n" + "="*60)
        print("步骤2: 解密文件")
        print("="*60)
        
        cipher = Fernet(self.fernet_key.encode())
        decrypted_files = []
        
        for filename, info in self.synced_files.items():
            local_path = info['local_path']
            
            if not os.path.exists(local_path):
                continue
            
            # 检查是否已解密
            decrypt_path = os.path.join(self.temp_decrypt_dir, filename)
            if os.path.exists(decrypt_path):
                decrypted_files.append(decrypt_path)
                continue
            
            try:
                print(f"  解密: {filename}")
                with open(local_path, 'rb') as f:
                    encrypted_data = f.read()
                
                decrypted_data = cipher.decrypt(encrypted_data)
                
                with open(decrypt_path, 'wb') as f:
                    f.write(decrypted_data)
                
                decrypted_files.append(decrypt_path)
                print(f"  ✓ 已解密: {filename} ({len(decrypted_data)} bytes)")
                
            except Exception as e:
                print(f"  ✗ 解密失败 {filename}: {e}")
        
        print(f"\n解密完成: 共 {len(decrypted_files)} 个文件")
        return decrypted_files
    
    def wait_for_stable(self, timeout=120):
        """等待确认没有新数据上传"""
        print("\n" + "="*60)
        print(f"步骤3: 等待数据稳定 ({timeout}秒)")
        print("="*60)
        
        for i in range(timeout, 0, -10):
            print(f"  剩余等待时间: {i} 秒...", end='\r')
            time.sleep(10)
        
        print("\n✓ 数据稳定，准备合并任务")
    
    def parse_filename(self, filename):
        """解析文件名获取任务信息"""
        # 格式: 99wpa_ctrl_job28_10043-1
        pattern = r'^\d+wp([a-z])_ctrl_job(\d+)_(\d+)-(\d+)'
        match = re.match(pattern, filename)
        
        if match:
            return {
                'printer_id': match.group(1),
                'job_id': match.group(2),
                'sub_task_id': int(match.group(3)),
                'operation_type': match.group(4)
            }
        return None
    
    def merge_tasks(self):
        """合并同任务的子任务文件"""
        print("\n" + "="*60)
        print("步骤4: 合并任务文件")
        print("="*60)
        
        # 按任务分组
        tasks = defaultdict(list)
        
        for filename in os.listdir(self.temp_decrypt_dir):
            file_path = os.path.join(self.temp_decrypt_dir, filename)
            info = self.parse_filename(filename)
            
            if info:
                task_key = f"job{info['job_id']}_type{info['operation_type']}_printer{info['printer_id']}"
                tasks[task_key].append({
                    'filename': filename,
                    'path': file_path,
                    'sub_task_id': info['sub_task_id'],
                    'info': info
                })
        
        merged_files = []
        
        for task_key, files in tasks.items():
            # 按子任务序号排序
            files.sort(key=lambda x: x['sub_task_id'])
            
            # 获取第一个文件的上传时间
            first_filename = files[0]['filename']
            upload_time_utc = self.synced_files.get(first_filename, {}).get('mtime', time.time())
            
            # UTC+0 转 UTC+8
            upload_time_local = datetime.fromtimestamp(upload_time_utc) + timedelta(hours=8)
            time_str = upload_time_local.strftime('%Y%m%d_%H%M%S')
            
            # 生成合并后的文件名
            info = files[0]['info']
            merged_filename = f"printer{info['printer_id']}_job{info['job_id']}_type{info['operation_type']}_{time_str}"
            merged_path = os.path.join(self.merge_dir, merged_filename)
            
            # 合并文件
            print(f"  合并任务: {merged_filename}")
            print(f"    子任务数: {len(files)}")
            
            with open(merged_path, 'wb') as outfile:
                for file_info in files:
                    with open(file_info['path'], 'rb') as infile:
                        outfile.write(infile.read())
            
            file_size = os.path.getsize(merged_path)
            print(f"    ✓ 合并完成: {file_size} bytes")
            
            merged_files.append({
                'path': merged_path,
                'filename': merged_filename,
                'info': info,
                'time': time_str
            })
        
        print(f"\n合并完成: 共 {len(merged_files)} 个任务")
        return merged_files
    
    def _convert_with_cupsfilter(self, input_file, output_file):
        """使用cupsfilter转换（最推荐）"""
        try:
            cmd = [
                'cupsfilter',
                '-m', 'application/pdf',
                '-p', '/etc/cups/ppd/default.ppd',  # 可选
                input_file
            ]
            
            with open(output_file, 'wb') as f:
                result = subprocess.run(
                    cmd,
                    stdout=f,
                    stderr=subprocess.PIPE,
                    timeout=60
                )
            
            if result.returncode == 0 and os.path.getsize(output_file) > 0:
                return True
        except Exception as e:
            print(f"    cupsfilter转换失败: {e}")
        return False
    
    def _convert_with_ghostscript(self, input_file, output_file):
        """使用ghostscript转换"""
        try:
            # 先尝试直接转换
            cmd = [
                'gs',
                '-dNOPAUSE',
                '-dBATCH',
                '-dSAFER',
                '-sDEVICE=pdfwrite',
                '-dCompatibilityLevel=1.4',
                '-dPDFSETTINGS=/printer',
                f'-sOutputFile={output_file}',
                input_file
            ]
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                timeout=60
            )
            
            if result.returncode == 0 and os.path.getsize(output_file) > 0:
                return True
        except Exception as e:
            print(f"    ghostscript转换失败: {e}")
        return False
    
    def _convert_with_imagemagick(self, input_file, output_file):
        """使用ImageMagick转换"""
        try:
            cmd = [
                'convert',
                input_file,
                '-quality', '90',
                output_file
            ]
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                timeout=60
            )
            
            if result.returncode == 0 and os.path.getsize(output_file) > 0:
                return True
        except Exception as e:
            print(f"    ImageMagick转换失败: {e}")
        return False
    
    def _parse_unirast_header(self, file_path):
        """解析unirast文件头获取图像信息"""
        try:
            with open(file_path, 'rb') as f:
                # 读取文件头
                header = f.read(1024)
                
                # unirast通常包含分辨率、颜色空间等信息
                # 这里是简化的解析，实际格式可能更复杂
                info = {
                    'width': 2480,  # A4 @ 300dpi
                    'height': 3508,
                    'dpi': 300,
                    'color_space': 'RGB'
                }
                
                return info
        except Exception as e:
            print(f"    解析文件头失败: {e}")
            return None
    
    def _convert_unirast_to_pdf_manual(self, input_file, output_file):
        """手动解析unirast并转换为PDF（备用方案）"""
        try:
            from PIL import Image
            import io
            
            # 读取文件
            with open(input_file, 'rb') as f:
                data = f.read()
            
            # 尝试跳过文件头，查找图像数据
            # unirast格式通常在头部之后是原始图像数据
            header_size = 1024  # 估计的头部大小
            
            if len(data) > header_size:
                image_data = data[header_size:]
                
                # 尝试创建图像
                info = self._parse_unirast_header(input_file)
                if info:
                    try:
                        img = Image.frombytes(
                            'RGB',
                            (info['width'], info['height']),
                            image_data
                        )
                        img.save(output_file, 'PDF', resolution=info['dpi'])
                        return True
                    except Exception:
                        pass
            
        except ImportError:
            print("    需要安装PIL: pip install Pillow")
        except Exception as e:
            print(f"    手动转换失败: {e}")
        
        return False
    
    def convert_to_pdf(self, merged_files):
        """将unirast格式转换为PDF"""
        print("\n" + "="*60)
        print("步骤5: 转换为PDF")
        print("="*60)
        
        success_count = 0
        fail_count = 0
        
        for file_info in merged_files:
            file_path = file_info['path']
            filename = file_info['filename']
            pdf_path = os.path.join(self.output_dir, f"{filename}.pdf")
            
            print(f"\n  处理: {filename}")
            
            converted = False
            
            # 方案1: 使用cupsfilter（最佳）
            if 'cupsfilter' in self.available_tools and not converted:
                print("    尝试方案1: cupsfilter")
                converted = self._convert_with_cupsfilter(file_path, pdf_path)
                if converted:
                    print("    ✓ cupsfilter转换成功")
            
            # 方案2: 使用ghostscript
            if 'gs' in self.available_tools and not converted:
                print("    尝试方案2: ghostscript")
                converted = self._convert_with_ghostscript(file_path, pdf_path)
                if converted:
                    print("    ✓ ghostscript转换成功")
            
            # 方案3: 使用ImageMagick
            if 'convert' in self.available_tools and not converted:
                print("    尝试方案3: ImageMagick")
                converted = self._convert_with_imagemagick(file_path, pdf_path)
                if converted:
                    print("    ✓ ImageMagick转换成功")
            
            # 方案4: 手动解析（需要Pillow）
            if not converted:
                print("    尝试方案4: 手动解析")
                converted = self._convert_unirast_to_pdf_manual(file_path, pdf_path)
                if converted:
                    print("    ✓ 手动转换成功")
            
            # 如果所有方案都失败，复制原文件
            if not converted:
                print("    ⚠ 所有转换方案失败，保存原始文件")
                shutil.copy(file_path, pdf_path.replace('.pdf', '.unirast'))
                fail_count += 1
            else:
                success_count += 1
                file_size = os.path.getsize(pdf_path)
                print(f"    文件大小: {file_size} bytes")
        
        print(f"\n转换完成: 成功 {success_count} 个，失败 {fail_count} 个")
        
        if fail_count > 0:
            print("\n提示: 安装转换工具以提高成功率:")
            print("  sudo apt-get install cups-filters ghostscript imagemagick")
            print("  pip install Pillow")
    
    def run(self):
        """运行完整流程"""
        print("\n" + "="*60)
        print("打印机审计数据同步系统")
        print("="*60)
        print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        start_time = time.time()
        
        try:
            # 1. 同步数据
            self.sync_from_server()
            
            # 2. 解密文件
            self.decrypt_files()
            
            # 3. 等待数据稳定
            if self.config.get('wait_stable', True):
                self.wait_for_stable(timeout=self.config.get('wait_timeout', 120))
            
            # 4. 合并任务
            merged_files = self.merge_tasks()
            
            # 5. 转换为PDF
            if merged_files:
                self.convert_to_pdf(merged_files)
            else:
                print("\n没有需要转换的文件")
            
            elapsed_time = time.time() - start_time
            print("\n" + "="*60)
            print(f"✓ 处理完成！耗时: {elapsed_time:.2f} 秒")
            print(f"结束时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print("="*60)
            
        except KeyboardInterrupt:
            print("\n\n用户中断操作")
        except Exception as e:
            print(f"\n✗ 发生错误: {e}")
            import traceback
            traceback.print_exc()


# 配置示例
if __name__ == "__main__":
    config = {
        # ========== 服务端配置 ==========
        'server_host': '192.168.1.100',
        'server_port': 22,
        'server_user': 'audit_user',
        
        # 认证方式1: 使用密码
        'server_password': 'your_password',
        
        # 认证方式2: 使用SSH密钥（推荐）
        # 'server_key_file': os.path.expanduser('~/.ssh/id_rsa'),
        
        'remote_dir': '/var/printer_data',
        
        # ========== 本地配置 ==========
        'local_sync_dir': './sync_data',
        'local_record_file': './sync_record.json',
        'temp_decrypt_dir': './decrypted_temp',
        'merge_dir': './merged_tasks',
        'output_dir': './pdf_output',
        
        # ========== 加密配置 ==========
        # 生成密钥: python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
        'fernet_key': 'your-fernet-key-here-32-bytes-base64-encoded==',
        
        # ========== 运行配置 ==========
        'wait_stable': True,      # 是否等待数据稳定
        'wait_timeout': 120,      # 等待时间（秒）
    }
    
    # 创建本地同步目录
    Path(config['local_sync_dir']).mkdir(parents=True, exist_ok=True)
    
    # 运行同步系统
    try:
        sync_system = PrintAuditSync(config)
        sync_system.run()
    except Exception as e:
        print(f"程序异常退出: {e}")
        import traceback
        traceback.print_exc()
