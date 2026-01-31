#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
打印机数据审计同步系统
功能：从服务端同步加密的打印数据，解密、合并、转换为PDF
"""

import os
import json
import subprocess
import time
from datetime import datetime, timedelta
from pathlib import Path
from cryptography.fernet import Fernet
import paramiko
from collections import defaultdict

class PrinterAuditSync:
    def __init__(self, config_file='config.json'):
        """初始化配置"""
        self.load_config(config_file)
        self.sync_record_file = 'sync_record.json'
        self.sync_records = self.load_sync_records()
        
    def load_config(self, config_file):
        """加载配置文件"""
        with open(config_file, 'r') as f:
            config = json.load(f)
        
        self.ssh_host = config['ssh_host']
        self.ssh_port = config.get('ssh_port', 22)
        self.ssh_user = config['ssh_user']
        self.ssh_password = config.get('ssh_password')
        self.ssh_key_file = config.get('ssh_key_file')
        self.remote_path = config['remote_path']
        self.local_download_path = Path(config['local_download_path'])
        self.decrypt_temp_path = Path(config['decrypt_temp_path'])
        self.output_path = Path(config['output_path'])
        self.fernet_key = config['fernet_key'].encode()
        
        # 创建必要的目录
        self.local_download_path.mkdir(parents=True, exist_ok=True)
        self.decrypt_temp_path.mkdir(parents=True, exist_ok=True)
        self.output_path.mkdir(parents=True, exist_ok=True)
        
    def load_sync_records(self):
        """加载同步记录"""
        if os.path.exists(self.sync_record_file):
            with open(self.sync_record_file, 'r') as f:
                return json.load(f)
        return {}
    
    def save_sync_records(self):
        """保存同步记录"""
        with open(self.sync_record_file, 'w') as f:
            json.dump(self.sync_records, f, indent=2)
    
    def sync_from_server(self):
        """从服务端增量同步数据"""
        print("开始从服务端同步数据...")
        
        # 建立SSH连接
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        if self.ssh_key_file:
            ssh.connect(self.ssh_host, port=self.ssh_port, 
                       username=self.ssh_user, key_filename=self.ssh_key_file)
        else:
            ssh.connect(self.ssh_host, port=self.ssh_port,
                       username=self.ssh_user, password=self.ssh_password)
        
        # 获取远程文件列表
        sftp = ssh.open_sftp()
        remote_files = sftp.listdir(self.remote_path)
        
        new_files = []
        for filename in remote_files:
            remote_file_path = f"{self.remote_path}/{filename}"
            stat = sftp.stat(remote_file_path)
            file_mtime = stat.st_mtime
            
            # 检查是否需要下载
            if filename not in self.sync_records or \
               self.sync_records[filename]['mtime'] < file_mtime:
                local_file = self.local_download_path / filename
                sftp.get(remote_file_path, str(local_file))
                
                self.sync_records[filename] = {
                    'mtime': file_mtime,
                    'sync_time': time.time(),
                    'upload_time': file_mtime  # 服务端上传时间
                }
                new_files.append(filename)
                print(f"已下载: {filename}")
        
        sftp.close()
        ssh.close()
        
        self.save_sync_records()
        print(f"同步完成，新增 {len(new_files)} 个文件")
        return new_files
    
    def decrypt_files(self, files):
        """解密文件"""
        print("开始解密文件...")
        fernet = Fernet(self.fernet_key)
        
        decrypted_files = []
        for filename in files:
            encrypted_file = self.local_download_path / filename
            decrypted_file = self.decrypt_temp_path / filename
            
            try:
                with open(encrypted_file, 'rb') as f:
                    encrypted_data = f.read()
                
                decrypted_data = fernet.decrypt(encrypted_data)
                
                with open(decrypted_file, 'wb') as f:
                    f.write(decrypted_data)
                
                decrypted_files.append(filename)
                print(f"已解密: {filename}")
            except Exception as e:
                print(f"解密失败 {filename}: {e}")
        
        return decrypted_files
    
    def wait_for_stable(self, timeout=120):
        """等待2分钟确认没有新数据上传"""
        print(f"等待 {timeout} 秒确认数据稳定...")
        time.sleep(timeout)
    
    def parse_filename(self, filename):
        """解析文件名：*_job任务序号_操作类型_子任务序号"""
        parts = filename.rsplit('_', 3)
        if len(parts) >= 4:
            prefix = parts[0]
            job_id = parts[1].replace('job', '')
            operation = parts[2]
            subtask = parts[3].split('.')[0]
            return {
                'prefix': prefix,
                'job_id': job_id,
                'operation': operation,
                'subtask': int(subtask),
                'filename': filename
            }
        return None
    
    def merge_job_files(self):
        """合并同任务的数据文件"""
        print("开始合并任务文件...")
        
        # 按任务分组
        jobs = defaultdict(list)
        for filename in os.listdir(self.decrypt_temp_path):
            parsed = self.parse_filename(filename)
            if parsed:
                job_key = f"{parsed['prefix']}_job{parsed['job_id']}_{parsed['operation']}"
                jobs[job_key].append(parsed)
        
        merged_files = []
        for job_key, file_list in jobs.items():
            # 按子任务序号排序
            file_list.sort(key=lambda x: x['subtask'])
            
            # 获取第一个子任务的上传时间（UTC 0时区）
            first_file = file_list[0]['filename']
            upload_time_utc = self.sync_records.get(first_file, {}).get('upload_time', time.time())
            
            # 转换为东8区时间
            upload_datetime = datetime.utcfromtimestamp(upload_time_utc) + timedelta(hours=8)
            time_str = upload_datetime.strftime('%Y%m%d_%H%M%S')
            
            # 合并文件
            merged_filename = f"{job_key}_{time_str}"
            merged_file = self.decrypt_temp_path / merged_filename
            
            with open(merged_file, 'wb') as outfile:
                for file_info in file_list:
                    file_path = self.decrypt_temp_path / file_info['filename']
                    with open(file_path, 'rb') as infile:
                        outfile.write(infile.read())
            
            merged_files.append(merged_filename)
            print(f"已合并任务: {job_key} -> {merged_filename}")
        
        return merged_files
    
    def convert_to_pdf(self, unirast_files):
        """将unirast格式转换为PDF"""
        print("开始转换为PDF...")
        
        for filename in unirast_files:
            input_file = self.decrypt_temp_path / filename
            output_file = self.output_path / f"{filename}.pdf"
            
            try:
                # 使用rasterview或其他工具转换
                # 注意：需要安装相应的转换工具
                cmd = [
                    'rasterview',  # 或使用其他转换工具
                    '-o', str(output_file),
                    str(input_file)
                ]
                
                # 如果rasterview不可用，可以使用cups-filters中的工具
                # 或者使用ImageMagick等
                subprocess.run(cmd, check=True)
                print(f"已转换: {filename} -> {filename}.pdf")
                
            except Exception as e:
                print(f"转换失败 {filename}: {e}")
                print("提示：请确保安装了unirast转PDF的工具，如cups-filters")
    
    def run(self):
        """运行完整流程"""
        print("=" * 50)
        print("打印机审计数据同步系统启动")
        print("=" * 50)
        
        # 1. 从服务端同步数据
        new_files = self.sync_from_server()
        
        if not new_files:
            print("没有新文件需要处理")
            return
        
        # 2. 解密文件
        decrypted_files = self.decrypt_files(new_files)
        
        # 3. 等待数据稳定
        self.wait_for_stable(120)
        
        # 4. 合并同任务文件并重命名
        merged_files = self.merge_job_files()
        
        # 5. 转换为PDF
        self.convert_to_pdf(merged_files)
        
        print("=" * 50)
        print("处理完成！")
        print("=" * 50)


# 配置文件示例 config.json
config_example = {
    "ssh_host": "your.server.com",
    "ssh_port": 22,
    "ssh_user": "audit_user",
    "ssh_key_file": "/path/to/private_key",
    "remote_path": "/var/printer_data",
    "local_download_path": "./downloads",
    "decrypt_temp_path": "./decrypted",
    "output_path": "./output_pdfs",
    "fernet_key": "your-fernet-key-here"
}

if __name__ == '__main__':
    # 首次运行时创建配置文件
    if not os.path.exists('config.json'):
        with open('config.json', 'w') as f:
            json.dump(config_example, f, indent=2)
        print("请先配置 config.json 文件")
    else:
        sync_system = PrinterAuditSync()
        sync_system.run()
