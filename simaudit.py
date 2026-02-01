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
        # 检查配置文件
        if not os.path.exists(config_file):
            self.create_default_config(config_file)
            print(f"已创建默认配置文件: {config_file}")
            print("请修改配置文件后再运行程序！")
            exit(0)
        
        self.load_config(config_file)
        self.sync_record_file = 'sync_record.json'
        self.sync_records = self.load_sync_records()
    
    def create_default_config(self, config_file):
        """创建默认配置文件"""
        config_example = {
            "ssh_host": "192.168.1.100",
            "ssh_port": 22,
            "ssh_user": "audit_user",
            "ssh_password": "your_password",
            "ssh_key_file": "",
            "remote_path": "/var/printer_data",
            "local_download_path": "./downloads",
            "decrypt_temp_path": "./decrypted",
            "output_path": "./output_pdfs",
            "fernet_key": "请使用Fernet.generate_key()生成密钥"
        }
        
        with open(config_file, 'w', encoding='utf-8') as f:
            json.dump(config_example, f, indent=2, ensure_ascii=False)
        
    def load_config(self, config_file):
        """加载配置文件"""
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                content = f.read().strip()
                if not content:
                    raise ValueError("配置文件为空")
                config = json.loads(content)
        except json.JSONDecodeError as e:
            print(f"配置文件格式错误: {e}")
            print("请检查config.json文件格式是否正确")
            exit(1)
        except Exception as e:
            print(f"读取配置文件失败: {e}")
            exit(1)
        
        # 验证必要的配置项
        required_fields = ['ssh_host', 'ssh_user', 'remote_path', 
                          'local_download_path', 'decrypt_temp_path', 
                          'output_path', 'fernet_key']
        
        for field in required_fields:
            if field not in config:
                print(f"配置文件缺少必要字段: {field}")
                exit(1)
        
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
            try:
                with open(self.sync_record_file, 'r') as f:
                    return json.load(f)
            except:
                return {}
        return {}
    
    def save_sync_records(self):
        """保存同步记录"""
        with open(self.sync_record_file, 'w') as f:
            json.dump(self.sync_records, f, indent=2)
    
    def sync_from_server(self):
        """从服务端增量同步数据"""
        print("开始从服务端同步数据...")
        
        try:
            # 建立SSH连接
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            
            if self.ssh_key_file and os.path.exists(self.ssh_key_file):
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
            
        except Exception as e:
            print(f"同步失败: {e}")
            return []
    
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
        """
        解析文件名：*wp打印机标识_ctrl_job任务序号_子任务序号-操作类型
        例如：datawp001_ctrl_job12345_001-print.dat
        """
        try:
            # 移除文件扩展名
            name_without_ext = filename.rsplit('.', 1)[0]
            
            # 分割文件名
            parts = name_without_ext.split('_')
            
            if len(parts) < 4:
                return None
            
            # 查找wp打印机标识的位置
            printer_id = None
            ctrl_index = -1
            
            for i, part in enumerate(parts):
                if 'wp' in part.lower():
                    printer_id = part
                if part == 'ctrl':
                    ctrl_index = i
                    break
            
            if not printer_id or ctrl_index == -1:
                return None
            
            # 获取job任务序号
            job_part = parts[ctrl_index + 1]
            if not job_part.startswith('job'):
                return None
            job_id = job_part.replace('job', '')
            
            # 获取子任务序号和操作类型
            last_part = parts[ctrl_index + 2]
            if '-' not in last_part:
                return None
            
            subtask_str, operation = last_part.split('-', 1)
            subtask = int(subtask_str)
            
            # 获取前缀（wp之前的部分）
            prefix_parts = []
            for i, part in enumerate(parts):
                if part == printer_id:
                    break
                prefix_parts.append(part)
            prefix = '_'.join(prefix_parts) if prefix_parts else ''
            
            return {
                'prefix': prefix,
                'printer_id': printer_id,
                'job_id': job_id,
                'operation': operation,
                'subtask': subtask,
                'filename': filename
            }
            
        except Exception as e:
            print(f"解析文件名失败 {filename}: {e}")
            return None
    
    def merge_job_files(self):
        """按打印机和任务合并数据文件"""
        print("开始合并任务文件...")
        
        # 按打印机+任务分组
        jobs = defaultdict(list)
        for filename in os.listdir(self.decrypt_temp_path):
            parsed = self.parse_filename(filename)
            if parsed:
                # 按打印机+任务序号+操作类型分组
                job_key = f"{parsed['printer_id']}_job{parsed['job_id']}_{parsed['operation']}"
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
            
            # 合并文件，文件名格式：打印机标识_job任务序号_操作类型_时间
            merged_filename = f"{job_key}_{time_str}"
            merged_file = self.decrypt_temp_path / merged_filename
            
            with open(merged_file, 'wb') as outfile:
                for file_info in file_list:
                    file_path = self.decrypt_temp_path / file_info['filename']
                    with open(file_path, 'rb') as infile:
                        outfile.write(infile.read())
            
            merged_files.append(merged_filename)
            print(f"已合并任务 [{file_list[0]['printer_id']}]: {job_key} ({len(file_list)}个子任务) -> {merged_filename}")
        
        return merged_files
    
    def convert_to_pdf(self, unirast_files):
        """将unirast格式转换为PDF"""
        print("开始转换为PDF...")
        
        for filename in unirast_files:
            input_file = self.decrypt_temp_path / filename
            output_file = self.output_path / f"{filename}.pdf"
            
            try:
                # 方法1: 使用rasterview (需要安装cups-filters)
                cmd = [
                    'rasterview',
                    '-o', str(output_file),
                    str(input_file)
                ]
                
                result = subprocess.run(cmd, capture_output=True, text=True)
                
                if result.returncode == 0:
                    print(f"✓ 已转换: {filename} -> {filename}.pdf")
                else:
                    # 方法2: 尝试使用gstoraster的逆向工具
                    print(f"rasterview转换失败，尝试其他方法...")
                    self.convert_unirast_alternative(input_file, output_file)
                    
            except FileNotFoundError:
                print(f"⚠ rasterview未安装，尝试替代方法...")
                self.convert_unirast_alternative(input_file, output_file)
            except Exception as e:
                print(f"✗ 转换失败 {filename}: {e}")
    
    def convert_unirast_alternative(self, input_file, output_file):
        """使用替代方法转换unirast"""
        try:
            # 尝试使用cups-raster工具
            cmd = [
                'rastertopdf',
                str(input_file),
                '1',  # job-id
                'user',  # user
                'title',  # title
                '1',  # copies
                ''  # options
            ]
            
            with open(output_file, 'wb') as f:
                result = subprocess.run(cmd, stdout=f, stderr=subprocess.PIPE)
            
            if result.returncode == 0:
                print(f"✓ 已转换 (rastertopdf): {output_file.name}")
            else:
                print(f"✗ 转换失败: {result.stderr.decode()}")
                
        except Exception as e:
            print(f"✗ 替代转换方法失败: {e}")
            print("提示：请安装 cups-filters 包")
    
    def run(self):
        """运行完整流程"""
        print("=" * 60)
        print("打印机审计数据同步系统启动")
        print("=" * 60)
        
        # 1. 从服务端同步数据
        new_files = self.sync_from_server()
        
        if not new_files:
            print("没有新文件需要处理")
            return
        
        # 2. 解密文件
        decrypted_files = self.decrypt_files(new_files)
        
        if not decrypted_files:
            print("没有成功解密的文件")
            return
        
        # 3. 等待数据稳定
        self.wait_for_stable(120)
        
        # 4. 按打印机合并同任务文件并重命名
        merged_files = self.merge_job_files()
        
        if not merged_files:
            print("没有可合并的任务文件")
            return
        
        # 5. 转换为PDF
        self.convert_to_pdf(merged_files)
        
        print("=" * 60)
        print(f"处理完成！共处理 {len(merged_files)} 个打印任务")
        print(f"输出目录: {self.output_path}")
        print("=" * 60)


if __name__ == '__main__':
    try:
        sync_system = PrinterAuditSync()
        sync_system.run()
    except KeyboardInterrupt:
        print("\n程序被用户中断")
    except Exception as e:
        print(f"程序运行出错: {e}")
        import traceback
        traceback.print_exc()
nirast_files:
            input_file = self.decrypt_temp_path / filename
            output_file = self.output_path / f"{filename}.pdf"
            
            try:
                # 方法1: 使用 rasterview (需要安装 cups-filters)
                cmd = [
                    'rasterview',
                    '-o', str(output_file),
                    str(input_file)
                ]
                
                result = subprocess.run(cmd, capture_output=True, text=True)
                
                if result.returncode == 0:
                    print(f"✓ 已转换: {filename} -> {filename}.pdf")
                else:
                    # 方法2: 尝试使用 cupsfilter
                    cmd2 = [
                        'cupsfilter',
                        '-m', 'application/pdf',
                        str(input_file)
                    ]
                    with open(output_file, 'wb') as f:
                        result2 = subprocess.run(cmd2, stdout=f, stderr=subprocess.PIPE)
                    
                    if result2.returncode == 0:
                        print(f"✓ 已转换: {filename} -> {filename}.pdf")
                    else:
                        print(f"✗ 转换失败 {filename}: 请检查转换工具是否安装")
                
            except FileNotFoundError:
                print(f"✗ 转换工具未找到，请安装: sudo apt install cups-filters")
                print(f"  或 macOS: brew install cups")
                break
            except Exception as e:
                print(f"✗ 转换失败 {filename}: {e}")
    
    def run(self):
        """运行完整流程"""
        print("=" * 60)
        print("打印机审计数据同步系统启动")
        print("=" * 60)
        
        # 1. 从服务端同步数据
        new_files = self.sync_from_server()
        
        if not new_files:
            print("没有新文件需要处理")
            return
        
        # 2. 解密文件
        decrypted_files = self.decrypt_files(new_files)
        
        if not decrypted_files:
            print("没有成功解密的文件")
            return
        
        # 3. 等待数据稳定
        self.wait_for_stable(120)
        
        # 4. 合并同任务文件并重命名
        merged_files = self.merge_job_files()
        
        if not merged_files:
            print("没有需要合并的任务文件")
            return
        
        # 5. 转换为PDF
        self.convert_to_pdf(merged_files)
        
        print("=" * 60)
        print("处理完成！")
        print(f"输出目录: {self.output_path}")
        print("=" * 60)


if __name__ == '__main__':
    try:
        sync_system = PrinterAuditSync()
        sync_system.run()
    except KeyboardInterrupt:
        print("\n程序被用户中断")
    except Exception as e:
        print(f"程序运行出错: {e}")
        import traceback
        traceback.print_exc()
