#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
打印机数据审计同步系统
功能：从服务端同步加密的打印数据，解密、合并、转换为PDF
文件命名格式：*wp打印机标识_ctrl__job任务序号_子任务序号-操作类型
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
            "fernet_key": "请使用Fernet.generate_key()生成密钥",
            "wait_stable_seconds": 120,
            "enable_pdf_conversion": true
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
        
        # 验证密钥是否已修改
        if config['fernet_key'] == "请使用Fernet.generate_key()生成密钥":
            print("错误：请先生成并配置Fernet密钥！")
            print("生成密钥方法：")
            print("  python3 -c \"from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())\"")
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
        self.wait_stable_seconds = config.get('wait_stable_seconds', 120)
        self.enable_pdf_conversion = config.get('enable_pdf_conversion', True)
        
        # 创建必要的目录
        self.local_download_path.mkdir(parents=True, exist_ok=True)
        self.decrypt_temp_path.mkdir(parents=True, exist_ok=True)
        self.output_path.mkdir(parents=True, exist_ok=True)
        
        print(f"配置加载成功:")
        print(f"  服务器: {self.ssh_host}:{self.ssh_port}")
        print(f"  远程路径: {self.remote_path}")
        print(f"  本地下载: {self.local_download_path}")
        print(f"  解密临时: {self.decrypt_temp_path}")
        print(f"  输出目录: {self.output_path}")
        
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
        print("\n" + "="*60)
        print("步骤1: 从服务端同步数据")
        print("="*60)
        
        try:
            # 建立SSH连接
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            
            print(f"正在连接到 {self.ssh_host}:{self.ssh_port}...")
            
            if self.ssh_key_file and os.path.exists(self.ssh_key_file):
                ssh.connect(self.ssh_host, port=self.ssh_port, 
                           username=self.ssh_user, key_filename=self.ssh_key_file)
            else:
                ssh.connect(self.ssh_host, port=self.ssh_port,
                           username=self.ssh_user, password=self.ssh_password)
            
            print("SSH连接成功！")
            
            # 获取远程文件列表
            sftp = ssh.open_sftp()
            
            try:
                remote_files = sftp.listdir(self.remote_path)
            except FileNotFoundError:
                print(f"错误：远程路径不存在: {self.remote_path}")
                sftp.close()
                ssh.close()
                return []
            
            print(f"远程文件总数: {len(remote_files)}")
            
            new_files = []
            for filename in remote_files:
                remote_file_path = f"{self.remote_path}/{filename}"
                try:
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
                            'upload_time': file_mtime
                        }
                        new_files.append(filename)
                        print(f"  ✓ 已下载: {filename}")
                except Exception as e:
                    print(f"  ✗ 处理文件 {filename} 时出错: {e}")
                    continue
            
            sftp.close()
            ssh.close()
            
            self.save_sync_records()
            print(f"\n同步完成，新增 {len(new_files)} 个文件")
            return new_files
            
        except paramiko.AuthenticationException:
            print("SSH认证失败，请检查用户名和密码/密钥")
            return []
        except paramiko.SSHException as e:
            print(f"SSH连接错误: {e}")
            return []
        except Exception as e:
            print(f"同步失败: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def decrypt_files(self, files):
        """解密文件"""
        print("\n" + "="*60)
        print("步骤2: 解密文件")
        print("="*60)
        
        try:
            fernet = Fernet(self.fernet_key)
        except Exception as e:
            print(f"密钥格式错误: {e}")
            return []
        
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
                print(f"  ✓ 已解密: {filename}")
            except Exception as e:
                print(f"  ✗ 解密失败 {filename}: {e}")
        
        print(f"\n解密完成，成功 {len(decrypted_files)}/{len(files)} 个文件")
        return decrypted_files
    
    def wait_for_stable(self, timeout=None):
        """等待确认没有新数据上传"""
        if timeout is None:
            timeout = self.wait_stable_seconds
        
        print("\n" + "="*60)
        print(f"步骤3: 等待数据稳定 ({timeout}秒)")
        print("="*60)
        
        for i in range(timeout, 0, -10):
            print(f"  剩余等待时间: {i} 秒...", end='\r')
            time.sleep(10)
        print(f"  等待完成！{' '*30}")
    
    def parse_filename(self, filename):
        """
        解析文件名：*wp打印机标识_ctrl__job任务序号_子任务序号-操作类型
        例如：prefix_wp001_ctrl__job123_001-print.dat
        返回：{
            'prefix': '前缀',
            'printer_id': 'wp001',
            'job_id': '123',
            'operation': 'print',
            'subtask': 1,
            'filename': '原文件名'
        }
        """
        try:
            # 移除文件扩展名
            name_without_ext = filename.rsplit('.', 1)[0] if '.' in filename else filename
            
            # 查找 _ctrl__ 分隔符
            if '_ctrl__' not in name_without_ext:
                return None
            
            # 分割前缀和后缀
            parts = name_without_ext.split('_ctrl__')
            if len(parts) != 2:
                return None
            
            prefix_part = parts[0]  # *wp打印机标识
            suffix_part = parts[1]  # job任务序号_子任务序号-操作类型
            
            # 提取打印机标识（wp开头的部分）
            prefix_segments = prefix_part.split('_')
            printer_id = None
            prefix = []
            
            for seg in prefix_segments:
                if seg.startswith('wp'):
                    printer_id = seg
                else:
                    prefix.append(seg)
            
            if not printer_id:
                return None
            
            # 解析后缀部分：job任务序号_子任务序号-操作类型
            suffix_segments = suffix_part.split('_')
            if len(suffix_segments) < 2:
                return None
            
            job_part = suffix_segments[0]  # job任务序号
            subtask_operation = '_'.join(suffix_segments[1:])  # 子任务序号-操作类型
            
            # 分离子任务序号和操作类型
            if '-' not in subtask_operation:
                return None
            
            subtask_str, operation = subtask_operation.split('-', 1)
            
            # 提取job序号
            if not job_part.startswith('job'):
                return None
            job_id = job_part.replace('job', '')
            
            return {
                'prefix': '_'.join(prefix) if prefix else '',
                'printer_id': printer_id,
                'job_id': job_id,
                'operation': operation,
                'subtask': int(subtask_str),
                'filename': filename
            }
            
        except Exception as e:
            print(f"  ✗ 解析文件名失败 {filename}: {e}")
            return None
    
    def merge_job_files(self):
        """按打印机和任务合并数据文件"""
        print("\n" + "="*60)
        print("步骤4: 合并任务文件")
        print("="*60)
        
        # 按打印机和任务分组：printer_id -> job_id -> operation -> files
        jobs = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
        
        all_files = list(os.listdir(self.decrypt_temp_path))
        print(f"待处理文件总数: {len(all_files)}")
        
        parsed_count = 0
        for filename in all_files:
            parsed = self.parse_filename(filename)
            if parsed:
                printer_id = parsed['printer_id']
                job_id = parsed['job_id']
                operation = parsed['operation']
                jobs[printer_id][job_id][operation].append(parsed)
                parsed_count += 1
        
        print(f"成功解析文件: {parsed_count}/{len(all_files)}")
        print(f"发现打印机数量: {len(jobs)}")
        
        merged_files = []
        
        # 按打印机处理
        for printer_id, printer_jobs in sorted(jobs.items()):
            print(f"\n处理打印机: {printer_id}")
            print(f"  任务数量: {len(printer_jobs)}")
            
            for job_id, operations in sorted(printer_jobs.items()):
                for operation, file_list in sorted(operations.items()):
                    # 按子任务序号排序
                    file_list.sort(key=lambda x: x['subtask'])
                    
                    # 获取第一个子任务的上传时间（UTC 0时区）
                    first_file = file_list[0]['filename']
                    upload_time_utc = self.sync_records.get(first_file, {}).get('upload_time', time.time())
                    
                    # 转换为东8区时间
                    upload_datetime = datetime.utcfromtimestamp(upload_time_utc) + timedelta(hours=8)
                    time_str = upload_datetime.strftime('%Y%m%d_%H%M%S')
                    
                    # 构建合并后的文件名
                    prefix = file_list[0]['prefix']
                    if prefix:
                        merged_filename = f"{prefix}_{printer_id}_job{job_id}_{operation}_{time_str}"
                    else:
                        merged_filename = f"{printer_id}_job{job_id}_{operation}_{time_str}"
                    
                    merged_file = self.decrypt_temp_path / merged_filename
                    
                    # 合并文件
                    try:
                        with open(merged_file, 'wb') as outfile:
                            for file_info in file_list:
                                file_path = self.decrypt_temp_path / file_info['filename']
                                with open(file_path, 'rb') as infile:
                                    outfile.write(infile.read())
                        
                        merged_files.append(merged_filename)
                        print(f"  ✓ Job{job_id}-{operation}: {len(file_list)}个子任务 -> {merged_filename}")
                    except Exception as e:
                        print(f"  ✗ 合并失败 Job{job_id}-{operation}: {e}")
        
        print(f"\n合并完成，生成 {len(merged_files)} 个合并文件")
        return merged_files
    
    def convert_to_pdf(self, unirast_files):
        """将unirast格式转换为PDF"""
        print("\n" + "="*60)
        print("步骤5: 转换为PDF")
        print("="*60)
        
        if not self.enable_pdf_conversion:
            print("PDF转换功能已禁用（配置文件设置）")
            return
        
        success_count = 0
        fail_count = 0
        
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
                
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
                
                if result.returncode == 0:
                    print(f"  ✓ 已转换: {filename}")
                    success_count += 1
                else:
                    # 方法2: 尝试使用cupsfilter
                    print(f"  尝试备用转换方法: {filename}")
                    self.convert_unirast_alternative(input_file, output_file)
                    success_count += 1
                    
            except FileNotFoundError:
                print(f"  ! 转换工具未找到，尝试备用方法: {filename}")
                try:
                    self.convert_unirast_alternative(input_file, output_file)
                    success_count += 1
                except Exception as e:
                    print(f"  ✗ 转换失败 {filename}: {e}")
                    fail_count += 1
            except subprocess.TimeoutExpired:
                print(f"  ✗ 转换超时 {filename}")
                fail_count += 1
            except Exception as e:
                print(f"  ✗ 转换失败 {filename}: {e}")
                fail_count += 1
        
        print(f"\n转换完成: 成功 {success_count}, 失败 {fail_count}")
    
    def convert_unirast_alternative(self, input_file, output_file):
        """备用转换方法：使用cupsfilter"""
        cmd = [
            'cupsfilter',
            '-m', 'application/pdf',
            str(input_file)
        ]
        
        with open(output_file, 'wb') as f:
            result = subprocess.run(cmd, stdout=f, stderr=subprocess.PIPE, timeout=60)
        
        if result.returncode == 0:
            print(f"  ✓ 已转换(备用方法): {input_file.name}")
        else:
            raise Exception(f"备用转换失败: {result.stderr.decode()}")
    
    def cleanup_temp_files(self):
        """清理临时文件（可选）"""
        print("\n是否清理临时文件？(y/n): ", end='')
        try:
            choice = input().strip().lower()
            if choice == 'y':
                import shutil
                shutil.rmtree(self.decrypt_temp_path)
                self.decrypt_temp_path.mkdir(parents=True, exist_ok=True)
                print("临时文件已清理")
        except:
            pass
    
    def run(self):
        """运行完整流程"""
        print("\n" + "="*60)
        print("打印机审计数据同步系统启动")
        print("="*60)
        print(f"启动时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        start_time = time.time()
        
        # 1. 从服务端同步数据
        new_files = self.sync_from_server()
        
        if not new_files:
            print("\n没有新文件需要处理")
            return
        
        # 2. 解密文件
        decrypted_files = self.decrypt_files(new_files)
        
        if not decrypted_files:
            print("\n没有成功解密的文件")
            return
        
        # 3. 等待数据稳定
        self.wait_for_stable()
        
        # 4. 合并同打印机同任务文件并重命名
        merged_files = self.merge_job_files()
        
        if not merged_files:
            print("\n没有文件需要合并")
            return
        
        # 5. 转换为PDF
        if self.enable_pdf_conversion:
            self.convert_to_pdf(merged_files)
        
        # 统计信息
        elapsed_time = time.time() - start_time
        
        print("\n" + "="*60)
        print("处理完成！")
        print("="*60)
        print(f"处理时间: {elapsed_time:.2f} 秒")
        print(f"新增文件: {len(new_files)}")
        print(f"解密成功: {len(decrypted_files)}")
        print(f"合并文件: {len(merged_files)}")
        print(f"输出目录: {self.output_path}")
        print("="*60)
        
        # 可选：清理临时文件
        # self.cleanup_temp_files()


def generate_key():
    """生成Fernet密钥的辅助函数"""
    key = Fernet.generate_key()
    print("生成的Fernet密钥:")
    print(key.decode())
    print("\n请将此密钥复制到config.json的fernet_key字段")


if __name__ == '__main__':
    import sys
    
    # 如果参数是 --generate-key，则生成密钥
    if len(sys.argv) > 1 and sys.argv[1] == '--generate-key':
        generate_key()
        exit(0)
    
    try:
        sync_system = PrinterAuditSync()
        sync_system.run()
    except KeyboardInterrupt:
        print("\n\n程序被用户中断")
    except Exception as e:
        print(f"\n程序运行出错: {e}")
        import traceback
        traceback.print_exc()
