def convert_to_pdf(self, unirast_files):
    """将unirast格式转换为PDF - 多种方法尝试"""
    print("开始转换为PDF...")
    
    for filename in unirast_files:
        input_file = self.decrypt_temp_path / filename
        output_file = self.output_path / f"{filename}.pdf"
        
        # 尝试多种转换方法
        success = False
        
        # 方法1: 使用 gstoraster + gs (最可靠)
        if not success:
            success = self.convert_with_ghostscript(input_file, output_file)
        
        # 方法2: 使用 rasterview
        if not success:
            success = self.convert_with_rasterview(input_file, output_file)
        
        # 方法3: 使用 ImageMagick
        if not success:
            success = self.convert_with_imagemagick(input_file, output_file)
        
        # 方法4: 使用 cups-raster 工具
        if not success:
            success = self.convert_with_cups_raster(input_file, output_file)
        
        if success:
            print(f"✓ 已转换: {filename} -> {filename}.pdf")
        else:
            print(f"✗ 转换失败: {filename}")
            print(f"  原始文件保存在: {input_file}")

def convert_with_ghostscript(self, input_file, output_file):
    """使用 Ghostscript 转换（推荐方法）"""
    try:
        # 先将 unirast 转为 PostScript，再转 PDF
        ps_file = input_file.with_suffix('.ps')
        
        # Step 1: unirast -> PostScript
        cmd1 = [
            'rastertops',
            str(input_file)
        ]
        
        with open(ps_file, 'w') as f:
            result = subprocess.run(cmd1, stdout=f, stderr=subprocess.PIPE, text=True)
        
        if result.returncode != 0:
            return False
        
        # Step 2: PostScript -> PDF
        cmd2 = [
            'gs',
            '-dNOPAUSE',
            '-dBATCH',
            '-sDEVICE=pdfwrite',
            '-dCompatibilityLevel=1.4',
            '-dPDFSETTINGS=/printer',
            f'-sOutputFile={output_file}',
            str(ps_file)
        ]
        
        result = subprocess.run(cmd2, capture_output=True, text=True)
        
        # 清理临时 PS 文件
        if ps_file.exists():
            ps_file.unlink()
        
        return result.returncode == 0
        
    except FileNotFoundError:
        return False
    except Exception as e:
        print(f"  Ghostscript 转换错误: {e}")
        return False

def convert_with_rasterview(self, input_file, output_file):
    """使用 rasterview 转换"""
    try:
        cmd = [
            'rasterview',
            '-o', str(output_file),
            str(input_file)
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        return result.returncode == 0
        
    except FileNotFoundError:
        return False
    except Exception as e:
        print(f"  rasterview 转换错误: {e}")
        return False

def convert_with_imagemagick(self, input_file, output_file):
    """使用 ImageMagick 转换"""
    try:
        cmd = [
            'convert',
            str(input_file),
            str(output_file)
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        return result.returncode == 0
        
    except FileNotFoundError:
        return False
    except Exception as e:
        print(f"  ImageMagick 转换错误: {e}")
        return False

def convert_with_cups_raster(self, input_file, output_file):
    """使用 CUPS raster 工具链转换"""
    try:
        # 使用 rastertopdf (如果可用)
        cmd = [
            'rastertopdf',
            '1',  # job ID
            'user',  # user
            'title',  # title
            '1',  # copies
            '',  # options
        ]
        
        with open(input_file, 'rb') as infile:
            with open(output_file, 'wb') as outfile:
                result = subprocess.run(
                    cmd,
                    stdin=infile,
                    stdout=outfile,
                    stderr=subprocess.PIPE,
                    timeout=60
                )
        
        return result.returncode == 0
        
    except FileNotFoundError:
        return False
    except Exception as e:
        print(f"  CUPS raster 转换错误: {e}")
        return False
