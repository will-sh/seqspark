#!/usr/bin/env python3
"""
测试脚本 - 验证SeqSpark工具的功能
"""

import os
import sys
import tempfile
import subprocess
from pathlib import Path

def create_test_fasta(filename, num_sequences=1000):
    """创建测试FASTA文件"""
    with open(filename, 'w') as f:
        for i in range(num_sequences):
            f.write(f">seq_{i+1} test sequence {i+1}\n")
            f.write(f"ATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCG\n")
    return filename

def create_test_fastq(filename, num_sequences=1000):
    """创建测试FASTQ文件"""
    with open(filename, 'w') as f:
        for i in range(num_sequences):
            f.write(f"@seq_{i+1} test sequence {i+1}\n")
            f.write(f"ATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCG\n")
            f.write(f"+\n")
            f.write(f"IIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIII\n")
    return filename

def count_sequences(filename):
    """计算序列数量"""
    count = 0
    with open(filename, 'r') as f:
        for line in f:
            if line.startswith('>') or line.startswith('@'):
                count += 1
    return count

def test_sampling(script_path, test_file, format_type, num_sequences):
    """测试采样功能"""
    print(f"测试 {format_type} 格式文件采样...")
    
    # 测试按数量采样
    sample_number = 100
    output_file = f"test_output_{format_type}_number.txt"
    
    try:
        cmd = [
            sys.executable, script_path,
            '-i', test_file,
            '-n', str(sample_number),
            '-o', output_file,
            '--master', 'local[1]',
            '--memory', '1g',
            '-q'
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            sampled_count = count_sequences(output_file)
            print(f"  ✓ 按数量采样成功: {sampled_count}/{sample_number}")
            
            if sampled_count <= sample_number:
                print(f"  ✓ 采样数量正确")
            else:
                print(f"  ✗ 采样数量错误: 期望<={sample_number}, 实际={sampled_count}")
        else:
            print(f"  ✗ 按数量采样失败: {result.stderr}")
            
        # 清理文件
        if os.path.exists(output_file):
            os.remove(output_file)
            
    except Exception as e:
        print(f"  ✗ 测试失败: {e}")
    
    # 测试按比例采样
    proportion = 0.1
    output_file = f"test_output_{format_type}_proportion.txt"
    
    try:
        cmd = [
            sys.executable, script_path,
            '-i', test_file,
            '-p', str(proportion),
            '-o', output_file,
            '--master', 'local[1]',
            '--memory', '1g',
            '-q'
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            sampled_count = count_sequences(output_file)
            expected_count = int(num_sequences * proportion)
            print(f"  ✓ 按比例采样成功: {sampled_count} (期望约{expected_count})")
            
            # 允许一定的误差范围
            if abs(sampled_count - expected_count) <= expected_count * 0.3:
                print(f"  ✓ 采样比例正确")
            else:
                print(f"  ✗ 采样比例错误: 期望约{expected_count}, 实际={sampled_count}")
        else:
            print(f"  ✗ 按比例采样失败: {result.stderr}")
            
        # 清理文件
        if os.path.exists(output_file):
            os.remove(output_file)
            
    except Exception as e:
        print(f"  ✗ 测试失败: {e}")

def main():
    """主测试函数"""
    print("SeqSpark 测试工具")
    print("=" * 50)
    
    # 获取脚本路径
    script_dir = Path(__file__).parent
    script_path = script_dir / "seqspark_sample.py"
    
    if not script_path.exists():
        print(f"错误: 找不到脚本文件 {script_path}")
        sys.exit(1)
    
    # 创建临时目录
    with tempfile.TemporaryDirectory() as temp_dir:
        os.chdir(temp_dir)
        
        # 创建测试文件
        fasta_file = create_test_fasta("test.fasta", 1000)
        fastq_file = create_test_fastq("test.fastq", 1000)
        
        print(f"创建测试文件:")
        print(f"  FASTA: {fasta_file} (1000 sequences)")
        print(f"  FASTQ: {fastq_file} (1000 sequences)")
        print()
        
        # 测试FASTA文件
        test_sampling(str(script_path), fasta_file, "fasta", 1000)
        print()
        
        # 测试FASTQ文件
        test_sampling(str(script_path), fastq_file, "fastq", 1000)
        print()
        
        # 测试格式检测
        print("测试格式检测...")
        try:
            cmd = [
                sys.executable, str(script_path),
                '-i', fasta_file,
                '-n', '50',
                '-o', 'test_auto_format.txt',
                '--master', 'local[1]',
                '--memory', '1g',
                '-q'
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                print("  ✓ 格式自动检测成功")
                if os.path.exists('test_auto_format.txt'):
                    os.remove('test_auto_format.txt')
            else:
                print(f"  ✗ 格式自动检测失败: {result.stderr}")
                
        except Exception as e:
            print(f"  ✗ 格式检测测试失败: {e}")
        
        print()
        print("测试完成！")
        print("=" * 50)

if __name__ == "__main__":
    main() 