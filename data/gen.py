import csv
import random
import zipfile
import numpy as np

def generate_zipf_key(alpha, n):
    """生成符合Zipf分布的key"""
    x = np.arange(1, n + 1)
    weights = x ** (-alpha)
    weights /= weights.sum()
    return np.random.choice(x, p=weights)

def generate_csv(filename, size_mb, alpha=1.3):
    """生成CSV文件"""
    target_size = size_mb * 1024 * 1024  # 转换为字节
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["timestamp", "command", "key", "size", "ttl"])

        total_size = 0
        while total_size < target_size:
            key = generate_zipf_key(alpha, 1000000)  # 假设key范围在1到1000000之间
            row = [0, 0, key, 0, 0]
            writer.writerow(row)
            total_size += len(','.join(map(str, row))) + 2  # 估算每行大小

if __name__ == "__main__":
    generate_csv("zipf_data.csv", 50)
