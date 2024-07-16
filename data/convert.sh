#!/bin/bash

# 提取 lbn 列
cut -d "," -f 5 cloud.csv > lbn_values.txt

# 构建 AccessRecord CSV 文件头
echo "timestamp,command,key,size,ttl" > access_records.csv

# 循环处理 lbn 值并生成 CSV 行
while read lbn; do
    echo "0,0,$lbn,0,0" >> access_records.csv
done < lbn_values.txt

# 删除临时文件
rm lbn_values.txt