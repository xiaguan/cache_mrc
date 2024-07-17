import csv

def convert_csv(input_file, output_file):
    with open(input_file, 'r') as infile, open(output_file, 'w', newline='') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)

        # 写入标题行
        writer.writerow(['timestamp','command','key', 'size', 'ttl'])

        # 处理数据行
        for row in reader:
            if row:  # 跳过空行
                writer.writerow([row[0], '0',row[1].strip(), row[2].strip(), '0'])  # 设置 ttl 为 0

if __name__ == '__main__':
    input_file = 'twitter_cluster52.csv'  # 替换为你的输入文件名
    output_file = 'test_twitter.csv'  # 替换为你的输出文件名
    convert_csv(input_file, output_file)
