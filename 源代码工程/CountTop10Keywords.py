from pyspark import SparkContext, SparkConf
import matplotlib.pyplot as plt
from collections import Counter

conf = SparkConf().setAppName("CountTop10Keywords")
sc = SparkContext(conf=conf)

file_path = "hdfs:///SogouQ/006/SogouQ.sample"  # 文件路径
keyword_rdd = (
    sc.textFile(file_path)  # 读取文件
    .filter(lambda line: bool(line.strip()))  # 去除空行
    .flatMap(lambda line: line.split()[2].split("|"))  # 以|分割查询词，将一个查询分割后的结果展开
    .map(lambda word: (word, 1))  # 将每个查询关键词映射为(word, 1)的二元组
    .reduceByKey(lambda x, y: x + y)  # 按关键词聚合并累加出现次数
)

# 输出top10查询关键词
top10_keywords = dict(Counter(dict(keyword_rdd.collect())).most_common(10))
print("Top 10 query keywords:")
for keyword, count in top10_keywords.items():
    print(keyword, count)

# 将top10查询关键词写入文件
with open("top10_keywords.txt", "w") as f:
    for keyword, count in top10_keywords.items():
        f.write("{} {}\n".format(keyword, count))
