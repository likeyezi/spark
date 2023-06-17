from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("CountQualifiedLines")
sc = SparkContext(conf=conf)

file_path = "hdfs:///SogouQ/006/SogouQ.sample"  # 文件路径
rdd = sc.textFile(file_path)  # 读取文件

qualified_count = (
    rdd.filter(lambda line: bool(line.strip()))  # 去除空行
       .map(lambda line: line.split())  # 按空格分割每一行数据
       .filter(lambda items: len(items) == 6)  # 过滤掉不是6列的记录
       .count()
)

print("符合要求的行数:", qualified_count)
sc.stop()