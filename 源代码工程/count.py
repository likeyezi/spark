from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("CountTotalLines")
sc = SparkContext(conf=conf)

file_path = "hdfs:///SogouQ/006/SogouQ.sample"  # 文件路径
rdd = sc.textFile(file_path)  # 读取文件
count = rdd.count()  # 统计文件总行数

print("文件总行数:", count)
sc.stop()