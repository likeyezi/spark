from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("CountSecondRankFirstClick")
sc = SparkContext(conf=conf)

file_path = "hdfs:///SogouQ/006/SogouQ.sample"  # 文件路径
rdd = sc.textFile(file_path)  # 读取文件

result_rdd = (
    rdd.filter(lambda line: bool(line.strip()))  # 去除空行
    .map(lambda line: line.split())  # 按空格分割每一行数据
    .filter(lambda items: len(items) == 6 and items[3] == "2" and items[4] == "1")  # 过滤符合条件的记录
    .cache()  # 缓存rdd，以便后续快速访问
)

count = result_rdd.count()  # 统计符合条件的记录总数
if count > 0:
    sample = result_rdd.take(1)[0]  # 随机选取一条符合条件的记录
    print("排名第2，点击顺序排在第1的数据: ", sample)  # 输出选取的记录
else:
    print("未找到记录的信息")  # 输出未找到记录的信息

result_rdd.unpersist()  # 释放缓存
sc.stop()
