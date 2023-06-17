from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("CountUserQueryTimes")
sc = SparkContext(conf=conf)

file_path = "hdfs:///SogouQ/006/SogouQ.sample"  # 文件路径
rdd = sc.textFile(file_path)  # 读取文件

query_times_rdd = (
    rdd.filter(lambda line: bool(line.strip()))  # 去除空行
       .map(lambda line: (line.split()[1], 1))  # 将每行数据映射为(user_id, 1)的二元组
       .reduceByKey(lambda x, y: x+y)  # 按user_id聚合，并累加查询次数
)

# 将查询次数的结果按次数降序排序
query_times_rdd_sorted = query_times_rdd.map(lambda x: (x[1], x[0])).sortByKey(ascending=False)

# 输出前10个查询次数最多的用户
for i, (count, user_id) in enumerate(query_times_rdd_sorted.take(10)):
    print("Top {} user: user_id={}, query times={}".format(i+1, user_id, count))
sc.stop()