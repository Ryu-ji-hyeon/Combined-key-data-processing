from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id,rand
import time

spark = SparkSession.builder.appName('missing').config("spark.executor.memory", "16g").getOrCreate()
#spark.conf.set("spark.sql.shuffle.partitions", "100")
spark.conf.set("spark.sql.debug.maxToStringFields", 1000)


# # 8억건
# aa = spark.read.csv("/home/data/bc_sac_202312141739_final-20231215163224.csv", header=True, inferSchema=True)

start = time.time()
# 2천만건
a_2000 = spark.read.csv("/home/data/최종결과_1120_4.csv", header=True, inferSchema=True,encoding='cp949')
a1_2000 = spark.read.csv("/home/data/최종결과_1120_4.csv", header=True, inferSchema=True,encoding='cp949')

aa = a_2000.union(a_2000).union(a_2000).union(a_2000).union(a_2000).a_2000.union(a_2000).union(a_2000).union(a_2000).union(a_2000)
aa1 = a1_2000.union(a1_2000).union(a1_2000).union(a1_2000).union(a1_2000).a1_2000.union(a1_2000).union(a1_2000).union(a1_2000).union(a1_2000)
# aa.show()

aa_seq_id = aa.withColumn("seq_id", monotonically_increasing_id())
result_A = aa_seq_id.join(aa1, aa_seq_id.seq_id  == aa1.colNo, how="inner")
result_A.show()


column_count = len(aa.columns)
print(f"Row count: {aa.count()}")
print(f"Column count: {column_count}")
end = time.time()
print(int(end-start),'초')




 