from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id,rand
import time

spark = SparkSession.builder.appName('missing').config("spark.executor.memory", "16g").getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", "100")
#2천만건 X 2천만건 join

# # 8억건
# aa = spark.read.csv("/home/data/bc_sac_202312141739_final-20231215163224.csv", header=True, inferSchema=True)

# 2천만건
a_2000 = spark.read.csv("/home/data/nettier_new.csv/part-00000-ac64c6d5-3dd9-4abd-b6d0-39ff48b361ed-c000.csv", header=True, inferSchema=True)


# 8억건 + seq_id
aa_seq_id = a_2000.withColumn("seq_id", monotonically_increasing_id())

# 8억건 + id(rand)
aa_id = a_2000.withColumn("id", (rand() * 800000000).cast("long"))

# # 결과 확인
# a_2000.show()

start = time.time()
# A_B를 기준으로 A와 조인
result_A = aa_seq_id.join(aa_id, aa_seq_id.seq_id  == aa_id.colNo, how="inner")
result_A.show()
result_B.show()
# # A_B를 기준으로 B와 조인
# result_B = a_2000.join(aa_id, a_2000.colNo == aa_id.id, how="inner")

# # X A_B를 기준으로 C와 조인
# result_C = A_B.join(C, A_B.C_id == C.id, how="inner")

# # A, B, C의 조인 결과를 A_B를 기준으로 조인
# final_result = result_A.join(result_B, result_A . colNo == result_B.colNo , how="inner")
# # X final_result = final_result.join(result_C, final_result . C_id_a == result_C . id_c, how="inner")
# final_result = final_result.repartition(1000)
# final_result.show()


column_count = len(result_A.columns)
print(f"Row count: {result_A.count()}")
print(f"Column count: {column_count}")
end = time.time()
print(int(end-start),'초')







# # DataFrame을 하나의 파티션으로 줄임
# aa_single_partition = aa_with_id.coalesce(1)

# # 결과를 CSV 파일로 저장
# output_path = "/home/data/plus_seq_id.csv"
# aa_single_partition.write.mode("overwrite").csv(output_path, header=True)


 