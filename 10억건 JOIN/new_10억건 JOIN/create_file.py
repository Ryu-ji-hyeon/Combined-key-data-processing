from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import time
from function import rdd
spark = SparkSession.builder \
    .appName('example') \
    .config("spark.local.dir", "/home/spark-temp") \
    .config("spark.executor.memory", "8g") \
    .config("spark.sql.shuffle.partitions", "10000") \
    .getOrCreate()
spark.stop()
spark = SparkSession.builder \
    .appName('example') \
    .config("spark.local.dir", "/home/spark-temp") \
    .config("spark.executor.memory", "8g") \
    .config("spark.sql.shuffle.partitions", "10000") \
    .getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", "10000")

if __name__ == "__main__":
    start = time.time()

    # 데이터 프레임 생성
    one1 = spark.read.csv("/home/data/8억건.csv", header=True, inferSchema=True, encoding='utf-8')
    one = one1.selectExpr('key_1210 as key_1210_one','sex_ctgo_cd as sex_ctgo_cd_one', 'cstmr_hom_scls_code as cstmr_hom_scls_code_one',
                      'job_cd as job_cd_one', 'indv_incm_amt as indv_incm_amt_one', 'seg_34 as seg_34_one',
                      'sale_date as sale_date_one', 'auth_time as auth_time_one', 'mer_tpbuz_no as mer_tpbuz_no_one',
                      'mer_rgn_scls_code as mer_rgn_scls_code_one', 'sale_amt as sale_amt_one', 
                      'column_6 as column_6_one', 'column_8 as column_8_one', 'column_9 as column_9_one',
                      'column_10 as column_10_one').withColumn("seq_id", monotonically_increasing_id())
    
    two1 = spark.read.csv("/home/data/8억건.csv", header=True, inferSchema=True, encoding='utf-8')
    two = two1.selectExpr('key_1210 as key_1210_two','sex_ctgo_cd as sex_ctgo_cd_two', 'cstmr_hom_scls_code as cstmr_hom_scls_code_two',
                      'job_cd as job_cd_two', 'indv_incm_amt as indv_incm_amt_two', 'seg_34 as seg_34_two',
                      'sale_date as sale_date_two', 'auth_time as auth_time_two', 'mer_tpbuz_no as mer_tpbuz_no_two',
                      'mer_rgn_scls_code as mer_rgn_scls_code_two', 'sale_amt as sale_amt_two', 
                      'column_6 as column_6_two', 'column_8 as column_8_two', 'column_9 as column_9_two',
                      'column_10 as column_10_two').withColumn("id", (rand() * 800000000).cast("long"))

    
    # Create aliases for the DataFrames
    a1_2000_alias = one.alias("a1")
    aaaaa1_2000_alias = two.alias("aaaaa1")    

    # Perform the inner join with aliases
    result = a1_2000_alias.join(aaaaa1_2000_alias, a1_2000_alias.seq_id == aaaaa1_2000_alias.id, how='inner')
  
    end = time.time()
    print("JOIN 성공,",f"경과 시간: {int(end - start)} 초")
    # result.show()
    
    end = time.time()
    

    result.cache()

    # result_id_values = result.select(col("seq_id")).orderBy(desc(col('seq_id'))).limit(10)
    
    end = time.time()
    print("필터링 성공,",f"경과 시간: {int(end - start)} 초")

    # Convert DataFrame to RDD
    result_rdd = result.rdd

    if result_rdd is not None:
       combined_df = rdd(result_rdd)

    
    
    end = time.time()
    print("파일 생성 성공,",f"경과 시간: {int(end - start)} 초")
    end = time.time()
    print(f"총 소요 시간: {int(end - start)} 초")







    