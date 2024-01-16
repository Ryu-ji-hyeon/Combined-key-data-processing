from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.window import Window 

spark = SparkSession.builder.appName('missing').config("spark.executor.memory", "4g").getOrCreate()

def main():
    
    df_spark = spark.read.csv('00카드 X 00기업\BC_예술의전당 범주형데이터.csv', header=True, inferSchema=True)

    # 테이블의 스키마를 보여주는 함수
    df_spark.printSchema()
    
    # 테이블에서 행을 가져오는 함수
    print(df_spark.collect())	
    
    # 테이블 결과를 보여주는 함수
    df_spark.show()
    
    # 서머리 결과를 보여주는 함수
    df_spark.describe()
    
    # filter( |, &, isin, ~isin, alias)
    df_spark.filter((col("col3")=='2') | (col('col5')=='3')).select(col('col6').alias('| 연산자')).show()
    df_spark.filter((col("col3")=='2') & (col('col5')=='3')).select(col('col6').alias('& 연산자')).show()
    df_spark.filter(col('col5').isin(['1','2'])).select(col('col6').alias('isin')).show()
    df_spark.filter(~col('col5').isin(['1','2'])).select(col('col6').alias('~isin')).show()
    
    # Like
    df_spark.filter(col('col5').like('%2%')).select(col('col6').alias('Like')).show()
    df_spark.filter(col('col5').startswith('A')).select(col('col6').alias('startswith')).show()
    df_spark.filter(col('col5').startswith('1')).select(col('col6').alias('endwiths')).show()
    df_spark.filter(col('col5').contains('1')).select(col('col6').alias('contains')).show()
    
    # groupBy, agg
    df_spark.groupBy("col5").count().alias('count').show()
    df_spark.groupBy("col5").sum("데이터건수_col5").alias('sum').show()
    df_spark.groupBy("col5").min("데이터건수_col5").alias('min').show() 
    df_spark.groupBy("col5").max("데이터건수_col5").alias('max').show() 
    df_spark.groupBy("col5").avg("데이터건수_col5").alias('avg').show()
    (df_spark.groupBy("col5")
    .agg(count("데이터건수_col5").alias("count_agg")
        , avg("데이터건수_col5").alias("avg_agg")
        , sum("데이터건수_col5").alias("sum_agg")
        , max("데이터건수_col5").alias("max_agg"))).show()
    
    # orderBy
    df_spark.groupBy("col5").avg("데이터건수_col5").orderBy(desc("col5")).alias('orderBy(desc)').show()
    df_spark.groupBy("col5").avg("데이터건수_col5").orderBy(asc("col5")).alias('orderBy(asc)').show()
    
    # join (INNER, FULL, LEFT, RIGHT)
    df_spark.join(df_spark, df_spark.col3 == df_spark.col5, how = 'inner').show()
    df_spark.join(df_spark, df_spark.col3 == df_spark.col5, how = "full").show()
    df_spark.join(df_spark, df_spark.col3 == df_spark.col5, how = "left").show()
    df_spark.join(df_spark, df_spark.col3 == df_spark.col5, how = "right").show()
    
    # LEFT anti join, RIGHT anti join ( A LEFT anti join B = A - (A X B), A RIGHT anti join B = B - (B X A) )
    df_spark.join(df_spark, df_spark.col3 == df_spark.col5, how = 'left_anti').show()
    df_spark.join(df_spark, df_spark.col3 == df_spark.col5, how = "right_anti").show()
    
    # union (unionByName 사용 시 컬럼 순서 조정할 필요 x)
    df_spark.union(df_spark).show()
    df_spark.unionByName(df_spark).show()
    
    # pivot (picot은 long table -> wide table, unpivot은 wide table -> long table)
    pivotDF= df_spark.groupBy("col5").pivot("col3").alias('pivot').show()
    unPivotDF = (pivotDF
             .select("col5", expr(df_spark))
             .filter(col("col3").isNotNull())
            ).show()
    
    # row_number
    df_spark.withColumn('col5'
    , row_number().over(Window.partitionBy('col5').orderBy(desc('col5')))).show()




if __name__ == "__main__":
    main()
