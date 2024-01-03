from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, DoubleType
from pyspark.sql import Row
import numpy as np
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.serializers import ArrowSerializer, PickleSerializer

# SparkSession 생성
spark = SparkSession.builder.appName("GenerateData").getOrCreate()

# 데이터 프레임을 생성할 갯수 (행의 수는 5천만으로, 열의 수는 10으로 줄임)
num_rows = 50000000
num_columns = 10  # 열의 수를 10개로 변경

# 데이터 스키마 정의
schema = StructType([
    StructField("id", LongType(), nullable=False),
    StructField("seq_id", LongType(), nullable=False),
])

# 열의 수를 10개로 줄임
for i in range(1, num_columns + 1):
    schema.add(StructField(f'col{i}', DoubleType(), nullable=True))

# id는 랜덤으로 겹치지 않는 수로, seq_id는 1부터 연속적인 수로 생성
sample_data = {
    'id': list(range(1, num_rows + 1)),
    'seq_id': list(range(1, num_rows + 1)),
}

# col1~col10은 rand 함수를 사용하여 생성
for i in range(1, num_columns + 1):
    sample_data[f'col{i}'] = list(np.random.random(size=num_rows))

# RDD를 사용하여 데이터프레임 생성
rdd = spark.sparkContext.parallelize([Row(**sample_data) for _ in range(1)], numSlices=4)

# 직렬화 방법을 Arrow로 변경
spark.sparkContext._conf.set("spark.serializer", ArrowSerializer().getName())
spark.sparkContext._conf.set("spark.executor.pyspark.serializer", ArrowSerializer().getName())
spark.sparkContext._conf.set("spark.driver.pyspark.serializer", ArrowSerializer().getName())

# 예전 코드와 같이 monotonically_increasing_id를 사용하여 id 컬럼 추가
df = spark.createDataFrame(rdd, schema=schema)
df = df.withColumn("id", monotonically_increasing_id())

# 데이터를 4개의 파티션으로 나눔 (적절한 숫자로 조절 가능)
df = df.repartition(4)

# CSV 파일로 저장
csv_file_path = 'generated_data_pyspark.csv'
df.write.csv(csv_file_path, header=True, mode="overwrite")

# 결과 확인
print(f"CSV 파일이 성공적으로 생성되었습니다. 경로: {csv_file_path}")
 