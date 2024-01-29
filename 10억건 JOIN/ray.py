import pandas as pd
import time
import ray
import psutil

# 총 사용 가능한 메모리를 가져옵니다 (GB)
total_memory_gb = psutil.virtual_memory().total / (1024 ** 3)

# 사용 가능한 메모리에 따라 값 설정 (바이트 단위
ray.init()  
# 1억건
@ray.remote
def read_csv(file_path):
        return pd.read_csv(file_path, header=0, encoding='utf-8', low_memory=True, usecols=["id"])
    
# 2천만건
@ray.remote
def read_csv1(file_path):
     return pd.read_csv(file_path, header=0, encoding='latin-1', low_memory=True, usecols=["colNo"])

@ray.remote
def merge_data(a1, aa):
    return pd.merge(a1, aa, left_on='id', right_on='colNo', how='inner')

def data_join():
    start = time.time()

    # # 2천만건
    # a_2000 = read_csv1.remote("data/2천만건_컬럼 21.csv")
    # 1억건
    a1_2000 = read_csv.remote("data/plus_id/1억건_id.csv")

    # 1억건
    a_2000 = read_csv1.remote("data/plus_id/1억건_id.csv")

    # aa = pd.concat([a_2000] * 5, ignore_index=True)
    # a1_2000 = pd.concat([a1_2000] * 2, ignore_index=True)
    
   #  Use ray.put() to store the data in the object store
    a_2000_id = ray.put(ray.get(a_2000))
    a1_2000_id = ray.put(ray.get(a1_2000))

    # Inner Join
    result_id = merge_data.remote(a1_2000_id, a_2000_id)
    
    # Retrieve the joined dataframe
    result = ray.get(result_id)
    

    # 결과 출력 (전체 데이터프레임)
    print(result)

    # row_count = len(result_A)
    # column_count = result_A.shape[1]
    # print(f"Row count: {row_count}")
    # print(f"Column count: {column_count}")

    # 결과를 CSV 파일로 저장
    # output_path = "/home/data/plus_id"
    # result_A.to_csv(output_path, index=False)

    end = time.time()
    print(int(end - start), '초')

    ray.shutdown()
    return result
    

