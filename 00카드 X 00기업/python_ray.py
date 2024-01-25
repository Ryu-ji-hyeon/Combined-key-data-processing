import ray
import pandas as pd
import time

ray.init(object_store_memory=4 * 1024 * 1024* 1024)  # 4GB

@ray.remote
def read_csv(file_path):
        return pd.read_csv(file_path, header=0, encoding='utf-8', low_memory=True, usecols=["id"])

@ray.remote
def read_csv1(file_path):
     return pd.read_csv(file_path, header=0, encoding='latin-1', low_memory=True, usecols=["colNo","col1"])

@ray.remote
def merge_data(a1, aa):
    return pd.merge(a1, aa, left_on='id', right_on='colNo', how='inner')

if __name__ == "__main__":
    start = time.time()

    # 2천만건
    a_2000 = read_csv1.remote("/home/data/최종결과_1120_4.csv")
    # 1억건
    a1_2000 = read_csv.remote("/home/data/plus_id/part-00000-1fadca7f-492d-4d8a-a3ca-de7ac97b188e-c000.csv")


    # aa = pd.concat([a_2000] * 5, ignore_index=True)
    # a1_2000 = pd.concat([a1_2000] * 2, ignore_index=True)
    
    #  Use ray.put() to store the data in the object store
    a_2000_id = ray.put(ray.get(a_2000))
    a1_2000_id = ray.put(ray.get(a1_2000))

    # Inner Join
    result_A = merge_data.remote(a1_2000, a_2000)
    result_A = ray.get(result_A)
    
    # result_A의 인덱스 추출
    result_A_indices = result_A.index

    # a1_2000 데이터프레임 가져오기
    a1_2000_df = ray.get(a1_2000)

    # a1_2000 데이터프레임에서 result_A의 인덱스를 사용하여 해당 행들을 가져오기
    filtered_dataframe = a1_2000_df.loc[result_A_indices]

    # 결과 출력 (전체 데이터프레임)
    print(filtered_dataframe)

    row_count = len(filtered_dataframe)
    column_count = filtered_dataframe.shape[1]
    print(f"Row count: {row_count}")
    print(f"Column count: {column_count}")

    # 결과를 CSV 파일로 저장
    # output_path = "/home/data/plus_id"
    # result_A.to_csv(output_path, index=False)

    end = time.time()
    print(int(end - start), '초')

    ray.shutdown()
