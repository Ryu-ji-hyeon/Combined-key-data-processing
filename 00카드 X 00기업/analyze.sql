-- 회원의 거주지 분포 현황
SELECT 
 col4,count(col4)
FROM pamaster_v2_data.table
group by col4 
ORDER by count(col4) desc;

-- 회원등급별 공연 관람 빈도
SELECT 
 col13,sum(col14)오페라, sum(col15) 클래식,sum(col16) 전시회
FROM pamaster_v2_data.table
group by col13

-- 성별 공연 관람 빈도
SELECT 
 col3,sum(col14)오페라, sum(col15) 클래식,sum(col16) 전시회
FROM pamaster_v2_data.table
group by col3

-- 연령대별 공연 관람 빈도
SELECT 
 col2,sum(col14)오페라, sum(col15) 클래식,sum(col16) 전시회
FROM pamaster_v2_data.table
group by col2

-- 자택주소지별 공연 관람 빈도 			
SELECT 
 col4,sum(col14)오페라, sum(col15) 클래식,sum(col16) 전시회
FROM pamaster_v2_data.table
group by col4

-- 직업별 공연 관람 빈도			
SELECT 
 col5,sum(col14)오페라, sum(col15) 클래식,sum(col16) 전시회
FROM pamaster_v2_data.table
group by col5

-- 소득 분위별 공연 관람 빈도
SELECT 
 col6,sum(col14)오페라, sum(col15) 클래식,sum(col16) 전시회
FROM pamaster_v2_data.table
group by col6

-- 고객분류별 공연관람 빈도 			
SELECT 
 col7,sum(col14)오페라, sum(col15) 클래식,sum(col16) 전시회
FROM pamaster_v2_data.table
group by col7

--  월별 카드 사용 금액 	
SELECT 
 col8 월별,sum(col12) 사용금액
FROM pamaster_v2_data.table
group by col8

--  시간대별 카드 사용 금액 	
SELECT 
 col9 시간대별,sum(col12) 사용금액
FROM pamaster_v2_data.table
group by col9

--  업종별 사용 금액 	
SELECT 
 col10 업종분류,sum(col12) 사용금액
FROM pamaster_v2_data.table
group by col10

--  가맹점 소재지별 카드 사용 금액 		
SELECT 
 col20 소재지,sum(col12) 사용금액
FROM pamaster_v2_data.table
group by col20

--  회원등급별 + 업종별 소비 금액 						
SELECT 
 col13,col10 업종분류,sum(col12) 사용금액
FROM pamaster_v2_data.table
group by col13,col10

--  성별 + 업종별 소비금액 		
SELECT 
 col3,col10 업종분류,sum(col12) 사용금액
FROM pamaster_v2_data.table
group by col3,col10

--  연령대별 + 업종별 소비금액 														
SELECT 
 col2,col10 업종분류,sum(col12) 사용금액
FROM pamaster_v2_data.table
group by col2,col10

--  자택주소지별 + 업종별 소비금액 																														
SELECT 
 col4,col10 업종분류,sum(col12) 사용금액
FROM pamaster_v2_data.table
group by col4,col10

--  직업별 + 업종별 소비금액 				
SELECT 
 col5,col10 업종분류,sum(col12) 사용금액
FROM pamaster_v2_data.table
group by col5,col10

--  소득 분위별 + 업종별 소비금액 												
SELECT 
 col6,col10 업종분류,sum(col12) 사용금액
FROM pamaster_v2_data.table
group by col6,col10

--  고객분류별 + 업종별 소비금액 																																			
SELECT 
 col7,col10 업종분류,sum(col12) 사용금액
FROM pamaster_v2_data.table
group by col7,col10

--  공연종류별 + 업종별 소비금액 			
SELECT
    col10 AS 업종,
    SUM(CASE WHEN col14 != 0 THEN col12 ELSE 0 END) AS 오페라소비금액,
    SUM(CASE WHEN col15 != 0 THEN col12 ELSE 0 END) AS 클래식소비금액,
    SUM(CASE WHEN col16 != 0 THEN col12 ELSE 0 END) AS 전시회소비금액
FROM
    pamaster_v2_data.table
GROUP BY
    col10;


-- 테이블 데이터 변경
START TRANSACTION;
    savepoint a1;

    UPDATE pamaster_v2_data.table
    SET col8 = SUBSTRING(col8, 1, 6)

    savepoint a2;

    UPDATE pamaster_v2_data.table
    SET col10 = '인삼제품'
    WHERE col10 ='8402'

    savepoint a3;

    ALTER TABLE pamaster_v2_data.table
    ADD COLUMN col19 text;

    UPDATE pamaster_v2_data.table
    SET col19 = SUBSTRING(col11, 1, 4);

    savepoint a4;

    ALTER TABLE pamaster_v2_data.table
    ADD COLUMN col20 text;

    UPDATE pamaster_v2_data.table
    SET col20 = '양평군'
    where col19 ='4183'

    savepoint a5;

    ALTER TABLE pamaster_v2_data.table
    ADD COLUMN col17 text;

    UPDATE pamaster_v2_data.table
    SET col17 = SUBSTRING(col4, 1, 2);

    ALTER TABLE pamaster_v2_data.table
    ADD COLUMN col18 text;

    UPDATE pamaster_v2_data.table
    SET col18 = '강원도'
    where col17 ='51'

    savepoint a6;

    UPDATE pamaster_v2_data.table
    SET col4 = '경산시'
    where col4 ='4729'

commit;
rollback to a1;

-- col21 추가 및 데이터 삽입
ALTER TABLE pamaster_v2_data.table
ADD COLUMN col21 text;

UPDATE pamaster_v2_data.table
SET 
CASE WHEN SUBSTRING(col11, 1, 2) = 41 THEN col21 = '경기도' ELSE col11 
CASE WHEN SUBSTRING(col11, 1, 2) = 11 THEN col21 = '서울특별시' ELSE col11 
END  


-- 회원의 거주지 분포 현황(광역시)
SELECT 
 col18,count (col18) 
from pamaster_v2_data.table
group by col18

-- 자택주소지별 공연 관람 빈도(광역시)
SELECT 
 col18,sum(col14)오페라, sum(col15) 클래식,sum(col16) 전시회
FROM pamaster_v2_data.table
group by col18

-- 가맹점 소재지별 카드 사용 금액(광역시)
SELECT 
 col21 소재지,sum(col12) 사용금액
FROM pamaster_v2_data.table
group by col21

-- 자택주소지별 + 업종별 소비금액(광역시)
SELECT 
 col10,col18 업종분류,sum(col12) 사용금액
FROM pamaster_v2_data.table
group by col10,col18

-- 소득 분위별 회원 등급 현황 
SELECT 
 col6 소득분위,col13 등급,count (col6)
FROM pamaster_v2_data.table
group by col6,col13

-- 성별 공연 관람 빈도 중복제거 
SELECT COUNT(*) 
FROM (
    SELECT COUNT(col1) AS count_col1
    FROM pamaster_v2_data.table
    WHERE col3 = '1' AND col16 <> ''
    GROUP BY col1   
) AS sub;

-- 연령대별 공연 관람 빈도 중복제거
SELECT col2, COUNT(*) 
FROM (
    SELECT col2, COUNT(col2) AS count_col1
    FROM pamaster_v2_data.table
    WHERE col16 <> ''
    GROUP BY col1 
) AS sub
GROUP BY col2;

-- 회원등급별 관람 빈도 중복제거
SELECT col13, COUNT(*) 
FROM (
    SELECT col13, COUNT(col13) AS count_col1
    FROM pamaster_v2_data.table
    WHERE col14 <> ''
    GROUP BY col1 
) AS sub
GROUP BY col13;

-- 직업별 관람 빈도 중복제거
SELECT col5, COUNT(*) 
FROM (
    SELECT col5, COUNT(col5) AS count_col1
    FROM pamaster_v2_data.table
    WHERE col16 <> ''
    GROUP BY col1 
) AS sub
GROUP BY col5;

-- 소득분위별 관람 빈도 중복제거
SELECT col6, COUNT(*) 
FROM (
    SELECT col6, COUNT(col6) AS count_col1
    FROM pamaster_v2_data.table
    WHERE col14 <> ''
    GROUP BY col1 
) AS sub
GROUP BY col6;

-- 고객분위별 관람 빈도 중복제거
SELECT col7, COUNT(*) 
FROM (
    SELECT col7, COUNT(col7) AS count_col1
    FROM pamaster_v2_data.table
    WHERE col16 <> ''
    GROUP BY col1 
) AS sub
GROUP BY col7;

-- 자택주소지별 관람 빈도 중복제거
SELECT col4, COUNT(*) 
FROM (
    SELECT col4, COUNT(col4) AS count_col1
    FROM pamaster_v2_data.table
    WHERE col14 <> ''
    GROUP BY col1 
) AS sub
GROUP BY col4;

-- 자택주소지별(광역시) 관람 빈도 중복제거
SELECT col18, COUNT(*) 
FROM (
    SELECT col18, COUNT(col18) AS count_col1
    FROM pamaster_v2_data.table
    WHERE col16 <> ''
    GROUP BY col1 
) AS sub
GROUP BY col18;

-- 소득 분위별 회원등급 현황 중복제거
SELECT col6, COUNT(*) 
FROM (
    SELECT col6, COUNT(col6) AS count_col1
    FROM pamaster_v2_data.table
    WHERE col13='싹틔우미'
    GROUP BY col1 
) AS sub
GROUP BY col6;

-- 거주지별 카드 사용빈도 중복제거
SELECT col18, COUNT(*) 
FROM (
    SELECT col18, COUNT(col4) AS count_col1
    FROM pamaster_v2_data.table
    GROUP BY col1 
) AS sub
GROUP BY col18;

--import csv
LOAD DATA LOCAL INFILE '/home/data/nettier.csv'
INTO TABLE pamaster_v2_data.nettier
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

--export csv
SELECT 
 * 
FROM pamaster_v2_data.nettier 
INTO OUTFILE '/home/data/nettier.csv' FIELDS TERMINATED BY ',';

-- 회원등급 정수로 치환
ALTER TABLE pamaster_v2_data.table
ADD COLUMN col22 int;

UPDATE pamaster_v2_data.table
SET col22 = 
    CASE 
        WHEN col13 ='무료' THEN 1 
        WHEN col13 ='싹틔우미' THEN 2 
        WHEN col13 ='그린' THEN 3 
        WHEN col13 ='블루' THEN 4
        WHEN col13 ='골드' THEN 5 
        WHEN col13 ='노블' THEN 6
        ELSE 0
    END;


-- 소득분위 정수로 치환
ALTER TABLE pamaster_v2_data.table
ADD COLUMN col23 int;

UPDATE pamaster_v2_data.table
SET col23 = 
    CASE 
        WHEN col6 ='0' THEN 0 
        WHEN col6 ='1000' THEN 1000 
        WHEN col6 ='2000' THEN 2000 
        WHEN col6 ='3000' THEN 3000
        WHEN col6 ='4000' THEN 4000 
        WHEN col6 ='5000' THEN 5000 
        WHEN col6 ='6000' THEN 6000 
        WHEN col6 ='7000' THEN 7000 
        WHEN col6 ='8000' THEN 8000 
        WHEN col6 ='9000' THEN 9000 
        WHEN col6 ='10000' THEN 10000 
        ELSE col23 
    END;

-- 회원등급과 소득분위의 상관관계 계수
SELECT 
    CORR (col22,col23)
FROM 
    pamaster_v2_data.table


