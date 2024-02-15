-- 회원의 거주지 분포 현황
SELECT 
 col4,count(col4)
FROM pamaster_v2_data.nettier
group by col4 
ORDER by count(col4) desc;

-- 회원등급별 공연 관람 빈도
SELECT 
 col13,sum(col14)오페라, sum(col15) 클래식,sum(col16) 전시회
FROM pamaster_v2_data.nettier
group by col13

-- 성별 공연 관람 빈도
SELECT 
 col3,sum(col14)오페라, sum(col15) 클래식,sum(col16) 전시회
FROM pamaster_v2_data.nettier
group by col3

-- 연령대별 공연 관람 빈도
SELECT 
 col2,sum(col14)오페라, sum(col15) 클래식,sum(col16) 전시회
FROM pamaster_v2_data.nettier
group by col2

-- 자택주소지별 공연 관람 빈도 			
SELECT 
 col4,sum(col14)오페라, sum(col15) 클래식,sum(col16) 전시회
FROM pamaster_v2_data.nettier
group by col4

-- 직업별 공연 관람 빈도			
SELECT 
 col5,sum(col14)오페라, sum(col15) 클래식,sum(col16) 전시회
FROM pamaster_v2_data.nettier
group by col5

-- 소득 분위별 공연 관람 빈도
SELECT 
 col6,sum(col14)오페라, sum(col15) 클래식,sum(col16) 전시회
FROM pamaster_v2_data.nettier
group by col6

-- 고객분류별 공연관람 빈도 			
SELECT 
 col7,sum(col14)오페라, sum(col15) 클래식,sum(col16) 전시회
FROM pamaster_v2_data.nettier
group by col7

--  월별 카드 사용 금액 	
SELECT 
 col8 월별,sum(col12) 사용금액
FROM pamaster_v2_data.nettier
group by col8

--  시간대별 카드 사용 금액 	
SELECT 
 col9 시간대별,sum(col12) 사용금액
FROM pamaster_v2_data.nettier
group by col9

--  업종별 사용 금액 	
SELECT 
 col10 업종분류,sum(col12) 사용금액
FROM pamaster_v2_data.nettier
group by col10

--  가맹점 소재지별 카드 사용 금액 		
SELECT 
 col20 소재지,sum(col12) 사용금액
FROM pamaster_v2_data.nettier
group by col20

--  회원등급별 + 업종별 소비 금액 						
SELECT 
 col13,col10 업종분류,sum(col12) 사용금액
FROM pamaster_v2_data.nettier
group by col13,col10

--  성별 + 업종별 소비금액 		
SELECT 
 col3,col10 업종분류,sum(col12) 사용금액
FROM pamaster_v2_data.nettier
group by col3,col10

--  연령대별 + 업종별 소비금액 														
SELECT 
 col2,col10 업종분류,sum(col12) 사용금액
FROM pamaster_v2_data.nettier
group by col2,col10

--  자택주소지별 + 업종별 소비금액 																														
SELECT 
 col4,col10 업종분류,sum(col12) 사용금액
FROM pamaster_v2_data.nettier
group by col4,col10

--  직업별 + 업종별 소비금액 				
SELECT 
 col5,col10 업종분류,sum(col12) 사용금액
FROM pamaster_v2_data.nettier
group by col5,col10

--  소득 분위별 + 업종별 소비금액 												
SELECT 
 col6,col10 업종분류,sum(col12) 사용금액
FROM pamaster_v2_data.nettier
group by col6,col10

--  고객분류별 + 업종별 소비금액 																																			
SELECT 
 col7,col10 업종분류,sum(col12) 사용금액
FROM pamaster_v2_data.nettier
group by col7,col10

--  공연종류별 + 업종별 소비금액 			
SELECT
    col10 AS 업종,
    SUM(CASE WHEN col14 != 0 THEN col12 ELSE 0 END) AS 오페라소비금액,
    SUM(CASE WHEN col15 != 0 THEN col12 ELSE 0 END) AS 클래식소비금액,
    SUM(CASE WHEN col16 != 0 THEN col12 ELSE 0 END) AS 전시회소비금액
FROM
    pamaster_v2_data.nettier
GROUP BY
    col10;


-- 테이블 데이터 변경
START TRANSACTION;
    savepoint a1;

    UPDATE pamaster_v2_data.nettier
    SET col8 = SUBSTRING(col8, 1, 6)

    savepoint a2;

    UPDATE pamaster_v2_data.nettier
    SET col10 = '인삼제품'
    WHERE col10 ='8402'

    savepoint a3;

    ALTER TABLE pamaster_v2_data.nettier
    ADD COLUMN col19 text;

    UPDATE pamaster_v2_data.nettier
    SET col19 = SUBSTRING(col11, 1, 4);

    savepoint a4;

    ALTER TABLE pamaster_v2_data.nettier
    ADD COLUMN col20 text;

    UPDATE pamaster_v2_data.nettier
    SET col20 = '양평군'
    where col19 ='4183'

    savepoint a5;

    ALTER TABLE pamaster_v2_data.nettier
    ADD COLUMN col17 text;

    UPDATE pamaster_v2_data.nettier
    SET col17 = SUBSTRING(col4, 1, 2);

    ALTER TABLE pamaster_v2_data.nettier
    ADD COLUMN col18 text;

    UPDATE pamaster_v2_data.nettier
    SET col18 = '강원도'
    where col17 ='51'

    savepoint a6;

    UPDATE pamaster_v2_data.nettier
    SET col4 = '경산시'
    where col4 ='4729'

commit;
rollback to a1;

-- col21 추가 및 데이터 삽입
ALTER TABLE pamaster_v2_data.nettier
ADD COLUMN col21 text;

UPDATE pamaster_v2_data.nettier
SET 
CASE WHEN SUBSTRING(col11, 1, 2) = 41 THEN col21 = '경기도' ELSE col11 
CASE WHEN SUBSTRING(col11, 1, 2) = 11 THEN col21 = '서울특별시' ELSE col11 
END  


-- 회원의 거주지 분포 현황(광역시)
SELECT 
 col18,count (col18) 
from pamaster_v2_data.nettier
group by col18

-- 자택주소지별 공연 관람 빈도(광역시)
SELECT 
 col18,sum(col14)오페라, sum(col15) 클래식,sum(col16) 전시회
FROM pamaster_v2_data.nettier
group by col18

-- 가맹점 소재지별 카드 사용 금액(광역시)
SELECT 
 col21 소재지,sum(col12) 사용금액
FROM pamaster_v2_data.nettier
group by col21

-- 자택주소지별 + 업종별 소비금액(광역시)
SELECT 
 col10,col18 업종분류,sum(col12) 사용금액
FROM pamaster_v2_data.nettier
group by col10,col18

-- 소득 분위별 회원 등급 현황 
SELECT 
 col6 소득분위,col13 등급,count (col6)
FROM pamaster_v2_data.nettier
group by col6,col13

--import csv
LOAD DATA LOCAL INFILE '/home/data/nettier.csv'
INTO TABLE pamaster_v2_data.nettier
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

--export csv
select 
* 
from pamaster_v2_data.nettier 
into outfile '/home/data/nettier.csv' fields terminated by ',';
