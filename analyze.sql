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