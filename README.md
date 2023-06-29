# Raw_Material_Project_Airflow
- 프로그래머스 데이터 엔지니어링 데브코스 3차 팀 프로젝트에서 사용할 Airflow 프로젝트

<BR>

## 전체 프로젝트 레포지토리
- https://github.com/KimChanw/Raw_Materials_Project

<BR>

## 사용하는 DAG
### 1. crude_oil_information.py
    - UTC 기준 매일 오후 12시에 실행
    - alphavantage api에서 날짜와 Brent유, WTI유 가격 데이터를 추출
    - 각 유가 상품 데이터는 join하여 별도 테이블로 저장
    - 실행마다 Full Refresh하여 저장 

<BR>

### 2. gold_silver_information.py
    - UTC 기준 월-금 새벽 4시에 실행
    - Nasdaq api에서 날짜와 금 / 은 가격 (달러 및 유로) 데이터를 추출
    - 실행마다 Full Refresh하여 저장 