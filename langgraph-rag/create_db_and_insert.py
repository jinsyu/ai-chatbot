import pandas as pd
import psycopg2
from psycopg2 import sql
from datetime import datetime
import os
from urllib.parse import urlparse
from dotenv import load_dotenv

# .env.local 파일 로드
load_dotenv('/Users/jinsyu/dev/ai-chatbot-baseone/.env.local')

# 데이터베이스 연결 정보
DATABASE_URL = os.environ.get('POSTGRES_URL')
if not DATABASE_URL:
    print("POSTGRES_URL 환경변수가 설정되지 않았습니다.")
    exit(1)

# URL 파싱
url = urlparse(DATABASE_URL)
db_params = {
    'host': url.hostname,
    'port': url.port,
    'database': url.path[1:],
    'user': url.username,
    'password': url.password
}

# 엑셀 파일 읽기
file_path = '/Users/jinsyu/Downloads/매출분석레포트_ZSDR0340.XLSX'
df = pd.read_excel(file_path, sheet_name=0)

# 테이블 생성 SQL (한글 컬럼명 사용)
create_table_sql = """
DROP TABLE IF EXISTS sales_analysis_report CASCADE;

CREATE TABLE sales_analysis_report (
    id SERIAL PRIMARY KEY,
    "판매처명" VARCHAR(255),
    "설계처명" VARCHAR(255),
    "자재명" VARCHAR(255),
    "자재그룹6명" VARCHAR(100),
    "자재그룹7명" VARCHAR(100),
    "청구금액" DECIMAL(15, 2),
    "청구일" DATE,
    "대금청구문서" VARCHAR(50),
    "대금청구품번" VARCHAR(50),
    "오더TYPE" VARCHAR(50),
    "매출오더문서" VARCHAR(50),
    "매출오더품번" VARCHAR(50),
    "영업형태" VARCHAR(50),
    "영업형태명" VARCHAR(100),
    "판매처그룹" VARCHAR(50),
    "판매처그룹명" VARCHAR(100),
    "판매처지역" VARCHAR(50),
    "판매처지역명" VARCHAR(100),
    "판매처" VARCHAR(50),
    "설계처그룹" VARCHAR(50),
    "설계처그룹명" VARCHAR(100),
    "설계처지역" VARCHAR(50),
    "설계처지역명" VARCHAR(100),
    "설계처" VARCHAR(50),
    "설계처검색어" VARCHAR(100),
    "자재" VARCHAR(100),
    "청구수량" DECIMAL(15, 3),
    "단위" VARCHAR(20),
    "판가" DECIMAL(15, 2),
    "매출할인" DECIMAL(15, 2),
    "본사마진" DECIMAL(15, 2),
    "설치수수료" DECIMAL(15, 2),
    "단위.1" VARCHAR(20),
    "자재그룹" VARCHAR(50),
    "자재그룹명" VARCHAR(100),
    "제품군" VARCHAR(50),
    "제품군명" VARCHAR(100),
    "자재유형" VARCHAR(50),
    "자재유형명" VARCHAR(100),
    "자재그룹1" VARCHAR(50),
    "자재그룹1명" VARCHAR(100),
    "자재그룹2" VARCHAR(50),
    "자재그룹2명" VARCHAR(100),
    "자재그룹3" VARCHAR(50),
    "자재그룹3명" VARCHAR(100),
    "자재그룹4" VARCHAR(50),
    "자재그룹4명" VARCHAR(100),
    "자재그룹5" VARCHAR(50),
    "자재그룹5명" VARCHAR(100),
    "자재그룹7" VARCHAR(50),
    "자재그룹6" VARCHAR(50),
    "자재그룹8" VARCHAR(50),
    "자재그룹8명" VARCHAR(100),
    "자재그룹9" VARCHAR(50),
    "자재그룹9명" VARCHAR(100),
    "자재그룹10" VARCHAR(50),
    "자재그룹10명" VARCHAR(100),
    "자재그룹11" VARCHAR(50),
    "자재그룹11명" VARCHAR(100),
    "통합코드" VARCHAR(50),
    "통합코드명" VARCHAR(100),
    "특성값" VARCHAR(100),
    "특성값명" VARCHAR(100),
    "설계모델" VARCHAR(100),
    "오더사유" VARCHAR(100),
    "오더사유내역" TEXT,
    "PO번호" VARCHAR(100),
    "PO일자" DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 인덱스 생성
CREATE INDEX idx_billing_date ON sales_analysis_report("청구일");
CREATE INDEX idx_customer_name ON sales_analysis_report("판매처명");
CREATE INDEX idx_designer_name ON sales_analysis_report("설계처명");
CREATE INDEX idx_material_code ON sales_analysis_report("자재");
CREATE INDEX idx_billing_document ON sales_analysis_report("대금청구문서");
CREATE INDEX idx_po_date ON sales_analysis_report("PO일자");
"""

# 데이터베이스 연결 및 테이블 생성
try:
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()
    
    print("데이터베이스에 연결되었습니다.")
    
    # 테이블 생성
    cur.execute(create_table_sql)
    conn.commit()
    print("테이블이 생성되었습니다.")
    
    # 데이터 삽입
    print(f"총 {len(df)}개의 레코드를 삽입합니다...")
    
    # NaN 값을 None으로 변환 (datetime 컬럼 특별 처리)
    for col in df.columns:
        if df[col].dtype == 'datetime64[ns]':
            df[col] = df[col].where(pd.notnull(df[col]), None)
        else:
            df[col] = df[col].where(pd.notnull(df[col]), None)
    
    # 데이터 삽입 SQL 준비
    columns = df.columns.tolist()
    insert_sql = sql.SQL("INSERT INTO sales_analysis_report ({}) VALUES ({})").format(
        sql.SQL(', ').join([sql.Identifier(col) for col in columns]),
        sql.SQL(', ').join(sql.Placeholder() * len(columns))
    )
    
    # 배치 삽입
    batch_size = 100
    for i in range(0, len(df), batch_size):
        batch = df.iloc[i:i+batch_size]
        
        for _, row in batch.iterrows():
            # NaT 및 nan 값을 None으로 변환
            values = []
            for val in row.values:
                if pd.isna(val):
                    values.append(None)
                else:
                    values.append(val)
            
            try:
                cur.execute(insert_sql, tuple(values))
            except Exception as e:
                print(f"레코드 삽입 오류 (행 {i}): {e}")
                print(f"문제가 있는 데이터: {values[:5]}...")  # 처음 5개 컬럼만 출력
                continue
        
        conn.commit()
        print(f"  {min(i+batch_size, len(df))} / {len(df)} 레코드 삽입 완료")
    
    print("모든 데이터가 성공적으로 삽입되었습니다.")
    
    # 데이터 검증
    cur.execute("SELECT COUNT(*) FROM sales_analysis_report")
    count = cur.fetchone()[0]
    print(f"\n검증: 테이블에 {count}개의 레코드가 있습니다.")
    
    # 샘플 데이터 확인
    cur.execute("""
        SELECT "판매처명", "자재명", "청구금액", "청구일" 
        FROM sales_analysis_report 
        LIMIT 5
    """)
    samples = cur.fetchall()
    print("\n샘플 데이터:")
    for sample in samples:
        print(f"  {sample}")
    
    # 요약 통계
    cur.execute("""
        SELECT 
            COUNT(*) as 총_레코드수,
            SUM("청구금액") as 총_청구금액,
            AVG("청구금액") as 평균_청구금액,
            MIN("청구일") as 최초_청구일,
            MAX("청구일") as 최종_청구일
        FROM sales_analysis_report
    """)
    stats = cur.fetchone()
    print(f"\n=== 요약 통계 ===")
    print(f"총 레코드수: {stats[0]:,}")
    print(f"총 청구금액: {stats[1]:,.0f}원")
    print(f"평균 청구금액: {stats[2]:,.0f}원")
    print(f"최초 청구일: {stats[3]}")
    print(f"최종 청구일: {stats[4]}")
    
    cur.close()
    conn.close()
    print("\n작업이 완료되었습니다.")
    
except Exception as e:
    print(f"오류 발생: {e}")
    if 'conn' in locals():
        conn.rollback()
        conn.close()