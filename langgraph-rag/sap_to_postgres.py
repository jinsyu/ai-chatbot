#!/usr/bin/env python3
"""
SAP 데이터를 PostgreSQL로 import하는 스크립트
"""

import os
import sys
import pandas as pd
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
from datetime import datetime

# 환경변수 설정
os.environ['SAPNWRFC_HOME'] = os.path.expanduser('~/sap/nwrfcsdk')
os.environ['DYLD_LIBRARY_PATH'] = f"{os.environ['SAPNWRFC_HOME']}/lib:{os.environ.get('DYLD_LIBRARY_PATH', '')}"

try:
    from pyrfc import Connection
    print("✅ pyrfc 모듈 로드 성공")
except ImportError as e:
    print(f"❌ pyrfc import 실패: {e}")
    sys.exit(1)

# 환경변수 로드
load_dotenv()
load_dotenv('.env.sap')  # SAP 연결 정보

# 데이터베이스 연결 정보
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://baseone:baseone@localhost:10003/baseone')

# SAP 연결 정보
SAP_CONFIG = {
    'ashost': os.getenv('SAP_ASHOST', 'YOUR_SAP_HOST'),
    'sysnr': os.getenv('SAP_SYSNR', '00'),
    'client': os.getenv('SAP_CLIENT', '100'),
    'user': os.getenv('SAP_USER', 'YOUR_USERNAME'),
    'passwd': os.getenv('SAP_PASSWORD', 'YOUR_PASSWORD'),
    'lang': os.getenv('SAP_LANG', 'KO'),
}

def read_sap_table(conn, table_name, fields=None, where_clause=None, max_rows=0):
    """
    SAP 테이블 데이터 읽기
    
    Args:
        conn: SAP 연결 객체
        table_name: 읽을 테이블명 (예: 'VBAK' - 판매 문서 헤더)
        fields: 읽을 필드 리스트 (None이면 모든 필드)
        where_clause: WHERE 조건 (SAP 형식)
        max_rows: 최대 읽을 행 수 (0이면 전체)
    """
    try:
        # 필드 정보 가져오기
        if fields is None:
            # 테이블의 모든 필드 정보 조회
            fields_info = conn.call('RFC_READ_TABLE',
                                   QUERY_TABLE=table_name,
                                   NO_DATA='X')  # 데이터는 가져오지 않고 필드 정보만
            fields = [{'FIELDNAME': f['FIELDNAME']} for f in fields_info['FIELDS'][:10]]  # 처음 10개 필드만
        else:
            fields = [{'FIELDNAME': f} for f in fields]
        
        # WHERE 조건 설정
        options = []
        if where_clause:
            # WHERE 조건을 72자씩 나누어 전달 (SAP 제한)
            for i in range(0, len(where_clause), 72):
                options.append({'TEXT': where_clause[i:i+72]})
        
        # 데이터 읽기
        result = conn.call('RFC_READ_TABLE',
                          QUERY_TABLE=table_name,
                          DELIMITER='|',
                          FIELDS=fields,
                          OPTIONS=options,
                          ROWCOUNT=max_rows if max_rows > 0 else 0)
        
        # 필드명 추출
        field_names = [f['FIELDNAME'] for f in result['FIELDS']]
        
        # 데이터 파싱
        data = []
        for row in result['DATA']:
            values = row['WA'].split('|')
            # 값 정리 (공백 제거)
            values = [v.strip() for v in values]
            data.append(dict(zip(field_names, values)))
        
        return pd.DataFrame(data)
    
    except Exception as e:
        print(f"❌ SAP 테이블 읽기 오류: {e}")
        return None

def create_sales_table(conn):
    """판매 데이터 테이블 생성"""
    cursor = conn.cursor()
    
    # 기존 테이블 삭제
    cursor.execute("DROP TABLE IF EXISTS sap_sales_data CASCADE")
    
    # 테이블 생성
    create_table_query = """
    CREATE TABLE sap_sales_data (
        id SERIAL PRIMARY KEY,
        "판매문서번호" VARCHAR(10),
        "판매문서유형" VARCHAR(4),
        "판매조직" VARCHAR(4),
        "유통채널" VARCHAR(2),
        "제품군" VARCHAR(2),
        "고객번호" VARCHAR(10),
        "고객명" TEXT,
        "자재번호" VARCHAR(18),
        "자재명" TEXT,
        "수량" NUMERIC(15, 3),
        "단위" VARCHAR(3),
        "금액" NUMERIC(20, 2),
        "통화" VARCHAR(5),
        "생성일자" DATE,
        "변경일자" DATE,
        "납품일자" DATE,
        "상태" VARCHAR(20),
        "비고" TEXT,
        "생성시간" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """
    
    cursor.execute(create_table_query)
    
    # 인덱스 생성
    cursor.execute('CREATE INDEX idx_sap_sales_doc ON sap_sales_data("판매문서번호")')
    cursor.execute('CREATE INDEX idx_sap_sales_customer ON sap_sales_data("고객번호")')
    cursor.execute('CREATE INDEX idx_sap_sales_material ON sap_sales_data("자재번호")')
    cursor.execute('CREATE INDEX idx_sap_sales_date ON sap_sales_data("생성일자")')
    
    conn.commit()
    print("✅ sap_sales_data 테이블 생성 완료")

def import_sales_from_sap():
    """SAP에서 판매 데이터 가져와서 PostgreSQL에 저장"""
    
    print("\n🔗 SAP 연결 중...")
    try:
        sap_conn = Connection(**SAP_CONFIG)
        print("✅ SAP 연결 성공")
    except Exception as e:
        print(f"❌ SAP 연결 실패: {e}")
        print("SAP 연결 정보를 확인하세요 (.env.sap 파일)")
        return
    
    print("\n🔗 PostgreSQL 연결 중...")
    try:
        pg_conn = psycopg2.connect(DATABASE_URL)
        print("✅ PostgreSQL 연결 성공")
    except Exception as e:
        print(f"❌ PostgreSQL 연결 실패: {e}")
        sap_conn.close()
        return
    
    try:
        # 1. 판매 헤더 데이터 읽기 (VBAK 테이블)
        print("\n📖 SAP 판매 데이터 읽기 중...")
        
        # 예제: 최근 30일 데이터만 가져오기
        # SAP 날짜 형식: YYYYMMDD
        from datetime import timedelta
        today = datetime.now()
        date_from = (today - timedelta(days=30)).strftime('%Y%m%d')
        date_to = today.strftime('%Y%m%d')
        
        where_clause = f"ERDAT >= '{date_from}' AND ERDAT <= '{date_to}'"
        
        # 판매 문서 헤더 읽기
        df_header = read_sap_table(
            sap_conn,
            'VBAK',  # 판매 문서 헤더
            fields=['VBELN', 'AUART', 'VKORG', 'VTWEG', 'SPART', 'KUNNR', 'ERDAT', 'ERZET'],
            where_clause=where_clause,
            max_rows=1000  # 테스트를 위해 1000개만
        )
        
        if df_header is not None and not df_header.empty:
            print(f"✅ {len(df_header)}개 판매 문서 조회 완료")
            
            # 2. PostgreSQL 테이블 생성
            create_sales_table(pg_conn)
            
            # 3. 데이터 변환 및 저장
            cursor = pg_conn.cursor()
            inserted_count = 0
            
            for _, row in df_header.iterrows():
                try:
                    # 데이터 변환
                    insert_query = """
                    INSERT INTO sap_sales_data (
                        "판매문서번호", "판매문서유형", "판매조직", "유통채널", 
                        "제품군", "고객번호", "생성일자"
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """
                    
                    # 날짜 변환 (YYYYMMDD -> DATE)
                    create_date = None
                    if row.get('ERDAT') and len(row['ERDAT']) == 8:
                        create_date = datetime.strptime(row['ERDAT'], '%Y%m%d').date()
                    
                    values = (
                        row.get('VBELN'),
                        row.get('AUART'),
                        row.get('VKORG'),
                        row.get('VTWEG'),
                        row.get('SPART'),
                        row.get('KUNNR'),
                        create_date
                    )
                    
                    cursor.execute(insert_query, values)
                    inserted_count += 1
                    
                    if inserted_count % 100 == 0:
                        print(f"  📝 {inserted_count}개 처리 중...")
                        pg_conn.commit()
                
                except Exception as e:
                    print(f"⚠️ 행 삽입 오류: {e}")
                    pg_conn.rollback()
                    continue
            
            pg_conn.commit()
            print(f"\n✅ {inserted_count}개 판매 데이터 저장 완료")
            
            # 4. 결과 확인
            cursor.execute("SELECT COUNT(*) FROM sap_sales_data")
            total_count = cursor.fetchone()[0]
            print(f"📊 총 {total_count}개 데이터 저장됨")
            
        else:
            print("⚠️ SAP에서 데이터를 가져오지 못했습니다.")
    
    except Exception as e:
        print(f"❌ 오류 발생: {e}")
        pg_conn.rollback()
    
    finally:
        sap_conn.close()
        pg_conn.close()
        print("\n✅ 연결 종료")

def import_custom_table(table_name, fields=None, pg_table_name=None):
    """
    사용자 정의 SAP 테이블을 PostgreSQL로 import
    
    Args:
        table_name: SAP 테이블명 (예: 'ZMMR0016' - 재고 테이블)
        fields: 가져올 필드 리스트
        pg_table_name: PostgreSQL 테이블명 (None이면 SAP 테이블명 소문자 사용)
    """
    if pg_table_name is None:
        pg_table_name = table_name.lower()
    
    print(f"\n📖 SAP 테이블 {table_name} 읽기...")
    
    try:
        # SAP 연결
        sap_conn = Connection(**SAP_CONFIG)
        
        # 데이터 읽기
        df = read_sap_table(sap_conn, table_name, fields=fields, max_rows=0)
        
        if df is not None and not df.empty:
            print(f"✅ {len(df)}개 행 읽기 완료")
            
            # PostgreSQL에 저장
            pg_conn = psycopg2.connect(DATABASE_URL)
            
            # pandas to_sql 사용하여 자동으로 테이블 생성 및 데이터 삽입
            from sqlalchemy import create_engine
            engine = create_engine(DATABASE_URL.replace('postgresql://', 'postgresql+psycopg2://'))
            
            df.to_sql(pg_table_name, engine, if_exists='replace', index=False)
            print(f"✅ PostgreSQL 테이블 {pg_table_name}에 저장 완료")
            
            pg_conn.close()
        
        sap_conn.close()
        
    except Exception as e:
        print(f"❌ 오류: {e}")

if __name__ == "__main__":
    print("=" * 50)
    print("SAP to PostgreSQL 데이터 이관")
    print("=" * 50)
    
    if SAP_CONFIG['ashost'] == 'YOUR_SAP_HOST':
        print("\n⚠️ SAP 연결 정보가 설정되지 않았습니다.")
        print("1. .env.sap 파일에 SAP 연결 정보를 입력하세요.")
        print("2. 다시 실행하세요.")
        print("\n예시:")
        print("SAP_ASHOST=10.0.0.1")
        print("SAP_SYSNR=00")
        print("SAP_CLIENT=100")
        print("SAP_USER=username")
        print("SAP_PASSWORD=password")
    else:
        # 판매 데이터 import
        import_sales_from_sap()
        
        # 다른 테이블 import 예제
        # import_custom_table('ZMMR0016')  # 재고 테이블
        # import_custom_table('T001', ['BUKRS', 'BUTXT'])  # 회사 코드