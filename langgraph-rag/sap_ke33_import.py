#!/usr/bin/env python3
"""
SAP KE33 (Profitability Analysis) 데이터 import
CO-PA 수익성 분석 데이터를 PostgreSQL로 저장
"""

import os
import sys
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from datetime import datetime, timedelta

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

# 데이터베이스 연결 정보
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://baseone:baseone@localhost:10003/baseone')

# SAP 연결 정보
SAP_CONFIG = {
    'ashost': os.getenv('SAP_ASHOST', '192.168.32.100'),
    'sysnr': os.getenv('SAP_SYSNR', '00'),
    'client': os.getenv('SAP_CLIENT', '100'),
    'user': os.getenv('SAP_USER', 'bc01'),
    'passwd': os.getenv('SAP_PASSWORD', 'dlsxjdpa2023@!'),
    'lang': 'KO',
}

def get_default_parameters(conn):
    """KE33에 필요한 기본 파라미터 조회"""
    params = {}
    
    # 1. 회사코드 조회 (T001)
    params['BUKRS'] = '1000'  # 기본값
    
    # 2. 조회기간 설정 (최근 3개월)
    today = datetime.now()
    params['DATE_FROM'] = (today - timedelta(days=90)).strftime('%Y%m%d')
    params['DATE_TO'] = today.strftime('%Y%m%d')
    params['GJAHR'] = str(today.year)  # 회계연도
    
    # 3. 버전 (실제 = 000)
    params['VERSN'] = '014'
    
    # 4. 경영단위 - 일반적으로 1000 또는 회사코드와 동일
    params['ERKRS'] = params['BUKRS']
    
    return params
r
def import_copa_data(erkrs=None, date_from=None, date_to=None, bukrs=None):
    """
    CO-PA 데이터 import
    
    Parameters:
    - erkrs: 경영단위 (기본값: 회사코드)
    - date_from: 시작일 (YYYYMMDD)
    - date_to: 종료일 (YYYYMMDD)
    - bukrs: 회사코드
    """
    
    print("="*60)
    print("KE33 CO-PA 데이터 Import")
    print("="*60)
    
    try:
        # SAP 연결
        sap_conn = Connection(**SAP_CONFIG)
        print("✅ SAP 연결 성공")
        
        # 기본 파라미터 설정
        params = get_default_parameters(sap_conn)
        
        # 사용자 지정 파라미터 오버라이드
        if erkrs:
            params['ERKRS'] = erkrs
        if date_from:
            params['DATE_FROM'] = date_from
        if date_to:
            params['DATE_TO'] = date_to
        if bukrs:
            params['BUKRS'] = bukrs
        
        print(f"\n📋 조회 파라미터:")
        print(f"  - 경영단위: {params.get('ERKRS', 'N/A')}")
        print(f"  - 회사코드: {params['BUKRS']}")
        print(f"  - 조회기간: {params['DATE_FROM']} ~ {params['DATE_TO']}")
        print(f"  - 버전: {params['VERSN']}")
        
        # CO-PA 데이터 조회 시도
        # 1. 먼저 CE1 테이블 시도 (경영단위별 실적)
        ce_tables = [
            f"CE1{params['ERKRS']}",  # 실적
            f"CE11000",  # 기본 경영단위
            "CE11001",   # 다른 가능한 경영단위
        ]
        
        data_found = False
        for ce_table in ce_tables:
            try:
                print(f"\n📖 {ce_table} 테이블 조회 시도...")
                
                # 테이블 필드 확인
                result = sap_conn.call('RFC_READ_TABLE',
                                      QUERY_TABLE=ce_table,
                                      DELIMITER='|',
                                      NO_DATA='X')
                
                if result['FIELDS']:
                    print(f"✅ {ce_table} 접근 가능 ({len(result['FIELDS'])}개 필드)")
                    
                    # 주요 필드 선택 (CO-PA 표준 필드)
                    copa_fields = []
                    field_names = [f['FIELDNAME'] for f in result['FIELDS']]
                    
                    # 필수 필드 체크
                    essential_fields = [
                        'BUKRS',    # 회사코드
                        'GJAHR',    # 회계연도
                        'PERDE',    # 기간
                        'KNDNR',    # 고객번호
                        'ARTNR',    # 제품번호
                        'KAUFN',    # 판매오더
                        'KDPOS',    # 판매오더항목
                        'VV010',    # 매출액 (또는 다른 값 필드)
                        'VV020',    # 매출원가
                        'VV030',    # 매출총이익
                    ]
                    
                    for field in essential_fields:
                        if field in field_names:
                            copa_fields.append({'FIELDNAME': field})
                    
                    # 추가 값 필드 (VV로 시작하는 필드들)
                    for field in field_names:
                        if field.startswith('VV') and len(copa_fields) < 30:
                            if {'FIELDNAME': field} not in copa_fields:
                                copa_fields.append({'FIELDNAME': field})
                    
                    if not copa_fields:
                        # 최소한 처음 20개 필드라도 가져오기
                        copa_fields = [{'FIELDNAME': f} for f in field_names[:20]]
                    
                    # WHERE 조건 설정
                    options = []
                    if 'BUKRS' in field_names:
                        options.append({'TEXT': f"BUKRS = '{params['BUKRS']}'"})
                    if 'GJAHR' in field_names:
                        options.append({'TEXT': f"AND GJAHR = '{params['GJAHR']}'"})
                    
                    # 데이터 조회
                    result_data = sap_conn.call('RFC_READ_TABLE',
                                                QUERY_TABLE=ce_table,
                                                DELIMITER='|',
                                                FIELDS=copa_fields,
                                                OPTIONS=options if options else [],
                                                ROWCOUNT=1000)
                    
                    if result_data['DATA']:
                        print(f"✅ {len(result_data['DATA'])}개 데이터 조회 성공")
                        
                        # DataFrame 생성
                        field_names = [f['FIELDNAME'] for f in result_data['FIELDS']]
                        data = []
                        for row in result_data['DATA']:
                            values = row['WA'].split('|')
                            values = [v.strip() for v in values]
                            data.append(dict(zip(field_names, values)))
                        
                        df = pd.DataFrame(data)
                        
                        # PostgreSQL 저장
                        save_copa_to_postgres(df, ce_table, params)
                        data_found = True
                        break
                        
            except Exception as e:
                print(f"  ⚠️ {ce_table} 조회 실패: {str(e)[:80]}")
                continue
        
        if not data_found:
            # 대체 방법: COSP 테이블 (원가센터 실적)
            print("\n📖 대체 방법: COSP 테이블 조회 (원가센터별 실적)...")
            try:
                result = sap_conn.call('RFC_READ_TABLE',
                                      QUERY_TABLE='COSP',
                                      DELIMITER='|',
                                      FIELDS=[
                                          {'FIELDNAME': 'OBJNR'},   # 오브젝트번호
                                          {'FIELDNAME': 'GJAHR'},   # 회계연도
                                          {'FIELDNAME': 'KSTAR'},   # 원가요소
                                          {'FIELDNAME': 'WKG001'},  # 1월 금액
                                          {'FIELDNAME': 'WKG002'},  # 2월 금액
                                          {'FIELDNAME': 'WKG003'},  # 3월 금액
                                          {'FIELDNAME': 'WKG004'},  # 4월 금액
                                          {'FIELDNAME': 'WKG005'},  # 5월 금액
                                          {'FIELDNAME': 'WKG006'},  # 6월 금액
                                      ],
                                      OPTIONS=[
                                          {'TEXT': f"GJAHR = '{params['GJAHR']}'"},
                                      ],
                                      ROWCOUNT=1000)
                
                if result['DATA']:
                    print(f"✅ COSP 테이블에서 {len(result['DATA'])}개 데이터 조회")
                    
                    # DataFrame 생성
                    field_names = [f['FIELDNAME'] for f in result['FIELDS']]
                    data = []
                    for row in result['DATA']:
                        values = row['WA'].split('|')
                        values = [v.strip() for v in values]
                        data.append(dict(zip(field_names, values)))
                    
                    df_cosp = pd.DataFrame(data)
                    save_cosp_to_postgres(df_cosp, params)
                    data_found = True
                    
            except Exception as e:
                print(f"  ⚠️ COSP 조회 실패: {str(e)[:80]}")
        
        if not data_found:
            print("\n⚠️ CO-PA 데이터를 찾을 수 없습니다.")
            print("가능한 원인:")
            print("1. 경영단위가 활성화되지 않음")
            print("2. 해당 기간에 데이터가 없음")
            print("3. 권한 부족")
        
        sap_conn.close()
        
    except Exception as e:
        print(f"❌ 오류 발생: {e}")

def save_copa_to_postgres(df, table_name, params):
    """CO-PA 데이터를 PostgreSQL에 저장"""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        
        # 테이블 생성
        cursor.execute("DROP TABLE IF EXISTS sap_ke33_copa CASCADE")
        
        create_table = """
        CREATE TABLE sap_ke33_copa (
            id SERIAL PRIMARY KEY,
            "회사코드" VARCHAR(4),
            "회계연도" VARCHAR(4),
            "기간" VARCHAR(3),
            "고객번호" VARCHAR(10),
            "제품번호" VARCHAR(18),
            "판매오더" VARCHAR(10),
            "매출액" NUMERIC(20,2),
            "매출원가" NUMERIC(20,2),
            "매출총이익" NUMERIC(20,2),
            "원본테이블" VARCHAR(30),
            "원본데이터" JSONB,
            "조회일시" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        cursor.execute(create_table)
        
        # 데이터 삽입
        inserted = 0
        for _, row in df.iterrows():
            # 금액 필드 처리
            revenue = 0
            cost = 0
            
            # VV 필드에서 금액 추출
            for col in df.columns:
                if col.startswith('VV'):
                    try:
                        value = float(row[col]) if row[col] else 0
                        if '010' in col:  # 매출
                            revenue = value
                        elif '020' in col:  # 원가
                            cost = value
                    except:
                        pass
            
            cursor.execute("""
                INSERT INTO sap_ke33_copa 
                ("회사코드", "회계연도", "기간", "고객번호", "제품번호", "판매오더",
                 "매출액", "매출원가", "매출총이익", "원본테이블", "원본데이터")
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            """, (
                row.get('BUKRS', params.get('BUKRS')),
                row.get('GJAHR', params.get('GJAHR')),
                row.get('PERDE', ''),
                row.get('KNDNR', ''),
                row.get('ARTNR', ''),
                row.get('KAUFN', ''),
                revenue,
                cost,
                revenue - cost,
                table_name,
                pd.Series(row).to_json()
            ))
            inserted += 1
            
            if inserted % 100 == 0:
                conn.commit()
                print(f"  📝 {inserted}개 저장 중...")
        
        conn.commit()
        conn.close()
        
        print(f"\n✅ sap_ke33_copa 테이블에 {inserted}개 데이터 저장 완료")
        
    except Exception as e:
        print(f"❌ PostgreSQL 저장 실패: {e}")

def save_cosp_to_postgres(df, params):
    """COSP (원가센터 실적) 데이터를 PostgreSQL에 저장"""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        
        # 테이블 생성
        cursor.execute("DROP TABLE IF EXISTS sap_ke33_cosp CASCADE")
        
        create_table = """
        CREATE TABLE sap_ke33_cosp (
            id SERIAL PRIMARY KEY,
            "오브젝트번호" VARCHAR(22),
            "회계연도" VARCHAR(4),
            "원가요소" VARCHAR(10),
            "1월" NUMERIC(20,2),
            "2월" NUMERIC(20,2),
            "3월" NUMERIC(20,2),
            "4월" NUMERIC(20,2),
            "5월" NUMERIC(20,2),
            "6월" NUMERIC(20,2),
            "합계" NUMERIC(20,2),
            "조회일시" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        cursor.execute(create_table)
        
        # 데이터 삽입
        inserted = 0
        for _, row in df.iterrows():
            # 월별 금액 합계
            total = 0
            amounts = []
            for i in range(1, 7):
                col = f'WKG{i:03d}'
                amount = float(row.get(col, 0)) if row.get(col) else 0
                amounts.append(amount)
                total += amount
            
            cursor.execute("""
                INSERT INTO sap_ke33_cosp 
                ("오브젝트번호", "회계연도", "원가요소", "1월", "2월", "3월", "4월", "5월", "6월", "합계")
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                row.get('OBJNR'),
                row.get('GJAHR'),
                row.get('KSTAR'),
                amounts[0], amounts[1], amounts[2],
                amounts[3], amounts[4], amounts[5],
                total
            ))
            inserted += 1
        
        conn.commit()
        conn.close()
        
        print(f"\n✅ sap_ke33_cosp 테이블에 {inserted}개 데이터 저장 완료")
        
    except Exception as e:
        print(f"❌ PostgreSQL 저장 실패: {e}")

if __name__ == "__main__":
    # KE33 데이터 import (파라미터 없으면 기본값 사용)
    import_copa_data(
        erkrs=None,      # 경영단위 (기본값 사용)
        date_from=None,  # 시작일 (최근 3개월)
        date_to=None,    # 종료일 (오늘)
        bukrs=None       # 회사코드 (T001에서 조회)
    )