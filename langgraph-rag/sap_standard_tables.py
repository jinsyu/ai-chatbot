#!/usr/bin/env python3
"""
SAP 표준 테이블 데이터 가져오기
"""

import os
import sys
import pandas as pd
import psycopg2
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

# 데이터베이스 연결 정보
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://baseone:baseone@localhost:10003/baseone')

# SAP 연결 정보
SAP_CONFIG = {
    'ashost': os.getenv('SAP_ASHOST', '192.168.32.100'),
    'sysnr': os.getenv('SAP_SYSNR', '00'),
    'client': os.getenv('SAP_CLIENT', '100'),
    'user': os.getenv('SAP_USER', 'bc01'),
    'passwd': os.getenv('SAP_PASSWORD', 'dlsxjdpa2023@!'),
    'lang': os.getenv('SAP_LANG', 'KO'),
}

def test_table_access(conn, table_name):
    """테이블 접근 가능 여부 테스트"""
    try:
        result = conn.call('RFC_READ_TABLE',
                          QUERY_TABLE=table_name,
                          DELIMITER='|',
                          ROWCOUNT=1,
                          NO_DATA='X')  # 데이터 없이 메타정보만
        
        fields = result.get('FIELDS', [])
        if fields:
            print(f"✅ {table_name}: 접근 가능 ({len(fields)}개 필드)")
            return True
        else:
            print(f"❌ {table_name}: 필드 정보 없음")
            return False
    except Exception as e:
        print(f"❌ {table_name}: 접근 불가 - {str(e)[:50]}")
        return False

def get_material_master():
    """자재 마스터 데이터 가져오기 (MARA, MAKT 테이블)"""
    
    print("\n🔗 SAP 연결 중...")
    try:
        sap_conn = Connection(**SAP_CONFIG)
        print("✅ SAP 연결 성공")
        
        # MARA: 자재 마스터 일반 데이터
        print("\n📖 MARA 테이블 (자재 마스터) 읽기...")
        result = sap_conn.call('RFC_READ_TABLE',
                              QUERY_TABLE='MARA',
                              DELIMITER='|',
                              FIELDS=[
                                  {'FIELDNAME': 'MATNR'},  # 자재번호
                                  {'FIELDNAME': 'MTART'},  # 자재유형
                                  {'FIELDNAME': 'MATKL'},  # 자재그룹
                                  {'FIELDNAME': 'MEINS'},  # 기본단위
                                  {'FIELDNAME': 'BRGEW'},  # 총중량
                                  {'FIELDNAME': 'NTGEW'},  # 순중량
                                  {'FIELDNAME': 'GEWEI'},  # 중량단위
                              ],
                              ROWCOUNT=100)
        
        if result['DATA']:
            print(f"✅ {len(result['DATA'])}개 자재 마스터 데이터 조회")
            
            # 데이터 파싱
            field_names = [f['FIELDNAME'] for f in result['FIELDS']]
            data = []
            for row in result['DATA']:
                values = row['WA'].split('|')
                values = [v.strip() for v in values]
                data.append(dict(zip(field_names, values)))
            
            df_mara = pd.DataFrame(data)
            print(df_mara.head())
            
            # PostgreSQL에 저장
            save_to_postgres(df_mara, 'sap_material_master')
        
        sap_conn.close()
        
    except Exception as e:
        print(f"❌ 오류: {e}")

def get_inventory_data():
    """재고 데이터 가져오기 (MARD 테이블)"""
    
    print("\n🔗 SAP 연결 중...")
    try:
        sap_conn = Connection(**SAP_CONFIG)
        print("✅ SAP 연결 성공")
        
        # MARD: 플랜트별 자재 재고
        print("\n📖 MARD 테이블 (재고) 읽기...")
        result = sap_conn.call('RFC_READ_TABLE',
                              QUERY_TABLE='MARD',
                              DELIMITER='|',
                              FIELDS=[
                                  {'FIELDNAME': 'MATNR'},  # 자재번호
                                  {'FIELDNAME': 'WERKS'},  # 플랜트
                                  {'FIELDNAME': 'LGORT'},  # 저장위치
                                  {'FIELDNAME': 'LABST'},  # 가용재고
                                  {'FIELDNAME': 'UMLME'},  # 이동중재고
                                  {'FIELDNAME': 'INSME'},  # 품질검사재고
                                  {'FIELDNAME': 'EINME'},  # 제한재고
                                  {'FIELDNAME': 'SPEME'},  # 블록재고
                              ],
                              ROWCOUNT=100)
        
        if result['DATA']:
            print(f"✅ {len(result['DATA'])}개 재고 데이터 조회")
            
            # 데이터 파싱
            field_names = [f['FIELDNAME'] for f in result['FIELDS']]
            data = []
            for row in result['DATA']:
                values = row['WA'].split('|')
                values = [v.strip() for v in values]
                data.append(dict(zip(field_names, values)))
            
            df_mard = pd.DataFrame(data)
            print(df_mard.head())
            
            # PostgreSQL에 저장
            save_to_postgres(df_mard, 'sap_inventory')
        
        sap_conn.close()
        
    except Exception as e:
        print(f"❌ 오류: {e}")

def save_to_postgres(df, table_name):
    """DataFrame을 PostgreSQL에 저장"""
    try:
        from sqlalchemy import create_engine
        
        # SQLAlchemy 엔진 생성
        engine = create_engine(DATABASE_URL.replace('postgresql://', 'postgresql+psycopg2://'))
        
        # 테이블에 저장 (기존 테이블 교체)
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        
        print(f"✅ {table_name} 테이블에 {len(df)}개 행 저장 완료")
        
        # 한글 컬럼명으로 뷰 생성
        if table_name == 'sap_inventory':
            conn = psycopg2.connect(DATABASE_URL)
            cursor = conn.cursor()
            
            cursor.execute(f"DROP VIEW IF EXISTS {table_name}_korean")
            create_view = f"""
            CREATE VIEW {table_name}_korean AS
            SELECT 
                "MATNR" as "자재번호",
                "WERKS" as "플랜트",
                "LGORT" as "저장위치",
                CAST("LABST" AS NUMERIC) as "가용재고",
                CAST("UMLME" AS NUMERIC) as "이동중재고",
                CAST("INSME" AS NUMERIC) as "품질검사재고",
                CAST("EINME" AS NUMERIC) as "제한재고",
                CAST("SPEME" AS NUMERIC) as "블록재고"
            FROM {table_name}
            """
            cursor.execute(create_view)
            conn.commit()
            conn.close()
            print(f"✅ {table_name}_korean 뷰 생성 완료")
        
    except Exception as e:
        print(f"❌ PostgreSQL 저장 실패: {e}")

def check_standard_tables():
    """표준 테이블 접근 가능 여부 확인"""
    
    print("\n🔗 SAP 연결 중...")
    try:
        sap_conn = Connection(**SAP_CONFIG)
        print("✅ SAP 연결 성공")
        
        # 확인할 표준 테이블 목록
        tables = {
            # 자재 마스터
            'MARA': '자재 마스터 일반',
            'MAKT': '자재 설명',
            'MARC': '자재 마스터 플랜트',
            'MARD': '자재 마스터 저장위치/재고',
            
            # 재고
            'MKPF': '자재 문서 헤더',
            'MSEG': '자재 문서 세그먼트',
            'MSKA': '판매 오더 재고',
            'MSKU': '특별 재고 (공급업체)',
            
            # 구매
            'EKKO': '구매 문서 헤더',
            'EKPO': '구매 문서 아이템',
            
            # 판매
            'VBAK': '판매 문서 헤더',
            'VBAP': '판매 문서 아이템',
            
            # 고객/공급업체
            'KNA1': '고객 마스터 일반',
            'LFA1': '공급업체 마스터 일반',
            
            # 회계
            'BKPF': '회계 문서 헤더',
            'BSEG': '회계 문서 세그먼트',
            
            # 기타
            'T001': '회사 코드',
            'T001W': '플랜트',
        }
        
        print("\n📋 표준 테이블 접근 권한 확인:")
        print("-" * 50)
        
        accessible_tables = []
        for table, desc in tables.items():
            if test_table_access(sap_conn, table):
                accessible_tables.append(table)
        
        print("\n✅ 접근 가능한 테이블:")
        for table in accessible_tables:
            print(f"  - {table}: {tables[table]}")
        
        sap_conn.close()
        return accessible_tables
        
    except Exception as e:
        print(f"❌ 오류: {e}")
        return []

if __name__ == "__main__":
    print("=" * 50)
    print("SAP 표준 테이블 데이터 Import")
    print("=" * 50)
    
    # 1. 접근 가능한 테이블 확인
    accessible = check_standard_tables()
    
    if accessible:
        print("\n데이터를 가져올 테이블 선택:")
        print("1. MARA/MAKT (자재 마스터)")
        print("2. MARD (재고)")
        print("3. 모두")
        
        # 자동으로 재고 데이터 가져오기
        if 'MARD' in accessible:
            get_inventory_data()
        
        # 자재 마스터도 가져오기
        if 'MARA' in accessible:
            get_material_master()