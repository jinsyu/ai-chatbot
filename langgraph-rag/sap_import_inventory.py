#!/usr/bin/env python3
"""
SAP에서 재고 데이터(ZMMR0016)를 가져와서 PostgreSQL에 저장
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

def get_table_fields(conn, table_name):
    """SAP 테이블의 필드 정보 가져오기"""
    try:
        result = conn.call('DDIF_FIELDINFO_GET',
                          TABNAME=table_name,
                          LANGU='KO')
        
        fields = []
        for field in result.get('DFIES_TAB', []):
            fields.append({
                'name': field.get('FIELDNAME'),
                'text': field.get('FIELDTEXT'),
                'type': field.get('DATATYPE'),
                'length': field.get('LENG')
            })
        return fields
    except:
        # 대체 방법: RFC_READ_TABLE로 필드 정보 가져오기
        try:
            result = conn.call('RFC_READ_TABLE',
                              QUERY_TABLE=table_name,
                              NO_DATA='X')
            fields = []
            for field in result.get('FIELDS', []):
                fields.append({
                    'name': field.get('FIELDNAME'),
                    'offset': field.get('OFFSET'),
                    'length': field.get('LENGTH'),
                    'type': field.get('TYPE')
                })
            return fields
        except Exception as e:
            print(f"필드 정보 조회 실패: {e}")
            return None

def read_sap_table_batch(conn, table_name, batch_size=5000):
    """SAP 테이블을 배치로 읽기"""
    all_data = []
    row_skip = 0
    
    try:
        while True:
            print(f"  📖 {row_skip}행부터 {batch_size}개 읽기 중...")
            
            # OPTIONS에 ROWSKIPS 추가
            options = [{'TEXT': f'ROWSKIPS = {row_skip}'}] if row_skip > 0 else []
            
            result = conn.call('RFC_READ_TABLE',
                              QUERY_TABLE=table_name,
                              DELIMITER='|',
                              ROWCOUNT=batch_size,
                              OPTIONS=options)
            
            # 데이터가 없으면 종료
            if not result['DATA']:
                break
            
            # 필드명 추출
            if not all_data:  # 첫 배치에서만 필드명 추출
                field_names = [f['FIELDNAME'] for f in result['FIELDS']]
            
            # 데이터 파싱
            for row in result['DATA']:
                values = row['WA'].split('|')
                values = [v.strip() for v in values]
                all_data.append(dict(zip(field_names, values)))
            
            # 읽은 행 수가 배치 크기보다 작으면 마지막 배치
            if len(result['DATA']) < batch_size:
                break
            
            row_skip += batch_size
        
        print(f"  ✅ 총 {len(all_data)}개 행 읽기 완료")
        return pd.DataFrame(all_data) if all_data else None
    
    except Exception as e:
        print(f"❌ 테이블 읽기 오류: {e}")
        return None

def import_zmmr0016():
    """ZMMR0016 테이블 (재고현황) import"""
    
    print("\n🔗 SAP 연결 중...")
    try:
        sap_conn = Connection(**SAP_CONFIG)
        print("✅ SAP 연결 성공")
    except Exception as e:
        print(f"❌ SAP 연결 실패: {e}")
        return
    
    try:
        # 1. 테이블 존재 여부 확인
        print("\n📋 ZMMR0016 테이블 정보 조회 중...")
        fields = get_table_fields(sap_conn, 'ZMMR0016')
        
        if fields:
            print(f"✅ {len(fields)}개 필드 확인")
            print("\n필드 목록:")
            for i, field in enumerate(fields[:10], 1):  # 처음 10개만 출력
                print(f"  {i}. {field['name']}")
        
        # 2. 데이터 읽기
        print("\n📖 ZMMR0016 데이터 읽기 중...")
        df = read_sap_table_batch(sap_conn, 'ZMMR0016', batch_size=5000)
        
        if df is not None and not df.empty:
            print(f"✅ {len(df)}개 행 읽기 완료")
            
            # 3. PostgreSQL에 저장
            print("\n💾 PostgreSQL에 저장 중...")
            pg_conn = psycopg2.connect(DATABASE_URL)
            cursor = pg_conn.cursor()
            
            # 기존 테이블 삭제
            cursor.execute("DROP TABLE IF EXISTS sap_inventory_status CASCADE")
            
            # 테이블 생성 (한글 컬럼명)
            create_table_query = """
            CREATE TABLE sap_inventory_status (
                id SERIAL PRIMARY KEY,
                "자재" TEXT,
                "자재명" TEXT,
                "자재유형" TEXT,
                "제품군명" TEXT,
                "자재유형명" TEXT,
                "공급업체" TEXT,
                "재고구분명" TEXT,
                "총재고수량" NUMERIC(15, 3),
                "재고금액" NUMERIC(20, 2),
                "가용재고수량" NUMERIC(15, 3),
                "가용재고금액" NUMERIC(20, 2),
                "원본데이터" JSONB,
                "생성일시" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            cursor.execute(create_table_query)
            
            # 데이터 삽입
            inserted_count = 0
            for _, row in df.iterrows():
                try:
                    # 주요 필드 추출 (SAP 필드명은 실제 확인 필요)
                    insert_query = """
                    INSERT INTO sap_inventory_status (
                        "자재", "자재명", "원본데이터"
                    ) VALUES (%s, %s, %s::jsonb)
                    """
                    
                    values = (
                        row.get('MATNR'),  # 자재번호
                        row.get('MAKTX'),  # 자재명
                        pd.Series(row).to_json()  # 전체 데이터를 JSON으로 저장
                    )
                    
                    cursor.execute(insert_query, values)
                    inserted_count += 1
                    
                    if inserted_count % 1000 == 0:
                        print(f"  📝 {inserted_count}개 저장 중...")
                        pg_conn.commit()
                
                except Exception as e:
                    if inserted_count == 0:  # 첫 번째 오류만 출력
                        print(f"⚠️ 행 삽입 오류: {e}")
                    pg_conn.rollback()
                    continue
            
            pg_conn.commit()
            print(f"\n✅ {inserted_count}개 데이터 PostgreSQL 저장 완료")
            
            # 결과 확인
            cursor.execute("SELECT COUNT(*) FROM sap_inventory_status")
            total_count = cursor.fetchone()[0]
            print(f"📊 총 {total_count}개 데이터 저장됨")
            
            pg_conn.close()
        else:
            print("⚠️ 데이터를 가져오지 못했습니다.")
    
    except Exception as e:
        print(f"❌ 오류 발생: {e}")
    
    finally:
        sap_conn.close()
        print("\n✅ SAP 연결 종료")

def check_available_tables():
    """사용 가능한 SAP 테이블 확인"""
    
    print("\n🔗 SAP 연결 중...")
    try:
        sap_conn = Connection(**SAP_CONFIG)
        print("✅ SAP 연결 성공")
        
        # 커스텀 테이블 조회 (Z로 시작하는 테이블)
        print("\n📋 커스텀 테이블 조회 중...")
        
        # DD02L 테이블에서 Z로 시작하는 테이블 조회
        result = sap_conn.call('RFC_READ_TABLE',
                              QUERY_TABLE='DD02L',
                              DELIMITER='|',
                              FIELDS=[
                                  {'FIELDNAME': 'TABNAME'},
                                  {'FIELDNAME': 'DDTEXT'},
                                  {'FIELDNAME': 'TABCLASS'}
                              ],
                              OPTIONS=[
                                  {'TEXT': "TABNAME LIKE 'Z%'"},
                                  {'TEXT': "AND TABCLASS = 'TRANSP'"}
                              ],
                              ROWCOUNT=100)
        
        if result['DATA']:
            print(f"\n✅ {len(result['DATA'])}개 커스텀 테이블 발견:")
            for row in result['DATA'][:20]:  # 처음 20개만 출력
                values = row['WA'].split('|')
                table_name = values[0].strip()
                table_desc = values[1].strip() if len(values) > 1 else ''
                print(f"  - {table_name}: {table_desc}")
        
        sap_conn.close()
        
    except Exception as e:
        print(f"❌ 오류: {e}")

if __name__ == "__main__":
    print("=" * 50)
    print("SAP 재고 데이터 Import")
    print("=" * 50)
    
    # import 옵션 선택
    print("\n작업 선택:")
    print("1. ZMMR0016 테이블 import")
    print("2. 사용 가능한 테이블 확인")
    print("3. 자동 실행 (테이블 확인)")
    
    # 사용 가능한 테이블 먼저 확인
    check_available_tables()
    
    # ZMMR0016이 있으면 import
    # import_zmmr0016()