#!/usr/bin/env python3
"""
SAP 테이블 전체 필드 및 데이터 자동 import
필드명과 타입을 자동으로 가져와서 PostgreSQL에 저장
"""

import os
import sys
import pandas as pd
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
from datetime import datetime
import json

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

def get_table_full_info(conn, table_name):
    """
    테이블의 모든 필드 정보를 자동으로 가져오기
    필드명, 데이터 타입, 설명, 길이 등 모든 메타데이터 포함
    """
    try:
        # 방법 1: DDIF_FIELDINFO_GET 사용 (더 상세한 정보)
        try:
            result = conn.call('DDIF_FIELDINFO_GET',
                              TABNAME=table_name,
                              LANGU='KO')  # 한국어 설명
            
            fields_info = []
            for field in result.get('DFIES_TAB', []):
                fields_info.append({
                    'FIELDNAME': field.get('FIELDNAME'),      # 필드명
                    'DATATYPE': field.get('DATATYPE'),        # 데이터 타입
                    'LENG': field.get('LENG'),                # 길이
                    'DECIMALS': field.get('DECIMALS'),        # 소수점
                    'FIELDTEXT': field.get('FIELDTEXT'),      # 필드 설명 (한글)
                    'SCRTEXT_L': field.get('SCRTEXT_L'),      # 긴 설명
                    'SCRTEXT_M': field.get('SCRTEXT_M'),      # 중간 설명
                    'SCRTEXT_S': field.get('SCRTEXT_S'),      # 짧은 설명
                    'KEYFLAG': field.get('KEYFLAG'),          # 키 필드 여부
                    'MANDATORY': field.get('MANDATORY'),       # 필수 필드 여부
                    'CHECKTABLE': field.get('CHECKTABLE'),    # 체크 테이블
                })
            
            print(f"✅ {table_name}: {len(fields_info)}개 필드 정보 조회 완료")
            return fields_info
            
        except:
            # 방법 2: RFC_READ_TABLE 사용 (기본 정보)
            result = conn.call('RFC_READ_TABLE',
                              QUERY_TABLE=table_name,
                              NO_DATA='X')
            
            fields_info = []
            for field in result.get('FIELDS', []):
                fields_info.append({
                    'FIELDNAME': field.get('FIELDNAME'),
                    'OFFSET': field.get('OFFSET'),
                    'LENGTH': field.get('LENGTH'),
                    'TYPE': field.get('TYPE'),
                    'FIELDTEXT': field.get('FIELDTEXT', ''),
                })
            
            print(f"✅ {table_name}: {len(fields_info)}개 필드 정보 조회 완료 (기본)")
            return fields_info
            
    except Exception as e:
        print(f"❌ {table_name} 필드 정보 조회 실패: {e}")
        return None

def read_table_with_all_fields(conn, table_name, max_rows=None):
    """
    테이블의 모든 필드를 자동으로 읽기
    필드를 지정하지 않으면 모든 필드를 가져옴
    """
    try:
        # 1. 모든 필드 정보 먼저 가져오기
        fields_info = get_table_full_info(conn, table_name)
        if not fields_info:
            return None, None
        
        # 2. RFC_READ_TABLE은 최대 512바이트 제한이 있음
        # 필드를 선택적으로 가져오거나 배치로 처리
        
        # 작은 테이블은 전체 필드 가져오기
        print(f"\n📖 {table_name} 테이블 데이터 읽기 중...")
        
        # 필드 리스트 생성 (필드명만)
        field_list = [{'FIELDNAME': f['FIELDNAME']} for f in fields_info[:50]]  # 처음 50개 필드만
        
        result = conn.call('RFC_READ_TABLE',
                          QUERY_TABLE=table_name,
                          DELIMITER='|',
                          FIELDS=field_list,
                          ROWCOUNT=max_rows if max_rows else 1000)
        
        if result['DATA']:
            print(f"✅ {len(result['DATA'])}개 행 읽기 완료")
            
            # 데이터 파싱
            field_names = [f['FIELDNAME'] for f in result['FIELDS']]
            data = []
            for row in result['DATA']:
                values = row['WA'].split('|')
                values = [v.strip() for v in values]
                data.append(dict(zip(field_names, values)))
            
            df = pd.DataFrame(data)
            return df, fields_info
        
        return None, fields_info
        
    except Exception as e:
        print(f"❌ 테이블 읽기 오류: {e}")
        return None, None

def create_postgres_table_auto(table_name, fields_info, df=None):
    """
    SAP 필드 정보를 기반으로 PostgreSQL 테이블 자동 생성
    한글 컬럼명도 코멘트로 추가
    """
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        
        # 테이블명 변환 (SAP_ 접두어 추가)
        pg_table_name = f"sap_{table_name.lower()}"
        
        # 기존 테이블 삭제
        cursor.execute(f"DROP TABLE IF EXISTS {pg_table_name} CASCADE")
        
        # CREATE TABLE 문 생성
        columns = ["id SERIAL PRIMARY KEY"]
        comments = []
        
        for field in fields_info[:50]:  # 처음 50개 필드만
            field_name = field.get('FIELDNAME', '')
            data_type = field.get('DATATYPE', field.get('TYPE', 'CHAR'))
            length = field.get('LENG', field.get('LENGTH', 50))
            decimals = field.get('DECIMALS', 0)
            field_text = field.get('FIELDTEXT', field.get('SCRTEXT_L', ''))
            
            # PostgreSQL 데이터 타입 매핑
            if data_type in ['NUMC', 'DEC', 'CURR', 'QUAN', 'FLTP']:
                if decimals and int(decimals) > 0:
                    pg_type = f"NUMERIC(15, {decimals})"
                else:
                    pg_type = "NUMERIC(15, 2)"
            elif data_type in ['DATS']:
                pg_type = "DATE"
            elif data_type in ['TIMS']:
                pg_type = "TIME"
            elif data_type in ['INT1', 'INT2', 'INT4']:
                pg_type = "INTEGER"
            else:
                pg_type = f"VARCHAR({length})"
            
            columns.append(f'"{field_name}" {pg_type}')
            
            # 한글 설명이 있으면 코멘트 추가
            if field_text:
                comments.append(f"COMMENT ON COLUMN {pg_table_name}.\"{field_name}\" IS '{field_text}';")
        
        # 메타데이터 컬럼 추가
        columns.append("created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        columns.append("sap_sync_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        
        # 테이블 생성
        create_sql = f"CREATE TABLE {pg_table_name} (\n    " + ",\n    ".join(columns) + "\n)"
        cursor.execute(create_sql)
        print(f"✅ {pg_table_name} 테이블 생성 완료")
        
        # 코멘트 추가
        for comment in comments:
            try:
                cursor.execute(comment)
            except:
                pass
        
        # 데이터 삽입
        if df is not None and not df.empty:
            # DataFrame 컬럼명을 테이블 컬럼과 맞춤
            df_columns = [col for col in df.columns if col in [f.get('FIELDNAME') for f in fields_info]]
            
            # 필드 타입 정보 매핑
            field_types = {f['FIELDNAME']: f.get('DATATYPE', f.get('TYPE', '')) for f in fields_info}
            
            inserted_count = 0
            for _, row in df.iterrows():
                insert_columns = []
                insert_values = []
                for col in df_columns:
                    value = row[col]
                    if pd.notna(value):
                        # 날짜 필드 처리 (DATS 타입)
                        if field_types.get(col) == 'DATS':
                            if value == '00000000' or value == '' or value == '0':
                                continue  # NULL로 처리
                            else:
                                # YYYYMMDD를 YYYY-MM-DD로 변환
                                try:
                                    if len(str(value)) == 8:
                                        value = f"{value[:4]}-{value[4:6]}-{value[6:8]}"
                                except:
                                    continue
                        
                        # 시간 필드 처리 (TIMS 타입)
                        elif field_types.get(col) == 'TIMS':
                            if value == '000000' or value == '' or value == '0':
                                continue  # NULL로 처리
                            else:
                                # HHMMSS를 HH:MM:SS로 변환
                                try:
                                    if len(str(value)) == 6:
                                        value = f"{value[:2]}:{value[2:4]}:{value[4:6]}"
                                except:
                                    continue
                        
                        # 숫자 필드 처리
                        elif field_types.get(col) in ['NUMC', 'DEC', 'CURR', 'QUAN', 'FLTP']:
                            try:
                                value = float(value) if value else 0
                            except:
                                value = 0
                        
                        insert_columns.append(f'"{col}"')
                        insert_values.append(value)
                
                if insert_columns:
                    try:
                        insert_sql = f"INSERT INTO {pg_table_name} ({', '.join(insert_columns)}) VALUES ({', '.join(['%s'] * len(insert_values))})"
                        cursor.execute(insert_sql, insert_values)
                        inserted_count += 1
                        if inserted_count % 100 == 0:
                            conn.commit()
                            print(f"  📝 {inserted_count}개 저장 중...")
                    except Exception as e:
                        if inserted_count == 0:
                            print(f"  ⚠️ 첫 번째 행 삽입 오류: {str(e)[:100]}")
                        continue
            
            print(f"✅ {inserted_count}개 데이터 삽입 완료 (전체: {len(df)}개)")
        
        conn.commit()
        conn.close()
        
        # 필드 정보를 별도 테이블에 저장
        save_field_info(table_name, fields_info)
        
    except Exception as e:
        print(f"❌ PostgreSQL 테이블 생성 실패: {e}")

def save_field_info(table_name, fields_info):
    """필드 메타데이터를 별도 테이블에 저장"""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        
        # 메타데이터 테이블 생성
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sap_table_metadata (
                id SERIAL PRIMARY KEY,
                table_name VARCHAR(50),
                field_name VARCHAR(50),
                field_text TEXT,
                data_type VARCHAR(20),
                length INTEGER,
                decimals INTEGER,
                key_flag VARCHAR(1),
                mandatory VARCHAR(1),
                check_table VARCHAR(50),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # 기존 데이터 삭제
        cursor.execute("DELETE FROM sap_table_metadata WHERE table_name = %s", (table_name,))
        
        # 새 데이터 삽입
        for field in fields_info:
            cursor.execute("""
                INSERT INTO sap_table_metadata 
                (table_name, field_name, field_text, data_type, length, decimals, key_flag, mandatory, check_table)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                table_name,
                field.get('FIELDNAME'),
                field.get('FIELDTEXT', field.get('SCRTEXT_L', '')),
                field.get('DATATYPE', field.get('TYPE')),
                field.get('LENG', field.get('LENGTH')),
                field.get('DECIMALS'),
                field.get('KEYFLAG'),
                field.get('MANDATORY'),
                field.get('CHECKTABLE')
            ))
        
        conn.commit()
        conn.close()
        print(f"✅ {table_name} 필드 메타데이터 저장 완료")
        
    except Exception as e:
        print(f"❌ 메타데이터 저장 실패: {e}")

def list_available_rfcs(conn):
    """사용 가능한 RFC 함수 목록 조회"""
    try:
        # RFC 함수 목록 조회 (TFDIR 테이블)
        result = conn.call('RFC_READ_TABLE',
                          QUERY_TABLE='TFDIR',
                          DELIMITER='|',
                          FIELDS=[
                              {'FIELDNAME': 'FUNCNAME'},
                              {'FIELDNAME': 'PNAME'},
                          ],
                          OPTIONS=[
                              {'TEXT': "FUNCNAME LIKE 'RFC_%'"},  # RFC로 시작하는 함수
                              {'TEXT': "OR FUNCNAME LIKE 'BAPI_%'"},  # BAPI 함수
                              {'TEXT': "OR FUNCNAME LIKE 'Z%'"},  # 커스텀 함수
                          ],
                          ROWCOUNT=100)
        
        print("\n📋 사용 가능한 RFC 함수:")
        for row in result['DATA'][:20]:  # 처음 20개만
            values = row['WA'].split('|')
            print(f"  - {values[0].strip()}")
        
        return result['DATA']
        
    except Exception as e:
        print(f"❌ RFC 목록 조회 실패: {e}")
        return []

def import_table_auto(table_name):
    """테이블을 자동으로 import (모든 필드 자동 감지)"""
    
    print(f"\n{'='*60}")
    print(f"테이블 {table_name} 자동 Import 시작")
    print(f"{'='*60}")
    
    try:
        # SAP 연결
        sap_conn = Connection(**SAP_CONFIG)
        print("✅ SAP 연결 성공")
        
        # 1. 필드 정보 자동 조회
        fields_info = get_table_full_info(sap_conn, table_name)
        
        if fields_info:
            print(f"\n📊 필드 정보 ({len(fields_info)}개):")
            for i, field in enumerate(fields_info[:10], 1):  # 처음 10개만 출력
                field_name = field.get('FIELDNAME')
                field_text = field.get('FIELDTEXT', field.get('SCRTEXT_L', ''))
                field_type = field.get('DATATYPE', field.get('TYPE', ''))
                print(f"  {i}. {field_name} ({field_type}): {field_text}")
            
            # 2. 데이터 읽기 (모든 필드)
            df, _ = read_table_with_all_fields(sap_conn, table_name, max_rows=1000)
            
            # 3. PostgreSQL 테이블 자동 생성 및 데이터 저장
            if df is not None:
                create_postgres_table_auto(table_name, fields_info, df)
                print(f"\n✅ {table_name} 테이블 import 완료!")
            
        sap_conn.close()
        
    except Exception as e:
        print(f"❌ 오류: {e}")

if __name__ == "__main__":
    print("=" * 60)
    print("SAP 테이블 자동 Import (전체 필드)")
    print("=" * 60)
    
    # 예제: 자주 사용하는 테이블 자동 import
    tables_to_import = [
        'MARA',  # 자재 마스터
        'MAKT',  # 자재 설명
        'MARD',  # 재고
        'T001',  # 회사 코드
        'KNA1',  # 고객 마스터
        'LFA1',  # 공급업체 마스터
    ]
    
    print("\n1. 개별 테이블 import")
    print("2. 여러 테이블 한번에 import")
    print("3. RFC 함수 목록 조회")
    print("\n자동 실행: MARA 테이블 import")
    
    # MARA 테이블 자동 import 테스트
    import_table_auto('MARA')