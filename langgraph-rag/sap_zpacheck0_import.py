#!/usr/bin/env python3
"""
SAP ZPACHECK0 데이터 import
조회기간: 3개월
버전: 14
회사코드: 1000
"""

import os
import sys
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from datetime import datetime, timedelta
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
    'lang': 'KO',
}

def import_zpacheck0_data():
    """
    ZPACHECK0 데이터 import
    고정 파라미터:
    - 회사코드: 1000
    - 버전: 014
    - 조회기간: 최근 3개월
    """
    
    print("="*60)
    print("ZPACHECK0 데이터 Import")
    print("="*60)
    
    try:
        # SAP 연결
        sap_conn = Connection(**SAP_CONFIG)
        print("✅ SAP 연결 성공")
        
        # 파라미터 설정
        today = datetime.now()
        date_from = (today - timedelta(days=90)).strftime('%Y%m%d')
        date_to = today.strftime('%Y%m%d')
        
        params = {
            'BUKRS': '1000',
            'VERSN': '014',
            'DATE_FROM': date_from,
            'DATE_TO': date_to,
            'GJAHR': str(today.year),
            'ERKRS': '1000'
        }
        
        print(f"\n📋 조회 파라미터:")
        print(f"  - 회사코드: {params['BUKRS']}")
        print(f"  - 버전: {params['VERSN']}")
        print(f"  - 조회기간: {params['DATE_FROM']} ~ {params['DATE_TO']}")
        print(f"  - 회계연도: {params['GJAHR']}")
        
        # 1. T-Code 정보 확인
        print("\n📖 ZPACHECK0 프로그램 정보 확인...")
        try:
            result = sap_conn.call('RFC_READ_TABLE',
                                  QUERY_TABLE='TSTC',
                                  DELIMITER='|',
                                  FIELDS=[
                                      {'FIELDNAME': 'TCODE'},
                                      {'FIELDNAME': 'PGMNA'},
                                  ],
                                  OPTIONS=[
                                      {'TEXT': "TCODE = 'ZPACHECK0'"},
                                  ])
            
            if result['DATA']:
                values = result['DATA'][0]['WA'].split('|')
                prog_name = values[1].strip()
                print(f"✅ T-Code: ZPACHECK0")
                print(f"   프로그램: {prog_name}")
                
                # 프로그램이 ZPACHECK0이면 직접 실행 불가, 관련 테이블 찾기
                if prog_name == 'ZPACHECK0':
                    print("\n📋 ZPACHECK0 관련 테이블 조회...")
        except:
            print("  ⚠️ ZPACHECK0 T-Code 정보를 찾을 수 없습니다.")
        
        # 2. CE 테이블 조회 (CO-PA 데이터)
        ce_tables = [
            f"CE4{params['ERKRS']}",  # 세그먼트 레벨
            f"CE3{params['ERKRS']}",  # 라인 아이템
            f"CE2{params['ERKRS']}",  # 계획 데이터
            f"CE1{params['ERKRS']}",  # 실적 데이터
            "CE41000",
            "CE31000",
            "CE21000",
            "CE11000",
        ]
        
        data_found = False
        for ce_table in ce_tables:
            try:
                print(f"\n📖 {ce_table} 테이블 조회 시도...")
                
                # 테이블 존재 확인
                result = sap_conn.call('RFC_READ_TABLE',
                                      QUERY_TABLE=ce_table,
                                      DELIMITER='|',
                                      NO_DATA='X')
                
                if result['FIELDS']:
                    print(f"✅ {ce_table} 접근 가능 ({len(result['FIELDS'])}개 필드)")
                    
                    # 버전 필드가 있는지 확인
                    field_names = [f['FIELDNAME'] for f in result['FIELDS']]
                    
                    # 필드 선택
                    selected_fields = []
                    
                    # 기본 필드
                    basic_fields = ['BUKRS', 'GJAHR', 'PERDE', 'VERSN', 'ERKRS', 'PAOBJNR']
                    for field in basic_fields:
                        if field in field_names:
                            selected_fields.append({'FIELDNAME': field})
                    
                    # 특성 필드 (PA로 시작)
                    for field in field_names:
                        if field.startswith('PA') and len(selected_fields) < 20:
                            selected_fields.append({'FIELDNAME': field})
                    
                    # 값 필드 (VV로 시작)
                    for field in field_names:
                        if field.startswith('VV') and len(selected_fields) < 40:
                            selected_fields.append({'FIELDNAME': field})
                    
                    if len(selected_fields) < 5:
                        # 최소한 처음 20개 필드
                        selected_fields = [{'FIELDNAME': f} for f in field_names[:20]]
                    
                    # WHERE 조건
                    options = []
                    if 'BUKRS' in field_names:
                        options.append({'TEXT': f"BUKRS = '{params['BUKRS']}'"})
                    if 'VERSN' in field_names:
                        options.append({'TEXT': f"AND VERSN = '{params['VERSN']}'"})
                    if 'GJAHR' in field_names:
                        options.append({'TEXT': f"AND GJAHR = '{params['GJAHR']}'"})
                    
                    # 데이터 조회
                    print(f"  📝 데이터 조회 중 (버전: {params['VERSN']})...")
                    result_data = sap_conn.call('RFC_READ_TABLE',
                                                QUERY_TABLE=ce_table,
                                                DELIMITER='|',
                                                FIELDS=selected_fields,
                                                OPTIONS=options if options else [],
                                                ROWCOUNT=5000)
                    
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
                        save_to_postgres(df, ce_table, params)
                        data_found = True
                        break
                    else:
                        print(f"  ⚠️ 버전 {params['VERSN']} 데이터가 없습니다.")
                        
            except Exception as e:
                print(f"  ⚠️ {ce_table} 조회 실패: {str(e)[:80]}")
                continue
        
        if not data_found:
            # 대체: COSP/COSS 테이블 (원가센터/원가오브젝트 실적)
            print("\n📖 대체 방법: COSP/COSS 테이블 조회...")
            try:
                # COSP: 원가센터 실적 (기본 계획)
                result = sap_conn.call('RFC_READ_TABLE',
                                      QUERY_TABLE='COSP',
                                      DELIMITER='|',
                                      FIELDS=[
                                          {'FIELDNAME': 'OBJNR'},
                                          {'FIELDNAME': 'GJAHR'},
                                          {'FIELDNAME': 'VERSN'},
                                          {'FIELDNAME': 'KSTAR'},
                                          {'FIELDNAME': 'WKG001'},
                                          {'FIELDNAME': 'WKG002'},
                                          {'FIELDNAME': 'WKG003'},
                                          {'FIELDNAME': 'WKG004'},
                                          {'FIELDNAME': 'WKG005'},
                                          {'FIELDNAME': 'WKG006'},
                                          {'FIELDNAME': 'WKG007'},
                                          {'FIELDNAME': 'WKG008'},
                                          {'FIELDNAME': 'WKG009'},
                                          {'FIELDNAME': 'WKG010'},
                                          {'FIELDNAME': 'WKG011'},
                                          {'FIELDNAME': 'WKG012'},
                                      ],
                                      OPTIONS=[
                                          {'TEXT': f"GJAHR = '{params['GJAHR']}'"},
                                          {'TEXT': f"AND VERSN = '{params['VERSN']}'"},
                                      ],
                                      ROWCOUNT=5000)
                
                if result['DATA']:
                    print(f"✅ COSP에서 {len(result['DATA'])}개 데이터 조회")
                    
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
                else:
                    print(f"  ⚠️ 버전 {params['VERSN']} 데이터가 COSP에도 없습니다.")
                    
            except Exception as e:
                print(f"  ⚠️ COSP 조회 실패: {str(e)[:80]}")
        
        if not data_found:
            print(f"\n⚠️ ZPACHECK0 관련 데이터를 찾을 수 없습니다.")
            print(f"   버전 {params['VERSN']}에 데이터가 없거나 접근 권한이 없을 수 있습니다.")
        
        sap_conn.close()
        print("\n✅ SAP 연결 종료")
        
    except Exception as e:
        print(f"❌ 오류 발생: {e}")

def save_to_postgres(df, table_name, params):
    """데이터를 PostgreSQL에 저장"""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        
        # 테이블 생성
        cursor.execute("DROP TABLE IF EXISTS sap_zpacheck0 CASCADE")
        
        create_table = """
        CREATE TABLE sap_zpacheck0 (
            id SERIAL PRIMARY KEY,
            "회사코드" VARCHAR(4),
            "회계연도" VARCHAR(4),
            "기간" VARCHAR(3),
            "버전" VARCHAR(3),
            "경영단위" VARCHAR(4),
            "오브젝트번호" VARCHAR(22),
            "원본테이블" VARCHAR(30),
            "원본데이터" JSONB,
            "조회일시" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        cursor.execute(create_table)
        
        # 값 필드를 위한 추가 컬럼 생성
        vv_columns = [col for col in df.columns if col.startswith('VV')]
        for vv_col in vv_columns[:20]:  # 최대 20개 값 필드
            cursor.execute(f'ALTER TABLE sap_zpacheck0 ADD COLUMN "{vv_col}" NUMERIC(20,2)')
        
        # 데이터 삽입
        inserted = 0
        for _, row in df.iterrows():
            # 기본 필드
            values = [
                row.get('BUKRS', params['BUKRS']),
                row.get('GJAHR', params['GJAHR']),
                row.get('PERDE', ''),
                row.get('VERSN', params['VERSN']),
                row.get('ERKRS', params['ERKRS']),
                row.get('PAOBJNR', ''),
                table_name,
                json.dumps(dict(row))
            ]
            
            # VV 필드 값 추가
            vv_values = []
            for vv_col in vv_columns[:20]:
                try:
                    vv_values.append(float(row[vv_col]) if row[vv_col] else 0)
                except:
                    vv_values.append(0)
            
            # SQL 생성
            placeholders = ['%s'] * (8 + len(vv_values))
            vv_cols_str = ''.join([f', "{col}"' for col in vv_columns[:20]])
            
            insert_sql = f"""
                INSERT INTO sap_zpacheck0 
                ("회사코드", "회계연도", "기간", "버전", "경영단위", "오브젝트번호", 
                 "원본테이블", "원본데이터"{vv_cols_str})
                VALUES ({', '.join(placeholders)})
            """
            
            cursor.execute(insert_sql, values + vv_values)
            inserted += 1
            
            if inserted % 500 == 0:
                conn.commit()
                print(f"  📝 {inserted}개 저장 중...")
        
        conn.commit()
        
        # 인덱스 생성
        cursor.execute('CREATE INDEX idx_zpacheck0_bukrs ON sap_zpacheck0("회사코드")')
        cursor.execute('CREATE INDEX idx_zpacheck0_versn ON sap_zpacheck0("버전")')
        cursor.execute('CREATE INDEX idx_zpacheck0_gjahr ON sap_zpacheck0("회계연도")')
        
        conn.commit()
        conn.close()
        
        print(f"\n✅ sap_zpacheck0 테이블에 {inserted}개 데이터 저장 완료")
        print(f"   - 테이블: {table_name}")
        print(f"   - 버전: {params['VERSN']}")
        print(f"   - 값 필드: {len(vv_columns)}개")
        
    except Exception as e:
        print(f"❌ PostgreSQL 저장 실패: {e}")

def save_cosp_to_postgres(df, params):
    """COSP 데이터를 PostgreSQL에 저장"""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        
        # 테이블 생성
        cursor.execute("DROP TABLE IF EXISTS sap_zpacheck0_cosp CASCADE")
        
        create_table = """
        CREATE TABLE sap_zpacheck0_cosp (
            id SERIAL PRIMARY KEY,
            "오브젝트번호" VARCHAR(22),
            "회계연도" VARCHAR(4),
            "버전" VARCHAR(3),
            "원가요소" VARCHAR(10),
            "1월" NUMERIC(20,2),
            "2월" NUMERIC(20,2),
            "3월" NUMERIC(20,2),
            "4월" NUMERIC(20,2),
            "5월" NUMERIC(20,2),
            "6월" NUMERIC(20,2),
            "7월" NUMERIC(20,2),
            "8월" NUMERIC(20,2),
            "9월" NUMERIC(20,2),
            "10월" NUMERIC(20,2),
            "11월" NUMERIC(20,2),
            "12월" NUMERIC(20,2),
            "연간합계" NUMERIC(20,2),
            "조회일시" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        cursor.execute(create_table)
        
        # 데이터 삽입
        inserted = 0
        for _, row in df.iterrows():
            # 월별 금액
            monthly = []
            total = 0
            for i in range(1, 13):
                col = f'WKG{i:03d}'
                amount = float(row.get(col, 0)) if row.get(col) else 0
                monthly.append(amount)
                total += amount
            
            cursor.execute("""
                INSERT INTO sap_zpacheck0_cosp 
                ("오브젝트번호", "회계연도", "버전", "원가요소",
                 "1월", "2월", "3월", "4월", "5월", "6월",
                 "7월", "8월", "9월", "10월", "11월", "12월", "연간합계")
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                row.get('OBJNR'),
                row.get('GJAHR'),
                row.get('VERSN'),
                row.get('KSTAR'),
                *monthly,
                total
            ))
            inserted += 1
            
            if inserted % 500 == 0:
                conn.commit()
                print(f"  📝 {inserted}개 저장 중...")
        
        conn.commit()
        conn.close()
        
        print(f"\n✅ sap_zpacheck0_cosp 테이블에 {inserted}개 데이터 저장 완료")
        print(f"   - 버전: {params['VERSN']}")
        print(f"   - 회계연도: {params['GJAHR']}")
        
    except Exception as e:
        print(f"❌ PostgreSQL 저장 실패: {e}")

if __name__ == "__main__":
    print("="*60)
    print("ZPACHECK0 Import 시작")
    print("고정 파라미터:")
    print("  - 회사코드: 1000")
    print("  - 버전: 014")
    print("  - 조회기간: 최근 3개월")
    print("="*60)
    
    # ZPACHECK0 데이터 import
    import_zpacheck0_data()