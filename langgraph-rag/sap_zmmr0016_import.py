#!/usr/bin/env python3
"""
SAP T-Code ZMMR0016 재고현황 데이터 import
MARD, MARA, MAKT, MBEW 테이블 조인하여 종합 재고 현황 생성
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
    'lang': 'KO',
}

def import_zmmr0016_data(max_rows=10000):
    """ZMMR0016 형태의 재고 현황 데이터 import"""
    
    print("="*60)
    print("ZMMR0016 재고현황 Import")
    print("="*60)
    
    try:
        # SAP 연결
        sap_conn = Connection(**SAP_CONFIG)
        print("✅ SAP 연결 성공")
        
        # 1. MARD - 재고 데이터 조회
        print(f"\n📖 재고 데이터 조회 중 (최대 {max_rows}개)...")
        result_mard = sap_conn.call('RFC_READ_TABLE',
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
                                        {'FIELDNAME': 'RETME'},  # 반품재고
                                        {'FIELDNAME': 'KLABS'},  # 미제한재고(누계)
                                        {'FIELDNAME': 'KINSM'},  # 품질재고(누계)
                                        {'FIELDNAME': 'KSPEM'},  # 블록재고(누계)
                                    ],
                                    OPTIONS=[
                                        {'TEXT': "LABST > 0 OR INSME > 0 OR SPEME > 0"},  # 재고가 있는 것만
                                    ],
                                    ROWCOUNT=max_rows)
        
        if not result_mard['DATA']:
            print("❌ 재고 데이터가 없습니다.")
            return
        
        print(f"✅ {len(result_mard['DATA'])}개 재고 데이터 조회 완료")
        
        # DataFrame 생성
        field_names = [f['FIELDNAME'] for f in result_mard['FIELDS']]
        data = []
        for row in result_mard['DATA']:
            values = row['WA'].split('|')
            values = [v.strip() for v in values]
            data.append(dict(zip(field_names, values)))
        
        df_mard = pd.DataFrame(data)
        
        # 2. 자재별 추가 정보 조회
        print("\n📖 자재 마스터 및 가격 정보 조회 중...")
        
        # 고유 자재 목록
        unique_materials = df_mard['MATNR'].unique()[:100]  # 처음 100개만
        
        # MARA 정보
        mara_data = []
        for i, matnr in enumerate(unique_materials):
            if i % 20 == 0:
                print(f"  처리중... {i}/{len(unique_materials)}")
            
            try:
                result = sap_conn.call('RFC_READ_TABLE',
                                      QUERY_TABLE='MARA',
                                      DELIMITER='|',
                                      FIELDS=[
                                          {'FIELDNAME': 'MATNR'},
                                          {'FIELDNAME': 'MTART'},  # 자재유형
                                          {'FIELDNAME': 'MATKL'},  # 자재그룹
                                          {'FIELDNAME': 'MEINS'},  # 기본단위
                                          {'FIELDNAME': 'BRGEW'},  # 총중량
                                          {'FIELDNAME': 'NTGEW'},  # 순중량
                                      ],
                                      OPTIONS=[
                                          {'TEXT': f"MATNR = '{matnr}'"},
                                      ])
                
                if result['DATA']:
                    values = result['DATA'][0]['WA'].split('|')
                    mara_data.append({
                        'MATNR': values[0].strip(),
                        'MTART': values[1].strip(),
                        'MATKL': values[2].strip(),
                        'MEINS': values[3].strip(),
                        'BRGEW': values[4].strip(),
                        'NTGEW': values[5].strip(),
                    })
            except:
                continue
        
        df_mara = pd.DataFrame(mara_data) if mara_data else pd.DataFrame()
        
        # MAKT 정보 (한글 자재명)
        makt_data = []
        for matnr in unique_materials[:50]:  # 처음 50개만
            try:
                result = sap_conn.call('RFC_READ_TABLE',
                                      QUERY_TABLE='MAKT',
                                      DELIMITER='|',
                                      FIELDS=[
                                          {'FIELDNAME': 'MATNR'},
                                          {'FIELDNAME': 'MAKTX'},
                                      ],
                                      OPTIONS=[
                                          {'TEXT': f"MATNR = '{matnr}' AND SPRAS = '3'"},
                                      ])
                
                if result['DATA']:
                    values = result['DATA'][0]['WA'].split('|')
                    makt_data.append({
                        'MATNR': values[0].strip(),
                        'MAKTX': values[1].strip(),
                    })
            except:
                continue
        
        df_makt = pd.DataFrame(makt_data) if makt_data else pd.DataFrame()
        
        print(f"✅ {len(df_mara)}개 자재 마스터, {len(df_makt)}개 자재명 조회 완료")
        
        # 3. 데이터 병합
        print("\n🔗 데이터 병합 중...")
        
        # MARD + MARA 조인
        if not df_mara.empty:
            df_result = pd.merge(df_mard, df_mara, on='MATNR', how='left')
        else:
            df_result = df_mard
        
        # + MAKT 조인
        if not df_makt.empty:
            df_result = pd.merge(df_result, df_makt, on='MATNR', how='left')
        
        # 4. PostgreSQL 저장
        print("\n💾 PostgreSQL 저장 중...")
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        
        # 테이블 생성
        cursor.execute("DROP TABLE IF EXISTS sap_zmmr0016_inventory CASCADE")
        
        create_table = """
        CREATE TABLE sap_zmmr0016_inventory (
            id SERIAL PRIMARY KEY,
            "자재번호" VARCHAR(18),
            "자재명" TEXT,
            "자재유형" VARCHAR(4),
            "자재그룹" VARCHAR(9),
            "플랜트" VARCHAR(4),
            "저장위치" VARCHAR(4),
            "가용재고" NUMERIC(15,3),
            "이동중재고" NUMERIC(15,3),
            "품질검사재고" NUMERIC(15,3),
            "제한재고" NUMERIC(15,3),
            "블록재고" NUMERIC(15,3),
            "반품재고" NUMERIC(15,3),
            "기본단위" VARCHAR(3),
            "총중량" NUMERIC(15,3),
            "순중량" NUMERIC(15,3),
            "생성일시" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        cursor.execute(create_table)
        
        # 데이터 삽입
        inserted = 0
        for _, row in df_result.iterrows():
            try:
                cursor.execute("""
                    INSERT INTO sap_zmmr0016_inventory 
                    ("자재번호", "자재명", "자재유형", "자재그룹", "플랜트", "저장위치",
                     "가용재고", "이동중재고", "품질검사재고", "제한재고", "블록재고", "반품재고",
                     "기본단위", "총중량", "순중량")
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    row.get('MATNR'),
                    row.get('MAKTX'),
                    row.get('MTART'),
                    row.get('MATKL'),
                    row.get('WERKS'),
                    row.get('LGORT'),
                    float(row.get('LABST', 0)) if row.get('LABST') else 0,
                    float(row.get('UMLME', 0)) if row.get('UMLME') else 0,
                    float(row.get('INSME', 0)) if row.get('INSME') else 0,
                    float(row.get('EINME', 0)) if row.get('EINME') else 0,
                    float(row.get('SPEME', 0)) if row.get('SPEME') else 0,
                    float(row.get('RETME', 0)) if row.get('RETME') else 0,
                    row.get('MEINS'),
                    float(row.get('BRGEW', 0)) if row.get('BRGEW') else 0,
                    float(row.get('NTGEW', 0)) if row.get('NTGEW') else 0,
                ))
                inserted += 1
                
                if inserted % 100 == 0:
                    conn.commit()
                    print(f"  📝 {inserted}개 저장 중...")
            except Exception as e:
                if inserted == 0:
                    print(f"  ⚠️ 삽입 오류: {e}")
                continue
        
        conn.commit()
        
        # 인덱스 생성
        cursor.execute('CREATE INDEX idx_zmmr0016_matnr ON sap_zmmr0016_inventory("자재번호")')
        cursor.execute('CREATE INDEX idx_zmmr0016_werks ON sap_zmmr0016_inventory("플랜트")')
        cursor.execute('CREATE INDEX idx_zmmr0016_lgort ON sap_zmmr0016_inventory("저장위치")')
        
        conn.commit()
        conn.close()
        
        print(f"\n✅ 총 {inserted}개 재고 데이터 저장 완료!")
        
        # 결과 요약
        print("\n📊 저장된 데이터 요약:")
        print(f"  - 총 행 수: {inserted}")
        print(f"  - 고유 자재: {df_result['MATNR'].nunique()}")
        print(f"  - 플랜트: {df_result['WERKS'].unique()[:5].tolist()}")
        
        sap_conn.close()
        
    except Exception as e:
        print(f"❌ 오류 발생: {e}")

if __name__ == "__main__":
    # ZMMR0016 재고현황 import
    import_zmmr0016_data(max_rows=5000)