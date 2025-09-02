#!/usr/bin/env python3
"""
제상품재고현황 Excel 파일을 PostgreSQL 데이터베이스에 import하는 스크립트
"""

import pandas as pd
import psycopg2
from psycopg2 import sql
import os
from dotenv import load_dotenv
from datetime import datetime

# 환경변수 로드
load_dotenv()

# 데이터베이스 연결 정보
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://baseone:baseone@localhost:10003/baseone')

def create_table(conn):
    """재고 현황 테이블 생성"""
    cursor = conn.cursor()
    
    # 기존 테이블이 있으면 삭제 (옵션)
    cursor.execute("DROP TABLE IF EXISTS inventory_status CASCADE")
    
    # 테이블 생성
    create_table_query = """
    CREATE TABLE inventory_status (
        id SERIAL PRIMARY KEY,
        material_code BIGINT,                    -- 자재
        material_name TEXT,                      -- 자재명
        material_type TEXT,                      -- 자재 유형
        product_group_name TEXT,                 -- 제품군명
        material_type_name TEXT,                 -- 자재유형명
        supplier TEXT,                           -- 공급업체
        inventory_category TEXT,                 -- 재고구분명
        total_inventory_qty INTEGER,             -- 총재고수량
        inventory_amount NUMERIC(20, 0),         -- 재고금액
        yul_jeong INTEGER,                       -- 율정
        yul_jeong_inventory_amount NUMERIC(20, 0), -- 율정재고금액
        available_inventory_qty INTEGER,         -- 가용재고수량
        available_inventory_amount NUMERIC(20, 0), -- 가용재고금액
        grade_b INTEGER,                         -- B등급
        grade_c INTEGER,                         -- C등급
        grade_d INTEGER,                         -- D등급
        grade_inventory_amount NUMERIC(20, 0),   -- 등급재고금액
        consignment_inventory INTEGER,           -- 위탁재고
        cy_inventory INTEGER,                    -- CY재고
        hold_inventory INTEGER,                  -- 보류재고
        production_rework INTEGER,               -- 생산재작업
        other_inventory_amount NUMERIC(20, 0),   -- 기타재고금액
        avg_sales_qty_6m NUMERIC(15, 2),        -- 6개월평균판매수량
        avg_sales_amount_6m NUMERIC(20, 0),     -- 6개월평균판매금액
        lt3m_qty NUMERIC(15, 2),                -- LT3M(수량)
        lt6m_qty NUMERIC(15, 2),                -- LT6M(수량)
        lt12m_qty NUMERIC(15, 2),               -- LT12M(수량)
        lt24m_qty NUMERIC(15, 2),               -- LT24M(수량)
        mt24m_qty NUMERIC(15, 2),               -- MT24M(수량)
        lt3m_amount NUMERIC(20, 0),             -- LT3M(금액)
        lt6m_amount NUMERIC(20, 0),             -- LT6M(금액)
        lt12m_amount NUMERIC(20, 0),            -- LT12M(금액)
        lt24m_amount NUMERIC(20, 0),            -- LT24M(금액)
        mt24m_amount NUMERIC(20, 0),            -- MT24M(금액)
        plant_material_status NUMERIC(10, 2),    -- 플랜트 자재상태
        inventory_type NUMERIC(10, 2),          -- 재고구분
        volume NUMERIC(15, 2),                  -- 볼륨
        material_group5_domestic TEXT,           -- 자재그룹5내역(내수)
        material_group5_export TEXT,             -- 자재그룹5내역(수출)
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """
    
    cursor.execute(create_table_query)
    
    # 인덱스 생성
    cursor.execute("CREATE INDEX idx_inventory_material_code ON inventory_status(material_code)")
    cursor.execute("CREATE INDEX idx_inventory_material_name ON inventory_status(material_name)")
    cursor.execute("CREATE INDEX idx_inventory_supplier ON inventory_status(supplier)")
    cursor.execute("CREATE INDEX idx_inventory_product_group ON inventory_status(product_group_name)")
    
    conn.commit()
    print("✅ 테이블 'inventory_status' 생성 완료")

def import_data(conn, excel_file):
    """Excel 데이터를 데이터베이스에 import"""
    
    # Excel 파일 읽기
    print(f"📖 Excel 파일 읽기: {excel_file}")
    df = pd.read_excel(excel_file)
    
    # 컬럼명 매핑
    column_mapping = {
        '자재': 'material_code',
        '자재명': 'material_name',
        '자재 유형': 'material_type',
        '제품군명': 'product_group_name',
        '자재유형명': 'material_type_name',
        '공급업체': 'supplier',
        '재고구분명': 'inventory_category',
        '총재고수량': 'total_inventory_qty',
        '재고금액': 'inventory_amount',
        '율정': 'yul_jeong',
        '율정재고금액': 'yul_jeong_inventory_amount',
        '가용재고수량': 'available_inventory_qty',
        '가용재고금액': 'available_inventory_amount',
        'B등급': 'grade_b',
        'C등급': 'grade_c',
        'D등급': 'grade_d',
        '등급재고금액': 'grade_inventory_amount',
        '위탁재고': 'consignment_inventory',
        'CY재고': 'cy_inventory',
        '보류재고': 'hold_inventory',
        '생산재작업': 'production_rework',
        '기타재고금액': 'other_inventory_amount',
        '6개월평균판매수량': 'avg_sales_qty_6m',
        '6개월평균판매금액': 'avg_sales_amount_6m',
        'LT3M(수량)': 'lt3m_qty',
        'LT6M(수량)': 'lt6m_qty',
        'LT12M(수량)': 'lt12m_qty',
        'LT24M(수량)': 'lt24m_qty',
        'MT24M(수량)': 'mt24m_qty',
        'LT3M(금액)': 'lt3m_amount',
        'LT6M(금액)': 'lt6m_amount',
        'LT12M(금액)': 'lt12m_amount',
        'LT24M(금액)': 'lt24m_amount',
        'MT24M(금액)': 'mt24m_amount',
        '플랜트 자재상태': 'plant_material_status',
        '재고구분': 'inventory_type',
        '볼륨': 'volume',
        '자재그룹5내역(내수)': 'material_group5_domestic',
        '자재그룹5내역(수출)': 'material_group5_export'
    }
    
    # 컬럼명 변경
    df_renamed = df.rename(columns=column_mapping)
    
    # NaN 값을 None으로 변환
    df_renamed = df_renamed.where(pd.notnull(df_renamed), None)
    
    cursor = conn.cursor()
    
    # INSERT 쿼리 준비
    columns = list(column_mapping.values())
    insert_query = sql.SQL("""
        INSERT INTO inventory_status ({})
        VALUES ({})
    """).format(
        sql.SQL(', ').join(map(sql.Identifier, columns)),
        sql.SQL(', ').join(sql.Placeholder() * len(columns))
    )
    
    # 데이터 삽입
    inserted_count = 0
    failed_count = 0
    for idx, row in df_renamed.iterrows():
        # NaN인 자재코드는 건너뛰기
        if pd.isna(row.get('material_code')):
            print(f"⏭️ 행 {idx} 건너뜀: 자재코드가 없음")
            continue
            
        values = [row.get(col) for col in columns]
        try:
            cursor.execute(insert_query, values)
            conn.commit()  # 각 행마다 commit
            inserted_count += 1
            if inserted_count % 100 == 0:
                print(f"  📝 {inserted_count}개 행 처리 중...")
        except Exception as e:
            failed_count += 1
            conn.rollback()  # 오류 발생 시 해당 행만 롤백
            if failed_count <= 5:  # 처음 5개 오류만 출력
                print(f"⚠️ 행 {idx} 삽입 실패: {e}")
                print(f"   자재코드: {row.get('material_code')}, 자재명: {row.get('material_name')}")
            continue
    print(f"✅ {inserted_count}개 행 삽입 완료")
    
    return inserted_count

def verify_import(conn):
    """데이터 import 검증"""
    cursor = conn.cursor()
    
    # 총 행 수 확인
    cursor.execute("SELECT COUNT(*) FROM inventory_status")
    total_rows = cursor.fetchone()[0]
    
    # 주요 통계 확인
    cursor.execute("""
        SELECT 
            COUNT(DISTINCT material_code) as unique_materials,
            COUNT(DISTINCT supplier) as unique_suppliers,
            COUNT(DISTINCT product_group_name) as unique_product_groups,
            SUM(total_inventory_qty) as total_qty,
            SUM(inventory_amount) as total_amount
        FROM inventory_status
    """)
    stats = cursor.fetchone()
    
    print("\n📊 Import 결과:")
    print(f"  - 총 행 수: {total_rows:,}")
    print(f"  - 고유 자재 수: {stats[0]:,}" if stats[0] else "  - 고유 자재 수: 0")
    print(f"  - 고유 공급업체 수: {stats[1]:,}" if stats[1] else "  - 고유 공급업체 수: 0")
    print(f"  - 고유 제품군 수: {stats[2]:,}" if stats[2] else "  - 고유 제품군 수: 0")
    print(f"  - 총 재고 수량: {stats[3]:,}" if stats[3] else "  - 총 재고 수량: 0")
    print(f"  - 총 재고 금액: {float(stats[4]):,.0f}원" if stats[4] else "  - 총 재고 금액: 0원")
    
    # 샘플 데이터 확인
    cursor.execute("""
        SELECT material_code, material_name, total_inventory_qty, inventory_amount
        FROM inventory_status
        WHERE inventory_amount > 0
        ORDER BY inventory_amount DESC
        LIMIT 5
    """)
    
    print("\n📈 상위 5개 재고 (금액 기준):")
    for row in cursor.fetchall():
        material_code = row[0] if row[0] else "N/A"
        material_name = row[1] if row[1] else "N/A"
        qty = row[2] if row[2] else 0
        amount = float(row[3]) if row[3] else 0
        print(f"  - [{material_code}] {material_name}: 수량 {qty:,}, 금액 {amount:,.0f}원")

def main():
    excel_file = '/Users/jinsyu/Downloads/제상품재고현황_ZMMR0016.XLSX'
    
    if not os.path.exists(excel_file):
        print(f"❌ 파일을 찾을 수 없습니다: {excel_file}")
        return
    
    try:
        # 데이터베이스 연결
        print(f"🔗 데이터베이스 연결 중...")
        conn = psycopg2.connect(DATABASE_URL)
        print("✅ 데이터베이스 연결 성공")
        
        # 테이블 생성
        create_table(conn)
        
        # 데이터 import
        imported_count = import_data(conn, excel_file)
        
        # 검증
        if imported_count > 0:
            verify_import(conn)
        
        # 연결 종료
        conn.close()
        print("\n✅ 작업 완료!")
        
    except Exception as e:
        print(f"❌ 오류 발생: {e}")
        if 'conn' in locals():
            conn.rollback()
            conn.close()

if __name__ == "__main__":
    main()