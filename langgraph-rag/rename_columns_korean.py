#!/usr/bin/env python3
"""
inventory_status 테이블의 컬럼명을 한글로 변경하는 스크립트
"""

import psycopg2
import os
from dotenv import load_dotenv

# 환경변수 로드
load_dotenv()

# 데이터베이스 연결 정보
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://baseone:baseone@localhost:10003/baseone')

def rename_columns_to_korean(conn):
    """테이블 컬럼명을 한글로 변경"""
    cursor = conn.cursor()
    
    print("🔄 inventory_status 테이블 컬럼명을 한글로 변경 중...")
    
    # 기존 테이블 백업 (선택사항)
    cursor.execute("DROP TABLE IF EXISTS inventory_status_backup")
    cursor.execute("CREATE TABLE inventory_status_backup AS SELECT * FROM inventory_status")
    print("✅ 백업 테이블 생성 완료")
    
    # 새로운 한글 컬럼명을 가진 테이블 생성
    create_table_query = """
    CREATE TABLE inventory_status_korean (
        id SERIAL PRIMARY KEY,
        "자재" BIGINT,
        "자재명" TEXT,
        "자재유형" TEXT,
        "제품군명" TEXT,
        "자재유형명" TEXT,
        "공급업체" TEXT,
        "재고구분명" TEXT,
        "총재고수량" INTEGER,
        "재고금액" NUMERIC(20, 0),
        "율정" INTEGER,
        "율정재고금액" NUMERIC(20, 0),
        "가용재고수량" INTEGER,
        "가용재고금액" NUMERIC(20, 0),
        "B등급" INTEGER,
        "C등급" INTEGER,
        "D등급" INTEGER,
        "등급재고금액" NUMERIC(20, 0),
        "위탁재고" INTEGER,
        "CY재고" INTEGER,
        "보류재고" INTEGER,
        "생산재작업" INTEGER,
        "기타재고금액" NUMERIC(20, 0),
        "6개월평균판매수량" NUMERIC(15, 2),
        "6개월평균판매금액" NUMERIC(20, 0),
        "LT3M수량" NUMERIC(15, 2),
        "LT6M수량" NUMERIC(15, 2),
        "LT12M수량" NUMERIC(15, 2),
        "LT24M수량" NUMERIC(15, 2),
        "MT24M수량" NUMERIC(15, 2),
        "LT3M금액" NUMERIC(20, 0),
        "LT6M금액" NUMERIC(20, 0),
        "LT12M금액" NUMERIC(20, 0),
        "LT24M금액" NUMERIC(20, 0),
        "MT24M금액" NUMERIC(20, 0),
        "플랜트자재상태" NUMERIC(10, 2),
        "재고구분" NUMERIC(10, 2),
        "볼륨" NUMERIC(15, 2),
        "자재그룹5내역_내수" TEXT,
        "자재그룹5내역_수출" TEXT,
        "생성일시" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        "수정일시" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """
    
    cursor.execute("DROP TABLE IF EXISTS inventory_status_korean")
    cursor.execute(create_table_query)
    print("✅ 한글 컬럼명 테이블 생성 완료")
    
    # 데이터 복사
    insert_query = """
    INSERT INTO inventory_status_korean (
        "자재", "자재명", "자재유형", "제품군명", "자재유형명", "공급업체", "재고구분명",
        "총재고수량", "재고금액", "율정", "율정재고금액", "가용재고수량", "가용재고금액",
        "B등급", "C등급", "D등급", "등급재고금액", "위탁재고", "CY재고", "보류재고",
        "생산재작업", "기타재고금액", "6개월평균판매수량", "6개월평균판매금액",
        "LT3M수량", "LT6M수량", "LT12M수량", "LT24M수량", "MT24M수량",
        "LT3M금액", "LT6M금액", "LT12M금액", "LT24M금액", "MT24M금액",
        "플랜트자재상태", "재고구분", "볼륨", "자재그룹5내역_내수", "자재그룹5내역_수출",
        "생성일시", "수정일시"
    )
    SELECT 
        material_code, material_name, material_type, product_group_name, material_type_name,
        supplier, inventory_category, total_inventory_qty, inventory_amount, yul_jeong,
        yul_jeong_inventory_amount, available_inventory_qty, available_inventory_amount,
        grade_b, grade_c, grade_d, grade_inventory_amount, consignment_inventory,
        cy_inventory, hold_inventory, production_rework, other_inventory_amount,
        avg_sales_qty_6m, avg_sales_amount_6m, lt3m_qty, lt6m_qty, lt12m_qty, lt24m_qty, mt24m_qty,
        lt3m_amount, lt6m_amount, lt12m_amount, lt24m_amount, mt24m_amount,
        plant_material_status, inventory_type, volume, material_group5_domestic, material_group5_export,
        created_at, updated_at
    FROM inventory_status
    """
    
    cursor.execute(insert_query)
    print(f"✅ {cursor.rowcount}개 행 복사 완료")
    
    # 기존 테이블 삭제하고 새 테이블 이름 변경
    cursor.execute("DROP TABLE inventory_status")
    cursor.execute("ALTER TABLE inventory_status_korean RENAME TO inventory_status")
    print("✅ 테이블 이름 변경 완료")
    
    # 인덱스 재생성
    cursor.execute('CREATE INDEX idx_inventory_material_code ON inventory_status("자재")')
    cursor.execute('CREATE INDEX idx_inventory_material_name ON inventory_status("자재명")')
    cursor.execute('CREATE INDEX idx_inventory_supplier ON inventory_status("공급업체")')
    cursor.execute('CREATE INDEX idx_inventory_product_group ON inventory_status("제품군명")')
    print("✅ 인덱스 재생성 완료")
    
    conn.commit()

def verify_korean_columns(conn):
    """한글 컬럼명 확인"""
    cursor = conn.cursor()
    
    # 컬럼명 조회
    cursor.execute("""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = 'inventory_status' 
        ORDER BY ordinal_position
    """)
    
    print("\n📋 inventory_status 테이블 컬럼 목록:")
    for col_name, data_type in cursor.fetchall():
        print(f"  - {col_name} ({data_type})")
    
    # 샘플 데이터 확인
    cursor.execute("""
        SELECT "자재", "자재명", "총재고수량", "재고금액"
        FROM inventory_status
        LIMIT 5
    """)
    
    print("\n📊 샘플 데이터:")
    for row in cursor.fetchall():
        print(f"  자재: {row[0]}, 자재명: {row[1]}, 수량: {row[2]}, 금액: {row[3]}")

def main():
    try:
        # 데이터베이스 연결
        print("🔗 데이터베이스 연결 중...")
        conn = psycopg2.connect(DATABASE_URL)
        print("✅ 데이터베이스 연결 성공")
        
        # 컬럼명 변경
        rename_columns_to_korean(conn)
        
        # 검증
        verify_korean_columns(conn)
        
        # 연결 종료
        conn.close()
        print("\n✅ 작업 완료! 모든 컬럼명이 한글로 변경되었습니다.")
        
    except Exception as e:
        print(f"❌ 오류 발생: {e}")
        if 'conn' in locals():
            conn.rollback()
            conn.close()

if __name__ == "__main__":
    main()