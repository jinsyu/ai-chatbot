#!/usr/bin/env python3
"""
ZSDR0340 국내영업 매출 분석레포트 Excel Import
Excel 파일을 SQLite 데이터베이스로 가져옵니다.
"""

import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from datetime import datetime
import logging
from typing import Dict, List, Any

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SalesDataImporter:
    def __init__(self, excel_path: str, db_path: str = "db.sqlite"):
        self.excel_path = excel_path
        self.db_path = db_path
        self.engine = None
        self.table_name = 'sap_zsdr0340_sales_detail'
        self.df = None
        
    def connect_database(self):
        """SQLite 데이터베이스 연결"""
        try:
            # SQLite 데이터베이스 연결
            self.engine = create_engine(f'sqlite:///{self.db_path}')
            logger.info(f"✅ SQLite 연결 성공: {self.db_path}")
            
            # 연결 테스트
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT sqlite_version()")).scalar()
                logger.info(f"  SQLite 버전: {result}")
            
            return True
        except Exception as e:
            logger.error(f"❌ SQLite 연결 실패: {e}")
            return False
    
    def analyze_excel(self):
        """Excel 파일 구조 분석"""
        try:
            logger.info(f"📊 Excel 파일 분석 중: {self.excel_path}")
            
            # Excel 파일의 모든 시트 확인
            xl_file = pd.ExcelFile(self.excel_path)
            logger.info(f"시트 목록: {xl_file.sheet_names}")
            
            # 첫 번째 시트 읽기 (또는 특정 시트)
            self.df = pd.read_excel(self.excel_path, sheet_name=0)
            
            logger.info(f"✅ 데이터 로드 완료")
            logger.info(f"  - 행 수: {len(self.df)}")
            logger.info(f"  - 열 수: {len(self.df.columns)}")
            logger.info(f"\n📋 컬럼 목록:")
            for col in self.df.columns:
                dtype = str(self.df[col].dtype)
                null_count = self.df[col].isnull().sum()
                logger.info(f"  - {col}: {dtype} (null: {null_count})")
            
            # 샘플 데이터 출력
            logger.info("\n📋 샘플 데이터 (처음 3행):")
            logger.info(self.df.head(3).to_string())
            
            return True
            
        except Exception as e:
            logger.error(f"Excel 파일 분석 실패: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def clean_column_names(self):
        """컬럼명 정리 및 표준화"""
        try:
            # 컬럼명 정리
            new_columns = []
            column_mapping = {}
            
            for col in self.df.columns:
                # 특수문자 제거 및 언더스코어로 변경
                clean_name = col.strip()
                # 영어 컬럼명으로 매핑 (필요시)
                # 여기서는 한글 그대로 사용
                new_columns.append(clean_name)
                column_mapping[col] = clean_name
            
            self.df.columns = new_columns
            logger.info("✅ 컬럼명 정리 완료")
            
            return column_mapping
            
        except Exception as e:
            logger.error(f"컬럼명 정리 실패: {e}")
            return None
    
    def infer_data_types(self):
        """데이터 타입 추론 및 변환"""
        try:
            logger.info("🔍 데이터 타입 추론 중...")
            
            for col in self.df.columns:
                # 날짜 컬럼 처리 - SQLite는 DATE 타입이 없으므로 문자열로 저장
                if '일' in col or '날짜' in col or 'date' in col.lower():
                    try:
                        # 날짜를 문자열로 변환 (YYYY-MM-DD 형식)
                        self.df[col] = pd.to_datetime(self.df[col], errors='coerce').dt.strftime('%Y-%m-%d')
                        logger.info(f"  {col}: 날짜 형식으로 변환")
                    except:
                        pass
                
                # 숫자 컬럼 처리
                elif self.df[col].dtype == 'object':
                    try:
                        # 콤마 제거 후 숫자 변환 시도
                        temp = self.df[col].astype(str).str.replace(',', '').str.replace(' ', '')
                        if temp.str.match(r'^-?\d+\.?\d*$').all():
                            self.df[col] = pd.to_numeric(temp, errors='coerce')
                            logger.info(f"  {col}: 숫자 타입으로 변환")
                    except:
                        pass
            
            logger.info("✅ 데이터 타입 추론 완료")
            return True
            
        except Exception as e:
            logger.error(f"데이터 타입 추론 실패: {e}")
            return False
    
    def create_table(self):
        """SQLite 테이블 생성"""
        try:
            # 기존 테이블 삭제
            with self.engine.connect() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {self.table_name}"))
                conn.commit()
            
            logger.info(f"📝 {self.table_name} 테이블 생성 중...")
            
            # pandas to_sql을 사용하여 테이블 자동 생성 (SQLite는 동적 타입)
            # 첫 번째 행만 사용하여 스키마 생성
            self.df.head(1).to_sql(
                self.table_name,
                self.engine,
                if_exists='replace',
                index=False
            )
            
            # 테이블 비우기 (스키마만 남기기)
            with self.engine.connect() as conn:
                conn.execute(text(f"DELETE FROM {self.table_name}"))
                conn.commit()
                
                # 인덱스 생성
                # 청구일 인덱스
                if '청구일' in self.df.columns:
                    conn.execute(text(f"CREATE INDEX idx_{self.table_name}_billing_date ON {self.table_name} (청구일)"))
                
                # 판매처 인덱스
                if '판매처' in self.df.columns:
                    conn.execute(text(f"CREATE INDEX idx_{self.table_name}_customer ON {self.table_name} (판매처)"))
                
                # 자재 인덱스
                if '자재' in self.df.columns:
                    conn.execute(text(f"CREATE INDEX idx_{self.table_name}_material ON {self.table_name} (자재)"))
                
                conn.commit()
            
            logger.info(f"✅ 테이블 생성 완료: {self.table_name}")
            return True
            
        except Exception as e:
            logger.error(f"테이블 생성 실패: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def save_to_database(self):
        """데이터를 SQLite에 저장"""
        try:
            logger.info(f"💾 {len(self.df)}개 레코드 저장 중...")
            
            # NaN을 None으로 변환 (PostgreSQL NULL)
            df_to_save = self.df.replace({np.nan: None})
            
            # SQLite는 변수 제한이 있으므로 작은 청크로 나누어 저장
            # 68개 컬럼 * 14행 = 952 변수 (SQLite 제한: 999)
            chunk_size = 14
            total_saved = 0
            
            for i in range(0, len(df_to_save), chunk_size):
                chunk = df_to_save.iloc[i:i+chunk_size]
                
                chunk.to_sql(
                    self.table_name,
                    self.engine,
                    if_exists='append',
                    index=False,
                    method='multi'
                )
                
                total_saved += len(chunk)
                if total_saved % 1000 == 0 or total_saved == len(df_to_save):
                    logger.info(f"  진행: {total_saved}/{len(df_to_save)} 레코드 저장됨")
            
            # 저장 확인
            with self.engine.connect() as conn:
                count_result = conn.execute(
                    text(f"SELECT COUNT(*) FROM {self.table_name}")
                ).scalar()
                
                logger.info(f"✅ 저장 완료: 총 {count_result}개 레코드")
                
                # 샘플 데이터 확인
                sample_result = conn.execute(
                    text(f"SELECT * FROM {self.table_name} LIMIT 3")
                ).fetchall()
                
                if sample_result:
                    logger.info("\n📋 저장된 데이터 샘플:")
                    for row in sample_result:
                        logger.info(f"  {dict(row._mapping)}")
            
            return True
            
        except Exception as e:
            logger.error(f"데이터 저장 실패: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def create_summary_view(self):
        """분석용 뷰 생성"""
        try:
            logger.info("📊 분석용 뷰 생성 중...")
            
            # SQLite는 CREATE OR REPLACE VIEW를 지원하지 않으므로 DROP 후 생성
            with self.engine.connect() as conn:
                try:
                    # 기존 뷰 삭제
                    conn.execute(text("DROP VIEW IF EXISTS v_monthly_sales_summary"))
                    
                    # 월별 매출 집계 뷰 (SQLite 버전)
                    view_sql = f"""
                    CREATE VIEW v_monthly_sales_summary AS
                    SELECT 
                        substr(청구일, 1, 7) as month,
                        COUNT(*) as transaction_count,
                        SUM(청구금액) as total_amount
                    FROM {self.table_name}
                    WHERE 청구일 IS NOT NULL
                    GROUP BY substr(청구일, 1, 7)
                    ORDER BY month DESC;
                    """
                    
                    conn.execute(text(view_sql))
                    conn.commit()
                    logger.info("✅ 월별 매출 집계 뷰 생성 완료")
                except Exception as e:
                    logger.warning(f"뷰 생성 실패: {e}")
            
            return True
            
        except Exception as e:
            logger.error(f"뷰 생성 실패: {e}")
            return False
    
    def run(self):
        """전체 프로세스 실행"""
        try:
            logger.info("=" * 60)
            logger.info("ZSDR0340 매출 데이터 Import 시작")
            logger.info("=" * 60)
            
            # SQLite 연결
            if not self.connect_database():
                return False
            
            # Excel 파일 분석
            if not self.analyze_excel():
                return False
            
            # 컬럼명 정리
            self.clean_column_names()
            
            # 데이터 타입 추론
            self.infer_data_types()
            
            # 테이블 생성
            if not self.create_table():
                return False
            
            # 데이터 저장
            if not self.save_to_database():
                return False
            
            # 분석 뷰 생성
            self.create_summary_view()
            
            logger.info("\n" + "=" * 60)
            logger.info("✅ Import 완료!")
            logger.info(f"테이블명: {self.table_name}")
            logger.info(f"총 레코드: {len(self.df)}")
            logger.info("=" * 60)
            
            return True
            
        except Exception as e:
            logger.error(f"프로세스 실행 실패: {e}")
            import traceback
            traceback.print_exc()
            return False

if __name__ == "__main__":
    excel_file = "/Users/jinsyu/Downloads/ZSDR0340_국내영업_매출_분석레포트.XLSX"
    db_file = "db.sqlite"  # SQLite 데이터베이스 파일
    
    if not os.path.exists(excel_file):
        logger.error(f"파일을 찾을 수 없습니다: {excel_file}")
    else:
        importer = SalesDataImporter(excel_file, db_file)
        importer.run()