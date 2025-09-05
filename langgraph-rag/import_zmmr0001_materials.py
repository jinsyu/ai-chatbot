#!/usr/bin/env python3
"""
ZMMR0001 자재리스트 Excel Import
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

class MaterialsDataImporter:
    def __init__(self, excel_path: str, db_path: str = "db.sqlite"):
        self.excel_path = excel_path
        self.db_path = db_path
        self.engine = None
        self.table_name = 'sap_zmmr0001_materials'
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
            
            # 첫 번째 시트 읽기 (처음 몇 행만 먼저 확인)
            df_sample = pd.read_excel(self.excel_path, sheet_name=0, nrows=5)
            logger.info(f"\n📋 샘플 데이터 (처음 5행):")
            logger.info(df_sample.to_string())
            
            # 전체 데이터 읽기
            self.df = pd.read_excel(self.excel_path, sheet_name=0)
            
            # 마지막 행이 합계 행인지 확인
            last_row = self.df.iloc[-1]
            # 첫 번째 컬럼이 비어있거나 자재명에 '합계'가 포함되어 있는지 확인
            if pd.isna(last_row.iloc[0]) or (isinstance(last_row.iloc[1], str) and '합계' in str(last_row.iloc[1]).lower()):
                logger.info("  ⚠️ 마지막 행이 합계 행으로 보입니다. 제외합니다.")
                self.df = self.df[:-1]  # 마지막 행 제외
            
            logger.info(f"\n✅ 데이터 로드 완료")
            logger.info(f"  - 행 수: {len(self.df)}")
            logger.info(f"  - 열 수: {len(self.df.columns)}")
            
            # 컬럼 정보 출력
            logger.info(f"\n📋 컬럼 정보:")
            for col in self.df.columns:
                dtype = str(self.df[col].dtype)
                null_count = self.df[col].isnull().sum()
                unique_count = self.df[col].nunique()
                
                # 샘플 값 (처음 non-null 값)
                sample_val = None
                for val in self.df[col]:
                    if pd.notna(val):
                        sample_val = val
                        break
                
                logger.info(f"  - {col}: {dtype}")
                logger.info(f"    null: {null_count}, unique: {unique_count}, sample: {sample_val}")
            
            return True
            
        except Exception as e:
            logger.error(f"Excel 파일 분석 실패: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def clean_and_prepare_data(self):
        """데이터 정리 및 준비"""
        try:
            logger.info("🧹 데이터 정리 중...")
            
            # 컬럼명 정리 (공백 제거)
            self.df.columns = [col.strip() for col in self.df.columns]
            
            # 데이터 타입 추론 및 변환
            for col in self.df.columns:
                # 날짜 컬럼 처리
                if '일' in col or '날짜' in col or 'date' in col.lower():
                    try:
                        # 날짜를 문자열로 변환 (SQLite는 DATE 타입이 없음)
                        self.df[col] = pd.to_datetime(self.df[col], errors='coerce').dt.strftime('%Y-%m-%d')
                        logger.info(f"  {col}: 날짜 형식으로 변환")
                    except:
                        pass
                
                # 숫자로 보이는 텍스트 컬럼 처리
                elif self.df[col].dtype == 'object':
                    # 샘플 값 확인
                    sample = self.df[col].dropna().head()
                    if len(sample) > 0:
                        # 숫자로 변환 가능한지 확인
                        try:
                            # 콤마 제거 후 숫자 변환 시도
                            temp = self.df[col].astype(str).str.replace(',', '').str.replace(' ', '')
                            if temp.str.match(r'^-?\d+\.?\d*$').any():
                                # 일부라도 숫자면 숫자로 변환 시도
                                self.df[col] = pd.to_numeric(temp, errors='coerce')
                                logger.info(f"  {col}: 숫자 형식으로 변환")
                        except:
                            pass
            
            # NaN을 None으로 변환 (SQLite NULL)
            self.df = self.df.replace({np.nan: None})
            
            logger.info("✅ 데이터 정리 완료")
            return True
            
        except Exception as e:
            logger.error(f"데이터 정리 실패: {e}")
            return False
    
    def create_table(self):
        """SQLite 테이블 생성"""
        try:
            logger.info(f"📝 {self.table_name} 테이블 생성 중...")
            
            # 기존 테이블 삭제
            with self.engine.connect() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {self.table_name}"))
                conn.commit()
            
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
                
                # 인덱스 생성 (주요 컬럼에 대해)
                try:
                    # 컬럼명에 따라 인덱스 생성
                    for col in self.df.columns:
                        if '자재' in col and ('번호' in col or '코드' in col):
                            conn.execute(text(f"CREATE INDEX idx_{self.table_name}_material ON {self.table_name} (`{col}`)"))
                            logger.info(f"  인덱스 생성: 자재번호/코드")
                        elif '플랜트' in col:
                            conn.execute(text(f"CREATE INDEX idx_{self.table_name}_plant ON {self.table_name} (`{col}`)"))
                            logger.info(f"  인덱스 생성: 플랜트")
                        elif '자재그룹' in col and '1' in col:  # 자재그룹1 같은 경우
                            conn.execute(text(f"CREATE INDEX idx_{self.table_name}_group ON {self.table_name} (`{col}`)"))
                            logger.info(f"  인덱스 생성: 자재그룹")
                    conn.commit()
                except Exception as e:
                    logger.warning(f"인덱스 생성 실패: {e}")
            
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
            
            # SQLite는 변수 제한이 있으므로 작은 청크로 나누어 저장
            # 컬럼 수에 따라 청크 크기 결정
            col_count = len(self.df.columns)
            max_vars = 900  # SQLite 제한보다 작게 설정 (안전 마진)
            chunk_size = max_vars // col_count
            chunk_size = max(1, min(chunk_size, 100))  # 1-100 사이로 제한
            
            logger.info(f"  청크 크기: {chunk_size} (컬럼 수: {col_count})")
            
            total_saved = 0
            
            for i in range(0, len(self.df), chunk_size):
                chunk = self.df.iloc[i:i+chunk_size]
                
                chunk.to_sql(
                    self.table_name,
                    self.engine,
                    if_exists='append',
                    index=False,
                    method='multi'
                )
                
                total_saved += len(chunk)
                # 진행 상황 출력 (1000개마다 또는 마지막)
                if total_saved % 1000 == 0 or total_saved == len(self.df):
                    logger.info(f"  진행: {total_saved}/{len(self.df)} 레코드 저장됨")
            
            # 저장 확인
            with self.engine.connect() as conn:
                count_result = conn.execute(
                    text(f"SELECT COUNT(*) FROM {self.table_name}")
                ).scalar()
                
                logger.info(f"✅ 저장 완료: 총 {count_result}개 레코드")
                
                # 샘플 데이터 확인 (처음 3개)
                sample_query = f"SELECT * FROM {self.table_name} LIMIT 3"
                sample_result = conn.execute(text(sample_query)).fetchall()
                
                if sample_result:
                    logger.info("\n📋 저장된 데이터 샘플:")
                    for i, row in enumerate(sample_result, 1):
                        logger.info(f"  레코드 {i}:")
                        row_dict = dict(row._mapping)
                        # 주요 컬럼만 출력 (너무 많으면 처음 5개)
                        for j, (k, v) in enumerate(row_dict.items()):
                            if j < 5:
                                logger.info(f"    {k}: {v}")
                        if len(row_dict) > 5:
                            logger.info(f"    ... 외 {len(row_dict) - 5}개 컬럼")
            
            return True
            
        except Exception as e:
            logger.error(f"데이터 저장 실패: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def create_summary_stats(self):
        """요약 통계 생성"""
        try:
            logger.info("📊 요약 통계 생성 중...")
            
            with self.engine.connect() as conn:
                # 전체 요약
                total_query = f"SELECT COUNT(*) as total FROM {self.table_name}"
                result = conn.execute(text(total_query)).scalar()
                logger.info(f"\n📈 전체 요약:")
                logger.info(f"  총 레코드수: {result:,}")
                
                # 테이블 스키마 정보
                schema_query = f"PRAGMA table_info({self.table_name})"
                schema_result = conn.execute(text(schema_query)).fetchall()
                logger.info(f"  컬럼 수: {len(schema_result)}")
                
                # 주요 컬럼별 통계 (있는 경우)
                for col_info in schema_result:
                    col_name = col_info[1]
                    
                    # 자재 관련 컬럼이면 고유값 수 계산
                    if '자재' in col_name or '플랜트' in col_name:
                        try:
                            distinct_query = f"SELECT COUNT(DISTINCT `{col_name}`) FROM {self.table_name}"
                            distinct_result = conn.execute(text(distinct_query)).scalar()
                            if distinct_result:
                                logger.info(f"  {col_name} 고유값: {distinct_result:,}개")
                        except:
                            pass
                    
                    # 그룹 컬럼이면 고유값 수 계산
                    elif '그룹' in col_name:
                        try:
                            distinct_query = f"SELECT COUNT(DISTINCT `{col_name}`) FROM {self.table_name}"
                            distinct_result = conn.execute(text(distinct_query)).scalar()
                            if distinct_result:
                                logger.info(f"  {col_name} 고유값: {distinct_result:,}개")
                        except:
                            pass
            
            return True
            
        except Exception as e:
            logger.error(f"요약 통계 생성 실패: {e}")
            return False
    
    def run(self):
        """전체 프로세스 실행"""
        try:
            logger.info("=" * 60)
            logger.info("ZMMR0001 자재리스트 데이터 Import (SQLite)")
            logger.info("=" * 60)
            
            # 데이터베이스 연결
            if not self.connect_database():
                return False
            
            # Excel 파일 분석
            if not self.analyze_excel():
                return False
            
            # 데이터 정리
            if not self.clean_and_prepare_data():
                return False
            
            # 테이블 생성
            if not self.create_table():
                return False
            
            # 데이터 저장
            if not self.save_to_database():
                return False
            
            # 요약 통계
            self.create_summary_stats()
            
            logger.info("\n" + "=" * 60)
            logger.info("✅ Import 완료!")
            logger.info(f"데이터베이스: {self.db_path}")
            logger.info(f"테이블명: {self.table_name}")
            logger.info(f"총 레코드: {len(self.df):,}")
            logger.info("=" * 60)
            
            return True
            
        except Exception as e:
            logger.error(f"프로세스 실행 실패: {e}")
            import traceback
            traceback.print_exc()
            return False

if __name__ == "__main__":
    excel_file = "/Users/jinsyu/Downloads/ZMMR0001_자재리스트.XLSX"
    db_file = "db.sqlite"  # SQLite 데이터베이스 파일
    
    if not os.path.exists(excel_file):
        logger.error(f"파일을 찾을 수 없습니다: {excel_file}")
    else:
        importer = MaterialsDataImporter(excel_file, db_file)
        importer.run()