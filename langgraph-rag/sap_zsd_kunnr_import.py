#!/usr/bin/env python3
"""
SAP ZSD_RFC_SEND_KUNNR RFC 함수 데이터 Import
고객 마스터 데이터를 SAP에서 PostgreSQL로 가져옵니다.
"""

import os
import sys
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text, Table, MetaData, Column, String, DateTime, Integer, Float
from sqlalchemy.dialects.postgresql import insert
from typing import Dict, List, Any
import logging
from dotenv import load_dotenv

# 환경변수 설정
os.environ['SAPNWRFC_HOME'] = os.path.expanduser('~/sap/nwrfcsdk')
os.environ['DYLD_LIBRARY_PATH'] = f"{os.environ['SAPNWRFC_HOME']}/lib:{os.environ.get('DYLD_LIBRARY_PATH', '')}"

try:
    from pyrfc import Connection
    print("✅ pyrfc 모듈 import 성공!")
except ImportError as e:
    print(f"❌ pyrfc import 실패: {e}")
    sys.exit(1)

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 환경변수 로드
load_dotenv()

# SAP 연결 정보
SAP_CONFIG = {
    'ashost': os.getenv('SAP_ASHOST', '192.168.32.100'),
    'sysnr': os.getenv('SAP_SYSNR', '00'),
    'client': os.getenv('SAP_CLIENT', '100'),
    'user': os.getenv('SAP_USER', 'bc01'),
    'passwd': os.getenv('SAP_PASSWORD', 'dlsxjdpa2023@!'),
    'lang': os.getenv('SAP_LANG', 'KO'),
}

# PostgreSQL 연결 정보
PG_CONFIG = {
    'host': 'localhost',
    'port': 10003,
    'database': 'baseone',
    'user': 'baseone',
    'password': 'baseone'
}

class ZSDKunnrImporter:
    def __init__(self):
        self.sap_conn = None
        self.pg_engine = None
        self.table_name = 'sap_zsd_kunnr'
        
    def connect_sap(self):
        """SAP 연결"""
        try:
            self.sap_conn = Connection(**SAP_CONFIG)
            logger.info(f"✅ SAP 연결 성공: {SAP_CONFIG['ashost']}")
            return True
        except Exception as e:
            logger.error(f"❌ SAP 연결 실패: {e}")
            return False
    
    def connect_postgres(self):
        """PostgreSQL 연결"""
        try:
            db_url = f"postgresql://{PG_CONFIG['user']}:{PG_CONFIG['password']}@{PG_CONFIG['host']}:{PG_CONFIG['port']}/{PG_CONFIG['database']}"
            self.pg_engine = create_engine(db_url)
            logger.info(f"✅ PostgreSQL 연결 성공: {PG_CONFIG['database']}")
            return True
        except Exception as e:
            logger.error(f"❌ PostgreSQL 연결 실패: {e}")
            return False
    
    def test_rfc_function(self):
        """ZSD_RFC_SEND_KUNNR 함수 테스트"""
        try:
            logger.info("🔍 ZSD_RFC_SEND_KUNNR RFC 함수 테스트 중...")
            
            # RFC 함수 호출 (파라미터 없이 시도)
            result = self.sap_conn.call('ZMM_RFC_SEND_MTR_REAL')
            
            logger.info("✅ RFC 함수 호출 성공!")
            
            # 결과 구조 확인
            if result:
                logger.info(f"📊 결과 키: {list(result.keys())}")
                
                # 주요 테이블 확인 (일반적으로 ET_KUNNR 또는 T_KUNNR 등의 이름)
                for key in result.keys():
                    if isinstance(result[key], list) and len(result[key]) > 0:
                        logger.info(f"  - {key}: {len(result[key])} rows")
                        if len(result[key]) > 0:
                            logger.info(f"    첫 번째 레코드 샘플: {result[key][0]}")
                
            return result
            
        except Exception as e:
            # 파라미터가 필요한 경우 재시도
            logger.warning(f"첫 번째 시도 실패: {e}")
            logger.info("파라미터를 포함하여 재시도...")
            
            try:
                # 일반적인 파라미터 시도
                result = self.sap_conn.call('ZSD_RFC_SEND_KUNNR',
                                           I_BUKRS='1000',  # 회사코드
                                           I_VKORG='1000')  # 영업조직
                
                logger.info("✅ RFC 함수 호출 성공 (파라미터 포함)!")
                return result
                
            except Exception as e2:
                logger.error(f"❌ RFC 함수 호출 실패: {e2}")
                return None
    
    def get_rfc_metadata(self):
        """RFC 함수 메타데이터 조회"""
        try:
            logger.info("📋 RFC 함수 메타데이터 조회 중...")
            
            # RFC_FUNCTION_SEARCH를 사용하여 함수 정보 조회
            search_result = self.sap_conn.call('RFC_FUNCTION_SEARCH',
                                              FUNCNAME='ZSD_RFC_SEND_KUNNR*')
            
            if search_result and 'FUNCTIONS' in search_result:
                for func in search_result['FUNCTIONS']:
                    logger.info(f"  함수명: {func.get('FUNCNAME', '')}")
                    logger.info(f"  그룹: {func.get('GROUPNAME', '')}")
            
            # RFC_GET_FUNCTION_INTERFACE를 사용하여 인터페이스 정보 조회
            interface_result = self.sap_conn.call('RFC_GET_FUNCTION_INTERFACE',
                                                 FUNCNAME='ZSD_RFC_SEND_KUNNR')
            
            if interface_result:
                logger.info("\n📌 Import 파라미터:")
                if 'PARAMS' in interface_result:
                    for param in interface_result['PARAMS']:
                        if param.get('PARAMCLASS') == 'I':
                            logger.info(f"  - {param.get('PARAMETER')}: {param.get('TABNAME')}")
                
                logger.info("\n📌 Export 파라미터:")
                if 'PARAMS' in interface_result:
                    for param in interface_result['PARAMS']:
                        if param.get('PARAMCLASS') == 'E':
                            logger.info(f"  - {param.get('PARAMETER')}: {param.get('TABNAME')}")
                
                logger.info("\n📌 테이블 파라미터:")
                if 'PARAMS' in interface_result:
                    for param in interface_result['PARAMS']:
                        if param.get('PARAMCLASS') == 'T':
                            logger.info(f"  - {param.get('PARAMETER')}: {param.get('TABNAME')}")
            
            return interface_result
            
        except Exception as e:
            logger.error(f"메타데이터 조회 실패: {e}")
            return None
    
    def fetch_data(self) -> Dict[str, Any]:
        """SAP에서 고객 데이터 조회"""
        try:
            logger.info("📊 ZSD_RFC_SEND_KUNNR 데이터 조회 중...")
            
            # 먼저 메타데이터 확인
            metadata = self.get_rfc_metadata()
            
            # RFC 함수 호출
            result = self.test_rfc_function()
            
            if not result:
                logger.error("RFC 함수 호출 실패")
                return None
            
            # 데이터가 있는 테이블 찾기
            data_table = None
            data_key = None
            
            for key in result.keys():
                if isinstance(result[key], list) and len(result[key]) > 0:
                    data_table = result[key]
                    data_key = key
                    break
            
            if data_table:
                logger.info(f"✅ {len(data_table)}개 레코드 조회 성공 (테이블: {data_key})")
                
                # 첫 5개 레코드 샘플 출력
                logger.info("\n📋 샘플 데이터 (처음 5개):")
                for i, record in enumerate(data_table[:5], 1):
                    logger.info(f"레코드 {i}:")
                    for field, value in record.items():
                        if value and str(value).strip():  # 비어있지 않은 값만 출력
                            logger.info(f"  {field}: {value}")
                
                return {
                    'table_name': data_key,
                    'data': data_table,
                    'count': len(data_table)
                }
            else:
                logger.warning("데이터가 비어있습니다.")
                return None
                
        except Exception as e:
            logger.error(f"데이터 조회 실패: {e}")
            return None
    
    def create_table(self, sample_data: Dict):
        """PostgreSQL 테이블 생성"""
        try:
            if not sample_data:
                logger.error("샘플 데이터가 없습니다.")
                return False
            
            # 테이블 삭제 및 재생성
            with self.pg_engine.connect() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {self.table_name} CASCADE"))
                conn.commit()
            
            logger.info(f"📝 {self.table_name} 테이블 생성 중...")
            
            # 동적으로 컬럼 생성 (모든 필드를 TEXT로)
            columns_sql = []
            for field_name in sample_data.keys():
                # PostgreSQL 컬럼명 규칙에 맞게 변환
                col_name = field_name.lower()
                columns_sql.append(f'"{col_name}" TEXT')
            
            # 메타데이터 컬럼 추가
            columns_sql.append('"import_date" TIMESTAMP DEFAULT CURRENT_TIMESTAMP')
            
            create_sql = f"""
            CREATE TABLE {self.table_name} (
                {', '.join(columns_sql)}
            )
            """
            
            with self.pg_engine.connect() as conn:
                conn.execute(text(create_sql))
                conn.commit()
            
            logger.info(f"✅ 테이블 생성 완료: {self.table_name}")
            return True
            
        except Exception as e:
            logger.error(f"테이블 생성 실패: {e}")
            return False
    
    def save_to_postgres(self, data_info: Dict):
        """PostgreSQL에 데이터 저장"""
        try:
            if not data_info or 'data' not in data_info:
                logger.error("저장할 데이터가 없습니다.")
                return False
            
            data = data_info['data']
            
            # 첫 번째 레코드로 테이블 생성
            if len(data) > 0:
                if not self.create_table(data[0]):
                    return False
            
            logger.info(f"💾 {len(data)}개 레코드 저장 중...")
            
            # DataFrame으로 변환
            df = pd.DataFrame(data)
            
            # 컬럼명 소문자로 변환
            df.columns = [col.lower() for col in df.columns]
            
            # 현재 시간 추가
            df['import_date'] = datetime.now()
            
            # 데이터 저장 (청크 단위로)
            chunk_size = 1000
            total_saved = 0
            
            for i in range(0, len(df), chunk_size):
                chunk = df.iloc[i:i+chunk_size]
                chunk.to_sql(
                    self.table_name,
                    self.pg_engine,
                    if_exists='append',
                    index=False,
                    method='multi'
                )
                total_saved += len(chunk)
                logger.info(f"  진행: {total_saved}/{len(df)} 레코드 저장됨")
            
            # 통계 출력
            with self.pg_engine.connect() as conn:
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
    
    def run(self):
        """전체 프로세스 실행"""
        try:
            logger.info("=" * 60)
            logger.info("SAP ZSD_RFC_SEND_KUNNR Import 시작")
            logger.info("=" * 60)
            
            # SAP 연결
            if not self.connect_sap():
                return False
            
            # PostgreSQL 연결
            if not self.connect_postgres():
                return False
            
            # 데이터 조회
            data_info = self.fetch_data()
            
            if data_info:
                # PostgreSQL에 저장
                if self.save_to_postgres(data_info):
                    logger.info("\n✅ Import 완료!")
                    logger.info(f"테이블명: {self.table_name}")
                    logger.info(f"레코드 수: {data_info['count']}")
                else:
                    logger.error("데이터 저장 실패")
            else:
                logger.error("조회된 데이터가 없습니다.")
            
            # 연결 종료
            if self.sap_conn:
                self.sap_conn.close()
                logger.info("SAP 연결 종료")
            
        except Exception as e:
            logger.error(f"프로세스 실행 실패: {e}")
            import traceback
            traceback.print_exc()
        
        finally:
            # 정리
            if self.sap_conn:
                try:
                    self.sap_conn.close()
                except:
                    pass

if __name__ == "__main__":
    importer = ZSDKunnrImporter()
    importer.run()