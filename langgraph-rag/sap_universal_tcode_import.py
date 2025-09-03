#!/usr/bin/env python3
"""
SAP Universal T-Code Import Script
모든 T-Code 데이터를 범용적으로 처리하는 스크립트
"""

import os
import sys
import pandas as pd
import psycopg2
from psycopg2.extras import Json
from dotenv import load_dotenv
from datetime import datetime, timedelta
import json
import argparse

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

# SAP 필드 한글 매핑
FIELD_MAPPING = {
    # 공통
    'MANDT': '클라이언트',
    'BUKRS': '회사코드',
    'WERKS': '플랜트',
    'LGORT': '저장위치',
    'MATNR': '자재번호',
    'MAKTX': '자재명',
    'MTART': '자재유형',
    'MATKL': '자재그룹',
    'MEINS': '기본단위',
    'BSTME': '주문단위',
    
    # 재고 관련
    'LABST': '가용재고',
    'UMLME': '이동중재고',
    'INSME': '품질검사재고',
    'EINME': '제한재고',
    'SPEME': '블록재고',
    'RETME': '반품재고',
    'KLABS': '미제한재고_누계',
    'KINSM': '품질재고_누계',
    'KSPEM': '블록재고_누계',
    
    # 중량/크기
    'BRGEW': '총중량',
    'NTGEW': '순중량',
    'GEWEI': '중량단위',
    'VOLUM': '부피',
    'VOLEH': '부피단위',
    
    # 날짜
    'ERSDA': '생성일',
    'ERDAT': '생성일',
    'ERZET': '생성시간',
    'ERNAM': '생성자',
    'LAEDA': '최종변경일',
    'AENAM': '변경자',
    
    # 회계
    'GJAHR': '회계연도',
    'MONAT': '회계기간',
    'PERDE': '기간',
    'VERSN': '버전',
    'OBJNR': '오브젝트번호',
    'KSTAR': '원가요소',
    'KOSTL': '원가센터',
    'AUFNR': '내부오더',
    
    # 판매
    'KUNNR': '고객번호',
    'KNDNR': '고객번호',
    'NAME1': '고객명',
    'NAME2': '고객명2',
    'VKORG': '판매조직',
    'VTWEG': '유통채널',
    'SPART': '제품군',
    'VBELN': '판매문서',
    'POSNR': '품목번호',
    'KAUFN': '판매오더',
    'KDPOS': '판매오더항목',
    'ARTNR': '제품번호',
    'VBTYP': '판매문서범주',
    'AUART': '판매문서유형',
    'AUDAT': '증빙일',
    'VDATU': '납품요청일',
    'BUKRS_VF': '청구회사코드',
    'BZIRK': '판매지역',
    'BZIRK_AUFT': '판매지역_오더',
    'PRSDT': '가격결정일',
    'ABGRU': '취소사유',
    'FKART': '청구유형',
    'FKDAT': '청구일',
    'FKSTO': '청구취소',
    'SFAKN': '취소청구',
    'KUNAG': '판매처',
    'KUNRG': '지급인',
    'ZUONR': '할당번호',
    'FKIMG': '청구수량',
    'SHKZG': '차대지시자',
    'KZWI1': '조건금액1',
    'KZWI2': '조건금액2', 
    'KZWI3': '조건금액3',
    'KZWI4': '조건금액4',
    'AUGRU': '오더사유',
    'GWLDT': 'PO일자',
    'PSPNR': '프로젝트번호',
    'VBTYP': '판매문서범주',
    'KURRF_DAT': '환율일자',
    'RFBSK': '전기상태',
    'AUBEL': '판매문서',
    'AUPOS': '판매품목',
    'PALEDGER': '원장',
    'VRGAR': '레코드유형',
    'VERSI': '버전',
    'PERBL': '전기기간',
    'VV005001': '월목표금액',
    'VKGRP': '영업그룹',
    'VKBUR': '영업사무소',
    'BSTNK': '고객PO번호',
    'BSTDK': '고객PO일자',
    'ARKTX': '품목텍스트',
    'VSTEL': '출하지점',
    'ROUTE': '경로',
    'KPEIN': '가격단위수량',
    
    # 구매
    'LIFNR': '공급업체',
    'EBELN': '구매문서',
    'EBELP': '구매품목',
    'EKGRP': '구매그룹',
    'EKORG': '구매조직',
    
    # 금액 필드
    'NETWR': '정가',
    'WAERK': '통화',
    'KWMENG': '주문수량',
    'VRKME': '판매단위',
    'NETPR': '단가',
    'MENGE': '수량',
    'DMBTR': '금액_현지통화',
    'WRBTR': '금액_문서통화',
    
    # CO-PA 값 필드
    'VV010': '매출액',
    'VV020': '매출원가',
    'VV030': '매출총이익',
    
    # 월별 금액 (WKG)
    'WKG001': '1월',
    'WKG002': '2월',
    'WKG003': '3월',
    'WKG004': '4월',
    'WKG005': '5월',
    'WKG006': '6월',
    'WKG007': '7월',
    'WKG008': '8월',
    'WKG009': '9월',
    'WKG010': '10월',
    'WKG011': '11월',
    'WKG012': '12월',
    
    # 기타
    'BUTXT': '회사명',
    'LAND1': '국가',
    'REGIO': '지역',
    'ORT01': '도시',
    'STRAS': '주소',
    'PSTLZ': '우편번호',
    'TELF1': '전화번호',
    'TELFX': '팩스번호',
    'SPERR': '블록',
    'LOEVM': '삭제표시',
    'XBLNR': '참조문서',
    'BELNR': '전표번호',
    'BUZEI': '항목',
    'BLART': '전표유형',
    'BLDAT': '전표일자',
    'BUDAT': '전기일자',
    'CPUDT': '입력일자',
    'USNAM': '사용자명',
    'TCODE': '트랜잭션코드',
    'BSCHL': '전기키',
    'SHKZG': '차변/대변',
    'MWSKZ': '세금코드',
    'GSBER': '사업영역',
    'PRCTR': '손익센터',
    'SEGMENT': '세그먼트',
    'ZUONR': '지정',
    'SGTXT': '적요',
    'AUFNR': '오더',
    'ANLN1': '자산번호',
    'ANLN2': '자산보조번호',
    'SAKNR': 'G/L계정',
    'HKONT': '총계정원장계정',
    'UMSKZ': '특별G/L',
    'ZFBDT': '기준일',
    'ZTERM': '지급조건',
    'ZBD1T': '현금할인일수1',
    'ZBD2T': '현금할인일수2',
    'ZBD3T': '순지급일수',
    'REBZG': '참조전표',
    'REBZJ': '참조연도',
    'REBZZ': '참조항목',
    'LZBKZ': '지급보류',
    'DISKP': '할인율',
    'WVERW': '사용목적',
    'SQIKZ': '품질검사',
    'PSTYP': '품목범주',
    'KNUMV': '조건문서',
    'KPOSN': '조건품목',
    'KSCHL': '조건유형',
    'KBETR': '조건금액',
    'KONWA': '조건통화',
    'KPEIN': '가격단위',
    'KMEIN': '조건단위',
    'KUMZA': '분자',
    'KUMNE': '분모',
    'AWTYP': '참조거래',
    'AWKEY': '참조키',
    'FIKRS': '재무영역',
    'XWBZK': '원천세',
    'QSSHB': '원천세액',
    'QBSHB': '과세표준액',
    'QSZDT': '원천세전기일',
    'QSSEC': '원천세코드',
    'EMPFB': '대체수취인',
    'XREF1': '참조키1',
    'XREF2': '참조키2',
    'XREF3': '참조키3',
    'DTWS1': '계획일1',
    'DTWS2': '계획일2',
    'DTWS3': '계획일3',
    'DTWS4': '계획일4',
    'XNEGP': '음수전기',
    'RFZEI': '지급기준',
    'CCINS': '카드회사',
    'CCNUM': '카드번호',
    'SSBLK': '지급보류사유',
    'MANSP': '수동보류',
    'MSCHL': '던닝키',
    'MANST': '던닝레벨',
    'MADAT': '독촉일',
    'VBUND': '회사',
    'XEGDR': '단일지급',
    'RECID': 'RecoveryID',
    'PPDIFF': '지급차액',
    'PPDIF2': '지급차액2',
    'PPDIF3': '지급차액3',
    'PYCUR': '지급통화',
    'PYAMT': '지급금액',
    'BVTYP': '파트너은행유형',
    'KTOSL': '거래유형',
    'AGZEI': '정산기간',
    'PERNR': '사원번호',
    'DMBE2': '그룹통화금액',
    'DMBE3': '하드통화금액',
    'DMBE4': '인덱스기준금액',
    'RDIFF': '반올림차액',
    'RDIF2': '반올림차액2',
    'RDIF3': '반올림차액3',
    'BDIFF': '평가차액',
    'BDIF2': '평가차액2',
    'BDIF3': '평가차액3',
    'XSTAT': '상태',
    'XRUEB': '이월',
    'XPANZ': '부분표시',
    'XSTOV': '역분개',
    'XSNET': '순지급',
    'XSERG': '보충',
    'XUMAN': '재분류',
    'XANET': '순자산',
    'XSKST': '원가차이',
    'XINVE': '투자',
    'XZAHL': '지급',
    'XMANU': '수동생성',
    'XBILK': '대차대조표계정',
    'GVTYP': '손익유형',
    'HKTID': '계정ID',
    'XNEGP': '음수전기가능',
    'VORGN': '거래유형',
    'FDLEV': '계획레벨',
    'FDGRP': '계획그룹',
    'FDWBT': '계획금액',
    'FDTAG': '계획일',
}

# T-Code별 설정
TCODE_CONFIGS = {
    'ZMMR0016': {
        'name': '재고현황',
        'tables': [
            {
                'table': 'MARD',
                'fields': ['MATNR', 'WERKS', 'LGORT', 'LABST', 'UMLME', 'INSME', 'EINME', 'SPEME', 'RETME'],
                'options': ["LABST > 0 OR INSME > 0 OR SPEME > 0"],
                'description': '재고 데이터'
            },
            {
                'table': 'MARA',
                'fields': ['MATNR', 'MTART', 'MATKL', 'MEINS', 'BRGEW', 'NTGEW'],
                'join_key': 'MATNR',
                'description': '자재 마스터'
            },
            {
                'table': 'MAKT',
                'fields': ['MATNR', 'MAKTX'],
                'options': ["SPRAS = '3'"],
                'join_key': 'MATNR',
                'description': '자재 설명'
            }
        ],
        'target_table': 'sap_zmmr0016_inventory'
    },
    'KE33': {
        'name': 'CO-PA 수익성 분석',
        'tables': [
            {
                'table': 'COSP',
                'fields': ['OBJNR', 'GJAHR', 'KSTAR', 'WKG001', 'WKG002', 'WKG003', 'WKG004', 'WKG005', 'WKG006'],
                'params': {'GJAHR': str(datetime.now().year)},
                'description': '원가센터 실적'
            }
        ],
        'target_table': 'sap_ke33_cosp'
    },
    'ZPACHECK0': {
        'name': '생산계획 확인',
        'tables': [
            {
                'table': 'COSP',
                'fields': ['OBJNR', 'GJAHR', 'VERSN', 'KSTAR', 'WKG001', 'WKG002', 'WKG003', 'WKG004', 'WKG005', 'WKG006', 'WKG007', 'WKG008', 'WKG009', 'WKG010', 'WKG011', 'WKG012'],
                'params': {
                    'VERSN': '014',
                    'BUKRS': '1000',
                    'GJAHR': str(datetime.now().year)
                },
                'description': '계획 데이터'
            }
        ],
        'target_table': 'sap_zpacheck0_plan'
    },
    'MM03': {
        'name': '자재 마스터 조회',
        'tables': [
            {
                'table': 'MARA',
                'fields': None,  # None이면 모든 필드
                'description': '자재 마스터'
            }
        ],
        'target_table': 'sap_mm03_material'
    },
    'MB52': {
        'name': '창고별 재고 리스트',
        'tables': [
            {
                'table': 'MARD',
                'fields': None,
                'description': '창고별 재고'
            }
        ],
        'target_table': 'sap_mb52_stock'
    },
    'ZSDR0164': {
        'name': '일일 영업 실적 현황_마스터기준',
        'params': {
            'VKBUR': 'E100',  # 영업사무소 (D100, R100, E100 중 선택)
            'AUDAT_FROM': (datetime.now() - timedelta(days=30)).strftime('%Y%m%d'),  # 최근 30일
            'AUDAT_TO': datetime.now().strftime('%Y%m%d')
        },
        'tables': [
            {
                # GET_DATA2,3: 수주 데이터 (VBAK, VBKD, VBAP)
                'table': 'VBAK',  # 판매 문서 헤더
                'fields': ['VBELN', 'AUDAT', 'VDATU', 'AUART', 'VKGRP', 'VKBUR', 'VKORG', 'VTWEG', 'SPART', 'KUNNR', 'WAERK', 'NETWR', 'BUKRS_VF'],
                'options': [],  # 동적으로 생성됨
                'description': '수주 헤더 (판매문서)'
            },
            {
                'table': 'VBKD',  # 판매 문서 영업 데이터
                'fields': ['VBELN', 'POSNR', 'BZIRK', 'PRSDT'],
                'options': ["POSNR = '000000'"],
                'join_key': 'VBELN',
                'description': '수주 영업 데이터'
            },
            {
                'table': 'VBAP',  # 판매 문서 아이템
                'fields': ['VBELN', 'POSNR', 'MATNR', 'MATKL', 'ARKTX', 'KWMENG', 'VRKME', 'NETWR', 'SPART', 'WERKS', 'LGORT', 'ABGRU'],
                'options': ["ABGRU = ' '"],  # 취소사유 없는 것만
                'join_key': 'VBELN',
                'description': '수주 아이템'
            }
        ],
        'target_table': 'sap_zsdr0164_daily_sales'
    },
    'ZSDR0164_BILLING': {
        'name': '일일 영업 실적 현황_청구문서',
        'params': {
            'VKBUR': 'E100',  # 영업사무소
            'FKDAT_FROM': (datetime.now() - timedelta(days=30)).strftime('%Y%m%d'),
            'FKDAT_TO': datetime.now().strftime('%Y%m%d')
        },
        'tables': [
            {
                # GET_DATA4,5: 매출/반품 데이터 (VBRK, VBRP)
                'table': 'VBRK',  # 청구 문서 헤더
                'fields': ['VBELN', 'FKART', 'FKDAT', 'FKSTO', 'SFAKN', 'WAERK', 'NETWR', 'KUNAG', 'VKORG', 'VTWEG', 'SPART', 'BUKRS', 'KURRF_DAT', 'RFBSK'],
                'options': ["FKDAT >= '20240101'", "FKART IN ('ZF1','ZF2','ZF3','ZF4','ZG2','ZL2','ZR1','ZR3')", "FKSTO = ' '"],
                'description': '매출/반품 헤더 (청구문서)'
            },
            {
                'table': 'VBRP',  # 청구 문서 아이템
                'fields': ['VBELN', 'POSNR', 'MATNR', 'NETWR', 'VKGRP', 'VKBUR', 'BZIRK_AUFT', 'SPART', 'AUBEL', 'AUPOS'],
                'join_key': 'VBELN',
                'description': '매출/반품 아이템'
            }
        ],
        'target_table': 'sap_zsdr0164_billing'
    },
    'ZSDR0340': {
        'name': '국영/해영 기간별 매출 세부내역 레포트_마스터기준',
        'params': {
            'VKBUR': 'D100',  # 영업사무소 (D100, R100, E100, W100, S100)
            'BUKRS': '1000',  # 회사코드
            'VKORG': '1000',  # 영업조직
            'FKDAT_FROM': (datetime.now() - timedelta(days=7)).strftime('%Y%m%d'),
            'FKDAT_TO': datetime.now().strftime('%Y%m%d')
        },
        'tables': [
            {
                # 메인 SELECT: 청구문서 + 판매문서 조인
                'table': 'VBRK',  # 청구 문서 헤더
                'fields': ['VBELN', 'FKART', 'FKDAT', 'FKSTO', 'SFAKN', 'KUNRG', 'VKORG', 'VTWEG', 'BUKRS', 'KURRF_DAT', 'ZUONR'],
                'options': [],  # 동적 생성
                'description': '청구문서 헤더',
                'max_rows': 10000  # VBRK만 제한
            },
            {
                'table': 'VBRP',  # 청구 문서 아이템
                'fields': ['VBELN', 'POSNR', 'MATNR', 'FKIMG', 'NETWR', 'SHKZG', 'AUBEL', 'AUPOS', 'VKBUR', 'KZWI1', 'KZWI2', 'KZWI3', 'KZWI4'],
                'join_key': 'VBELN',
                'description': '청구문서 아이템',
                'batch_join': True  # 배치 조인 방식 사용
            }
        ],
        'target_table': 'sap_zsdr0340_sales_detail'
    },
    'ZSDR0164_TARGET': {
        'name': '월목표 (CO-PA)',
        'tables': [
            {
                # GET_DATA1: 월목표 데이터 (CE21000)
                'table': 'CE21000',
                'fields': ['PALEDGER', 'VRGAR', 'VERSI', 'PERBL', 'VKBUR', 'BUKRS', 'VKORG', 'VKGRP', 'BZIRK', 'KNDNR', 'ARTNR', 'VV005001'],  # VV005001이 목표금액
                'options': ["PALEDGER = '01'", "VRGAR = 'F'"],
                'params': {
                    'VERSI': '014',  # 버전 (분기별로 다름)
                    'PERBL': str(datetime.now().year) + '001'  # 연도+기간
                },
                'description': '월목표 (CO-PA)'
            }
        ],
        'target_table': 'sap_zsdr0164_target'
    }
}

class SAPTCodeImporter:
    def __init__(self, tcode=None, config=None):
        """
        범용 T-Code Importer
        
        Parameters:
        - tcode: T-Code 이름 (TCODE_CONFIGS에 정의된 것)
        - config: 사용자 정의 config (기존 T-Code 없을 때)
        """
        self.tcode = tcode
        if tcode and tcode in TCODE_CONFIGS:
            self.config = TCODE_CONFIGS[tcode]
        elif config:
            self.config = config
        else:
            raise ValueError(f"T-Code '{tcode}'가 정의되지 않았습니다. config 파라미터를 제공하세요.")
        
        self.sap_conn = None
        self.data_frames = {}
        
    def connect(self):
        """SAP 연결"""
        try:
            self.sap_conn = Connection(**SAP_CONFIG)
            print(f"✅ SAP 연결 성공 (T-Code: {self.tcode or 'Custom'})")
            return True
        except Exception as e:
            print(f"❌ SAP 연결 실패: {e}")
            return False
    
    def read_table(self, table_config):
        """SAP 테이블 읽기 (무제한 행)"""
        table_name = table_config['table']
        fields = table_config.get('fields')
        options = []
        
        # 전역 params 처리 (T-Code 레벨)
        if 'params' in self.config:
            params = self.config['params']
            
            # ZSDR0164 특별 처리
            if self.tcode == 'ZSDR0164':
                if table_name == 'VBAK':
                    # 수주 데이터 조건
                    options.append({'TEXT': f"VKBUR = '{params.get('VKBUR', 'E100')}'"})
                    options.append({'TEXT': f"AND AUDAT >= '{params.get('AUDAT_FROM', '20240101')}'"})
                    options.append({'TEXT': f"AND AUDAT <= '{params.get('AUDAT_TO', datetime.now().strftime('%Y%m%d'))}'"})
                    options.append({'TEXT': "AND AUART IN ('ZDR','ZOR1','ZOR3','ZEOR','ZELC','ZETP','ZES1','ZETS','ZCR','ZEIP','ZDKB','ZPS1')"})
            elif self.tcode == 'ZSDR0164_BILLING':
                if table_name == 'VBRK':
                    # 청구 데이터 조건
                    options.append({'TEXT': f"FKDAT >= '{params.get('FKDAT_FROM', '20240101')}'"})
                    options.append({'TEXT': f"AND FKDAT <= '{params.get('FKDAT_TO', datetime.now().strftime('%Y%m%d'))}'"})
                if table_name == 'VBRP':
                    options.append({'TEXT': f"VKBUR = '{params.get('VKBUR', 'E100')}'"})
            elif self.tcode == 'ZSDR0340':
                if table_name == 'VBRK':
                    # ZSDR0340 조건 (소스코드 참조)
                    options.append({'TEXT': f"FKDAT >= '{params.get('FKDAT_FROM', '20240101')}'"})
                    options.append({'TEXT': f"AND FKDAT <= '{params.get('FKDAT_TO', datetime.now().strftime('%Y%m%d'))}'"})
                    options.append({'TEXT': "AND FKSTO = ' '"})  # 청구취소 아닌 것
                    options.append({'TEXT': "AND SFAKN = ' '"})  # 취소청구 번호 없는 것
                    options.append({'TEXT': f"AND VKORG = '{params.get('VKORG', '1000')}'"})
                    options.append({'TEXT': f"AND BUKRS = '{params.get('BUKRS', '1000')}'"})
                    # VBRK 테이블의 VBELN 저장 (VBRP 조인용)
                    self.vbrk_vbelns = set()
                if table_name == 'VBRP':
                    # VBRP는 VBELN으로 필터링해야 함
                    options.append({'TEXT': f"VKBUR = '{params.get('VKBUR', 'D100')}'"})
                    # 날짜 조건 추가 (VBRP는 ERDAT 필드가 있으면 사용)
                    # 또는 VBRK에서 조회한 VBELN으로 필터링이 더 정확함
        
        # OPTIONS 생성
        if 'options' in table_config:
            for opt in table_config['options']:
                if opt not in [o['TEXT'] for o in options]:  # 중복 방지
                    options.append({'TEXT': opt})
        
        # 테이블별 params 처리
        if 'params' in table_config:
            for key, value in table_config['params'].items():
                options.append({'TEXT': f"{key} = '{value}'"})
        
        print(f"\n📖 {table_name} 테이블 조회 중...")
        print(f"   설명: {table_config.get('description', 'N/A')}")
        
        try:
            # 먼저 필드 정보 가져오기
            if fields is None:
                # 모든 필드 가져오기
                result = self.sap_conn.call('RFC_READ_TABLE',
                                           QUERY_TABLE=table_name,
                                           DELIMITER='|',
                                           NO_DATA='X')
                
                all_fields = result.get('FIELDS', [])
                # 너무 많은 필드는 제한 (RFC 제한 때문)
                fields = [{'FIELDNAME': f['FIELDNAME']} for f in all_fields[:50]]
                print(f"   전체 {len(all_fields)}개 필드 중 {len(fields)}개 선택")
            else:
                fields = [{'FIELDNAME': f} for f in fields]
            
            # 데이터 조회 - 무제한 (ROWCOUNT 파라미터 제거)
            all_data = []
            offset = 0
            batch_size = 10000  # 한 번에 가져올 데이터 수
            
            while True:
                # ROWSKIPS와 ROWCOUNT를 사용한 페이징
                result = self.sap_conn.call('RFC_READ_TABLE',
                                           QUERY_TABLE=table_name,
                                           DELIMITER='|',
                                           FIELDS=fields,
                                           OPTIONS=options if options else [],
                                           ROWSKIPS=offset,
                                           ROWCOUNT=batch_size)
                
                data = result.get('DATA', [])
                if not data:
                    break
                
                all_data.extend(data)
                offset += len(data)
                
                print(f"   {offset}개 조회 중...")
                
                # 배치가 가득 차지 않았으면 마지막
                if len(data) < batch_size:
                    break
            
            if all_data:
                print(f"✅ {table_name}: 총 {len(all_data)}개 데이터 조회 완료")
                
                # DataFrame 생성
                field_names = [f['FIELDNAME'] for f in result.get('FIELDS', [])]
                data_list = []
                
                for row in all_data:
                    values = row['WA'].split('|')
                    values = [v.strip() for v in values]
                    data_list.append(dict(zip(field_names, values)))
                
                df = pd.DataFrame(data_list)
                return df
            else:
                print(f"⚠️ {table_name}: 데이터 없음")
                return pd.DataFrame()
                
        except Exception as e:
            print(f"❌ {table_name} 조회 실패: {str(e)[:100]}")
            return pd.DataFrame()
    
    def import_data(self, save_to_db=True):
        """T-Code 데이터 import"""
        if not self.connect():
            return False
        
        print(f"\n{'='*60}")
        print(f"T-Code: {self.config.get('name', self.tcode)}")
        print(f"{'='*60}")
        
        # 각 테이블 읽기
        for table_config in self.config['tables']:
            df = self.read_table(table_config)
            
            if not df.empty:
                table_name = table_config['table']
                self.data_frames[table_name] = df
                
                # ZSDR0340 특별 처리: VBRK의 VBELN만 사용
                if self.tcode == 'ZSDR0340' and table_name == 'VBRK':
                    # VBRK의 VBELN 저장
                    self.vbrk_vbelns = df['VBELN'].unique().tolist()
                    print(f"   VBRK VBELN 개수: {len(self.vbrk_vbelns)}개")
                
                # 배치 조인 처리 (ZSDR0340)
                if table_config.get('batch_join') and self.tcode == 'ZSDR0340' and table_name == 'VBRP':
                    if hasattr(self, 'vbrk_vbelns') and self.vbrk_vbelns:
                        print(f"   VBRP 배치 조회 시작 (VBELN {len(self.vbrk_vbelns)}개)...")
                        
                        # VBRK의 VBELN으로 VBRP 배치 조회
                        vbrp_data = []
                        batch_size = 50  # 한 번에 조회할 VBELN 수
                        
                        for i in range(0, len(self.vbrk_vbelns), batch_size):
                            batch_vbelns = self.vbrk_vbelns[i:i+batch_size]
                            
                            # IN 조건 생성
                            vbeln_conditions = []
                            for vbeln in batch_vbelns:
                                vbeln_conditions.append(f"VBELN = '{vbeln}'")
                            
                            # OPTIONS 생성 (OR 조건)
                            options = []
                            options.append({'TEXT': f"VKBUR = '{self.config['params'].get('VKBUR', 'D100')}'"})
                            if vbeln_conditions:
                                options.append({'TEXT': f"AND ({' OR '.join(vbeln_conditions[:10])})"})  # SAP 제한으로 10개씩만
                            
                            # VBRP 조회
                            try:
                                result = self.sap_conn.call('RFC_READ_TABLE',
                                                           QUERY_TABLE='VBRP',
                                                           DELIMITER='|',
                                                           FIELDS=[{'FIELDNAME': f} for f in table_config['fields']],
                                                           OPTIONS=options,
                                                           ROWCOUNT=10000)
                                
                                if result.get('DATA'):
                                    field_names = [f['FIELDNAME'] for f in result.get('FIELDS', [])]
                                    for row in result['DATA']:
                                        values = row['WA'].split('|')
                                        values = [v.strip() for v in values]
                                        vbrp_data.append(dict(zip(field_names, values)))
                                    
                                    print(f"      배치 {i//batch_size + 1}: {len(result['DATA'])}개")
                                
                            except Exception as e:
                                print(f"      배치 조회 실패: {str(e)[:50]}")
                        
                        if vbrp_data:
                            df = pd.DataFrame(vbrp_data)
                            print(f"   VBRP 총 {len(df)}개 조회 완료")
                            self.data_frames[table_name] = df
                        else:
                            print("   VBRP 데이터 없음")
                            return True
                
                # 일반 Join 처리
                if 'join_key' in table_config and len(self.data_frames) > 1:
                    # 첫 번째 테이블과 조인
                    first_table = list(self.data_frames.keys())[0]
                    
                    self.data_frames[first_table] = pd.merge(
                        self.data_frames[first_table],
                        df,
                        on=table_config['join_key'],
                        how='left'
                    )
                    # 조인된 테이블은 제거
                    del self.data_frames[table_name]
        
        # PostgreSQL 저장
        if save_to_db and self.data_frames:
            self.save_to_postgres()
        
        self.sap_conn.close()
        return True
    
    def save_to_postgres(self):
        """PostgreSQL에 저장"""
        target_table = self.config.get('target_table', f"sap_{self.tcode.lower() if self.tcode else 'custom'}")
        
        try:
            conn = psycopg2.connect(DATABASE_URL)
            cursor = conn.cursor()
            
            # 기존 테이블 삭제
            cursor.execute(f"DROP TABLE IF EXISTS {target_table} CASCADE")
            
            # 메인 DataFrame 가져오기
            main_df = list(self.data_frames.values())[0] if self.data_frames else pd.DataFrame()
            
            if main_df.empty:
                print("⚠️ 저장할 데이터가 없습니다.")
                return
            
            # 동적 테이블 생성
            columns = []
            column_mapping = {}  # 원본 컬럼명 -> 한글 컬럼명 매핑
            used_columns = set()  # 중복 체크용
            
            for col in main_df.columns:
                # 한글 컬럼명 가져오기
                korean_col = FIELD_MAPPING.get(col, col)
                
                # 중복 컬럼 처리
                if korean_col in used_columns:
                    # 중복된 경우 원본 컬럼명 사용
                    korean_col = f"{korean_col}_{col}"
                
                used_columns.add(korean_col)
                column_mapping[col] = korean_col
                
                # 데이터 타입 추론
                dtype = main_df[col].dtype
                
                if 'int' in str(dtype):
                    pg_type = 'BIGINT'
                elif 'float' in str(dtype):
                    pg_type = 'NUMERIC(20,3)'
                elif 'date' in col.lower() or 'datum' in col.lower():
                    pg_type = 'DATE'
                else:
                    # 최대 길이 확인
                    max_len = main_df[col].astype(str).str.len().max()
                    if max_len > 255:
                        pg_type = 'TEXT'
                    else:
                        pg_type = f'VARCHAR({max(max_len + 20, 50)})'
                
                # 컬럼명 안전하게 변환
                safe_col = korean_col.replace(' ', '_').replace('-', '_')
                columns.append(f'"{safe_col}" {pg_type}')
            
            # 테이블 생성
            create_sql = f"""
            CREATE TABLE {target_table} (
                id SERIAL PRIMARY KEY,
                {', '.join(columns)},
                "원본_tcode" VARCHAR(20),
                "조회일시" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            cursor.execute(create_sql)
            print(f"\n✅ 테이블 {target_table} 생성 완료")
            
            # 데이터 삽입
            inserted = 0
            for _, row in main_df.iterrows():
                # NULL 처리 및 데이터 정리
                values = []
                for col in main_df.columns:
                    val = row[col]
                    
                    # SAP 빈 값 처리
                    if pd.isna(val) or val == '' or val == '00000000' or val == '0':
                        values.append(None)
                    else:
                        values.append(str(val).strip())
                
                # 추가 컬럼
                values.append(self.tcode or 'CUSTOM')
                
                # INSERT 문 생성
                placeholders = ', '.join(['%s'] * len(values))
                safe_cols = [f'"{column_mapping[col].replace(" ", "_").replace("-", "_")}"' for col in main_df.columns]
                insert_sql = f"""
                INSERT INTO {target_table} 
                ({', '.join(safe_cols)}, "원본_tcode")
                VALUES ({placeholders})
                """
                
                cursor.execute(insert_sql, values)
                inserted += 1
                
                # 배치 커밋
                if inserted % 1000 == 0:
                    conn.commit()
                    print(f"  📝 {inserted}개 저장 중...")
            
            conn.commit()
            
            # 인덱스 생성 (주요 컬럼이 있으면)
            for col in ['MATNR', 'WERKS', 'BUKRS', 'GJAHR']:
                if col in main_df.columns:
                    korean_col = column_mapping[col]
                    safe_col = korean_col.replace(' ', '_').replace('-', '_')
                    try:
                        cursor.execute(f'CREATE INDEX idx_{target_table}_{col.lower()} ON {target_table}("{safe_col}")')
                    except:
                        pass
            
            conn.commit()
            conn.close()
            
            print(f"\n✅ 총 {inserted}개 데이터를 {target_table}에 저장 완료!")
            
            # 요약 정보
            print("\n📊 저장 요약:")
            print(f"  - 테이블: {target_table}")
            print(f"  - 행 수: {inserted}")
            print(f"  - 컬럼 수: {len(main_df.columns)}")
            
        except Exception as e:
            print(f"❌ PostgreSQL 저장 실패: {e}")

def add_custom_tcode(tcode_name, table_configs, target_table=None):
    """새로운 T-Code 설정 추가"""
    TCODE_CONFIGS[tcode_name] = {
        'name': tcode_name,
        'tables': table_configs,
        'target_table': target_table or f"sap_{tcode_name.lower()}"
    }

def list_available_tcodes():
    """사용 가능한 T-Code 목록"""
    print("\n📋 사용 가능한 T-Code:")
    for tcode, config in TCODE_CONFIGS.items():
        print(f"  - {tcode}: {config['name']}")
        for table in config['tables']:
            print(f"    └─ {table['table']}: {table.get('description', 'N/A')}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='SAP Universal T-Code Importer')
    parser.add_argument('tcode', nargs='?', help='T-Code 이름 (예: ZMMR0016, KE33)')
    parser.add_argument('--list', action='store_true', help='사용 가능한 T-Code 목록 표시')
    parser.add_argument('--table', help='커스텀 테이블 이름 (T-Code가 정의되지 않은 경우)')
    parser.add_argument('--fields', help='조회할 필드 (쉼표로 구분)')
    parser.add_argument('--target', help='저장할 PostgreSQL 테이블 이름')
    
    args = parser.parse_args()
    
    if args.list:
        list_available_tcodes()
    elif args.tcode:
        # 기존 T-Code 실행
        importer = SAPTCodeImporter(args.tcode)
        importer.import_data()
    elif args.table:
        # 커스텀 테이블 import
        fields = args.fields.split(',') if args.fields else None
        config = {
            'name': f'Custom: {args.table}',
            'tables': [
                {
                    'table': args.table,
                    'fields': fields,
                    'description': 'Custom table import'
                }
            ],
            'target_table': args.target or f"sap_{args.table.lower()}"
        }
        importer = SAPTCodeImporter(config=config)
        importer.import_data()
    else:
        print("사용법:")
        print("  python sap_universal_tcode_import.py ZMMR0016")
        print("  python sap_universal_tcode_import.py --list")
        print("  python sap_universal_tcode_import.py --table MARA --fields MATNR,MTART,MATKL")
        print("\n기본 T-Code 실행 예시:")
        print("  python sap_universal_tcode_import.py KE33")