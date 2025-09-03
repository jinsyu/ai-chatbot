#!/usr/bin/env python3
"""
SAP RFC 연결 테스트 스크립트
"""

import os
import sys
from datetime import datetime

# 환경변수 설정
os.environ['SAPNWRFC_HOME'] = os.path.expanduser('~/sap/nwrfcsdk')
os.environ['DYLD_LIBRARY_PATH'] = f"{os.environ['SAPNWRFC_HOME']}/lib:{os.environ.get('DYLD_LIBRARY_PATH', '')}"

try:
    from pyrfc import Connection
    print("✅ pyrfc 모듈 import 성공!")
except ImportError as e:
    print(f"❌ pyrfc import 실패: {e}")
    print("\n다음 사항을 확인하세요:")
    print("1. SAPNWRFC_HOME 환경변수가 설정되었는지")
    print("2. SAP NWRFC SDK가 올바른 위치에 설치되었는지")
    print("3. pyrfc가 설치되었는지")
    sys.exit(1)

# SAP 연결 정보
from dotenv import load_dotenv
load_dotenv('.env')

SAP_CONFIG = {
    'ashost': os.getenv('SAP_ASHOST', '192.168.32.100'),  # SAP 서버 호스트
    'sysnr': os.getenv('SAP_SYSNR', '00'),                # System Number
    'client': os.getenv('SAP_CLIENT', '100'),             # Client 번호
    'user': os.getenv('SAP_USER', 'bc01'),                # SAP 사용자명
    'passwd': os.getenv('SAP_PASSWORD', 'dlsxjdpa2023@!'), # SAP 비밀번호
    'lang': os.getenv('SAP_LANG', 'KO'),                  # 언어
}

def test_connection():
    """SAP 연결 테스트"""
    print("\n🔗 SAP 연결 테스트 시작...")
    print(f"   호스트: {SAP_CONFIG['ashost']}")
    print(f"   시스템: {SAP_CONFIG['sysnr']}")
    print(f"   클라이언트: {SAP_CONFIG['client']}")
    
    try:
        # SAP 연결
        conn = Connection(**SAP_CONFIG)
        print("✅ SAP 연결 성공!")
        
        # 연결 정보 확인
        print("\n📊 연결 정보:")
        print(f"   버전: {conn.get_connection_attributes()}")
        
        # 간단한 RFC 호출 테스트 (서버 정보 조회)
        result = conn.call('RFC_SYSTEM_INFO')
        print("\n📋 시스템 정보:")
        for key, value in result.items():
            print(f"   {key}: {value}")
        
        # 연결 종료
        conn.close()
        print("\n✅ 연결 종료 완료")
        
    except Exception as e:
        print(f"❌ 연결 실패: {e}")
        print("\n연결 정보를 확인하고 SAP_CONFIG를 수정하세요.")

def test_table_read():
    """SAP 테이블 데이터 읽기 예제"""
    print("\n📖 SAP 테이블 읽기 테스트...")
    
    try:
        conn = Connection(**SAP_CONFIG)
        
        # RFC_READ_TABLE을 사용하여 테이블 데이터 읽기
        # 예: 회사 코드 테이블 (T001)
        result = conn.call('RFC_READ_TABLE',
                          QUERY_TABLE='T001',  # 읽을 테이블명
                          DELIMITER='|',
                          ROWCOUNT=10,          # 최대 행 수
                          FIELDS=[               # 읽을 필드
                              {'FIELDNAME': 'BUKRS'},  # 회사 코드
                              {'FIELDNAME': 'BUTXT'},  # 회사명
                          ])
        
        print(f"✅ {len(result['DATA'])}개 행 조회 성공")
        
        # 결과 파싱 및 출력
        fields = [f['FIELDNAME'] for f in result['FIELDS']]
        print(f"\n필드: {fields}")
        
        print("\n데이터:")
        for row in result['DATA'][:5]:  # 처음 5개만 출력
            values = row['WA'].split('|')
            for i, field in enumerate(fields):
                print(f"  {field}: {values[i].strip()}")
            print("  ---")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ 테이블 읽기 실패: {e}")

def create_sap_config_file():
    """SAP 연결 정보를 .env 파일에 저장"""
    env_content = """
# SAP RFC 연결 정보
SAP_ASHOST=YOUR_SAP_HOST
SAP_SYSNR=00
SAP_CLIENT=100
SAP_USER=YOUR_USERNAME
SAP_PASSWORD=YOUR_PASSWORD
SAP_LANG=KO

# SAP NWRFC SDK 경로
SAPNWRFC_HOME=~/sap/nwrfcsdk
"""
    
    env_file = '.env.sap'
    with open(env_file, 'w') as f:
        f.write(env_content)
    
    print(f"\n📄 {env_file} 파일이 생성되었습니다.")
    print("   실제 SAP 연결 정보로 수정해주세요.")

if __name__ == "__main__":
    print("=" * 50)
    print("SAP RFC 연결 테스트")
    print("=" * 50)
    
    # pyrfc 버전 확인
    try:
        import pyrfc
        print(f"📦 pyrfc 버전: {pyrfc.__version__}")
    except:
        pass
    
    # 연결 정보가 기본값인 경우 설정 파일 생성
    if SAP_CONFIG['ashost'] == 'YOUR_SAP_HOST':
        print("\n⚠️ SAP 연결 정보가 설정되지 않았습니다.")
        create_sap_config_file()
        print("\n1. 위 파일에 실제 SAP 연결 정보를 입력하세요.")
        print("2. 이 스크립트의 SAP_CONFIG 부분을 수정하세요.")
        print("3. 다시 실행하세요.")
    else:
        # 연결 테스트
        test_connection()
        
        # 테이블 읽기 테스트 (옵션)
        # test_table_read()