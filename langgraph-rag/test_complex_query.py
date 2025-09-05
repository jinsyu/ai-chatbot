#!/usr/bin/env python3
"""
복잡한 쿼리 테스트 스크립트
부족수량 분석 쿼리를 단계별로 테스트
"""

import asyncio
import os
from dotenv import load_dotenv
from langchain_sql_agent import LangChainSQLAgent

load_dotenv()

async def test_complex_query():
    """복잡한 쿼리 테스트"""
    
    # 데이터베이스 경로
    db_url = "sqlite:///db.sqlite"
    
    # 에이전트 생성 (verbose 모드 활성화)
    agent = LangChainSQLAgent(
        db_url=db_url,
        max_iterations=30,  # 충분한 반복 횟수
        enable_streaming=False,
        verbose=True  # 디버그 출력 활성화
    )
    
    # 복잡한 쿼리 테스트
    query = """
    부족수량이 가장 많은 자재 1개에 대해:
    1. 자재코드와 자재명
    2. 총 부족수량
    3. 최근 3개월 월별 판매량
    4. 자재 마스터 정보 (제품유형, 제품군, 가격)
    모두 보여주세요.
    """
    
    print("\n" + "="*80)
    print("📋 테스트 쿼리:")
    print(query)
    print("="*80 + "\n")
    
    # 에이전트 실행
    result = await agent.run(query)
    
    # 결과 출력
    print("\n" + "="*80)
    print("📊 실행 결과:")
    print("="*80)
    
    if result['success']:
        print(f"✅ 성공")
        print(f"⏱️ 실행 시간: {result.get('execution_time', 0):.2f}초")
        print(f"🔧 생성된 SQL: {result.get('sql', 'N/A')}")
        print(f"📊 결과 행 수: {len(result.get('results', [])) if result.get('results') else 0}")
        
        print("\n📝 응답:")
        print(result.get('response', 'No response'))
        
        # 메트릭 출력
        metrics = result.get('metrics', {})
        print(f"\n📈 메트릭:")
        print(f"  - 토큰 사용: {metrics.get('total_tokens', 0)}")
        print(f"  - 도구 호출: {metrics.get('tool_calls', 0)}")
        print(f"  - 결과 수: {metrics.get('result_count', 0)}")
        
    else:
        print(f"❌ 실패")
        print(f"에러: {result.get('error', 'Unknown error')}")
    
    return result

async def test_simple_queries():
    """간단한 쿼리들 테스트"""
    
    db_url = "sqlite:///db.sqlite"
    agent = LangChainSQLAgent(
        db_url=db_url,
        max_iterations=20,
        verbose=False  # 간단한 테스트는 verbose off
    )
    
    simple_queries = [
        "재고 테이블에 몇 개의 행이 있나요?",
        "자재 471422의 정보를 보여주세요",
        "최근 1개월 판매 데이터 5개만 보여주세요"
    ]
    
    print("\n" + "="*80)
    print("간단한 쿼리 테스트")
    print("="*80)
    
    for q in simple_queries:
        print(f"\n테스트: {q}")
        result = await agent.run(q)
        if result['success']:
            print(f"  ✅ 성공 ({result.get('execution_time', 0):.2f}초)")
        else:
            print(f"  ❌ 실패: {result.get('error', '')}")

if __name__ == "__main__":
    print("🚀 SQL Agent 테스트 시작...\n")
    
    # 간단한 쿼리 먼저 테스트
    asyncio.run(test_simple_queries())
    
    # 복잡한 쿼리 테스트
    print("\n" + "="*80)
    print("복잡한 쿼리 테스트 시작")
    print("="*80)
    
    asyncio.run(test_complex_query())
    
    print("\n✅ 테스트 완료!")