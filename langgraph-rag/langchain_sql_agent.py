"""
LangChain SQL ReAct Agent - Production Ready
자연어를 SQL로 변환하고 실행하는 고급 에이전트
ReAct (Reasoning + Acting) 패턴을 사용하여 동적으로 문제 해결
"""

from typing import List, Dict, Any, Optional
from langchain_openai import AzureChatOpenAI
from langchain_community.utilities import SQLDatabase
from langchain_community.agent_toolkits import create_sql_agent
from langchain.agents.agent_types import AgentType
from langchain_core.callbacks import StreamingStdOutCallbackHandler
from langchain_core.outputs import LLMResult
import os
import time
from datetime import datetime
import json
from dotenv import load_dotenv
import asyncio
import traceback
from dataclasses import dataclass, field
import re
from sqlalchemy import text

load_dotenv()

# ========================
# 성능 메트릭 추적
# ========================

@dataclass
class AgentMetrics:
    """
    에이전트 성능 메트릭을 추적하는 데이터 클래스
    
    각 쿼리 실행에 대한 상세한 메트릭 정보를 수집하여
    성능 분석과 최적화에 활용합니다.
    """
    query: str  # 사용자의 원본 쿼리
    start_time: float = field(default_factory=time.time)  # 시작 시간
    end_time: Optional[float] = None  # 종료 시간
    total_tokens: int = 0  # 총 토큰 사용량
    prompt_tokens: int = 0  # 프롬프트 토큰
    completion_tokens: int = 0  # 완성 토큰
    tool_calls: int = 0  # 도구 호출 횟수
    error_recoveries: int = 0  # 에러 복구 시도 횟수
    success: bool = False  # 성공 여부
    error_message: Optional[str] = None  # 에러 메시지 (실패 시)
    sql_generated: Optional[str] = None  # 생성된 SQL 쿼리
    result_count: int = 0  # 반환된 결과 행 수
    
    def finalize(self):
        """메트릭 수집을 완료하고 종료 시간을 기록"""
        self.end_time = time.time()
        
    @property
    def duration(self) -> float:
        """실행 시간을 초 단위로 계산"""
        if self.end_time:
            return self.end_time - self.start_time
        return time.time() - self.start_time
    
    def to_dict(self) -> Dict:
        """메트릭을 딕셔너리로 변환 (JSON 직렬화용)"""
        return {
            "query": self.query,
            "duration": f"{self.duration:.2f}s",
            "total_tokens": self.total_tokens,
            "tool_calls": self.tool_calls,
            "error_recoveries": self.error_recoveries,
            "success": self.success,
            "result_count": self.result_count,
            "sql": self.sql_generated,
            "error": self.error_message
        }


class TokenCountingCallback(StreamingStdOutCallbackHandler):
    """
    LLM 호출 시 토큰 사용량을 추적하는 콜백 핸들러
    
    Azure OpenAI의 응답에서 토큰 사용량 정보를 추출하여
    메트릭 객체에 누적합니다.
    """
    
    def __init__(self, metrics: AgentMetrics):
        super().__init__()
        self.metrics = metrics
    
    def on_llm_end(self, response: LLMResult, **kwargs: Any):
        """
        LLM 호출이 완료될 때 호출되는 콜백
        응답에서 토큰 사용량 정보를 추출합니다.
        """
        if response.llm_output and 'token_usage' in response.llm_output:
            usage = response.llm_output['token_usage']
            self.metrics.prompt_tokens += usage.get('prompt_tokens', 0)
            self.metrics.completion_tokens += usage.get('completion_tokens', 0)
            self.metrics.total_tokens += usage.get('total_tokens', 0)


class LangChainSQLAgent:
    """
    LangChain 기반 SQL ReAct Agent
    
    자연어 쿼리를 SQL로 변환하고 실행하는 지능형 에이전트입니다.
    ReAct 패턴을 사용하여 단계적으로 추론하고 행동하며,
    에러 발생 시 자동으로 복구를 시도합니다.
    
    주요 기능:
    - 자연어 → SQL 변환
    - 자동 스키마 탐색
    - SQL 쿼리 검증 및 실행
    - 에러 자동 복구 (최대 5회)
    - 결과 포맷팅
    - 성능 메트릭 추적
    """
    
    def __init__(
        self, 
        db_url: str,
        max_iterations: int = 5,
        enable_streaming: bool = False,
        verbose: bool = False
    ):
        """
        SQL Agent 초기화
        
        Args:
            db_url: 데이터베이스 연결 URL
            max_iterations: 최대 ReAct 반복 횟수 (기본값: 5)
            enable_streaming: 스트리밍 출력 활성화 여부
            verbose: 상세 로그 출력 여부
        """
        self.db_url = db_url
        self.max_iterations = max_iterations
        self.enable_streaming = enable_streaming
        self.verbose = verbose
        
        # 성능 메트릭 히스토리
        self.metrics_history: List[AgentMetrics] = []
        
        # 쿼리 결과 제한 설정
        self.max_rows = 1000  # 최대 반환 행 수
        self.default_limit = 100  # 기본 LIMIT 값
        
        # 컴포넌트 초기화
        self._initialize_components()
    
    def _initialize_components(self):
        """
        에이전트 컴포넌트 초기화
        
        데이터베이스 연결, LLM, 프롬프트 템플릿 등
        필요한 모든 컴포넌트를 설정합니다.
        """
        
        # 1. 데이터베이스 연결 설정
        # SQLAlchemy를 통해 PostgreSQL 데이터베이스에 연결
        self.db = SQLDatabase.from_uri(
            self.db_url,
            sample_rows_in_table_info=3,  # 스키마 정보에 샘플 데이터 3행 포함
            include_tables=None,  # None = 모든 테이블 포함
            view_support=True  # View도 테이블처럼 쿼리 가능
        )
        
        # 2. Azure OpenAI LLM 설정
        # SQL 생성에 특화된 설정으로 LLM 초기화
        self.llm = AzureChatOpenAI(
            deployment_name=os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME", "gpt-4o"),
            api_key=os.getenv("AZURE_OPENAI_API_KEY"),
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
            api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2024-08-01-preview"),
            temperature=0,  # 0 = 결정론적 (SQL은 정확해야 하므로)
            max_tokens=2000  # SQL 쿼리와 응답에 충분한 토큰
        )
        
        # 3. 시스템 프롬프트 설정
        # 에이전트의 행동 지침과 SQL 생성 규칙을 정의
        prefix = """You are an expert SQL analyst with deep expertise in PostgreSQL.

## 극도로 중요: 반드시 SQL 쿼리를 실행해야 합니다!

사용자가 데이터를 요청할 때는 반드시:
1. sql_db_list_tables로 테이블 목록 확인
2. sql_db_schema로 테이블 구조 확인
3. SQL 쿼리 작성
4. **sql_db_query로 쿼리 실행** (필수! 텍스트로만 답변하지 말고 실제 데이터를 보여주세요)
5. 실제 쿼리 결과를 반환

단순히 SQL을 말로만 설명하지 말고, 반드시 sql_db_query를 사용하여 실제 데이터를 가져와야 합니다.

## 주요 목표
자연어 질문을 효율적이고 안전한 SQL 쿼리로 변환하여 정확한 결과를 제공합니다.

## 엄격한 규칙
1. **보안 최우선**: SELECT 쿼리만 허용. INSERT/UPDATE/DELETE/DROP 절대 금지
2. **효율성**: 대용량 결과 방지를 위해 항상 LIMIT 추가 (기본값: 100)
3. **정확성**: 대소문자 구분이 필요한 컬럼명은 큰따옴표 사용

## 쿼리 작성 가이드
- 명시적 JOIN 구문 사용 (INNER JOIN, LEFT JOIN 등)
- NULL 처리는 IS NULL/IS NOT NULL 사용
- 텍스트 범위 컬럼 (예: "10~50")은 숫자가 아닌 TEXT로 처리
- 적절한 집계 함수 사용 (COUNT, SUM, AVG, MIN, MAX)
- GROUP BY에 집계되지 않은 모든 컬럼 포함
- 중복 제거 시 DISTINCT 사용
- 정렬된 결과를 위해 ORDER BY 추가

## 에러 복구 전략
쿼리 실패 시:
1. 에러 메시지 상세 분석
2. 컬럼명과 데이터 타입 재확인
3. 테이블 관계 검증
4. 대체 쿼리 접근 방식 시도
5. 최대 5회까지 재시도 (절대 포기하지 않음)

사용 가능한 도구:"""
        
        suffix = """시작!

질문: {input}
생각: 반드시 SQL 쿼리를 실행해야 합니다. 먼저 데이터베이스의 테이블을 확인하여 스키마를 이해해야 합니다.
{agent_scratchpad}"""
        
        # 4. SQL Agent 생성
        # create_sql_agent를 사용하여 모든 컴포넌트를 통합
        self.agent = create_sql_agent(
            llm=self.llm,
            db=self.db,
            agent_type=AgentType.OPENAI_FUNCTIONS,  # OpenAI 함수 호출 방식 사용
            verbose=self.verbose,  # 디버깅용 상세 출력
            max_iterations=self.max_iterations,  # ReAct 최대 반복 횟수
            max_execution_time=60,  # 최대 실행 시간 60초 (무한 루프 방지)
            early_stopping_method="force",  # 시간/반복 제한 도달 시 강제 종료
            handle_parsing_errors=True,  # 파싱 에러 자동 처리
            prefix=prefix,  # 시스템 프롬프트 앞부분
            suffix=suffix,  # 시스템 프롬프트 뒷부분
            format_instructions=None,  # 기본 포맷 사용
            input_variables=None,  # 기본 변수 사용
            return_intermediate_steps=True  # 중간 단계 반환
        )
    
    def _create_callbacks(self, metrics: AgentMetrics):
        """
        에이전트 실행 시 사용할 콜백 핸들러 생성
        
        토큰 카운팅과 스트리밍 출력을 위한 콜백을 설정합니다.
        """
        callbacks = []
        
        # 토큰 사용량 추적
        callbacks.append(TokenCountingCallback(metrics))
        
        # 스트리밍 출력 (활성화된 경우)
        if self.enable_streaming:
            callbacks.append(StreamingStdOutCallbackHandler())
        
        return callbacks
    
    async def run(
        self,
        query: str,
        session_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        SQL Agent 실행 (비동기)
        
        사용자의 자연어 쿼리를 받아 SQL로 변환하고 실행합니다.
        전체 프로세스를 추적하며 상세한 메트릭을 반환합니다.
        
        Args:
            query: 사용자의 자연어 질문
            session_id: 세션 ID (향후 대화 컨텍스트 관리용)
            
        Returns:
            실행 결과와 메트릭을 포함한 딕셔너리
            - success: 성공 여부
            - query: 원본 쿼리
            - response: 에이전트 응답
            - sql: 생성된 SQL 쿼리
            - results: 쿼리 실행 결과 (행 데이터)
            - metrics: 성능 메트릭
            - execution_time: 실행 시간
            - error: 에러 메시지 (실패 시)
        """
        # 실행 시작 로그
        if self.verbose:
            print(f"\n{'='*80}")
            print(f"🤖 LangChain SQL Agent 시작")
            print(f"📝 쿼리: {query}")
            print(f"🕐 시작 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"{'='*80}\n")
        
        # 메트릭 초기화
        metrics = AgentMetrics(query=query)
        
        try:
            # 콜백 설정
            callbacks = self._create_callbacks(metrics)
            
            # 에이전트 실행 시작
            start_time = time.time()
            
            # 에이전트 호출 (ReAct 루프 실행)
            # SQL 실행을 강제하기 위해 명령 추가
            enhanced_query = f"{query}\n\n중요: 반드시 sql_db_query 도구를 사용하여 SQL을 실행하고 실제 데이터를 반환하세요."
            result = await self.agent.ainvoke(
                {"input": enhanced_query},
                config={"callbacks": callbacks}
            )
            
            execution_time = time.time() - start_time
            
            # 결과에서 응답 메시지 추출
            output_message = result.get("output", "")
            
            # 중간 단계에서 SQL 쿼리와 결과 추출
            sql_query = None
            results = None
            intermediate_steps = result.get("intermediate_steps", [])
            
            for action, observation in intermediate_steps:
                # sql_db_query 액션에서 SQL 추출
                if hasattr(action, 'tool') and action.tool == 'sql_db_query':
                    sql_query = action.tool_input
                    if self.verbose:
                        print(f"🔍 SQL 쿼리 발견: {sql_query[:100]}...")
                    metrics.sql_generated = sql_query
                    
                    # observation에서 결과 파싱 시도
                    if observation:
                        # SQLDatabase.run의 결과는 텍스트 형식이므로 파싱 필요
                        results = self._parse_sql_observation(observation)
                        if results:
                            metrics.result_count = len(results)
                            if self.verbose:
                                print(f"📊 쿼리 결과: {len(results)}개 행")
            
            # intermediate_steps에서 SQL을 찾지 못한 경우 output에서 추출
            if not sql_query:
                sql_query = self._extract_sql_from_output(output_message)
                if sql_query:
                    metrics.sql_generated = sql_query
                    # SQL을 직접 실행하여 결과 가져오기
                    results = self._execute_sql_and_get_results(sql_query)
                    if results:
                        metrics.result_count = len(results)
                        if self.verbose:
                            print(f"📊 쿼리 결과: {len(results)}개 행")
            
            # 여전히 결과가 없으면 출력에서 추출 시도 (폴백)
            if not results:
                results = self._extract_results_from_output(output_message)
                if results:
                    metrics.result_count = len(results)
            
            # 마지막 시도: 출력 메시지에서 테이블 형식 찾기
            if not results and "|" in output_message:
                results = self._parse_table_from_text(output_message)
                if results:
                    metrics.result_count = len(results)
            
            # 성공 처리
            metrics.success = True
            metrics.finalize()
            
            # 메트릭 히스토리에 저장
            self.metrics_history.append(metrics)
            
            if self.verbose:
                print(f"\n{'='*80}")
                print(f"✅ 성공: {execution_time:.2f}초")
                print(f"📊 토큰: {metrics.total_tokens} | 도구 호출: {metrics.tool_calls}")
                if sql_query:
                    print(f"📝 SQL: {sql_query[:100]}...")
                print(f"{'='*80}\n")
            
            return {
                "success": True,
                "query": query,
                "response": output_message,
                "sql": sql_query,
                "results": results,
                "metrics": metrics.to_dict(),
                "execution_time": execution_time
            }
            
        except Exception as e:
            # 에러 처리
            metrics.success = False
            metrics.error_message = str(e)
            metrics.finalize()
            
            # 메트릭 히스토리에 저장
            self.metrics_history.append(metrics)
            
            if self.verbose:
                print(f"\n❌ 에러: {str(e)}")
                traceback.print_exc()
            
            return {
                "success": False,
                "query": query,
                "error": str(e),
                "metrics": metrics.to_dict()
            }
    
    def run_sync(self, query: str, session_id: Optional[str] = None) -> Dict[str, Any]:
        """
        SQL Agent 실행 (동기)
        
        비동기 run 메서드의 동기 버전입니다.
        """
        return asyncio.run(self.run(query, session_id))
    
    def _extract_sql_from_output(self, output: str) -> Optional[str]:
        """
        에이전트 출력에서 SQL 쿼리 추출
        
        응답 텍스트에서 SQL 코드 블록이나 SELECT 문을 찾아 추출합니다.
        
        Args:
            output: 에이전트의 응답 텍스트
            
        Returns:
            추출된 SQL 쿼리 문자열 또는 None
        """
        if not output:
            return None
            
        # 방법 1: SQL 코드 블록 찾기 (```sql ... ```)
        sql_pattern = r'```sql\s*(.*?)\s*```'
        matches = re.findall(sql_pattern, output, re.DOTALL | re.IGNORECASE)
        if matches:
            return matches[0].strip()
        
        # 방법 2: SELECT로 시작하는 문장 찾기
        lines = output.split('\n')
        sql_lines = []
        in_query = False
        
        for line in lines:
            # SELECT로 시작하는 줄 찾기
            if line.strip().upper().startswith('SELECT'):
                in_query = True
                sql_lines = [line]
            elif in_query:
                sql_lines.append(line)
                # 세미콜론으로 끝나면 쿼리 완료
                if ';' in line:
                    break
        
        if sql_lines:
            return '\n'.join(sql_lines).strip()
        
        return None
    
    def _parse_sql_observation(self, observation: str) -> Optional[List[Dict]]:
        """
        SQL 실행 결과 (observation)를 파싱하여 딕셔너리 리스트로 변환
        
        Args:
            observation: sql_db_query 도구의 출력 (테이블 형식 텍스트)
            
        Returns:
            결과 데이터의 딕셔너리 리스트 또는 None
        """
        try:
            if not observation or observation.strip() == "":
                return None
            
            # 결과를 파싱하여 딕셔너리 리스트로 변환
            lines = observation.strip().split('\n')
            if len(lines) < 2:  # 최소한 헤더와 데이터 1줄이 있어야 함
                return None
            
            # 첫 줄을 헤더로 사용
            headers = [h.strip() for h in lines[0].split('|') if h.strip()]
            
            results = []
            for line in lines[1:]:
                if '|' in line:  # 데이터 행인 경우
                    values = [v.strip() for v in line.split('|')]
                    # 빈 문자열 제거하지 않고 헤더와 같은 수만 확인
                    if len(values) == len(headers):
                        row_dict = {headers[i]: values[i].strip() for i in range(len(headers))}
                        results.append(row_dict)
            
            # 최대 행 수 제한
            if len(results) > self.max_rows:
                results = results[:self.max_rows]
            
            return results if results else None
            
        except Exception as e:
            if self.verbose:
                print(f"Observation 파싱 에러: {str(e)}")
            return None
    
    def _extract_results_from_output(self, output: str) -> Optional[List[Dict]]:
        """
        에이전트 출력에서 쿼리 실행 결과 추출
        
        응답에서 테이블 형태의 데이터를 찾아 딕셔너리 리스트로 변환합니다.
        현재는 기본 구현이며, 필요에 따라 확장 가능합니다.
        
        Args:
            output: 에이전트의 응답 텍스트
            
        Returns:
            결과 데이터의 딕셔너리 리스트 또는 None
        """
        # TODO: 응답에서 실제 데이터 추출 로직 구현
        # 현재는 None 반환 (에이전트가 직접 포맷팅한 결과 사용)
        return None
    
    def _parse_table_from_text(self, text: str) -> Optional[List[Dict]]:
        """
        텍스트에서 마크다운 테이블 형식 파싱
        
        Args:
            text: 테이블을 포함한 텍스트
            
        Returns:
            파싱된 데이터 리스트
        """
        try:
            lines = text.split('\n')
            table_lines = []
            in_table = False
            
            for line in lines:
                # 테이블 시작 찾기
                if '|' in line and not in_table:
                    in_table = True
                    table_lines = [line]
                elif in_table and '|' in line:
                    table_lines.append(line)
                elif in_table and '|' not in line and len(table_lines) > 2:
                    # 테이블 끝
                    break
            
            if len(table_lines) < 3:  # 최소 헤더, 구분선, 데이터 1줄
                return None
            
            # 헤더 파싱
            header_line = table_lines[0]
            headers = [h.strip() for h in header_line.split('|') if h.strip()]
            
            # 데이터 파싱
            results = []
            for line in table_lines[2:]:  # 구분선 다음부터
                if '---' in line or '===' in line:  # 구분선 스킵
                    continue
                values = [v.strip() for v in line.split('|') if v.strip() != '']
                if len(values) == len(headers):
                    row = {headers[i]: values[i] for i in range(len(headers))}
                    results.append(row)
            
            return results if results else None
            
        except Exception as e:
            if self.verbose:
                print(f"테이블 파싱 에러: {str(e)}")
            return None
    
    def _execute_sql_and_get_results(self, sql: str) -> Optional[List[Dict]]:
        """
        SQL 쿼리를 직접 실행하고 결과를 딕셔너리 리스트로 반환
        
        Args:
            sql: 실행할 SQL 쿼리
            
        Returns:
            결과 데이터의 딕셔너리 리스트 또는 None
        """
        try:
            # 직접 DB 연결을 사용하여 쿼리 실행
            with self.db._engine.connect() as conn:
                result = conn.execute(text(sql))
                
                # 열 이름 가져오기
                columns = list(result.keys())
                
                # 결과를 딕셔너리 리스트로 변환
                results = []
                for row in result:
                    row_dict = {columns[i]: row[i] for i in range(len(columns))}
                    results.append(row_dict)
                    
                    # 최대 행 수 확인
                    if len(results) >= self.max_rows:
                        break
                
                return results if results else None
            
        except Exception as e:
            if self.verbose:
                print(f"SQL 실행 중 에러: {str(e)}")
            
            # 폴백: SQLDatabase.run 메서드 사용
            try:
                result_str = self.db.run(sql)
                
                if not result_str or result_str.strip() == "":
                    return None
                
                # 결과를 파싱하여 딕셔너리 리스트로 변환
                lines = result_str.strip().split('\n')
                if len(lines) < 2:  # 최소한 헤더와 데이터 1줄이 있어야 함
                    return None
                
                # 첫 줄을 헤더로 사용
                headers = [h.strip() for h in lines[0].split('|') if h.strip()]
                
                results = []
                for line in lines[1:]:
                    if '|' in line:  # 데이터 행인 경우
                        values = [v.strip() for v in line.split('|')]
                        # 빈 문자열 제거하지 않고 헤더와 같은 수만 확인
                        if len(values) == len(headers):
                            row_dict = {headers[i]: values[i].strip() for i in range(len(headers))}
                            results.append(row_dict)
                
                # 최대 행 수 제한
                if len(results) > self.max_rows:
                    results = results[:self.max_rows]
                
                return results if results else None
                
            except Exception as e2:
                if self.verbose:
                    print(f"SQL 실행 폴백 에러: {str(e2)}")
                return None
    
    def get_metrics_summary(self) -> Dict[str, Any]:
        """
        누적된 메트릭 요약 통계 반환
        
        에이전트가 실행된 모든 쿼리의 통계 정보를 제공합니다.
        성능 분석과 최적화에 활용할 수 있습니다.
        
        Returns:
            요약 통계 딕셔너리
            - total_queries: 총 쿼리 수
            - successful: 성공한 쿼리 수
            - failed: 실패한 쿼리 수
            - success_rate: 성공률
            - avg_duration: 평균 실행 시간
            - avg_tokens: 평균 토큰 사용량
            - total_tokens: 총 토큰 사용량
            - queries: 최근 5개 쿼리 상세 정보
        """
        if not self.metrics_history:
            return {"message": "아직 실행된 쿼리가 없습니다"}
        
        successful = [m for m in self.metrics_history if m.success]
        failed = [m for m in self.metrics_history if not m.success]
        
        avg_duration = sum(m.duration for m in self.metrics_history) / len(self.metrics_history)
        avg_tokens = sum(m.total_tokens for m in self.metrics_history) / len(self.metrics_history)
        
        return {
            "total_queries": len(self.metrics_history),
            "successful": len(successful),
            "failed": len(failed),
            "success_rate": f"{(len(successful) / len(self.metrics_history) * 100):.1f}%",
            "avg_duration": f"{avg_duration:.2f}s",
            "avg_tokens": int(avg_tokens),
            "total_tokens": sum(m.total_tokens for m in self.metrics_history),
            "queries": [m.to_dict() for m in self.metrics_history[-5:]]  # 최근 5개
        }
    
    def clear_metrics(self):
        """메트릭 히스토리 초기화"""
        self.metrics_history.clear()
    
    def set_max_rows(self, max_rows: int):
        """
        최대 반환 행 수 설정
        
        Args:
            max_rows: 최대 행 수 (1-10000)
        """
        self.max_rows = min(max(1, max_rows), 10000)
        self.default_limit = min(self.max_rows, 100)