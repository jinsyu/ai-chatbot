"""
Text-to-SQL Agent using LangGraph
자연어 쿼리를 SQL로 변환하고 실행하는 전용 에이전트
"""

from typing import List, TypedDict, Annotated, Dict, Any, Optional
from langchain_core.messages import BaseMessage, HumanMessage, AIMessage, SystemMessage
from langchain_openai import AzureChatOpenAI
from langgraph.graph import StateGraph, END
from langgraph.graph.message import add_messages
from sqlalchemy import create_engine, text, inspect
import os
import re
from dotenv import load_dotenv
from datetime import datetime
import json
from decimal import Decimal
import traceback

load_dotenv()

# ========================
# Configuration
# ========================

# Decimal을 처리할 수 있는 JSON Encoder
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)

# Azure OpenAI 설정
def get_llm(temperature=0.0):
    """Azure OpenAI LLM 인스턴스 생성"""
    return AzureChatOpenAI(
        deployment_name=os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME", "gpt-4o"),
        api_key=os.getenv("AZURE_OPENAI_API_KEY"),
        azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
        api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2024-08-01-preview"),
        temperature=temperature,
        # max_tokens=2000
    )

# ========================
# State Definition
# ========================

class TextToSqlState(TypedDict):
    """Text-to-SQL 에이전트의 상태"""
    messages: Annotated[List[BaseMessage], add_messages]
    user_query: str
    conversation_history: Optional[List[Dict]]
    schema_info: Optional[str]
    generated_sql: Optional[str]
    validation_result: Optional[Dict]
    query_results: Optional[List[Dict]]
    formatted_response: Optional[str]
    error: Optional[str]
    retry_count: int

# ========================
# Text-to-SQL Node
# ========================

class TextToSqlNode:
    """
    🗄️ Text-to-SQL 노드
    사용자의 자연어 질의를 SQL로 변환하고 실행하는 핵심 노드
    """
    
    def __init__(self, db_url: str):
        self.db_url = db_url
        # 대용량 데이터 처리를 위한 설정
        self.engine = create_engine(
            db_url,
            pool_size=20,  # 커넥션 풀 크기 증가
            max_overflow=40,  # 최대 오버플로우 증가
            pool_timeout=1200,  # 타임아웃 20분으로 증가
            pool_recycle=3600  # 1시간마다 커넥션 재활용
        )
        self.llm = get_llm()
        self.max_retries = 3
        self.max_rows = 100000  # 최대 10,000행 반환
        self.default_limit = 500000  # 기본 LIMIT 50만건
    
    def get_detailed_schema(self) -> str:
        """데이터베이스 스키마 정보를 상세히 추출"""
        inspector = inspect(self.engine)
        schema_info = []
        
        for table_name in inspector.get_table_names():
            schema_info.append(f"\nTable: {table_name}")
            
            # 컬럼 정보
            columns = inspector.get_columns(table_name)
            for col in columns:
                col_type = str(col['type'])
                nullable = " (nullable)" if col['nullable'] else " (NOT NULL)"
                default = f" DEFAULT {col['default']}" if col.get('default') else ""
                schema_info.append(f"  - {col['name']}: {col_type}{nullable}{default}")
            
            # Primary Key
            pk = inspector.get_pk_constraint(table_name)
            if pk and pk['constrained_columns']:
                schema_info.append(f"  Primary Key: {', '.join(pk['constrained_columns'])}")
            
            # Foreign Keys
            fks = inspector.get_foreign_keys(table_name)
            for fk in fks:
                schema_info.append(f"  Foreign Key: {', '.join(fk['constrained_columns'])} -> {fk['referred_table']}.{', '.join(fk['referred_columns'])}")
            
            # Indexes
            indexes = inspector.get_indexes(table_name)
            for idx in indexes:
                if not idx.get('unique'):
                    schema_info.append(f"  Index: {idx['name']} on ({', '.join(idx['column_names'])})")
        
        return "\n".join(schema_info)
    
    async def analyze_schema(self, state: TextToSqlState) -> Dict:
        """스키마 분석 단계"""
        print("\n📊 Analyzing database schema...")
        
        try:
            schema_info = self.get_detailed_schema()
            
            # 스키마 요약 생성
            table_count = schema_info.count('\nTable:')
            
            return {
                "schema_info": schema_info,
                "messages": [AIMessage(content=f"Schema analyzed: {table_count} tables found")]
            }
        except Exception as e:
            error_msg = f"Schema analysis failed: {str(e)}"
            print(f"❌ {error_msg}")
            return {
                "error": error_msg,
                "messages": [AIMessage(content=error_msg)]
            }
    
    async def generate_sql(self, state: TextToSqlState) -> Dict:
        """SQL 생성 단계"""
        print("\n🔧 Generating SQL query...")
        
        if not state.get('schema_info'):
            return {
                "error": "No schema information available",
                "messages": [AIMessage(content="Schema information is required")]
            }
        
        # 향상된 프롬프트
        prompt = f"""You are an expert PostgreSQL query generator. Convert the natural language query to SQL.

DATABASE SCHEMA:
{state['schema_info']}

STRICT RULES:
1. Generate ONLY SELECT queries (read-only operations)
2. Use double quotes for case-sensitive column names (e.g., "userId", "createdAt")
3. Use single quotes for string values
4. Always use explicit JOIN syntax, never implicit joins
5. Add appropriate WHERE clauses for filtering
6. Include ORDER BY for sorted results
7. Add LIMIT clause to prevent large result sets (default: LIMIT 100)
8. Use aggregate functions (COUNT, SUM, AVG, etc.) when appropriate
9. Handle NULL values properly with IS NULL/IS NOT NULL
10. Use DISTINCT when uniqueness is required
11. IMPORTANT: price_per_kg column contains text ranges like "10~50", NOT numeric values - DO NOT cast to NUMERIC

COMMON PATTERNS:
- Date filtering: Use DATE_TRUNC() or date comparison operators
- Text search: Use ILIKE for case-insensitive matching
- Counting: Use COUNT(*) for row counts, COUNT(DISTINCT column) for unique counts
- Grouping: Always include non-aggregate columns in GROUP BY
- Price columns: Usually stored as TEXT with ranges (e.g., "10~50"), not numeric values

IMPORTANT SCHEMA NOTES:
- If you see columns like 'price', 'price_per_kg', etc., they are likely TEXT fields with ranges
- For aggregations on price fields, use COUNT instead of SUM
- For sales/revenue calculations, look for actual numeric columns, not price range fields

USER QUERY: {state['user_query']}

Generate a single PostgreSQL query. Return ONLY the SQL query without any explanation or markdown:
"""
        
        try:
            response = await self.llm.ainvoke([SystemMessage(content=prompt)])
            sql_query = response.content.strip()
            
            # SQL 정리
            sql_query = re.sub(r'```sql\s*', '', sql_query)
            sql_query = re.sub(r'```\s*', '', sql_query)
            sql_query = sql_query.strip()
            
            # 세미콜론 확인
            if not sql_query.endswith(';'):
                sql_query += ';'
            
            print(f"Generated SQL: {sql_query}")
            
            return {
                "generated_sql": sql_query,
                "messages": [AIMessage(content=f"SQL generated successfully")]
            }
            
        except Exception as e:
            error_msg = f"SQL generation failed: {str(e)}"
            print(f"❌ {error_msg}")
            return {
                "error": error_msg,
                "messages": [AIMessage(content=error_msg)]
            }
    
    async def validate_sql(self, state: TextToSqlState) -> Dict:
        """SQL 검증 단계"""
        print("\n✅ Validating SQL query...")
        
        sql = state.get('generated_sql', '')
        
        if not sql:
            return {
                "validation_result": {"valid": False, "reason": "No SQL query to validate"},
                "messages": [AIMessage(content="No SQL query to validate")]
            }
        
        # 보안 검증
        forbidden_keywords = [
            'INSERT', 'UPDATE', 'DELETE', 'DROP', 'CREATE', 'ALTER', 
            'TRUNCATE', 'GRANT', 'REVOKE', 'EXECUTE', 'EXEC'
        ]
        
        sql_upper = sql.upper()
        
        for keyword in forbidden_keywords:
            if re.search(r'\b' + keyword + r'\b', sql_upper):
                return {
                    "validation_result": {"valid": False, "reason": f"Forbidden operation: {keyword}"},
                    "error": f"Security violation: {keyword} operations not allowed",
                    "messages": [AIMessage(content=f"Forbidden operation detected: {keyword}")]
                }
        
        # SELECT 또는 WITH로 시작하는지 확인
        if not (sql_upper.startswith('SELECT') or sql_upper.startswith('WITH')):
            return {
                "validation_result": {"valid": False, "reason": "Must be SELECT query"},
                "error": "Only SELECT queries are allowed",
                "messages": [AIMessage(content="Query must start with SELECT or WITH")]
            }
        
        # SQL Injection 패턴 체크
        dangerous_patterns = [
            r';\s*DROP',
            r';\s*DELETE',
            r'--\s*',
            r'/\*.*\*/',
            r'UNION\s+ALL\s+SELECT.*FROM\s+information_schema'
        ]
        
        for pattern in dangerous_patterns:
            if re.search(pattern, sql, re.IGNORECASE):
                return {
                    "validation_result": {"valid": False, "reason": "Dangerous pattern detected"},
                    "error": "Potentially dangerous SQL pattern detected",
                    "messages": [AIMessage(content="Security risk detected in query")]
                }
        
        # LIMIT 체크 및 추가 (대용량 데이터 처리)
        if 'LIMIT' not in sql_upper:
            sql = sql.replace(';', f' LIMIT {self.default_limit};')
            state['generated_sql'] = sql
            print(f"Added LIMIT {self.default_limit} to query")
        
        return {
            "validation_result": {"valid": True, "sql": sql},
            "generated_sql": sql,
            "messages": [AIMessage(content="SQL validation passed")]
        }
    
    async def execute_query(self, state: TextToSqlState) -> Dict:
        """쿼리 실행 단계"""
        print("\n🚀 Executing SQL query...")
        
        validation = state.get('validation_result', {})
        if not validation.get('valid'):
            return {
                "error": "Cannot execute invalid query",
                "messages": [AIMessage(content="Query validation failed")]
            }
        
        sql = state.get('generated_sql', '')
        
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(sql))
                rows = result.fetchall()
                
                # 결과를 딕셔너리 리스트로 변환 (대용량 데이터 처리)
                if rows:
                    columns = result.keys()
                    results = []
                    # 최대 max_rows까지 처리
                    for row in rows[:self.max_rows]:
                        row_dict = {}
                        for i, col in enumerate(columns):
                            value = row[i]
                            # Decimal, datetime 등 처리
                            if isinstance(value, Decimal):
                                value = float(value)
                            elif isinstance(value, datetime):
                                value = value.isoformat()
                            row_dict[col] = value
                        results.append(row_dict)
                    
                    # 전체 행 수 정보 추가
                    total_rows = len(rows)
                    if total_rows > self.max_rows:
                        print(f"⚠️ Results truncated: showing {self.max_rows} of {total_rows} rows")
                else:
                    results = []
                
                print(f"✅ Query executed: {len(results)} rows returned")
                
                return {
                    "query_results": results,
                    "messages": [AIMessage(content=f"Query executed: {len(results)} rows returned")]
                }
                
        except Exception as e:
            error_msg = f"Query execution failed: {str(e)}"
            print(f"❌ {error_msg}")
            traceback.print_exc()
            
            return {
                "error": error_msg,
                "messages": [AIMessage(content=error_msg)]
            }
    
    async def format_response(self, state: TextToSqlState) -> Dict:
        """응답 포맷팅 단계"""
        print("\n📝 Formatting response...")
        
        results = state.get('query_results', [])
        sql = state.get('generated_sql', '')
        query = state.get('user_query', '')
        
        if state.get('error'):
            response = f"❌ Error: {state['error']}"
        elif not results:
            response = "No results found for your query."
        else:
            # 결과 요약
            response_parts = []
            response_parts.append(f"✅ Query executed successfully")
            response_parts.append(f"📊 Found {len(results)} results\n")
            
            # SQL 쿼리 표시
            response_parts.append("**Generated SQL:**")
            response_parts.append(f"```sql\n{sql}\n```\n")
            
            # 결과 테이블 (처음 20개만)
            if results:
                response_parts.append("**Results:**")
                
                # Markdown 테이블 생성
                headers = list(results[0].keys())
                table = "| " + " | ".join(headers) + " |\n"
                table += "| " + " | ".join(["---"] * len(headers)) + " |\n"
                
                for row in results[:20]:
                    values = []
                    for h in headers:
                        val = row.get(h, '')
                        if val is None:
                            val = 'NULL'
                        elif isinstance(val, str) and len(val) > 50:
                            val = val[:47] + '...'
                        values.append(str(val))
                    table += "| " + " | ".join(values) + " |\n"
                
                response_parts.append(table)
                
                if len(results) > 20:
                    response_parts.append(f"\n*... and {len(results) - 20} more rows*")
            
            response = "\n".join(response_parts)
        
        return {
            "formatted_response": response,
            "messages": [AIMessage(content="Response formatted")]
        }

# ========================
# Main Agent
# ========================

class TextToSqlAgent:
    """Text-to-SQL 에이전트"""
    
    def __init__(self, db_url: str):
        self.db_url = db_url
        self.node = TextToSqlNode(db_url)
        self.graph = self._build_graph()
    
    def _build_graph(self) -> StateGraph:
        """LangGraph 워크플로우 구성"""
        workflow = StateGraph(TextToSqlState)
        
        # 노드 추가
        workflow.add_node("analyze_schema", self.node.analyze_schema)
        workflow.add_node("generate_sql", self.node.generate_sql)
        workflow.add_node("validate_sql", self.node.validate_sql)
        workflow.add_node("execute_query", self.node.execute_query)
        workflow.add_node("format_response", self.node.format_response)
        
        # 엣지 설정
        workflow.set_entry_point("analyze_schema")
        
        # 선형 플로우
        workflow.add_edge("analyze_schema", "generate_sql")
        workflow.add_edge("generate_sql", "validate_sql")
        
        # 조건부 엣지: 검증 결과에 따라
        workflow.add_conditional_edges(
            "validate_sql",
            lambda x: "execute" if x.get('validation_result', {}).get('valid') else "retry",
            {
                "execute": "execute_query",
                "retry": END  # 재시도 로직은 외부에서 처리
            }
        )
        
        workflow.add_edge("execute_query", "format_response")
        workflow.add_edge("format_response", END)
        
        return workflow.compile()
    
    async def run(self, query: str, conversation_history: List[Dict] = None) -> Dict[str, Any]:
        """에이전트 실행"""
        print(f"\n{'='*60}")
        print(f"🤖 Text-to-SQL Agent Started")
        print(f"📝 Query: {query}")
        print(f"{'='*60}")
        
        initial_state = {
            "messages": [HumanMessage(content=query)],
            "user_query": query,
            "conversation_history": conversation_history or [],
            "schema_info": None,
            "generated_sql": None,
            "validation_result": None,
            "query_results": None,
            "formatted_response": None,
            "error": None,
            "retry_count": 0
        }
        
        try:
            result = await self.graph.ainvoke(initial_state)
            
            print(f"\n{'='*60}")
            print(f"✅ Agent Completed Successfully")
            print(f"{'='*60}\n")
            
            return {
                "success": True,
                "query": query,
                "sql": result.get("generated_sql"),
                "response": result.get("formatted_response"),
                "results": result.get("query_results"),
                "error": result.get("error")
            }
            
        except Exception as e:
            print(f"\n❌ Agent failed: {str(e)}")
            traceback.print_exc()
            
            return {
                "success": False,
                "query": query,
                "error": str(e),
                "response": f"An error occurred: {str(e)}"
            }

# ========================
# Test & Main
# ========================

async def test_agent():
    """에이전트 테스트"""
    
    # 테스트용 쿼리들
    test_queries = [
        "Show me all users",
        "How many chats are there?",
        "Show recent messages with user information",
        "What are the most active users?",
    ]
    
    # DB URL from environment
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        print("❌ DATABASE_URL not set in environment")
        return
    
    agent = TextToSqlAgent(db_url)
    
    for query in test_queries:
        print(f"\n{'='*80}")
        print(f"Testing: {query}")
        print('='*80)
        
        result = await agent.run(query)
        
        if result['success']:
            print(f"\n✅ Success!")
            print(f"SQL: {result.get('sql', 'N/A')}")
            print(f"Results: {len(result.get('results', [])) if result.get('results') else 0} rows")
        else:
            print(f"\n❌ Failed: {result.get('error')}")

if __name__ == "__main__":
    import asyncio
    
    # 테스트 실행
    asyncio.run(test_agent())