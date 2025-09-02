from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import List, AsyncGenerator, Optional, Dict, Any
import json
import asyncio
import os
from dotenv import load_dotenv
from langgraph_rag import TextToSqlAgent
import uvicorn

load_dotenv()

app = FastAPI(
    title="Text-to-SQL & RAG API",
    description="자연어를 SQL로 변환하고 실행하는 API",
    version="2.0.0"
)

# CORS 설정 - 개발 환경에서 모든 origin 허용
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 개발 환경에서는 모든 origin 허용
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 전역 Text-to-SQL 에이전트 인스턴스
text_to_sql_agent: Optional[TextToSqlAgent] = None

def get_text_to_sql_agent() -> TextToSqlAgent:
    """Text-to-SQL 에이전트 인스턴스 가져오기 (싱글톤)"""
    global text_to_sql_agent
    if text_to_sql_agent is None:
        db_url = os.getenv("DATABASE_URL")
        if not db_url:
            raise ValueError("DATABASE_URL environment variable not set")
        text_to_sql_agent = TextToSqlAgent(db_url)
        print("✅ Text-to-SQL Agent initialized")
    return text_to_sql_agent

@app.on_event("startup")
async def startup_event():
    """서버 시작 시 에이전트 초기화"""
    try:
        get_text_to_sql_agent()
        print("✅ All agents initialized successfully")
    except Exception as e:
        print(f"❌ Failed to initialize agents: {e}")

class Message(BaseModel):
    role: str
    content: str


class ChatRequest(BaseModel):
    messages: List[Message]

class TextToSqlRequest(BaseModel):
    query: str
    conversation_history: Optional[List[Dict]] = None
    max_rows: Optional[int] = 1000

class TextToSqlResponse(BaseModel):
    success: bool
    query: str
    sql: Optional[str]
    response: Optional[str]
    results: Optional[List[Dict[str, Any]]]
    error: Optional[str]
    row_count: Optional[int]
    truncated: Optional[bool]


async def stream_langgraph_response(messages: List[Message], files_content: Optional[List[str]] = None, file_names: Optional[List[str]] = None) -> AsyncGenerator[str, None]:
    """새로운 RAG 파이프라인 v2를 사용한 스트리밍 응답 생성"""
    try:
        # 메시지 검증
        if not messages:
            yield f"data: {json.dumps({'error': 'No messages provided'})}\n\n"
            return
        
        last_user_message = None
        for msg in reversed(messages):
            if msg.role == "user":
                last_user_message = msg.content
                break
        
        # 파일 컨텍스트가 있으면 마지막 사용자 메시지에 추가
        attached_files = []
        if files_content and last_user_message:
            file_context = "\n\n=== 첨부된 파일 내용 ===\n" + "\n\n".join(files_content)
            last_user_message = last_user_message + file_context
            
            # 첨부된 파일 정보 저장
            if file_names:
                attached_files = file_names
                print(f"[첨부 파일] {len(attached_files)}개 파일 처리: {', '.join(attached_files)}")
        
        if not last_user_message:
            yield f"data: {json.dumps({'error': 'No user message found'})}\n\n"
            return

        # 새로운 파이프라인 생성
        pipeline = create_pipeline()
        
        # 전체 대화 히스토리 전달
        conversation_history = [
            {"role": msg.role, "content": msg.content} 
            for msg in messages
        ]
        
        # 첨부파일 내용 준비 (파일 컨텍스트가 이미 메시지에 포함되어 있지만, 별도로도 전달)
        attached_files_content = files_content if files_content else None
        
        # 실시간 스트리밍
        async for chunk in pipeline.astream(last_user_message, conversation_history, attached_files_content):
            if chunk.startswith("STEP:"):
                step_info = chunk.replace("STEP:", "")
                yield f"data: {json.dumps({'step': step_info})}\n\n"
            elif chunk.startswith("METADATA:search_type:"):
                search_type = chunk.replace("METADATA:search_type:", "")
                yield f"data: {json.dumps({'search_type': search_type})}\n\n"
            elif chunk.startswith("SOURCES:"):
                sources_json = chunk.replace("SOURCES:", "")
                # 출처 정보 파싱
                try:
                    sources = json.loads(sources_json)
                    # 출처 정보 {len(sources)}개 파싱 성공
                    
                    # sources를 개별적으로 전송 (한 번에 보내면 잘릴 수 있음)
                    # 먼저 sources 시작 신호 전송
                    yield f"data: {json.dumps({'sources_start': True, 'count': min(len(sources), 10)})}\n\n"
                    
                    # 각 source를 개별적으로 전송 (최대 10개)
                    for i, source in enumerate(sources[:10]):
                        title = source.get("title", "")
                        if len(title) > 30:
                            title = title[:27] + "..."
                        
                        source_data = {
                            'source_item': {
                                "type": source.get("type", ""),
                                "title": title,
                                "url": source.get("url", "")
                            },
                            'index': i
                        }
                        
                        # 각 source를 개별 SSE 메시지로 전송
                        yield f"data: {json.dumps(source_data, ensure_ascii=True)}\n\n"
                        await asyncio.sleep(0.001)  # 짧은 지연으로 버퍼 플러시
                    
                    # 첨부 파일이 있으면 sources에 추가
                    if attached_files:
                        for j, file_name in enumerate(attached_files):
                            file_source = {
                                'source_item': {
                                    "type": "attached_file",
                                    "title": file_name,
                                    "url": ""
                                },
                                'index': len(sources[:10]) + j
                            }
                            yield f"data: {json.dumps(file_source, ensure_ascii=True)}\n\n"
                            await asyncio.sleep(0.001)
                    
                    # sources 종료 신호 전송
                    yield f"data: {json.dumps({'sources_end': True})}\n\n"
                    # 출처 정보 전송 완료
                    
                except json.JSONDecodeError as e:
                    print(f"[오류] 출처 JSON 파싱 실패: {e}")
                    # 에러 시 빈 sources 전송
                    yield f"data: {json.dumps({'sources': []})}\n\n"
            else:
                # 테이블 마크다운 포맷 수정 적용
                fixed_chunk = fix_markdown_tables(chunk)
                # 실시간으로 청크 전송
                yield f"data: {json.dumps({'content': fixed_chunk})}\n\n"
                await asyncio.sleep(0.001)  # 아주 짧은 지연만
        
        # 스트리밍 완료
        yield f"data: [DONE]\n\n"

    except ValueError as ve:
        error_msg = str(ve)
        if "Azure has not provided the response" in error_msg and "content filter" in error_msg:
            print(f"[Azure 콘텐츠 필터 오류] {ve}")
            yield f"data: {json.dumps({'content': '죄송합니다. 안전 필터에 의해 응답이 차단되었습니다. 질문을 다시 구성하여 시도해주세요.'})}\n\n"
        else:
            print(f"[ValueError] {ve}")
            yield f"data: {json.dumps({'error': f'응답 생성 중 오류가 발생했습니다: {str(ve)}'})}\n\n"
        yield f"data: [DONE]\n\n"
    except Exception as e:
        print(f"[오류] 스트리밍 중 오류: {e}")
        import traceback
        traceback.print_exc()
        yield f"data: {json.dumps({'content': '응답 생성 중 예기치 못한 오류가 발생했습니다. 다시 시도해주세요.'})}\n\n"
        yield f"data: [DONE]\n\n"


@app.post("/api/chat")
async def chat(request: ChatRequest):
    """제제 연구 챗봇 API (LangGraph 사용) - 텍스트만"""
    try:
        return StreamingResponse(
            stream_langgraph_response(request.messages),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            }
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/text-to-sql", response_model=TextToSqlResponse)
async def text_to_sql(request: TextToSqlRequest):
    """
    Text-to-SQL API 엔드포인트
    자연어 쿼리를 SQL로 변환하고 실행
    
    Parameters:
    - query: 자연어 질문
    - conversation_history: 이전 대화 내역 (선택)
    - max_rows: 반환할 최대 행 수 (선택, 기본 1000, 최대 10000)
    """
    try:
        agent = get_text_to_sql_agent()
        
        # 최대 행 수 설정
        if request.max_rows:
            agent.node.max_rows = min(request.max_rows, 10000)  # 최대 10,000행
            agent.node.default_limit = min(request.max_rows, 10000)
        
        # 에이전트 실행
        result = await agent.run(
            query=request.query,
            conversation_history=request.conversation_history
        )
        
        # 결과 처리
        row_count = len(result.get("results", [])) if result.get("results") else 0
        truncated = row_count >= agent.node.max_rows
        
        return TextToSqlResponse(
            success=result.get("success", False),
            query=request.query,
            sql=result.get("sql"),
            response=result.get("response"),
            results=result.get("results"),
            error=result.get("error"),
            row_count=row_count,
            truncated=truncated
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

async def stream_text_to_sql_response(query: str, max_rows: int = 1000) -> AsyncGenerator[str, None]:
    """Text-to-SQL 스트리밍 응답"""
    try:
        agent = get_text_to_sql_agent()
        
        # 최대 행 수 설정
        agent.node.max_rows = min(max_rows, 10000)
        agent.node.default_limit = min(max_rows, 10000)
        
        # 단계별 진행 상황 전송
        yield f"data: {json.dumps({'step': 'Analyzing database schema...'})}\n\n"
        await asyncio.sleep(0.1)
        
        # 에이전트 실행
        result = await agent.run(query=query)
        
        if result.get("success"):
            # SQL 쿼리 전송
            if result.get("sql"):
                yield f"data: {json.dumps({'sql': result.get('sql')})}\n\n"
                await asyncio.sleep(0.1)
            
            # 결과 개수 전송
            row_count = len(result.get("results", [])) if result.get("results") else 0
            yield f"data: {json.dumps({'row_count': row_count})}\n\n"
            
            # 응답 전송 (청크 단위로)
            response = result.get("response", "")
            for i in range(0, len(response), 100):
                chunk = response[i:i+100]
                yield f"data: {json.dumps({'content': chunk})}\n\n"
                await asyncio.sleep(0.01)
            
            # 결과 데이터 전송 (대용량 처리)
            if result.get("results"):
                yield f"data: {json.dumps({'results_start': True})}\n\n"
                
                # 결과를 배치로 전송 (100개씩)
                results = result.get("results")
                batch_size = 100
                for i in range(0, len(results), batch_size):
                    batch = results[i:i+batch_size]
                    yield f"data: {json.dumps({'results_batch': batch, 'batch_index': i // batch_size})}\n\n"
                    await asyncio.sleep(0.05)
                
                yield f"data: {json.dumps({'results_end': True})}\n\n"
        else:
            # 에러 전송
            error_msg = result.get("error", "Unknown error occurred")
            yield f"data: {json.dumps({'error': error_msg})}\n\n"
        
        yield f"data: [DONE]\n\n"
        
    except Exception as e:
        yield f"data: {json.dumps({'error': str(e)})}\n\n"
        yield f"data: [DONE]\n\n"

@app.post("/api/text-to-sql/stream")
async def text_to_sql_stream(request: TextToSqlRequest):
    """Text-to-SQL 스트리밍 API"""
    try:
        return StreamingResponse(
            stream_text_to_sql_response(
                query=request.query,
                max_rows=request.max_rows or 1000
            ),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            }
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/schema")
async def get_schema():
    """데이터베이스 스키마 정보 반환"""
    try:
        agent = get_text_to_sql_agent()
        schema_info = agent.node.get_detailed_schema()
        
        # 스키마를 구조화된 형태로 파싱
        tables = []
        current_table = None
        
        for line in schema_info.split('\n'):
            if line.startswith('Table:'):
                if current_table:
                    tables.append(current_table)
                current_table = {
                    "name": line.replace('Table:', '').strip(),
                    "columns": [],
                    "primary_key": None,
                    "foreign_keys": [],
                    "indexes": []
                }
            elif current_table and line.strip().startswith('-'):
                # 컬럼 정보
                col_info = line.strip()[2:]  # "- " 제거
                if ':' in col_info:
                    name, rest = col_info.split(':', 1)
                    current_table["columns"].append({
                        "name": name.strip(),
                        "type": rest.strip()
                    })
            elif current_table and 'Primary Key:' in line:
                current_table["primary_key"] = line.split(':', 1)[1].strip()
            elif current_table and 'Foreign Key:' in line:
                current_table["foreign_keys"].append(line.split(':', 1)[1].strip())
            elif current_table and 'Index:' in line:
                current_table["indexes"].append(line.split(':', 1)[1].strip())
        
        if current_table:
            tables.append(current_table)
        
        return {
            "success": True,
            "tables": tables,
            "table_count": len(tables)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/health")
async def health_check():
    """헬스 체크"""
    try:
        agent = get_text_to_sql_agent()
        return {
            "status": "healthy",
            "service": "Text-to-SQL & RAG API",
            "database": "connected" if agent.node.engine else "disconnected",
            "max_rows": agent.node.max_rows,
            "default_limit": agent.node.default_limit
        }
    except:
        return {
            "status": "unhealthy",
            "service": "Text-to-SQL & RAG API",
            "database": "disconnected"
        }

if __name__ == "__main__":
    port = int(os.getenv("API_PORT", 6001))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)
