# Text-to-SQL Agent with LangGraph

자연어를 SQL로 변환하고 실행하는 파이썬 기반 에이전트입니다.

## 주요 기능

- **Text-to-SQL 변환**: 자연어 질문을 PostgreSQL 쿼리로 자동 변환
- **대용량 데이터 처리**: 최대 10,000행까지 처리 가능
- **보안 검증**: SQL Injection 방지 및 읽기 전용 쿼리만 허용
- **스트리밍 지원**: 실시간 결과 스트리밍
- **Next.js 통합**: 프론트엔드 채팅 인터페이스와 완벽 통합

## 설치 방법

### 1. Python 환경 설정

```bash
# Python 3.9+ 필요
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 2. 환경 변수 설정

`.env` 파일 생성:
```env
# Azure OpenAI
AZURE_OPENAI_API_KEY=your_api_key
AZURE_OPENAI_ENDPOINT=https://your-endpoint.openai.azure.com
AZURE_OPENAI_DEPLOYMENT_NAME=gpt-4o
AZURE_OPENAI_API_VERSION=2024-08-01-preview

# Database
DATABASE_URL=postgresql://user:password@localhost:5432/dbname

# API
API_PORT=6001
```

### 3. 서버 실행

```bash
python main.py
```

서버가 `http://localhost:6001`에서 실행됩니다.

## API 엔드포인트

### 1. Text-to-SQL 실행
```http
POST /api/text-to-sql
Content-Type: application/json

{
  "query": "Show me all users who registered this month",
  "max_rows": 1000
}
```

### 2. 스트리밍 Text-to-SQL
```http
POST /api/text-to-sql/stream
Content-Type: application/json

{
  "query": "What are the most active users?",
  "max_rows": 100
}
```

### 3. 스키마 정보
```http
GET /api/schema
```

### 4. 헬스 체크
```http
GET /api/health
```

## Next.js 통합

### 1. 환경 변수 추가 (.env.local)
```env
TEXT_TO_SQL_API_URL=http://localhost:6001
```

### 2. 채팅에서 사용

채팅 인터페이스에서 다음과 같은 질문 가능:
- "데이터베이스에 사용자가 몇 명 있어?"
- "이번 달에 가입한 사용자 목록 보여줘"
- "가장 활발한 사용자 10명은?"
- "최근 메시지와 사용자 정보를 조인해서 보여줘"

## 보안 고려사항

- **읽기 전용**: SELECT 쿼리만 허용
- **SQL Injection 방지**: 파라미터화된 쿼리 사용
- **제한 설정**: 자동 LIMIT 추가 (기본 1000행)
- **검증 시스템**: 위험한 패턴 자동 감지

## 성능 최적화

- **커넥션 풀링**: 20개 커넥션, 40개 오버플로우
- **결과 스트리밍**: 대용량 데이터 배치 처리
- **캐싱**: 스키마 정보 캐싱
- **비동기 처리**: 모든 작업 비동기 실행

## 트러블슈팅

### 1. 데이터베이스 연결 실패
- DATABASE_URL 확인
- PostgreSQL 서버 실행 상태 확인
- 네트워크 방화벽 설정 확인

### 2. Azure OpenAI 오류
- API 키와 엔드포인트 확인
- 배포 이름 확인
- API 버전 호환성 확인

### 3. 대용량 데이터 처리
- max_rows 파라미터 조정
- 메모리 사용량 모니터링
- 쿼리 최적화 필요시 인덱스 추가

## 개발 모드

```bash
# 자동 리로드 활성화
uvicorn main:app --reload --host 0.0.0.0 --port 6001
```

## 프로덕션 배포

```bash
# Gunicorn 사용
gunicorn main:app -w 4 -k uvicorn.workers.UvicornWorker --bind 0.0.0.0:6001
```

## 라이선스

MIT