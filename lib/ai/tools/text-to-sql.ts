import { generateUUID } from '@/lib/utils';
import { tool, type UIMessageStreamWriter } from 'ai';
import { z } from 'zod';
import type { Session } from 'next-auth';
import type { ChatMessage } from '@/lib/types';

interface TextToSqlProps {
  session: Session;
  dataStream: UIMessageStreamWriter<ChatMessage>;
}

// Python API URL (개발/프로덕션에 따라 변경)
// Next.js에서는 서버 사이드에서만 process.env 접근 가능
const TEXT_TO_SQL_API_URL = process.env.TEXT_TO_SQL_API_URL || 'http://localhost:6001';

export const textToSql = ({ session, dataStream }: TextToSqlProps) =>
  tool({
    description:
      'Query database using natural language. Use this tool when users ask about data, statistics, or need database information. Examples: "show all users", "how many chats", "recent messages", "most active users".',
    inputSchema: z.object({
      query: z.string().describe('Natural language query to execute on the database'),
      maxRows: z.number().optional().default(1000).describe('Maximum number of rows to return'),
    }),
    execute: async ({ query, maxRows }) => {
      console.log('[Text-to-SQL] Tool called with query:', query);
      console.log('[Text-to-SQL] API URL:', TEXT_TO_SQL_API_URL);
      
      try {
        // Python API 호출
        const response = await fetch(`${TEXT_TO_SQL_API_URL}/api/text-to-sql`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({
            query,
            max_rows: maxRows,
          }),
        });

        if (!response.ok) {
          throw new Error(`API error: ${response.statusText}`);
        }

        const result = await response.json();
        console.log('[Text-to-SQL] API Response:', result);

        if (result.success && !result.error) {
          // SQL 쿼리 전송
          if (result.sql) {
            dataStream.write({
              type: 'data-sql',
              data: result.sql,
              transient: true,
            });
          }

          // 결과 개수 전송
          if (result.row_count !== undefined) {
            dataStream.write({
              type: 'data-rowCount',
              data: result.row_count,
              transient: true,
            });
          }

          // 결과 테이블 데이터 전송 (Sheet artifact처럼 처리 가능)
          if (result.results && result.results.length > 0) {
            const id = generateUUID();
            
            // Sheet artifact로 변환
            dataStream.write({
              type: 'data-kind',
              data: 'sheet',
              transient: true,
            });

            dataStream.write({
              type: 'data-id',
              data: id,
              transient: true,
            });

            dataStream.write({
              type: 'data-title',
              data: `SQL Query Results (${result.row_count} rows)`,
              transient: true,
            });

            // CSV 형식으로 변환
            const headers = Object.keys(result.results[0]);
            const csvData = [
              headers.join(','),
              ...result.results.map((row: any) => 
                headers.map((h: string) => {
                  const val = row[h];
                  if (val === null || val === undefined) return '';
                  if (typeof val === 'string' && val.includes(',')) {
                    return `"${val.replace(/"/g, '""')}"`;
                  }
                  return String(val);
                }).join(',')
              )
            ].join('\n');

            dataStream.write({
              type: 'data-content',
              data: csvData,
              transient: true,
            });

            dataStream.write({
              type: 'data-finish',
              data: null,
              transient: true,
            });
          }

          return {
            success: true,
            query: result.query,
            sql: result.sql,
            rowCount: result.row_count,
            message: result.response || `Query executed successfully. Retrieved ${result.row_count} rows.`,
          };
        } else {
          // 에러가 있어도 사용자에게 메시지 반환
          const errorMessage = result.error || 'Query execution failed';
          console.error('[Text-to-SQL] Query failed:', errorMessage);
          
          return {
            success: false,
            query: result.query,
            sql: result.sql,
            error: errorMessage,
            message: `죄송합니다. 쿼리 실행 중 오류가 발생했습니다: ${errorMessage}\n\n생성된 SQL:\n\`\`\`sql\n${result.sql}\n\`\`\`\n\n다른 방식으로 질문해 주시거나, 더 구체적인 정보를 제공해 주세요.`,
          };
        }
      } catch (error) {
        console.error('Text-to-SQL error:', error);
        
        dataStream.write({
          type: 'data-error',
          data: error instanceof Error ? error.message : 'Unknown error',
          transient: true,
        });

        return {
          success: false,
          error: error instanceof Error ? error.message : 'Failed to execute query',
        };
      }
    },
  });