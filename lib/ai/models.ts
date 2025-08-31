export const DEFAULT_CHAT_MODEL: string = 'chat-model';

export interface ChatModel {
  id: string;
  name: string;
  description: string;
}

export const chatModels: Array<ChatModel> = [
  {
    id: 'chat-model',
    name: '베이스원GPT 기본',
    description: '일반적인 대화와 작업을 위한 모델',
  },
  {
    id: 'chat-model-reasoning',
    name: '베이스원GPT 전문가',
    description: '심화 분석과 추론이 필요한 작업용',
  },
];
