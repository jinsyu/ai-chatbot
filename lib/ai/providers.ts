import {
  customProvider,
  extractReasoningMiddleware,
  wrapLanguageModel,
} from 'ai';
import { createAzure } from '@ai-sdk/azure';
import {
  artifactModel,
  chatModel,
  reasoningModel,
  titleModel,
} from './models.test';
import { isTestEnvironment } from '../constants';
import { createOpenAI } from '@ai-sdk/openai';

// Azure OpenAI 설정
const azure = createAzure({
  apiKey: process.env.AZURE_OPENAI_API_KEY,
  resourceName: 'aoai-bo-dev-2025', // Azure resource name from the endpoint
  // apiVersion: '2025-01-01-preview', // 이미지 지원을 위한 최신 API 버전
});

const openai = createOpenAI({
  apiKey: process.env.OPENAI_API_KEY,
  baseURL: 'https://api.openai.com/v1',
});

const deploymentName = process.env.AZURE_OPENAI_DEPLOYMENT_NAME || 'gpt-4.1';

export const myProvider = isTestEnvironment
  ? customProvider({
      languageModels: {
        'chat-model': chatModel,
        'chat-model-reasoning': reasoningModel,
        'title-model': titleModel,
        'artifact-model': artifactModel,
      },
    })
  : customProvider({
      languageModels: {
        'chat-model': azure(deploymentName),
        'chat-model-reasoning': wrapLanguageModel({
          model: azure(deploymentName),
          middleware: extractReasoningMiddleware({ tagName: 'think' }),
        }),
        'title-model': azure(deploymentName),
        'artifact-model': azure(deploymentName),
      },
    });
