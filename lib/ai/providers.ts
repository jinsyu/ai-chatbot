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

// Azure OpenAI 설정
const azure = createAzure({
  apiKey: process.env.AZURE_OPENAI_API_KEY,
  resourceName: 'aoai-bo-dev-2025', // Azure resource name from the endpoint
  // apiVersion: '2024-12-01-preview',
});

const deploymentName = process.env.AZURE_OPENAI_DEPLOYMENT_NAME || 'gpt-4o';

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
