// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import { createNewChat, getChatMessages, getDoc } from '../../firebase';
import { createMessageId } from './messageHandlers';
import { getLatestMessages } from './utils';

export const initializeActiveChat = async ({
  user,
  caseNotes,
  labResults,
  extractedDisease,
  extractedEvents,
  setActiveChat,
  setChatHistory,
  userId
}) => {
  console.log('[CHAT_DEBUG] Initializing active chat:', {
    hasUser: !!user,
    userId: user?.uid || userId,
    hasExtractedDisease: !!extractedDisease,
    hasExtractedEvents: !!extractedEvents
  });

  try {
    const timestamp = new Date();
    const messageId = createMessageId('initial');
    
    console.log('[CHAT_DEBUG] Creating initial message with:', {
      timestamp: new Date(),
      messageId,
      extractedDisease,
      numEvents: extractedEvents?.length
    });

    // Create the initial message that contains all the case information
    const initialMessage = {
      content: JSON.stringify({
        caseNotes,
        labResults,
        extractedDisease,
        extractedEvents
      }),
      role: 'user',
      timestamp,
      type: 'initial_case',
      messageId
    };

    // Create a new chat with the initial message
    console.log('[CHAT_DEBUG] Creating new chat in Firestore');
    const chatId = await createNewChat(user?.uid || userId, [initialMessage]);
    
    console.log('[CHAT_DEBUG] Setting active chat with ID:', chatId);
    // Set the active chat
    setActiveChat({
      id: chatId,
      messages: [initialMessage]
    });

    console.log('[CHAT_DEBUG] Setting chat history with initialization message');
    // Update chat history
    setChatHistory([{
      id: messageId,
      text: `Case initialized with disease: ${extractedDisease}`,
      isUser: false,
      timestamp,
      initialCase: {
        caseNotes,
        labResults,
        extractedDisease,
        extractedEvents
      }
    }]);

    console.log('[CHAT_DEBUG] Active chat initialization complete');
    return chatId;
  } catch (error) {
    console.error('[CHAT_DEBUG] Error initializing active chat:', error);
    throw error;
  }
};

export const initializeNewChat = ({
  setActiveChat,
  setChatHistory
}) => {
  setActiveChat(null);
  setChatHistory([]);
};

export const handleChatSelect = async ({
  chat,
  setActiveChat,
  setHasDocumentMessages,
  setChatHistory,
  initializeNewChat,
  userId
}) => {
  if (chat) {
    setActiveChat(chat);
    setHasDocumentMessages(false);
    
    const messages = [];
    
    // Fetch the latest messages from Firestore
    const latestMessages = await getChatMessages(userId, chat.id);
    
    // Process the fetched messages
    latestMessages.forEach(msg => {
      switch (msg.type) {
        case 'initial_case':
          const initialCase = JSON.parse(msg.content);
          messages.push({
            id: msg.messageId,
            text: `Case initialized with disease: ${initialCase.extractedDisease}`,
            isUser: false,
            timestamp: msg.timestamp,
            initialCase
          });
          break;
          
        case 'message':
          messages.push({
            id: msg.messageId,
            text: msg.content,
            isUser: msg.role === 'user',
            timestamp: msg.timestamp
          });
          break;
          
        case 'document':
          const docContent = JSON.parse(msg.content);
          messages.push({
            id: msg.messageId,
            type: 'document',
            isUser: false,
            currentProgress: docContent.currentProgress,
            articles: docContent.articles,
            timestamp: msg.timestamp
          });
          setHasDocumentMessages(true);
          break;
          
        case 'analysis':
          messages.push({
            id: msg.messageId,
            isUser: false,
            analysis: msg.content,
            timestamp: msg.timestamp
          });
          break;
          
        case 'error':
          messages.push({
            id: msg.messageId,
            text: msg.content,
            isUser: false,
            error: msg.error,
            timestamp: msg.timestamp
          });
          break;
      }
    });

    // Sort messages by timestamp if needed
    messages.sort((a, b) => a.timestamp - b.timestamp);
    
    setChatHistory(messages);
  } else {
    initializeNewChat();
  }
};
