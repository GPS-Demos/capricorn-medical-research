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

import { useState, useCallback, useEffect, useMemo } from 'react';
import { handleSendMessage } from './chat/messageHandlers';
import { initializeActiveChat, initializeNewChat, handleChatSelect } from './chat/chatState';
import { deleteChat } from '../firebase';

const useChat = (user, selectedTemplate) => {
  // Use authenticated user ID (regular or anonymous)
  const userId = user?.uid;
  const [chatHistory, setChatHistory] = useState([]);
  const [isLoadingDocs, setIsLoadingDocs] = useState(false);
  const [isLoadingAnalysis, setIsLoadingAnalysis] = useState(false);
  const [activeChat, setActiveChat] = useState(null);
  const [message, setMessage] = useState('');
  const [hasDocumentMessages, setHasDocumentMessages] = useState(false);

  useEffect(() => {
    setHasDocumentMessages(chatHistory.some(msg => msg.type === 'document' || msg.documents));
  }, [chatHistory]);

  const handleInitializeActiveChat = useCallback(async (caseNotes, labResults, extractedDisease, extractedEvents) => {
    // If there's an active temporary chat, delete it first
    if (!user && activeChat) {
      try {
        await deleteChat(userId, activeChat.id);
      } catch (error) {
        console.error('Error deleting temporary chat:', error);
      }
    }

    return initializeActiveChat({
      user: { uid: userId },
      caseNotes,
      labResults,
      extractedDisease,
      extractedEvents,
      setActiveChat,
      setChatHistory
    });
  }, [user, userId, activeChat]);

  const handleInitializeNewChat = useCallback(() => {
    initializeNewChat({
      setActiveChat,
      setChatHistory
    });
  }, []);

  const handleChatSelection = useCallback(async (chat) => {
    await handleChatSelect({
      chat,
      setActiveChat,
      setChatHistory,
      setHasDocumentMessages,
      initializeNewChat: handleInitializeNewChat,
      userId
    });
  }, [handleInitializeNewChat, userId]);

  const handleMessageSend = useCallback(async (e) => {
    await handleSendMessage({
      e,
      message,
      setMessage,
      user: { uid: userId },
      activeChat,
      setActiveChat,
      setChatHistory,
      isLoadingDocs,
      setIsLoadingDocs,
      isLoadingAnalysis,
      setIsLoadingAnalysis,
      selectedTemplate,
      userId
    });
  }, [message, isLoadingDocs, isLoadingAnalysis, activeChat, user, selectedTemplate]);

  return {
    chatHistory,
    isLoadingDocs,
    isLoadingAnalysis,
    activeChat,
    message,
    setMessage,
    handleChatSelect: handleChatSelection,
    handleSendMessage: handleMessageSend,
    initializeNewChat: handleInitializeNewChat,
    initializeActiveChat: handleInitializeActiveChat,
    hasDocumentMessages,
    setHasDocumentMessages
  };
};

export default useChat;
