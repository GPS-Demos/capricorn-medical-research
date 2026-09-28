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

// src/components/PrintButton.js
import React from 'react';
import { marked } from 'marked';

// Configure marked options
marked.setOptions({
  gfm: true, // GitHub Flavored Markdown
  breaks: true, // Convert \n to <br>
  sanitize: false, // Allow HTML
  smartLists: true, // Use smarter list behavior
  smartypants: true, // Use "smart" typographic punctuation
  xhtml: false // Don't close empty tags with a slash
});

const escapeHtml = (str) => {
  if (!str) return '';
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#039;');
};

const sanitizeHtml = (html) => {
  if (!html) return '';
  return String(html)
    .replace(/<script\b[^<]*(?:(?!<\/script>)<[^<]*)*<\/script>/gi, '')
    .replace(/on\w+\s*=\s*(["']).*?\1/gi, '')
    .replace(/javascript:/gi, '');
};

const PrintButton = ({ content, title = 'Analysis Results' }) => {
  const handlePrint = () => {
    // Create a new window with secure flags
    const printWindow = window.open('', '_blank', 'noopener,noreferrer');
    if (!printWindow) return;
    printWindow.opener = null;
    
    // Convert markdown to HTML using marked
    const renderedContent = sanitizeHtml(marked.parse(content));
    const safeTitle = escapeHtml(title);
    
    // Add content to the new window with proper styling
    printWindow.document.write(`
      <!DOCTYPE html>
      <html>
        <head>
          <title>${safeTitle}</title>
          <style>
            body {
              font-family: Arial, sans-serif;
              line-height: 1.6;
              color: #333;
              max-width: 800px;
              margin: 0 auto;
              padding: 20px;
            }
            h1 {
              font-size: 24px;
              margin-bottom: 20px;
              color: #333;
            }
            h2 {
              font-size: 20px;
              margin-top: 20px;
              margin-bottom: 10px;
            }
            h3 {
              font-size: 18px;
              margin-top: 16px;
              margin-bottom: 8px;
            }
            p {
              margin-bottom: 16px;
            }
            ul, ol {
              margin-bottom: 16px;
              padding-left: 20px;
            }
            table {
              border-collapse: collapse;
              width: 100%;
              margin-bottom: 16px;
            }
            th, td {
              border: 1px solid #ddd;
              padding: 8px;
              text-align: left;
            }
            th {
              background-color: #f2f2f2;
            }
            .prose {
              max-width: none;
            }
            .prose p {
              margin-bottom: 16px;
            }
            .prose h1, .prose h2, .prose h3 {
              margin-top: 24px;
              margin-bottom: 16px;
              font-weight: bold;
            }
            .prose ul, .prose ol {
              margin-bottom: 16px;
              padding-left: 20px;
            }
            .prose li {
              margin-bottom: 4px;
            }
            .prose a {
              color: #2563eb;
              text-decoration: underline;
            }
            .prose code {
              background-color: #f1f5f9;
              padding: 2px 4px;
              border-radius: 4px;
              font-family: monospace;
            }
            .prose blockquote {
              border-left: 4px solid #e5e7eb;
              padding-left: 16px;
              font-style: italic;
            }
            .disclaimer {
              font-size: 10px;
              font-style: italic;
              color: #666;
              margin-top: 30px;
              border-top: 1px solid #eee;
              padding-top: 10px;
            }
            @media print {
              body {
                padding: 0;
              }
              button {
                display: none;
              }
            }
          </style>
        </head>
        <body>
          <h1>${safeTitle}</h1>
          <div class="content prose">
            ${renderedContent}
          </div>
          <div class="disclaimer">
            For professional medical use only. Results require physician interpretation and clinical judgment. 
            Capricorn is an advanced search tool, not a substitute for professional medical advice.
          </div>
          <button onclick="window.print(); window.close();" style="margin-top: 20px; padding: 8px 16px; background: #f0f0f0; border: 1px solid #ddd; cursor: pointer;">
            Print
          </button>
        </body>
      </html>
    `);
    
    // Finish writing to the document
    printWindow.document.close();
    
    // Focus the new window
    printWindow.focus();
  };

  return (
    <button
      onClick={handlePrint}
      className="absolute bottom-4 right-4 p-2 rounded-md transition-all duration-200 ease-in-out bg-white hover:bg-gray-100 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-opacity-50"
      aria-label="Print analysis results"
      title="Print"
    >
      <div className="flex items-center text-gray-600">
        <span className="mr-2 text-sm">Export</span>
        <svg 
          xmlns="http://www.w3.org/2000/svg" 
          className="w-4 h-4" 
          fill="none" 
          viewBox="0 0 24 24" 
          stroke="currentColor"
        >
          <path 
            strokeLinecap="round" 
            strokeLinejoin="round" 
            strokeWidth={2} 
            d="M17 17h2a2 2 0 002-2v-4a2 2 0 00-2-2H5a2 2 0 00-2 2v4a2 2 0 002 2h2m2 4h6a2 2 0 002-2v-4a2 2 0 00-2-2H9a2 2 0 00-2 2v4a2 2 0 002 2zm8-12V5a2 2 0 00-2-2H9a2 2 0 00-2 2v4h10z" 
          />
        </svg>
      </div>
    </button>
  );
};

export default PrintButton;
