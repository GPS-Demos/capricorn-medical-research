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

import React, { useState, useEffect } from 'react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import { Maximize2, Minimize2, ChevronDown, ChevronUp } from 'lucide-react';

const CustomTable = ({ children, title, isSpecialTable }) => {
  const [isExpanded, setIsExpanded] = useState(!isSpecialTable);
  const toggleExpand = () => setIsExpanded(!isExpanded);

  return (
    <div className="my-2 border border-gray-300 rounded-lg shadow-sm overflow-hidden">
      <div className="bg-gray-50 p-2 flex justify-end items-center">
        <button 
          onClick={toggleExpand}
          className="focus:outline-none flex items-center"
        >
          {isExpanded ? (
            <>
              <span className="mr-1 text-sm text-gray-500 hover:text-gray-700">Collapse</span>
              <ChevronUp size={16} className="text-gray-500" />
            </>
          ) : (
            <>
              <span className="mr-1 text-sm text-blue-600 hover:text-blue-800 font-medium">See more</span>
              <ChevronDown size={16} className="text-gray-500" />
            </>
          )}
        </button>
      </div>
      <div className={`transition-all duration-300 ease-in-out ${isExpanded ? 'max-h-full' : 'max-h-0'} overflow-hidden`}>
        <div className="overflow-x-auto">
          <table className="min-w-full bg-white">
            {children}
          </table>
        </div>
      </div>
    </div>
  );
};

const renderFormattedText = (text) => {
  if (typeof text !== 'string') return text;
  
  const combinedRegex = /(\[PMCID:\s*PMC\d+\s*\(\d+\)\]\(https:\/\/[^\)]+\)|\[PMID:\s*\d+\])/g;
  if (combinedRegex.test(text)) {
    const parts = text.split(combinedRegex);
    return parts.map((part, index) => {
      const pmcidMatch = part.match(/^\[PMCID:\s*(PMC\d+)\s*\((\d+)\)\]\((https:\/\/[^\)]+)\)$/);
      if (pmcidMatch) {
        return (
          <a 
            key={index}
            href={pmcidMatch[3]}
            target="_blank"
            rel="noopener noreferrer"
            className="text-blue-600 hover:text-blue-800 font-medium"
          >
            PMCID: {pmcidMatch[1]} ({pmcidMatch[2]})
          </a>
        );
      }
      const pmidMatch = part.match(/^\[PMID:\s*(\d+)\]$/);
      if (pmidMatch) {
        return (
          <a 
            key={index}
            href={`https://pubmed.ncbi.nlm.nih.gov/${pmidMatch[1]}/`}
            target="_blank"
            rel="noopener noreferrer"
            className="text-blue-600 hover:text-blue-800 font-medium"
          >
            PMID: {pmidMatch[1]}
          </a>
        );
      }
      return part;
    });
  }

  // Handle bullet points
  if (text.includes('* ')) {
    return (
      <ul className="list-disc pl-4 space-y-1">
        {text.split('* ').filter(Boolean).map((item, i) => (
          <li key={i} className="text-gray-700">{item.trim()}</li>
        ))}
      </ul>
    );
  }

  return text;
};

const CustomTableCell = ({ children, isHeader }) => {
  const content = React.Children.map(children, child => {
    if (typeof child === 'string') {
      return renderFormattedText(child);
    }
    return child;
  });

  const Tag = isHeader ? 'th' : 'td';
  const baseClasses = "px-6 py-4 text-sm border-t whitespace-pre-wrap";
  const headerClasses = isHeader 
    ? "font-semibold text-gray-700 uppercase tracking-wider bg-gray-50" 
    : "text-gray-600 align-top";

  return (
    <Tag className={`${baseClasses} ${headerClasses}`}>
      {content}
    </Tag>
  );
};

const parseMarkdownContent = (content) => {
  const sections = content.split(/(?=## )/);
  return sections.map(section => {
    const [title, ...body] = section.split('\n');
    const isSpecialTable = ['Actionable Events Analysis', 'Treatment Recommendations', 'Multi-Target Opportunities'].some(
      specialTitle => title.includes(specialTitle)
    );
    return {
      title: title.replace('## ', '').trim(),
      content: body.join('\n'),
      isSpecialTable
    };
  });
};

const MarkdownRenderer = ({ content }) => {
  const [isExpanded, setIsExpanded] = useState(false);
  const [parsedContent, setParsedContent] = useState([]);

  useEffect(() => {
    setParsedContent(parseMarkdownContent(content));
  }, [content]);

  const components = {
    table: ({ children, ...props }) => <CustomTable {...props}>{children}</CustomTable>,
    thead: ({ children }) => <thead className="bg-gray-50 border-b border-gray-300">{children}</thead>,
    tbody: ({ children }) => <tbody className="divide-y divide-gray-200">{children}</tbody>,
    th: ({ children }) => <CustomTableCell isHeader={true}>{children}</CustomTableCell>,
    td: ({ children }) => <CustomTableCell isHeader={false}>{children}</CustomTableCell>,
    h1: ({ node, ...props }) => <h1 className="text-2xl font-bold text-gray-900 mt-6 mb-4" {...props} />,
    h2: ({ node, ...props }) => <h2 className="text-xl font-semibold text-gray-800 mt-5 mb-3" {...props} />,
    h3: ({ node, ...props }) => <h3 className="text-lg font-medium text-gray-800 mt-4 mb-2" {...props} />,
    p: ({ node, ...props }) => <p className="text-gray-600 leading-relaxed mb-4" {...props} />,
    ul: ({ node, ...props }) => <ul className="list-disc pl-5 mb-4 text-gray-600" {...props} />,
    ol: ({ node, ...props }) => <ol className="list-decimal pl-5 mb-4 text-gray-600" {...props} />,
    a: ({ node, ...props }) => (
      <a 
        className="text-blue-600 hover:text-blue-800 font-medium" 
        target="_blank" 
        rel="noopener noreferrer"
        {...props} 
      />
    ),
    strong: ({ node, ...props }) => <strong className="font-semibold text-gray-900" {...props} />,
    em: ({ node, ...props }) => <em className="text-gray-800" {...props} />,
    blockquote: ({ node, ...props }) => (
      <blockquote className="border-l-4 border-gray-200 pl-4 py-2 my-4 text-gray-600 italic" {...props} />
    ),
    code: ({ node, inline, ...props }) => (
      inline 
        ? <code className="px-1 py-0.5 bg-gray-100 rounded text-sm font-mono text-gray-800" {...props} />
        : <code className="block bg-gray-50 p-4 rounded-lg text-sm font-mono text-gray-800 overflow-x-auto" {...props} />
    ),
  };

  return (
    <div className={`prose max-w-none ${isExpanded ? 'fixed inset-0 z-50 bg-white overflow-auto p-8' : 'relative'}`}>
      <button 
        onClick={() => setIsExpanded(!isExpanded)} 
        className="absolute top-2 right-2 p-1 bg-gray-200 rounded-full hover:bg-gray-300 transition-colors"
        aria-label={isExpanded ? "Minimize" : "Maximize"}
      >
        {isExpanded ? <Minimize2 size={20} /> : <Maximize2 size={20} />}
      </button>
      {parsedContent.map((section, index) => (
        <div key={index}>
          <h2 className="text-xl font-semibold text-gray-800 mt-5 mb-3">{section.title}</h2>
          <ReactMarkdown 
            children={section.content}
            remarkPlugins={[remarkGfm]}
            components={{
              ...components,
              table: (props) => <CustomTable title={section.title} isSpecialTable={section.isSpecialTable} {...props} />,
            }}
          />
        </div>
      ))}
    </div>
  );
};

export default MarkdownRenderer;
