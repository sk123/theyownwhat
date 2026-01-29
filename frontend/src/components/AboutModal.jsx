import React from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { X, Info } from 'lucide-react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import readmeContent from '../../../README.md?raw';

export default function AboutModal({ isOpen, onClose, onShowFreshness }) {
    if (!isOpen) return null;

    return (
        <AnimatePresence>
            <div className="fixed inset-0 z-[100] flex items-center justify-center p-4 bg-black/40 backdrop-blur-sm">
                <motion.div
                    initial={{ opacity: 0, scale: 0.95, y: 20 }}
                    animate={{ opacity: 1, scale: 1, y: 0 }}
                    exit={{ opacity: 0, scale: 0.95, y: 20 }}
                    className="bg-white rounded-2xl shadow-2xl w-full max-w-3xl overflow-hidden flex flex-col max-h-[90vh]"
                >
                    {/* Header */}
                    <div className="p-6 border-b border-gray-100 flex items-center justify-between bg-gray-50/50">
                        <div className="flex items-center gap-2 text-blue-600">
                            <Info size={20} className="font-bold" />
                            <h2 className="text-xl font-black tracking-tight text-gray-900">About This Project</h2>
                        </div>
                        <button
                            onClick={onClose}
                            className="p-2 hover:bg-gray-200 rounded-full transition-colors"
                        >
                            <X size={20} className="text-gray-400" />
                        </button>
                    </div>

                    {/* Content */}
                    <div className="p-8 overflow-y-auto text-gray-600 custom-prose">
                        <ReactMarkdown
                            remarkPlugins={[remarkGfm]}
                            components={{
                                h1: ({ node, ...props }) => <h1 className="text-2xl font-black text-gray-900 mb-4 pb-2 border-b" {...props} />,
                                h2: ({ node, ...props }) => <h2 className="text-xl font-bold text-gray-800 mt-6 mb-3" {...props} />,
                                h3: ({ node, ...props }) => <h3 className="text-lg font-bold text-gray-800 mt-4 mb-2" {...props} />,
                                p: ({ node, ...props }) => <p className="mb-4 text-sm leading-relaxed" {...props} />,
                                ul: ({ node, ...props }) => <ul className="list-disc pl-5 mb-4 space-y-1" {...props} />,
                                li: ({ node, ...props }) => <li className="text-sm" {...props} />,
                                a: ({ node, ...props }) => <a className="text-blue-600 hover:underline font-medium" {...props} />,
                                blockquote: ({ node, ...props }) => <blockquote className="border-l-4 border-gray-200 pl-4 italic my-4" {...props} />,
                                code: ({ node, inline, className, children, ...props }) => {
                                    return inline ?
                                        <code className="bg-gray-100 px-1 py-0.5 rounded text-xs font-mono text-pink-500" {...props}>{children}</code> :
                                        <code className="block bg-gray-50 p-4 rounded-lg text-xs font-mono overflow-x-auto mb-4" {...props}>{children}</code>
                                }
                            }}
                        >
                            {readmeContent}
                        </ReactMarkdown>

                        <div className="mt-10 pt-6 border-t border-gray-100 flex items-center justify-between">
                            <div className="flex flex-col">
                                <span className="text-xs font-bold text-gray-400 uppercase tracking-widest">Transparency</span>
                                <span className="text-sm text-gray-500">View data update logs and source freshness.</span>
                            </div>
                            <button
                                onClick={onShowFreshness}
                                className="px-6 py-2.5 bg-slate-900 hover:bg-slate-800 text-white font-bold rounded-xl transition-all shadow-lg flex items-center gap-2"
                            >
                                <Info size={16} />
                                Data Freshness Report
                            </button>
                        </div>
                    </div>
                </motion.div>
            </div>
        </AnimatePresence>
    );
}
