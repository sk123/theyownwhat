import React from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { X, Info, Database, Globe, Zap, ShieldAlert, Mail } from 'lucide-react';

export default function AboutModal({ isOpen, onClose }) {
    if (!isOpen) return null;

    return (
        <AnimatePresence>
            <div className="fixed inset-0 z-[100] flex items-center justify-center p-4 bg-black/40 backdrop-blur-sm">
                <motion.div
                    initial={{ opacity: 0, scale: 0.95, y: 20 }}
                    animate={{ opacity: 1, scale: 1, y: 0 }}
                    exit={{ opacity: 0, scale: 0.95, y: 20 }}
                    className="bg-white rounded-2xl shadow-2xl w-full max-w-2xl overflow-hidden flex flex-col max-h-[90vh]"
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
                    <div className="p-8 overflow-y-auto space-y-8 text-sm leading-relaxed text-gray-600">

                        <section>
                            <h3 className="flex items-center gap-2 font-bold text-gray-900 mb-3 uppercase tracking-wider text-xs">
                                <Zap size={14} className="text-blue-500" />
                                The Mission
                            </h3>
                            <p>
                                <strong>they own WHAT??</strong> is an investigative and advocacy tool designed to bring transparency to Connecticut's property landscape. By linking fragmented public records, we reveal the hidden networks of ownership that shape our neighborhoods.
                            </p>
                        </section>

                        <section className="bg-gradient-to-br from-indigo-50 to-blue-50 p-4 rounded-xl border border-blue-100">
                            <h3 className="flex items-center gap-2 font-bold text-blue-900 mb-3 uppercase tracking-wider text-xs">
                                <Zap size={14} className="text-blue-600" />
                                Recent Updates (January 2026)
                            </h3>
                            <ul className="space-y-2 text-xs text-blue-900/80 font-medium">
                                <li className="flex items-start gap-2">
                                    <span className="mt-1 w-1.5 h-1.5 rounded-full bg-blue-500 shrink-0" />
                                    <span><strong>Data Refresh:</strong> Ingested latest 2024/2025 datasets: 9.6M businesses, 1.7M principals, and 1.3M parcels.</span>
                                </li>
                                <li className="flex items-start gap-2">
                                    <span className="mt-1 w-1.5 h-1.5 rounded-full bg-blue-500 shrink-0" />
                                    <span><strong>UI Overhaul:</strong> Modernized interface with glassmorphism design, improved cards, and responsive layout.</span>
                                </li>
                                <li className="flex items-start gap-2">
                                    <span className="mt-1 w-1.5 h-1.5 rounded-full bg-blue-500 shrink-0" />
                                    <span><strong>Smart Search:</strong> Added instant autocomplete for businesses, owners, and addresses with real-time feedback.</span>
                                </li>
                            </ul>
                        </section>

                        <section>
                            <h3 className="flex items-center gap-2 font-bold text-gray-900 mb-3 uppercase tracking-wider text-xs">
                                <Globe size={14} className="text-blue-500" />
                                New Feature: AI Digest
                            </h3>
                            <p>
                                The <strong>AI Digest</strong> performs automated web searches across multiple entities simultaneously to identify:
                            </p>
                            <ul className="list-disc list-inside mt-2 space-y-1 ml-4 text-gray-500">
                                <li>Systemic tenant complaints and property condition issues.</li>
                                <li>Legal violations and court case patterns.</li>
                                <li>Corporate footprints and out-of-state investment trends.</li>
                                <li>Verified news sources and public documentation links.</li>
                            </ul>
                        </section>

                        <section className="grid grid-cols-1 md:grid-cols-2 gap-6">
                            <div>
                                <h3 className="flex items-center gap-2 font-bold text-gray-900 mb-3 uppercase tracking-wider text-xs">
                                    <Database size={14} className="text-blue-500" />
                                    Data Sources
                                </h3>
                                <ul className="space-y-1 text-gray-500">
                                    <li>• CT SOTS Business Registry</li>
                                    <li>• Municipal Parcel & CAMA Records</li>
                                    <li>• Real-time News Highlights</li>
                                    <li>• 2025 Assessment Data</li>
                                </ul>
                            </div>
                            <div>
                                <h3 className="flex items-center gap-2 font-bold text-gray-900 mb-3 uppercase tracking-wider text-xs">
                                    <ShieldAlert size={14} className="text-blue-500" />
                                    How it works
                                </h3>
                                <p className="text-xs">
                                    Our system uses name normalization and link-analysis to connect principals to businesses, and businesses to properties, creating a "graph" of ownership that surpasses simple database lookups.
                                </p>
                            </div>
                        </section>

                        <section className="bg-blue-50 p-4 rounded-xl border border-blue-100">
                            <h3 className="font-bold text-blue-900 mb-2 text-xs uppercase">Transparency Notice</h3>
                            <p className="text-xs text-blue-800/80">
                                This tool is for informational and advocacy purposes. While we strive for 100% accuracy in our linking logic, users should verify critical findings with primary municipal and state sources.
                            </p>
                        </section>

                        <div className="pt-4 border-t border-gray-100 flex items-center justify-between">
                            <div className="flex items-center gap-2">
                                <Mail size={14} className="text-gray-400" />
                                <span className="text-[11px] text-gray-400 font-medium">Questions? Reach out to <a href="mailto:salmunk@gmail.com" className="text-blue-600 hover:underline">salmunk@gmail.com</a></span>
                            </div>
                            <span className="text-[10px] text-gray-300 font-bold uppercase tracking-widest leading-none">Version 2.0 // 2025</span>
                        </div>
                    </div>
                </motion.div>
            </div>
        </AnimatePresence>
    );
}
