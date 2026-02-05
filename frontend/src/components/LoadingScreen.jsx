/* src/components/LoadingScreen.jsx */
import React from 'react';
import { motion, AnimatePresence } from 'framer-motion';

export default function LoadingScreen({ visible, entities, properties }) {
    // Calculate progress percentage (capped at 100%)
    // The previous formula used entities * 10 as a goal. 
    // We'll keep it simple: progress is based on properties loaded relative to an expected depth.
    const rawProgress = entities > 0 ? Math.min((properties / Math.max(entities * 8, 50)) * 100, 100) : 10;
    const progress = Math.min(Math.round(rawProgress), 100);

    // Fleeting one-liners for what's actually happening
    const getStatusMessage = () => {
        if (progress < 15) return "Scanning ownership registry...";
        if (progress < 35) return "Resolving corporate hierarchies...";
        if (progress < 55) return "Cross-referencing secondary entities...";
        if (progress < 75) return "Linking property portfolios...";
        if (progress < 90) return "Calculating aggregate valuations...";
        return "Finalizing network visualization...";
    };

    return (
        <AnimatePresence>
            {visible && (
                <motion.div
                    initial={{ opacity: 0 }}
                    animate={{ opacity: 1 }}
                    exit={{ opacity: 0 }}
                    className="fixed inset-0 z-[200] flex items-center justify-center bg-slate-900/80 backdrop-blur-xl"
                >
                    <div className="text-center p-8 max-w-sm w-full relative">

                        <motion.div
                            animate={{ rotate: 360 }}
                            transition={{ repeat: Infinity, duration: 3, ease: "linear" }}
                            className="relative w-24 h-24 mx-auto mb-8"
                        >
                            <div className="absolute inset-0 rounded-full border-4 border-slate-700"></div>
                            <div className="absolute inset-0 rounded-full border-4 border-t-blue-500 border-r-transparent border-b-transparent border-l-transparent"></div>
                        </motion.div>

                        <motion.div
                            initial={{ opacity: 0, y: 10 }}
                            animate={{ opacity: 1, y: 0 }}
                            transition={{ delay: 0.2 }}
                        >
                            <h3 className="text-2xl font-black text-white mb-2 tracking-tight">Building Network</h3>
                            <p className="text-sm text-slate-400 font-medium mb-8 h-5 italic">
                                {getStatusMessage()}
                            </p>

                            {/* Progress Bar - Minimal, no bullshit */}
                            <div className="space-y-3">
                                <div className="bg-white/5 p-5 rounded-2xl border border-white/10 backdrop-blur-md">
                                    <div className="flex justify-between items-baseline mb-3">
                                        <span className="text-[10px] font-bold text-slate-500 uppercase tracking-widest">Progress</span>
                                        <span className="text-3xl font-black text-blue-400">
                                            {progress}%
                                        </span>
                                    </div>
                                    <div className="h-2.5 bg-slate-800 rounded-full overflow-hidden">
                                        <motion.div
                                            className="h-full bg-gradient-to-r from-blue-600 via-blue-500 to-indigo-500"
                                            initial={{ width: "0%" }}
                                            animate={{ width: `${progress}%` }}
                                            transition={{ duration: 0.8, ease: "easeOut" }}
                                        />
                                    </div>
                                </div>
                            </div>
                        </motion.div>
                    </div>
                </motion.div>
            )}
        </AnimatePresence>
    );
}
