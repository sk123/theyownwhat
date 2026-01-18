/* src/components/LoadingScreen.jsx */
import React from 'react';
import { motion, AnimatePresence } from 'framer-motion';

export default function LoadingScreen({ visible, entities, properties }) {
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
                            <p className="text-sm text-slate-400 font-medium mb-8">
                                Tracing ownership links and aggregating property data...
                            </p>

                            <div className="grid grid-cols-2 gap-4">
                                <div className="bg-white/5 p-4 rounded-2xl border border-white/10 backdrop-blur-md">
                                    <div className="text-3xl font-black text-blue-400">{entities}</div>
                                    <div className="text-[10px] uppercase font-bold text-slate-500 tracking-wider">Entities Found</div>
                                </div>
                                <div className="bg-white/5 p-4 rounded-2xl border border-white/10 backdrop-blur-md">
                                    <div className="text-3xl font-black text-indigo-400">{properties}</div>
                                    <div className="text-[10px] uppercase font-bold text-slate-500 tracking-wider">Properties Linked</div>
                                </div>
                            </div>
                        </motion.div>
                    </div>
                </motion.div>
            )}
        </AnimatePresence>
    );
}
