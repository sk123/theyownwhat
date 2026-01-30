/* src/components/LoadingScreen.jsx */
import React, { useEffect, useRef } from 'react';
import { motion, AnimatePresence, useSpring } from 'framer-motion';

function AnimatedCounter({ value, className }) {
    const ref = useRef(null);
    const motionValue = useSpring(0, { stiffness: 60, damping: 20 });

    useEffect(() => {
        motionValue.set(value || 0);
    }, [value, motionValue]);

    useEffect(() => {
        return motionValue.on("change", (latest) => {
            if (ref.current) {
                ref.current.textContent = Math.round(latest).toLocaleString();
            }
        });
    }, [motionValue]);

    return <span ref={ref} className={className}>0</span>;
}

export default function LoadingScreen({ visible, entities, properties }) {
    let statusText = "Tracing ownership links and aggregating property data...";
    if (properties > 0) statusText = "Linking properties to network...";
    else if (entities > 0) statusText = "Found entities, retrieving properties...";

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
                                {statusText}
                            </p>

                            {/* Progress Bar */}
                            <div className="space-y-3">
                                <div className="bg-white/5 p-4 rounded-2xl border border-white/10 backdrop-blur-md">
                                    <div className="mb-3">
                                        <div className="flex justify-between items-baseline mb-2">
                                            <span className="text-xs font-bold text-slate-400 uppercase tracking-wider">Progress</span>
                                            <span className="text-2xl font-black text-blue-400">
                                                {Math.round((properties / Math.max(entities * 10, 100)) * 100)}%
                                            </span>
                                        </div>
                                        <div className="h-2 bg-slate-700 rounded-full overflow-hidden">
                                            <motion.div
                                                className="h-full bg-gradient-to-r from-blue-500 to-indigo-500"
                                                initial={{ width: "0%" }}
                                                animate={{ width: `${Math.min((properties / Math.max(entities * 10, 100)) * 100, 100)}%` }}
                                                transition={{ duration: 0.5, ease: "easeOut" }}
                                            />
                                        </div>
                                    </div>
                                    <div className="text-xs text-slate-500 font-medium">
                                        {entities > 0 && `${entities} entities found`}
                                        {properties > 0 && entities > 0 && " • "}
                                        {properties > 0 && `${properties} parcel records loaded`}
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
