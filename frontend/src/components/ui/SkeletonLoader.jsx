import React from 'react';
import { motion } from 'framer-motion';

const SkeletonLoader = ({ type = 'card', count = 1 }) => {
    const renderSkeleton = (index) => {
        if (type === 'card') {
            return (
                <motion.div
                    key={index}
                    initial={{ opacity: 0 }}
                    animate={{ opacity: 1 }}
                    transition={{ duration: 0.3 }}
                    className="bg-white rounded-2xl p-6 shadow-sm border border-slate-100/60 w-full"
                >
                    <div className="flex animate-pulse flex-col space-y-6">
                        <div className="flex items-center space-x-4">
                            <div className="h-12 w-12 rounded-xl bg-slate-100"></div>
                            <div className="flex-1 space-y-2 py-1">
                                <div className="h-4 bg-slate-100 rounded w-2/3"></div>
                                <div className="h-3 bg-slate-100 rounded w-1/3"></div>
                            </div>
                        </div>
                        <div className="space-y-3">
                            <div className="grid grid-cols-3 gap-4">
                                <div className="h-3 bg-slate-100 rounded col-span-2"></div>
                                <div className="h-3 bg-slate-100 rounded col-span-1"></div>
                            </div>
                            <div className="h-3 bg-slate-100 rounded w-5/6"></div>
                            <div className="h-3 bg-slate-100 rounded w-4/6"></div>
                        </div>
                    </div>
                </motion.div>
            );
        }

        if (type === 'list-item') {
            return (
                <motion.div
                    key={index}
                    initial={{ opacity: 0 }}
                    animate={{ opacity: 1 }}
                    transition={{ duration: 0.2, delay: index * 0.05 }}
                    className="flex items-center space-x-4 animate-pulse p-4 bg-white rounded-xl border border-slate-50 mb-2"
                >
                    <div className="rounded-full bg-slate-100 h-10 w-10 shrink-0"></div>
                    <div className="flex-1 space-y-2 py-1">
                        <div className="h-3.5 bg-slate-100 rounded-md w-1/3"></div>
                        <div className="h-2.5 bg-slate-100/80 rounded-md w-1/4"></div>
                    </div>
                </motion.div>
            );
        }

        if (type === 'stat') {
            return (
                <motion.div
                    key={index}
                    initial={{ opacity: 0 }}
                    animate={{ opacity: 1 }}
                    className="flex flex-col space-y-2 animate-pulse"
                >
                    <div className="h-3 bg-slate-100 rounded w-16"></div>
                    <div className="h-8 bg-slate-100 rounded w-24"></div>
                </motion.div>
            );
        }

        return null;
    };

    return (
        <div className={type === 'list-item' ? 'space-y-2' : type === 'card' ? 'grid gap-4' : 'flex gap-6'}>
            {Array.from({ length: count }).map((_, i) => renderSkeleton(i))}
        </div>
    );
};

export default SkeletonLoader;
