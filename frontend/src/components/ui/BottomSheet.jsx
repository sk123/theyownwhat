import React, { useEffect } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { X } from 'lucide-react';

const BottomSheet = ({ isOpen, onClose, title, children }) => {
    // Prevent scrolling on body when open
    useEffect(() => {
        if (isOpen) {
            document.body.style.overflow = 'hidden';
            // Optional: add a class to root for iOS momentum scrolling fix
            document.documentElement.classList.add('ios-scroll-lock');
        } else {
            document.body.style.overflow = 'unset';
            document.documentElement.classList.remove('ios-scroll-lock');
        }
        return () => {
            document.body.style.overflow = 'unset';
            document.documentElement.classList.remove('ios-scroll-lock');
        };
    }, [isOpen]);

    return (
        <AnimatePresence>
            {isOpen && (
                <>
                    {/* Backdrop */}
                    <motion.div
                        initial={{ opacity: 0 }}
                        animate={{ opacity: 1 }}
                        exit={{ opacity: 0 }}
                        onClick={onClose}
                        className="fixed inset-0 z-[100] bg-slate-900/40 backdrop-blur-sm"
                    />

                    {/* Sheet */}
                    <motion.div
                        initial={{ y: '100%' }}
                        animate={{ y: 0 }}
                        exit={{ y: '100%' }}
                        transition={{
                            type: 'spring',
                            damping: 25,
                            stiffness: 300,
                            mass: 0.8
                        }}
                        drag="y"
                        dragConstraints={{ top: 0 }}
                        dragElastic={0.2}
                        onDragEnd={(e, info) => {
                            if (info.offset.y > 100 || info.velocity.y > 500) {
                                onClose();
                            }
                        }}
                        className="fixed inset-x-0 bottom-0 z-[101] rounded-t-[2rem] bg-white shadow-2xl flex flex-col max-h-[90vh] overflow-hidden"
                        style={{ paddingBottom: 'env(safe-area-inset-bottom)' }}
                    >
                        {/* Drag Handle */}
                        <div className="flex justify-center pt-3 pb-2 cursor-grab active:cursor-grabbing shrink-0">
                            <div className="w-12 h-1.5 rounded-full bg-slate-200" />
                        </div>

                        {/* Header */}
                        {title && (
                            <div className="px-6 pb-4 flex items-center justify-between shrink-0">
                                <h3 className="text-xl font-black text-slate-800 tracking-tight">{title}</h3>
                                <button
                                    onClick={onClose}
                                    className="p-2 -mr-2 rounded-full bg-slate-50 text-slate-400 hover:bg-slate-100 hover:text-slate-700 transition-colors"
                                >
                                    <X size={20} />
                                </button>
                            </div>
                        )}

                        {/* Content */}
                        <div className="px-6 pb-8 overflow-y-auto overscroll-contain flex-1">
                            {children}
                        </div>
                    </motion.div>
                </>
            )}
        </AnimatePresence>
    );
};

export default BottomSheet;
