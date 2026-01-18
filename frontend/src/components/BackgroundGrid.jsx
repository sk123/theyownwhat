/* src/components/BackgroundGrid.jsx */
import React from 'react';

export default function BackgroundGrid() {
    return (
        <div className="absolute inset-0 z-0 overflow-hidden pointer-events-none">
            <div className="absolute top-0 inset-x-0 h-px bg-gradient-to-r from-transparent via-slate-200 to-transparent opacity-50"></div>
            <div className="absolute inset-0"
                style={{
                    backgroundImage: 'radial-gradient(circle at 1px 1px, #e2e8f0 1px, transparent 0)',
                    backgroundSize: '40px 40px'
                }}>
            </div>
            <div className="absolute top-0 right-0 w-[500px] h-[500px] bg-blue-100/50 rounded-full blur-3xl -translate-y-1/2 translate-x-1/2"></div>
            <div className="absolute bottom-0 left-0 w-[500px] h-[500px] bg-indigo-100/50 rounded-full blur-3xl translate-y-1/3 -translate-x-1/4"></div>
        </div>
    );
}
