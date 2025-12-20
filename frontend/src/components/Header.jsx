import React from 'react';
import { Home, BookOpen, AlertCircle, RefreshCw } from 'lucide-react';

export default function Header({ onHome, onReset, onAbout }) {
    return (
        <header className="bg-white border-b border-gray-200 sticky top-0 z-50">
            <div className="container mx-auto px-4 h-16 flex items-center justify-between">
                <div
                    className="flex items-center gap-3 cursor-pointer group"
                    onClick={onHome}
                >
                    <div className="w-10 h-10 bg-blue-50 rounded-xl flex items-center justify-center group-hover:bg-blue-100 transition-colors">
                        <Home className="w-6 h-6 text-blue-600" />
                    </div>
                    <div>
                        <h1 className="font-bold text-lg leading-tight text-gray-900">they own WHAT??</h1>
                        <p className="text-xs text-gray-500 font-medium tracking-wide">CT PROPERTY EXPLORER</p>
                    </div>
                </div>

                <div className="flex items-center gap-2">
                    {onReset && (
                        <button
                            onClick={onReset}
                            className="hidden md:flex items-center gap-2 px-3 py-1.5 text-sm font-semibold text-purple-600 bg-purple-50 hover:bg-purple-100 rounded-lg transition-colors"
                        >
                            <RefreshCw className="w-4 h-4" />
                            Start Over
                        </button>
                    )}

                    <button
                        className="flex items-center gap-2 px-3 py-1.5 text-sm font-semibold text-gray-600 hover:bg-gray-50 rounded-lg transition-colors"
                        onClick={onAbout}
                    >
                        <AlertCircle className="w-4 h-4" />
                        <span className="hidden sm:inline">About</span>
                    </button>
                </div>
            </div>
        </header>
    );
}
