import React from 'react';
import { Home, BookOpen, AlertCircle, RefreshCw, Toolbox, LogIn, LayoutDashboard, Database, MessageSquare } from 'lucide-react';

export default function Header({ onHome, onReset, onAbout, OnOpenToolbox, toolboxEnabled, onShowFreshness, onReportIssue }) {
    const [user, setUser] = React.useState(null);

    React.useEffect(() => {
        if (toolboxEnabled) {
            // Only check auth if toolbox is enabled
            fetch('/api/auth/me')
                .then(res => res.json())
                .then(data => {
                    if (data.authenticated) {
                        setUser(data.user);
                    }
                })
                .catch(err => console.error("Auth check failed", err));
        }
    }, [toolboxEnabled]);

    const handleLogin = () => {
        window.location.href = '/api/auth/login';
    };

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
                        <p className="text-xs text-gray-500 font-medium tracking-wide">CONNECTICUT PROPERTY EXPLORER</p>
                    </div>
                </div>

                <div className="flex items-center gap-2">
                    {onShowFreshness && (
                        <button
                            onClick={onShowFreshness}
                            className="flex items-center gap-2 px-3 py-1.5 text-sm font-semibold text-teal-700 bg-teal-50 hover:bg-teal-100 rounded-lg transition-colors border border-teal-100 mr-2"
                        >
                            <Database className="w-4 h-4" />
                            <span className="hidden sm:inline">Data Freshness</span>
                        </button>
                    )}

                    {user ? (
                        <button
                            onClick={OnOpenToolbox}
                            className="flex items-center gap-2 px-3 py-1.5 text-sm font-semibold text-blue-700 bg-blue-50 hover:bg-blue-100 rounded-lg transition-colors border border-blue-100"
                        >
                            <LayoutDashboard className="w-4 h-4" />
                            <span className="hidden sm:inline">Toolbox</span>
                        </button>
                    ) : (
                        toolboxEnabled && (
                            <button
                                onClick={handleLogin}
                                className="flex items-center gap-2 px-3 py-1.5 text-sm font-semibold text-gray-700 bg-gray-50 hover:bg-gray-100 rounded-lg transition-colors border border-gray-200"
                            >
                                <LogIn className="w-4 h-4" />
                                <span className="hidden sm:inline">Sign In</span>
                            </button>
                        )
                    )}

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

                    {onReportIssue && (
                        <button
                            className="flex items-center gap-2 px-3 py-1.5 text-sm font-semibold text-rose-600 bg-rose-50 hover:bg-rose-100 rounded-lg transition-colors border border-rose-100 ml-2"
                            onClick={onReportIssue}
                            title="Report Data Issue"
                        >
                            <MessageSquare className="w-4 h-4" />
                            <span className="hidden lg:inline">Report Issue</span>
                        </button>
                    )}
                </div>
            </div>
        </header>
    );
}
