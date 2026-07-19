import React from 'react';
import { Home, BookOpen, AlertCircle, RefreshCw, Toolbox, LogIn, LayoutDashboard, Database, MessageSquare, Heart, Building2, TrendingUp, ShieldAlert, Menu } from 'lucide-react';

export default function Header({ onHome, onDatasets, onReset, onAbout, OnOpenToolbox, toolboxEnabled, onShowFreshness, onReportIssue, onHartfordPlayground, onBurstDetector, evictionToolsEnabled, currentView, activeState, onStateChange }) {
    const [user, setUser] = React.useState(null);
    const isDatasetLanding = currentView === 'datasets';

    React.useEffect(() => {
        if (toolboxEnabled) {
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
        <>
            {/* Desktop & Mobile Header */}
            <header className="bg-white border-b border-gray-200 sticky top-0 z-50">
                <div className="container mx-auto px-4 h-14 md:h-16 flex items-center justify-between">
                    <div className="flex items-center gap-4">
                        <div
                            className="flex items-center gap-3 cursor-pointer group"
                            onClick={onHome}
                        >
                            <div className="w-9 h-9 md:w-10 md:h-10 bg-gradient-to-br from-blue-50 to-indigo-50/50 rounded-xl flex items-center justify-center group-hover:from-blue-100 group-hover:to-indigo-100 transition-all duration-300 shadow-sm border border-slate-100 group-hover:border-blue-200">
                                <svg viewBox="0 0 24 24" className="w-5.5 h-5.5 md:w-6 md:h-6" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
                                    {/* The isometric hexagonal shape representing a building outline */}
                                    <path d="M12 2L2 7l10 5 10-5-10-5z" stroke="url(#logo-grad-blue)" fill="url(#logo-grad-blue-light)" />
                                    <path d="M2 17l10 5 10-5" stroke="url(#logo-grad-indigo)" />
                                    <path d="M2 7v10" stroke="url(#logo-grad-indigo)" />
                                    <path d="M12 12v10" stroke="url(#logo-grad-purple)" />
                                    <path d="M22 7v10" stroke="url(#logo-grad-purple)" />
                                    
                                    {/* Network nodes overlay at key vertices */}
                                    <circle cx="12" cy="2" r="1.5" fill="#2563eb" stroke="#ffffff" strokeWidth="0.8" />
                                    <circle cx="2" cy="7" r="1.5" fill="#4f46e5" stroke="#ffffff" strokeWidth="0.8" />
                                    <circle cx="22" cy="7" r="1.5" fill="#7c3aed" stroke="#ffffff" strokeWidth="0.8" />
                                    
                                    {/* Central glowing hub */}
                                    <circle cx="12" cy="12" r="2.2" fill="#3b82f6" stroke="#ffffff" strokeWidth="1" />
                                    <circle cx="12" cy="12" r="4" fill="#3b82f6" stroke="#3b82f6" strokeWidth="0" opacity="0.3" className="animate-pulse" />
                                    
                                    {/* Modern gradients definition */}
                                    <defs>
                                        <linearGradient id="logo-grad-blue" x1="0%" y1="0%" x2="100%" y2="100%">
                                            <stop offset="0%" stopColor="#3b82f6" />
                                            <stop offset="100%" stopColor="#1d4ed8" />
                                        </linearGradient>
                                        <linearGradient id="logo-grad-blue-light" x1="0%" y1="0%" x2="100%" y2="100%">
                                            <stop offset="0%" stopColor="#eff6ff" stopOpacity="0.8" />
                                            <stop offset="100%" stopColor="#dbeafe" stopOpacity="0.8" />
                                        </linearGradient>
                                        <linearGradient id="logo-grad-indigo" x1="0%" y1="0%" x2="100%" y2="100%">
                                            <stop offset="0%" stopColor="#6366f1" />
                                            <stop offset="100%" stopColor="#4338ca" />
                                        </linearGradient>
                                        <linearGradient id="logo-grad-purple" x1="0%" y1="0%" x2="100%" y2="100%">
                                            <stop offset="0%" stopColor="#a855f7" />
                                            <stop offset="100%" stopColor="#6d28d9" />
                                        </linearGradient>
                                    </defs>
                                </svg>
                            </div>
                            <div>
                                <h1 className="font-black text-xl md:text-2xl leading-tight text-gray-900 tracking-tight">they own <span className="text-transparent bg-clip-text bg-gradient-to-r from-blue-600 to-indigo-600">WHAT??</span></h1>
                                 {isDatasetLanding ? (
                                     <p className="text-[10px] md:text-xs text-gray-500 font-medium tracking-wide">LANDLORD / PROPERTY EXPLORER <span className="text-[9px] opacity-30">v0.1.6</span></p>
                                 ) : activeState === 'CT' ? (
                                     <p className="text-[10px] md:text-xs text-gray-500 font-medium tracking-wide">CONNECTICUT LANDLORD / PROPERTY EXPLORER <span className="text-[9px] opacity-30">v0.1.6</span></p>
                                 ) : (
                                     <p className="text-[10px] md:text-xs text-gray-500 font-medium tracking-wide flex items-center gap-1.5">
                                         <span>
                                             {activeState === 'NY' ? 'NYC' :
                                              activeState === 'DC' ? 'D.C.' :
                                              activeState === 'BALTIMORE' ? 'BALTIMORE' :
                                              activeState === 'BOSTON' ? 'BOSTON' :
                                              activeState === 'DETROIT' ? 'DETROIT' :
                                              activeState === 'PHILADELPHIA' ? 'PHILADELPHIA' :
                                              activeState === 'CHICAGO' ? 'CHICAGO' :
                                              activeState === 'MIAMI' ? 'MIAMI' :
                                              activeState === 'MINNEAPOLIS' ? 'MINNEAPOLIS' :
                                              activeState === 'NJ' ? 'NEW JERSEY' :
                                              activeState} LANDLORD / PROPERTY EXPLORER
                                         </span>
                                         <span className="text-[8px] font-black bg-violet-100 text-violet-700 px-1 py-0.5 rounded uppercase tracking-wider">Beta</span>
                                     </p>
                                 )}
                             </div>
                         </div>
 
                        {/* State Toggle Selector (Desktop only) */}
                        {!isDatasetLanding && <div className="hidden lg:flex items-center gap-0.5 bg-slate-100 p-1 rounded-xl border border-slate-300 shadow-inner scale-90 md:scale-100 shrink-0">
                            <button
                                onClick={() => onStateChange('CT')}
                                className={`px-2.5 py-1.5 text-xs font-black rounded-lg transition-all ${activeState === 'CT' ? 'bg-white text-blue-600 shadow-sm border border-blue-200' : 'text-slate-600 hover:text-slate-900 hover:bg-white/50'}`}
                            >
                                CT
                            </button>
                            <button
                                onClick={() => onStateChange('NY')}
                                className={`px-2.5 py-1.5 text-xs font-black rounded-lg transition-all ${activeState === 'NY' ? 'bg-white text-indigo-600 shadow-sm border border-indigo-200' : 'text-slate-600 hover:text-slate-900 hover:bg-white/50'}`}
                            >
                                NYC
                            </button>
                            <button
                                onClick={() => onStateChange('DC')}
                                className={`px-2.5 py-1.5 text-xs font-black rounded-lg transition-all ${activeState === 'DC' ? 'bg-white text-indigo-600 shadow-sm border border-indigo-200' : 'text-slate-600 hover:text-slate-900 hover:bg-white/50'}`}
                            >
                                D.C.
                            </button>
                            <button
                                onClick={() => onStateChange('BALTIMORE')}
                                className={`px-2.5 py-1.5 text-xs font-black rounded-lg transition-all ${activeState === 'BALTIMORE' ? 'bg-white text-indigo-600 shadow-sm border border-indigo-200' : 'text-slate-600 hover:text-slate-900 hover:bg-white/50'}`}
                            >
                                Balt
                            </button>
                            <button
                                onClick={() => onStateChange('BOSTON')}
                                className={`px-2.5 py-1.5 text-xs font-black rounded-lg transition-all ${activeState === 'BOSTON' ? 'bg-white text-indigo-600 shadow-sm border border-indigo-200' : 'text-slate-600 hover:text-slate-900 hover:bg-white/50'}`}
                            >
                                Boston
                            </button>
                             <button
                                 onClick={() => onStateChange('DETROIT')}
                                 className={`px-2.5 py-1.5 text-xs font-black rounded-lg transition-all ${activeState === 'DETROIT' ? 'bg-white text-indigo-600 shadow-sm border border-indigo-200' : 'text-slate-600 hover:text-slate-900 hover:bg-white/50'}`}
                             >
                                 Detroit
                             </button>

                              <button
                                  onClick={() => onStateChange('MINNEAPOLIS')}
                                  className={`px-2.5 py-1.5 text-xs font-black rounded-lg transition-all ${activeState === 'MINNEAPOLIS' ? 'bg-white text-indigo-600 shadow-sm border border-indigo-200' : 'text-slate-600 hover:text-slate-900 hover:bg-white/50'}`}
                              >
                                  Minneapolis
                              </button>
                              <button
                                  onClick={() => onStateChange('NJ')}
                                  className={`px-2.5 py-1.5 text-xs font-black rounded-lg transition-all ${activeState === 'NJ' ? 'bg-white text-indigo-600 shadow-sm border border-indigo-200' : 'text-slate-600 hover:text-slate-900 hover:bg-white/50'}`}
                              >
                                  NJ
                              </button>
                        </div>}
                    </div>
                    {/* Desktop-only buttons */}
                    <div className="hidden md:flex items-center gap-2">
                        {user && (
                            <button
                                onClick={OnOpenToolbox}
                                className="flex items-center gap-1.5 px-3 py-1.5 text-xs font-bold text-blue-700 bg-blue-50 hover:bg-blue-100 rounded-lg transition-colors border border-blue-100"
                            >
                                <LayoutDashboard className="w-3.5 h-3.5" />
                                <span>Toolbox</span>
                            </button>
                        )}

                        {onReset && (
                            <button
                                onClick={onReset}
                                className="flex items-center gap-1.5 px-3 py-1.5 text-xs font-bold text-purple-700 bg-purple-50 hover:bg-purple-100 rounded-lg transition-colors border border-purple-200"
                            >
                                <RefreshCw className="w-3.5 h-3.5" />
                                <span>Start Over</span>
                            </button>
                        )}

                        {(user || onReset) && <div className="w-px h-5 bg-slate-200 mx-1"></div>}



                        {onHartfordPlayground && (
                            <button
                                onClick={onHartfordPlayground}
                                className={`flex items-center gap-1.5 px-3 py-1.5 text-xs font-bold rounded-lg transition-colors border ${currentView === 'hartford' ? 'text-red-700 bg-red-50 border-red-100' : 'text-slate-600 bg-white hover:bg-slate-50 border-slate-200'}`}
                            >
                                <ShieldAlert className="w-3.5 h-3.5 text-red-500" />
                                <span>Rap Sheets</span>
                            </button>
                        )}

                        {onShowFreshness && (
                            <button
                                onClick={onShowFreshness}
                                className={`flex items-center gap-1.5 px-3 py-1.5 text-xs font-bold rounded-lg transition-colors border text-slate-600 bg-white hover:bg-slate-50 border-slate-200`}
                            >
                                <Database className="w-3.5 h-3.5" />
                                <span>Data Completeness</span>
                            </button>
                        )}

                        <button
                            onClick={onAbout}
                            className="p-2 text-slate-500 hover:text-slate-800 hover:bg-slate-50 rounded-lg border border-transparent hover:border-slate-200 transition-all"
                            title="About"
                        >
                            <AlertCircle className="w-4 h-4" />
                        </button>

                        <a
                            href="https://github.com/sponsors/sk123"
                            target="_blank"
                            rel="noopener noreferrer"
                            className="p-2 text-slate-500 hover:text-pink-600 hover:bg-pink-50 rounded-lg border border-transparent hover:border-pink-100 transition-all group"
                            title="Pledge Support"
                        >
                            <Heart size={16} className="fill-transparent group-hover:fill-pink-500 transition-colors" />
                        </a>

                        {onReportIssue && (
                            <button
                                onClick={onReportIssue}
                                className="p-2 text-slate-500 hover:text-rose-600 hover:bg-rose-50 rounded-lg border border-transparent hover:border-rose-100 transition-all"
                                title="Feedback"
                            >
                                <MessageSquare className="w-4 h-4" />
                            </button>
                        )}
                    </div>
                </div>

                {/* Mobile & Tablet City Navigation Row */}
                {!isDatasetLanding && <div className="lg:hidden bg-slate-50 border-t border-slate-200/60 px-4 py-1.5 flex items-center overflow-x-auto gap-2 scrollbar-none shadow-sm">
                    <div className="flex items-center gap-1.5 shrink-0">

                        <button
                            onClick={() => onStateChange('CT')}
                            className={`px-2.5 py-1 text-xs font-black rounded-lg transition-all ${activeState === 'CT' ? 'bg-blue-600 text-white shadow-sm' : 'bg-white border border-slate-200 text-slate-600 hover:bg-slate-100'}`}
                        >
                            CT
                        </button>
                        <button
                            onClick={() => onStateChange('NY')}
                            className={`px-2.5 py-1 text-xs font-black rounded-lg transition-all ${activeState === 'NY' ? 'bg-indigo-600 text-white shadow-sm' : 'bg-white border border-slate-200 text-slate-600 hover:bg-slate-100'}`}
                        >
                            NYC
                        </button>
                        <button
                            onClick={() => onStateChange('DC')}
                            className={`px-2.5 py-1 text-xs font-black rounded-lg transition-all ${activeState === 'DC' ? 'bg-indigo-600 text-white shadow-sm' : 'bg-white border border-slate-200 text-slate-600 hover:bg-slate-100'}`}
                        >
                            D.C.
                        </button>
                        <button
                            onClick={() => onStateChange('BALTIMORE')}
                            className={`px-2.5 py-1 text-xs font-black rounded-lg transition-all ${activeState === 'BALTIMORE' ? 'bg-indigo-600 text-white shadow-sm' : 'bg-white border border-slate-200 text-slate-600 hover:bg-slate-100'}`}
                        >
                            Baltimore
                        </button>
                        <button
                            onClick={() => onStateChange('BOSTON')}
                            className={`px-2.5 py-1 text-xs font-black rounded-lg transition-all ${activeState === 'BOSTON' ? 'bg-indigo-600 text-white shadow-sm' : 'bg-white border border-slate-200 text-slate-600 hover:bg-slate-100'}`}
                        >
                            Boston
                        </button>
                        <button
                            onClick={() => onStateChange('DETROIT')}
                            className={`px-2.5 py-1 text-xs font-black rounded-lg transition-all ${activeState === 'DETROIT' ? 'bg-indigo-600 text-white shadow-sm' : 'bg-white border border-slate-200 text-slate-600 hover:bg-slate-100'}`}
                        >
                            Detroit
                        </button>

                        <button
                            onClick={() => onStateChange('MINNEAPOLIS')}
                            className={`px-2.5 py-1 text-xs font-black rounded-lg transition-all ${activeState === 'MINNEAPOLIS' ? 'bg-indigo-600 text-white shadow-sm' : 'bg-white border border-slate-200 text-slate-600 hover:bg-slate-100'}`}
                        >
                            Minneapolis
                        </button>
                        <button
                            onClick={() => onStateChange('NJ')}
                            className={`px-2.5 py-1 text-xs font-black rounded-lg transition-all ${activeState === 'NJ' ? 'bg-indigo-600 text-white shadow-sm' : 'bg-white border border-slate-200 text-slate-600 hover:bg-slate-100'}`}
                        >
                            NJ
                        </button>
                    </div>
                </div>}
            </header>

            {/* Mobile Bottom Navigation Bar */}
            <nav className="md:hidden fixed bottom-0 left-0 right-0 z-[100] bg-white/95 backdrop-blur-xl border-t border-slate-200 shadow-[0_-4px_20px_rgba(0,0,0,0.08)] pb-[env(safe-area-inset-bottom)]">
                <div className="flex items-stretch justify-around px-1 h-16">
                    <MobileNavItem
                        icon={Home}
                        label="Home"
                        active={currentView === 'home'}
                        onClick={onHome}
                    />
                    {onHartfordPlayground && (
                        <MobileNavItem
                            icon={ShieldAlert}
                            label="Rap Sheets"
                            active={currentView === 'hartford'}
                            onClick={onHartfordPlayground}
                            accent="red"
                        />
                    )}
                    {evictionToolsEnabled && onBurstDetector && (
                        <MobileNavItem
                            icon={TrendingUp}
                            label="Surges"
                            active={currentView === 'burst'}
                            onClick={onBurstDetector}
                            accent="amber"
                        />
                    )}
                    {onShowFreshness && (
                        <MobileNavItem
                            icon={Database}
                            label="Data Completeness"
                            onClick={onShowFreshness}
                            accent="teal"
                        />
                    )}
                    <MobileNavItem
                        icon={AlertCircle}
                        label="About"
                        onClick={onAbout}
                    />
                    {onReportIssue && (
                        <MobileNavItem
                            icon={MessageSquare}
                            label="Feedback"
                            onClick={onReportIssue}
                            accent="rose"
                        />
                    )}
                </div>
            </nav>
        </>
    );
}

function MobileNavItem({ icon: Icon, label, active, onClick, accent }) {
    const activeColors = {
        red: 'text-red-600',
        amber: 'text-amber-600',
        teal: 'text-teal-600',
        rose: 'text-rose-600',
    };
    const indicatorColors = {
        red: 'bg-red-600',
        amber: 'bg-amber-600',
        teal: 'bg-teal-600',
        rose: 'bg-rose-600',
    };

    const activeColor = active ? (accent ? activeColors[accent] : 'text-blue-600') : 'text-slate-400';
    const indicatorColor = accent ? indicatorColors[accent] : 'bg-blue-600';

    return (
        <button
            onClick={onClick}
            className={`flex flex-col items-center justify-center gap-0.5 flex-1 py-2 transition-colors relative ${activeColor}`}
        >
            {active && (
                <div className={`absolute top-0 left-1/2 -translate-x-1/2 w-8 h-0.5 rounded-full ${indicatorColor}`} />
            )}
            <Icon size={20} strokeWidth={active ? 2.5 : 1.5} />
            <span className={`text-[10px] font-bold tracking-wide ${active ? 'opacity-100' : 'opacity-70'}`}>{label}</span>
        </button>
    );
}
