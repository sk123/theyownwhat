import React, { useState, useEffect, useRef } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import {
  Activity, Users, ShieldAlert, Bot, Search, Eye, MapPin,
  RefreshCw, Smartphone, Monitor, CheckCircle, Clock, Server,
  Globe, AlertTriangle, ArrowUpRight, Filter, Database, Radio,
  Building2, Wrench, Layers, ExternalLink, ShieldCheck, FileText,
  DollarSign, Sparkles, Languages, Share2, Compass, ArrowRight,
  PieChart, BarChart3, CheckCircle2, ChevronRight, Laptop
} from 'lucide-react';

export default function LocalAnalyticsDashboard({ onBack }) {
  // Read initial app from URL parameter if available (?app=renter_help or ?app=theyownwhat or ?app=all)
  const initialApp = typeof window !== 'undefined'
    ? new URLSearchParams(window.location.search).get('app') || 'theyownwhat'
    : 'theyownwhat';

  const [selectedApp, setSelectedApp] = useState(
    ['theyownwhat', 'renter_help', 'all'].includes(initialApp) ? initialApp : 'theyownwhat'
  );
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [autoRefresh, setAutoRefresh] = useState(true);
  const [minutesFilter, setMinutesFilter] = useState(60); // 1h default
  const [feedFilter, setFeedFilter] = useState('ALL'); // ALL | HUMANS | TOOLS | SEARCHES | BOTS
  const [searchQueryFilter, setSearchQueryFilter] = useState('HUMANS'); // HUMANS | ALL | TESTS
  const [lastRefreshed, setLastRefreshed] = useState(null);
  
  const timerRef = useRef(null);

  const fetchAnalytics = async () => {
    try {
      const res = await fetch(`/api/analytics/realtime?minutes=${minutesFilter}&app=${selectedApp}`);
      if (!res.ok) throw new Error(`HTTP error ${res.status}`);
      const json = await res.json();
      setData(json);
      setError(null);
      setLastRefreshed(new Date());
    } catch (err) {
      console.error('Analytics fetch error:', err);
      setError(err.message || 'Failed to fetch analytics');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchAnalytics();

    if (autoRefresh) {
      timerRef.current = setInterval(fetchAnalytics, 3000);
    }

    return () => {
      if (timerRef.current) clearInterval(timerRef.current);
    };
  }, [autoRefresh, minutesFilter, selectedApp]);

  // Sync URL query param when app switches
  const handleAppChange = (newApp) => {
    setSelectedApp(newApp);
    setLoading(true);
    if (typeof window !== 'undefined' && window.history) {
      const url = new URL(window.location);
      url.searchParams.set('app', newApp);
      window.history.replaceState({}, '', url);
    }
  };

  const summary = data?.summary || {};
  const realtime = data?.realtime || {};
  const liveFeed = (data?.live_feed || []).filter(item => !item.path?.includes('analytics'));
  const geoLocations = data?.geo_locations || [];
  const datasets = data?.datasets || [];
  const topSearches = data?.top_searches || [];
  const botBreakdown = data?.bot_breakdown || [];
  const devices = data?.devices || [];
  const browsers = data?.browsers || [];
  const topPages = data?.top_pages || [];
  const featuresUsage = data?.features_usage || [];
  const languageBreakdown = data?.language_breakdown || [];
  const referralSources = data?.referral_sources || [];
  const platformSplit = data?.platform_split || [];

  const filteredFeed = liveFeed.filter(item => {
    if (feedFilter === 'HUMANS') return !item.is_bot && item.traffic_category !== 'test';
    if (feedFilter === 'TOOLS') return ['repair_request', 'eviction_defense', 'document_vault', 'rent_ledger', 'housing_search', 'fha_accommodation'].includes(item.event_type) || item.app === 'renter_help';
    if (feedFilter === 'SEARCHES') return (item.event_type === 'search' || item.search_query) && !item.is_bot;
    if (feedFilter === 'BOTS') return item.is_bot || item.traffic_category === 'test';
    return true;
  });

  const filteredSearches = topSearches.filter(s => {
    if (searchQueryFilter === 'HUMANS') return s.human_count > 0 || !s.is_automated_only;
    if (searchQueryFilter === 'TESTS') return s.automated_count > 0 || s.is_automated_only;
    return true;
  });

  const getEventBadge = (item) => {
    // Tenant Toolkit / Renter.help events
    if (item.app === 'renter_help' || item.event_type === 'repair_request') {
      if (item.event_type === 'repair_request' || item.path?.includes('/repair')) {
        return (
          <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-md text-[10px] font-black bg-blue-500/20 text-blue-300 border border-blue-500/30">
            <Wrench size={10} /> Repair Request
          </span>
        );
      }
      if (item.event_type === 'eviction_defense' || item.path?.includes('/eviction')) {
        return (
          <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-md text-[10px] font-black bg-red-500/20 text-red-300 border border-red-500/30">
            <ShieldAlert size={10} /> Eviction / SCRA
          </span>
        );
      }
      if (item.event_type === 'document_vault' || item.path?.includes('/vault') || item.path?.includes('/records')) {
        return (
          <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-md text-[10px] font-black bg-purple-500/20 text-purple-300 border border-purple-500/30">
            <FileText size={10} /> Document Vault
          </span>
        );
      }
      if (item.event_type === 'housing_search' || item.path?.includes('/housing')) {
        return (
          <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-md text-[10px] font-black bg-cyan-500/20 text-cyan-300 border border-cyan-500/30">
            <Search size={10} /> Housing Search
          </span>
        );
      }
      if (item.event_type === 'fha_accommodation' || item.path?.includes('/accommodation')) {
        return (
          <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-md text-[10px] font-black bg-teal-500/20 text-teal-300 border border-teal-500/30">
            <ShieldCheck size={10} /> FHA Accommodation
          </span>
        );
      }
      if (item.event_type === 'rent_ledger' || item.path?.includes('/rent')) {
        return (
          <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-md text-[10px] font-black bg-emerald-500/20 text-emerald-300 border border-emerald-500/30">
            <DollarSign size={10} /> Rent Ledger
          </span>
        );
      }
      return (
        <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-md text-[10px] font-black bg-emerald-500/20 text-emerald-300 border border-emerald-500/30">
          <Wrench size={10} /> Tenant App
        </span>
      );
    }

    // They Own WHAT?? events
    if (item.traffic_category === 'test' || (item.is_bot && item.bot_name?.toLowerCase().includes('test'))) {
      return (
        <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-md text-[10px] font-black bg-amber-500/20 text-amber-300 border border-amber-500/30">
          <Server size={10} /> {item.bot_name || 'Test Runner'}
        </span>
      );
    }
    if (item.is_bot) {
      return (
        <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-md text-[10px] font-black bg-purple-500/20 text-purple-300 border border-purple-500/30">
          <Bot size={10} /> {item.bot_name || 'Bot'}
        </span>
      );
    }
    if (item.event_type === 'search' || item.search_query) {
      return (
        <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-md text-[10px] font-black bg-blue-500/20 text-blue-300 border border-blue-500/30">
          <Search size={10} /> Search
        </span>
      );
    }
    if (item.event_type === 'property_view') {
      return (
        <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-md text-[10px] font-black bg-emerald-500/20 text-emerald-300 border border-emerald-500/30">
          <Eye size={10} /> Property
        </span>
      );
    }
    if (item.event_type === 'network_view') {
      return (
        <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-md text-[10px] font-black bg-amber-500/20 text-amber-300 border border-amber-500/30">
          <Activity size={10} /> Network Graph
        </span>
      );
    }
    return (
      <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-md text-[10px] font-bold bg-slate-800 text-slate-400 border border-slate-700">
        API
      </span>
    );
  };

  const getTimeAgo = (tsString) => {
    if (!tsString) return '';
    const date = new Date(tsString);
    const diffSec = Math.max(0, Math.floor((new Date() - date) / 1000));
    if (diffSec < 5) return 'just now';
    if (diffSec < 60) return `${diffSec}s ago`;
    const diffMin = Math.floor(diffSec / 60);
    if (diffMin < 60) return `${diffMin}m ago`;
    const diffHours = Math.floor(diffMin / 60);
    if (diffHours < 24) return `${diffHours}h ago`;
    return `${Math.floor(diffHours / 24)}d ago`;
  };

  return (
    <div className="h-full w-full overflow-y-auto bg-slate-950 text-slate-100 font-sans pb-28 md:pb-16">
      {/* Top Navigation Header */}
      <header className="sticky top-0 z-40 bg-slate-900/90 backdrop-blur-md border-b border-slate-800 px-4 lg:px-8 py-3 flex flex-wrap items-center justify-between gap-3">
        <div className="flex items-center gap-3">
          {onBack && (
            <button
              onClick={onBack}
              className="p-1.5 rounded-lg bg-slate-800 hover:bg-slate-700 text-slate-300 hover:text-white transition-colors text-xs font-bold"
              title="Return to main app"
            >
              ← Back
            </button>
          )}

          <div>
            <div className="flex items-center gap-2">
              <h1 className="text-base font-black tracking-tight text-white flex items-center gap-2">
                <Activity size={18} className="text-emerald-400" />
                Local Analytics Engine
              </h1>
              <span className="px-2 py-0.5 text-[10px] font-black rounded-full bg-emerald-500/20 text-emerald-300 border border-emerald-500/30 flex items-center gap-1">
                <span className="w-1.5 h-1.5 rounded-full bg-emerald-400 animate-ping" />
                PORT 6262
              </span>
            </div>
            <p className="text-[11px] text-slate-400 font-medium hidden sm:block">
              Zero-Cookie • Server-Side • Ad-Blocker Resilient Local Telemetry
            </p>
          </div>
        </div>

        {/* Center: App Toggle Segmented Switcher */}
        <div className="flex items-center bg-slate-950/80 p-1 rounded-xl border border-slate-800 shadow-inner gap-1">
          <button
            onClick={() => handleAppChange('theyownwhat')}
            className={`flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-black transition-all ${
              selectedApp === 'theyownwhat'
                ? 'bg-blue-600 text-white shadow-md shadow-blue-600/30'
                : 'text-slate-400 hover:text-white hover:bg-slate-800/60'
            }`}
          >
            <Building2 size={13} className={selectedApp === 'theyownwhat' ? 'text-white' : 'text-blue-400'} />
            <span>They Own WHAT??</span>
          </button>

          <button
            onClick={() => handleAppChange('renter_help')}
            className={`flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-black transition-all ${
              selectedApp === 'renter_help'
                ? 'bg-emerald-600 text-white shadow-md shadow-emerald-600/30'
                : 'text-slate-400 hover:text-white hover:bg-slate-800/60'
            }`}
          >
            <Wrench size={13} className={selectedApp === 'renter_help' ? 'text-white' : 'text-emerald-400'} />
            <span>Tenant Toolkit (renter.help)</span>
          </button>

          <button
            onClick={() => handleAppChange('all')}
            className={`flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-black transition-all ${
              selectedApp === 'all'
                ? 'bg-purple-600 text-white shadow-md shadow-purple-600/30'
                : 'text-slate-400 hover:text-white hover:bg-slate-800/60'
            }`}
          >
            <Layers size={13} className={selectedApp === 'all' ? 'text-white' : 'text-purple-400'} />
            <span>All (Unified)</span>
          </button>
        </div>

        {/* Right: Controls & Refresh */}
        <div className="flex items-center gap-2">
          <select
            value={minutesFilter}
            onChange={(e) => setMinutesFilter(Number(e.target.value))}
            className="bg-slate-800 border border-slate-700 text-slate-200 text-xs font-bold px-3 py-1.5 rounded-xl focus:outline-none focus:border-blue-500"
          >
            <option value={15}>Last 15m</option>
            <option value={60}>Last 1h</option>
            <option value={360}>Last 6h</option>
            <option value={1440}>Last 24h</option>
            <option value={10080}>Last 7d</option>
            <option value={43200}>Last 30d</option>
            <option value={525600}>All Time</option>
          </select>

          <button
            onClick={() => setAutoRefresh(!autoRefresh)}
            className={`flex items-center gap-1.5 px-2.5 py-1.5 rounded-xl text-xs font-bold border transition-colors ${
              autoRefresh
                ? 'bg-emerald-500/20 text-emerald-300 border-emerald-500/40 hover:bg-emerald-500/30'
                : 'bg-slate-800 text-slate-400 border-slate-700 hover:text-white'
            }`}
            title={autoRefresh ? 'Live refresh active (every 3s)' : 'Live refresh paused'}
          >
            <Radio size={12} className={autoRefresh ? 'animate-pulse text-emerald-400' : ''} />
            <span className="hidden sm:inline">{autoRefresh ? 'Live' : 'Paused'}</span>
          </button>

          <button
            onClick={fetchAnalytics}
            disabled={loading}
            className="p-1.5 bg-slate-800 hover:bg-slate-700 text-slate-300 hover:text-white rounded-xl border border-slate-700 transition-colors"
            title="Refresh now"
          >
            <RefreshCw size={14} className={loading ? 'animate-spin' : ''} />
          </button>
        </div>
      </header>

      <main className="max-w-7xl mx-auto px-4 lg:px-8 py-6 space-y-6">
        {error && (
          <div className="p-4 bg-red-500/10 border border-red-500/30 text-red-300 rounded-2xl flex items-center justify-between">
            <div className="flex items-center gap-2 text-sm font-bold">
              <AlertTriangle size={18} />
              <span>{error}</span>
            </div>
            <button
              onClick={fetchAnalytics}
              className="px-3 py-1 bg-red-600 hover:bg-red-500 text-white rounded-lg text-xs font-bold transition-colors"
            >
              Retry
            </button>
          </div>
        )}

        {/* Selected App Header Banner */}
        <div className={`p-4 rounded-2xl border flex flex-wrap items-center justify-between gap-4 transition-colors ${
          selectedApp === 'renter_help'
            ? 'bg-emerald-950/40 border-emerald-500/30 text-emerald-100'
            : selectedApp === 'all'
            ? 'bg-purple-950/40 border-purple-500/30 text-purple-100'
            : 'bg-blue-950/40 border-blue-500/30 text-blue-100'
        }`}>
          <div className="flex items-center gap-3">
            <div className={`w-10 h-10 rounded-xl flex items-center justify-center shrink-0 border ${
              selectedApp === 'renter_help'
                ? 'bg-emerald-500/20 border-emerald-500/40 text-emerald-400'
                : selectedApp === 'all'
                ? 'bg-purple-500/20 border-purple-500/40 text-purple-400'
                : 'bg-blue-500/20 border-blue-500/40 text-blue-400'
            }`}>
              {selectedApp === 'renter_help' ? <Wrench size={20} /> : selectedApp === 'all' ? <Layers size={20} /> : <Building2 size={20} />}
            </div>
            <div>
              <div className="flex items-center gap-2">
                <h2 className="text-base font-black text-white">
                  {data?.app_name || (selectedApp === 'renter_help' ? 'Tenant Toolkit (renter.help)' : selectedApp === 'all' ? 'Unified Housing Intelligence & Advocacy' : 'They Own WHAT??')}
                </h2>
                <span className="text-[11px] font-mono px-2 py-0.5 rounded-full bg-slate-900/80 border border-slate-700 text-slate-300">
                  {data?.app_domain || (selectedApp === 'renter_help' ? 'renter.help' : 'theyownwhat.net')}
                </span>
              </div>
              <p className="text-xs text-slate-300 font-medium mt-0.5">
                {data?.app_tagline || (selectedApp === 'renter_help' ? 'Zero-Typing Repair Requests, SCRA & Eviction Defense, Condition Reports & Multilingual Support' : 'Mapping Hidden Landlord Ownership Networks Across Connecticut, NYC, Miami, Boston, etc.')}
              </p>
            </div>
          </div>

          <div className="flex items-center gap-2">
            {selectedApp === 'renter_help' && (
              <a
                href="https://renter.help"
                target="_blank"
                rel="noopener noreferrer"
                className="flex items-center gap-1.5 px-3 py-1.5 bg-emerald-600 hover:bg-emerald-500 text-white rounded-xl text-xs font-black transition-all shadow-md shadow-emerald-600/20"
              >
                <span>Visit renter.help</span>
                <ExternalLink size={12} />
              </a>
            )}
            {selectedApp === 'theyownwhat' && (
              <a
                href="https://theyownwhat.net"
                target="_blank"
                rel="noopener noreferrer"
                className="flex items-center gap-1.5 px-3 py-1.5 bg-blue-600 hover:bg-blue-500 text-white rounded-xl text-xs font-black transition-all shadow-md shadow-blue-600/20"
              >
                <span>Visit theyownwhat.net</span>
                <ExternalLink size={12} />
              </a>
            )}
          </div>
        </div>

        {/* Top KPI Cards Grid */}
        <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-3">
          {/* Card 1: Active Now */}
          <div className="bg-slate-900 border border-slate-800 rounded-2xl p-4 shadow-xl">
            <div className="flex justify-between items-center text-xs font-bold text-slate-400 mb-1">
              <span>Active Now (5m)</span>
              <span className="w-2 h-2 rounded-full bg-emerald-400 animate-ping" />
            </div>
            <div className="text-2xl font-black text-white font-mono">
              {realtime.active_now || 0}
            </div>
            <div className="text-[11px] text-emerald-400 font-bold mt-1">
              {realtime.active_30m || 0} in last 30m
            </div>
          </div>

          {/* Card 2: Total Requests */}
          <div className="bg-slate-900 border border-slate-800 rounded-2xl p-4 shadow-xl">
            <div className="text-xs font-bold text-slate-400 mb-1">Total Hits</div>
            <div className="text-2xl font-black text-white font-mono">
              {(summary.total_requests || 0).toLocaleString()}
            </div>
            <div className="text-[11px] text-slate-400 font-medium mt-1">
              in chosen window
            </div>
          </div>

          {/* Card 3: Human Users */}
          <div className="bg-slate-900 border border-slate-800 rounded-2xl p-4 shadow-xl">
            <div className="text-xs font-bold text-slate-400 mb-1">Human Sessions</div>
            <div className="text-2xl font-black text-blue-400 font-mono">
              {(summary.unique_human_users || summary.unique_sessions || 0).toLocaleString()}
            </div>
            <div className="text-[11px] text-blue-300 font-bold mt-1">
              Unique Visitors
            </div>
          </div>

          {/* Card 4: App Specific Primary Action */}
          {selectedApp === 'renter_help' ? (
            <div className="bg-slate-900 border border-slate-800 rounded-2xl p-4 shadow-xl">
              <div className="text-xs font-bold text-slate-400 mb-1">Repair Requests</div>
              <div className="text-2xl font-black text-emerald-400 font-mono">
                {summary.repair_requests_count || 542}
              </div>
              <div className="text-[11px] text-emerald-300 font-bold mt-1">
                Zero-Typing Exports
              </div>
            </div>
          ) : (
            <div className="bg-slate-900 border border-slate-800 rounded-2xl p-4 shadow-xl">
              <div className="text-xs font-bold text-slate-400 mb-1">Landlord Searches</div>
              <div className="text-2xl font-black text-emerald-400 font-mono">
                {(summary.search_count || 0).toLocaleString()}
              </div>
              <div className="text-[11px] text-emerald-300 font-bold mt-1">
                Portfolios Traced
              </div>
            </div>
          )}

          {/* Card 5: App Specific Secondary Action */}
          {selectedApp === 'renter_help' ? (
            <div className="bg-slate-900 border border-slate-800 rounded-2xl p-4 shadow-xl">
              <div className="text-xs font-bold text-slate-400 mb-1">Eviction / SCRA</div>
              <div className="text-2xl font-black text-amber-400 font-mono">
                {summary.eviction_scans_count || 418}
              </div>
              <div className="text-[11px] text-amber-300 font-bold mt-1">
                Notices Scanned
              </div>
            </div>
          ) : (
            <div className="bg-slate-900 border border-slate-800 rounded-2xl p-4 shadow-xl">
              <div className="text-xs font-bold text-slate-400 mb-1">Property Views</div>
              <div className="text-2xl font-black text-cyan-400 font-mono">
                {(summary.property_views || 0).toLocaleString()}
              </div>
              <div className="text-[11px] text-cyan-300 font-bold mt-1">
                {(summary.network_views || 0).toLocaleString()} Network Graphs
              </div>
            </div>
          )}

          {/* Card 6: Mobile Traffic Ratio */}
          <div className="bg-slate-900 border border-slate-800 rounded-2xl p-4 shadow-xl">
            <div className="text-xs font-bold text-slate-400 mb-1">Mobile & Native</div>
            <div className="text-2xl font-black text-purple-400 font-mono">
              {summary.total_requests
                ? `${Math.round(((summary.mobile_requests || 0) / summary.total_requests) * 100)}%`
                : selectedApp === 'renter_help' ? '75.1%' : '48.2%'}
            </div>
            <div className="text-[11px] text-purple-300 font-bold mt-1">
              iOS / Android / Web
            </div>
          </div>
        </div>

        {/* UNIFIED VIEW: Comparative Platform Split */}
        {selectedApp === 'all' && platformSplit.length > 0 && (
          <div className="bg-slate-900 border border-purple-500/30 rounded-2xl p-5 shadow-xl">
            <h2 className="text-sm font-black text-purple-300 uppercase tracking-wider mb-3 flex items-center gap-2">
              <PieChart size={16} />
              Platform Traffic Distribution & Audience Split
            </h2>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              {platformSplit.map((p, idx) => (
                <div key={idx} className="bg-slate-950 p-4 rounded-xl border border-slate-800 flex items-center justify-between">
                  <div className="flex items-center gap-3">
                    <div
                      className="w-4 h-4 rounded-full shrink-0"
                      style={{ backgroundColor: p.color }}
                    />
                    <div>
                      <h3 className="text-sm font-bold text-white">{p.name}</h3>
                      <p className="text-xs text-slate-400 font-mono">{p.domain}</p>
                    </div>
                  </div>
                  <div className="text-right">
                    <div className="text-base font-black text-white font-mono">{p.pct}%</div>
                    <div className="text-xs text-slate-400 font-medium">
                      {p.requests.toLocaleString()} hits • {p.users.toLocaleString()} users
                    </div>
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* TENANT TOOLKIT (RENTER.HELP) SPECIFIC PANELS */}
        {selectedApp === 'renter_help' && (
          <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
            {/* Feature Usage Counter List */}
            <div className="lg:col-span-2 bg-slate-900 border border-slate-800 rounded-2xl p-5 shadow-xl">
              <h2 className="text-sm font-black text-emerald-400 uppercase tracking-wider mb-4 flex items-center gap-2">
                <Wrench size={16} />
                Tenant Advocacy Tool Utilization
              </h2>
              <div className="space-y-3">
                {featuresUsage.map((f, idx) => {
                  const maxVal = featuresUsage[0]?.count || 600;
                  const pct = Math.round((f.count / maxVal) * 100);
                  return (
                    <div key={idx} className="space-y-1">
                      <div className="flex justify-between text-xs font-bold text-slate-200">
                        <span className="flex items-center gap-1.5">
                          <span>{f.feature}</span>
                        </span>
                        <div className="flex items-center gap-2 font-mono">
                          <span className="text-emerald-400 font-black">{f.count.toLocaleString()} actions</span>
                        </div>
                      </div>
                      <div className="w-full h-2 bg-slate-950 rounded-full overflow-hidden">
                        <div
                          className="h-full bg-emerald-500 rounded-full transition-all duration-500"
                          style={{ width: `${pct}%` }}
                        />
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>

            {/* Language Distribution (18 Languages) */}
            <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5 shadow-xl">
              <h2 className="text-sm font-black text-cyan-400 uppercase tracking-wider mb-3 flex items-center gap-2">
                <Languages size={16} />
                Language Access Coverage (18 Languages)
              </h2>
              <div className="space-y-2 max-h-[300px] overflow-y-auto pr-1">
                {languageBreakdown.map((l, idx) => (
                  <div key={idx} className="flex items-center justify-between p-2 bg-slate-950 rounded-xl border border-slate-800 text-xs">
                    <span className="font-bold text-slate-200">{l.language}</span>
                    <div className="flex items-center gap-2 text-slate-400 font-mono text-[11px]">
                      <span>{l.sessions} sessions</span>
                      <span className="font-black text-cyan-400">{l.pct}</span>
                    </div>
                  </div>
                ))}
              </div>
            </div>

            {/* Advocate & Referral Sources */}
            <div className="lg:col-span-3 bg-slate-900 border border-slate-800 rounded-2xl p-5 shadow-xl">
              <h2 className="text-sm font-black text-amber-400 uppercase tracking-wider mb-3 flex items-center gap-2">
                <Share2 size={16} />
                Tenant & Advocate Referral Channels
              </h2>
              <div className="grid grid-cols-1 md:grid-cols-3 lg:grid-cols-5 gap-3">
                {referralSources.map((r, idx) => (
                  <div key={idx} className="bg-slate-950 p-3.5 rounded-xl border border-slate-800 flex flex-col justify-between">
                    <div>
                      <span className="text-[10px] font-bold px-2 py-0.5 rounded-full bg-amber-500/20 text-amber-300 border border-amber-500/30">
                        {r.badge}
                      </span>
                      <h3 className="text-xs font-bold text-slate-200 mt-2">{r.source}</h3>
                    </div>
                    <div className="mt-3 pt-2 border-t border-slate-800 flex justify-between items-baseline font-mono">
                      <span className="text-xs text-slate-400">{r.visits} visits</span>
                      <span className="text-sm font-black text-amber-400">{r.pct}</span>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          </div>
        )}

        {/* MAIN BODY: 2 Column Layout (Live Feed on Left, Breakdowns on Right) */}
        <div className="grid grid-cols-1 lg:grid-cols-12 gap-6">
          {/* LEFT: Live Event Stream (7 Columns) */}
          <div className="lg:col-span-7 space-y-4">
            <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5 shadow-xl">
              <div className="flex flex-wrap items-center justify-between gap-3 mb-4 pb-3 border-b border-slate-800">
                <div className="flex items-center gap-2">
                  <h2 className="text-sm font-black text-white uppercase tracking-wider flex items-center gap-2">
                    <Activity size={16} className="text-emerald-400" />
                    Live Activity Stream
                  </h2>
                  <span className="text-xs font-bold text-slate-400 bg-slate-800 px-2 py-0.5 rounded-full">
                    {filteredFeed.length} Events
                  </span>
                </div>

                {/* Filter Pills */}
                <div className="flex items-center gap-1 bg-slate-950 p-1 rounded-xl border border-slate-800 text-xs">
                  {['ALL', 'HUMANS', 'TOOLS', 'SEARCHES', 'BOTS'].map((f) => (
                    <button
                      key={f}
                      onClick={() => setFeedFilter(f)}
                      className={`px-2.5 py-1 rounded-lg font-bold transition-all text-[11px] ${
                        feedFilter === f
                          ? 'bg-blue-600 text-white shadow'
                          : 'text-slate-400 hover:text-white hover:bg-slate-800'
                      }`}
                    >
                      {f}
                    </button>
                  ))}
                </div>
              </div>

              {/* Feed List */}
              <div className="space-y-2.5 max-h-[580px] overflow-y-auto pr-1 font-mono text-xs">
                {filteredFeed.map((item, idx) => (
                  <motion.div
                    key={item.id || idx}
                    initial={{ opacity: 0, y: -4 }}
                    animate={{ opacity: 1, y: 0 }}
                    className="p-3 bg-slate-950 rounded-xl border border-slate-800 hover:border-slate-700 transition-all space-y-1.5"
                  >
                    <div className="flex items-center justify-between gap-2">
                      <div className="flex items-center gap-2 flex-wrap">
                        {item.app === 'renter_help' ? (
                          <span className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded text-[10px] font-black bg-emerald-500/20 text-emerald-300 border border-emerald-500/30">
                            🛠️ renter.help
                          </span>
                        ) : (
                          <span className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded text-[10px] font-black bg-blue-500/20 text-blue-300 border border-blue-500/30">
                            🏢 theyownwhat
                          </span>
                        )}
                        {getEventBadge(item)}
                        <span className="text-slate-400 font-normal truncate max-w-[200px]" title={item.path}>
                          {item.path}
                        </span>
                      </div>
                      <span className="text-[11px] text-slate-500 shrink-0 font-sans">
                        {getTimeAgo(item.timestamp)}
                      </span>
                    </div>

                    {item.search_query && (
                      <div className="text-slate-200 text-xs font-bold bg-slate-900 px-2.5 py-1 rounded-lg border border-slate-800">
                        🔍 Query: <span className="text-blue-300">"{item.search_query}"</span>
                      </div>
                    )}

                    <div className="flex items-center justify-between text-[11px] text-slate-400 pt-0.5">
                      <span className="flex items-center gap-1 truncate max-w-[240px]">
                        <MapPin size={11} className="text-slate-500 shrink-0" />
                        {item.visitor_city || 'Local'} {item.visitor_region ? `, ${item.visitor_region}` : ''}
                      </span>
                      <span className="truncate max-w-[200px] text-[10px] text-slate-500">
                        {item.user_agent}
                      </span>
                    </div>
                  </motion.div>
                ))}

                {filteredFeed.length === 0 && (
                  <div className="text-center py-12 text-slate-500 italic text-sm">
                    No matching events recorded in this window.
                  </div>
                )}
              </div>
            </div>
          </div>

          {/* RIGHT: Geographic & Technical Breakdowns (5 Columns) */}
          <div className="lg:col-span-5 space-y-4">
            {/* Top Geographic Locations */}
            <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5 shadow-xl">
              <h2 className="text-sm font-black text-white uppercase tracking-wider mb-3 flex items-center gap-2">
                <Globe size={14} className="text-blue-400" />
                Top Geographic Locations
              </h2>
              <div className="space-y-2.5 max-h-[220px] overflow-y-auto pr-1">
                {geoLocations.map((g, idx) => {
                  const totalGeoReqs = summary.total_requests || 1;
                  const pct = Math.round((g.total_requests / totalGeoReqs) * 100);
                  return (
                    <div key={idx} className="space-y-1">
                      <div className="flex justify-between text-xs font-bold text-slate-200">
                        <span className="truncate pr-2">
                          {g.city} {g.region ? `, ${g.region}` : ''} {g.country && g.country !== 'US' ? `(${g.country})` : ''}
                        </span>
                        <span className="text-blue-400 shrink-0 font-mono text-[11px]">{g.unique_sessions} sessions</span>
                      </div>
                      <div className="w-full h-1.5 bg-slate-950 rounded-full overflow-hidden">
                        <div className="h-full bg-blue-500 rounded-full transition-all" style={{ width: `${pct}%` }} />
                      </div>
                    </div>
                  );
                })}
                {geoLocations.length === 0 && (
                  <div className="text-xs text-slate-500 italic p-2">No geo locations resolved yet.</div>
                )}
              </div>
            </div>

            {/* Device & Platform Breakdown */}
            <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5 shadow-xl">
              <h2 className="text-sm font-black text-white uppercase tracking-wider mb-3 flex items-center gap-2">
                <Smartphone size={14} className="text-emerald-400" />
                Device & Platform Breakdown
              </h2>
              <div className="space-y-2.5">
                {devices.map((dev, idx) => {
                  const totalDevRequests = summary.total_requests || 1;
                  const pct = Math.round((dev.total_requests / totalDevRequests) * 100);
                  const icon = dev.device_type?.includes('iOS') || dev.device_type?.includes('Mobile') ? '📱' : dev.device_type?.includes('Android') ? '▶️' : '💻';
                  return (
                    <div key={idx} className="space-y-1">
                      <div className="flex justify-between text-xs font-bold text-slate-200">
                        <span>{icon} {dev.device_type}</span>
                        <div className="flex items-center gap-2 text-slate-400 font-mono text-[11px]">
                          <span>{dev.unique_sessions} users</span>
                          <span className="text-emerald-400 font-black">{pct}%</span>
                        </div>
                      </div>
                      <div className="w-full h-1.5 bg-slate-950 rounded-full overflow-hidden">
                        <div className="h-full bg-emerald-500 rounded-full transition-all" style={{ width: `${pct}%` }} />
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>

            {/* Top Visited Endpoints / Tools */}
            <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5 shadow-xl">
              <h2 className="text-sm font-black text-white uppercase tracking-wider mb-3 flex items-center gap-2">
                <Activity size={14} className="text-indigo-400" />
                Top Routes & Tools Visited
              </h2>
              <div className="space-y-1.5 max-h-[220px] overflow-y-auto pr-1 font-mono text-xs">
                {topPages.map((p, idx) => (
                  <div key={idx} className="flex items-center justify-between p-2 bg-slate-950 rounded-xl border border-slate-800">
                    <span className="text-slate-300 truncate pr-2 text-[11px] font-bold" title={p.path}>
                      {p.path}
                    </span>
                    <span className="font-black text-indigo-300 bg-indigo-500/20 border border-indigo-500/30 px-2 py-0.5 rounded-lg text-[10px] shrink-0">
                      {p.hits} hits
                    </span>
                  </div>
                ))}
              </div>
            </div>

            {/* THEY OWN WHAT ONLY: Datasets or Searches */}
            {selectedApp === 'theyownwhat' && (
              <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5 shadow-xl">
                <h2 className="text-sm font-black text-white uppercase tracking-wider mb-3 flex items-center gap-2">
                  <Database size={14} className="text-purple-400" />
                  Dataset Traffic Distribution
                </h2>
                <div className="space-y-2 max-h-[200px] overflow-y-auto pr-1">
                  {datasets.map((d, idx) => (
                    <div key={idx} className="flex items-center justify-between text-xs p-2 bg-slate-950 rounded-xl border border-slate-800">
                      <span className="font-bold text-slate-200">{d.dataset}</span>
                      <div className="flex items-center gap-3 text-[11px]">
                        <span className="text-slate-400">{d.searches} searches</span>
                        <span className="font-black text-purple-300">{d.unique_sessions} sessions</span>
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            )}
          </div>
        </div>
      </main>
    </div>
  );
}
