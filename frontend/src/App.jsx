import React, { useState } from 'react';
import Header from './components/Header';
import SearchBar from './components/SearchBar';
import NetworkView from './components/NetworkView';
import PropertyTable from './components/PropertyTable';
import { api } from './api';
import Insights from './components/Insights';
import SearchResults from './components/SearchResults';
import PropertyDetailsModal from './components/PropertyDetailsModal';
import EntityDetailsModal from './components/EntityDetailsModal';
import NetworkAnalysisModal from './components/NetworkAnalysisModal';
import AboutModal from './components/AboutModal';
import { motion, AnimatePresence } from 'framer-motion';
import { Sparkles, Loader2, Search, ArrowRight, Building2, TrendingUp, Users } from 'lucide-react';

// NOTE: This is a simplified App.jsx. In a real scenario we'd use React Router.
function App() {
  const [view, setView] = useState('home'); // home | dashboard
  const [loading, setLoading] = useState(false);
  const [insights, setInsights] = useState(null);
  const [searchResults, setSearchResults] = useState(null);
  const [networkData, setNetworkData] = useState({
    principals: [],
    businesses: [],
    properties: [],
    links: []
  });
  const [stats, setStats] = useState({
    totalValue: 0,
    totalProperties: 0,
  });

  // Dashboard State
  const [selectedCity, setSelectedCity] = useState('All');
  const [selectedEntityId, setSelectedEntityId] = useState(null);
  const [showAnalysis, setShowAnalysis] = useState(false);
  const [showAbout, setShowAbout] = useState(false);

  // Mobile Tabs
  const [activeMobileTab, setActiveMobileTab] = useState('properties');
  const [aiEnabled, setAiEnabled] = useState(false);

  const [loadingInsights, setLoadingInsights] = useState(true);
  const [streamingStatus, setStreamingStatus] = useState({
    entities: 0,
    properties: 0,
    active: false
  });

  // Init Insights
  React.useEffect(() => {
    api.get('/health')
      .then(data => {
        if (data && data.ai_enabled) {
          setAiEnabled(true);
        }
      })
      .catch(err => console.warn("Health check failed", err));

    setLoadingInsights(true);
    api.get('/insights')
      .then(data => {
        console.log("Insights loaded:", data);
        setInsights(data);
      })
      .catch(err => console.error("Failed to load insights", err))
      .finally(() => setLoadingInsights(false));
  }, []);

  // Search Handler
  const handleSearch = async (type, term) => {
    setLoading(true);
    setSearchResults(null); // Clear previous results
    try {
      const results = await api.get(`/search?type=${type}&term=${term}`);
      console.log("Search results:", results);

      if (results && results.length > 0) {
        setSearchResults(results);
      } else {
        console.warn("No results found for", type, term);
        alert("No results found.");
      }
    } catch (err) {
      console.error(err);
      alert("Search failed. Please try again.");
    } finally {
      setLoading(false);
    }
  };

  // Load Network Stream
  const loadNetwork = async (id, type) => {
    setLoading(true);
    setStreamingStatus({ entities: 0, properties: 0, active: true });

    const newData = { principals: [], businesses: [], properties: [], links: [] };
    const seenEntities = new Set();

    api.streamNetwork(id, type,
      (chunk) => {
        if (chunk.type === 'entities') {
          if (chunk.data.entities) {
            setStreamingStatus(prev => ({
              ...prev,
              entities: prev.entities + chunk.data.entities.length
            }));
            chunk.data.entities.forEach(e => {
              const key = `${e.type}_${e.id}`;
              if (!seenEntities.has(key)) {
                seenEntities.add(key);
                if (e.type === 'principal') newData.principals.push(e);
                else newData.businesses.push(e);

                // Simple heurustic
                if (e.type === 'principal') {
                  e.isEntity = e.name && e.name.match(/(LLC|INC|CORP|LTD|GROUP|HOLDINGS)/i);
                }
                // Extract connections if present
                if (e.details && e.details.connections && Array.isArray(e.details.connections)) {
                  e.details.connections.forEach(conn => {
                    // Connection can be ID string or object
                    const targetId = typeof conn === 'object' ? conn.id : conn;
                    if (targetId) {
                      newData.links.push({ source: e.id, target: targetId });
                    }
                  });
                }
              }
            });
          }

          if (chunk.data.links) {
            if (Array.isArray(chunk.data.links)) {
              newData.links.push(...chunk.data.links);
            } else if (typeof chunk.data.links === 'object') {
              Object.entries(chunk.data.links).forEach(([k, v]) => {
                if (Array.isArray(v)) {
                  v.forEach(item => {
                    if (item && typeof item === 'object' && item.source && item.target) {
                      newData.links.push(item);
                    } else if (typeof item === 'string' || typeof item === 'number') {
                      newData.links.push({ source: k, target: item });
                    }
                  });
                }
              });
            }
          }
        } else if (chunk.type === 'properties') {
          if (Array.isArray(chunk.data)) {
            setStreamingStatus(prev => ({
              ...prev,
              properties: prev.properties + chunk.data.length
            }));
            for (const prop of chunk.data) {
              newData.properties.push(prop);
            }
          }
        }
      },
      () => {
        // On Complete
        setNetworkData(newData);

        // Calc stats
        const totalVal = newData.properties.reduce((acc, p) => {
          const val = parseFloat(String(p.assessed_value || '0').replace(/[^0-9.]/g, ''));
          return acc + val;
        }, 0);

        setStats({
          totalProperties: newData.properties.length,
          totalValue: totalVal
        });

        setStreamingStatus(prev => ({ ...prev, active: false }));
        setView('dashboard');
        setLoading(false);
      },
      (err) => {
        console.error("Stream error", err);
        setStreamingStatus(prev => ({ ...prev, active: false }));
        setLoading(false);
      }
    );
  };

  const handleReset = () => {
    setView('home');
    setNetworkData({ principals: [], businesses: [], properties: [] });
    setSearchResults(null);
    setSelectedCity('All');
    setSelectedEntityId(null);
  };

  // Filter Properties Logic
  const filteredProperties = React.useMemo(() => {
    // Pre-calculate allowed business IDs if an entity is selected
    // Pre-calculate allowed business IDs and Principal Norm Name if an entity is selected
    let allowedBusinessIds = null;
    let selectedPrincipalNorm = null;

    if (selectedEntityId) {
      const sId = String(selectedEntityId);
      allowedBusinessIds = new Set();
      allowedBusinessIds.add(sId); // If it's a business ID

      // Check if it's a principal to also filter by direct ownership
      const principal = networkData.principals.find(p => String(p.id) === sId);
      if (principal) {
        // It's a principal, try to get normalized name from ID or details
        selectedPrincipalNorm = principal.id; // ID is usually the norm name for principals
      }

      // Find all businesses linked to this entity (handling backend key prefixes)
      // Link keys are 'principal_NAME' or 'business_ID'. sId is likely 'NAME' or 'ID'.
      const variants = new Set([sId]);
      if (!sId.startsWith('principal_')) variants.add(`principal_${sId}`);
      if (!sId.startsWith('business_')) variants.add(`business_${sId}`);

      networkData.links.forEach(link => {
        const src = String(link.source);
        const tgt = String(link.target);

        // If this link connects TO our selected entity
        if (variants.has(src)) {
          // The other end is linked. Add it (stripping prefix).
          allowedBusinessIds.add(tgt.replace(/^(principal_|business_)/, ''));
        }
        if (variants.has(tgt)) {
          allowedBusinessIds.add(src.replace(/^(principal_|business_)/, ''));
        }
      });
    }

    return networkData.properties.filter(p => {
      // City Filter
      if (selectedCity !== 'All' && p.city !== selectedCity) return false;

      // Entity Filter
      if (selectedEntityId) {
        let match = false;

        // 1. Business Link Match
        const bid = p.details?.business_id ? String(p.details.business_id) : null;
        if (bid && allowedBusinessIds.has(bid)) match = true;

        // 2. Direct Principal Match (if we identified a principal)
        if (!match && selectedPrincipalNorm) {
          const ownerNorm = p.details?.owner_norm;
          const coOwnerNorm = p.details?.co_owner_norm;
          if (ownerNorm === selectedPrincipalNorm || coOwnerNorm === selectedPrincipalNorm) {
            match = true;
          }
        }

        if (!match) return false;
      }

      return true;
    });
  }, [networkData.properties, selectedCity, selectedEntityId, networkData.links]);

  const [selectedProperty, setSelectedProperty] = useState(null);
  const [selectedDetailEntity, setSelectedDetailEntity] = useState(null);

  // Background Grid Component
  const BackgroundGrid = () => (
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

  return (
    <div className="h-screen bg-slate-50 flex flex-col overflow-hidden font-sans text-slate-900 selection:bg-blue-100 selection:text-blue-900">
      <Header
        onHome={handleReset}
        onReset={view === 'dashboard' ? handleReset : null}
        onAbout={() => setShowAbout(true)}
      />

      <LoadingScreen
        visible={streamingStatus.active}
        entities={streamingStatus.entities}
        properties={streamingStatus.properties}
      />

      <main className="flex-1 overflow-hidden relative z-10">
        {/* HERO / SEARCH SECTION */}
        <AnimatePresence mode="wait">
          {view === 'home' && (
            <div className="h-full overflow-y-auto w-full relative">
              <BackgroundGrid />

              <div className="container mx-auto px-4 pt-12 pb-24 relative z-10">
                <motion.div
                  initial={{ opacity: 0, y: 30 }}
                  animate={{ opacity: 1, y: 0 }}
                  exit={{ opacity: 0, y: -30 }}
                  transition={{ duration: 0.6, ease: "easeOut" }}
                  className="max-w-5xl mx-auto"
                >
                  {/* Hero Content */}
                  <div className="text-center mb-16">


                    <h1 className="text-6xl md:text-8xl font-black text-slate-900 mb-6 tracking-tighter leading-[0.9]">
                      they own <span className="text-transparent bg-clip-text bg-gradient-to-r from-blue-600 to-indigo-600">WHAT??</span>
                    </h1>

                    <p className="text-xl text-slate-500 max-w-2xl mx-auto font-medium leading-relaxed mb-12">
                      Uncover hidden property networks, connect LLCs to real owners, and analyze portfolio value with AI-powered insights.
                    </p>

                    <div className="max-w-2xl mx-auto relative group">
                      <div className="absolute -inset-1 bg-gradient-to-r from-blue-500 to-indigo-500 rounded-2xl opacity-20 group-hover:opacity-30 blur transition duration-500"></div>
                      <div className="relative bg-white rounded-xl shadow-xl border border-slate-100">
                        <SearchBar onSearch={handleSearch} isLoading={loading} />
                      </div>
                    </div>
                  </div>

                  {/* Results or Insights */}
                  <div className="relative min-h-[400px]">
                    {searchResults ? (
                      <SearchResults
                        results={searchResults}
                        onSelect={(id, type) => loadNetwork(id, type)}
                      />
                    ) : (
                      <div className="mt-8">
                        {loadingInsights ? (
                          <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                            {[1, 2, 3].map(i => (
                              <div key={i} className="h-48 bg-white rounded-2xl border border-slate-100 shadow-sm animate-pulse"></div>
                            ))}
                          </div>
                        ) : (
                          <Insights data={insights} onSelect={(id, type) => loadNetwork(id, type)} />
                        )}
                      </div>
                    )}
                  </div>
                </motion.div>
              </div>
            </div>
          )}
        </AnimatePresence>

        {/* DASHBOARD VIEW */}
        {view === 'dashboard' && (
          <motion.div
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            className="flex flex-col h-full w-full max-w-[1920px] mx-auto px-4 py-3 gap-3 overflow-hidden bg-slate-50/50 backdrop-blur-sm lg:overflow-hidden overflow-y-auto"
          >
            {/* Stats Row */}
            <div className="flex flex-col md:flex-row gap-3 items-stretch">
              <div className="grid grid-cols-2 lg:grid-cols-4 gap-3 flex-1">
                <StatCard label="Properties" value={stats.totalProperties} icon={<Building2 className="w-4 h-4 text-slate-400" />} />
                <StatCard label="Portfolio Value" value={`$${(stats.totalValue / 1000000).toFixed(1)}M`} highlight icon={<TrendingUp className="w-4 h-4 text-blue-200" />} />
                <StatCard label="Businesses" value={networkData.businesses.length} icon={<Building2 className="w-4 h-4 text-slate-400" />} />
                <StatCard label="Principals" value={networkData.principals.length} icon={<Users className="w-4 h-4 text-slate-400" />} />
              </div>
              {aiEnabled && (
                <button
                  onClick={() => setShowAnalysis(true)}
                  className="px-6 bg-slate-900 hover:bg-slate-800 text-white rounded-xl shadow-lg hover:shadow-xl hover:-translate-y-0.5 transition-all flex items-center justify-center gap-2 min-w-[140px] group"
                >
                  <div className="relative">
                    <Sparkles className="w-5 h-5 text-indigo-400 group-hover:text-indigo-300 transition-colors" />
                    <div className="absolute inset-0 bg-indigo-400/50 blur-sm opacity-50 animate-pulse"></div>
                  </div>
                  <div className="text-left">
                    <div className="text-[10px] font-bold uppercase tracking-wider text-slate-400 leading-none mb-0.5">Generate</div>
                    <div className="text-sm font-bold">AI Digest</div>
                  </div>
                </button>
              )}
            </div>

            {/* Cross-Filtering & City Selection Controls */}
            <DashboardControls
              properties={networkData.properties}
              selectedCity={selectedCity}
              onSelectCity={setSelectedCity}
              selectedEntityId={selectedEntityId}
              onClearEntity={() => setSelectedEntityId(null)}
            />

            {/* --- MOBILE TAB LAYOUT (lg:hidden) --- */}
            <div className="flex flex-col lg:hidden relative border border-slate-200 rounded-xl bg-white shadow-sm mt-2">

              {/* Sticky Tab Header */}
              <div className="flex items-center border-b border-gray-100 bg-white/95 backdrop-blur-md sticky top-0 z-30 shadow-sm">
                <button
                  onClick={() => setActiveMobileTab('properties')}
                  className={`flex-1 py-3 text-[10px] font-bold uppercase tracking-wider transition-colors border-b-2 ${activeMobileTab === 'properties' ? 'border-blue-500 text-blue-700 bg-blue-50/50' : 'border-transparent text-gray-500 hover:text-gray-700'}`}
                >
                  Properties
                </button>
                <button
                  onClick={() => setActiveMobileTab('businesses')}
                  className={`flex-1 py-3 text-[10px] font-bold uppercase tracking-wider transition-colors border-b-2 ${activeMobileTab === 'businesses' ? 'border-emerald-500 text-emerald-700 bg-emerald-50/50' : 'border-transparent text-gray-500 hover:text-gray-700'}`}
                >
                  Businesses
                </button>
                <button
                  onClick={() => setActiveMobileTab('principals')}
                  className={`flex-1 py-3 text-[10px] font-bold uppercase tracking-wider transition-colors border-b-2 ${activeMobileTab === 'principals' ? 'border-indigo-500 text-indigo-700 bg-indigo-50/50' : 'border-transparent text-gray-500 hover:text-gray-700'}`}
                >
                  Principals
                </button>

                {/* Start Over Button */}
                <button
                  onClick={handleReset}
                  className="px-3 py-3 text-gray-400 hover:text-red-500 hover:bg-red-50 border-b-2 border-transparent transition-colors"
                  title="Start Over"
                >
                  <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
                    <path d="M3 12a9 9 0 1 0 9-9 9.75 9.75 0 0 0-6.74 2.74L3 8" />
                    <path d="M3 3v5h5" />
                  </svg>
                </button>
              </div>

              {/* Content Area - Auto Height for Page Scroll */}
              <div className="bg-white min-h-[500px]">
                {activeMobileTab === 'properties' && (
                  <div className="flex flex-col">
                    <PropertyTable
                      properties={filteredProperties}
                      highlightedEntityId={selectedEntityId}
                      onSelectProperty={setSelectedProperty}
                      forceExpanded={true}
                    />
                  </div>
                )}

                {activeMobileTab === 'businesses' && (
                  <div className="flex flex-col">
                    <NetworkView
                      networkData={networkData}
                      selectedEntityId={selectedEntityId}
                      onSelectEntity={(id, type) => setSelectedEntityId(id === selectedEntityId ? null : id)}
                      onViewDetails={(entity, type) => setSelectedDetailEntity({ entity, type })}
                      mobileSection="businesses"
                      autoHeight={true}
                    />
                  </div>
                )}

                {activeMobileTab === 'principals' && (
                  <div className="flex flex-col">
                    <NetworkView
                      networkData={networkData}
                      selectedEntityId={selectedEntityId}
                      onSelectEntity={(id, type) => setSelectedEntityId(id === selectedEntityId ? null : id)}
                      onViewDetails={(entity, type) => setSelectedDetailEntity({ entity, type })}
                      mobileSection="principals"
                      autoHeight={true}
                    />
                  </div>
                )}
              </div>
            </div>

            {/* --- DESKTOP GRID LAYOUT (hidden lg:grid) --- */}
            <div className="hidden lg:grid grid-cols-12 gap-4 flex-1 min-h-0 overflow-hidden">
              {/* Left: Network List */}
              <div className="col-span-4 h-full bg-white rounded-2xl border border-slate-200 shadow-sm overflow-hidden flex flex-col">
                <div className="p-4 border-b border-slate-100 bg-slate-50/50">
                  <h3 className="text-xs font-bold uppercase tracking-wider text-slate-500">Ownership Network</h3>
                </div>
                <div className="flex-1 overflow-hidden">
                  <NetworkView
                    networkData={networkData}
                    selectedEntityId={selectedEntityId}
                    onSelectEntity={(id, type) => setSelectedEntityId(id === selectedEntityId ? null : id)}
                    onViewDetails={(entity, type) => setSelectedDetailEntity({ entity, type })}
                  />
                </div>
              </div>

              {/* Right: Property Table */}
              <div className="col-span-8 h-full bg-white rounded-2xl border border-slate-200 shadow-sm overflow-hidden flex flex-col">
                <div className="p-4 border-b border-slate-100 bg-slate-50/50 flex justify-between items-center">
                  <h3 className="text-xs font-bold uppercase tracking-wider text-slate-500">Property Portfolio</h3>
                  <div className="text-xs font-bold text-slate-400">{filteredProperties.length} Assets</div>
                </div>
                <div className="flex-1 overflow-hidden">
                  <PropertyTable
                    properties={filteredProperties}
                    highlightedEntityId={selectedEntityId}
                    onSelectProperty={setSelectedProperty}
                  />
                </div>
              </div>
            </div>
          </motion.div>
        )}

        <PropertyDetailsModal
          property={selectedProperty}
          onClose={() => setSelectedProperty(null)}
        />
        <EntityDetailsModal
          entity={selectedDetailEntity?.entity}
          type={selectedDetailEntity?.type}
          onClose={() => setSelectedDetailEntity(null)}
        />

        <NetworkAnalysisModal
          isOpen={showAnalysis}
          onClose={() => setShowAnalysis(false)}
          networkData={networkData}
          stats={stats}
        />
        <AboutModal
          isOpen={showAbout}
          onClose={() => setShowAbout(false)}
        />
      </main>
    </div>
  );
}

function StatCard({ label, value, highlight, icon }) {
  return (
    <div className={`p-4 rounded-xl border flex flex-col justify-center transition-all hover:shadow-md ${highlight
      ? 'bg-gradient-to-br from-blue-600 to-indigo-700 text-white border-blue-600 shadow-lg shadow-blue-500/20'
      : 'bg-white border-slate-200 text-slate-900'
      }`}>
      <div className="flex items-center gap-2 mb-1 opacity-80">
        {icon}
        <span className={`text-[10px] font-bold uppercase tracking-wider ${highlight ? 'text-blue-100' : 'text-slate-400'}`}>{label}</span>
      </div>
      <div className="text-2xl font-black tracking-tight">{value}</div>
    </div>
  );
}

function DashboardControls({ properties, selectedCity, onSelectCity, selectedEntityId, onClearEntity }) {
  const cities = React.useMemo(() => {
    const set = new Set(properties.map(p => p.city).filter(Boolean));
    return ['All', ...Array.from(set).sort()];
  }, [properties]);

  if (properties.length === 0) return null;

  return (
    <div className="flex flex-col sm:flex-row gap-3 items-center justify-between bg-white px-4 py-3 rounded-xl border border-slate-200 shadow-sm">
      <div className="flex items-center gap-3 overflow-x-auto w-full no-scrollbar">
        <span className="text-[10px] font-bold text-slate-400 uppercase tracking-wider whitespace-nowrap">Filter City:</span>
        <div className="flex gap-1.5 flex-1">
          {cities.map(city => (
            <button
              key={city}
              onClick={() => onSelectCity(city)}
              className={`px-3 py-1.5 rounded-lg text-xs font-bold whitespace-nowrap transition-all ${selectedCity === city
                ? 'bg-slate-900 text-white shadow-md'
                : 'bg-slate-50 text-slate-500 hover:bg-slate-100'
                }`}
            >
              {city}
            </button>
          ))}
        </div>
      </div>

      {selectedEntityId && (
        <button
          onClick={onClearEntity}
          className="ml-4 px-3 py-1.5 bg-amber-50 text-amber-700 text-xs font-bold rounded-lg border border-amber-200 hover:bg-amber-100 transition-colors flex items-center gap-2 whitespace-nowrap animate-in fade-in slide-in-from-right-2"
        >
          <span>Entity Filter Active</span>
          <X size={12} />
        </button>
      )}
    </div>
  );
}

// Simple X icon component for the filter button
const X = ({ size = 16 }) => (
  <svg width={size} height={size} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="3" strokeLinecap="round" strokeLinejoin="round">
    <path d="M18 6 6 18" /><path d="m6 6 12 12" />
  </svg>
);


function LoadingScreen({ visible, entities, properties }) {
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

export default App;
