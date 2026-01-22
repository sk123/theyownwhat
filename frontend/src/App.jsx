import React, { useState, Suspense, useTransition } from 'react';
import Header from './components/Header';
import SearchBar from './components/SearchBar';
import NetworkView from './components/NetworkView';
import PropertyTable from './components/PropertyTable';
import { api } from './api';
import Insights from './components/Insights';
import SearchResults from './components/SearchResults';
const PropertyDetailsModal = React.lazy(() => import('./components/PropertyDetailsModal'));
const EntityDetailsModal = React.lazy(() => import('./components/EntityDetailsModal'));
const NetworkAnalysisModal = React.lazy(() => import('./components/NetworkAnalysisModal'));
const AboutModal = React.lazy(() => import('./components/AboutModal'));
const MultiPropertyMapModal = React.lazy(() => import('./components/MultiPropertyMapModal'));
import LoadingScreen from './components/LoadingScreen';
// import DashboardControls from './components/DashboardControls'; // Removed
import StatCard from './components/StatCard';
import BackgroundGrid from './components/BackgroundGrid';
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
    totalAppraised: 0,
    totalProperties: 0,
    humanCount: 0,
    entityCount: 0
  });

  // Map Selection State
  const [selectedMapProperties, setSelectedMapProperties] = useState(null);

  // Dashboard State
  const [selectedCity, setSelectedCity] = useState('All');
  const [selectedEntityId, setSelectedEntityId] = useState(null);
  const [showAnalysis, setShowAnalysis] = useState(false);
  const [showAbout, setShowAbout] = useState(false);

  // Mobile Tabs
  const [activeMobileTab, setActiveMobileTab] = useState('properties');
  const [isPending, startTransition] = useTransition();

  const handleTabChange = (tab) => {
    startTransition(() => {
      setActiveMobileTab(tab);
    });
  };
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

        const totalApp = newData.properties.reduce((acc, p) => {
          const val = parseFloat(String(p.appraised_value || '0').replace(/[^0-9.]/g, ''));
          return acc + val;
        }, 0);

        // Calculate Principal Breakdown
        let hCount = 0;
        let eCount = 0;
        newData.principals.forEach(p => {
          if (p.name && p.name.match(/(LLC|INC|CORP|LTD|GROUP|HOLDINGS|REALTY|MANAGEMENT|TRUST|LP|PARTNERSHIP)/i)) {
            eCount++;
          } else {
            hCount++;
          }
        });

        setStats({
          totalProperties: newData.properties.length,
          totalValue: totalVal,
          totalAppraised: totalApp,
          humanCount: hCount,
          entityCount: eCount
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

  // Compute cities list for the filter, derived from ALL properties (unfiltered)
  const allCities = React.useMemo(() => {
    const set = new Set(networkData.properties.map(p => p.city).filter(Boolean));
    return ['All', ...Array.from(set).sort()];
  }, [networkData.properties]);

  const [selectedProperty, setSelectedProperty] = useState(null);
  const [selectedDetailEntity, setSelectedDetailEntity] = useState(null);



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
                        <SearchBar
                          onSearch={handleSearch}
                          isLoading={loading}
                          onSelect={(item) => {
                            let type = 'owner';
                            let id = item.value;

                            if (item.type === 'Business') {
                              type = 'business';
                              id = item.id;
                            } else if (item.type === 'Business Principal') {
                              type = 'principal';
                            }
                            // Property Owner/Co-Owner fallback to 'owner' and use name (item.value)

                            console.log("Direct load:", id, type);
                            loadNetwork(id, type);
                          }}
                        />
                      </div>
                    </div>

                    <div className="mt-8 flex items-center justify-center animate-fade-in-up delay-75">
                      <div className="bg-white/80 backdrop-blur-sm border border-slate-200 shadow-sm rounded-full px-5 py-2 flex items-center gap-3">
                        <div className="w-2.5 h-2.5 rounded-full bg-blue-500 animate-pulse"></div>
                        <p className="text-slate-600 text-sm font-medium">
                          <span className="text-slate-900 font-bold">New here?</span> Click on a network below or search to begin discovery.
                        </p>
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
            className="flex flex-col h-full w-full max-w-[1920px] mx-auto px-4 py-3 gap-3 overflow-y-auto bg-slate-50/50 backdrop-blur-sm"
          >
            {/* Stats Row */}
            <div className="flex flex-col md:flex-row gap-3 items-stretch">
              <div className="grid grid-cols-2 lg:grid-cols-4 gap-3 flex-1">
                <StatCard label="Properties" value={stats.totalProperties} icon={<Building2 className="w-4 h-4 text-slate-400" />} />
                <StatCard
                  label="Portfolio Value"
                  value={`$${(stats.totalValue / 1000000).toFixed(1)}M`}
                  sub={`Appraised: $${(stats.totalAppraised / 1000000).toFixed(1)}M`}
                  highlight
                  icon={<TrendingUp className="w-4 h-4 text-blue-200" />}
                />
                <StatCard label="Businesses" value={networkData.businesses.length} icon={<Building2 className="w-4 h-4 text-slate-400" />} />
                <StatCard
                  label="Principals"
                  value={stats.humanCount + stats.entityCount}
                  sub={`${stats.humanCount} Human / ${stats.entityCount} Entity`}
                  icon={<Users className="w-4 h-4 text-slate-400" />}
                />
              </div>
              {/* Stats Row End */}
            </div>

            {/* Cross-Filtering & City Selection Controls - MOVED TO PROPERTY TABLE */}



            {/* --- MOBILE TAB LAYOUT (lg:hidden) --- */}
            <div className="flex flex-col lg:hidden relative border border-slate-200 rounded-xl bg-white shadow-sm mt-2">

              {/* Sticky Tab Header */}
              <div className="flex items-center border-b border-gray-100 bg-white/95 backdrop-blur-md sticky top-0 z-30 shadow-sm">
                <button
                  onClick={() => handleTabChange('properties')}
                  className={`flex-1 py-3 text-[10px] font-bold uppercase tracking-wider transition-colors border-b-2 ${activeMobileTab === 'properties' ? 'border-blue-500 text-blue-700 bg-blue-50/50' : 'border-transparent text-gray-500 hover:text-gray-700'}`}
                >
                  Properties
                </button>
                <button
                  onClick={() => handleTabChange('businesses')}
                  className={`flex-1 py-3 text-[10px] font-bold uppercase tracking-wider transition-colors border-b-2 ${activeMobileTab === 'businesses' ? 'border-emerald-500 text-emerald-700 bg-emerald-50/50' : 'border-transparent text-gray-500 hover:text-gray-700'}`}
                >
                  Businesses
                </button>
                <button
                  onClick={() => handleTabChange('principals')}
                  className={`flex-1 py-3 text-[10px] font-bold uppercase tracking-wider transition-colors border-b-2 ${activeMobileTab === 'principals' ? 'border-indigo-500 text-indigo-700 bg-indigo-50/50' : 'border-transparent text-gray-500 hover:text-gray-700'}`}
                >
                  Principals
                </button>

                {/* Start Over Button */}
                <button
                  onClick={handleReset}
                  className="px-3 py-3 text-gray-400 hover:text-red-500 hover:bg-red-50 border-b-2 border-transparent transition-colors"
                  title="Start Over"
                  aria-label="Start Over"
                >
                  <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
                    <path d="M3 12a9 9 0 1 0 9-9 9.75 9.75 0 0 0-6.74 2.74L3 8" />
                    <path d="M3 3v5h5" />
                  </svg>
                </button>
              </div>

              {/* Content Area - Auto Height for Page Scroll */}
              <div className={`bg-white min-h-[500px] transition-opacity duration-200 ${isPending ? 'opacity-50' : 'opacity-100'}`}>
                {activeMobileTab === 'properties' && (
                  <div className="flex flex-col">
                    <PropertyTable
                      properties={filteredProperties}
                      highlightedEntityId={selectedEntityId}
                      onSelectProperty={setSelectedProperty}
                      onMapSelected={setSelectedMapProperties}
                      onAiDigest={aiEnabled ? () => setShowAnalysis(true) : null}
                      forceExpanded={true}
                      autoHeight={true}

                      // Filter Props
                      cities={allCities}
                      selectedCity={selectedCity}
                      onSelectCity={setSelectedCity}
                      onClearEntity={() => setSelectedEntityId(null)}
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
            <div className="hidden lg:grid grid-cols-12 gap-4 flex-1 min-h-0 overflow-auto">
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
              <div className="col-span-8 flex-1 overflow-auto flex flex-col min-h-0">
                <div className="p-4 border-b border-slate-100 bg-slate-50/50 flex justify-between items-center shrink-0">
                  <h3 className="text-xs font-bold uppercase tracking-wider text-slate-500">Property Portfolio</h3>
                  <div className="text-xs font-bold text-slate-400">{filteredProperties.length} Assets</div>
                </div>
                <div className="flex-1 flex flex-col min-h-0 overflow-auto">
                  <PropertyTable
                    properties={filteredProperties}
                    highlightedEntityId={selectedEntityId}
                    onSelectProperty={setSelectedProperty}
                    onMapSelected={setSelectedMapProperties}
                    onAiDigest={aiEnabled ? () => setShowAnalysis(true) : null}

                    // Filter Props
                    cities={allCities}
                    selectedCity={selectedCity}
                    onSelectCity={setSelectedCity}
                    onClearEntity={() => setSelectedEntityId(null)}
                  />
                </div>
              </div>
            </div>
          </motion.div>
        )}

        <Suspense fallback={null}>
          <PropertyDetailsModal
            property={selectedProperty}
            networkData={networkData}
            onViewEntity={(entity, type) => setSelectedDetailEntity({ entity, type })}
            onClose={() => setSelectedProperty(null)}
          />
          <EntityDetailsModal
            entity={selectedDetailEntity?.entity}
            type={selectedDetailEntity?.type}
            networkData={networkData}
            onNavigate={(entity, type) => setSelectedDetailEntity({ entity, type })}
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
          <MultiPropertyMapModal
            properties={selectedMapProperties}
            onClose={() => setSelectedMapProperties(null)}
          />
        </Suspense>
      </main>
    </div >
  );
}

export default App;
