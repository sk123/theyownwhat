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
import { Sparkles } from 'lucide-react';


// NOTE: This is a simplified App.jsx. In a real scenario we'd use React Router.
// But for this "Single Page App" feel, conditional rendering works great.

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

  const [loadingInsights, setLoadingInsights] = useState(true);

  // Init Insights
  React.useEffect(() => {
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

    const newData = { principals: [], businesses: [], properties: [], links: [] };
    const seenEntities = new Set();

    api.streamNetwork(id, type,
      (chunk) => {
        if (chunk.type === 'entities') {
          if (chunk.data.entities) {
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

        setView('dashboard');
        setLoading(false);
      },
      (err) => {
        console.error("Stream error", err);
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

  return (
    <div className="h-screen bg-slate-50 flex flex-col overflow-hidden">
      <Header
        onHome={handleReset}
        onReset={view === 'dashboard' ? handleReset : null}
        onAbout={() => setShowAbout(true)}
      />

      <main className="flex-1 overflow-hidden relative">
        {/* HERO / SEARCH SECTION */}
        <AnimatePresence mode="wait">
          {view === 'home' && (
            <div className="h-full overflow-y-auto w-full">
              <div className="container mx-auto px-4 pt-8 pb-20">
                <motion.div
                  initial={{ opacity: 0, y: 20 }}
                  animate={{ opacity: 1, y: 0 }}
                  exit={{ opacity: 0, y: -20 }}
                  className="max-w-4xl mx-auto text-center pt-12 pb-8"
                >
                  <h2 className="text-5xl md:text-7xl font-black text-gray-900 mb-4 tracking-tighter">
                    they own <span className="text-blue-600">WHAT??</span>
                  </h2>
                  <div className="flex items-center justify-center gap-2 mb-12 opacity-50">
                    <p className="text-[11px] text-gray-400 font-bold uppercase tracking-[0.2em]">
                      Click on a network or search to begin
                    </p>
                  </div>

                  <div className="mb-20">
                    <SearchBar onSearch={handleSearch} isLoading={loading} />
                    {searchResults && (
                      <SearchResults
                        results={searchResults}
                        onSelect={(id, type) => loadNetwork(id, type)}
                      />
                    )}
                  </div>

                  {/* Insights */}
                  {!searchResults && (
                    <div className="mt-20 text-left">
                      {loadingInsights ? (
                        <div className="grid grid-cols-1 md:grid-cols-3 gap-6 opacity-50">
                          {[1, 2, 3].map(i => (
                            <div key={i} className="h-32 bg-gray-200 rounded-xl animate-pulse"></div>
                          ))}
                        </div>
                      ) : (
                        <Insights data={insights} onSelect={(id, type) => loadNetwork(id, type)} />
                      )}
                    </div>
                  )}
                </motion.div>
              </div>
            </div>
          )}
        </AnimatePresence>

        {/* DASHBOARD VIEW */}
        {view === 'dashboard' && (
          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            className="flex flex-col h-full w-full max-w-[1920px] mx-auto px-4 py-2 gap-2 overflow-hidden"
          >
            {/* Stats Row */}
            <div className="flex gap-2 mb-2 items-center">
              <div className="grid grid-cols-2 lg:grid-cols-4 gap-2 flex-1">
                <StatCard label="Properties" value={stats.totalProperties} />
                <StatCard label="Total Assessed" value={`$${(stats.totalValue / 1000000).toFixed(1)}M`} highlight />
                <StatCard label="Businesses" value={networkData.businesses.length} />
                <StatCard label="Principals" value={networkData.principals.length} />
              </div>
              <button
                onClick={() => setShowAnalysis(true)}
                className="h-full px-4 bg-gradient-to-br from-indigo-600 to-purple-700 text-white rounded-xl shadow-lg hover:shadow-xl transition-all flex flex-col items-center justify-center min-w-[80px]"
              >
                <Sparkles className="w-5 h-5 mb-1" />
                <span className="text-[10px] font-bold uppercase tracking-wider">AI Digest</span>
              </button>
            </div>

            {/* Cross-Filtering & City Selection Controls */}
            <DashboardControls
              properties={networkData.properties}
              selectedCity={selectedCity}
              onSelectCity={setSelectedCity}
              selectedEntityId={selectedEntityId}
              onClearEntity={() => setSelectedEntityId(null)}
            />

            {/* Main Content Grid - Fixed height container for desktop, auto for mobile */}
            <div className="grid grid-cols-1 lg:grid-cols-12 gap-4 flex-1 min-h-0 overflow-hidden">
              {/* Left: Network List */}
              <div className="lg:col-span-4 h-[500px] lg:h-full max-h-full overflow-hidden">
                <NetworkView
                  networkData={networkData}
                  selectedEntityId={selectedEntityId}
                  onSelectEntity={(id, type) => setSelectedEntityId(id === selectedEntityId ? null : id)}
                  onViewDetails={(entity, type) => setSelectedDetailEntity({ entity, type })}
                />
              </div>

              {/* Right: Property Table */}
              <div className="lg:col-span-8 h-[500px] lg:h-full max-h-full overflow-hidden flex flex-col min-h-0">
                <PropertyTable
                  properties={filteredProperties}
                  highlightedEntityId={selectedEntityId}
                  onSelectProperty={setSelectedProperty}
                />
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
  // Map string icon name to component if needed, or just pass component
  return (
    <div className={`p-3 rounded-xl border flex flex-col justify-center ${highlight ? 'bg-blue-600 text-white border-blue-600 shadow-blue-200 shadow-lg' : 'bg-white border-gray-200 shadow-sm'}`}>
      <div className={`text-xl font-bold ${highlight ? 'text-white' : 'text-gray-900'}`}>{value}</div>
      <div className={`text-[10px] font-semibold uppercase tracking-wider ${highlight ? 'text-blue-100' : 'text-gray-500'}`}>{label}</div>
    </div>
  );
}



function DashboardControls({ properties, selectedCity, onSelectCity, selectedEntityId, onClearEntity }) {
  // Derive unique cities
  const cities = React.useMemo(() => {
    const set = new Set(properties.map(p => p.city).filter(Boolean));
    return ['All', ...Array.from(set).sort()];
  }, [properties]);

  if (properties.length === 0) return null;

  return (
    <div className="flex flex-col sm:flex-row gap-4 items-start sm:items-center justify-between bg-white p-4 rounded-xl border border-gray-200 shadow-sm animate-in fade-in slide-in-from-top-2 w-full">
      <div className="flex items-center gap-2 w-full sm:w-auto">
        <span className="text-xs font-bold text-gray-500 uppercase mr-2 whitespace-nowrap">Municipality:</span>

        {/* Mobile Dropdown */}
        <div className="sm:hidden w-full">
          <select
            value={selectedCity}
            onChange={(e) => onSelectCity(e.target.value)}
            className="w-full bg-gray-50 border border-gray-200 text-gray-900 text-sm rounded-lg focus:ring-blue-500 focus:border-blue-500 block p-2.5"
          >
            {cities.map(city => (
              <option key={city} value={city}>{city}</option>
            ))}
          </select>
        </div>

        {/* Desktop Buttons */}
        <div className="hidden sm:flex flex-wrap gap-2">
          {cities.map(city => (
            <button
              key={city}
              onClick={() => onSelectCity(city)}
              className={`px-3 py-1.5 rounded-lg text-xs font-bold whitespace-nowrap transition-all ${selectedCity === city
                ? 'bg-blue-600 text-white shadow-md'
                : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
                }`}
            >
              {city}
            </button>
          ))}
        </div>

        {selectedEntityId && (
          <button
            onClick={onClearEntity}
            className="px-3 py-1.5 bg-amber-50 text-amber-700 text-xs font-bold rounded-lg border border-amber-200 hover:bg-amber-100 transition-colors flex items-center gap-2"
          >
            <span>Filter Active: Entity Selected</span>
            <span className="bg-amber-200 px-1.5 rounded-md text-[10px]">✕</span>
          </button>
        )}
      </div>
    </div>
  );
}

export default App;
