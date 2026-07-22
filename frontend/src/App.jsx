import React, { useState, Suspense, useTransition } from 'react';
import Header from './components/Header';
import SearchBar from './components/SearchBar';
import NetworkView from './components/NetworkView';
import PropertyTable from './components/PropertyTable';
import { api } from './api';
import Insights from './components/Insights';
import NetworkProfileCard from './components/NetworkProfileCard';
import SearchResults from './components/SearchResults';
import LoadingScreen from './components/LoadingScreen';
import ToolboxDashboard from './components/ToolboxDashboard';
import StatCard from './components/StatCard';
import BackgroundGrid from './components/BackgroundGrid';
import { motion, AnimatePresence } from 'framer-motion';
import { Sparkles, Loader2, Search, ArrowRight, Building2, TrendingUp, Users, ShieldAlert, ChevronRight, List } from 'lucide-react';
import LandlordMonitor from './components/LandlordMonitor';
import CityExplorer from './components/CityExplorer';
import DatasetLanding from './components/DatasetLanding';
import { getJurisdictionConfig, isPropertyInActiveJurisdiction } from './utils/jurisdiction';
const BurstDetector = React.lazy(() => import('./components/BurstDetector'));

const PropertyDetailsModal = React.lazy(() => import('./components/PropertyDetailsModal'));
const EntityDetailsModal = React.lazy(() => import('./components/EntityDetailsModal'));
const AboutModal = React.lazy(() => import('./components/AboutModal'));
const MultiPropertyMapModal = React.lazy(() => import('./components/MultiPropertyMapModal'));
const FreshnessModal = React.lazy(() => import('./components/FreshnessModal'));
const FeedbackModal = React.lazy(() => import('./components/FeedbackModal'));

const CITY_EXPLORER_STATES = new Set(['NY', 'DC', 'BALTIMORE', 'BOSTON', 'DETROIT', 'PHILADELPHIA', 'CHICAGO', 'MIAMI', 'MINNEAPOLIS', 'NJ']);
const DATASET_STORAGE_KEY = 'theyownwhat.dataset';
const DATASET_PATHS = {
  CT: '/ct',
  NY: '/nyc',
  DC: '/dc',
  BALTIMORE: '/baltimore',
  BOSTON: '/boston',
  DETROIT: '/detroit',
  PHILADELPHIA: '/philadelphia',
  CHICAGO: '/chicago',
  MIAMI: '/miami',
  MINNEAPOLIS: '/minneapolis',
  NJ: '/nj',
};
const PATH_DATASETS = Object.fromEntries(Object.entries(DATASET_PATHS).map(([state, path]) => [path, state]));

const SEO_BY_DATASET = {
  LANDING: {
    title: 'They Own WHAT?? | Landlord & Property Explorer',
    description: 'Choose a source-backed landlord and property dataset to explore ownership networks, property portfolios, code records, subsidies, and public ownership links.',
    path: '/',
  },
  CT: {
    title: 'They Own WHAT?? | Connecticut Landlord & Property Explorer',
    description: 'Explore Connecticut landlord networks, municipal property records, code records, subsidies, and public ownership links from source-loaded public data.',
    path: '/ct',
  },
  NY: {
    title: 'They Own WHAT?? | NYC Landlord & Property Explorer',
    description: 'Explore New York City landlord and property records using source-loaded HPD registration, property, housing, and subsidy data.',
    path: '/nyc',
  },
  DC: {
    title: 'They Own WHAT?? | Washington, D.C. Property Explorer',
    description: 'Explore Washington, D.C. property assessment records and owner networks from source-loaded public data.',
    path: '/dc',
  },
  BALTIMORE: {
    title: 'They Own WHAT?? | Baltimore Landlord & Property Explorer',
    description: 'Explore Baltimore property ownership records, housing/code layers, and source-backed public property data.',
    path: '/baltimore',
  },
  BOSTON: {
    title: 'They Own WHAT?? | Boston Landlord & Property Explorer',
    description: 'Explore Boston property assessment records, ownership networks, and public violation-source enrichment.',
    path: '/boston',
  },
  DETROIT: {
    title: 'They Own WHAT?? | Detroit Landlord & Property Explorer',
    description: 'Explore Detroit property records, ownership networks, and municipal assessment database.',
    path: '/detroit',
  },
  PHILADELPHIA: {
    title: 'They Own WHAT?? | Philadelphia Landlord & Property Explorer',
    description: 'Explore Philadelphia property records, OPA ownership networks, and municipal assessment database.',
    path: '/philadelphia',
  },
  CHICAGO: {
    title: 'They Own WHAT?? | Chicago Landlord & Property Explorer',
    description: 'Explore Chicago property records, owner networks, and municipal assessment database.',
    path: '/chicago',
  },
  MIAMI: {
    title: 'They Own WHAT?? | Miami Landlord & Property Explorer',
    description: 'Explore Miami property records, owner networks, and Florida business registration data.',
    path: '/miami',
  },
  MINNEAPOLIS: {
    title: 'They Own WHAT?? | Minneapolis Landlord & Property Explorer',
    description: 'Explore Minneapolis property records, active rental licenses, owner networks, and municipal database.',
    path: '/minneapolis',
  },
  NJ: {
    title: 'They Own WHAT?? | New Jersey BHI Landlord & Property Explorer',
    description: 'Explore New Jersey DCA BHI active-building registration records and conservative owner networks from public source-loaded data.',
    path: '/nj',
  },
};

function datasetFromPath(pathname = window.location.pathname) {
  const normalized = pathname.replace(/\/+$/, '') || '/';
  return PATH_DATASETS[normalized] || null;
}

function datasetHomeView(state) {
  return CITY_EXPLORER_STATES.has(state) ? 'nyc' : 'home';
}

function getStoredDataset() {
  try {
    const stored = window.localStorage.getItem(DATASET_STORAGE_KEY);
    return DATASET_PATHS[stored] ? stored : null;
  } catch {
    return null;
  }
}

function setMetaAttribute(selector, attribute, value) {
  const element = document.querySelector(selector);
  if (element && value) element.setAttribute(attribute, value);
}

function updatePageSeo(config) {
  const seo = config || SEO_BY_DATASET.LANDING;
  const canonicalUrl = `https://theyownwhat.net${seo.path}`;
  document.title = seo.title;
  setMetaAttribute('meta[name="description"]', 'content', seo.description);
  setMetaAttribute('meta[property="og:title"]', 'content', seo.title);
  setMetaAttribute('meta[property="og:description"]', 'content', seo.description);
  setMetaAttribute('meta[property="og:url"]', 'content', canonicalUrl);
  setMetaAttribute('meta[name="twitter:title"]', 'content', seo.title);
  setMetaAttribute('meta[name="twitter:description"]', 'content', seo.description);
  setMetaAttribute('link[rel="canonical"]', 'href', canonicalUrl);
}

function NetworkJumpBar({ propertyCount, businessCount, principalCount, activeTarget, onJump }) {
  const items = [
    { id: 'properties', label: 'Properties', count: propertyCount, icon: Building2 },
    { id: 'businesses', label: 'Businesses', count: businessCount, icon: List },
    { id: 'principals', label: 'Principals', count: principalCount, icon: Users },
  ];

  return (
    <div className="sticky top-0 z-30 rounded-xl border border-slate-200 bg-white/95 shadow-sm backdrop-blur">
      <div className="flex items-center gap-1 overflow-x-auto px-2 py-1.5">
        <span className="hidden shrink-0 px-2 text-[9px] font-black uppercase tracking-wider text-slate-400 sm:inline">
          Jump to
        </span>
        {items.map(({ id, label, count, icon: Icon }) => {
          const isActive = activeTarget === id;
          return (
            <button
              key={id}
              type="button"
              onClick={() => onJump(id)}
              className={`flex shrink-0 items-center gap-1.5 rounded-lg border px-2.5 py-1.5 text-[10px] font-black uppercase tracking-wider transition-colors ${
                isActive
                  ? 'border-blue-200 bg-blue-50 text-blue-700'
                  : 'border-slate-200 bg-white text-slate-600 hover:bg-slate-50 hover:text-slate-900'
              }`}
            >
              <Icon size={13} />
              <span>{label}</span>
              <span className={`rounded-md px-1.5 py-px text-[9px] ${isActive ? 'bg-blue-100 text-blue-700' : 'bg-slate-100 text-slate-500'}`}>
                {(count || 0).toLocaleString()}
              </span>
            </button>
          );
        })}
      </div>
    </div>
  );
}

// NOTE: This is a simplified App.jsx. In a real scenario we'd use React Router.
function App() {
  const initialDataset = (() => {
    const raw = datasetFromPath();
    return raw;
  })();
  const initialStoredDataset = (() => {
    const stored = getStoredDataset();
    return stored;
  })();
  const [view, setView] = useState(initialDataset ? datasetHomeView(initialDataset) : 'datasets'); // datasets | home | dashboard | toolbox | hartford | nyc
  const [activeState, setActiveState] = useState(initialDataset || initialStoredDataset || 'CT'); // CT | NY | DC | BALTIMORE | BOSTON
  const [lastDataset, setLastDataset] = useState(initialStoredDataset);
  const [cityExplorerKey, setCityExplorerKey] = useState(0);
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
  const [includeNonLocalProperties, setIncludeNonLocalProperties] = useState(false);
  const [selectedEntityId, setSelectedEntityId] = useState(null);
  const [showAbout, setShowAbout] = useState(false);
  const [showFreshness, setShowFreshness] = useState(false);
  const [showFeedback, setShowFeedback] = useState(false);
  const [feedbackEntity, setFeedbackEntity] = useState(null);

  const handleOpenFeedback = (entity) => {
    setFeedbackEntity(entity);
    setShowFeedback(true);
  };

  // Mobile Tabs
  const [activeMobileTab, setActiveMobileTab] = useState('properties');
  const [isPending, startTransition] = useTransition();

  const handleTabChange = (tab) => {
    startTransition(() => {
      setActiveMobileTab(tab);
    });
  };
  const [aiEnabled, setAiEnabled] = useState(false);
  const [toolboxEnabled, setToolboxEnabled] = useState(false);
  const [evictionToolsEnabled, setEvictionToolsEnabled] = useState(false);

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
        if (data && data.toolbox_enabled !== undefined) {
          setToolboxEnabled(data.toolbox_enabled);
        }
      })
      .catch(err => console.warn("Health check failed", err));

    api.get('/features')
      .then(data => {
        if (data && data.eviction_tools_enabled !== undefined) {
          setEvictionToolsEnabled(data.eviction_tools_enabled);
        }
      })
      .catch(err => console.warn("Features check failed", err));

    setLoadingInsights(true);
    api.get('/insights')
      .then(data => {
        console.log("Insights loaded:", data);
        setInsights(data);
      })
      .catch(err => console.error("Failed to load insights", err))
      .finally(() => setLoadingInsights(false));

    // Listen for Hartford Playground trigger from Insights
    const handleOpenPlayground = () => setView('hartford');
    window.addEventListener('open-playground', handleOpenPlayground);

    return () => {
      window.removeEventListener('open-playground', handleOpenPlayground);
    };
  }, []);

  // Search Handler
  const handleSearch = async (type, term) => {
    setLoading(true);
    setSearchResults(null); // Clear previous results
    try {
      // Use 'all' type for unified search
      const results = await api.get(`/search?type=all&term=${encodeURIComponent(term)}&state=${activeState}`);
      console.log("Search results:", results);

      if (results && results.length > 0) {
        setSearchResults(results);
      } else {
        console.warn("No results found for", term);
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
  const loadNetwork = async (id, type, name, city) => {
    setLoading(true);
    setStreamingStatus({ entities: 0, properties: 0, active: true });
    setSelectedCity('All');
    setSelectedEntityId(null);
    setIncludeNonLocalProperties(false);

    if (city) {
      const cityUpper = city.toUpperCase().trim();
      const stateMap = {
        'NYC': 'NY',
        'BALTIMORE': 'BALTIMORE',
        'BOSTON': 'BOSTON',
        'DETROIT': 'DETROIT',
        'DC': 'DC',
        'NJ': 'NJ',
        'NEW JERSEY': 'NJ',
      };
      if (stateMap[cityUpper]) {
        setActiveState(stateMap[cityUpper]);
      } else {
        setActiveState('CT');
      }
    }

    const newData = { principals: [], businesses: [], properties: [], links: [], initialEntityName: name };
    const seenEntities = new Set();

    const streamCity = city || (activeState === 'NY' ? 'NYC' : activeState === 'Balt' ? 'BALTIMORE' : activeState ? activeState.toUpperCase() : 'HARTFORD');

    api.streamNetwork(id, type, name,
      (chunk) => {
        if (chunk.type === 'network_info') {
          // Store backend-provided network name and stats
          if (chunk.data.name) newData.networkName = chunk.data.name;
          if (chunk.data.building_count) newData.building_count = chunk.data.building_count;
          if (chunk.data.unit_count) newData.unit_count = chunk.data.unit_count;
          if (chunk.data.eviction_summary) newData.evictionSummary = chunk.data.eviction_summary;
          if (chunk.data.code_enforcement_summary) newData.codeEnforcementSummary = chunk.data.code_enforcement_summary;
          if (chunk.data.connection_signals) newData.connection_signals = chunk.data.connection_signals;
          if (chunk.data.transaction_summary) newData.transactionSummary = chunk.data.transaction_summary;
        } else if (chunk.type === 'entities') {
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
                  e.isEntity = e.name && e.name.match(/(LLC|INC|CORP|LTD|GROUP|HOLDINGS|REALTY|MANAGEMENT|TRUST|LP|PARTNERSHIP|FUND|INVESTMENT)/i);
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
              if (prop.is_complex && Array.isArray(prop.units)) {
                // Flatten backend complexes back into units for the PropertyTable
                // which handles its own grouping logic
                for (const unit of prop.units) {
                  newData.properties.push(unit);
                }
              } else {
                newData.properties.push(prop);
              }
            }
          }
        }
      },
      () => {
        // On Complete
        setNetworkData(newData);

        // on Complete calc stats
        const totalVal = newData.properties.reduce((acc, p) => {
          if (p.is_network_member === false) return acc; // Only sum value of owned properties
          const val = parseFloat(String(p.assessed_value || '0').replace(/[^0-9.]/g, ''));
          return acc + val;
        }, 0);

        const totalApp = newData.properties.reduce((acc, p) => {
          if (p.is_network_member === false) return acc;
          const val = parseFloat(String(p.appraised_value || '0').replace(/[^0-9.]/g, ''));
          return acc + val;
        }, 0);

        const ownedCount = newData.properties.filter(p => p.is_network_member !== false).length;

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
          totalProperties: newData.properties.length, // All fetched properties (including neighbors)
          ownedProperties: ownedCount, // Only explicit network members
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
      },
      streamCity
    );
  };

  const resetNetworkView = () => {
    setNetworkData({ principals: [], businesses: [], properties: [], links: [] });
    setSearchResults(null);
    setSelectedCity('All');
    setSelectedEntityId(null);
    setIncludeNonLocalProperties(false);
    setSelectedMapProperties(null);
  };

  const setDatasetPath = (state, replace = false) => {
    const nextPath = DATASET_PATHS[state] || '/';
    if (window.location.pathname === nextPath) return;
    const method = replace ? 'replaceState' : 'pushState';
    window.history[method]({ dataset: state }, '', nextPath);
  };

  const rememberDataset = (state) => {
    setLastDataset(state);
    try {
      window.localStorage.setItem(DATASET_STORAGE_KEY, state);
    } catch {
      // Ignore storage failures; dataset selection still works for this session.
    }
  };

  const openDataset = (state, { replace = false } = {}) => {
    if (!DATASET_PATHS[state]) return;
    resetNetworkView();
    setActiveState(state);
    setIncludeNonLocalProperties(false);
    setSelectedCity('All');
    setSelectedEntityId(null);
    rememberDataset(state);
    setDatasetPath(state, replace);
    if (CITY_EXPLORER_STATES.has(state)) {
      setCityExplorerKey(key => key + 1);
    }
    setView(datasetHomeView(state));
  };

  const showDatasetPicker = ({ replace = false } = {}) => {
    resetNetworkView();
    const method = replace ? 'replaceState' : 'pushState';
    if (window.location.pathname !== '/') {
      window.history[method]({ dataset: null }, '', '/');
    }
    setView('datasets');
  };

  const handleReset = () => {
    resetNetworkView();
    setView(datasetHomeView(activeState));
  };

  const handleHeaderHome = () => {
    if (view === 'datasets') {
      showDatasetPicker({ replace: true });
      return;
    }
    resetNetworkView();
    if (CITY_EXPLORER_STATES.has(activeState)) {
      setCityExplorerKey(key => key + 1);
      setView('nyc');
      return;
    }
    setView('home');
  };

  React.useEffect(() => {
    const handlePopState = () => {
      const routeDataset = datasetFromPath();
      resetNetworkView();
      if (routeDataset) {
        setActiveState(routeDataset);
        rememberDataset(routeDataset);
        if (CITY_EXPLORER_STATES.has(routeDataset)) {
          setCityExplorerKey(key => key + 1);
        }
        setView(datasetHomeView(routeDataset));
      } else {
        setView('datasets');
      }
    };
    window.addEventListener('popstate', handlePopState);
    return () => window.removeEventListener('popstate', handlePopState);
  }, []);

  React.useEffect(() => {
    updatePageSeo(view === 'datasets' ? SEO_BY_DATASET.LANDING : SEO_BY_DATASET[activeState]);
  }, [activeState, view]);

  const jurisdictionConfig = React.useMemo(() => getJurisdictionConfig(activeState), [activeState]);

  const localPropertyCount = React.useMemo(
    () => networkData.properties.filter(p => isPropertyInActiveJurisdiction(p, activeState)).length,
    [networkData.properties, activeState]
  );

  const nonLocalPropertyCount = Math.max(0, networkData.properties.length - localPropertyCount);

  const scopedProperties = React.useMemo(() => {
    if (includeNonLocalProperties) return networkData.properties;
    return networkData.properties.filter(p => isPropertyInActiveJurisdiction(p, activeState));
  }, [networkData.properties, includeNonLocalProperties, activeState]);

  const scopedStats = React.useMemo(() => {
    const totalVal = scopedProperties.reduce((acc, p) => {
      if (p.is_network_member === false || p.is_in_network === false) return acc;
      const val = parseFloat(String(p.assessed_value || '0').replace(/[^0-9.]/g, ''));
      return acc + val;
    }, 0);
    const totalApp = scopedProperties.reduce((acc, p) => {
      if (p.is_network_member === false || p.is_in_network === false) return acc;
      const val = parseFloat(String(p.appraised_value || '0').replace(/[^0-9.]/g, ''));
      return acc + val;
    }, 0);
    const ownedCount = scopedProperties.filter(p => p.is_network_member !== false && p.is_in_network !== false).length;
    return {
      ...stats,
      totalProperties: scopedProperties.length,
      ownedProperties: ownedCount,
      totalValue: totalVal,
      totalAppraised: totalApp
    };
  }, [scopedProperties, stats]);

  const scopedNetworkData = React.useMemo(() => ({
    ...networkData,
    properties: scopedProperties,
    building_count: includeNonLocalProperties ? networkData.building_count : null,
    unit_count: includeNonLocalProperties ? networkData.unit_count : null
  }), [networkData, scopedProperties, includeNonLocalProperties]);

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

      // Also try name-based matching (backend uses canonicalized names as link keys)
      const matchPrincipal = networkData.principals.find(p => String(p.id) === sId);
      if (matchPrincipal && matchPrincipal.name) {
        // Canonicalize: uppercase, sort parts alphabetically (matches backend canonicalize_person_name)
        const canon = matchPrincipal.name.toUpperCase().trim().replace(/[`"'.]/g, '').replace(/\s+/g, ' ').split(' ').sort().join(' ');
        variants.add(`principal_${canon}`);
        variants.add(canon);
      }
      const business = networkData.businesses.find(b => String(b.id) === sId);
      if (business) {
        variants.add(`business_${business.id}`);
      }

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

    return scopedProperties.filter(p => {
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
  }, [scopedProperties, selectedCity, selectedEntityId, networkData.principals, networkData.businesses, networkData.links]);

  // Compute cities list for the filter, derived from ALL properties (unfiltered)
  // Show the largest 15 municipalities by property count, not alphabetically
  const allCities = React.useMemo(() => {
    const cityCounts = {};
    scopedProperties.forEach(p => {
      if (p.city) {
        cityCounts[p.city] = (cityCounts[p.city] || 0) + 1;
      }
    });
    // Sort cities by count descending, then alphabetically for ties
    const sortedCities = Object.entries(cityCounts)
      .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
      .map(([city]) => city);
    // Take the largest 15
    return ['All', ...sortedCities.slice(0, 15)];
  }, [scopedProperties]);

  React.useEffect(() => {
    if (selectedCity !== 'All' && !allCities.includes(selectedCity)) {
      setSelectedCity('All');
    }
  }, [allCities, selectedCity]);

  const handleNetworkJump = (target) => {
    handleTabChange(target);

    window.requestAnimationFrame(() => {
      const isMobile = window.matchMedia('(max-width: 1023px)').matches;
      const elementId = isMobile
        ? 'mobile-network-panels'
        : target === 'properties'
          ? 'properties-panel'
          : 'entities-panel';
      document.getElementById(elementId)?.scrollIntoView({
        behavior: 'smooth',
        block: 'start'
      });
    });
  };

  const [selectedProperty, setSelectedProperty] = useState(null);
  const [selectedDetailEntity, setSelectedDetailEntity] = useState(null);



  // Maintenance Mode State
  const [maintenanceMode, setMaintenanceMode] = useState(false);

  // Poll System Status
  React.useEffect(() => {
    const checkStatus = () => {
      api.get('/system/status')
        .then(data => {
          if (data && data.maintenance !== undefined) {
            const isDevPort = window.location.port === '6264';
            const isMaintenanceTest = new URLSearchParams(window.location.search).has('testMaintenance');
            setMaintenanceMode(isDevPort && !isMaintenanceTest ? false : data.maintenance);
          }
        })
        .catch(() => {
          // If status check fails, assume ok or ignore
        });
    };

    checkStatus();
    const interval = setInterval(checkStatus, 10000); // Check every 10s
    return () => clearInterval(interval);
  }, []);

  return (
    <div className="h-screen bg-slate-50 flex flex-col overflow-hidden font-sans text-slate-900 selection:bg-blue-100 selection:text-blue-900">
      <Header
        onHome={handleHeaderHome}
        onDatasets={() => showDatasetPicker()}
        onReset={view === 'dashboard' ? handleReset : null}
        onAbout={() => setShowAbout(true)}
        OnOpenToolbox={() => setView('toolbox')}
        toolboxEnabled={toolboxEnabled}
        onShowFreshness={() => setShowFreshness(true)}
        onReportIssue={() => setShowFeedback(true)}
        onHartfordPlayground={() => {
          setActiveState('CT');
          setView('hartford');
        }}
        onBurstDetector={evictionToolsEnabled ? () => setView('burst') : null}
        evictionToolsEnabled={evictionToolsEnabled}
        currentView={view}
        activeState={activeState}
        onStateChange={(state) => openDataset(state)}
      />

      {/* Maintenance Overlay */}
      <AnimatePresence>
        {maintenanceMode && (
          <motion.div
            initial={{ opacity: 0, y: -50 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: -50 }}
            className="absolute top-20 left-0 right-0 z-50 flex justify-center pointer-events-none"
          >
            <div className="bg-amber-50 border border-amber-200 text-amber-800 px-6 py-4 rounded-xl shadow-xl flex items-center gap-4 max-w-lg pointer-events-auto">
              <div className="p-2 bg-amber-100 rounded-full animate-pulse">
                <Loader2 size={24} className="animate-spin text-amber-600" />
              </div>
              <div>
                <h3 className="font-bold text-lg">System Maintenance in Progress</h3>
                <p className="text-sm text-amber-700">Updates and improvements are being applied. Some data may be temporarily unavailable.</p>
              </div>
            </div>
          </motion.div>
        )}
      </AnimatePresence>

      <LoadingScreen
        visible={streamingStatus.active}
        entities={streamingStatus.entities}
        properties={streamingStatus.properties}
      />

      <main className="flex-1 overflow-hidden relative z-10 pb-20 md:pb-0">

        {/* HERO / SEARCH SECTION */}
        <AnimatePresence mode="wait">
          {view === 'datasets' && (
            <motion.div
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              className="h-full w-full"
            >
              <DatasetLanding
                activeDataset={activeState}
                lastDataset={lastDataset}
                onSelect={(state) => openDataset(state)}
                onOpenMonitor={() => {
                  setActiveState('CT');
                  setView('hartford');
                }}
              />
            </motion.div>
          )}

          {view === 'home' && (
            <div className="h-full overflow-y-auto w-full relative">
              <BackgroundGrid />

              <div className="container mx-auto px-4 pt-4 md:pt-6 pb-24 md:pb-16 relative z-10">
                <motion.div
                  initial={{ opacity: 0, y: 20 }}
                  animate={{ opacity: 1, y: 0 }}
                  exit={{ opacity: 0, y: -20 }}
                  transition={{ duration: 0.4, ease: "easeOut" }}
                  className="max-w-7xl mx-auto"
                >
                  {activeState === 'NY' && (
                    <div
                      onClick={() => setView('nyc')}
                      className="mb-6 max-w-4xl mx-auto bg-gradient-to-r from-violet-50 to-indigo-50 border border-violet-200 text-violet-900 px-5 py-4 rounded-2xl flex items-start gap-3 shadow-md cursor-pointer hover:shadow-lg hover:border-violet-300 transition-all group"
                    >
                      <Building2 className="text-violet-500 shrink-0 mt-0.5" size={20} />
                      <div className="text-sm flex-1">
                        <span className="font-extrabold text-violet-800">NYC Landlord Networks — Now Live:</span>
                        <p className="text-xs text-violet-700 mt-1 leading-relaxed">
                          193,000+ HPD registrations · 83,000+ ownership networks · 2.5M residential units tracked.
                          Search any landlord, LLC, or address.
                        </p>
                      </div>
                      <ChevronRight size={16} className="text-violet-400 shrink-0 mt-1 group-hover:translate-x-1 transition-transform" />
                    </div>
                  )}

                  {/* Search + Tools Toolbar */}
                  <div className="mb-6 flex flex-col md:flex-row items-stretch gap-2 max-w-5xl mx-auto">
                    {/* Search Bar */}
                    <div className="flex-[3] relative group min-w-0">
                      <div className="absolute -inset-0.5 bg-gradient-to-r from-blue-500/25 to-indigo-500/25 rounded-xl opacity-40 group-hover:opacity-70 blur-sm transition duration-500"></div>
                      <div className="relative bg-white rounded-xl shadow-lg border border-blue-200/60 h-full">
                        {!maintenanceMode && <SearchBar
                          activeState={activeState}
                          onSearch={handleSearch}
                          isLoading={loading}
                          onSelect={async (item) => {
                            let type = 'owner';
                            let id = item.id || item.value;

                            if (item.type === 'Business' || item.type === 'business') {
                              type = 'business';
                              id = item.id;
                              loadNetwork(id, type, item.label || item.value);
                            } else if (item.type === 'Business Principal' || item.type === 'principal') {
                              type = 'principal';
                              loadNetwork(id, type, item.label || item.value);
                            } else if (item.type === 'Address' || item.type === 'address') {
                              if (item.business_id && item.owner) {
                                loadNetwork(item.business_id, 'business', item.owner);
                              } else if (item.principal_id && item.owner) {
                                loadNetwork(item.principal_id, 'principal', item.owner);
                              } else if (item.owner) {
                                loadNetwork(item.owner, 'owner', item.owner);
                              } else {
                                loadNetwork(item.value, 'address', item.value);
                              }
                            } else {
                              loadNetwork(id, 'owner', item.label || item.value);
                            }
                          }}
                        />}
                      </div>
                    </div>
                    {/* Action Buttons — height matches search */}
                    <button
                      onClick={() => setView('hartford')}
                      className="flex items-center justify-center gap-1.5 px-3 py-2 rounded-xl bg-white border border-slate-200 hover:border-red-300 hover:shadow-lg text-[11px] font-extrabold text-slate-700 hover:text-red-600 transition-all text-center leading-tight"
                    >
                      <ShieldAlert size={14} className="text-red-500 shrink-0" />
                      <span><span className="hidden sm:inline">Landlord</span> Rap<br />Sheets</span>
                      <span className="text-[8px] font-bold bg-red-100 text-red-500 px-1 py-0.5 rounded-full uppercase tracking-wider">Beta</span>
                    </button>
                    {evictionToolsEnabled && (
                      <button
                        onClick={() => setView('burst')}
                        className="flex items-center justify-center gap-1.5 px-2.5 py-2 rounded-xl bg-white border border-slate-200 hover:border-amber-300 hover:shadow-lg text-[11px] font-extrabold text-slate-700 hover:text-amber-600 transition-all text-center leading-tight"
                      >
                        <TrendingUp size={14} className="text-amber-500 shrink-0" />
                        <span><span className="hidden sm:inline">Eviction</span> Surge<br />Detector</span>
                        <span className="text-[8px] font-bold bg-amber-100 text-amber-600 px-1 py-0.5 rounded-full uppercase tracking-wider">Beta</span>
                      </button>
                    )}
                  </div>

                  {/* Results or Insights */}
                  <div className="relative min-h-[400px]">
                    {searchResults ? (
                      <SearchResults
                        results={searchResults}
                        onSelect={(id, type, name) => loadNetwork(id, type, name)}
                      />
                    ) : (
                      <div>
                        {loadingInsights ? (
                          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                            {[1, 2, 3].map(i => (
                              <div key={i} className="h-48 bg-white rounded-2xl border border-slate-100 shadow-sm animate-pulse"></div>
                            ))}
                          </div>
                        ) : !maintenanceMode ? (
                          <Insights
                            activeState={activeState}
                            data={insights}
                            onSelect={(id, type, name) => loadNetwork(id, type, name)}
                            toolboxEnabled={toolboxEnabled}
                          />
                        ) : (
                          <div className="p-12 text-center bg-white/50 rounded-3xl border border-dashed border-slate-200 backdrop-blur-sm">
                            <p className="text-slate-400 font-medium">Top networks are temporarily hidden during system maintenance.</p>
                          </div>
                        )}
                      </div>
                    )}
                  </div>
                </motion.div>
              </div>
            </div>
          )
          }

          {/* DASHBOARD VIEW */}
          {
            view === 'dashboard' && (
              <motion.div
                initial={{ opacity: 0 }}
                animate={{ opacity: 1 }}
                className="flex flex-col h-full w-full max-w-[1920px] mx-auto px-3 md:px-4 py-2 gap-2 overflow-y-auto bg-slate-50/50 backdrop-blur-sm"
              >


                 {/* Compact Dashboard Header Strip */}
                 <div className="flex flex-col gap-2 shrink-0">
                   <NetworkProfileCard 
                     networkData={scopedNetworkData}
                     stats={scopedStats}
                     networkName={networkData.networkName} 
                     initialEntityName={networkData.initialEntityName}
                     onBack={handleReset}
                     onOpenFeedback={handleOpenFeedback}
                     onExport={() => {
                       const csvContent = "data:text/csv;charset=utf-8,"
                         + "Address,City,Owner,Assessed Value,Appraised Value\n"
                         + filteredProperties.map(p => `"${p.address || p.location}","${p.city || p.property_city}","${p.owner}","${p.assessed_value}","${p.appraised_value}"`).join("\n");
                       const encodedUri = encodeURI(csvContent);
                       const link = document.createElement("a");
                       link.setAttribute("href", encodedUri);
                       link.setAttribute("download", "portfolio_export.csv");
                       document.body.appendChild(link);
                       link.click();
                     }}
                     featureNav={
                       <NetworkJumpBar
                         propertyCount={filteredProperties.length}
                         businessCount={networkData.businesses.length}
                         principalCount={networkData.principals.length}
                         activeTarget={activeMobileTab}
                         onJump={handleNetworkJump}
                       />
                     }
                   />
                 </div>



                {/* Cross-Filtering & City Selection Controls - MOVED TO PROPERTY TABLE */}



                {/* --- MOBILE TAB LAYOUT (lg:hidden) --- */}
                <div id="mobile-network-panels" className="flex flex-col lg:hidden relative border border-slate-200 rounded-xl bg-white shadow-sm mt-1 scroll-mt-2">

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
                          activeState={activeState}
                          jurisdictionConfig={jurisdictionConfig}
                          includeNonLocalProperties={includeNonLocalProperties}
                          onIncludeNonLocalPropertiesChange={setIncludeNonLocalProperties}
                          localPropertyCount={localPropertyCount}
                          nonLocalPropertyCount={nonLocalPropertyCount}
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
                          activeState={activeState}
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
                          activeState={activeState}
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
                <div className="hidden lg:grid grid-cols-12 gap-3 flex-1 min-h-[360px] overflow-auto">
                  {/* Left: Network List */}
                  <div id="entities-panel" className="col-span-4 h-full bg-white rounded-xl border border-slate-200 shadow-sm overflow-hidden flex flex-col scroll-mt-2">
                    <div className="flex-1 overflow-hidden">
                      <NetworkView
                        networkData={networkData}
                        activeState={activeState}
                        selectedEntityId={selectedEntityId}
                        onSelectEntity={(id, type) => setSelectedEntityId(id === selectedEntityId ? null : id)}
                        onViewDetails={(entity, type) => setSelectedDetailEntity({ entity, type })}
                      />
                    </div>
                  </div>

                  {/* Right: Property Table */}
                  <div id="properties-panel" className="col-span-8 flex-1 overflow-auto flex flex-col min-h-0 scroll-mt-2">
                    <div className="px-3 py-2 border-b border-slate-100 bg-slate-50/50 flex justify-between items-center shrink-0">

                      <div className="text-xs font-bold text-slate-400">
                        {filteredProperties.length} {jurisdictionConfig.localLabel} Assets
                        {!includeNonLocalProperties && nonLocalPropertyCount > 0 && (
                          <span className="ml-2 text-amber-600">({nonLocalPropertyCount} {jurisdictionConfig.outsideLabel} hidden)</span>
                        )}
                      </div>
                    </div>
                    <div className="flex-1 flex flex-col min-h-0 overflow-auto">
                      <PropertyTable
                        properties={filteredProperties}
                        activeState={activeState}
                        jurisdictionConfig={jurisdictionConfig}
                        includeNonLocalProperties={includeNonLocalProperties}
                        onIncludeNonLocalPropertiesChange={setIncludeNonLocalProperties}
                        localPropertyCount={localPropertyCount}
                        nonLocalPropertyCount={nonLocalPropertyCount}
                        highlightedEntityId={selectedEntityId}
                        onSelectProperty={setSelectedProperty}
                        onMapSelected={setSelectedMapProperties}

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
            )
          }

          {view === 'toolbox' && (
            <motion.div
              initial={{ opacity: 0, x: 20 }}
              animate={{ opacity: 1, x: 0 }}
              className="h-full w-full"
            >
              <ToolboxDashboard onBack={() => setView(datasetHomeView(activeState))} />
            </motion.div>
          )}

          {view === 'burst' && (
            <motion.div
              initial={{ opacity: 0, scale: 0.95 }}
              animate={{ opacity: 1, scale: 1 }}
              className="h-full w-full"
            >
              <React.Suspense fallback={
                <div className="flex items-center justify-center h-full">
                  <Loader2 className="w-8 h-8 animate-spin text-slate-400" />
                </div>
              }>
                <BurstDetector onSelectEntity={(id, type, name, city) => loadNetwork(id, type, name, city)} />
              </React.Suspense>
            </motion.div>
          )}

          {view === 'nyc' && (
            <motion.div
              initial={{ opacity: 0, x: 20 }}
              animate={{ opacity: 1, x: 0 }}
              className="h-full w-full"
            >
              <CityExplorer
                key={`${activeState}-${cityExplorerKey}`}
                city={activeState === 'NY' ? 'nyc' : activeState.toLowerCase()}
                onBack={() => showDatasetPicker()}
                onMapSelected={setSelectedMapProperties}
              />
            </motion.div>
          )}

          {view === 'hartford' && (
            <motion.div
              initial={{ opacity: 0, scale: 0.95 }}
              animate={{ opacity: 1, scale: 1 }}
              className="h-full w-full min-h-0 flex flex-col overflow-y-auto md:overflow-hidden touch-pan-y"
            >
              <LandlordMonitor
                initialCity={
                  activeState === 'NY' ? 'NYC' :
                  activeState === 'Balt' ? 'BALTIMORE' :
                  activeState ? activeState.toUpperCase() : 'HARTFORD'
                }
                onSelectEntity={(id, type, name, city) => loadNetwork(id, type, name, city)}
              />
            </motion.div>
          )}
        </AnimatePresence>

        <Suspense fallback={null}>
          <PropertyDetailsModal
            property={selectedProperty}
            networkData={networkData}
            onViewEntity={(entity, type) => setSelectedDetailEntity({ entity, type })}
            onClose={() => setSelectedProperty(null)}
            onOpenFeedback={handleOpenFeedback}
          />
          <EntityDetailsModal
            entity={selectedDetailEntity?.entity}
            type={selectedDetailEntity?.type}
            networkData={networkData}
            onNavigate={(entity, type) => setSelectedDetailEntity({ entity, type })}
            onViewProperty={setSelectedProperty}
            onClose={() => setSelectedDetailEntity(null)}
            onOpenFeedback={handleOpenFeedback}
          />

          <AboutModal
            isOpen={showAbout}
            onClose={() => setShowAbout(false)}
            onShowFreshness={() => {
              setShowAbout(false);
              setShowFreshness(true);
            }}
            networkData={networkData}
          />
          <FreshnessModal
            isOpen={showFreshness}
            onClose={() => setShowFreshness(false)}
          />
          <FeedbackModal
            isOpen={showFeedback}
            onClose={() => {
              setShowFeedback(false);
              setFeedbackEntity(null);
            }}
            initialEntity={feedbackEntity || (selectedEntityId ? { id: selectedEntityId, title: 'Selected Entity' } : null)}
          />
          <MultiPropertyMapModal
            properties={selectedMapProperties}
            onClose={() => setSelectedMapProperties(null)}
          />
        </Suspense>
      </main >
    </div >
  );
}


export default App;
