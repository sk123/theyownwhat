import React, { useState, Suspense, useTransition, useRef, useCallback, useEffect } from 'react';
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
import { Sparkles, Loader2, Search, ArrowRight, ArrowUp, ArrowDown, Building2, TrendingUp, Users, ShieldAlert, ChevronRight, List, CircleAlert, X } from 'lucide-react';
import LandlordMonitor from './components/LandlordMonitor';
import CityExplorer from './components/CityExplorer';
import DatasetLanding from './components/DatasetLanding';
import AlgorithmicEstimateBanner from './components/AlgorithmicEstimateBanner';
import { MobileBelowFoldCue, NotablePortfoliosSkeleton } from './components/NotablePortfoliosCue';
import { stripReviewPrefix } from './context/ReviewerModeContext';
import { getCtPropertyUnitCount, getJurisdictionConfig, isPropertyInActiveJurisdiction } from './utils/jurisdiction';
const BurstDetector = React.lazy(() => import('./components/BurstDetector'));

const PropertyDetailsModal = React.lazy(() => import('./components/PropertyDetailsModal'));
const EntityDetailsModal = React.lazy(() => import('./components/EntityDetailsModal'));
const AboutModal = React.lazy(() => import('./components/AboutModal'));
const UpgradeModal = React.lazy(() => import('./components/UpgradeModal'));
const MultiPropertyMapModal = React.lazy(() => import('./components/MultiPropertyMapModal'));
const FreshnessModal = React.lazy(() => import('./components/FreshnessModal'));
const FeedbackModal = React.lazy(() => import('./components/FeedbackModal'));
const LocalAnalyticsDashboard = React.lazy(() => {
  const modules = import.meta.glob('./components/LocalAnalyticsDashboard.jsx');
  const loader = modules['./components/LocalAnalyticsDashboard.jsx'];
  if (loader) return loader();
  return Promise.resolve({
    default: ({ onBack }) => (
      <div className="min-h-screen bg-slate-900 text-white flex items-center justify-center p-8">
        <div className="text-center max-w-md bg-slate-800 p-6 rounded-xl shadow-xl border border-slate-700">
          <h2 className="text-xl font-bold mb-2">Local Analytics Feature</h2>
          <p className="text-slate-400 text-sm mb-4">The Local Analytics Dashboard is configured for local administrative environments.</p>
          {onBack && (
            <button onClick={onBack} className="px-4 py-2 bg-blue-600 hover:bg-blue-500 rounded-lg text-sm font-medium transition-colors">
              Return to Explorer
            </button>
          )}
        </div>
      </div>
    )
  });
});

const CITY_EXPLORER_STATES = new Set([
  'NY', 'NYS', 'DC', 'BALTIMORE', 'BOSTON', 'DETROIT', 'PHILADELPHIA',
  'CHICAGO', 'MIAMI', 'MINNEAPOLIS', 'NJ',
  'MA', 'FL', 'IL', 'CA', 'MN', 'MT', 'OH',
  'CO', 'VT', 'WI', 'NC', 'AR', 'VA',
]);
const DATASET_STORAGE_KEY = 'theyownwhat.dataset';
const DATASET_PATHS = {
  CT: '/ct',
  NY: '/ny',
  NYS: '/ny',
  DC: '/dc',
  BALTIMORE: '/baltimore',
  DETROIT: '/detroit',
  PHILADELPHIA: '/philadelphia',
  CHICAGO: '/il',
  IL: '/il',
  MIAMI: '/fl',
  FL: '/fl',
  MINNEAPOLIS: '/mn',
  MN: '/mn',
  NJ: '/nj',
  MA: '/ma',
  BOSTON: '/ma',
  CA: '/ca',
  MT: '/mt',
  OH: '/oh',
  CO: '/co',
  VT: '/vt',
  WI: '/wi',
  NC: '/nc',
  AR: '/ar',
  VA: '/va',
};
const PATH_DATASETS = {
  ...Object.fromEntries(Object.entries(DATASET_PATHS).map(([state, path]) => [path, state])),
  '/ma': 'MA',
  '/boston': 'MA',
  '/fl': 'FL',
  '/miami': 'FL',
  '/ny': 'NY',
  '/nyc': 'NY',
  '/il': 'IL',
  '/chicago': 'IL',
  '/mn': 'MN',
  '/minneapolis': 'MN',
  '/ca': 'CA',
  '/mt': 'MT',
  '/oh': 'OH',
};

const SEO_BY_DATASET = {
  LANDING: {
    title: 'They Own WHAT?? | Property Owner & Network Explorer',
    description: 'Choose a source-backed property ownership dataset to explore owner networks, property portfolios, code records, subsidies, and public corporate links.',
    path: '/',
  },
  CT: {
    title: 'They Own WHAT?? | Connecticut Property Owner Explorer',
    description: 'Explore Connecticut property owner networks, municipal property records, code records, subsidies, and public ownership links from source-loaded public data.',
    path: '/ct',
  },
  NY: {
    title: 'They Own WHAT?? | New York Statewide & NYC Property Owner Explorer',
    description: 'Explore unified New York City (HPD, PLUTO, Class C code violations & Marshal evictions) and New York State county tax parcels, corporate links, and NHPD subsidies.',
    path: '/ny',
  },
  DC: {
    title: 'They Own WHAT?? | Washington, D.C. Property Owner Explorer',
    description: 'Explore Washington, D.C. property assessment records and owner networks from source-loaded public data.',
    path: '/dc',
  },
  BALTIMORE: {
    title: 'They Own WHAT?? | Baltimore Property Owner Explorer',
    description: 'Explore Baltimore property ownership records, housing/code layers, and source-backed public property data.',
    path: '/baltimore',
  },
  BOSTON: {
    title: 'They Own WHAT?? | Massachusetts (incl. Boston) Property Owner Explorer',
    description: 'Explore Massachusetts & Boston property assessment records, owner networks, nominee realty trust links, and Boston ISD code violations.',
    path: '/ma',
  },
  DETROIT: {
    title: 'They Own WHAT?? | Detroit Property Owner Explorer',
    description: 'Explore Detroit property records, ownership networks, and municipal assessment database.',
    path: '/detroit',
  },
  PHILADELPHIA: {
    title: 'They Own WHAT?? | Philadelphia Property Owner Explorer',
    description: 'Explore Philadelphia OPA assessments, L&I active rental licenses, code violations, PA DOS corporate Governor filings, and NHPD subsidies.',
    path: '/philadelphia',
  },
  CHICAGO: {
    title: 'They Own WHAT?? | Illinois & Chicagoland Property Owner Explorer',
    description: 'Explore Illinois property records across Chicago, suburban Cook County, DuPage County, Chicago building violations, and corporate networks.',
    path: '/il',
  },
  IL: {
    title: 'They Own WHAT?? | Illinois Statewide Property Owner Explorer',
    description: 'Explore Illinois property parcels across Chicago, all 130+ Cook County suburbs, DuPage GIS, Chicago building violations, corporate filings, and NHPD subsidies.',
    path: '/il',
  },
  MIAMI: {
    title: 'They Own WHAT?? | Florida & Miami-Dade Property Owner Explorer',
    description: 'Explore unified Florida & Miami-Dade property records across all 67 counties, Sunbiz corporate principals, DBPR apartment licenses, and code enforcement.',
    path: '/fl',
  },
  MINNEAPOLIS: {
    title: 'They Own WHAT?? | Minnesota & Twin Cities Property Owner Explorer',
    description: 'Explore unified Minnesota property records across Hennepin, Ramsey, Dakota, Anoka & Olmsted (Rochester) Counties, Minneapolis active rental licenses, code cases, and corporate filings.',
    path: '/mn',
  },
  MN: {
    title: 'They Own WHAT?? | Minnesota Property Owner Explorer',
    description: 'Explore unified Minnesota property records across Hennepin, Ramsey, Dakota, Anoka & Olmsted (Rochester) Counties, Minneapolis active rental licenses, code cases, and corporate filings.',
    path: '/mn',
  },
  NJ: {
    title: 'They Own WHAT?? | New Jersey BHI Property Owner Explorer',
    description: 'Explore New Jersey DCA BHI active-building registration records and conservative owner networks from public source-loaded data.',
    path: '/nj',
  },
  NYS: {
    title: 'They Own WHAT?? | New York Statewide & NYC Property Owner Explorer',
    description: 'Explore unified New York City and New York State county tax parcels linked to HPD registrations, NY DOS corporate filings, code cases, evictions, and NHPD.',
    path: '/ny',
  },
  MA: {
    title: 'They Own WHAT?? | Massachusetts Statewide Property Owner Explorer',
    description: 'Explore Massachusetts MassGIS Level 3 municipal tax parcels across all 351 cities and towns, unified with Boston Assessing, ISD code cases, owner networks, and NHPD subsidies.',
    path: '/ma',
  },
  FL: {
    title: 'They Own WHAT?? | Florida Statewide Property Owner Explorer',
    description: 'Explore unified Florida property parcels across all 67 counties (including Miami-Dade & Hillsborough CAMA), Sunbiz principals, DBPR Ch. 509 licenses, code enforcement, and NHPD.',
    path: '/fl',
  },
  CA: {
    title: 'They Own WHAT?? | LA & San Francisco Registered-Lessor Explorer (Partial)',
    description: 'Partial California coverage (LA & SF only): business-tax registered real-estate lessors, eviction notices, code enforcement, NHPD subsidies, and CA SOS LLC/corporate officer filings.',
    path: '/ca',
  },
  MT: {
    title: 'They Own WHAT?? | Montana Statewide Property Owner Explorer',
    description: 'Explore Montana MSDI / DOR ORION Cadastral improved property parcels across all 56 counties linked to corporate officer filings and NHPD subsidies.',
    path: '/mt',
  },
  OH: {
    title: 'They Own WHAT?? | Ohio Property Owner Explorer',
    description: 'Explore 1.34M Ohio County Auditor improved CAMA property parcels across Columbus (Franklin), Cleveland (Cuyahoga), Cincinnati (Hamilton) & Toledo (Lucas), recent transfers, corporate filings, and NHPD subsidies.',
    path: '/oh',
  },
  CO: {
    title: 'They Own WHAT?? | Colorado Statewide Property Owner Explorer',
    description: 'Explore Colorado public property parcels linked to Colorado Secretary of State filings, Denver active rental licenses, and NHPD subsidies.',
    path: '/co',
  },
  NC: {
    title: 'They Own WHAT?? | North Carolina Statewide Property Owner Explorer',
    description: 'Explore North Carolina NC OneMap property parcels across all 100 counties, property ownership networks, and NHPD subsidies.',
    path: '/nc',
  },
  WI: {
    title: 'They Own WHAT?? | Wisconsin Statewide Property Owner Explorer',
    description: 'Explore Wisconsin statewide residential and multifamily property parcels, owner networks, and NHPD subsidies.',
    path: '/wi',
  },
  VT: {
    title: 'They Own WHAT?? | Vermont Statewide Property Owner Explorer',
    description: 'Explore Vermont VCGI standardized Grand List property parcels, property ownership networks, and NHPD subsidies.',
    path: '/vt',
  },
  AR: {
    title: 'They Own WHAT?? | Arkansas Statewide Property Owner Explorer',
    description: 'Explore Arkansas GIS Office property parcels across all 75 counties linked to corporate officer filings and NHPD subsidies.',
    path: '/ar',
  },
  VA: {
    title: 'They Own WHAT?? | Virginia Statewide Property Owner Explorer',
    description: 'Explore Virginia VGIN & county CAMA property parcels linked to Virginia SCC corporate officer filings, recent sales, and NHPD subsidies.',
    path: '/va',
  },
};

function datasetFromPath(pathname = window.location.pathname) {
  const normalized = stripReviewPrefix(pathname).replace(/\/+$/, '') || '/';
  return PATH_DATASETS[normalized] || null;
}

function datasetHomeView(state) {
  return CITY_EXPLORER_STATES.has(state) ? 'nyc' : 'home';
}

function getStoredDataset() {
  try {
    const stored = window.localStorage.getItem(DATASET_STORAGE_KEY);
    if (stored === 'BOSTON') return 'MA';
    if (stored === 'MIAMI') return 'FL';
    if (stored === 'NYS') return 'NY';
    if (stored === 'CHICAGO') return 'IL';
    if (stored === 'MINNEAPOLIS') return 'MN';
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
    { id: 'properties', label: 'Properties', count: propertyCount, icon: Building2, primary: true },
    { id: 'businesses', label: 'Businesses', count: businessCount, icon: List, primary: false },
    { id: 'principals', label: 'Principals', count: principalCount, icon: Users, primary: false },
  ];

  return (
    <div role="tablist" aria-label="Network portfolio sections" className="flex items-stretch gap-1.5 w-full py-0.5">
      {items.map(({ id, label, count, icon: Icon, primary }) => {
        const isActive = activeTarget === id;
        return (
          <button
            key={id}
            type="button"
            role="tab"
            aria-selected={isActive}
            onClick={() => onJump(id)}
            className={`${primary ? 'flex-[1.18]' : 'flex-1'} min-w-0 min-h-[38px] flex items-center justify-center gap-1 sm:gap-1.5 rounded-xl py-2 px-2 text-[10px] font-black uppercase tracking-wide transition-all cursor-pointer ${
              isActive
                ? primary
                  ? 'bg-blue-600 text-white shadow-md ring-1 ring-blue-700/30'
                  : 'bg-slate-900 text-white shadow-md'
                : primary
                  ? 'border-2 border-blue-400 bg-white text-blue-950 shadow-sm hover:bg-blue-50'
                  : 'border border-slate-200 bg-white text-slate-600 hover:bg-slate-50 hover:text-slate-900 shadow-2xs'
            }`}
          >
            <Icon
              size={12}
              aria-hidden="true"
              className={`shrink-0 hidden min-[370px]:inline ${
                isActive ? 'text-white' : primary ? 'text-blue-600' : 'text-slate-400'
              }`}
            />
            <span className="truncate">{label}</span>
            <span
              className={`rounded-full px-1.5 py-0.5 text-[9.5px] font-black shrink-0 tabular-nums ${
                isActive
                  ? 'bg-white/25 text-white'
                  : primary
                    ? 'bg-blue-100 text-blue-800'
                    : 'bg-slate-100 text-slate-600'
              }`}
            >
              {(count || 0).toLocaleString()}
            </span>
          </button>
        );
      })}
    </div>
  );
}

class DashboardErrorBoundary extends React.Component {
  constructor(props) {
    super(props);
    this.state = { hasError: false, error: null };
  }
  static getDerivedStateFromError(error) {
    return { hasError: true, error };
  }
  componentDidCatch(error, errorInfo) {
    console.error("DashboardErrorBoundary caught an error:", error, errorInfo);
  }
  render() {
    if (this.state.hasError) {
      return (
        <div className="p-4 bg-red-50 border border-red-200 rounded-xl m-2 text-center shadow-xs">
          <h3 className="text-xs font-bold text-red-800">Something went wrong displaying this section</h3>
          <p className="text-[11px] text-red-600 mt-0.5">{this.state.error?.message || 'Render error'}</p>
          <button
            onClick={() => {
              this.setState({ hasError: false, error: null });
              if (this.props.onReset) this.props.onReset();
            }}
            className="mt-2 px-3 py-1 bg-red-600 hover:bg-red-700 text-white rounded-lg text-[10px] font-bold uppercase tracking-wider"
          >
            Reload Section
          </button>
        </div>
      );
    }
    return this.props.children;
  }
}

// NOTE: This is a simplified App.jsx. In a real scenario we'd use React Router.
function App() {
  const initialAnalytics = typeof window !== 'undefined' && (window.location.pathname.replace(/\/+$/, '') === '/analytics' || new URLSearchParams(window.location.search).has('analytics'));
  const initialDataset = (() => {
    const raw = datasetFromPath();
    return raw;
  })();
  const initialStoredDataset = (() => {
    const stored = getStoredDataset();
    return stored;
  })();
  const [view, setView] = useState(initialAnalytics ? 'analytics' : (initialDataset ? datasetHomeView(initialDataset) : 'datasets')); // datasets | home | dashboard | toolbox | hartford | nyc | analytics
  const [activeState, setActiveState] = useState(initialDataset || initialStoredDataset || 'CT'); // CT | NY | DC | BALTIMORE | BOSTON
  const [liveJurisdictionKeys, setLiveJurisdictionKeys] = useState(null);
  const [monitorCity, setMonitorCity] = useState('HARTFORD');
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

  // Dashboard Scroll & State
  const dashboardScrollRef = useRef(null);
  const ctHomeScrollRef = useRef(null);
  const ctNotableSectionRef = useRef(null);
  const [isScrolledPastCards, setIsScrolledPastCards] = useState(false);
  const [isAtDashboardTop, setIsAtDashboardTop] = useState(true);

  const handleDashboardScroll = useCallback(() => {
    const scroller = dashboardScrollRef.current;
    if (!scroller) return;
    const st = scroller.scrollTop;
    setIsAtDashboardTop(st <= 20);

    const mobilePanelsEl = document.getElementById('mobile-network-panels');
    if (mobilePanelsEl && window.matchMedia('(max-width: 1023px)').matches) {
      const scrollerRect = scroller.getBoundingClientRect();
      const panelsRect = mobilePanelsEl.getBoundingClientRect();
      setIsScrolledPastCards(panelsRect.top - scrollerRect.top <= 160);
    } else {
      setIsScrolledPastCards(st > 250);
    }
  }, []);

  const handleScrollToTop = useCallback(() => {
    if (dashboardScrollRef.current) {
      dashboardScrollRef.current.scrollTo({ top: 0, behavior: 'smooth' });
    }
  }, []);

  // Dashboard State
  const [selectedCity, setSelectedCity] = useState('All');
  const [includeNonLocalProperties, setIncludeNonLocalProperties] = useState(false);
  const [selectedEntityId, setSelectedEntityId] = useState(null);
  const [showAbout, setShowAbout] = useState(false);
  const [showUpgrade, setShowUpgrade] = useState(false);
  const [showFreshness, setShowFreshness] = useState(false);
  const [showFeedback, setShowFeedback] = useState(false);
  const [feedbackEntity, setFeedbackEntity] = useState(null);
  const [searchNotice, setSearchNotice] = useState(null);
  const [cityInitialTarget, setCityInitialTarget] = useState(null);

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

    // Listen for Hartford Playground trigger from Insights
    const handleOpenPlayground = () => setView('hartford');
    window.addEventListener('open-playground', handleOpenPlayground);

    return () => {
      window.removeEventListener('open-playground', handleOpenPlayground);
    };
  }, []);

  // Lazy-load the 2.9MB CT insights table ONLY when entering CT view (view === 'home')
  const insightsRequestedRef = useRef(false);
  React.useEffect(() => {
    if (view !== 'home' || insightsRequestedRef.current) return;
    insightsRequestedRef.current = true;
    setLoadingInsights(true);
    api.get('/insights')
      .then(data => {
        setInsights(data);
      })
      .catch(err => console.error("Failed to load insights", err))
      .finally(() => setLoadingInsights(false));
  }, [view]);

  // Search Handler
  const handleSearch = async (type, term, state) => {
    const targetState = state || activeState || 'CT';
    if (targetState !== activeState) {
      setActiveState(targetState);
    }
    setLoading(true);
    setSearchNotice(null);
    setSearchResults(null); // Clear previous results
    resetNetworkView();
    try {
      // Use 'all' type for unified search
      const searchType = type || 'all';
      const results = await api.get(`/search?type=${encodeURIComponent(searchType)}&term=${encodeURIComponent(term)}&state=${targetState}`);
      console.log("Search results:", results);

      if (results && results.length > 0) {
        setSearchResults(results);
        setDatasetPath(targetState);
        setView('home');
      } else {
        console.warn("No results found for", term);
        setSearchNotice({
          tone: 'info',
          text: `No matching records found for "${term}" in ${targetState}. Try a broader owner surname, entity root, or street name without unit numbers.`,
        });
      }
    } catch (err) {
      console.error(err);
      setSearchNotice({
        tone: 'error',
        text: 'Search request failed. Please try again in a moment.',
      });
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

    let resolvedState = activeState;
    if (city) {
      const cityUpper = city.toUpperCase().trim();
      const stateMap = {
        'NYC': 'NY',
        'NY': 'NY',
        'NEW YORK CITY': 'NY',
        'NYS': 'NYS',
        'NEW YORK STATE': 'NYS',
        'BALTIMORE': 'BALTIMORE',
        'BALT': 'BALTIMORE',
        'MD': 'BALTIMORE',
        'BOSTON': 'MA',
        'MA': 'MA',
        'MASSACHUSETTS': 'MA',
        'DETROIT': 'DETROIT',
        'MI': 'DETROIT',
        'DC': 'DC',
        'PHILADELPHIA': 'PHILADELPHIA',
        'PA': 'PHILADELPHIA',
        'CHICAGO': 'CHICAGO',
        'IL': 'CHICAGO',
        'MIAMI': 'MIAMI',
        'FL': 'FL',
        'FLORIDA': 'FL',
        'MINNEAPOLIS': 'MINNEAPOLIS',
        'MN': 'MINNEAPOLIS',
        'NJ': 'NJ',
        'NEW JERSEY': 'NJ',
        'CO': 'CO',
        'COLORADO': 'CO',
        'VT': 'VT',
        'VERMONT': 'VT',
        'WI': 'WI',
        'WISCONSIN': 'WI',
        'NC': 'NC',
        'NORTH CAROLINA': 'NC',
      };
      resolvedState = stateMap[cityUpper] || 'CT';
      setActiveState(resolvedState);
    }

    const newData = { principals: [], businesses: [], properties: [], links: [], initialEntityName: name };
    const seenEntities = new Set();

    const streamCity = (city && city.toUpperCase().trim() === 'NY')
      ? 'NYC'
      : (city || (resolvedState === 'NY' ? 'NYC' : resolvedState === 'Balt' ? 'BALTIMORE' : resolvedState ? resolvedState.toUpperCase() : 'HARTFORD'));

    api.streamNetwork(id, type, name,
      (chunk) => {
        if (chunk.type === 'network_info') {
          // Store backend-provided network name and stats
          if (chunk.data.name) newData.networkName = chunk.data.name;
          if (chunk.data.building_count) newData.building_count = chunk.data.building_count;
          if (chunk.data.unit_count) newData.unit_count = chunk.data.unit_count;
          if (chunk.data.eviction_summary) newData.evictionSummary = chunk.data.eviction_summary;
          if (chunk.data.code_enforcement_summary) {
            newData.codeEnforcementSummary = chunk.data.code_enforcement_summary;
            newData.codeSummary = chunk.data.code_enforcement_summary;
          }
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
        const totalUnits = newData.properties.reduce((acc, p) => {
          if (p.is_network_member === false || p.is_in_network === false) return acc;
          return acc + getCtPropertyUnitCount(p);
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
          totalProperties: newData.properties.length, // All fetched properties (including neighbors)
          ownedProperties: ownedCount, // Only explicit network members
          totalUnits,
          totalValue: totalVal,
          totalAppraised: totalApp,
          humanCount: hCount,
          entityCount: eCount
        });

        setStreamingStatus(prev => ({ ...prev, active: false }));
        setActiveMobileTab('properties');
        setIsAtDashboardTop(true);
        setIsScrolledPastCards(false);
        setView('dashboard');
        setLoading(false);
        window.requestAnimationFrame(() => {
          if (dashboardScrollRef.current) dashboardScrollRef.current.scrollTop = 0;
        });
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
    const basePath = DATASET_PATHS[state] || '/';
    const inReviewPath = window.location.pathname.toLowerCase() === '/review' || window.location.pathname.toLowerCase().startsWith('/review/');
    const nextPath = inReviewPath ? (basePath === '/' ? '/review' : `/review${basePath}`) : basePath;
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

  const openDataset = (state, { replace = false, initialTarget = null } = {}) => {
    if (!DATASET_PATHS[state]) return;
    resetNetworkView();
    setSearchNotice(null);
    setCityInitialTarget(initialTarget);
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

  const handleSelectCtAutocompleteItem = (item) => {
    if (!item) return;
    setActiveState('CT');
    rememberDataset('CT');
    setDatasetPath('CT');
    setSearchNotice(null);

    const id = item.id || item.value;
    if (item.type === 'Ownership Network' || item.type === 'network') {
      loadNetwork(String(id), 'network', item.label || item.value, 'CT');
    } else if (item.type === 'Business' || item.type === 'business') {
      loadNetwork(item.id, 'business', item.label || item.value, 'CT');
    } else if (item.type === 'Business Principal' || item.type === 'principal') {
      loadNetwork(id, 'principal', item.label || item.value, 'CT');
    } else if (item.type === 'Address' || item.type === 'address') {
      if (item.business_id && item.owner) {
        loadNetwork(item.business_id, 'business', item.owner, 'CT');
      } else if (item.principal_id && item.owner) {
        loadNetwork(item.principal_id, 'principal', item.owner, 'CT');
      } else if (item.owner) {
        loadNetwork(item.owner, 'owner', item.owner, 'CT');
      } else {
        loadNetwork(item.value, 'address', item.value, 'CT');
      }
    } else {
      loadNetwork(id, 'owner', item.label || item.value, 'CT');
    }
  };

  const showDatasetPicker = ({ replace = false } = {}) => {
    resetNetworkView();
    const method = replace ? 'replaceState' : 'pushState';
    const inReviewPath = window.location.pathname.toLowerCase() === '/review' || window.location.pathname.toLowerCase().startsWith('/review/');
    const homePath = inReviewPath ? '/review' : '/';
    if (window.location.pathname !== homePath) {
      window.history[method]({ dataset: null }, '', homePath);
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
    const totalUnits = scopedProperties.reduce((acc, p) => {
      if (p.is_network_member === false || p.is_in_network === false) return acc;
      return acc + getCtPropertyUnitCount(p);
    }, 0);
    return {
      ...stats,
      totalProperties: scopedProperties.length,
      ownedProperties: ownedCount,
      totalUnits,
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
    setActiveMobileTab(target);

    window.requestAnimationFrame(() => {
      window.requestAnimationFrame(() => {
        const isMobile = window.matchMedia('(max-width: 1023px)').matches;
        if (isMobile) {
          document.getElementById('mobile-network-panels')?.scrollIntoView({
            behavior: 'smooth',
            block: 'start'
          });
        } else {
          const elementId = target === 'properties' ? 'properties-panel' : 'entities-panel';
          const el = document.getElementById(elementId);
          if (el && dashboardScrollRef.current) {
            const topOffset = el.offsetTop - 85;
            dashboardScrollRef.current.scrollTo({
              top: Math.max(0, topOffset),
              behavior: 'smooth'
            });
          }
        }
      });
    });
  };

  const [selectedProperty, setSelectedProperty] = useState(null);
  const [selectedDetailEntity, setSelectedDetailEntity] = useState(null);



  // Maintenance Mode State
  const [maintenanceMode, setMaintenanceMode] = useState(false);

  // Poll System Status (visibility-gated so background mobile tabs do not poll)
  React.useEffect(() => {
    const checkStatus = () => {
      if (typeof document !== 'undefined' && document.visibilityState === 'hidden') return;
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
    const interval = setInterval(checkStatus, 30000);
    const onVisibilityChange = () => {
      if (document.visibilityState === 'visible') checkStatus();
    };
    document.addEventListener('visibilitychange', onVisibilityChange);
    return () => {
      clearInterval(interval);
      document.removeEventListener('visibilitychange', onVisibilityChange);
    };
  }, []);

  return (
    <div className="h-dvh bg-slate-50 flex flex-col overflow-hidden font-sans text-slate-900 selection:bg-blue-100 selection:text-blue-900">
      <Header
        onHome={handleHeaderHome}
        onDatasets={() => showDatasetPicker()}
        onReset={view === 'dashboard' ? handleReset : null}
        onAbout={() => setShowAbout(true)}
        onShowUpgrade={() => setShowUpgrade(true)}
        OnOpenToolbox={() => setView('toolbox')}
        toolboxEnabled={toolboxEnabled}
        onShowFreshness={() => setShowFreshness(true)}
        onReportIssue={() => setShowFeedback(true)}
        onHartfordPlayground={() => {
          const defaultMonitorCity = activeState === 'NY' ? 'NYC' : activeState === 'DETROIT' ? 'DETROIT' : 'HARTFORD';
          setMonitorCity(defaultMonitorCity);
          if (activeState !== 'NY' && activeState !== 'DETROIT') {
            setActiveState('CT');
          }
          setView('hartford');
        }}
        onBurstDetector={evictionToolsEnabled ? () => setView('burst') : null}
        evictionToolsEnabled={evictionToolsEnabled}
        currentView={view}
        activeState={activeState}
        liveJurisdictionKeys={liveJurisdictionKeys}
        onStateChange={(state) => openDataset(state)}
        onOpenAnalytics={() => setView('analytics')}
      />

      {/* Non-blocking Search Notice Banner */}
      <AnimatePresence>
        {searchNotice && (
          <motion.div
            initial={{ opacity: 0, y: -16 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: -16 }}
            className="relative z-40 mx-auto mt-2 max-w-2xl px-4 w-full"
          >
            <div className={`flex items-start justify-between gap-3 rounded-xl border px-4 py-2.5 text-xs font-medium shadow-sm ${
              searchNotice.tone === 'error'
                ? 'border-red-200 bg-red-50 text-red-800'
                : 'border-amber-200 bg-amber-50 text-amber-900'
            }`}>
              <div className="flex items-start gap-2">
                <CircleAlert className="h-4 w-4 shrink-0 mt-0.5" aria-hidden="true" />
                <span>{searchNotice.text}</span>
              </div>
              <button
                type="button"
                onClick={() => setSearchNotice(null)}
                aria-label="Dismiss search notice"
                className="p-0.5 rounded hover:bg-black/5 shrink-0"
              >
                <X className="h-3.5 w-3.5" aria-hidden="true" />
              </button>
            </div>
          </motion.div>
        )}
      </AnimatePresence>

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

      <main className="flex-1 min-h-0 overflow-hidden relative">

        {/* HERO / SEARCH SECTION */}
        <AnimatePresence mode="wait">
          {view === 'analytics' && (
            <motion.div
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              className="h-full w-full overflow-hidden"
            >
              <Suspense fallback={<LoadingScreen message="Loading Local Analytics..." />}>
                <LocalAnalyticsDashboard onBack={() => setView(initialDataset ? datasetHomeView(initialDataset) : 'datasets')} />
              </Suspense>
            </motion.div>
          )}

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
                onLiveKeysChange={setLiveJurisdictionKeys}
                onSelect={(state) => openDataset(state)}
                onSearch={handleSearch}
                onSelectCtItem={handleSelectCtAutocompleteItem}
                onOpenCityResult={(state, target) => openDataset(state, { initialTarget: target })}
                onShowUpgrade={() => setShowUpgrade(true)}
                onShowFreshness={() => setShowFreshness(true)}
                onOpenMonitor={(city) => {
                  const targetCity = (city || 'HARTFORD').toUpperCase().trim();
                  setMonitorCity(targetCity === 'NY' ? 'NYC' : targetCity);
                  if (targetCity === 'NYC' || targetCity === 'NY') setActiveState('NY');
                  else if (targetCity === 'DETROIT') setActiveState('DETROIT');
                  else setActiveState('CT');
                  setView('hartford');
                }}
                onOpenEntity={(id, type, name, state) => {
                  if (state) setActiveState(state);
                  loadNetwork(id, type, name, state);
                }}
              />
            </motion.div>
          )}

          {view === 'home' && (
            <div ref={ctHomeScrollRef} className="h-full overflow-y-auto w-full relative">
              <BackgroundGrid />

              <div className="mx-auto w-full max-w-[1720px] px-3 sm:px-5 pt-2.5 md:pt-3 pb-24 md:pb-14 relative z-10">
                <motion.div
                  initial={{ opacity: 0, y: 12 }}
                  animate={{ opacity: 1, y: 0 }}
                  exit={{ opacity: 0, y: -12 }}
                  transition={{ duration: 0.25, ease: "easeOut" }}
                  className="w-full"
                >
                  {/* Compact Search Bar (aligned directly below "they own WHAT??") + Quick Tools */}
                  <div className="mb-3 flex flex-col md:flex-row md:items-center md:justify-between gap-2">
                    <div className="w-full md:max-w-xl lg:max-w-2xl">
                      {!maintenanceMode && (
                        <SearchBar
                          activeState={activeState}
                          onSearch={handleSearch}
                          onSelect={handleSelectCtAutocompleteItem}
                        />
                      )}
                    </div>

                    <div className="flex flex-wrap items-center gap-2 shrink-0">
                      <button
                        type="button"
                        onClick={() => setView('hartford')}
                        className="inline-flex items-center gap-1.5 px-3 py-2 rounded-xl bg-white border border-slate-200 hover:border-red-300 hover:bg-red-50/40 shadow-2xs text-xs font-extrabold text-slate-700 hover:text-red-600 transition-all whitespace-nowrap cursor-pointer"
                      >
                        <ShieldAlert size={14} className="text-red-500 shrink-0" aria-hidden="true" />
                        <span>Owner Rap Sheets</span>
                      </button>
                      {evictionToolsEnabled && (
                        <button
                          type="button"
                          onClick={() => setView('burst')}
                          className="inline-flex items-center gap-1.5 px-3 py-2 rounded-xl bg-white border border-slate-200 hover:border-amber-300 hover:bg-amber-50/40 shadow-2xs text-xs font-extrabold text-slate-700 hover:text-amber-600 transition-all whitespace-nowrap cursor-pointer"
                        >
                          <TrendingUp size={14} className="text-amber-500 shrink-0" aria-hidden="true" />
                          <span>Eviction Surge Detector</span>
                        </button>
                      )}
                    </div>
                  </div>

                  {/* Results or Insights */}
                  <div className="relative min-h-[400px]">
                    {searchResults ? (
                      <SearchResults
                        results={searchResults}
                        onSelect={(id, type, name) => loadNetwork(id, type, name)}
                      />
                    ) : (
                      <>
                        <div ref={ctNotableSectionRef} className="scroll-mt-4">
                          {loadingInsights ? (
                            <NotablePortfoliosSkeleton
                              heading="Notable Connecticut Networks"
                              jurisdictionName="Connecticut"
                              columns={3}
                              cardCount={6}
                            />
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
                        <MobileBelowFoldCue
                          targetRef={ctNotableSectionRef}
                          scrollRootRef={ctHomeScrollRef}
                          loading={loadingInsights}
                          count={Array.isArray(insights?.Statewide) ? insights.Statewide.length : (insights ? 50 : 0)}
                          label="Notable Portfolios"
                          jurisdictionName="Connecticut"
                          enabled={!maintenanceMode && !searchResults}
                        />
                      </>
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
                ref={dashboardScrollRef}
                onScroll={handleDashboardScroll}
                initial={{ opacity: 0 }}
                animate={{ opacity: 1 }}
                className="flex flex-col h-full w-full max-w-[1920px] mx-auto px-2 sm:px-3 md:px-4 pt-2 pb-[calc(5.5rem+env(safe-area-inset-bottom,0px))] md:pb-4 lg:py-2 gap-2 overflow-y-auto overscroll-y-contain bg-slate-50/50"
              >


                 {/* Compact Dashboard Header Strip + Quick Search under "they own WHAT??" */}
                 <div className="flex flex-col gap-2 shrink-0">
                   {!maintenanceMode && (
                     <div className="w-full md:max-w-lg">
                       <SearchBar
                         activeState={activeState}
                         onSearch={handleSearch}
                         onSelect={handleSelectCtAutocompleteItem}
                       />
                     </div>
                   )}
                   <DashboardErrorBoundary onReset={handleReset}>
                     <NetworkProfileCard 
                       networkData={scopedNetworkData}
                       stats={scopedStats}
                       networkName={networkData.networkName} 
                       initialEntityName={networkData.initialEntityName}
                       onBack={handleReset}
                       onOpenFeedback={handleOpenFeedback}
                       onViewProperty={(prop) => setSelectedProperty(prop)}
                       onSelectEntity={(id, type, name, city) => loadNetwork(id, type, name, city)}
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
                   </DashboardErrorBoundary>
                   <AlgorithmicEstimateBanner />
                 </div>

                {/* Sticky Jump-Back Bar when cards have scrolled off-screen */}
                <AnimatePresence>
                  {isScrolledPastCards && (
                    <motion.div
                      initial={{ opacity: 0, y: -15 }}
                      animate={{ opacity: 1, y: 0 }}
                      exit={{ opacity: 0, y: -15 }}
                      transition={{ duration: 0.15 }}
                      className="sticky top-0 z-40 bg-white/95 backdrop-blur-md border border-slate-200 rounded-xl px-3.5 py-2 shadow-md flex items-center justify-between gap-3 shrink-0 mb-1"
                    >
                      <div className="flex items-center gap-2 min-w-0">
                        <span className="font-black text-xs md:text-sm text-slate-900 truncate">
                          {networkData?.networkName || 'Owner Portfolio'}
                        </span>
                        <span className="text-[10px] text-blue-700 bg-blue-50 border border-blue-200 font-extrabold px-2 py-0.5 rounded-full shrink-0">
                          {(filteredProperties || []).length} Properties
                        </span>
                      </div>
                      <button
                        onClick={handleScrollToTop}
                        className="flex items-center gap-1.5 text-xs font-black uppercase tracking-wider px-3.5 py-1.5 rounded-lg bg-blue-600 hover:bg-blue-700 text-white shadow-xs transition-all cursor-pointer shrink-0 active:scale-95"
                        title="Jump back up to summary cards at top of page"
                      >
                        <ArrowUp size={13} className="shrink-0" />
                        <span>Jump to Cards</span>
                      </button>
                    </motion.div>
                  )}
                </AnimatePresence>

                {/* --- MOBILE TAB LAYOUT (lg:hidden) --- */}
                <div id="mobile-network-panels" className="flex flex-col lg:hidden relative mt-1 scroll-mt-2 min-h-[calc(100dvh-80px)] shrink-0">
                  <div className="mb-2 rounded-2xl bg-slate-200/80 border border-slate-300/80 p-1.5 shadow-xs">
                    <NetworkJumpBar
                      propertyCount={filteredProperties.length}
                      businessCount={networkData.businesses.length}
                      principalCount={networkData.principals.length}
                      activeTarget={activeMobileTab}
                      onJump={(tab) => handleTabChange(tab)}
                    />
                  </div>

                  {activeMobileTab !== 'properties' && filteredProperties.length > 0 && (
                    <button
                      type="button"
                      onClick={() => handleTabChange('properties')}
                      className="mb-2 w-full flex items-center justify-between gap-2 rounded-xl border border-blue-200 bg-blue-50/90 hover:bg-blue-100 px-3.5 py-2.5 text-left transition-colors cursor-pointer shadow-2xs"
                    >
                      <span className="flex items-center gap-2 min-w-0 text-xs font-bold text-blue-950">
                        <Building2 size={15} className="text-blue-600 shrink-0" aria-hidden="true" />
                        <span>
                          View all <strong>{filteredProperties.length.toLocaleString()} properties &amp; addresses</strong>
                        </span>
                      </span>
                      <ChevronRight size={15} className="text-blue-600 shrink-0" aria-hidden="true" />
                    </button>
                  )}

                  {/* Content Area - Auto Height for Page Scroll */}
                  <div className={`border border-slate-200 rounded-xl bg-white shadow-sm overflow-hidden min-h-[500px] transition-opacity duration-200 ${isPending ? 'opacity-50' : 'opacity-100'}`}>
                    {activeMobileTab === 'properties' && (
                      <div className="flex flex-col">
                        <PropertyTable
                          properties={filteredProperties}
                          activeState={activeState}
                          networkName={networkData.networkName}
                          networkId={String(networkData.network_id || networkData.id || '')}
                          jurisdictionConfig={jurisdictionConfig}
                          includeNonLocalProperties={includeNonLocalProperties}
                          onIncludeNonLocalPropertiesChange={setIncludeNonLocalProperties}
                          localPropertyCount={localPropertyCount}
                          nonLocalPropertyCount={nonLocalPropertyCount}
                          highlightedEntityId={selectedEntityId}
                          onSelectProperty={setSelectedProperty}
                          onMapSelected={setSelectedMapProperties}
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
                <div className="hidden lg:grid grid-cols-12 gap-3 h-[calc(100dvh-80px)] min-h-[650px] shrink-0">
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
                  <div id="properties-panel" className="col-span-8 h-full bg-white rounded-xl border border-slate-200 shadow-sm overflow-hidden flex flex-col min-h-0 scroll-mt-2">
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
                        networkName={networkData.networkName}
                        networkId={String(networkData.network_id || networkData.id || '')}
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

                {/* Floating Jump to Properties / Jump to Cards Pill (Mobile only) */}
                <AnimatePresence mode="wait">
                  {isScrolledPastCards ? (
                    <motion.button
                      key="jump-to-cards"
                      type="button"
                      initial={{ opacity: 0, scale: 0.85, y: 15 }}
                      animate={{ opacity: 1, scale: 1, y: 0 }}
                      exit={{ opacity: 0, scale: 0.85, y: 15 }}
                      transition={{ duration: 0.18 }}
                      onClick={handleScrollToTop}
                      className="fixed bottom-[calc(4.75rem+env(safe-area-inset-bottom,0px))] md:bottom-6 right-4 md:right-6 z-40 bg-slate-900/90 hover:bg-blue-600 text-white shadow-2xl backdrop-blur-md px-4 py-2.5 rounded-full text-xs font-black uppercase tracking-wider flex lg:hidden items-center gap-1.5 transition-all border border-white/15 cursor-pointer active:scale-95"
                      title="Jump back to summary cards at top"
                    >
                      <ArrowUp size={14} className="text-blue-400 shrink-0" aria-hidden="true" />
                      <span>Jump to Cards</span>
                    </motion.button>
                  ) : isAtDashboardTop && filteredProperties.length > 0 ? (
                    <motion.button
                      key="jump-to-properties"
                      type="button"
                      initial={{ opacity: 0, scale: 0.85, y: 15 }}
                      animate={{ opacity: 1, scale: 1, y: 0 }}
                      exit={{ opacity: 0, scale: 0.85, y: 15 }}
                      transition={{ duration: 0.18 }}
                      onClick={(e) => {
                        e.preventDefault();
                        e.stopPropagation();
                        handleNetworkJump('properties');
                      }}
                      className="fixed bottom-[calc(4.75rem+env(safe-area-inset-bottom,0px))] md:bottom-6 right-4 md:right-6 z-40 bg-blue-600 hover:bg-blue-700 text-white shadow-2xl px-4 py-2.5 rounded-full text-xs font-black uppercase tracking-wide flex lg:hidden items-center gap-1.5 transition-all ring-2 ring-white/80 cursor-pointer active:scale-95"
                      title="Open properties tab and scroll to property list"
                    >
                      <ArrowDown size={14} className="text-blue-200 shrink-0" aria-hidden="true" />
                      <span>View {filteredProperties.length.toLocaleString()} Properties</span>
                    </motion.button>
                  ) : null}
                </AnimatePresence>
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
              className="h-full w-full min-h-0 overflow-hidden"
            >
              <CityExplorer
                key={`${activeState}-${cityExplorerKey}`}
                city={
                  activeState === 'NY' || activeState === 'NYS' ? 'ny' :
                  activeState === 'BOSTON' ? 'ma' :
                  activeState === 'MIAMI' ? 'fl' :
                  activeState === 'CHICAGO' ? 'il' :
                  activeState === 'MINNEAPOLIS' ? 'mn' :
                  activeState.toLowerCase()
                }
                onBack={() => showDatasetPicker()}
                onMapSelected={setSelectedMapProperties}
                onOpenSiblingNetwork={(sibJur, sibNetworkKey) => {
                  const targetState = sibJur === 'nyc' || sibJur === 'ny' ? 'NY' : sibJur.toUpperCase();
                  openDataset(targetState, { initialTarget: { networkKey: sibNetworkKey } });
                }}
                initialNetworkKey={cityInitialTarget?.networkKey || null}
                initialBbl={cityInitialTarget?.bbl || null}
                initialQuery={cityInitialTarget?.query || null}
                onInitialConsumed={() => setCityInitialTarget(null)}
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
                initialCity={monitorCity}
                onSelectEntity={(id, type, name, city) => loadNetwork(id, type, name, city)}
              />
            </motion.div>
          )}
        </AnimatePresence>
      </main>

      <Suspense fallback={null}>
        <PropertyDetailsModal
          property={selectedProperty}
          networkData={networkData}
          onViewEntity={(entity, type) => setSelectedDetailEntity({ entity, type })}
          onSelectProperty={setSelectedProperty}
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
        <UpgradeModal
          isOpen={showUpgrade}
          onClose={() => setShowUpgrade(false)}
          onShowFreshness={() => setShowFreshness(true)}
          onOpenRapSheets={() => {
            const defaultMonitorCity = activeState === 'NY' ? 'NYC' : activeState === 'DETROIT' ? 'DETROIT' : 'HARTFORD';
            setMonitorCity(defaultMonitorCity);
            if (activeState !== 'NY' && activeState !== 'DETROIT') {
              setActiveState('CT');
            }
            setView('hartford');
          }}
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
          activeState={activeState}
        />
        <MultiPropertyMapModal
          properties={selectedMapProperties}
          onClose={() => setSelectedMapProperties(null)}
        />
      </Suspense>
    </div >
  );
}


export default App;
