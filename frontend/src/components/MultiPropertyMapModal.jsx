import React, { useState, useEffect } from 'react';
import { X, Map as MapIcon, Loader2, AlertCircle, User, DollarSign, Building2, Sliders, Search, Layers, Eye, ShieldAlert } from 'lucide-react';
import { motion, AnimatePresence } from 'framer-motion';
import { MapContainer, TileLayer, Marker, Popup, useMap } from 'react-leaflet';
import MarkerClusterGroup from 'react-leaflet-cluster';
import 'leaflet/dist/leaflet.css';
import L from 'leaflet';

// Fix for default Leaflet marker icons in React
import icon from 'leaflet/dist/images/marker-icon.png';
import iconShadow from 'leaflet/dist/images/marker-shadow.png';

let DefaultIcon = L.icon({
    iconUrl: icon,
    shadowUrl: iconShadow,
    iconSize: [25, 41],
    iconAnchor: [12, 41]
});

L.Marker.prototype.options.icon = DefaultIcon;

const CT_BOUNDS = { minLat: 40.8, maxLat: 42.3, minLon: -73.9, maxLon: -71.6 };
const US_BOUNDS = { minLat: 24, maxLat: 50, minLon: -125, maxLon: -66 };

const isNyProperty = (prop = {}) => {
    return String(prop.source || '').toUpperCase() === 'NYS_OPEN_DATA' || !!(prop.bbl || prop.borough);
};

const isValidCoordinate = (prop, lat, lon) => {
    const latNum = Number(lat);
    const lonNum = Number(lon);
    if (!Number.isFinite(latNum) || !Number.isFinite(lonNum)) return false;
    if ((latNum === 0 && lonNum === 0) || (latNum === -1 && lonNum === -1)) return false;
    if (latNum < US_BOUNDS.minLat || latNum > US_BOUNDS.maxLat || lonNum < US_BOUNDS.minLon || lonNum > US_BOUNDS.maxLon) return false;
    if (!isNyProperty(prop)) {
        return latNum >= CT_BOUNDS.minLat && latNum <= CT_BOUNDS.maxLat && lonNum >= CT_BOUNDS.minLon && lonNum <= CT_BOUNDS.maxLon;
    }
    return true;
};

const coordsFromProperty = (prop) => {
    const lat = prop?.latitude ?? prop?.lat;
    const lon = prop?.longitude ?? prop?.lon;
    if (!isValidCoordinate(prop, lat, lon)) return null;
    return { lat: Number(lat), lon: Number(lon) };
};

// Helper to fit map bounds dynamically
function MapBounds({ markers }) {
    const map = useMap();
    useEffect(() => {
        if (markers.length > 0) {
            const validMarkers = markers.filter(m => isValidCoordinate(m, m.lat, m.lon));
            if (validMarkers.length > 0) {
                const bounds = L.latLngBounds(validMarkers.map(m => [m.lat, m.lon]));
                map.fitBounds(bounds, { padding: [50, 50] });
            }
        }
    }, [markers, map]);
    return null;
}

export default function MultiPropertyMapModal({ properties, onClose }) {
    if (!properties) return null;

    const [markers, setMarkers] = useState([]);
    const [geoStatus, setGeoStatus] = useState({ total: 0, current: 0, completed: false, errors: 0 });
    const [loading, setLoading] = useState(true);

    // Map customization options
    const [baseMap, setBaseMap] = useState('dark'); // 'standard' | 'dark' | 'satellite'
    const [colorMode, setColorMode] = useState('owner'); // 'standard' | 'owner' | 'valuation' | 'evictions' | 'network'
    const [clustering, setClustering] = useState(true);
    const [filterText, setFilterText] = useState('');

    useEffect(() => {
        if (!properties || properties.length === 0) return;

        let isMounted = true;
        const toGeocode = [...properties];
        setGeoStatus({ total: toGeocode.length, current: 0, completed: false, errors: 0 });

        const cacheKey = "tow_geo_cache";
        const getCache = () => {
            try { return JSON.parse(localStorage.getItem(cacheKey) || '{}'); }
            catch { return {}; }
        };
        const updateCache = (addr, coords) => {
            const cache = getCache();
            cache[addr] = coords;
            try { localStorage.setItem(cacheKey, JSON.stringify(cache)); } catch { }
        };

        const fetchCoordinates = async () => {
            const results = [];
            let processed = 0;
            const uncached = [];

            // 1. First pass: Handle cached/existing coordinates instantly
            for (const prop of toGeocode) {
                const existingCoords = coordsFromProperty(prop);
                if (existingCoords) {
                    results.push({ ...prop, ...existingCoords });
                    processed++;
                } else {
                    const isNyc = !!(prop.bbl || prop.borough);
                    const state = isNyc ? 'NY' : 'CT';
                    const cityVal = prop.city || prop.property_city || prop.borough || '';
                    const addressFull = `${prop.address || prop.location}, ${cityVal}, ${state}`;
                    const cache = getCache();
                    const cachedCoords = cache[addressFull];

                    if (cachedCoords && isValidCoordinate(prop, cachedCoords.lat, cachedCoords.lon)) {
                        results.push({ ...prop, lat: Number(cachedCoords.lat), lon: Number(cachedCoords.lon) });
                        processed++;
                    } else {
                        uncached.push(prop);
                    }
                }
            }

            if (isMounted) {
                setGeoStatus(prev => ({ ...prev, current: processed }));
                setMarkers([...results]);
            }

            // 2. Process uncached in batches via Backend API
            const BATCH_SIZE = 100;
            const batches = [];
            for (let i = 0; i < uncached.length; i += BATCH_SIZE) {
                batches.push(uncached.slice(i, i + BATCH_SIZE));
            }

            if (batches.length > 0) {
                const apiBase = window.location.hostname === 'localhost' ? 'http://localhost:8000' : '';

                await Promise.all(batches.map(async (batch) => {
                    const propertyIds = batch.map(p => p.id).filter(id => id);
                    if (propertyIds.length === 0 || !isMounted) return;

                    try {
                        const res = await fetch(`${apiBase}/api/geocoding/batch`, {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify({ property_ids: propertyIds })
                        });

                        if (res.ok) {
                            const data = await res.json();
                            data.forEach(geo => {
                                const prop = batch.find(p => String(p.id) === String(geo.id));
                                if (prop && isValidCoordinate(prop, geo.lat, geo.lon)) {
                                    const result = { ...prop, lat: Number(geo.lat), lon: Number(geo.lon) };
                                    results.push(result);

                                    const isNyc = !!(prop.bbl || prop.borough);
                                    const state = isNyc ? 'NY' : 'CT';
                                    const addressFull = `${prop.address || prop.location}, ${prop.city || prop.property_city || prop.borough || ''}, ${state}`;
                                    updateCache(addressFull, { lat: Number(geo.lat), lon: Number(geo.lon) });
                                }
                            });
                        }
                    } catch (e) {
                        console.error("Batch Geocoding error", e);
                    }

                    processed += batch.length;
                    if (isMounted) {
                        setGeoStatus(prev => ({ ...prev, current: Math.min(processed, toGeocode.length) }));
                        setMarkers([...results]);
                    }
                }));
            }

            if (isMounted) {
                setLoading(false);
                setGeoStatus(prev => ({ ...prev, completed: true }));
            }
        };

        fetchCoordinates();

        return () => { isMounted = false; };
    }, [properties]);

    // Build unique owner colors for the map visualization
    const uniqueOwners = Array.from(new Set(markers.map(m => m.owner || m.owner_norm).filter(Boolean)));
    const ownerColors = {};
    const premiumPalette = [
        '#3b82f6', '#ef4444', '#10b981', '#f59e0b', '#8b5cf6', 
        '#ec4899', '#06b6d4', '#f97316', '#14b8a6', '#a855f7',
        '#059669', '#dc2626', '#d97706', '#2563eb', '#7c3aed'
    ];
    uniqueOwners.forEach((owner, idx) => {
        ownerColors[owner] = premiumPalette[idx % premiumPalette.length];
    });

    // Dynamic pinpoint creation depending on custom colorMode
    const createMarkerIcon = (marker) => {
        let color = '#3b82f6'; // fallback
        const ownerName = marker.owner || marker.owner_norm;

        if (colorMode === 'owner') {
            color = ownerColors[ownerName] || '#3b82f6';
        } else if (colorMode === 'valuation') {
            const valStr = String(marker.assessed_value || marker.appraised_value || '0');
            const val = parseFloat(valStr.replace(/[^0-9.]/g, ''));
            if (val > 1000000) color = '#f59e0b'; // Gold for >1M
            else if (val > 500000) color = '#a855f7'; // Purple for 500k-1M
            else if (val > 250000) color = '#3b82f6'; // Blue
            else color = '#10b981'; // Green for smaller
        } else if (colorMode === 'evictions') {
            // Evictions count check
            const evCount = parseInt(marker.eviction_count || (marker.evictions ? marker.evictions.length : 0)) || 0;
            color = evCount > 0 ? '#ef4444' : '#3b82f6'; // Red for evictions
        } else if (colorMode === 'network') {
            color = marker.is_in_network !== false ? '#3b82f6' : '#94a3b8'; // Grey for neighboring sibling records
        }

        return L.divIcon({
            html: `
                <div class="relative flex items-center justify-center" style="width: 24px; height: 24px;">
                    <div class="absolute w-6 h-6 rounded-full opacity-35 animate-ping" style="background-color: ${color};"></div>
                    <div class="w-3.5 h-3.5 rounded-full border-2 border-white shadow-lg" style="background-color: ${color};"></div>
                </div>
            `,
            className: '',
            iconSize: [24, 24],
            iconAnchor: [12, 12]
        });
    };

    // Filter markers based on sidebar search input
    const filteredMarkers = markers.filter(m => {
        if (!isValidCoordinate(m, m.lat, m.lon)) return false;
        const query = filterText.toLowerCase();
        const address = (m.address || m.location || "").toLowerCase();
        const owner = (m.owner || m.owner_norm || "").toLowerCase();
        const city = (m.city || m.property_city || "").toLowerCase();
        return address.includes(query) || owner.includes(query) || city.includes(query);
    });

    const getTileLayerDetails = () => {
        switch (baseMap) {
            case 'dark':
                return {
                    url: "https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png",
                    attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>'
                };
            case 'satellite':
                return {
                    url: "https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
                    attribution: 'Tiles &copy; Esri &mdash; Source: Esri, i-cubed, USDA, USGS, AEX, GeoEye, Getmapping, Aerogrid, IGN, IGP, UPR-EGP, and the GIS User Community'
                };
            case 'standard':
            default:
                return {
                    url: "https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png",
                    attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
                };
        }
    };

    const tileDetails = getTileLayerDetails();

    const firstValidProp = properties.find(p => coordsFromProperty(p));
    const firstValidCoords = firstValidProp ? coordsFromProperty(firstValidProp) : null;
    const initialCenter = firstValidProp 
        ? [firstValidCoords.lat, firstValidCoords.lon] 
        : [41.5623, -72.8252];

    return (
        <AnimatePresence>
            <div className="fixed inset-0 z-[60] flex items-center justify-center p-4">
                <motion.div
                    initial={{ opacity: 0 }}
                    animate={{ opacity: 1 }}
                    exit={{ opacity: 0 }}
                    onClick={onClose}
                    className="absolute inset-0 bg-slate-900/75 backdrop-blur-sm"
                />

                <motion.div
                    initial={{ opacity: 0, scale: 0.95 }}
                    animate={{ opacity: 1, scale: 1 }}
                    exit={{ opacity: 0, scale: 0.95 }}
                    className="relative bg-white rounded-2xl shadow-2xl w-full max-w-6xl h-[85vh] flex flex-col overflow-hidden border border-slate-100"
                >
                    {/* Header */}
                    <div className="p-4 border-b border-slate-100 flex items-center justify-between bg-slate-50/50 shrink-0">
                        <div>
                            <h2 className="text-lg font-black text-slate-900 flex items-center gap-2">
                                <MapIcon size={20} className="text-blue-600" />
                                Interactive Network Portfolio Map
                            </h2>
                            <p className="text-xs text-slate-500 font-semibold uppercase tracking-wider mt-0.5">
                                Showing {filteredMarkers.length} of {markers.length} properties
                                {loading && ` • Geocoding ${geoStatus.current}/${geoStatus.total}...`}
                            </p>
                        </div>
                        <button onClick={onClose} className="p-2 hover:bg-slate-100 rounded-full text-slate-400 hover:text-slate-600 transition-colors">
                            <X size={20} />
                        </button>
                    </div>

                    {/* Main Split Layout */}
                    <div className="flex-1 flex flex-col md:flex-row overflow-hidden relative">
                        
                        {/* Left Customization and Filter Control Sidebar */}
                        <div className="w-full md:w-80 border-r border-slate-100 flex flex-col bg-slate-50 overflow-y-auto p-4 shrink-0 space-y-5">
                            
                            {/* Search Box */}
                            <div className="space-y-1.5">
                                <label className="block text-[10px] font-bold text-slate-400 uppercase tracking-widest">Search Properties</label>
                                <div className="relative">
                                    <Search className="absolute left-3 top-2.5 h-4 w-4 text-slate-400" />
                                    <input
                                        type="text"
                                        placeholder="Address, owner, or city..."
                                        value={filterText}
                                        onChange={(e) => setFilterText(e.target.value)}
                                        className="w-full pl-9 pr-4 py-2 text-xs font-semibold rounded-lg border border-slate-200 focus:outline-none focus:ring-2 focus:ring-blue-500/20 bg-white placeholder-slate-400 text-slate-700"
                                    />
                                </div>
                            </div>

                            {/* Base Map Selector */}
                            <div className="space-y-2">
                                <label className="block text-[10px] font-bold text-slate-400 uppercase tracking-widest flex items-center gap-1.5">
                                    <Layers size={12} className="text-slate-400" />
                                    Map Style
                                </label>
                                <div className="grid grid-cols-3 gap-1 bg-white p-1 rounded-lg border border-slate-200">
                                    {['standard', 'dark', 'satellite'].map((style) => (
                                        <button
                                            key={style}
                                            onClick={() => setBaseMap(style)}
                                            className={`py-1.5 text-[10px] font-black rounded capitalize transition-all ${
                                                baseMap === style
                                                    ? 'bg-slate-900 text-white shadow'
                                                    : 'text-slate-500 hover:bg-slate-50'
                                            }`}
                                        >
                                            {style === 'standard' ? 'Street' : style}
                                        </button>
                                    ))}
                                </div>
                            </div>

                            {/* Color Mode Selector */}
                            <div className="space-y-2">
                                <label className="block text-[10px] font-bold text-slate-400 uppercase tracking-widest flex items-center gap-1.5">
                                    <Eye size={12} className="text-slate-400" />
                                    Color Code Markers By
                                </label>
                                <div className="space-y-1 bg-white p-1.5 rounded-lg border border-slate-200">
                                    {[
                                        { id: 'standard', label: 'Standard Theme (Blue)' },
                                        { id: 'owner', label: 'Owner Shell LLCs' },
                                        { id: 'valuation', label: 'Valuation Range' },
                                        { id: 'evictions', label: 'Eviction Filings' },
                                        { id: 'network', label: 'Direct vs Neighbors' }
                                    ].map((mode) => (
                                        <button
                                            key={mode.id}
                                            onClick={() => setColorMode(mode.id)}
                                            className={`w-full text-left px-2.5 py-1.5 text-xs font-bold rounded transition-colors flex items-center justify-between ${
                                                colorMode === mode.id
                                                    ? 'bg-blue-50 text-blue-700'
                                                    : 'text-slate-600 hover:bg-slate-50'
                                            }`}
                                        >
                                            <span>{mode.label}</span>
                                            {colorMode === mode.id && <div className="w-1.5 h-1.5 rounded-full bg-blue-600" />}
                                        </button>
                                    ))}
                                </div>
                            </div>

                            {/* Marker Clustering Toggle */}
                            <div className="flex items-center justify-between bg-white p-2.5 rounded-lg border border-slate-200">
                                <div className="flex flex-col">
                                    <span className="text-xs font-bold text-slate-700">Cluster Markers</span>
                                    <span className="text-[9px] font-medium text-slate-400">Group pins at high zoom levels</span>
                                </div>
                                <button
                                    onClick={() => setClustering(!clustering)}
                                    className={`relative inline-flex h-5 w-9 shrink-0 cursor-pointer rounded-full border-2 border-transparent transition-colors duration-200 ease-in-out focus:outline-none ${
                                        clustering ? 'bg-blue-600' : 'bg-slate-200'
                                    }`}
                                >
                                    <span
                                        className={`pointer-events-none inline-block h-4 w-4 transform rounded-full bg-white shadow ring-0 transition duration-200 ease-in-out ${
                                            clustering ? 'translate-x-4' : 'translate-x-0'
                                        }`}
                                    />
                                </button>
                            </div>

                            {/* Color Legend Card */}
                            {colorMode !== 'standard' && (
                                <div className="p-3 rounded-lg border border-slate-200 bg-white space-y-2">
                                    <div className="text-[10px] font-bold text-slate-400 uppercase tracking-widest">Legend</div>
                                    <div className="space-y-1.5 text-xs font-semibold text-slate-600">
                                        {colorMode === 'owner' && (
                                            <div className="space-y-1">
                                                <div className="text-[9px] text-slate-400 font-medium mb-1">Each LLC shell is assigned a distinct palette color:</div>
                                                {uniqueOwners.slice(0, 5).map((owner) => (
                                                    <div key={owner} className="flex items-center gap-2">
                                                        <div className="w-2.5 h-2.5 rounded-full shrink-0" style={{ backgroundColor: ownerColors[owner] }} />
                                                        <span className="truncate max-w-[180px]">{owner}</span>
                                                    </div>
                                                ))}
                                                {uniqueOwners.length > 5 && (
                                                    <div className="text-[10px] text-slate-400 italic font-medium pl-4">+ {uniqueOwners.length - 5} more LLCs</div>
                                                )}
                                            </div>
                                        )}
                                        {colorMode === 'valuation' && (
                                            <div className="space-y-1.5">
                                                <div className="flex items-center gap-2">
                                                    <div className="w-2.5 h-2.5 rounded-full bg-[#f59e0b]" />
                                                    <span>Premium (&gt;$1.0M Assessed)</span>
                                                </div>
                                                <div className="flex items-center gap-2">
                                                    <div className="w-2.5 h-2.5 rounded-full bg-[#a855f7]" />
                                                    <span>High Value ($500K - $1.0M)</span>
                                                </div>
                                                <div className="flex items-center gap-2">
                                                    <div className="w-2.5 h-2.5 rounded-full bg-[#3b82f6]" />
                                                    <span>Mid Range ($250K - $500K)</span>
                                                </div>
                                                <div className="flex items-center gap-2">
                                                    <div className="w-2.5 h-2.5 rounded-full bg-[#10b981]" />
                                                    <span>Affordable (&lt;$250K)</span>
                                                </div>
                                            </div>
                                        )}
                                        {colorMode === 'evictions' && (
                                            <div className="space-y-1.5">
                                                <div className="flex items-center gap-2">
                                                    <div className="w-2.5 h-2.5 rounded-full bg-[#ef4444]" />
                                                    <span>Has Eviction Filings</span>
                                                </div>
                                                <div className="flex items-center gap-2">
                                                    <div className="w-2.5 h-2.5 rounded-full bg-[#3b82f6]" />
                                                    <span>No Evictions Filed</span>
                                                </div>
                                            </div>
                                        )}
                                        {colorMode === 'network' && (
                                            <div className="space-y-1.5">
                                                <div className="flex items-center gap-2">
                                                    <div className="w-2.5 h-2.5 rounded-full bg-[#3b82f6]" />
                                                    <span>Direct Portfolio Member</span>
                                                </div>
                                                <div className="flex items-center gap-2">
                                                    <div className="w-2.5 h-2.5 rounded-full bg-[#94a3b8]" />
                                                    <span>Base Sibling (Neighbor)</span>
                                                </div>
                                            </div>
                                        )}
                                    </div>
                                </div>
                            )}

                        </div>

                        {/* Right Interactive Leaflet Map Panel */}
                        <div className="flex-1 relative bg-slate-100 h-full">
                            {loading && markers.length === 0 ? (
                                <div className="absolute inset-0 flex flex-col items-center justify-center text-slate-500 gap-3 bg-slate-50/80 backdrop-blur z-[10]">
                                    <Loader2 className="w-8 h-8 animate-spin text-blue-500" />
                                    <span className="font-bold text-slate-700">Geocoding addresses... ({geoStatus.current}/{geoStatus.total})</span>
                                    <div className="w-64 h-2 bg-slate-200 rounded-full overflow-hidden">
                                        <div
                                            className="h-full bg-blue-500 transition-all duration-300"
                                            style={{ width: `${(geoStatus.current / geoStatus.total) * 100}%` }}
                                        />
                                    </div>
                                </div>
                            ) : (
                                <MapContainer key={baseMap} center={initialCenter} zoom={10} style={{ height: '100%', width: '100%', outline: 'none' }}>
                                    <TileLayer
                                        attribution={tileDetails.attribution}
                                        url={tileDetails.url}
                                    />
                                    
                                    {clustering ? (
                                        <MarkerClusterGroup
                                            chunkedLoading
                                            spiderfyOnMaxZoom={true}
                                            showCoverageOnHover={false}
                                            maxClusterRadius={60}
                                            iconCreateFunction={(cluster) => {
                                                const count = cluster.getChildCount();
                                                let size = 'small';
                                                if (count >= 10) size = 'medium';
                                                if (count >= 50) size = 'large';
                                                const sizeMap = { small: 30, medium: 40, large: 50 };
                                                return L.divIcon({
                                                    html: `<div style="background: #3b82f6; color: white; border-radius: 50%; width: ${sizeMap[size]}px; height: ${sizeMap[size]}px; display: flex; align-items: center; justify-content: center; font-weight: bold; border: 3px solid white; box-shadow: 0 2px 8px rgba(0,0,0,0.3);">${count}</div>`,
                                                    className: '',
                                                    iconSize: L.point(sizeMap[size], sizeMap[size])
                                                });
                                            }}
                                        >
                                            {filteredMarkers.map((m, idx) => (
                                                <Marker key={idx} position={[m.lat, m.lon]} icon={createMarkerIcon(m)}>
                                                    <Popup>
                                                        <div className="min-w-[220px] p-1 text-slate-800">
                                                            <div className="flex items-start gap-3 mb-3 pb-2 border-b border-slate-100">
                                                                <div className={`p-2 rounded-lg shrink-0 ${m.isComplex ? 'bg-indigo-50 text-indigo-600' : 'bg-blue-50 text-blue-600'}`}>
                                                                    <Building2 size={16} />
                                                                </div>
                                                                <div className="flex-1 min-w-0">
                                                                    <div className="text-sm font-bold text-slate-900 leading-tight truncate">
                                                                        {m.address || m.location}
                                                                    </div>
                                                                    <div className="flex items-center gap-2 mt-0.5">
                                                                        <span className="text-[10px] text-slate-400 font-bold uppercase tracking-wider">{m.city || m.property_city}</span>
                                                                        {(m.unit || m.derivedUnit) && (
                                                                            <span className="text-[9px] bg-slate-100 text-slate-500 px-1.5 py-0.5 rounded font-black">
                                                                                UNIT {m.derivedUnit || m.unit}
                                                                            </span>
                                                                        )}
                                                                    </div>
                                                                </div>
                                                            </div>

                                                            <div className="space-y-2">
                                                                <div className="flex items-center gap-2 text-[11px] text-slate-600">
                                                                    <User size={12} className="text-slate-400 shrink-0" />
                                                                    <span className="truncate font-semibold text-slate-700">{m.owner || m.owner_norm}</span>
                                                                </div>
                                                                
                                                                {/* Show evictions details on marker popup */}
                                                                {parseInt(m.eviction_count || 0) > 0 && (
                                                                    <div className="flex items-center gap-2 text-[11px] text-red-600 bg-red-50 px-2 py-1 rounded border border-red-100">
                                                                        <ShieldAlert size={12} className="shrink-0" />
                                                                        <span className="font-bold">{m.eviction_count} Eviction Filings</span>
                                                                    </div>
                                                                )}

                                                                {(m.assessed_value || m.appraised_value) && (
                                                                    <div className="flex items-center gap-2 text-xs font-bold text-slate-900 bg-slate-50 p-1.5 rounded-md border border-slate-100">
                                                                        <DollarSign size={12} className="text-green-600" />
                                                                        <span className="font-mono">{m.assessed_value || m.appraised_value}</span>
                                                                        <span className="text-[9px] text-slate-400 font-bold ml-auto uppercase tracking-wider">
                                                                            {m.assessed_value ? 'Assessed' : 'Appraised'}
                                                                        </span>
                                                                    </div>
                                                                )}
                                                            </div>
                                                        </div>
                                                    </Popup>
                                                </Marker>
                                            ))}
                                        </MarkerClusterGroup>
                                    ) : (
                                        filteredMarkers.map((m, idx) => (
                                            <Marker key={idx} position={[m.lat, m.lon]} icon={createMarkerIcon(m)}>
                                                <Popup>
                                                    <div className="min-w-[220px] p-1 text-slate-800">
                                                        <div className="flex items-start gap-3 mb-3 pb-2 border-b border-slate-100">
                                                            <div className={`p-2 rounded-lg shrink-0 ${m.isComplex ? 'bg-indigo-50 text-indigo-600' : 'bg-blue-50 text-blue-600'}`}>
                                                                <Building2 size={16} />
                                                            </div>
                                                            <div className="flex-1 min-w-0">
                                                                <div className="text-sm font-bold text-slate-900 leading-tight truncate">
                                                                    {m.address || m.location}
                                                                </div>
                                                                <div className="flex items-center gap-2 mt-0.5">
                                                                    <span className="text-[10px] text-slate-400 font-bold uppercase tracking-wider">{m.city || m.property_city}</span>
                                                                    {(m.unit || m.derivedUnit) && (
                                                                        <span className="text-[9px] bg-slate-100 text-slate-500 px-1.5 py-0.5 rounded font-black">
                                                                            UNIT {m.derivedUnit || m.unit}
                                                                        </span>
                                                                    )}
                                                                </div>
                                                            </div>
                                                        </div>

                                                        <div className="space-y-2">
                                                            <div className="flex items-center gap-2 text-[11px] text-slate-600">
                                                                    <User size={12} className="text-slate-400 shrink-0" />
                                                                    <span className="truncate font-semibold text-slate-700">{m.owner || m.owner_norm}</span>
                                                            </div>

                                                            {parseInt(m.eviction_count || 0) > 0 && (
                                                                <div className="flex items-center gap-2 text-[11px] text-red-600 bg-red-50 px-2 py-1 rounded border border-red-100">
                                                                    <ShieldAlert size={12} className="shrink-0" />
                                                                    <span className="font-bold">{m.eviction_count} Eviction Filings</span>
                                                                </div>
                                                            )}

                                                            {(m.assessed_value || m.appraised_value) && (
                                                                <div className="flex items-center gap-2 text-xs font-bold text-slate-900 bg-slate-50 p-1.5 rounded-md border border-slate-100">
                                                                    <DollarSign size={12} className="text-green-600" />
                                                                    <span className="font-mono">{m.assessed_value || m.appraised_value}</span>
                                                                    <span className="text-[9px] text-slate-400 font-bold ml-auto uppercase tracking-wider">
                                                                        {m.assessed_value ? 'Assessed' : 'Appraised'}
                                                                    </span>
                                                                </div>
                                                            )}
                                                        </div>
                                                    </div>
                                                </Popup>
                                            </Marker>
                                        ))
                                    )}
                                    <MapBounds markers={filteredMarkers} />
                                </MapContainer>
                            )}

                            {/* Floating Geocoding/Loading Toast */}
                            {loading && markers.length > 0 && (
                                <div className="absolute top-4 right-4 bg-white/95 backdrop-blur px-3 py-2 rounded-lg shadow-lg text-[10px] font-black uppercase tracking-wider text-slate-700 flex items-center gap-2 z-[1000] border border-slate-200">
                                    <Loader2 className="w-3.5 h-3.5 animate-spin text-blue-600" />
                                    <span>Processing... {geoStatus.current}/{geoStatus.total}</span>
                                </div>
                            )}
                        </div>

                    </div>
                </motion.div>
            </div>
        </AnimatePresence>
    );
}
