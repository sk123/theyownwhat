import React, { useState, useEffect } from 'react';
import { X, Map as MapIcon, Loader2, AlertCircle } from 'lucide-react';
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

// Helper to fit map bounds
function MapBounds({ markers }) {
    const map = useMap();
    useEffect(() => {
        if (markers.length > 0) {
            const bounds = L.latLngBounds(markers.map(m => [m.lat, m.lon]));
            map.fitBounds(bounds, { padding: [50, 50] });
        }
    }, [markers, map]);
    return null;
}

export default function MultiPropertyMapModal({ properties, onClose }) {
    if (!properties) return null;

    const [markers, setMarkers] = useState([]);
    const [geoStatus, setGeoStatus] = useState({ total: 0, current: 0, completed: false, errors: 0 });
    const [loading, setLoading] = useState(true);

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

            // 1. First pass: Handle cached/existing instantly
            for (const prop of toGeocode) {
                if (prop.latitude && prop.longitude) {
                    results.push({ ...prop, lat: prop.latitude, lon: prop.longitude });
                    processed++;
                } else {
                    const addressFull = `${prop.address}, ${prop.city}, CT`;
                    const cache = getCache();

                    if (cache[addressFull]) {
                        results.push({ ...prop, ...cache[addressFull] });
                        processed++;
                    } else {
                        uncached.push(prop);
                    }
                }
            }

            // Update state with initial cached results
            if (isMounted) {
                setGeoStatus(prev => ({ ...prev, current: processed }));
                setMarkers([...results]);
            }

            // 2. Process uncached in parallel batches via Backend API
            const BATCH_SIZE = 100;

            for (let i = 0; i < uncached.length; i += BATCH_SIZE) {
                if (!isMounted) break;

                const batch = uncached.slice(i, i + BATCH_SIZE);
                const propertyIds = batch.map(p => p.id).filter(id => id);

                if (propertyIds.length > 0) {
                    try {
                        const res = await fetch('http://localhost:8000/api/geocoding/batch', {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify({ property_ids: propertyIds })
                        });

                        if (res.ok) {
                            const data = await res.json();
                            // Merge results
                            data.forEach(geo => {
                                const prop = batch.find(p => String(p.id) === String(geo.id));
                                if (prop) {
                                    const result = { ...prop, lat: geo.lat, lon: geo.lon };
                                    results.push(result);

                                    // Update local cache
                                    const addressFull = `${prop.address}, ${prop.city}, CT`;
                                    updateCache(addressFull, { lat: geo.lat, lon: geo.lon });
                                }
                            });
                        }
                    } catch (e) {
                        console.error("Batch Geocoding error", e);
                    }
                }

                processed += batch.length;

                if (isMounted) {
                    setGeoStatus(prev => ({ ...prev, current: processed }));
                    setMarkers([...results]);
                }
            }

            if (isMounted) {
                setLoading(false);
                setGeoStatus(prev => ({ ...prev, completed: true }));
            }
        };

        fetchCoordinates();

        return () => { isMounted = false; };
    }, [properties]);

    return (
        <AnimatePresence>
            <div className="fixed inset-0 z-[60] flex items-center justify-center p-4">
                <motion.div
                    initial={{ opacity: 0 }}
                    animate={{ opacity: 1 }}
                    exit={{ opacity: 0 }}
                    onClick={onClose}
                    className="absolute inset-0 bg-black/60 backdrop-blur-sm"
                />

                <motion.div
                    initial={{ opacity: 0, scale: 0.95 }}
                    animate={{ opacity: 1, scale: 1 }}
                    exit={{ opacity: 0, scale: 0.95 }}
                    className="relative bg-white rounded-2xl shadow-2xl w-full max-w-4xl h-[80vh] flex flex-col overflow-hidden"
                >
                    {/* Header */}
                    <div className="p-4 border-b border-gray-100 flex items-center justify-between bg-gray-50/50">
                        <div>
                            <h2 className="text-xl font-bold text-gray-900 flex items-center gap-2">
                                <MapIcon size={20} className="text-blue-600" />
                                Property Map
                            </h2>
                            <p className="text-sm text-gray-500">
                                Showing {markers.length} properties
                                {loading && ` • Geocoding ${geoStatus.current}/${geoStatus.total}...`}
                            </p>
                        </div>
                        <button onClick={onClose} className="p-2 hover:bg-gray-100 rounded-full text-gray-400 hover:text-gray-600">
                            <X size={20} />
                        </button>
                    </div>

                    {/* Content */}
                    <div className="flex-1 relative bg-slate-100">
                        {loading && markers.length === 0 ? (
                            <div className="absolute inset-0 flex flex-col items-center justify-center text-gray-500 gap-3">
                                <Loader2 className="w-8 h-8 animate-spin text-blue-500" />
                                <span>Geocoding addresses... ({geoStatus.current}/{geoStatus.total})</span>
                                <div className="w-64 h-2 bg-gray-200 rounded-full overflow-hidden">
                                    <div
                                        className="h-full bg-blue-500 transition-all duration-300"
                                        style={{ width: `${(geoStatus.current / geoStatus.total) * 100}%` }}
                                    />
                                </div>
                            </div>
                        ) : (
                            <MapContainer center={[41.5623, -72.8252]} zoom={10} style={{ height: '100%', width: '100%' }}>
                                <TileLayer
                                    attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
                                    url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
                                />
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
                                    {markers.map((m, idx) => (
                                        <Marker key={idx} position={[m.lat, m.lon]}>
                                            <Popup>
                                                <div className="font-medium">{m.address}</div>
                                                <div className="text-xs text-gray-500">{m.city}</div>
                                            </Popup>
                                        </Marker>
                                    ))}
                                </MarkerClusterGroup>
                                <MapBounds markers={markers} />
                            </MapContainer>
                        )}

                        {/* Overlay Status */}
                        {loading && markers.length > 0 && (
                            <div className="absolute top-4 right-4 bg-white/90 backdrop-blur px-3 py-2 rounded-lg shadow-lg text-xs font-bold flex items-center gap-2 z-[1000]">
                                <Loader2 className="w-3 h-3 animate-spin text-blue-600" />
                                <span>Processing... {geoStatus.current}/{geoStatus.total}</span>
                            </div>
                        )}
                    </div>
                </motion.div>
            </div>
        </AnimatePresence>
    );
}
