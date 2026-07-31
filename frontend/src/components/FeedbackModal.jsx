import { useState, useEffect, useRef } from 'react';
import { X, MessageSquare, AlertTriangle, Send, Search, Check, Link, Unlink, FileQuestion, HelpCircle, ArrowRight, ArrowLeft, Briefcase, User, MapPin } from 'lucide-react';

const SearchInput = ({
    label,
    value,
    fieldName,
    placeholder,
    activeSearchField,
    setActiveSearchField,
    searchTerm,
    handleSearch,
    searchResults,
    searching,
    selectEntity,
    onClear,
}) => {
    const containerRef = useRef(null);

    useEffect(() => {
        const handleClickOutside = (e) => {
            if (containerRef.current && !containerRef.current.contains(e.target)) {
                if (activeSearchField === fieldName) {
                    setActiveSearchField(null);
                }
            }
        };
        document.addEventListener('mousedown', handleClickOutside);
        return () => document.removeEventListener('mousedown', handleClickOutside);
    }, [activeSearchField, fieldName, setActiveSearchField]);

    const typeConfig = {
        'Business': { icon: Briefcase, color: 'text-blue-600', bg: 'bg-blue-50', badge: 'bg-blue-100 text-blue-800' },
        'Business Principal': { icon: User, color: 'text-indigo-600', bg: 'bg-indigo-50', badge: 'bg-indigo-100 text-indigo-800' },
        'Property Owner': { icon: User, color: 'text-emerald-600', bg: 'bg-emerald-50', badge: 'bg-emerald-100 text-emerald-800' },
        'Address': { icon: MapPin, color: 'text-rose-600', bg: 'bg-rose-50', badge: 'bg-rose-100 text-rose-800' },
        'Ownership Network': { icon: Link, color: 'text-purple-600', bg: 'bg-purple-50', badge: 'bg-purple-100 text-purple-800' },
    };

    return (
        <div className="mb-4 relative" ref={containerRef}>
            <label className="block text-xs font-bold text-gray-500 uppercase tracking-wide mb-1.5">{label}</label>

            {value ? (
                <div className="flex items-center justify-between p-3 bg-blue-50 border border-blue-200 rounded-lg text-blue-800">
                    <div className="flex items-center gap-2 overflow-hidden">
                        <Check className="w-4 h-4 text-blue-600 flex-shrink-0" />
                        <span className="font-medium truncate">{value.name || value.label || value.title}</span>
                        <span className="text-xs opacity-70 border-l border-blue-200 pl-2 ml-1">{value.type}</span>
                    </div>
                    <button
                        type="button"
                        onClick={onClear}
                        className="p-1 hover:bg-blue-100 rounded-full transition-colors"
                    >
                        <X className="w-4 h-4" />
                    </button>
                </div>
            ) : (
                <div className="relative">
                    {activeSearchField === fieldName ? (
                        <>
                            <Search className="absolute left-3 top-3 w-4 h-4 text-blue-500 z-10" />
                            <input
                                autoFocus
                                type="text"
                                value={searchTerm}
                                onChange={e => handleSearch(e.target.value)}
                                placeholder="Search by business, owner, address, or network..."
                                className="w-full pl-9 pr-9 py-2.5 border border-blue-500 ring-2 ring-blue-100 rounded-lg text-sm bg-white outline-none transition-all"
                            />
                            <button
                                type="button"
                                onClick={() => setActiveSearchField(null)}
                                className="absolute right-3 top-3 text-gray-400 hover:text-gray-600 z-10"
                            >
                                <X className="w-4 h-4" />
                            </button>

                            {/* Dropdown */}
                            {(searchResults.length > 0 || searching) && (
                                <div className="absolute top-full left-0 right-0 mt-1 bg-white border border-gray-200 rounded-xl shadow-2xl max-h-64 overflow-y-auto z-[300]">
                                    {searching && <div className="p-4 text-xs text-center text-gray-400 font-medium">Searching across all jurisdictions...</div>}
                                    {!searching && searchResults.length === 0 && (
                                        <div className="p-4 text-xs text-center text-gray-400 font-medium">No matching entities found.</div>
                                    )}
                                    {searchResults.map((res, idx) => {
                                        const config = typeConfig[res.type] || { icon: Search, color: 'text-gray-400', bg: 'bg-gray-50', badge: 'bg-gray-100 text-gray-600' };
                                        const IconComp = config.icon;
                                        return (
                                            <div
                                                key={res.id || `${res.name}-${idx}`}
                                                onMouseDown={(e) => {
                                                    e.preventDefault();
                                                    selectEntity(res);
                                                }}
                                                className="p-3 hover:bg-blue-50/70 cursor-pointer flex items-center justify-between border-b border-gray-100 last:border-0 transition-colors"
                                            >
                                                <div className="flex items-center gap-3 truncate pr-2">
                                                    <div className={`p-2 rounded-lg ${config.bg} ${config.color} shrink-0`}>
                                                        <IconComp className="w-4 h-4" />
                                                    </div>
                                                    <div className="truncate">
                                                        <div className="text-sm font-semibold text-gray-900 truncate">{res.name || res.label}</div>
                                                        <div className="text-xs text-gray-500 truncate">{res.context || res.type}</div>
                                                    </div>
                                                </div>
                                                <div className="flex items-center gap-1.5 shrink-0">
                                                    {res.type && (
                                                        <span className={`text-[10px] font-bold px-2 py-0.5 rounded-full ${config.badge}`}>{res.type}</span>
                                                    )}
                                                    {res._source === 'nyc' && (
                                                        <span className="text-[9px] font-black px-1.5 py-0.5 rounded bg-violet-100 text-violet-700">NYC</span>
                                                    )}
                                                </div>
                                            </div>
                                        );
                                    })}
                                </div>
                            )}
                        </>
                    ) : (
                        <div
                            onClick={() => { setActiveSearchField(fieldName); handleSearch(''); }}
                            className="w-full pl-9 pr-4 py-2.5 border border-gray-200 rounded-lg text-sm bg-gray-50 text-gray-500 cursor-text hover:border-gray-300 hover:bg-white transition-all flex items-center"
                        >
                            <Search className="absolute left-3 w-4 h-4 text-gray-400" />
                            {placeholder}
                        </div>
                    )}
                </div>
            )}
        </div>
    );
};

const FeedbackModal = ({ isOpen, onClose, initialEntity = null, activeState = null }) => {
    // Flow state: 'menu' | 'link' | 'unlink' | 'correction' | 'missing' | 'other'
    const [flow, setFlow] = useState('menu');
    const [step, setStep] = useState(1);

    // Form Data
    const [description, setDescription] = useState('');
    const [entityA, setEntityA] = useState(null);
    const [entityB, setEntityB] = useState(null);

    // Search State
    const [activeSearchField, setActiveSearchField] = useState(null); // 'A' or 'B' or 'single'
    const [searchTerm, setSearchTerm] = useState('');
    const [searchResults, setSearchResults] = useState([]);
    const [searching, setSearching] = useState(false);

    // Submission State
    const [submitting, setSubmitting] = useState(false);
    const [success, setSuccess] = useState(false);

    useEffect(() => {
        if (isOpen) {
            resetState();
            if (initialEntity) {
                setEntityA(initialEntity);
            }
        }
    }, [isOpen, initialEntity]);

    const resetState = () => {
        setFlow('menu');
        setStep(1);
        setDescription('');
        setEntityA(null);
        setEntityB(null);
        setSearchTerm('');
        setSearchResults([]);
        setSuccess(false);
        setSubmitting(false);
        setActiveSearchField(null);
    };

    const handleSearch = async (term) => {
        setSearchTerm(term);
        if (!term || term.length < 2) { setSearchResults([]); return; }
        setSearching(true);
        try {
            let url = `/api/autocomplete?q=${encodeURIComponent(term)}&type=all`;
            if (activeState) {
                url += `&state=${encodeURIComponent(activeState)}`;
            }
            const res = await fetch(url);
            if (res.ok) {
                const data = await res.json();
                const mapped = data.map(item => ({
                    id: item.id || item.value || item.name,
                    name: item.label || item.name || item.value,
                    type: item.type || 'Entity',
                    context: item.context || '',
                    jurisdiction: item.jurisdiction || activeState || 'CT',
                    ...item
                }));
                setSearchResults(mapped);
            } else {
                setSearchResults([]);
            }
        } catch (e) {
            console.error('Feedback autocomplete search failed', e);
            setSearchResults([]);
        } finally {
            setSearching(false);
        }
    };

    const selectEntity = (entity) => {
        if (activeSearchField === 'A' || activeSearchField === 'single') {
            setEntityA(entity);
        } else if (activeSearchField === 'B') {
            setEntityB(entity);
        }
        setSearchTerm('');
        setSearchResults([]);
        setActiveSearchField(null);
    };

    const handleSubmit = async () => {
        if (!description) return;

        setSubmitting(true);

        // Map flow to backend report_type
        let reportType = 'other';
        let entities = [];

        if (flow === 'link') {
            reportType = 'link_request';
            if (entityA) entities.push(entityA);
            if (entityB) entities.push(entityB);
        } else if (flow === 'unlink') {
            reportType = 'unlink_request';
            if (entityA) entities.push(entityA);
            if (entityB) entities.push(entityB);
        } else if (flow === 'correction') {
            reportType = 'data_correction';
            if (entityA) entities.push(entityA);
        } else if (flow === 'missing') {
            reportType = 'missing_data';
        }

        try {
            const res = await fetch('/api/feedback', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    report_type: reportType,
                    description,
                    related_entities: entities
                })
            });
            if (!res.ok) throw new Error("Submission failed");
            setSuccess(true);
            setTimeout(() => {
                onClose();
            }, 2500);
        } catch (e) {
            alert("Failed to submit report. Please try again.");
        } finally {
            setSubmitting(false);
        }
    };

    if (!isOpen) return null;

    return (
        <div className="fixed inset-0 z-[200] overflow-y-auto bg-black/60 backdrop-blur-sm px-4 pt-16 pb-4 flex justify-center items-start md:items-center md:p-4" onClick={onClose}>
            <div className="bg-white rounded-2xl shadow-2xl w-full max-w-lg flex flex-col my-auto h-auto max-h-none md:max-h-[90vh] overflow-visible" onClick={e => e.stopPropagation()}>

                {/* Header */}
                <div className="p-4 border-b border-gray-100 flex justify-between items-center bg-gray-50 rounded-t-2xl">
                    <h3 className="font-bold text-gray-900 flex items-center gap-2">
                        {flow === 'menu' && <MessageSquare className="w-5 h-5 text-gray-600" />}
                        {flow === 'link' && <Link className="w-5 h-5 text-blue-600" />}
                        {flow === 'unlink' && <Unlink className="w-5 h-5 text-orange-600" />}
                        {flow === 'correction' && <FileQuestion className="w-5 h-5 text-amber-600" />}
                        {flow === 'missing' && <HelpCircle className="w-5 h-5 text-purple-600" />}
                        {flow === 'other' && <MessageSquare className="w-5 h-5 text-gray-600" />}

                        {flow === 'menu' && "Feedback"}
                        {flow === 'link' && "Report Missing Connection"}
                        {flow === 'unlink' && "Report Incorrect Connection"}
                        {flow === 'correction' && "Report Incorrect Information"}
                        {flow === 'missing' && "Report Missing Data"}
                        {flow === 'other' && "General Feedback"}
                    </h3>
                    <button onClick={onClose}><X className="w-5 h-5 text-gray-400 hover:text-gray-600" /></button>
                </div>

                {success ? (
                    <div className="p-12 text-center flex flex-col items-center gap-4">
                        <div className="w-16 h-16 bg-green-100 rounded-full flex items-center justify-center text-green-600 animate-in zoom-in-50 duration-300">
                            <Check className="w-8 h-8" />
                        </div>
                        <h4 className="text-xl font-bold text-gray-900">Report Submitted!</h4>
                        <p className="text-gray-500">Thank you for helping us improve the data.</p>
                    </div>
                ) : (
                    <div className="flex-1 overflow-y-visible p-6">

                        {/* MENU VIEW */}
                        {flow === 'menu' && (
                            <div className="grid grid-cols-1 gap-3">
                                <button onClick={() => setFlow('link')} className="p-4 rounded-xl border border-gray-200 hover:border-blue-400 hover:bg-blue-50 transition-all flex items-center gap-4 group text-left">
                                    <div className="p-2 bg-blue-100 text-blue-600 rounded-lg group-hover:scale-110 transition-transform"><Link className="w-5 h-5" /></div>
                                    <div className="flex-1">
                                        <div className="font-semibold text-gray-900">Missing Connection</div>
                                        <div className="text-xs text-gray-500 mt-0.5">Two entities should be linked but aren't.</div>
                                    </div>
                                    <ArrowRight className="w-4 h-4 text-gray-300 group-hover:text-blue-500" />
                                </button>

                                <button onClick={() => setFlow('unlink')} className="p-4 rounded-xl border border-gray-200 hover:border-orange-400 hover:bg-orange-50 transition-all flex items-center gap-4 group text-left">
                                    <div className="p-2 bg-orange-100 text-orange-600 rounded-lg group-hover:scale-110 transition-transform"><Unlink className="w-5 h-5" /></div>
                                    <div className="flex-1">
                                        <div className="font-semibold text-gray-900">Incorrect Connection</div>
                                        <div className="text-xs text-gray-500 mt-0.5">Two entities are linked but shouldn't be.</div>
                                    </div>
                                    <ArrowRight className="w-4 h-4 text-gray-300 group-hover:text-orange-500" />
                                </button>

                                <button onClick={() => setFlow('correction')} className="p-4 rounded-xl border border-gray-200 hover:border-amber-400 hover:bg-amber-50 transition-all flex items-center gap-4 group text-left">
                                    <div className="p-2 bg-amber-100 text-amber-600 rounded-lg group-hover:scale-110 transition-transform"><FileQuestion className="w-5 h-5" /></div>
                                    <div className="flex-1">
                                        <div className="font-semibold text-gray-900">Incorrect Information</div>
                                        <div className="text-xs text-gray-500 mt-0.5">Property or business details are wrong.</div>
                                    </div>
                                    <ArrowRight className="w-4 h-4 text-gray-300 group-hover:text-amber-500" />
                                </button>

                                <button onClick={() => setFlow('missing')} className="p-4 rounded-xl border border-gray-200 hover:border-purple-400 hover:bg-purple-50 transition-all flex items-center gap-4 group text-left">
                                    <div className="p-2 bg-purple-100 text-purple-600 rounded-lg group-hover:scale-110 transition-transform"><HelpCircle className="w-5 h-5" /></div>
                                    <div className="flex-1">
                                        <div className="font-semibold text-gray-900">Missing Data</div>
                                        <div className="text-xs text-gray-500 mt-0.5">An entity or record is completely missing.</div>
                                    </div>
                                    <ArrowRight className="w-4 h-4 text-gray-300 group-hover:text-purple-500" />
                                </button>

                                <button onClick={() => setFlow('other')} className="p-4 rounded-xl border border-gray-200 hover:border-gray-400 hover:bg-gray-50 transition-all flex items-center gap-4 group text-left">
                                    <div className="p-2 bg-gray-100 text-gray-600 rounded-lg group-hover:scale-110 transition-transform"><MessageSquare className="w-5 h-5" /></div>
                                    <div className="flex-1">
                                        <div className="font-semibold text-gray-900">Other / General Feedback</div>
                                        <div className="text-xs text-gray-500 mt-0.5">Questions, comments, or UI suggestions.</div>
                                    </div>
                                    <ArrowRight className="w-4 h-4 text-gray-300 group-hover:text-gray-500" />
                                </button>
                            </div>
                        )}

                        {/* FLOW VIEWS */}
                        {flow !== 'menu' && (
                            <div className="space-y-6">
                                {/* Entity Selectors */}
                                {(flow === 'link' || flow === 'unlink') && (
                                    <div className="p-4 bg-gray-50 rounded-xl border border-gray-100">
                                        <h4 className="text-sm font-semibold text-gray-900 mb-4">Which two entities are involved?</h4>
                                        <SearchInput
                                            label="First Entity"
                                            value={entityA}
                                            fieldName="A"
                                            placeholder="Search for property, business, or person..."
                                            activeSearchField={activeSearchField}
                                            setActiveSearchField={setActiveSearchField}
                                            searchTerm={searchTerm}
                                            handleSearch={handleSearch}
                                            searchResults={searchResults}
                                            searching={searching}
                                            selectEntity={selectEntity}
                                            onClear={() => setEntityA(null)}
                                        />
                                        <div className="flex justify-center -my-2 relative z-10">
                                            <div className="p-1.5 bg-white border border-gray-200 rounded-full shadow-sm text-gray-400">
                                                {flow === 'link' ? <Link className="w-3 h-3" /> : <Unlink className="w-3 h-3" />}
                                            </div>
                                        </div>
                                        <SearchInput
                                            label="Second Entity"
                                            value={entityB}
                                            fieldName="B"
                                            placeholder="Search for property, business, or person..."
                                            activeSearchField={activeSearchField}
                                            setActiveSearchField={setActiveSearchField}
                                            searchTerm={searchTerm}
                                            handleSearch={handleSearch}
                                            searchResults={searchResults}
                                            searching={searching}
                                            selectEntity={selectEntity}
                                            onClear={() => setEntityB(null)}
                                        />
                                    </div>
                                )}

                                {flow === 'correction' && (
                                    <div className="p-4 bg-gray-50 rounded-xl border border-gray-100">
                                        <h4 className="text-sm font-semibold text-gray-900 mb-4">Which entity has incorrect info?</h4>
                                        <SearchInput
                                            label="Entity"
                                            value={entityA}
                                            fieldName="single"
                                            placeholder="Search for property, business, or person..."
                                            activeSearchField={activeSearchField}
                                            setActiveSearchField={setActiveSearchField}
                                            searchTerm={searchTerm}
                                            handleSearch={handleSearch}
                                            searchResults={searchResults}
                                            searching={searching}
                                            selectEntity={selectEntity}
                                            onClear={() => setEntityA(null)}
                                        />
                                    </div>
                                )}

                                {/* Description */}
                                <div>
                                    <label className="block text-sm font-medium text-gray-700 mb-2">
                                        {flow === 'link' && "Why should they be linked?"}
                                        {flow === 'unlink' && "Why should they be unlinked?"}
                                        {flow === 'correction' && "What information is incorrect?"}
                                        {flow === 'missing' && "What data is missing? Please provide details."}
                                        {flow === 'other' && "How can we improve?"}
                                    </label>
                                    <textarea
                                        autoFocus={flow === 'missing'}
                                        value={description}
                                        onChange={e => setDescription(e.target.value)}
                                        placeholder="Please provide source URLs or explanation..."
                                        className="w-full h-32 p-3 border border-gray-200 rounded-lg text-sm focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none resize-none"
                                    ></textarea>
                                </div>
                            </div>
                        )}

                    </div>
                )}

                {/* Footer */}
                {!success && (
                    <div className="p-4 border-t border-gray-100 bg-gray-50 flex justify-between items-center rounded-b-2xl">
                        {flow !== 'menu' ? (
                            <button
                                onClick={() => setFlow('menu')}
                                className="text-sm font-medium text-gray-500 hover:text-gray-900 flex items-center gap-1"
                            >
                                <ArrowLeft className="w-4 h-4" /> Back
                            </button>
                        ) : <div></div>}

                        <div className="flex gap-3">
                            <button onClick={onClose} className="px-4 py-2 text-sm font-medium text-gray-600 hover:bg-gray-200 rounded-lg">Cancel</button>

                            {flow !== 'menu' && (
                                <button
                                    onClick={handleSubmit}
                                    disabled={!description || submitting || ((flow === 'link' || flow === 'unlink') && (!entityA && !entityB))}
                                    className={`px-4 py-2 text-sm font-medium text-white rounded-lg flex items-center gap-2 ${(!description || submitting)
                                        ? 'bg-gray-300 cursor-not-allowed'
                                        : 'bg-teal-600 hover:bg-teal-700 shadow-sm'
                                        }`}
                                >
                                    {submitting ? 'Sending...' : (
                                        <>
                                            <span>Submit</span>
                                            <Send className="w-4 h-4" />
                                        </>
                                    )}
                                </button>
                            )}
                        </div>
                    </div>
                )}
            </div>
        </div>
    );
};

export default FeedbackModal;
