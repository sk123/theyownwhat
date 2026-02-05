import { useState, useEffect } from 'react';
import { X, MessageSquare, AlertTriangle, Send, Search, Check, Link, Unlink, FileQuestion, HelpCircle, ArrowRight, ArrowLeft } from 'lucide-react';

const FeedbackModal = ({ isOpen, onClose, initialEntity = null }) => {
    // Flow state: 'menu' | 'link' | 'unlink' | 'correction' | 'missing'
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
                // If opened with context, default to correction flow but allowing navigation back?
                // Actually, let's just pre-fill entity A if they choose a relevant flow
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
    };

    const handleSearch = async (term) => {
        setSearchTerm(term);
        // Updated to matched backend's minimum length requirement of 3
        if (term.length < 3) {
            setSearchResults([]);
            return;
        }
        setSearching(true);
        try {
            // Updated to match backend signature: type (required), term (required)
            const res = await fetch(`/api/search?type=all&term=${encodeURIComponent(term)}`);
            if (!res.ok) throw new Error(`Search failed: ${res.status}`);
            const data = await res.json();
            setSearchResults(data || []);
        } catch (e) {
            console.error("Search failed", e);
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

    // --- Sub-Components ---

    const SearchInput = ({ label, value, fieldName, placeholder }) => (
        <div className="mb-4">
            <label className="block text-xs font-bold text-gray-500 uppercase tracking-wide mb-1.5">{label}</label>

            {value ? (
                <div className="flex items-center justify-between p-3 bg-blue-50 border border-blue-200 rounded-lg text-blue-800">
                    <div className="flex items-center gap-2 overflow-hidden">
                        <Check className="w-4 h-4 text-blue-600 flex-shrink-0" />
                        {/* Updated to prefer 'name' as per backend SearchResult model */}
                        <span className="font-medium truncate">{value.name || value.title}</span>
                        <span className="text-xs opacity-70 border-l border-blue-200 pl-2 ml-1">{value.type}</span>
                    </div>
                    <button
                        onClick={() => {
                            if (fieldName === 'A') setEntityA(null);
                            if (fieldName === 'B') setEntityB(null);
                            if (fieldName === 'single') setEntityA(null);
                        }}
                        className="p-1 hover:bg-blue-100 rounded-full"
                    >
                        <X className="w-4 h-4" />
                    </button>
                </div>
            ) : (
                <div className="relative">
                    {activeSearchField === fieldName ? (
                        <>
                            <Search className="absolute left-3 top-3 w-4 h-4 text-blue-500" />
                            <input
                                autoFocus
                                type="text"
                                value={searchTerm}
                                onChange={e => handleSearch(e.target.value)}
                                placeholder="Search for property, business, or person..."
                                className="w-full pl-9 pr-9 py-2.5 border border-blue-500 ring-2 ring-blue-100 rounded-lg text-sm bg-white outline-none transition-all"
                                onBlur={() => setTimeout(() => setActiveSearchField(null), 200)} // Delay to allow click
                            />
                            <button
                                onMouseDown={() => setActiveSearchField(null)}
                                className="absolute right-3 top-3 text-gray-400 hover:text-gray-600"
                            >
                                <X className="w-4 h-4" />
                            </button>

                            {/* Dropdown */}
                            {(searchResults.length > 0 || searching) && (
                                <div className="absolute top-full left-0 right-0 mt-1 bg-white border border-gray-100 rounded-lg shadow-xl max-h-48 overflow-y-auto z-10">
                                    {searching && <div className="p-3 text-xs text-center text-gray-400">Searching...</div>}
                                    {searchResults.map(res => (
                                        <div
                                            key={res.id}
                                            onMouseDown={() => selectEntity(res)} // Use onMouseDown to trigger before blur
                                            className="p-3 hover:bg-blue-50 cursor-pointer flex items-center justify-between border-b border-gray-50 last:border-0"
                                        >
                                            <div className="truncate">
                                                {/* Updated to usage 'name' field */}
                                                <div className="text-sm font-medium text-gray-900">{res.name}</div>
                                                <div className="text-xs text-gray-500">{res.type}</div>
                                            </div>
                                        </div>
                                    ))}
                                </div>
                            )}
                        </>
                    ) : (
                        <div
                            onClick={() => { setActiveSearchField(fieldName); setSearchTerm(''); }}
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

    if (!isOpen) return null;

    return (
        // Increased z-index to 100 to ensure it sits above the sticky header (z-50)
        <div className="fixed inset-0 z-[100] flex items-center justify-center p-4 bg-black/60 backdrop-blur-sm" onClick={onClose}>
            <div className="bg-white rounded-2xl shadow-2xl w-full max-w-lg overflow-hidden flex flex-col max-h-[90vh]" onClick={e => e.stopPropagation()}>

                {/* Header */}
                <div className="p-4 border-b border-gray-100 flex justify-between items-center bg-gray-50">
                    <h3 className="font-bold text-gray-900 flex items-center gap-2">
                        {flow === 'menu' && <MessageSquare className="w-5 h-5 text-gray-600" />}
                        {flow === 'link' && <Link className="w-5 h-5 text-blue-600" />}
                        {flow === 'unlink' && <Unlink className="w-5 h-5 text-orange-600" />}
                        {flow === 'correction' && <FileQuestion className="w-5 h-5 text-amber-600" />}
                        {flow === 'missing' && <HelpCircle className="w-5 h-5 text-purple-600" />}

                        {flow === 'menu' && "Report Data Issue"}
                        {flow === 'link' && "Report Missing Connection"}
                        {flow === 'unlink' && "Report Incorrect Connection"}
                        {flow === 'correction' && "Report Incorrect Information"}
                        {flow === 'missing' && "Report Missing Data"}
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
                    <div className="flex-1 overflow-y-auto p-6">

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
                    <div className="p-4 border-t border-gray-100 bg-gray-50 flex justify-between items-center">
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
