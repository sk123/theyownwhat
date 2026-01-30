import React, { useState, useEffect, useMemo } from 'react';
import {
    DndContext,
    closestCenter,
    KeyboardSensor,
    PointerSensor,
    useSensor,
    useSensors,
    DragOverlay,
    useDraggable,
    useDroppable
} from '@dnd-kit/core';
import { CSS } from '@dnd-kit/utilities';
import {
    MapPin,
    Building2,
    Tag,
    Plus,
    MoreVertical,
    Trash2,
    AlertCircle,
    CheckCircle2,
    Image as ImageIcon,
    FileText,
    Layers,
    TrendingUp,
    Briefcase,
    ChevronDown,
    ChevronRight,
    Search,
    Edit2,
    Users,
    User,
    DollarSign
} from 'lucide-react';
import { api } from '../api';
import UnitDetailModal from './UnitDetailModal';
import AddTargetModal from './AddTargetModal';
import AddBuildingModal from './AddBuildingModal';
import AddPropertyModal from './AddPropertyModal';
import AddUnitModal from './AddUnitModal';
import ManageTeamModal from './ManageTeamModal';

// ... 





/*                               ID Generation                                */
/* -------------------------------------------------------------------------- */
// Drag IDs:
// - Single Prop: "prop-{id}"
// - Smart Group: "group-{address}" (Encoded to base64 or safe string)
//
// Drop IDs:
// - Complex: "complex-{id}"
// - Sidebar: "sidebar-drop-zone"

/* -------------------------------------------------------------------------- */
/*                                Sub Components                              */
/* -------------------------------------------------------------------------- */

function TrashDroppable({ alwaysVisible }) {
    const { setNodeRef, isOver } = useDroppable({
        id: 'trash-zone',
        data: { type: 'trash' }
    });

    return (
        <div
            ref={setNodeRef}
            className={`
                fixed bottom-12 left-1/2 -translate-x-1/2 z-[200] 
                w-28 h-28 rounded-full flex flex-col items-center justify-center 
                transition-all duration-500 shadow-2xl border-4
                ${isOver
                    ? 'bg-red-500 scale-125 rotate-6 border-white'
                    : alwaysVisible
                        ? 'bg-slate-900/60 backdrop-blur-xl border-white/40 scale-110'
                        : 'bg-slate-900/20 backdrop-blur-md border-white/10 opacity-40 hover:opacity-100'
                }
            `}
        >
            <Trash2
                size={36}
                className={`transition-colors ${isOver ? 'text-white' : 'text-white/60'}`}
            />
            <span className={`text-[10px] font-black uppercase tracking-widest mt-1 ${isOver ? 'text-white' : 'text-white/40'}`}>
                {isOver ? 'Drop to Remove' : alwaysVisible ? 'Drop Here' : 'Trash'}
            </span>
            {isOver && (
                <div className="absolute -top-16 bg-red-600 text-white text-[11px] font-black px-5 py-2 rounded-full uppercase tracking-widest whitespace-nowrap animate-bounce shadow-2xl ring-4 ring-white/20">
                    Confirm Removal?
                </div>
            )}
        </div>
    );
}


function DraggableProperty({ property, isOverlay, onClick }) {
    const { attributes, listeners, setNodeRef, transform } = useDraggable({
        id: `prop-${property.id}`,
        data: { property, type: 'single' }
    });

    const style = {
        transform: CSS.Translate.toString(transform),
        opacity: isOverlay ? 0.8 : 1,
    };

    if (!property) return null;

    // Use custom overrides if available
    const displayAddress = property.custom_address || property.address;
    const displayUnit = property.custom_unit || property.unit;

    return (
        <div
            ref={setNodeRef}
            style={style}
            {...listeners}
            {...attributes}
            onClick={(e) => {
                // Prevent drag click from triggering immediately if simple click? 
                // Actually dnd-kit handles this well. 
                // We might need to check if it was a drag or click.
                // For now, simple onClick usually works if not dragging.
                if (onClick && !isOverlay) onClick(property);
            }}
            className={`
        p-3 bg-white border border-slate-200 rounded-lg shadow-sm 
        hover:shadow-md hover:border-blue-300 transition-all cursor-grab active:cursor-grabbing
        flex flex-col gap-1 group relative
        ${isOverlay ? 'scale-105 rotate-2 shadow-xl z-50' : ''}
      `}
        >
            <div className="flex justify-between items-start">
                <span className="text-sm font-bold text-slate-800 line-clamp-2 leading-tight">
                    {displayAddress}
                </span>
                {displayUnit && (
                    <span className="text-[10px] font-mono bg-slate-100 text-slate-500 px-1.5 py-0.5 rounded">
                        #{displayUnit}
                    </span>
                )}
            </div>

            {/* In-Card Details */}
            {!isOverlay && (
                <div className="flex flex-col gap-0.5 mt-2 mb-1 border-l-2 border-slate-100 pl-2">
                    {property.owner && (
                        <div className="flex items-center gap-1 min-w-0">
                            <User size={10} className="text-slate-300 shrink-0" />
                            <span className="text-[10px] text-slate-500 font-bold truncate">
                                {property.owner}
                            </span>
                        </div>
                    )}
                    {property.assessed_value && (
                        <div className="flex items-center gap-1 min-w-0">
                            <DollarSign size={10} className="text-emerald-300 shrink-0" />
                            <span className="text-[10px] text-emerald-600 font-black">
                                ${Number(property.assessed_value).toLocaleString()}
                            </span>
                        </div>
                    )}
                </div>
            )}

            <div className={`flex items-center justify-between ${!isOverlay ? 'mt-auto pt-2 border-t border-slate-50' : 'mt-2'}`}>
                <span className="text-[10px] text-slate-400 font-medium uppercase truncate max-w-[100px]">
                    {property.city}
                </span>

                {/* Metadata Indicators */}
                <div className="flex items-center gap-1.5 opacity-60">
                    {(property.notes_count > 0) && <FileText size={10} className="text-blue-500" />}
                    {(property.photos_count > 0) && <ImageIcon size={10} className="text-amber-500" />}
                    {(property.tags_count > 0) && <Tag size={10} className="text-emerald-500" />}
                </div>
            </div>
        </div>
    );
}

function DraggableGroup({ address, properties, isOverlay }) {
    const safeId = useMemo(() => btoa(address).replace(/=/g, ''), [address]);
    const { attributes, listeners, setNodeRef, transform } = useDraggable({
        id: `group-${safeId}`,
        data: { properties, type: 'group' }
    });

    const style = {
        transform: CSS.Translate.toString(transform),
        opacity: isOverlay ? 0.9 : 1,
    };

    return (
        <div
            ref={setNodeRef}
            style={style}
            {...listeners}
            {...attributes}
            className={`
                relative p-3 bg-white border-2 border-slate-300 border-dashed rounded-xl shadow-sm 
                hover:shadow-lg hover:border-blue-400 transition-all cursor-grab active:cursor-grabbing
                flex flex-col gap-1 group
                ${isOverlay ? 'scale-105 rotate-1 shadow-2xl z-50 bg-blue-50 border-blue-400 border-solid' : ''}
            `}
        >
            {/* Stack Effect */}
            {!isOverlay && (
                <div className="absolute inset-x-1 -bottom-1 h-2 bg-slate-100 border border-slate-200 rounded-b-lg -z-10"></div>
            )}

            <div className="flex items-center justify-between">
                <div className="flex items-center gap-2">
                    <Layers size={14} className="text-blue-600" />
                    <span className="text-sm font-black text-slate-800 line-clamp-1">
                        {address}
                    </span>
                </div>
                <span className="text-[10px] font-bold bg-blue-100 text-blue-700 px-2 py-0.5 rounded-full">
                    {properties.length} Units
                </span>
            </div>

            <div className="flex items-center justify-between mt-2 pt-2 border-t border-slate-100">
                <span className="text-[10px] text-slate-400 font-bold uppercase">
                    {properties[0]?.city}
                </span>
                <span className="text-[9px] font-mono text-slate-400">
                    Bulk Group
                </span>
            </div>
        </div>
    );
}

function ComplexDroppable({ complex, properties, onPropertyClick, onRename, onAddUnit }) {
    const { setNodeRef: setDroppableRef, isOver } = useDroppable({
        id: `complex-${complex.id}`,
        data: { complex }
    });
    const [isExpanded, setIsExpanded] = useState(false);
    const [isEditing, setIsEditing] = useState(false);
    const [editName, setEditName] = useState(complex.name);

    const {
        attributes,
        listeners,
        setNodeRef: setDraggableRef,
        transform,
        isDragging
    } = useDraggable({
        id: `complexdrag-${complex.id}`,
        data: { type: 'complex', complex }
    });

    const setNodeRef = (node) => {
        setDroppableRef(node);
        setDraggableRef(node);
    };

    const style = {
        transform: CSS.Translate.toString(transform),
        opacity: isDragging ? 0.5 : 1,
        transition: isDragging ? 'none' : undefined,
        zIndex: isDragging ? 1000 : undefined
    };

    const handleSave = () => {
        if (editName !== complex.name) {
            onRename(complex.id, editName);
        }
        setIsEditing(false);
    };

    return (
        <div
            ref={setNodeRef}
            style={style}
            className={`
                flex flex-col rounded-xl border-2 transition-all duration-200 bg-white
                ${isOver ? 'border-blue-500 bg-blue-50 ring-4 ring-blue-100' : 'border-slate-200 hover:border-blue-200'}
                ${isExpanded ? 'bg-slate-50/50' : ''}
                ${isDragging ? 'shadow-2xl ring-2 ring-blue-400 cursor-grabbing' : ''}
            `}
        >
            {/* Header */}
            <div
                {...listeners}
                {...attributes}
                ref={setDraggableRef}
                className="p-3 flex items-center justify-between cursor-grab active:cursor-grabbing hover:bg-slate-50 rounded-t-xl transition-colors select-none"
                onClick={(e) => {
                    // Prevent toggle if we are clicking an input or button
                    if (!isEditing && !e.target.closest('button') && !e.target.closest('input')) {
                        setIsExpanded(!isExpanded);
                    }
                }}
            >
                <div className="flex items-center gap-2 flex-1 min-w-0">
                    <div className="flex items-center gap-2">
                        <div className={`p-1 rounded hover:bg-slate-200 text-slate-400 transition-colors`}>
                            {isExpanded ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
                        </div>
                        {/* Drag Handle Icon */}
                        <Layers size={14} className="text-slate-300 group-hover:text-blue-400 shrink-0" />
                    </div>

                    <div className="flex flex-col flex-1 min-w-0">
                        {isEditing ? (
                            <input
                                autoFocus
                                type="text"
                                value={editName}
                                onChange={e => setEditName(e.target.value)}
                                onClick={e => e.stopPropagation()}
                                onBlur={handleSave}
                                onKeyDown={e => {
                                    if (e.key === 'Enter') handleSave();
                                    if (e.key === 'Escape') setIsEditing(false);
                                }}
                                className="font-bold text-slate-700 text-sm bg-white border border-blue-300 rounded px-1 outline-none focus:ring-2 focus:ring-blue-100"
                            />
                        ) : (
                            <div className="flex items-center gap-2 group/edit">
                                <h4
                                    className="font-bold text-slate-700 text-sm truncate"
                                    onDoubleClick={(e) => {
                                        e.stopPropagation();
                                        setIsEditing(true);
                                    }}
                                    title="Double click to rename"
                                >
                                    {complex.name}
                                </h4>
                                <button
                                    onClick={(e) => {
                                        e.stopPropagation();
                                        setIsEditing(true);
                                    }}
                                    className="opacity-0 group-hover/edit:opacity-100 p-0.5 rounded hover:bg-slate-200 text-slate-400"
                                >
                                    <Edit2 size={10} />
                                </button>
                            </div>
                        )}
                        <span className="text-[10px] text-slate-400 font-medium">
                            {properties.length} Units • {properties.length > 0 ? properties[0].city : ' Empty'}
                        </span>
                    </div>
                </div>
                <div className="flex items-center gap-2">
                    <button
                        onClick={(e) => {
                            e.stopPropagation();
                            onAddUnit(complex);
                        }}
                        className="p-1 rounded-full bg-slate-100 hover:bg-blue-100 text-slate-400 hover:text-blue-600 transition-colors"
                        title="Add Custom Unit"
                    >
                        <Plus size={14} />
                    </button>
                    <div className={`w-2 h-2 rounded-full bg-${complex.color || 'blue'}-500 shadow-sm flex-shrink-0`}></div>
                </div>
            </div>

            {/* List (Collapsible) */}
            {isExpanded && (
                <div className="p-3 pt-0 flex flex-col gap-2 border-t border-slate-100 mt-1">
                    {properties.map(p => (
                        <DraggableProperty
                            key={p.id}
                            property={p}
                            onClick={onPropertyClick}
                        />
                    ))}
                    {properties.length === 0 && (
                        <div className="text-center text-xs text-slate-400 italic py-4">
                            Drop units here
                        </div>
                    )}
                </div>
            )}
        </div>
    );
}

function StatBanner({ properties, complexes, unassignedCount }) {
    const totalVal = properties.reduce((acc, p) => {
        const valStr = String(p.assessed_value || '0');
        const val = parseFloat(valStr.replace(/[^0-9.]/g, '')) || 0;
        return acc + val;
    }, 0);

    return (
        <div className="bg-white border-b border-gray-200 px-6 py-4 flex flex-col md:flex-row gap-6 items-center shadow-sm z-20 shrink-0">
            <div className="flex items-center gap-3 mr-auto">
                <div className="p-2 bg-blue-50 rounded-lg">
                    <Briefcase className="w-6 h-6 text-blue-600" />
                </div>
                <div>
                    <h2 className="text-lg font-black text-slate-800">Dashboard</h2>
                </div>
            </div>

            <div className="flex items-center gap-8">
                <div className="flex flex-col items-center">
                    <span className="text-[10px] font-bold text-slate-400 uppercase tracking-wider">Total Units</span>
                    <span className="text-xl font-black text-slate-800">{properties.length}</span>
                </div>
                <div className="w-px h-8 bg-slate-100"></div>
                <div className="flex flex-col items-center">
                    <span className="text-[10px] font-bold text-slate-400 uppercase tracking-wider">Complexes</span>
                    <span className="text-xl font-black text-slate-800">{complexes.length}</span>
                </div>
                <div className="w-px h-8 bg-slate-100"></div>
                <div className="flex flex-col items-center">
                    <span className="text-[10px] font-bold text-slate-400 uppercase tracking-wider">Unassigned</span>
                    <span className={`text-xl font-black ${unassignedCount > 0 ? 'text-amber-500' : 'text-slate-300'}`}>
                        {unassignedCount}
                    </span>
                </div>
                <div className="w-px h-8 bg-slate-100"></div>
                <div className="flex flex-col items-end">
                    <span className="text-[10px] font-bold text-slate-400 uppercase tracking-wider">Total Value</span>
                    <span className="text-xl font-black text-emerald-600">
                        ${(totalVal / 1000000).toFixed(1)}M
                    </span>
                </div>
            </div>
        </div>
    );
}

function DraggableSidebarComplex({ complex }) {
    const { attributes, listeners, setNodeRef, transform } = useDraggable({
        id: `complexdrag-${complex.id}`,
        data: { type: 'complex', complex }
    });
    const style = transform ? {
        transform: CSS.Translate.toString(transform),
        zIndex: 999,
    } : undefined;

    return (
        <div ref={setNodeRef} style={style} {...listeners} {...attributes}
            className="p-3 bg-white border border-slate-300 rounded-lg shadow-sm flex items-center gap-3 cursor-grab hover:border-blue-400 group"
        >
            <div className={`w-8 h-8 rounded bg-${complex.color || 'blue'}-100 flex items-center justify-center text-${complex.color || 'blue'}-600`}>
                <Building2 size={16} />
            </div>
            <div className="flex-1 min-w-0">
                <h4 className="font-bold text-slate-700 text-xs truncate">{complex.name}</h4>
                <div className="flex items-center gap-1.5 mt-0.5">
                    <span className="text-[10px] text-slate-400 font-mono bg-slate-100 px-1 rounded">
                        ID: {complex.id}
                    </span>
                </div>
            </div>
            <div className="text-slate-300 group-hover:text-blue-400">
                <Layers size={14} />
            </div>
        </div>
    );
}

function TargetAreaColumn({ target, complexes, children, onRename }) {
    const [isEditing, setIsEditing] = useState(false);
    const [newName, setNewName] = useState(target);

    const { setNodeRef, isOver } = useDroppable({
        id: `target-${target}`,
        data: { type: 'muni', muni: target }
    });

    const handleRename = async () => {
        if (newName && newName !== target) {
            await onRename(target, newName);
        }
        setIsEditing(false);
    };

    return (
        <div
            ref={setNodeRef}
            className={`w-80 flex-shrink-0 flex flex-col h-full transition-colors rounded-xl p-2 -ml-2
                ${isOver ? 'bg-blue-50/50 ring-2 ring-blue-100' : ''}
            `}
        >
            <div className="flex items-center gap-2 px-2 mb-4 group/header">
                {isEditing ? (
                    <input
                        autoFocus
                        value={newName}
                        onChange={e => setNewName(e.target.value)}
                        onBlur={handleRename}
                        onKeyDown={e => e.key === 'Enter' && handleRename()}
                        className="bg-white border-2 border-blue-400 px-2 py-1 rounded-lg text-xs font-black text-slate-700 w-full focus:outline-none shadow-lg"
                    />
                ) : (
                    <>
                        <h3 className="font-black text-slate-400 uppercase tracking-widest text-xs flex items-center gap-2 truncate">
                            <MapPin size={12} />
                            {target}
                        </h3>
                        <button
                            onClick={() => setIsEditing(true)}
                            className="opacity-0 group-hover/header:opacity-60 hover:!opacity-100 transition-opacity text-slate-400"
                        >
                            <Edit2 size={10} />
                        </button>
                    </>
                )}
                <span className="ml-auto bg-slate-200 text-slate-500 px-1.5 py-0.5 rounded text-[9px] font-bold">
                    {complexes.length}
                </span>
            </div>

            <div className="flex-1 overflow-y-auto pr-2 pb-12 flex flex-col gap-4">
                {children}
            </div>
        </div>
    );
}


/* -------------------------------------------------------------------------- */
/*                               Main Component                               */
/* -------------------------------------------------------------------------- */

export default function ToolboxDashboard({ toolboxEnabled }) {
    const [groups, setGroups] = useState([]);
    const [selectedGroup, setSelectedGroup] = useState(null);
    const [properties, setProperties] = useState([]);
    const [complexes, setComplexes] = useState([]);
    const [activeDragData, setActiveDragData] = useState(null); // { property, type, properties (if group) }

    // Modals
    const [selectedPropertyForDetail, setSelectedPropertyForDetail] = useState(null);
    const [showAddProperty, setShowAddProperty] = useState(false);

    // Playground Modals
    const [showAddTarget, setShowAddTarget] = useState(false);
    const [showAddBuilding, setShowAddBuilding] = useState(false);
    const [addBuildingTarget, setAddBuildingTarget] = useState(null); // Muni name
    const [showAddUnit, setShowAddUnit] = useState(false);
    const [addUnitTargetComplex, setAddUnitTargetComplex] = useState(null); // { id, name }
    const [showManageTeam, setShowManageTeam] = useState(false);
    const [extraTargets, setExtraTargets] = useState([]); // Empty columns


    const onAddTarget = (name) => {
        if (!extraTargets.includes(name)) {
            setExtraTargets(prev => [...prev, name]);
        }
    };

    // Debug Cache
    useEffect(() => {
        console.log("TOOLBOX V3 DASHBOARD MOUNTED");
    }, []);

    // Load User's Groups
    useEffect(() => {
        api.get('/groups').then(data => {
            if (data && data.length > 0) {
                setGroups(data);
                setSelectedGroup(data[0]);
            }
        }).catch(err => console.warn("Failed to load groups", err));
    }, []);

    // Load Data for Selected Group
    const loadGroupData = () => {
        if (!selectedGroup) return;
        // Load Properties
        api.get(`/groups/${selectedGroup.id}/properties`).then(setProperties)
            .catch(err => console.error("Properties load failed", err));
        // Load Complexes
        api.get(`/groups/${selectedGroup.id}/complexes`).then(setComplexes)
            .catch(err => console.error("Complexes load failed", err));
    };

    useEffect(() => {
        loadGroupData();
    }, [selectedGroup]);

    // Derived State: Group unassigned props by address
    const unassignedGroups = useMemo(() => {
        const raw = properties.filter(p => !p.complex_id);
        const grouped = {};

        raw.forEach(p => {
            const addr = p.address || "Unknown Address";
            if (!grouped[addr]) grouped[addr] = [];
            grouped[addr].push(p);
        });

        // Convert to array
        return Object.entries(grouped).map(([addr, props]) => ({
            address: addr,
            properties: props,
            isGroup: props.length > 1
        })).sort((a, b) => b.properties.length - a.properties.length); // Biggest groups first
    }, [properties]);

    const propertiesByComplex = useMemo(() => {
        const map = {};
        complexes.forEach(c => map[c.id] = []);
        properties.forEach(p => {
            if (p.complex_id && map[p.complex_id]) {
                map[p.complex_id].push(p);
            }
        });
        return map;
    }, [properties, complexes]);

    // Group Complexes by Municipality (Kanban Columns)
    const complexesByTarget = useMemo(() => {
        const map = {};

        // 1. From Complexes
        complexes.forEach(c => {
            const t = c.municipality || "General";
            if (!map[t]) map[t] = [];
            map[t].push(c);
        });

        // 2. From Extra Targets (Empty Columns)
        extraTargets.forEach(t => {
            if (!map[t]) map[t] = [];
        });

        return map;
    }, [complexes, extraTargets]);

    const sensors = useSensors(
        useSensor(PointerSensor, { activationConstraint: { distance: 8 } }),
        useSensor(KeyboardSensor)
    );

    const handleDragStart = (event) => {
        setActiveDragData(event.active.data.current);
    };

    const handleDragEnd = async (event) => {
        const { active, over } = event;
        const dragType = active.data.current?.type; // 'single', 'group', 'complex'
        setActiveDragData(null);

        if (!over) return;

        // Trash Zone Deletion
        if (over.id === 'trash-zone') {
            const dragItem = active.data.current;
            if (dragType === 'complex') {
                if (confirm(`Are you sure you want to remove the building "${dragItem.complex.name}"? All units will remain in this group but become unassigned.`)) {
                    try {
                        await api.delete(`/groups/${selectedGroup.id}/complexes/${dragItem.complex.id}`);
                        loadGroupData();
                    } catch (err) {
                        alert("Failed to delete building");
                    }
                }
            } else if (dragType === 'single' || dragType === 'group') {
                const ids = dragType === 'single' ? [parseInt(active.id.replace('prop-', ''))] : dragItem.properties.map(p => p.id);
                if (confirm(`Remove ${ids.length} item(s) from this group?`)) {
                    try {
                        for (const id of ids) {
                            await api.delete(`/groups/${selectedGroup.id}/properties/${id}`);
                        }
                        loadGroupData();
                    } catch (err) {
                        alert("Failed to remove items");
                    }
                }
            }
            return;
        }

        // Handle Complex Drag (Sidebar -> Muni)
        if (dragType === 'complex') {
            const complexId = active.data.current.complex.id;
            const targetMuni = over.data.current?.type === 'muni' ? over.data.current.muni : null;

            if (targetMuni) {
                // Optimistic
                setComplexes(prev => prev.map(c => c.id === complexId ? { ...c, municipality: targetMuni } : c));
                try {
                    await api.put(`/groups/${selectedGroup.id}/complexes/${complexId}`, { municipality: targetMuni });
                } catch (err) {
                    console.error("Move complex failed");
                }
            }
            return;
        }

        // Determine destination
        const targetType = over.id.split('-')[0]; // "complex" or "sidebar"
        let newComplexId = null;

        if (targetType === 'complex') {
            newComplexId = parseInt(over.id.replace('complex-', ''));
        } else if (over.id === 'sidebar-drop-zone') {
            newComplexId = null; // Unassign
        } else {
            return; // Dropped somewhere invalid
        }

        // Determine IDs to move
        let idsToMove = [];
        if (dragType === 'single') {
            const propId = parseInt(active.id.replace('prop-', ''));
            idsToMove = [propId];
        } else if (dragType === 'group') {
            // "group-{base64}"
            const groupProps = active.data.current.properties;
            idsToMove = groupProps.map(p => p.id);
        }

        if (idsToMove.length === 0) return;

        // Optimistic Update
        setProperties(prev => prev.map(p => {
            if (idsToMove.includes(p.id)) return { ...p, complex_id: newComplexId };
            return p;
        }));

        // API Call
        try {
            await api.put(`/groups/${selectedGroup.id}/properties/assign`, {
                property_ids: idsToMove,
                complex_id: newComplexId
            });
        } catch (err) {
            console.error("Move failed", err);
            // Revert on fail? (omitted for brevity)
        }
    };

    const handleRenameTarget = async (oldName, newName) => {
        try {
            await api.put(`/groups/${selectedGroup.id}/targets/rename`, { old_name: oldName, new_name: newName });
            loadGroupData();
        } catch (err) {
            alert("Failed to rename target");
        }
    };

    const handleRenameComplex = async (complexId, newName) => {
        // Optimistic
        setComplexes(prev => prev.map(c => c.id === complexId ? { ...c, name: newName } : c));
        try {
            await api.put(`/groups/${selectedGroup.id}/complexes/${complexId}`, { name: newName });
        } catch (err) {
            console.error("Rename failed");
            // Revert?
        }
    };
    const handleCreateCustomUnit = (complex) => {
        setAddUnitTargetComplex(complex);
        setShowAddUnit(true);
    };

    const onAddCustomUnit = async (name) => {
        if (!addUnitTargetComplex || !selectedGroup) return;
        try {
            const res = await api.post(`/groups/${selectedGroup.id}/properties/custom`, {
                name,
                complex_id: addUnitTargetComplex.id
            });
            // res.id is negative according to backend
            const newUnit = {
                id: res.id,
                address: name,
                complex_id: addUnitTargetComplex.id,
                is_custom: true,
                city: "Custom Unit"
            };
            setProperties(prev => [...prev, newUnit]);
        } catch (err) {
            alert("Failed to create custom unit");
        }
    };

    const handleCreateComplex = async (data) => {
        const { type, propertyId, name, target } = data;
        try {
            if (type === 'import') {
                await api.post(`/groups/${selectedGroup.id}/import_building`, {
                    source_property_id: propertyId,
                    target_area: target
                });
            } else {
                await api.post(`/groups/${selectedGroup.id}/complexes`, {
                    name,
                    municipality: target
                });
            }
            loadGroupData();
        } catch (err) {
            alert(type === 'import' ? "Failed to import building" : "Failed to create complex");
        }
    };


    if (!toolboxEnabled) return <div className="p-12 text-center text-slate-400">Toolbox not enabled.</div>;

    const unassignedCount = properties.filter(p => !p.complex_id).length;

    return (
        <DndContext sensors={sensors} collisionDetection={closestCenter} onDragStart={handleDragStart} onDragEnd={handleDragEnd}>
            <div className="flex flex-col h-full bg-slate-50 overflow-hidden relative">

                {/* Stat Banner */}
                {selectedGroup && (
                    <StatBanner
                        properties={properties}
                        complexes={complexes}
                        unassignedCount={unassignedCount}
                    />
                )}

                <div className="flex flex-1 overflow-hidden">
                    {/* Sidebar: Unassigned */}
                    <div className="w-80 bg-white border-r flex flex-col z-10 shadow-sm shrink-0">
                        <div className="p-4 border-b">
                            <label className="text-[10px] font-bold text-slate-400 uppercase tracking-wider mb-1 block">Active Organization</label>
                            <div className="flex items-center gap-2">
                                <select
                                    className="flex-1 text-sm font-bold text-slate-700 bg-slate-50 border-transparent rounded focus:ring-blue-500"
                                    value={selectedGroup?.id || ''}
                                    onChange={e => setSelectedGroup(groups.find(g => g.id === parseInt(e.target.value)))}
                                >
                                    {groups.map(g => <option key={g.id} value={g.id}>{g.name}</option>)}
                                </select>
                                <button
                                    onClick={() => setShowManageTeam(true)}
                                    className="p-1.5 hover:bg-slate-100 text-slate-400 hover:text-blue-500 rounded transition-colors"
                                    title="Manage Team"
                                >
                                    <Users size={16} />
                                </button>
                            </div>
                        </div>

                        <div className="flex-1 overflow-y-auto p-3 flex flex-col gap-6">

                            {/* Unassigned Complexes section */}
                            <div className="flex flex-col gap-2">
                                <div className="flex items-center justify-between">
                                    <h3 className="text-xs font-bold text-slate-400 uppercase">
                                        Buildings ({complexes.filter(c => !c.municipality).length})
                                    </h3>
                                    <button
                                        onClick={() => {
                                            setAddBuildingTarget(null);
                                            setShowAddBuilding(true);
                                        }}
                                        className="p-1 px-2 hover:bg-blue-50 text-blue-600 rounded text-[10px] font-bold flex items-center gap-1"
                                    >
                                        <Plus size={12} /> New
                                    </button>
                                </div>
                                <div className="flex flex-col gap-2 min-h-[50px] bg-slate-50/50 rounded-lg p-2 border border-slate-100">
                                    {complexes.filter(c => !c.municipality).map(c => (
                                        <DraggableSidebarComplex key={c.id} complex={c} />
                                    ))}
                                    {complexes.filter(c => !c.municipality).length === 0 && (
                                        <div className="text-[10px] text-slate-300 text-center py-2">No unassigned buildings</div>
                                    )}
                                </div>
                            </div>

                            {/* Unassigned Units section */}
                            <div className="flex flex-col gap-2">
                                <div className="flex items-center justify-between">
                                    <h3 className="text-xs font-bold text-slate-400 uppercase">
                                        Unassigned Units ({unassignedCount})
                                    </h3>
                                    <button
                                        onClick={() => setShowAddProperty(true)}
                                        className="p-1.5 hover:bg-blue-50 text-blue-600 rounded-lg transition-colors"
                                        title="Add Property"
                                    >
                                        <Plus size={16} />
                                    </button>
                                </div>

                                <div className="bg-slate-50/50 rounded-lg p-2 border border-slate-100 min-h-[100px]">
                                    <SidebarDroppable>
                                        {unassignedGroups.map(group => (
                                            group.isGroup ? (
                                                <DraggableGroup
                                                    key={group.address}
                                                    address={group.address}
                                                    properties={group.properties}
                                                />
                                            ) : (
                                                <DraggableProperty
                                                    key={group.properties[0].id}
                                                    property={group.properties[0]}
                                                    onClick={(p) => setSelectedPropertyForDetail(p)}
                                                />
                                            )
                                        ))}
                                    </SidebarDroppable>
                                </div>
                            </div>
                        </div>
                    </div>

                    {/* Main Board */}
                    <div className="flex-1 overflow-x-auto overflow-y-hidden p-6">
                        <div className="h-full flex gap-6">
                            {/* Columns by Target Area */}
                            {Object.entries(complexesByTarget).sort().map(([target, targetComplexes]) => (
                                <TargetAreaColumn
                                    key={target}
                                    target={target}
                                    complexes={targetComplexes}
                                    onRename={handleRenameTarget}
                                >
                                    {targetComplexes.map(complex => (
                                        <ComplexDroppable
                                            key={complex.id}
                                            complex={complex}
                                            properties={propertiesByComplex[complex.id] || []}
                                            onPropertyClick={(p) => setSelectedPropertyForDetail(p)}
                                            onRename={handleRenameComplex}
                                            onAddUnit={handleCreateCustomUnit}
                                        />
                                    ))}

                                    <button
                                        onClick={() => {
                                            setAddBuildingTarget(target);
                                            setShowAddBuilding(true);
                                        }}
                                        className="w-full py-3 border-2 border-dashed border-slate-200 rounded-xl text-slate-400 hover:text-blue-500 hover:border-blue-300 hover:bg-blue-50 transition-all flex items-center justify-center gap-2 text-sm font-bold mt-2"
                                    >
                                        <Plus size={16} />
                                        Add Building
                                    </button>
                                </TargetAreaColumn>
                            ))}

                            {/* New Target Column */}
                            <div className="w-80 flex-shrink-0 flex flex-col pt-0">
                                <button
                                    onClick={() => setShowAddTarget(true)}
                                    className="w-full h-32 border-2 border-dashed border-slate-200 rounded-xl flex flex-col items-center justify-center gap-2 text-slate-400 hover:text-blue-500 hover:border-blue-300 hover:bg-blue-50 transition-all group"
                                >
                                    <div className="p-3 bg-slate-100 rounded-full group-hover:bg-blue-100 transition-colors">
                                        <Plus size={24} />
                                    </div>
                                    <span className="font-bold text-[10px] uppercase tracking-widest text-slate-400 group-hover:text-blue-500">Add New Area</span>
                                </button>
                            </div>
                        </div>
                    </div>
                </div>
            </div>

            <DragOverlay>
                {activeDragData ? (
                    activeDragData.type === 'group' ? (
                        <DraggableGroup
                            address={activeDragData.properties[0].address}
                            properties={activeDragData.properties}
                            isOverlay
                        />
                    ) : activeDragData.type === 'complex' ? (
                        <div className="p-4 bg-white border-2 border-blue-500 rounded-xl shadow-2xl w-80 scale-105 rotate-2">
                            <div className="flex items-center gap-3">
                                <div className="p-2 bg-blue-50 rounded-lg text-blue-600">
                                    <Building2 size={24} />
                                </div>
                                <div>
                                    <h4 className="font-black text-slate-800 text-sm">{activeDragData.complex.name}</h4>
                                    <span className="text-[10px] font-bold text-slate-400">Moving Entire Building</span>
                                </div>
                            </div>
                        </div>
                    ) : (
                        <DraggableProperty
                            property={activeDragData.property}
                            isOverlay
                        />
                    )
                ) : null}
            </DragOverlay>

            {/* Modals */}
            {
                selectedPropertyForDetail && selectedGroup && (
                    <UnitDetailModal
                        property={selectedPropertyForDetail}
                        group={selectedGroup}
                        onClose={() => setSelectedPropertyForDetail(null)}
                        onUpdate={(updatedProp) => {
                            // Local update
                            setProperties(prev => prev.map(p => p.id === updatedProp.id ? updatedProp : p));
                        }}
                    />
                )
            }

            {
                showAddProperty && selectedGroup && (
                    <AddPropertyModal
                        group={selectedGroup}
                        onClose={() => setShowAddProperty(false)}
                        onAdded={() => {
                            loadGroupData(); // Refresh list to get new property
                        }}
                    />
                )
            }

            {
                showAddUnit && addUnitTargetComplex && (
                    <AddUnitModal
                        isOpen={showAddUnit}
                        onClose={() => setShowAddUnit(false)}
                        onAdd={(name) => onAddCustomUnit(name)}
                        complexName={addUnitTargetComplex.name}
                    />
                )
            }

            {
                showManageTeam && selectedGroup && (
                    <ManageTeamModal
                        isOpen={showManageTeam}
                        onClose={() => setShowManageTeam(false)}
                        groupId={selectedGroup.id}
                    />
                )
            }

            <AddTargetModal
                isOpen={showAddTarget}
                onClose={() => setShowAddTarget(false)}
                onAdd={onAddTarget}
            />

            <AddBuildingModal
                isOpen={showAddBuilding}
                onClose={() => setShowAddBuilding(false)}
                onAdd={handleCreateComplex}
                targetArea={addBuildingTarget}
            />

            <TrashDroppable alwaysVisible={!!activeDragData} />
        </DndContext>
    );
}

function SidebarDroppable({ children }) {
    const { setNodeRef, isOver } = useDroppable({
        id: 'sidebar-drop-zone'
    });

    return (
        <div ref={setNodeRef} className={`flex flex-col gap-2 min-h-[200px] transition-colors rounded-lg ${isOver ? 'bg-slate-100 ring-2 ring-slate-300' : ''}`}>
            {children}
        </div>
    );
}
