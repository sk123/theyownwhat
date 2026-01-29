import React, { useState, useEffect } from 'react';
import { Users, Plus, Building2, Calendar, MessageSquare, ArrowLeft, Loader2, MapPin, ChevronRight, UserPlus, Trash2, Shield, Mail, MoreVertical, GripVertical } from 'lucide-react';
import { DndContext, closestCenter, KeyboardSensor, PointerSensor, useSensor, useSensors } from '@dnd-kit/core';
import { arrayMove, SortableContext, sortableKeyboardCoordinates, rectSortingStrategy, useSortable } from '@dnd-kit/sortable';
import { CSS } from '@dnd-kit/utilities';


function SortableGroupItem({ group, onClick }) {
    const {
        attributes,
        listeners,
        setNodeRef,
        transform,
        transition,
        isDragging
    } = useSortable({ id: group.id });

    const style = {
        transform: CSS.Transform.toString(transform),
        transition,
        zIndex: isDragging ? 50 : 'auto',
        opacity: isDragging ? 0.5 : 1,
    };

    return (
        <div
            ref={setNodeRef}
            style={style}
            className="group relative flex flex-col justify-between bg-white rounded-2xl p-6 border border-slate-200 shadow-sm hover:shadow-lg hover:border-blue-200 transition-all cursor-default"
        >
            {/* Drag Handle */}
            <div
                {...attributes}
                {...listeners}
                className="absolute top-4 right-4 p-1.5 text-slate-300 hover:text-slate-500 cursor-grab active:cursor-grabbing rounded hover:bg-slate-100 transition-colors z-10"
            >
                <GripVertical size={16} />
            </div>

            <div>
                <div className="flex items-center gap-3 mb-4">
                    <div className={`w-10 h-10 rounded-xl flex items-center justify-center text-sm font-bold shadow-sm ${group.role === 'organizer' ? 'bg-indigo-100 text-indigo-700' : 'bg-blue-100 text-blue-700'
                        }`}>
                        {group.name.substring(0, 1).toUpperCase()}
                    </div>
                    <div>
                        <span className="text-[10px] font-bold text-slate-400 uppercase tracking-wider block">
                            {group.role}
                        </span>
                    </div>
                </div>

                <h3 className="text-lg font-bold text-slate-900 mb-2 line-clamp-1 group-hover:text-blue-600 transition-colors">{group.name}</h3>
                <p className="text-sm text-slate-500 line-clamp-2 min-h-[40px] mb-6">
                    {group.description || "No description provided."}
                </p>
            </div>

            <div className="flex items-center justify-between pt-6 border-t border-slate-100">
                <div className="flex items-center gap-4">
                    <div className="flex flex-col items-center group/stat">
                        <Building2 size={16} className="text-slate-300 mb-1 group-hover/stat:text-blue-500 transition-colors" />
                        <span className="text-[10px] font-bold text-slate-500">{group.property_count || 0} PROPS</span>
                    </div>
                    <div className="flex flex-col items-center group/stat">
                        <Users size={16} className="text-slate-300 mb-1 group-hover/stat:text-emerald-500 transition-colors" />
                        <span className="text-[10px] font-bold text-slate-500">{group.member_count || 1} MEMS</span>
                    </div>
                    <div className="flex flex-col items-center group/stat">
                        <MessageSquare size={16} className="text-slate-300 mb-1 group-hover/stat:text-purple-500 transition-colors" />
                        <span className="text-[10px] font-bold text-slate-500">0 NOTES</span>
                    </div>
                </div>

                <button
                    onClick={() => onClick(group)}
                    className="bg-slate-900 text-white text-xs font-bold px-4 py-2 rounded-lg hover:bg-blue-600 transition-colors shadow-lg shadow-slate-900/10 hover:shadow-blue-600/20"
                >
                    Open Group
                </button>
            </div>
        </div>
    );
}

export default function ToolboxDashboard({ onBack }) {
    const [groups, setGroups] = useState([]);
    const [loading, setLoading] = useState(true);
    const [selectedGroup, setSelectedGroup] = useState(null);
    const [groupProperties, setGroupProperties] = useState([]);
    const [loadingProps, setLoadingProps] = useState(false);

    const [showCreateModal, setShowCreateModal] = useState(false);
    const [newGroupName, setNewGroupName] = useState('');
    const [newGroupDesc, setNewGroupDesc] = useState('');

    const [groupMembers, setGroupMembers] = useState([]);
    const [loadingMembers, setLoadingMembers] = useState(false);
    const [showInviteModal, setShowInviteModal] = useState(false);
    const [inviteEmail, setInviteEmail] = useState('');
    const [inviteRole, setInviteRole] = useState('member');
    const [inviteError, setInviteError] = useState('');

    const sensors = useSensors(
        useSensor(PointerSensor),
        useSensor(KeyboardSensor, {
            coordinateGetter: sortableKeyboardCoordinates,
        })
    );

    useEffect(() => {
        fetchGroups();
    }, []);

    const fetchGroups = async () => {
        setLoading(true);
        try {
            const res = await fetch('/api/groups');
            const data = await res.json();

            // Restore order from localStorage
            const savedOrder = localStorage.getItem('groupOrder');
            if (savedOrder) {
                const orderMap = JSON.parse(savedOrder);
                // orderMap is id -> index, or just an array of IDs
                // Assuming array of IDs for simplicity
                if (Array.isArray(orderMap)) {
                    data.sort((a, b) => {
                        const idxA = orderMap.indexOf(a.id);
                        const idxB = orderMap.indexOf(b.id);
                        if (idxA === -1) return 1;
                        if (idxB === -1) return -1;
                        return idxA - idxB;
                    });
                }
            }

            setGroups(data);
        } catch (err) {
            console.error("Failed to fetch groups", err);
        } finally {
            setLoading(false);
        }
    };

    const handleDragEnd = (event) => {
        const { active, over } = event;

        if (active.id !== over.id) {
            setGroups((items) => {
                const oldIndex = items.findIndex((i) => i.id === active.id);
                const newIndex = items.findIndex((i) => i.id === over.id);
                const newOrder = arrayMove(items, oldIndex, newIndex);

                // Save new order
                const orderIds = newOrder.map(g => g.id);
                localStorage.setItem('groupOrder', JSON.stringify(orderIds));

                return newOrder;
            });
        }
    };

    const fetchGroupProperties = async (groupId) => {
        setLoadingProps(true);
        try {
            const res = await fetch(`/api/groups/${groupId}/properties`);
            const data = await res.json();
            setGroupProperties(data);
        } catch (err) {
            console.error("Failed to fetch group properties", err);
        } finally {
            setLoadingProps(false);
        }
    };

    const handleOpenGroup = (group) => {
        setSelectedGroup(group);
        fetchGroupProperties(group.id);
        fetchGroupMembers(group.id);
    };

    const fetchGroupMembers = async (groupId) => {
        setLoadingMembers(true);
        try {
            const res = await fetch(`/api/groups/${groupId}/members`);
            const data = await res.json();
            setGroupMembers(Array.isArray(data) ? data : []);
        } catch (err) {
            console.error("Failed to fetch group members", err);
        } finally {
            setLoadingMembers(false);
        }
    };

    const handleInviteMember = async (e) => {
        e.preventDefault();
        setInviteError('');
        try {
            const res = await fetch(`/api/groups/${selectedGroup.id}/members`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ email: inviteEmail, role: inviteRole })
            });
            if (res.ok) {
                setInviteEmail('');
                setShowInviteModal(false);
                fetchGroupMembers(selectedGroup.id);
            } else {
                const errData = await res.json();
                setInviteError(errData.detail || "Failed to invite user");
            }
        } catch (err) {
            setInviteError("Connection error");
        }
    };

    const handleUpdateMemberRole = async (userId, newRole) => {
        try {
            const res = await fetch(`/api/groups/${selectedGroup.id}/members/${userId}`, {
                method: 'PATCH',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ role: newRole })
            });
            if (res.ok) {
                fetchGroupMembers(selectedGroup.id);
            }
        } catch (err) {
            console.error("Failed to update role", err);
        }
    };

    const handleRemoveMember = async (userId) => {
        if (!confirm("Are you sure you want to remove this member?")) return;
        try {
            const res = await fetch(`/api/groups/${selectedGroup.id}/members/${userId}`, {
                method: 'DELETE'
            });
            if (res.ok) {
                if (userId === selectedGroup.user_id) { // Removing self
                    setSelectedGroup(null);
                    fetchGroups();
                } else {
                    fetchGroupMembers(selectedGroup.id);
                }
            }
        } catch (err) {
            console.error("Failed to remove member", err);
        }
    };

    const handleCreateGroup = async (e) => {
        e.preventDefault();
        try {
            const res = await fetch('/api/groups', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ name: newGroupName, description: newGroupDesc })
            });
            if (res.ok) {
                setNewGroupName('');
                setNewGroupDesc('');
                setShowCreateModal(false);
                fetchGroups();
            }
        } catch (err) {
            console.error("Failed to create group", err);
        }
    };

    if (selectedGroup) {
        return (
            <div className="flex flex-col h-full bg-slate-50">
                <div className="bg-white border-b border-slate-200 px-6 py-4 flex items-center justify-between">
                    <div className="flex items-center gap-4">
                        <button
                            onClick={() => setSelectedGroup(null)}
                            className="p-2 hover:bg-slate-100 rounded-lg transition-colors text-slate-500"
                        >
                            <ArrowLeft size={20} />
                        </button>
                        <div>
                            <h2 className="text-xl font-bold text-slate-900">{selectedGroup.name}</h2>
                            <p className="text-xs text-slate-500 font-medium">Group Details & Properties</p>
                        </div>
                    </div>
                    <div className="flex items-center gap-2">
                        <span className="px-2 py-1 bg-blue-50 text-blue-600 text-[10px] font-bold uppercase tracking-wider rounded">
                            {selectedGroup.role}
                        </span>
                    </div>
                </div>

                <div className="flex-1 overflow-y-auto p-6">
                    <div className="max-w-4xl mx-auto space-y-6">
                        {/* Group Stats/Info */}
                        <div className="bg-white rounded-2xl border border-slate-200 p-6 shadow-sm">
                            <h3 className="text-sm font-bold text-slate-400 uppercase tracking-widest mb-4">About this Group</h3>
                            <p className="text-slate-700 font-medium leading-relaxed">
                                {selectedGroup.description || "No description provided for this group."}
                            </p>
                        </div>

                        {/* Members Section */}
                        <div className="bg-white rounded-2xl border border-slate-200 p-6 shadow-sm">
                            <div className="flex items-center justify-between mb-6">
                                <div className="flex items-center gap-2">
                                    <h3 className="text-sm font-bold text-slate-400 uppercase tracking-widest">Team Members</h3>
                                    <span className="bg-slate-100 text-slate-500 text-[10px] font-bold px-2 py-0.5 rounded-full">
                                        {groupMembers.length}
                                    </span>
                                </div>
                                {selectedGroup.role === 'organizer' && (
                                    <button
                                        onClick={() => setShowInviteModal(true)}
                                        className="text-xs font-bold text-blue-600 hover:bg-blue-50 px-3 py-1.5 rounded-lg flex items-center gap-1.5 transition-colors"
                                    >
                                        <UserPlus size={14} /> Invite
                                    </button>
                                )}
                            </div>

                            {loadingMembers ? (
                                <div className="flex items-center justify-center py-4 text-slate-400 gap-2">
                                    <Loader2 className="animate-spin" size={16} />
                                    <span className="text-sm font-medium">Loading members...</span>
                                </div>
                            ) : (
                                <div className="space-y-3">
                                    {groupMembers.map(member => (
                                        <div key={member.id} className="flex items-center justify-between p-3 rounded-xl bg-slate-50 border border-slate-100 group/member">
                                            <div className="flex items-center gap-3">
                                                <img
                                                    src={member.picture_url || `https://api.dicebear.com/7.x/initials/svg?seed=${member.full_name}`}
                                                    className="w-8 h-8 rounded-full bg-white border border-slate-200"
                                                    alt={member.full_name}
                                                />
                                                <div className="min-w-0">
                                                    <div className="text-sm font-bold text-slate-900 truncate">
                                                        {member.full_name}
                                                    </div>
                                                    <div className="text-[10px] text-slate-500 truncate">{member.email}</div>
                                                </div>
                                            </div>
                                            <div className="flex items-center gap-3">
                                                <div className="flex flex-col items-end">
                                                    <span className={`text-[9px] font-black uppercase px-2 py-0.5 rounded ${member.role === 'organizer' ? 'bg-indigo-50 text-indigo-600' : 'bg-slate-200 text-slate-600'
                                                        }`}>
                                                        {member.role}
                                                    </span>
                                                </div>
                                                {selectedGroup.role === 'organizer' && (
                                                    <div className="flex items-center gap-1 opacity-100 sm:opacity-0 group-hover/member:opacity-100 transition-opacity">
                                                        <select
                                                            className="text-[10px] font-bold bg-transparent border-none focus:ring-0 text-slate-400 cursor-pointer hover:text-blue-600 outline-none"
                                                            value={member.role}
                                                            onChange={(e) => handleUpdateMemberRole(member.id, e.target.value)}
                                                        >
                                                            <option value="member">Member</option>
                                                            <option value="organizer">Organizer</option>
                                                        </select>
                                                        <button
                                                            onClick={() => handleRemoveMember(member.id)}
                                                            className="p-1.5 text-slate-300 hover:text-red-500 rounded-lg transition-colors"
                                                            title="Remove member"
                                                        >
                                                            <Trash2 size={14} />
                                                        </button>
                                                    </div>
                                                )}
                                            </div>
                                        </div>
                                    ))}
                                </div>
                            )}
                        </div>

                        {/* Properties List */}
                        <div className="space-y-4">
                            <div className="flex items-center justify-between">
                                <h3 className="text-sm font-bold text-slate-900 uppercase tracking-widest flex items-center gap-2">
                                    <Building2 size={16} /> Associated Properties
                                </h3>
                                <span className="text-xs font-bold bg-slate-100 text-slate-500 px-2 py-0.5 rounded-full">
                                    {groupProperties.length} Assets
                                </span>
                            </div>

                            {loadingProps ? (
                                <div className="h-32 flex items-center justify-center gap-2 text-slate-400">
                                    <Loader2 className="animate-spin" size={20} />
                                    <span className="font-medium">Loading properties...</span>
                                </div>
                            ) : groupProperties.length === 0 ? (
                                <div className="bg-white rounded-2xl border-2 border-dashed border-slate-200 p-12 text-center">
                                    <p className="text-slate-500 text-sm font-medium">No properties added yet.</p>
                                    <p className="text-[10px] text-slate-400 mt-1">Search for a property in the home view and click "Add to Group" to see it here.</p>
                                </div>
                            ) : (
                                <div className="grid grid-cols-1 gap-3">
                                    {groupProperties.map(prop => (
                                        <div key={prop.id} className="bg-white p-4 rounded-xl border border-slate-200 shadow-sm hover:border-blue-200 transition-colors flex items-center justify-between group">
                                            <div className="flex items-center gap-4 min-w-0">
                                                <div className="w-10 h-10 bg-slate-50 rounded-lg flex items-center justify-center text-slate-400 group-hover:bg-blue-50 group-hover:text-blue-500 transition-colors">
                                                    <MapPin size={20} />
                                                </div>
                                                <div className="min-w-0">
                                                    <div className="text-sm font-bold text-slate-900 truncate">{prop.location}</div>
                                                    <div className="flex items-center gap-2 mt-0.5">
                                                        <span className="text-[10px] font-bold text-slate-400 uppercase">{prop.property_city}</span>
                                                        <span className="text-[10px] text-slate-300">•</span>
                                                        <span className="text-[10px] text-slate-500 truncate">{prop.owner}</span>
                                                    </div>
                                                </div>
                                            </div>
                                            <div className="flex items-center gap-6">
                                                <div className="hidden sm:flex flex-col items-end">
                                                    <span className="text-xs font-mono font-bold text-slate-700">${prop.assessed_value}</span>
                                                    <span className="text-[9px] text-slate-400 uppercase font-bold tracking-tighter">Assessed</span>
                                                </div>
                                                <ChevronRight size={20} className="text-slate-300 group-hover:text-blue-500 transition-colors" />
                                            </div>
                                        </div>
                                    ))}
                                </div>
                            )}
                        </div>
                    </div>
                </div>
            </div>
        );
    }

    return (
        <div className="flex flex-col h-full bg-slate-50">
            {/* Header */}
            <div className="bg-white border-b border-slate-200 px-6 py-4 flex items-center justify-between">
                <div className="flex items-center gap-4">
                    <button
                        onClick={onBack}
                        className="p-2 hover:bg-slate-100 rounded-lg transition-colors text-slate-500"
                    >
                        <ArrowLeft size={20} />
                    </button>
                    <div>
                        <h2 className="text-xl font-bold text-slate-900">Organizer Dashboard</h2>
                        <p className="text-xs text-slate-500 font-medium">Manage your tenant groups and properties</p>
                    </div>
                </div>
                <button
                    onClick={() => setShowCreateModal(true)}
                    className="flex items-center gap-2 bg-blue-600 hover:bg-blue-700 text-white px-4 py-2 rounded-xl font-bold text-sm shadow-lg shadow-blue-500/20 transition-all"
                >
                    <Plus size={18} />
                    New Group
                </button>
            </div>

            <div className="flex-1 overflow-y-auto p-6">
                {loading ? (
                    <div className="h-64 flex flex-col items-center justify-center text-slate-400 gap-3">
                        <Loader2 className="animate-spin" size={32} />
                        <p className="font-medium">Loading your groups...</p>
                    </div>
                ) : groups.length === 0 ? (
                    <div className="max-w-md mx-auto mt-12 text-center p-8 bg-white rounded-2xl border-2 border-dashed border-slate-200">
                        <div className="w-16 h-16 bg-slate-50 rounded-2xl flex items-center justify-center mx-auto mb-4">
                            <Users className="text-slate-400" size={32} />
                        </div>
                        <h3 className="text-lg font-bold text-slate-900 mb-2">No Groups Yet</h3>
                        <p className="text-slate-500 text-sm mb-6 leading-relaxed">
                            Create your first group to start organizing tenants and tracking property conditions together.
                        </p>
                        <button
                            onClick={() => setShowCreateModal(true)}
                            className="text-blue-600 font-bold text-sm hover:underline"
                        >
                            + Create a Group
                        </button>
                    </div>
                ) : (
                    <DndContext
                        sensors={sensors}
                        collisionDetection={closestCenter}
                        onDragEnd={handleDragEnd}
                    >
                        <SortableContext items={groups} strategy={rectSortingStrategy}>
                            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
                                {groups.map(group => (
                                    <SortableGroupItem
                                        key={group.id}
                                        group={group}
                                        onClick={handleOpenGroup}
                                    />
                                ))}
                                <button
                                    onClick={() => setShowCreateModal(true)}
                                    className="bg-white rounded-2xl border-2 border-dashed border-slate-200 hover:border-blue-400 hover:bg-blue-50/50 transition-all flex flex-col items-center justify-center p-6 group h-full min-h-[280px]"
                                >
                                    <div className="w-12 h-12 bg-blue-50 rounded-xl flex items-center justify-center text-blue-600 mb-4 group-hover:bg-blue-100 group-hover:scale-110 transition-all">
                                        <Plus size={24} />
                                    </div>
                                    <span className="text-lg font-bold text-slate-900 mb-1">Create New Group</span>
                                    <span className="text-sm text-slate-500">Add another organizing bucket</span>
                                </button>
                            </div>
                        </SortableContext>
                    </DndContext>
                )}
            </div>

            {/* Create Group Modal */}
            {showCreateModal && (
                <div className="fixed inset-0 z-[100] flex items-center justify-center p-4 bg-slate-900/40 backdrop-blur-sm">
                    <div className="bg-white rounded-3xl w-full max-w-md shadow-2xl overflow-hidden border border-white">
                        <div className="p-8">
                            <h3 className="text-2xl font-black text-slate-900 tracking-tight mb-2">Create New Group</h3>
                            <p className="text-slate-500 text-sm font-medium mb-6">Give your organizing effort a name and description.</p>

                            <form onSubmit={handleCreateGroup} className="space-y-4">
                                <div>
                                    <label className="block text-[10px] font-bold text-slate-400 uppercase tracking-widest mb-1.5 ml-1">Group Name</label>
                                    <input
                                        type="text"
                                        className="w-full px-4 py-3 bg-slate-50 border border-slate-200 rounded-xl focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none transition-all font-medium"
                                        placeholder="e.g. Hartford Tenants Union"
                                        value={newGroupName}
                                        onChange={e => setNewGroupName(e.target.value)}
                                        required
                                    />
                                </div>
                                <div>
                                    <label className="block text-[10px] font-bold text-slate-400 uppercase tracking-widest mb-1.5 ml-1">Description</label>
                                    <textarea
                                        className="w-full px-4 py-3 bg-slate-50 border border-slate-200 rounded-xl focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none transition-all font-medium h-24 resize-none"
                                        placeholder="What is this group advocating for?"
                                        value={newGroupDesc}
                                        onChange={e => setNewGroupDesc(e.target.value)}
                                    ></textarea>
                                </div>

                                <div className="flex gap-3 pt-4">
                                    <button
                                        type="button"
                                        onClick={() => setShowCreateModal(false)}
                                        className="flex-1 py-3 border border-slate-200 text-slate-600 font-bold rounded-xl hover:bg-slate-50 transition-colors"
                                    >
                                        Cancel
                                    </button>
                                    <button
                                        type="submit"
                                        className="flex-1 py-3 bg-blue-600 text-white font-bold rounded-xl shadow-lg shadow-blue-500/20 hover:bg-blue-700 transition-colors"
                                    >
                                        Create Group
                                    </button>
                                </div>
                            </form>
                        </div>
                    </div>
                </div>
            )}

            {/* Invite Member Modal */}
            {showInviteModal && (
                <div className="fixed inset-0 z-[100] flex items-center justify-center p-4 bg-slate-900/40 backdrop-blur-sm">
                    <div className="bg-white rounded-3xl w-full max-w-md shadow-2xl overflow-hidden border border-white">
                        <div className="p-8">
                            <div className="flex items-center justify-between mb-2">
                                <h3 className="text-2xl font-black text-slate-900 tracking-tight">Invite Member</h3>
                                <button onClick={() => setShowInviteModal(false)} className="text-slate-400 hover:text-slate-600">
                                    <Plus className="rotate-45" size={24} />
                                </button>
                            </div>
                            <p className="text-slate-500 text-sm font-medium mb-6">Add a collaborator to this group by their email address.</p>

                            <form onSubmit={handleInviteMember} className="space-y-4">
                                <div>
                                    <label className="block text-[10px] font-bold text-slate-400 uppercase tracking-widest mb-1.5 ml-1">Email Address</label>
                                    <div className="relative">
                                        <Mail className="absolute left-4 top-1/2 -translate-y-1/2 text-slate-400" size={18} />
                                        <input
                                            type="email"
                                            className="w-full pl-11 pr-4 py-3 bg-slate-50 border border-slate-200 rounded-xl focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none transition-all font-medium"
                                            placeholder="colleague@example.com"
                                            value={inviteEmail}
                                            onChange={e => setInviteEmail(e.target.value)}
                                            required
                                        />
                                    </div>
                                </div>
                                <div>
                                    <label className="block text-[10px] font-bold text-slate-400 uppercase tracking-widest mb-1.5 ml-1">Initial Role</label>
                                    <div className="grid grid-cols-2 gap-2">
                                        <button
                                            type="button"
                                            onClick={() => setInviteRole('member')}
                                            className={`py-3 rounded-xl border font-bold text-xs transition-all ${inviteRole === 'member'
                                                ? 'bg-blue-600 text-white border-blue-600 shadow-lg shadow-blue-500/20'
                                                : 'bg-slate-50 border-slate-200 text-slate-500 hover:bg-slate-100'
                                                }`}
                                        >
                                            Member
                                        </button>
                                        <button
                                            type="button"
                                            onClick={() => setInviteRole('organizer')}
                                            className={`py-3 rounded-xl border font-bold text-xs transition-all ${inviteRole === 'organizer'
                                                ? 'bg-blue-600 text-white border-blue-600 shadow-lg shadow-blue-500/20'
                                                : 'bg-slate-50 border-slate-200 text-slate-500 hover:bg-slate-100'
                                                }`}
                                        >
                                            Organizer
                                        </button>
                                    </div>
                                </div>

                                {inviteError && (
                                    <p className="text-red-500 text-[10px] font-bold bg-red-50 p-2 rounded-lg border border-red-100">
                                        {inviteError}
                                    </p>
                                )}

                                <div className="flex gap-3 pt-4">
                                    <button
                                        type="button"
                                        onClick={() => setShowInviteModal(false)}
                                        className="flex-1 py-3 border border-slate-200 text-slate-600 font-bold rounded-xl hover:bg-slate-50 transition-colors"
                                    >
                                        Cancel
                                    </button>
                                    <button
                                        type="submit"
                                        className="flex-1 py-3 bg-blue-600 text-white font-bold rounded-xl shadow-lg shadow-blue-500/20 hover:bg-blue-700 transition-colors flex items-center justify-center gap-2"
                                    >
                                        <UserPlus size={18} />
                                        Send Invite
                                    </button>
                                </div>
                            </form>
                        </div>
                    </div>
                </div>
            )}
        </div>
    );
}
