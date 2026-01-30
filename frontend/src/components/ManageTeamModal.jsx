import React, { useState, useEffect } from 'react';
import { Users, X, UserPlus, Shield, Mail, Trash2, Search, Loader2, Check } from 'lucide-react';
import { api } from '../api';

export default function ManageTeamModal({ isOpen, onClose, groupId }) {
    const [members, setMembers] = useState([]);
    const [loading, setLoading] = useState(true);
    const [inviteEmail, setInviteEmail] = useState('');
    const [inviting, setInviting] = useState(false);
    const [searchQuery, setSearchQuery] = useState('');
    const [searchResults, setSearchResults] = useState([]);
    const [searching, setSearching] = useState(false);

    useEffect(() => {
        if (isOpen && groupId) {
            loadMembers();
        }
    }, [isOpen, groupId]);

    const loadMembers = async () => {
        setLoading(true);
        try {
            const data = await api.get(`/groups/${groupId}/members`);
            setMembers(data);
        } catch (err) {
            console.error("Failed to load members", err);
        } finally {
            setLoading(false);
        }
    };

    const handleInvite = async (email, role = 'member') => {
        setInviting(true);
        try {
            await api.post(`/groups/${groupId}/members`, { email, role });
            setInviteEmail('');
            setSearchResults([]);
            loadMembers();
        } catch (err) {
            alert(err.message || "Failed to invite member");
        } finally {
            setInviting(false);
        }
    };

    const handleUpdateRole = async (userId, newRole) => {
        try {
            await api.patch(`/groups/${groupId}/members/${userId}`, { role: newRole });
            loadMembers();
        } catch (err) {
            alert("Failed to update role");
        }
    };

    const handleRemoveMember = async (userId) => {
        if (!confirm("Are you sure you want to remove this member?")) return;
        try {
            await api.delete(`/groups/${groupId}/members/${userId}`);
            loadMembers();
        } catch (err) {
            alert("Failed to remove member");
        }
    };

    const handleSearch = async (e) => {
        const val = e.target.value;
        setInviteEmail(val);
        if (val.length > 2) {
            setSearching(true);
            try {
                const results = await api.get(`/users/search?email=${val}`);
                setSearchResults(results);
            } catch (err) {
                console.error("Search failed", err);
            } finally {
                setSearching(false);
            }
        } else {
            setSearchResults([]);
        }
    };

    if (!isOpen) return null;

    return (
        <div className="fixed inset-0 z-[110] flex items-center justify-center p-4 bg-slate-900/60 backdrop-blur-sm">
            <div className="bg-white rounded-2xl shadow-2xl w-full max-w-2xl h-[80vh] flex flex-col overflow-hidden animate-in fade-in zoom-in duration-200">
                <div className="p-4 border-b border-slate-100 flex items-center justify-between bg-slate-50/50">
                    <div className="flex items-center gap-3">
                        <div className="w-10 h-10 bg-blue-100 rounded-xl flex items-center justify-center text-blue-600">
                            <Users size={20} />
                        </div>
                        <div>
                            <h3 className="font-black text-slate-700 text-lg leading-tight">Manage Team</h3>
                            <p className="text-[10px] text-slate-400 font-bold uppercase tracking-wider">Group Collaboration</p>
                        </div>
                    </div>
                    <button onClick={onClose} className="p-2 hover:bg-slate-200 rounded-full transition-colors text-slate-400">
                        <X size={20} />
                    </button>
                </div>

                <div className="flex-1 overflow-y-auto p-6 space-y-8">
                    {/* Invite Section */}
                    <div className="space-y-4">
                        <h4 className="text-xs font-black text-slate-400 uppercase tracking-widest flex items-center gap-2">
                            <UserPlus size={14} />
                            Invite Member
                        </h4>
                        <div className="relative">
                            <div className="flex gap-2">
                                <div className="relative flex-1">
                                    <Mail className="absolute left-4 top-1/2 -translate-y-1/2 text-slate-300" size={18} />
                                    <input
                                        type="text"
                                        value={inviteEmail}
                                        onChange={handleSearch}
                                        placeholder="Enter user email..."
                                        className="w-full pl-11 pr-4 py-3 bg-slate-50 border border-slate-200 rounded-xl font-bold text-slate-700 focus:ring-2 focus:ring-blue-500 outline-none transition-all"
                                    />
                                    {searching && (
                                        <Loader2 className="absolute right-4 top-1/2 -translate-y-1/2 text-blue-500 animate-spin" size={18} />
                                    )}
                                </div>
                                <button
                                    onClick={() => handleInvite(inviteEmail)}
                                    disabled={!inviteEmail || inviting}
                                    className="px-6 bg-blue-600 hover:bg-blue-700 disabled:opacity-50 text-white font-bold rounded-xl shadow-lg shadow-blue-200 transition-all flex items-center gap-2"
                                >
                                    {inviting ? <Loader2 size={18} className="animate-spin" /> : <Plus size={18} />}
                                    Invite
                                </button>
                            </div>

                            {/* Search Results Dropdown */}
                            {searchResults.length > 0 && (
                                <div className="absolute top-full left-0 right-0 mt-2 bg-white border border-slate-200 rounded-xl shadow-xl z-10 overflow-hidden">
                                    {searchResults.map(user => (
                                        <button
                                            key={user.id}
                                            onClick={() => handleInvite(user.email)}
                                            className="w-full p-3 flex items-center gap-3 hover:bg-slate-50 transition-colors border-b border-slate-50 last:border-0"
                                        >
                                            <div className="w-8 h-8 rounded-full overflow-hidden bg-slate-100 flex-shrink-0">
                                                {user.picture_url ? (
                                                    <img src={user.picture_url} alt="" className="w-full h-full object-cover" />
                                                ) : (
                                                    <div className="w-full h-full flex items-center justify-center text-slate-400 font-bold text-xs uppercase">
                                                        {user.full_name?.charAt(0)}
                                                    </div>
                                                )}
                                            </div>
                                            <div className="text-left">
                                                <div className="text-xs font-black text-slate-700 leading-tight">{user.full_name}</div>
                                                <div className="text-[10px] text-slate-400 font-bold">{user.email}</div>
                                            </div>
                                            <div className="ml-auto text-blue-500 font-bold text-[10px] uppercase">Click to Invite</div>
                                        </button>
                                    ))}
                                </div>
                            )}
                        </div>
                    </div>

                    {/* Member List */}
                    <div className="space-y-4">
                        <h4 className="text-xs font-black text-slate-400 uppercase tracking-widest flex items-center gap-2">
                            <Shield size={14} />
                            Team Members ({members.length})
                        </h4>

                        {loading ? (
                            <div className="py-12 flex flex-col items-center justify-center text-slate-400 gap-3">
                                <Loader2 size={32} className="animate-spin" />
                                <span className="text-xs font-bold uppercase tracking-widest">Loading Team...</span>
                            </div>
                        ) : (
                            <div className="space-y-2">
                                {members.map(member => (
                                    <div key={member.id} className="p-4 bg-slate-50 border border-slate-100 rounded-2xl flex items-center gap-4 group">
                                        <div className="w-10 h-10 rounded-full overflow-hidden bg-slate-200 flex-shrink-0 ring-2 ring-white">
                                            {member.picture_url ? (
                                                <img src={member.picture_url} alt="" className="w-full h-full object-cover" />
                                            ) : (
                                                <div className="w-full h-full flex items-center justify-center text-slate-400 font-bold text-sm uppercase">
                                                    {member.full_name?.charAt(0)}
                                                </div>
                                            )}
                                        </div>

                                        <div className="flex-1 min-w-0">
                                            <div className="text-sm font-black text-slate-700 flex items-center gap-2">
                                                {member.full_name}
                                                {member.role === 'organizer' && (
                                                    <span className="px-1.5 py-0.5 bg-amber-100 text-amber-700 text-[8px] font-black uppercase rounded tracking-tighter">Admin</span>
                                                )}
                                            </div>
                                            <div className="text-[10px] text-slate-400 font-bold">{member.email}</div>
                                        </div>

                                        <div className="flex items-center gap-4">
                                            <select
                                                value={member.role}
                                                onChange={(e) => handleUpdateRole(member.id, e.target.value)}
                                                className="bg-white border border-slate-200 text-slate-600 text-[10px] font-black uppercase tracking-wider p-1.5 rounded-lg focus:ring-2 focus:ring-blue-500 outline-none"
                                            >
                                                <option value="organizer">Organizer</option>
                                                <option value="member">Member</option>
                                                <option value="viewer">Viewer</option>
                                            </select>

                                            <button
                                                onClick={() => handleRemoveMember(member.id)}
                                                className="p-2 text-slate-300 hover:text-red-500 hover:bg-red-50 rounded-lg transition-all"
                                            >
                                                <Trash2 size={16} />
                                            </button>
                                        </div>
                                    </div>
                                ))}
                            </div>
                        )}
                    </div>
                </div>

                <div className="p-4 border-t border-slate-100 bg-slate-50/50 flex justify-between items-center">
                    <p className="text-[10px] text-slate-400 font-bold max-w-[300px]">
                        Organizers can add/remove members and change roles. Members can contribute data. Viewers can only browse.
                    </p>
                    <button
                        onClick={onClose}
                        className="px-6 py-2 bg-slate-200 hover:bg-slate-300 text-slate-600 font-black rounded-xl text-xs uppercase tracking-widest transition-colors"
                    >
                        Close
                    </button>
                </div>
            </div>
        </div>
    );
}
