const networkData = {
    links: [
        { source: "business_001t000000abcd", target: "principal_DOE JOHN" }
    ],
    principals: [
        { id: "123", name: "JOHN DOE", type: "principal", isEntity: false }
    ]
};

const principalCounts = new Map();
networkData.links.forEach(l => {
    const s = String(l.source);
    const t = String(l.target);
    principalCounts.set(s, (principalCounts.get(s) || 0) + 1);
    principalCounts.set(t, (principalCounts.get(t) || 0) + 1);
});

console.log("principalCounts:", principalCounts);

const normalizeId = (id) => {
    if (!id) return '';
    let n = String(id).toUpperCase().trim();
    n = n.replace(/[`"'.]/g, ''); 
    const suffixes = ['JR', 'SR', 'III', 'IV', 'II', 'ESQ', 'MD', 'PHD', 'DDS'];
    const suffixRegex = new RegExp(`\\s+(${suffixes.join('|')})$`);
    n = n.replace(suffixRegex, '');
    n = n.replace(/\s+/g, ' '); 
    n = n.trim();
    if (n.includes(',')) {
        const parts = n.split(',').map(s => s.trim());
        if (parts.length >= 2) {
            const last = parts[0];
            const first = parts[1];
            n = `${first} ${last}`;
        }
    }
    n = n.replace(/\s+/g, ' ').trim();
    n = n.replace(/GUREVITOH/g, "GUREVITCH");
    n = n.replace(/MANACHEM/g, "MENACHEM");
    n = n.replace(/GURAVITCH/g, "GUREVITCH");
    n = n.split(' ').sort().join(' ');
    return n;
};

const getCount = (p) => {
    const candidates = new Set();
    if (p) {
        if (typeof p !== 'object') {
            candidates.add(String(p));
        } else {
            if (p.id) candidates.add(String(p.id));
            if (p.name) candidates.add(String(p.name));
            if (p.details?.name_c) candidates.add(String(p.details.name_c));
        }
    }

    let max = 0;
    candidates.forEach(c => {
        const sId = c;
        const raw = principalCounts.get(sId) || 0;
        const norm = normalizeId(sId);
        
        const princKey = `principal_${norm}`;
        const princCount = principalCounts.get(princKey) || 0;
        
        const bizKey = `business_${sId}`;
        const bizCount = principalCounts.get(bizKey) || 0;
        
        console.log(`Checking candidate: ${sId} -> norm: ${norm}`);
        console.log(`  raw: ${raw}, princKey (${princKey}): ${princCount}, bizKey (${bizKey}): ${bizCount}`);

        max = Math.max(max, raw, princCount, bizCount);
    });
    return max;
};

console.log("Count for JOHN DOE:", getCount(networkData.principals[0]));
