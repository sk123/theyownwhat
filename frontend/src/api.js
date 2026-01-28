const API_BASE = '/api';

export const api = {
    get: async (endpoint) => {
        const res = await fetch(`${API_BASE}${endpoint}`);
        console.log(`API GET ${endpoint}:`, res.status);
        if (!res.ok) throw new Error(`API Error: ${res.statusText}`);
        const data = await res.json();
        console.log(`API response data:`, data);
        return data;
    },
    post: async (endpoint, body) => {
        const res = await fetch(`${API_BASE}${endpoint}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body),
        });
        if (!res.ok) throw new Error(`API Error: ${res.statusText}`);
        return res.json();
    },

    // Streaming helper for network loading
    streamNetwork: async (entityId, entityType, entityName, onChunk, onComplete, onError) => {
        try {
            const response = await fetch(`${API_BASE}/network/stream_load`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    entity_id: entityId,
                    entity_type: entityType,
                    entity_name: entityName
                })
            });

            if (!response.ok) throw new Error(`Stream Error: ${response.statusText}`);

            const reader = response.body.getReader();
            const decoder = new TextDecoder();
            let buffer = '';

            while (true) {
                const { value, done } = await reader.read();
                if (done) break;

                buffer += decoder.decode(value, { stream: true });
                const lines = buffer.split('\n');
                buffer = lines.pop(); // Keep partial line

                for (const line of lines) {
                    if (!line.trim()) continue;
                    let chunk;
                    try {
                        chunk = JSON.parse(line);
                    } catch (e) {
                        console.error('Error parsing chunk JSON', e, line);
                        continue;
                    }

                    try {
                        onChunk(chunk);
                    } catch (e) {
                        console.error('Error in onChunk handler', e);
                    }
                }
            }
            onComplete();
        } catch (err) {
            if (onError) onError(err);
            else console.error(err);
        }
    }
};
