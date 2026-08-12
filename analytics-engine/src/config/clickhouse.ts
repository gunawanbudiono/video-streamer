import { createClient, ClickHouseClient } from '@clickhouse/client';
import { config } from './index.js';

/** ClickHouse client cache — one per database */
const clientCache = new Map<string, ClickHouseClient>();

/** Get or create a ClickHouse client for a specific database and user */
export function getClickHouseClient(opts: {
    database: string;
    username?: string;
    password?: string;
}): ClickHouseClient {
    const cacheKey = `${opts.database}:${opts.username || 'default'}`;
    const cached = clientCache.get(cacheKey);
    if (cached) return cached;

    const client = createClient({
        url: config.clickhouse.host,
        database: opts.database,
        username: opts.username || 'default',
        password: opts.password || '',
        clickhouse_settings: {
            wait_end_of_query: 1,
            max_memory_usage: '8589934592', // 8 GB limit explicitly for ingestion payloads
            max_query_size: '104857600',    // 100 MB — prevents parser crash on large IN(...) clauses
        },
        request_timeout: 300_000,
    });

    clientCache.set(cacheKey, client);
    return client;
}

/** Get a default client (for admin operations on 'default' db) */
export function getDefaultClient(): ClickHouseClient {
    return getClickHouseClient({ database: 'default' });
}

/** Close all cached clients (for graceful shutdown) */
export async function closeAllClients(): Promise<void> {
    for (const [key, client] of clientCache.entries()) {
        await client.close();
        clientCache.delete(key);
    }
}
