import dotenv from 'dotenv';
import { existsSync } from 'fs';
import { resolve } from 'path';

// Load .env.local first, then .env as fallback
const envLocal = resolve(process.cwd(), '.env.local');
const envFile = resolve(process.cwd(), '.env');
dotenv.config({ path: existsSync(envLocal) ? envLocal : envFile });

export const config = {
    port: parseInt(process.env.PORT || '3001', 10),
    host: process.env.HOST || '127.0.0.1',
    logLevel: process.env.LOG_LEVEL || 'info',

    clickhouse: {
        host: process.env.CLICKHOUSE_HOST || 'http://localhost:8123',
    },

    adminMasterKey: process.env.ADMIN_MASTER_KEY || '',
    postgresUrl: process.env.POSTGRES_URL || 'postgresql://neonvault:neonvault_secret@localhost:5432/neonvault_db',
} as const;

/** Parse CMS-related env vars dynamically */
export function getCmsEnvConfig(): Record<string, {
    apiKey: string;
    chUser: string;
    chPass: string;
    chIngestUser: string;
    chIngestPass: string;
}> {
    const cmsConfigs: Record<string, any> = {};
    for (const [key, value] of Object.entries(process.env)) {
        const apiMatch = key.match(/^API_KEY_(.+)$/);
        if (apiMatch && value) {
            const cmsId = apiMatch[1].toLowerCase();
            cmsConfigs[cmsId] = {
                ...(cmsConfigs[cmsId] || {}),
                apiKey: value,
            };
        }
        const userMatch = key.match(/^CH_USER_(.+)$/);
        if (userMatch && value) {
            const cmsId = userMatch[1].toLowerCase();
            cmsConfigs[cmsId] = {
                ...(cmsConfigs[cmsId] || {}),
                chUser: value,
            };
        }
        const passMatch = key.match(/^CH_PASS_(.+)$/);
        if (passMatch && value) {
            const cmsId = passMatch[1].toLowerCase();
            cmsConfigs[cmsId] = {
                ...(cmsConfigs[cmsId] || {}),
                chPass: value,
            };
        }
        const ingestUserMatch = key.match(/^CH_INGEST_USER_(.+)$/);
        if (ingestUserMatch && value) {
            const cmsId = ingestUserMatch[1].toLowerCase();
            cmsConfigs[cmsId] = {
                ...(cmsConfigs[cmsId] || {}),
                chIngestUser: value,
            };
        }
        const ingestPassMatch = key.match(/^CH_INGEST_PASS_(.+)$/);
        if (ingestPassMatch && value) {
            const cmsId = ingestPassMatch[1].toLowerCase();
            cmsConfigs[cmsId] = {
                ...(cmsConfigs[cmsId] || {}),
                chIngestPass: value,
            };
        }
    }
    return cmsConfigs;
}
