/**
 * Test: upload sample CSV.gz files to Analytics Engine
 * Usage: node test-data/test-upload.mjs
 */
import { readFileSync } from 'fs';
import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';
import { Blob } from 'buffer';

const __dirname = dirname(fileURLToPath(import.meta.url));
const API = 'http://127.0.0.1:3001';
const API_KEY = 'demo-key-change-in-production';

async function main() {
    // Build multipart form manually since Node.js native FormData may not handle File
    const boundary = `----FormBoundary${Date.now()}`;
    const CRLF = '\r\n';

    function filePart(fieldName, fileName, buffer) {
        return Buffer.concat([
            Buffer.from(`--${boundary}${CRLF}Content-Disposition: form-data; name="${fieldName}"; filename="${fileName}"${CRLF}Content-Type: application/gzip${CRLF}${CRLF}`),
            buffer,
            Buffer.from(CRLF),
        ]);
    }

    const claimRaw = readFileSync(resolve(__dirname, 'claim_raw.csv.gz'));
    const videoclaim = readFileSync(resolve(__dirname, 'videoclaim.csv.gz'));
    const assetSummary = readFileSync(resolve(__dirname, 'asset_summary.csv.gz'));

    const body = Buffer.concat([
        filePart('claim_raw', 'claim_raw.csv.gz', claimRaw),
        filePart('videoclaim', 'videoclaim.csv.gz', videoclaim),
        filePart('asset_summary', 'asset_summary.csv.gz', assetSummary),
        Buffer.from(`--${boundary}--${CRLF}`),
    ]);

    console.log('📤 Uploading ads revenue data (month 202601)...');

    const res = await fetch(`${API}/api/v1/ingest/ads-revenue?month=202601`, {
        method: 'POST',
        headers: {
            'X-API-Key': API_KEY,
            'Content-Type': `multipart/form-data; boundary=${boundary}`,
        },
        body,
    });

    const data = await res.json();
    console.log('Response:', JSON.stringify(data, null, 2));

    if (data.job_id) {
        console.log('\n⏳ Polling job status...');
        for (let i = 0; i < 20; i++) {
            await new Promise(r => setTimeout(r, 2000));
            const jobRes = await fetch(`${API}/api/v1/ingest/jobs/${data.job_id}`, {
                headers: { 'X-API-Key': API_KEY },
            });
            const job = await jobRes.json();
            const s = job.data?.status;
            const rows = job.data?.total_rows || 0;
            console.log(`  [${i + 1}] Status: ${s} | Rows: ${rows}`);
            if (s === 'completed' || s === 'failed') {
                if (s === 'failed') console.log('  Error:', job.data?.error_message);
                break;
            }
        }
    }

    // Wait a moment for processing
    await new Promise(r => setTimeout(r, 2000));

    // Query analytics
    console.log('\n📈 Fetching analytics summary...');
    const sumRes = await fetch(`${API}/api/v1/analytics/summary?month=202601`, {
        headers: { 'X-API-Key': API_KEY },
    });
    const summary = await sumRes.json();
    console.log(JSON.stringify(summary, null, 2));

    console.log('\n🏆 Fetching top assets...');
    const topRes = await fetch(`${API}/api/v1/analytics/top-assets?month=202601&limit=5`, {
        headers: { 'X-API-Key': API_KEY },
    });
    const top = await topRes.json();
    console.log(JSON.stringify(top, null, 2));

    console.log('\n🌍 Fetching by country...');
    const countryRes = await fetch(`${API}/api/v1/analytics/by-country?month=202601`, {
        headers: { 'X-API-Key': API_KEY },
    });
    const country = await countryRes.json();
    console.log(JSON.stringify(country, null, 2));

    console.log('\n✅ Test complete!');
}

main().catch(console.error);
