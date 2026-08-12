/**
 * Diagnostic: test subscription upload with detailed error capture
 */
import { readFileSync, writeFileSync } from 'fs';
import { gzipSync } from 'zlib';
import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const API = 'http://127.0.0.1:3001';
const API_KEY = 'demo-key-change-in-production';

const csvContent = `Day\tCountry\tVideo ID\tAsset ID\tAsset Title\tAsset Labels\tArtist\tAlbum\tLabel\tPartner Revenue
20260101\tID\tVID001\tA001\tTest Song\tLabel1\tArtist1\tAlbum1\tLabel1\t0.50
20260102\tUS\tVID002\tA002\tTest Song 2\tLabel2\tArtist2\tAlbum2\tLabel2\t1.25`;

const gzipped = gzipSync(Buffer.from(csvContent));
const tempPath = resolve(__dirname, 'diag_sub.csv.gz');
writeFileSync(tempPath, gzipped);

const boundary = `----Boundary${Date.now()}`;
const CRLF = '\r\n';
const body = Buffer.concat([
    Buffer.from(`--${boundary}${CRLF}Content-Disposition: form-data; name="subscription"; filename="diag_sub.csv.gz"${CRLF}Content-Type: application/gzip${CRLF}${CRLF}`),
    readFileSync(tempPath),
    Buffer.from(CRLF),
    Buffer.from(`--${boundary}--${CRLF}`),
]);

console.log('📤 Uploading...');
const res = await fetch(`${API}/api/v1/ingest/subscription?month=202601`, {
    method: 'POST',
    headers: { 'X-API-Key': API_KEY, 'Content-Type': `multipart/form-data; boundary=${boundary}` },
    body,
});
const data = await res.json();
console.log('📦 Response:', JSON.stringify(data));

if (data.job_id) {
    // Poll more aggressively
    for (let i = 0; i < 30; i++) {
        await new Promise(r => setTimeout(r, 1000));
        const jr = await fetch(`${API}/api/v1/ingest/jobs/${data.job_id}`, {
            headers: { 'X-API-Key': API_KEY },
        });
        const j = await jr.json();
        const s = j.data?.status;
        const rows = j.data?.total_rows;
        const err = j.data?.error_message;
        console.log(`  [${i + 1}] status=${s} rows=${rows} err=${err || '-'}`);
        if (s === 'completed' || s === 'failed') {
            console.log('✅ Final:', JSON.stringify(j.data, null, 2));
            break;
        }
    }
}
