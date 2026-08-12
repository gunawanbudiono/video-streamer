/**
 * Test subscription upload (simplified)
 */
import { readFileSync } from 'fs';
import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const API = 'http://127.0.0.1:3001';
const API_KEY = 'demo-key-change-in-production';

async function main() {
    // Create a simple subscription CSV
    const csvContent = [
        'Day\tCountry\tVideo ID\tVideo Channel ID\tAsset ID\tAsset Channel ID\tAsset Title\tAsset Labels\tAsset Type\tCustom ID\tISRC\tUPC\tGRid\tArtist\tAlbum\tLabel\tClaim Type\tContent Type\tOffer\tOwned Views\tMonetized Views : Audio\tMonetized Views : Audiovisual\tMonetized Views\tYouTube Revenue Split\tPartner Revenue : Pro Rata\tPartner Revenue : Per Subscriber Min\tPartner Revenue',
        '20260101\tID\tdQw4w9WgXcQ\tUCxyz\tA123456789\tUCxyz\tLagu Cinta Sejati\tCinta Records\tsound_recording\tCUSTOM001\tIDXXX2600001\t602341000001\t\tAnisa Rahmania\tAlbum Cinta\tCinta Records\tAudio\taudio\tPremium\t5000\t4500\t500\t5000\t1.20\t0.80\t0.10\t0.90',
        '20260101\tUS\tabc123video\tUCxyz\tA987654321\tUCjp\tKokoro no Uta\tTokyo Music\tsound_recording\tCUSTOM002\tIDXXX2600002\t602341000002\t\tYuki Tanaka\tHeart Songs\tTokyo Music\tAudio\taudio\tPremium\t3000\t2700\t300\t3000\t0.90\t0.60\t0.08\t0.68'
    ].join('\n');

    // Write temp file
    const { writeFileSync } = await import('fs');
    const { createGzip } = await import('zlib');
    const tempPath = resolve(__dirname, 'sub_test.csv.gz');

    // Gzip it
    const { gzipSync } = await import('zlib');
    writeFileSync(tempPath, gzipSync(Buffer.from(csvContent)));

    // Build multipart  
    const boundary = `----FormBoundary${Date.now()}`;
    const CRLF = '\r\n';
    const buffer = readFileSync(tempPath);

    const body = Buffer.concat([
        Buffer.from(`--${boundary}${CRLF}Content-Disposition: form-data; name="subscription"; filename="sub_test.csv.gz"${CRLF}Content-Type: application/gzip${CRLF}${CRLF}`),
        buffer,
        Buffer.from(CRLF),
        Buffer.from(`--${boundary}--${CRLF}`),
    ]);

    console.log('📤 Uploading subscription data (month 202601)...');
    const res = await fetch(`${API}/api/v1/ingest/subscription?month=202601`, {
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
        for (let i = 0; i < 15; i++) {
            await new Promise(r => setTimeout(r, 2000));
            const jobRes = await fetch(`${API}/api/v1/ingest/jobs/${data.job_id}`, {
                headers: { 'X-API-Key': API_KEY },
            });
            const job = await jobRes.json();
            console.log(`  [${i + 1}] Status: ${job.data?.status} | Rows: ${job.data?.total_rows || 0} | Error: ${job.data?.error_message || '-'}`);
            if (job.data?.status === 'completed' || job.data?.status === 'failed') break;
        }
    }
}

main().catch(console.error);
