const fetch = require('node-fetch');

async function test() {
    const urls = [
        'http://localhost:3001/api/v1/ingest/archive-files?cms_id=DtIzPW10SINp5maPSwiuV&month=202606', // Suara Mas
        'http://localhost:3001/api/v1/ingest/archive-files?cms_id=w3UR4sfdICvy3h75hKBGk&month=202606'  // StarHits MCN
    ];

    for (const url of urls) {
        console.log(`\nFetching: ${url}`);
        try {
            const res = await fetch(url, { headers: { 'X-API-Key': 'org_f813e59c-4427-4da0-9f07-f6864ee64342' } });
            const data = await res.json();
            console.log("Response:", JSON.stringify(data, null, 2));
        } catch (e) {
            console.error("Error:", e.message);
        }
    }
}

test();
