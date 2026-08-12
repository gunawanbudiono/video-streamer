const XLSX = require('xlsx');
const path = require('path');
const { createClient } = require('@clickhouse/client');

async function main() {
    const ch = createClient({
        url: 'http://localhost:8123',
        username: 'default',
        password: '',
        database: 'db_DtIzPW10SINp5maPSwiuV'
    });

    try {
        const xlsxPath = path.join(__dirname, '..', '..', '..', 'scratch', 'compare_sheet.xlsx');
        console.log("Loading Excel file:", xlsxPath);
        const workbook = XLSX.readFile(xlsxPath);

        console.log("Parsing Video ID sheet (Sheet2)...");
        const videoSheet = workbook.Sheets['Sheet2'];
        const videoRows = XLSX.utils.sheet_to_json(videoSheet);

        const sheetVideos = new Map();
        for (const row of videoRows) {
            const id = (row['Video ID'] || '').toString().trim().toLowerCase();
            const rev = parseFloat(row['Partner Revenue']) || 0;
            if (id) {
                sheetVideos.set(id, (sheetVideos.get(id) || 0) + rev);
            }
        }

        console.log("Querying ClickHouse for Video IDs...");
        const chVideoRes = await ch.query({
            query: `
                SELECT video_id, sum(partner_rev_total) as db_rev
                FROM ads_revenue_enriched
                WHERE upload_month = 202606 AND (adjustment_type = '' OR lower(adjustment_type) = 'none' OR adjustment_type IS NULL)
                GROUP BY video_id
            `,
            format: 'JSONEachRow'
        });
        const chVideoRows = await chVideoRes.json();
        
        const dbVideos = new Map();
        for (const row of chVideoRows) {
            const id = (row.video_id || '').trim().toLowerCase();
            const rev = parseFloat(row.db_rev) || 0;
            if (id) {
                dbVideos.set(id, (dbVideos.get(id) || 0) + rev);
            }
        }

        let videoOnlyInSheet = [];
        let videoOnlyInDb = [];

        const allVideoKeys = new Set([...sheetVideos.keys(), ...dbVideos.keys()]);
        for (const key of allVideoKeys) {
            if (sheetVideos.has(key) && !dbVideos.has(key)) {
                videoOnlyInSheet.push({ id: key, val: sheetVideos.get(key) });
            } else if (!sheetVideos.has(key) && dbVideos.has(key)) {
                videoOnlyInDb.push({ id: key, val: dbVideos.get(key) });
            }
        }

        console.log("\n=================================");
        console.log("Unmatched Videos (Case-Insensitive):");
        console.log("=================================");
        console.log("Only in Spreadsheet:", JSON.stringify(videoOnlyInSheet, null, 2));
        console.log("Only in ClickHouse DB:", JSON.stringify(videoOnlyInDb, null, 2));

    } catch (e) {
        console.error("Error in comparison script:", e);
    } finally {
        await ch.close();
    }
}

main();
