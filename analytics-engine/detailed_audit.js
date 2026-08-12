import * as dotenv from 'dotenv';
import path from 'path';
import { google } from 'googleapis';

dotenv.config({ path: path.resolve(process.cwd(), '.env.local') });
dotenv.config({ path: path.resolve(process.cwd(), '.env') });

const TARGET_DATE = "2026-05-31";
const CMS_ID = "MCmf4OH49HkyPyuNAA91Ew";

async function main() {
    console.log("Initializing YouTube API client...");
    const clientId = process.env.YOUTUBE_CLIENT_ID;
    const clientSecret = process.env.YOUTUBE_CLIENT_SECRET;
    const refreshToken = process.env.YOUTUBE_REFRESH_TOKEN;

    if (!clientId || !clientSecret || !refreshToken) {
        console.error("Missing YouTube API credentials in env variables.");
        return;
    }

    const oauth2Client = new google.auth.OAuth2(clientId, clientSecret);
    oauth2Client.setCredentials({ refresh_token: refreshToken });

    const youtubeAnalytics = google.youtubeAnalytics({
        version: 'v2',
        auth: oauth2Client
    });

    console.log("1. Fetching GLOBAL totals from YouTube Analytics API...");
    const apiGlobalRes = await youtubeAnalytics.reports.query({
        ids: `contentOwner==${CMS_ID}`,
        startDate: TARGET_DATE,
        endDate: TARGET_DATE,
        metrics: 'estimatedRevenue,estimatedAdRevenue,estimatedRedPartnerRevenue,estimatedTransactionRevenue',
    });

    const apiGlobalRow = apiGlobalRes.data.rows?.[0] || [0, 0, 0, 0];
    const apiGlobal = {
        revenue: parseFloat(apiGlobalRow[0] || '0'),
        ads: parseFloat(apiGlobalRow[1] || '0'),
        premium: parseFloat(apiGlobalRow[2] || '0'),
        transaction: parseFloat(apiGlobalRow[3] || '0'),
    };

    console.log("2. Fetching ClickHouse GLOBAL totals...");
    const fetch = (await import('node-fetch')).default;
    
    // Fetch ClickHouse totals
    const chGlobalQuery = {
        db: `db_${CMS_ID}`,
        query: `
            SELECT 
                sum(partner_rev_total) as revenue,
                sum(partner_rev_auction + partner_rev_reserved) as ads,
                sum(partner_rev_red) as premium,
                sum(partner_rev_transaction) as transactions,
                sum(if(claim_origin = 'Channel Bonus', partner_rev_transaction, 0)) as bonus
            FROM estimated_revenue_daily
            WHERE day = '${TARGET_DATE}'
        `
    };

    const chGlobalRes = await fetch('http://127.0.0.1:3001/api/v1/admin/query', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'x-api-key': 'org_f813e59c-4427-4da0-9f07-f6864ee64342'
        },
        body: JSON.stringify(chGlobalQuery)
    });
    const chGlobalJson = await chResClean(chGlobalRes);
    const chGlobalRow = chGlobalJson[0] || { revenue: 0, ads: 0, premium: 0, transactions: 0, bonus: 0 };
    
    const chGlobal = {
        revenue: parseFloat(chGlobalRow.revenue) || 0,
        ads: parseFloat(chGlobalRow.ads) || 0,
        premium: parseFloat(chGlobalRow.premium) || 0,
        transaction: parseFloat(chGlobalRow.transactions) || 0,
        bonus: parseFloat(chGlobalRow.bonus) || 0
    };

    console.log("\n========================================================");
    console.log("             GLOBAL REVENUE BREAKDOWN COMPARISON        ");
    console.log("========================================================");
    console.log(`Metric         API Total ($)   ClickHouse ($)   Diff ($)`);
    console.log(`--------------------------------------------------------`);
    console.log(`Total Revenue  ${apiGlobal.revenue.toFixed(4).padStart(13)}  ${chGlobal.revenue.toFixed(4).padStart(14)}  ${(chGlobal.revenue - apiGlobal.revenue).toFixed(4).padStart(10)}`);
    console.log(`Ad Revenue     ${apiGlobal.ads.toFixed(4).padStart(13)}  ${chGlobal.ads.toFixed(4).padStart(14)}  ${(chGlobal.ads - apiGlobal.ads).toFixed(4).padStart(10)}`);
    console.log(`Premium (Subs) ${apiGlobal.premium.toFixed(4).padStart(13)}  ${chGlobal.premium.toFixed(4).padStart(14)}  ${(chGlobal.premium - apiGlobal.premium).toFixed(4).padStart(10)}`);
    console.log(`Transaction    ${apiGlobal.transaction.toFixed(4).padStart(13)}  ${chGlobal.transaction.toFixed(4).padStart(14)}  ${(chGlobal.transaction - apiGlobal.transaction).toFixed(4).padStart(10)}`);
    console.log(`Channel Bonus  ${"0.0000".padStart(13)}  ${chGlobal.bonus.toFixed(4).padStart(14)}  ${chGlobal.bonus.toFixed(4).padStart(10)}`);
    console.log(`========================================================\n`);

    console.log("3. Fetching detailed VIDEO level data from API...");
    const apiVideoRes = await youtubeAnalytics.reports.query({
        ids: `contentOwner==${CMS_ID}`,
        startDate: TARGET_DATE,
        endDate: TARGET_DATE,
        metrics: 'estimatedRevenue,views',
        dimensions: 'video',
        maxResults: 200,
        sort: '-estimatedRevenue'
    });

    const apiVideoRows = apiVideoRes.data.rows || [];
    const apiVideoMap = new Map();
    const videoIds = [];
    for (const r of apiVideoRows) {
        const vidId = r[0];
        const rev = parseFloat(r[1] || '0');
        const views = parseInt(r[2] || '0', 10);
        apiVideoMap.set(vidId, { rev, views });
        videoIds.push(vidId);
    }

    console.log("4. Fetching ClickHouse details for these videos...");
    const formattedVids = videoIds.map(v => `'${v}'`).join(',');
    const chVideoQuery = {
        db: `db_${CMS_ID}`,
        query: `
            SELECT 
                video_id,
                any(channel_id) as channel_id,
                any(asset_id) as asset_id,
                sum(partner_rev_total) as revenue,
                sum(owned_views) as views
            FROM estimated_revenue_daily
            WHERE day = '${TARGET_DATE}'
              AND video_id IN (${formattedVids})
            GROUP BY video_id
        `
    };

    const chVideoRes = await fetch('http://127.0.0.1:3001/api/v1/admin/query', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'x-api-key': 'org_f813e59c-4427-4da0-9f07-f6864ee64342'
        },
        body: JSON.stringify(chVideoQuery)
    });
    const chVideoJson = await chResClean(chVideoRes);
    
    const perfectMatches = [];
    const mismatches = [];

    for (const chV of chVideoJson) {
        const vidId = chV.video_id;
        const chRev = parseFloat(chV.revenue) || 0;
        const apiV = apiVideoMap.get(vidId);
        
        if (apiV) {
            const diff = chRev - apiV.rev;
            const dataPoint = {
                video_id: vidId,
                channel_id: chV.channel_id,
                asset_id: chV.asset_id,
                api_rev: apiV.rev,
                ch_rev: chRev,
                diff: diff
            };

            if (Math.abs(diff) < 0.01) {
                perfectMatches.push(dataPoint);
            } else {
                mismatches.push(dataPoint);
            }
        }
    }

    console.log("=========================================================================");
    console.log("                      PERFECT MATCH SAMPLES (NO DIFF)                    ");
    console.log("=========================================================================");
    console.log(String("Video ID").padEnd(14) + String("Channel ID").padEnd(25) + String("Asset ID").padEnd(20) + String("API Rev").padStart(10) + String("CH Rev").padStart(10));
    console.log("-------------------------------------------------------------------------");
    perfectMatches.slice(0, 5).forEach(m => {
        console.log(
            m.video_id.padEnd(14) + 
            m.channel_id.padEnd(25) + 
            m.asset_id.padEnd(20) + 
            m.api_rev.toFixed(3).padStart(10) + 
            m.ch_rev.toFixed(3).padStart(10)
        );
    });
    console.log(`... and ${Math.max(0, perfectMatches.length - 5)} more perfect matches.\n`);

    console.log("=========================================================================");
    console.log("                        MISMATCH SAMPLES (WITH DIFF)                      ");
    console.log("=========================================================================");
    console.log(String("Video ID").padEnd(14) + String("Channel ID").padEnd(25) + String("Asset ID").padEnd(20) + String("API Rev").padStart(10) + String("CH Rev").padStart(10) + String("Diff").padStart(10));
    console.log("-------------------------------------------------------------------------");
    if (mismatches.length === 0) {
        console.log("No mismatches found in the top video list! Everything matches perfectly.");
    } else {
        mismatches.slice(0, 5).forEach(m => {
            console.log(
                m.video_id.padEnd(14) + 
                m.channel_id.padEnd(25) + 
                m.asset_id.padEnd(20) + 
                m.api_rev.toFixed(3).padStart(10) + 
                m.ch_rev.toFixed(3).padStart(10) + 
                m.diff.toFixed(3).padStart(10)
            );
        });
        console.log(`... and ${Math.max(0, mismatches.length - 5)} more mismatches.\n`);
    }
}

async function chResClean(res) {
    const json = await res.json();
    return json.data || [];
}

main().catch(console.error);
