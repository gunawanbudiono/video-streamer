const { createClient } = require('@clickhouse/client');

async function main() {
    const client = createClient({
        url: 'http://localhost:8123',
        username: 'default',
        password: '',
        database: 'db_DtIzPW10SINp5maPSwiuV'
    });

    try {
        console.log("Checking empty vs populated titles in subscription_revenue:");
        const r4 = await client.query({
            query: `
                SELECT video_title != '' as has_title, count() as count
                FROM subscription_revenue
                WHERE upload_month = 202606
                GROUP BY has_title
            `,
            format: 'JSONEachRow'
        });
        const rows4 = await r4.json();
        console.log(JSON.stringify(rows4, null, 2));

        console.log("\nChecking empty vs populated channel names in subscription_revenue:");
        const r5 = await client.query({
            query: `
                SELECT channel_display_name != '' as has_channel, count() as count
                FROM subscription_revenue
                WHERE upload_month = 202606
                GROUP BY has_channel
            `,
            format: 'JSONEachRow'
        });
        const rows5 = await r5.json();
        console.log(JSON.stringify(rows5, null, 2));

    } catch (e) {
        console.error("Error executing query:", e);
    } finally {
        await client.close();
    }
}

main();
