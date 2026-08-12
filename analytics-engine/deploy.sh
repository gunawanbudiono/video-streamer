#!/bin/bash

# Analytics Engine Deployment Script
# Path: analytics-engine/deploy.sh

# Get the directory of the script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "=================================================="
echo "🚀 Starting Analytics Engine Deployment..."
echo "=================================================="

# 1. Pull latest code from Git
echo "📥 [1/3] Pulling latest changes from Git..."
git pull origin main
if [ $? -ne 0 ]; then
    echo "❌ Error: git pull failed! Aborting deployment."
    exit 1
fi
echo "✅ Git pull successful!"
echo ""

# 2. Build TypeScript application
echo "🛠️ [2/3] Building analytics-engine..."
npm install
if [ $? -ne 0 ]; then
    echo "❌ Error: npm install failed!"
    exit 1
fi

npm run build
if [ $? -ne 0 ]; then
    echo "❌ Error: npm run build failed!"
    exit 1
fi
echo "✅ Build successful!"
echo ""

# 3. Restart PM2 process
echo "🔄 [3/3] Restarting PM2 process..."
# Try restarting specific app name first, fallback to all if not found
pm2 restart analytics-engine || pm2 restart analytics-api || pm2 restart all
if [ $? -ne 0 ]; then
    echo "⚠️ Warning: PM2 restart failed."
    exit 1
fi
echo "✅ PM2 process restarted successfully!"
echo ""

echo "=================================================="
echo "🎉 Analytics Engine Deployment Completed!"
echo "=================================================="
