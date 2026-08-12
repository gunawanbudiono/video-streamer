@echo off
title ClickHouse & Analytics Admin Tunnel (Ports 8123 & 3002)
echo ====================================================================
echo 🔐 Membuka Secure SSH Tunnel ke VPS (Port 8123 & 3002)...
echo ====================================================================
echo.
echo Tunnel ini akan menghubungkan dua port secara aman:
echo.
echo 📊 [1] ClickHouse Web UI:
echo     http://localhost:8123/play
echo.
echo ⚙️ [2] Analytics Engine Admin Panel (Dialihkan ke port 3002):
echo     http://localhost:3002/admin
echo.
echo [PENTING] Jangan tutup jendela command prompt ini selama Anda mengakses UIs!
echo.
plink -P 2222 -pw martabakpecenongan -L 8123:127.0.0.1:8123 -L 3002:127.0.0.1:3002 siogut@103.195.102.191
echo.
echo Tunnel tertutup.
pause
