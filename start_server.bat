@echo off
echo ========================================
echo    ETL Dashboard Server Startup
echo ========================================
echo.
echo Starting Flask ETL Server...
echo.
echo ✅ Server will be available at: http://127.0.0.1:5000
echo ❌ DO NOT use port 5500 or open HTML files directly
echo.
echo 📋 Instructions:
echo    1. Wait for server to start
echo    2. Open browser and go to: http://127.0.0.1:5000
echo    3. Press Ctrl+C to stop the server
echo.
echo 🔧 Debug URLs:
echo    - Main page: http://127.0.0.1:5000
echo    - Health check: http://127.0.0.1:5000/api/health
echo    - Debug info: http://127.0.0.1:5000/debug
echo.
echo ========================================
echo.
python flask_etl_server.py
echo.
echo Server stopped. Press any key to exit...
pause
