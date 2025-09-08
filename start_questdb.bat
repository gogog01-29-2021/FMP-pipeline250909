@echo off
echo Starting QuestDB...
cd /d C:\etf_data_project\questdb-9.0.3-rt-windows-x86-64\bin
start "QuestDB Server" questdb.exe start -d ..\..\questdb_data
echo.
echo QuestDB is starting...
echo.
echo Access QuestDB at:
echo   Web Console: http://localhost:9000
echo   PostgreSQL: localhost:8812
echo   InfluxDB: localhost:9009
echo.
echo To stop QuestDB, close the QuestDB Server window.
pause