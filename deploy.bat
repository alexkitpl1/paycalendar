@echo off
title Deploy PayCalendar to Railway
setlocal
set ROOT=%~dp0
set RAILWAY=%ROOT%railway.exe

echo.
echo  ========================================
echo   PayCalendar - Deploy to Railway
echo  ========================================
echo.

:: Download railway.exe if not present
if not exist "%RAILWAY%" (
    echo  Скачиваю Railway CLI...
    powershell -NoProfile -Command "Invoke-WebRequest -Uri 'https://github.com/railwayapp/cli/releases/latest/download/railway-x86_64-pc-windows-msvc.exe' -OutFile '%RAILWAY%'"
    if not exist "%RAILWAY%" (
        echo.
        echo  Не удалось скачать автоматически.
        echo  Скачай вручную:
        echo  https://github.com/railwayapp/cli/releases/latest
        echo  Файл: railway-x86_64-pc-windows-msvc.exe
        echo  Переименуй в railway.exe и положи рядом с deploy.bat
        echo.
        pause
        exit /b 1
    )
    echo  Railway CLI скачан!
    echo.
)

echo  Логин в Railway (откроется браузер)...
"%RAILWAY%" login
if errorlevel 1 (
    echo  Ошибка логина
    pause
    exit /b 1
)

echo.
echo  Создаю проект и деплою...
"%RAILWAY%" up --detach
if errorlevel 1 (
    echo  Ошибка деплоя
    pause
    exit /b 1
)

echo.
echo  Получаю URL...
"%RAILWAY%" domain

echo.
echo  ========================================
echo   ГОТОВО!
echo   Открой URL выше на Android телефоне
echo   Пароль - это ACCESS_KEY из Variables
echo  ========================================
echo.
pause
