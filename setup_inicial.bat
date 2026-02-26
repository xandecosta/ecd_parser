@echo off
echo ============================================================
echo   Configurando Ambiente Inicial para Python (SPED-ECD)
echo ============================================================
echo.

:: Verifica se o Python está instalado
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERRO] Python nao encontrado! Instale o Python para prosseguir.
    pause
    exit /b
)

echo [1/3] Criando ambiente virtual (.venv)...
python -m venv .venv

echo [2/3] Ativando ambiente e instalando dependências...
call .venv\Scripts\activate
pip install --upgrade pip
pip install -r requirements.txt

echo [3/3] Criando estrutura de pastas básicas (se nao existirem)...
if not exist data\input mkdir data\input
if not exist data\output mkdir data\output

echo.
echo ============================================================
echo   Setup concluído! Use o comando ".\venv\Scripts\activate"
echo   antes de rodar o programa principal.
echo ============================================================
pause
