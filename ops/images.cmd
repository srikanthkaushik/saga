@echo off
setlocal
rem Builds the six deployment images (and optionally pushes them) from the repository root.
rem   ops\images.cmd                  build, tag :latest
rem   ops\images.cmd 2026.10.09       build, tag :2026.10.09 and :latest
rem   ops\images.cmd 2026.10.09 push  build, tag both, push both
rem Registry: %SAGA_REGISTRY% (default ghcr.io/srikanthkaushik). Pushing to GHCR needs a one-time
rem   docker login ghcr.io -u <github user>   (password: a personal access token with write:packages)

if not defined SAGA_REGISTRY set "SAGA_REGISTRY=ghcr.io/srikanthkaushik"
set "TAG=%~1"
if "%TAG%"=="" set "TAG=latest"
cd /d "%~dp0.."

echo == Packaging jars
call mvn -q clean package -DskipITs
if errorlevel 1 exit /b 1

echo == Building images %SAGA_REGISTRY%/saga-*:%TAG%
for %%m in (order-service payment-service inventory-service) do (
    docker build -q -f docker\app.Dockerfile --build-arg MODULE=%%m -t %SAGA_REGISTRY%/saga-%%m:%TAG% -t %SAGA_REGISTRY%/saga-%%m:latest .
    if errorlevel 1 exit /b 1
)
docker build -q -f docker\app.Dockerfile --build-arg MODULE=saga-ui -t %SAGA_REGISTRY%/saga-console:%TAG% -t %SAGA_REGISTRY%/saga-console:latest .
if errorlevel 1 exit /b 1
docker build -q -f docker\keycloak\Dockerfile -t %SAGA_REGISTRY%/saga-keycloak:%TAG% -t %SAGA_REGISTRY%/saga-keycloak:latest .
if errorlevel 1 exit /b 1
docker build -q -f docker\postgres\Dockerfile -t %SAGA_REGISTRY%/saga-postgres:%TAG% -t %SAGA_REGISTRY%/saga-postgres:latest .
if errorlevel 1 exit /b 1

if /i not "%~2"=="push" (
    echo == Built. Push with: ops\images.cmd %TAG% push
    exit /b 0
)
echo == Pushing
for %%i in (order-service payment-service inventory-service console keycloak postgres) do (
    docker push -q %SAGA_REGISTRY%/saga-%%i:%TAG%
    if errorlevel 1 exit /b 1
    if /i not "%TAG%"=="latest" (
        docker push -q %SAGA_REGISTRY%/saga-%%i:latest
        if errorlevel 1 exit /b 1
    )
)
echo == Pushed %SAGA_REGISTRY%/saga-*:%TAG%
