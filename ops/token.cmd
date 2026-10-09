@echo off
rem Gets a Keycloak access token for a dev user and puts it in %TOKEN% (in the calling CMD window).
rem   call ops\token.cmd operator operator
rem   curl -H "Authorization: Bearer %TOKEN%" http://localhost:8081/actuator/stucksagas
rem Dev users: viewer/viewer, operator/operator, admin/admin. Tokens expire after 5 minutes - call it again.
rem Uses the public 'saga-cli' client with the password grant: local development only.

if "%~2"=="" (
    echo Usage: call ops\token.cmd ^<user^> ^<password^>
    exit /b 1
)
set "TOKEN="
set "SAGA_KEYCLOAK=%SAGA_KEYCLOAK_URL%"
if not defined SAGA_KEYCLOAK set "SAGA_KEYCLOAK=http://localhost:8180"

rem The response starts {"access_token":"<token>",... so with " as delimiter the token is field 4
for /f tokens^=4^ delims^=^" %%t in ('curl -s -d "grant_type=password&client_id=saga-cli&username=%~1&password=%~2" %SAGA_KEYCLOAK%/realms/saga/protocol/openid-connect/token') do set "TOKEN=%%t"

if not defined TOKEN (
    echo No token. Is Keycloak running at %SAGA_KEYCLOAK%? Keycloak said:
    curl -s -d "grant_type=password&client_id=saga-cli&username=%~1&password=%~2" %SAGA_KEYCLOAK%/realms/saga/protocol/openid-connect/token
    echo.
    set "SAGA_KEYCLOAK="
    exit /b 1
)
if "%TOKEN:~0,2%" NEQ "ey" (
    echo Keycloak refused: %TOKEN% ^(wrong user or password?^)
    set "TOKEN="
    set "SAGA_KEYCLOAK="
    exit /b 1
)
set "SAGA_KEYCLOAK="
echo TOKEN set for %~1 (valid 5 minutes).
