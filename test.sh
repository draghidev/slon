#!/bin/sh
set -eu

SLON_ROOT=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
ENVIRONMENT="$SLON_ROOT/eng/postgres"
PORT=${SLON_TEST_PORT:-55432}
PROJECT=${COMPOSE_PROJECT_NAME:-slon-tests}

cleanup() {
    docker compose --project-name "$PROJECT" --file "$ENVIRONMENT/compose.yml" down --volumes --remove-orphans
}
trap cleanup EXIT HUP INT TERM

SLON_TEST_PORT="$PORT" docker compose --project-name "$PROJECT" \
    --file "$ENVIRONMENT/compose.yml" up --detach --wait

if [ "$#" -eq 0 ]; then
    FILTER=
elif [ "${1:-}" = "--auth" ] && [ "$#" -eq 1 ]; then
    FILTER='--filter TestCategory=PostgreSqlAuthenticationIntegration'
else
    echo "usage: $0 [--auth]" >&2
    exit 2
fi

SLON_AUTH_INTEGRATION=1 SLON_TEST_HOST=127.0.0.1 SLON_TEST_PORT="$PORT" \
    dotnet test "$SLON_ROOT/Slon.Tests/Slon.Tests.csproj" -c Release --no-restore $FILTER
