#!/usr/bin/env bash
# Deadbolt one-command launcher.
#
#   ./run.sh            build + start PostgreSQL and the API, open http://localhost:8000
#   ./run.sh bench      run the 4-strategy benchmark inside the API container
#   ./run.sh test       run the test suite inside the API container
#   ./run.sh logs       follow API logs
#   ./run.sh down       stop everything (keeps the database volume)
#   ./run.sh clean      stop everything and delete the database volume
#   ./run.sh local      run the API on this machine (needs Python 3.11+ and a PostgreSQL at $DATABASE_URL)
set -euo pipefail

cd "$(dirname "$0")"
URL="http://localhost:8000"

if docker compose version >/dev/null 2>&1; then
  DC="docker compose"
elif command -v docker-compose >/dev/null 2>&1; then
  DC="docker-compose"
else
  DC=""
fi

need_docker() {
  if [ -z "$DC" ]; then
    echo "Docker Compose not found. Install Docker Desktop, or use: ./run.sh local" >&2
    exit 1
  fi
}

wait_for_api() {
  printf "waiting for API"
  for _ in $(seq 1 60); do
    if curl -fsS "$URL/api/health" >/dev/null 2>&1; then
      echo " ready"
      return 0
    fi
    printf "."
    sleep 1
  done
  echo
  echo "API did not become healthy. Check: ./run.sh logs" >&2
  exit 1
}

open_browser() {
  if command -v xdg-open >/dev/null 2>&1; then xdg-open "$URL" >/dev/null 2>&1 || true
  elif command -v open >/dev/null 2>&1; then open "$URL" || true
  elif command -v start >/dev/null 2>&1; then start "$URL" || true
  fi
}

case "${1:-up}" in
  up)
    need_docker
    $DC up --build -d
    wait_for_api
    echo
    echo "  Deadbolt dashboard : $URL"
    echo "  API docs           : $URL/docs"
    echo "  PostgreSQL         : postgresql://deadbolt:deadbolt@localhost:5432/deadbolt"
    echo
    echo "  Benchmark all strategies: ./run.sh bench"
    open_browser
    ;;
  bench)
    need_docker
    shift || true
    $DC exec api python load_simulator.py --mode direct --compare --users 1000 --requests 10000 --markdown "$@"
    ;;
  test)
    need_docker
    $DC exec api python -m pytest -q
    ;;
  logs)
    need_docker
    $DC logs -f api
    ;;
  down)
    need_docker
    $DC down
    ;;
  clean)
    need_docker
    $DC down -v
    ;;
  local)
    export DATABASE_URL="${DATABASE_URL:-postgresql://deadbolt:deadbolt@localhost:5432/deadbolt}"
    if [ ! -d .venv ]; then
      python3 -m venv .venv
    fi
    # shellcheck disable=SC1091
    . .venv/bin/activate
    pip install -q -r requirements.txt
    echo "Starting Deadbolt on $URL (DATABASE_URL=$DATABASE_URL)"
    exec uvicorn app.main:app --host 0.0.0.0 --port 8000
    ;;
  *)
    echo "usage: ./run.sh [up|bench|test|logs|down|clean|local]" >&2
    exit 2
    ;;
esac
