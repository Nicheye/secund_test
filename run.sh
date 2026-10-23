#!/usr/bin/env bash
# One-command launcher for the payment service stack. Run `./run.sh help` for usage.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

# The demo service sits behind a compose profile; run.sh always wants it. Profiles the caller
# already set are kept, and demo is not listed twice.
case ",${COMPOSE_PROFILES:-}," in
*,demo,*) ;;
*) COMPOSE_PROFILES="${COMPOSE_PROFILES:+$COMPOSE_PROFILES,}demo" ;;
esac
export COMPOSE_PROFILES

STATE_FILE=".run.env"
WAIT_TIMEOUT="${WAIT_TIMEOUT:-180}"
DEFAULT_API_PORT=8000
DEFAULT_DEMO_PORT=8090
RABBITMQ_AMQP_PORT=5672
RABBITMQ_UI_PORT=15672

if [ -t 1 ]; then
  BOLD=$'\033[1m'
  RED=$'\033[31m'
  YELLOW=$'\033[33m'
  GREEN=$'\033[32m'
  RESET=$'\033[0m'
else
  BOLD="" RED="" YELLOW="" GREEN="" RESET=""
fi

info() { printf '%s\n' "$*"; }
warn() { printf '%sWARNING:%s %s\n' "$YELLOW" "$RESET" "$*" >&2; }
die() {
  printf '%sERROR:%s %s\n' "$RED" "$RESET" "$*" >&2
  exit 1
}

usage() {
  cat <<'EOF'
Usage: ./run.sh [command] [options]

Commands:
  start [--no-open]   Build and start everything, wait until healthy (default command).
                      Free host ports are picked automatically. Set NO_OPEN=1 or pass
                      --no-open to skip opening the demo UI in the browser.
  stop                Stop and remove the containers (data volumes are kept).
  reset [-y]          Stop and delete containers AND data volumes (asks first unless -y).
  logs [service]      Follow logs of all services or of one (api, demo, relay, consumer, ...).
  status              Show container state and the URLs of the running stack.
  test                Create .venv if needed, install dev dependencies and run pytest
                      (the tests start their own PostgreSQL and RabbitMQ, so Docker is required).
  help                Show this help.

Environment: API_PORT, DEMO_PORT (preferred host ports; defaults 8000 and 8090),
NO_OPEN, WAIT_TIMEOUT (seconds, default 180).
EOF
}

require_docker() {
  command -v docker >/dev/null 2>&1 ||
    die "docker is not installed or not on PATH. Install Docker: https://docs.docker.com/get-docker/"
  docker compose version >/dev/null 2>&1 ||
    die "'docker compose' (Compose v2 plugin) is not available. Update Docker or install the Compose plugin."
  docker info >/dev/null 2>&1 ||
    die "the Docker daemon is not running. Start Docker Desktop (or the docker service) and retry."
}

# Read KEY from .env without sourcing it (last assignment wins, quotes stripped).
env_file_value() {
  [ -f .env ] || return 0
  local line
  line=$(grep -E "^[[:space:]]*$1=" .env | tail -n 1) || return 0
  line="${line#*=}"
  line="${line%%#*}"
  line="${line#"${line%%[![:space:]]*}"}"
  line="${line%"${line##*[![:space:]]}"}"
  line="${line#\"}"
  line="${line%\"}"
  line="${line#\'}"
  line="${line%\'}"
  printf '%s' "$line"
}

state_value() {
  [ -f "$STATE_FILE" ] || return 0
  grep -E "^$1=" "$STATE_FILE" | tail -n 1 | cut -d= -f2- || true
}

load_state() {
  API_PORT=$(state_value API_PORT)
  DEMO_PORT=$(state_value DEMO_PORT)
  API_PORT="${API_PORT:-${API_PORT_ENV:-$(env_file_value API_PORT)}}"
  DEMO_PORT="${DEMO_PORT:-${DEMO_PORT_ENV:-$(env_file_value DEMO_PORT)}}"
  API_PORT="${API_PORT:-$DEFAULT_API_PORT}"
  DEMO_PORT="${DEMO_PORT:-$DEFAULT_DEMO_PORT}"
  export API_PORT DEMO_PORT
}

port_in_use() {
  # Something accepts TCP connections on this port on loopback.
  (exec 3<>"/dev/tcp/127.0.0.1/$1") >/dev/null 2>&1
}

service_running() {
  [ -n "$(docker compose ps --status running -q "$1" 2>/dev/null)" ]
}

valid_port() {
  case "$1" in
  '' | *[!0-9]*) return 1 ;;
  esac
  [ "$1" -ge 1 ] && [ "$1" -le 65535 ]
}

# find_free_port <preferred> <port-to-avoid>
find_free_port() {
  local port="$1" avoid="${2:-}"
  valid_port "$port" || die "invalid port '$port'"
  while port_in_use "$port" || [ "$port" = "$avoid" ]; do
    port=$((port + 1))
    [ "$port" -le 65535 ] || die "no free port found"
  done
  printf '%s' "$port"
}

choose_ports() {
  local want_api="${API_PORT_ENV:-$(env_file_value API_PORT)}"
  local want_demo="${DEMO_PORT_ENV:-$(env_file_value DEMO_PORT)}"
  want_api="${want_api:-$DEFAULT_API_PORT}"
  want_demo="${want_demo:-$DEFAULT_DEMO_PORT}"
  valid_port "$want_api" || die "API_PORT '$want_api' is not a valid port"
  valid_port "$want_demo" || die "DEMO_PORT '$want_demo' is not a valid port"

  if service_running api && service_running demo && [ -f "$STATE_FILE" ]; then
    # Our own containers hold the ports: keep them so a second ./run.sh is a no-op update.
    load_state
    return
  fi

  API_PORT=$(find_free_port "$want_api" "")
  DEMO_PORT=$(find_free_port "$want_demo" "$API_PORT")
  [ "$API_PORT" = "$want_api" ] || info "Port $want_api is busy, using API_PORT=$API_PORT instead."
  [ "$DEMO_PORT" = "$want_demo" ] || info "Port $want_demo is busy, using DEMO_PORT=$DEMO_PORT instead."
  export API_PORT DEMO_PORT
  printf 'API_PORT=%s\nDEMO_PORT=%s\n' "$API_PORT" "$DEMO_PORT" >"$STATE_FILE"
}

warn_fixed_ports() {
  service_running rabbitmq && return 0
  local port
  for port in "$RABBITMQ_AMQP_PORT" "$RABBITMQ_UI_PORT"; do
    if port_in_use "$port"; then
      warn "host port $port (RabbitMQ) is already in use. It is fixed in docker-compose.yml, so the rabbitmq container will fail to start. Free the port (or change it in docker-compose.yml) and rerun."
    fi
  done
}

ensure_env_file() {
  if [ -f .env ]; then
    return
  fi
  [ -f .env.example ] || die ".env.example is missing, cannot create .env"
  cp .env.example .env
  info "Created .env from .env.example."
}

# The key the running api container really uses (Compose resolved .env, the shell environment
# and quoting rules); asking the container avoids re-implementing that parsing here.
api_key() {
  local key
  key=$(docker compose exec -T api printenv API_KEY 2>/dev/null) || key=""
  printf '%s' "${key:-<unavailable: the api container is not running>}"
}

rabbit_credentials() {
  local user pass
  user=$(env_file_value RABBITMQ_USER)
  pass=$(env_file_value RABBITMQ_PASSWORD)
  printf '%s / %s' "${user:-guest}" "${pass:-guest}"
}

# Printed only when the docs routes are enabled (they are public, for local development).
docs_summary_line() {
  case "$(docker compose exec -T api printenv API_DOCS_ENABLED 2>/dev/null || true)" in
  true)
    printf '  %sAPI docs%s     http://127.0.0.1:%s/docs  (public, API_DOCS_ENABLED=true)' \
      "$BOLD" "$RESET" "$API_PORT"
    ;;
  *) printf '  %sAPI docs%s     disabled (set API_DOCS_ENABLED=true in .env to enable /docs)' "$BOLD" "$RESET" ;;
  esac
}

print_summary() {
  cat <<EOF

${GREEN}${BOLD}Payment service is up.${RESET}

  ${BOLD}Demo UI${RESET}      http://127.0.0.1:${DEMO_PORT}/
  ${BOLD}API base URL${RESET} http://127.0.0.1:${API_PORT}/api/v1
$(docs_summary_line)
  ${BOLD}RabbitMQ UI${RESET}  http://127.0.0.1:${RABBITMQ_UI_PORT}  (login: $(rabbit_credentials))
  ${BOLD}API key${RESET}      $(api_key)   (send as the X-API-Key header)

  Webhook receivers inside the compose network:
    http://demo:9100/hook        always 200
    http://demo:9100/hook/fail   always 500 (shows retries and the DLQ)

  Logs:   ./run.sh logs [service]
  Status: ./run.sh status
  Stop:   ./run.sh stop        (./run.sh reset also deletes the data)
  Tests:  ./run.sh test        (needs Docker; starts its own PostgreSQL and RabbitMQ)
EOF
}

open_browser() {
  local url="$1" opener=""
  if command -v open >/dev/null 2>&1; then
    opener=open
  elif command -v xdg-open >/dev/null 2>&1; then
    opener=xdg-open
  fi
  if [ -n "$opener" ]; then
    "$opener" "$url" >/dev/null 2>&1 || info "Could not open the browser, open $url manually."
  else
    info "No browser opener found, open $url manually."
  fi
}

# Health status of a service as reported by Docker from the container's own healthcheck
# (empty if the container does not exist yet). The api healthcheck is authenticated inside
# the container with the key Compose actually gave it, so run.sh never re-parses .env.
service_health() {
  local id
  id=$(docker compose ps -q "$1" 2>/dev/null | head -n 1) || return 0
  [ -n "$id" ] || return 0
  docker inspect -f '{{.State.Health.Status}}' "$id" 2>/dev/null || true
}

# Prints a failure reason if the one-shot migrate container finished with an error.
migrate_failure() {
  local id state code
  id=$(docker compose ps -a -q migrate 2>/dev/null | head -n 1) || return 0
  [ -n "$id" ] || return 0
  state=$(docker inspect -f '{{.State.Status}}' "$id" 2>/dev/null || true)
  code=$(docker inspect -f '{{.State.ExitCode}}' "$id" 2>/dev/null || true)
  if [ "$state" = "exited" ] && [ "$code" != "0" ]; then
    printf 'the migrate container exited with code %s' "$code"
  fi
}

# Prints a failure reason if api or demo is unhealthy or its container has exited.
unhealthy_service() {
  local service id state
  for service in api demo; do
    if [ "$(service_health "$service")" = unhealthy ]; then
      printf 'the %s container is unhealthy' "$service"
      return 0
    fi
    id=$(docker compose ps -a -q "$service" 2>/dev/null | head -n 1) || continue
    [ -n "$id" ] || continue
    state=$(docker inspect -f '{{.State.Status}}' "$id" 2>/dev/null || true)
    if [ "$state" = exited ] || [ "$state" = dead ]; then
      printf 'the %s container has %s' "$service" "$state"
      return 0
    fi
  done
}

on_interrupt() {
  printf '\n'
  info "Interrupted. The containers keep running in the background; use ./run.sh status or ./run.sh stop."
  exit 130
}

wait_until_ready() {
  local start now elapsed reason spin='|/-\' i=0 api_ok demo_ok
  start=$(date +%s)
  trap on_interrupt INT TERM
  while true; do
    api_ok="$(service_health api)"
    demo_ok="$(service_health demo)"
    api_ok="${api_ok:-waiting}"
    demo_ok="${demo_ok:-waiting}"
    if [ "$api_ok" = healthy ] && [ "$demo_ok" = healthy ]; then
      printf '\r\033[K'
      trap - INT TERM
      return 0
    fi
    reason=$(migrate_failure)
    if [ -z "$reason" ]; then
      reason=$(unhealthy_service)
    fi
    if [ -n "$reason" ]; then
      printf '\r\033[K'
      trap - INT TERM
      fail_with_diagnostics "$reason"
    fi
    now=$(date +%s)
    elapsed=$((now - start))
    if [ "$elapsed" -ge "$WAIT_TIMEOUT" ]; then
      printf '\r\033[K'
      trap - INT TERM
      fail_with_diagnostics "timed out after ${WAIT_TIMEOUT}s waiting for the api and demo containers"
    fi
    printf '\r%s Waiting for services (api: %s, demo: %s) %ss/%ss ' \
      "${spin:i%4:1}" "$api_ok" "$demo_ok" "$elapsed" "$WAIT_TIMEOUT"
    i=$((i + 1))
    sleep 1
  done
}

fail_with_diagnostics() {
  printf '%sERROR:%s %s\n\n' "$RED" "$RESET" "$1" >&2
  docker compose ps -a >&2 || true
  printf '\nLast log lines:\n' >&2
  docker compose logs --tail 40 >&2 || true
  exit 1
}

cmd_start() {
  local open_page=1
  [ "${NO_OPEN:-0}" = "1" ] && open_page=0
  while [ $# -gt 0 ]; do
    case "$1" in
    --no-open) open_page=0 ;;
    *) die "unknown option '$1' (see ./run.sh help)" ;;
    esac
    shift
  done
  require_docker
  ensure_env_file
  choose_ports
  warn_fixed_ports
  info "Starting the stack (first build can take a few minutes)..."
  docker compose up --build -d || fail_with_diagnostics "docker compose up failed"
  wait_until_ready
  print_summary
  if [ "$open_page" = 1 ]; then
    open_browser "http://127.0.0.1:${DEMO_PORT}/"
  fi
}

cmd_stop() {
  require_docker
  load_state
  docker compose down
}

cmd_reset() {
  local yes=0
  while [ $# -gt 0 ]; do
    case "$1" in
    -y | --yes) yes=1 ;;
    *) die "unknown option '$1' (see ./run.sh help)" ;;
    esac
    shift
  done
  require_docker
  load_state
  if [ "$yes" != 1 ]; then
    printf 'This deletes the containers AND the database/RabbitMQ volumes. Continue? [y/N] '
    local answer=""
    read -r answer || true
    case "$answer" in
    y | Y | yes | YES) ;;
    *)
      info "Aborted."
      return 0
      ;;
    esac
  fi
  docker compose down -v
  rm -f "$STATE_FILE"
}

cmd_logs() {
  require_docker
  load_state
  docker compose logs -f --tail 100 "$@"
}

cmd_status() {
  require_docker
  load_state
  docker compose ps -a
  if service_running api && service_running demo; then
    print_summary
  else
    info ""
    info "The stack is not fully running. Start it with ./run.sh"
  fi
}

cmd_test() {
  require_docker
  command -v python3 >/dev/null 2>&1 || die "python3 is required to run the tests"
  info "Note: the tests start their own PostgreSQL and RabbitMQ containers, so Docker must keep running."
  if [ ! -d .venv ]; then
    python3 -m venv .venv
  fi
  .venv/bin/python -m pip install --quiet -e '.[dev]'
  .venv/bin/python -m pytest "$@"
}

main() {
  local command="${1:-start}"
  [ $# -gt 0 ] && shift
  # Remember which ports the user asked for explicitly, before load_state overwrites them.
  API_PORT_ENV="${API_PORT:-}"
  DEMO_PORT_ENV="${DEMO_PORT:-}"
  case "$command" in
  start | up) cmd_start "$@" ;;
  --no-open) cmd_start --no-open "$@" ;;
  stop | down) cmd_stop ;;
  reset) cmd_reset "$@" ;;
  logs) cmd_logs "$@" ;;
  status) cmd_status ;;
  test) cmd_test "$@" ;;
  help | -h | --help) usage ;;
  *)
    usage >&2
    exit 2
    ;;
  esac
}

main "$@"
