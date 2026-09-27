#!/usr/bin/env bash
set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
run_directory="$(mktemp -d)"
process_ids=()

cleanup() {
	for process_id in "${process_ids[@]}"; do
		kill "${process_id}" 2>/dev/null || true
	done
	for process_id in "${process_ids[@]}"; do
		wait "${process_id}" 2>/dev/null || true
	done
	rm -rf -- "${run_directory}"
}
trap cleanup EXIT INT TERM

if ! command -v k6 >/dev/null 2>&1; then
	echo "k6 is required: https://grafana.com/docs/k6/latest/set-up/install-k6/" >&2
	exit 1
fi
if ! command -v curl >/dev/null 2>&1; then
	echo "curl is required to wait for local services" >&2
	exit 1
fi

cd "${repository_root}"
go build -o "${run_directory}/gatex" ./cmd/gateway
go build -o "${run_directory}/mockbackend" ./cmd/mockbackend

"${run_directory}/mockbackend" -listen 127.0.0.1:18081 -name backend-1 -delay "${BACKEND_DELAY:-2ms}" >"${run_directory}/backend-1.log" 2>&1 &
process_ids+=("$!")
"${run_directory}/mockbackend" -listen 127.0.0.1:18082 -name backend-2 -delay "${BACKEND_DELAY:-2ms}" >"${run_directory}/backend-2.log" 2>&1 &
process_ids+=("$!")
"${run_directory}/gatex" -config loadtest/gateway.yaml >"${run_directory}/gateway.log" 2>&1 &
process_ids+=("$!")

wait_for_url() {
	local name="$1"
	local url="$2"
	for _ in {1..100}; do
		if curl --fail --silent --show-error "${url}" >/dev/null 2>&1; then
			return
		fi
		sleep 0.05
	done
	echo "${name} did not become ready at ${url}" >&2
	sed -n '1,120p' "${run_directory}/${name}.log" >&2 || true
	exit 1
}

wait_for_url backend-1 http://127.0.0.1:18081/healthz
wait_for_url backend-2 http://127.0.0.1:18082/healthz
wait_for_url gateway http://127.0.0.1:18080/readyz

summary_arguments() {
	local scenario="$1"
	if [[ -n "${RESULT_DIR:-}" ]]; then
		mkdir -p "${RESULT_DIR}"
		printf '%s\n' "--summary-export=${RESULT_DIR}/${scenario}.json"
	fi
}

run_scenario() {
	local name="$1"
	local target_url="$2"
	local expect_gateway="$3"
	local summary=()
	while IFS= read -r argument; do
		summary+=("${argument}")
	done < <(summary_arguments "${name}")

	echo
	echo "== ${name}: ${target_url} =="
	TARGET_URL="${target_url}" \
	EXPECT_GATEWAY="${expect_gateway}" \
	RATE="${RATE:-1000}" \
	DURATION="${DURATION:-10s}" \
	k6 run "${summary[@]}" loadtest/gateway.js
}

run_scenario direct-backend http://127.0.0.1:18081/api/items false
run_scenario gateway http://127.0.0.1:18080/api/items true
