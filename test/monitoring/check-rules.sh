#!/usr/bin/env bash
set -euo pipefail

# Test the actual rendered PrometheusRule from each chart, not a copied rule.
# Inputs are helm template output files rendered in namespace "brokers".
if (( $# == 0 )); then
  echo "usage: $0 RENDERED_CHART.yaml [...]" >&2
  exit 1
fi
fixture="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/observations.test.yaml"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
cp "$fixture" "$work/tests.yaml"
chmod 755 "$work"
chmod 644 "$work/tests.yaml"

for rendered in "$@"; do
  # Extract the spec of the PrometheusRule document. promtool validates the
  # result, so a missing rule or a change in the rendered structure fails closed.
  awk '
    /^---$/ { rule = 0; spec = 0 }
    /^kind: PrometheusRule$/ { rule = 1 }
    rule && /^spec:$/ { spec = 1; next }
    rule && spec { sub(/^  /, ""); print }
  ' "$rendered" > "$work/rules.yaml"
  test -s "$work/rules.yaml"
  chmod 644 "$work/rules.yaml"
  echo "Testing monitoring rules from $rendered"
  docker run --rm --network none --entrypoint /bin/promtool \
    --mount "type=bind,source=$work,target=/work,readonly" -w /work \
    prom/prometheus:v3.5.5 test rules tests.yaml
done
