param(
    [ValidateSet('baseline', 'scheduler')][string]$Variant = 'scheduler',
    [string]$SourcePath = (Split-Path $PSScriptRoot -Parent),
    [Parameter(Mandatory)][string]$OutputDirectory,
    [string]$GoImage = 'golang:1.25.0',
    [string]$ModuleCache = 'cursus-recovery-measurement-modules',
    [switch]$Offline
)
# Requires PowerShell 7, Docker Compose and a local Go toolchain. SourcePath may
# name an instrumented baseline checkout; the harness always comes from this tree.
$ErrorActionPreference = 'Stop'
$testPath = (Resolve-Path (Split-Path $PSScriptRoot -Parent)).Path.Replace('\', '/')
$sourcePath = (Resolve-Path $SourcePath).Path.Replace('\', '/')
New-Item -ItemType Directory -Force -Path $OutputDirectory | Out-Null
$resultsPath = (Resolve-Path $OutputDirectory).Path.Replace('\', '/')
$project = "cursus-recovery-$Variant"
$existing = & docker ps -aq --filter "name=^/$project-broker-"
if ($LASTEXITCODE -ne 0 -or $existing) { throw 'Refusing to reuse existing measurement containers' }
$binary = "$resultsPath/broker-$Variant"
$buildOptions = @()
if ($Offline) { $buildOptions = @('--network', 'none', '-e', 'GOPROXY=off') }
& docker run --rm --name "$project-build" --cpus 2 --memory 2g @buildOptions --mount "type=bind,source=$sourcePath,target=/src,readonly" --mount "type=bind,source=$resultsPath,target=/out" --mount "type=volume,source=$ModuleCache,target=/go/pkg/mod" --mount type=volume,source=cursus-recovery-measurement-build,target=/root/.cache/go-build -e GOMAXPROCS=2 -w /src $GoImage go build -tags e2e_faults -o "/out/broker-$Variant" ./cmd/broker
if ($LASTEXITCODE -ne 0) { throw 'Broker build failed' }
$composePath = "$resultsPath/compose-$Variant.yml"
$lines = [System.Collections.Generic.List[string]]::new()
$lines.Add('services:')
foreach ($node in 1..3) {
    $brokerPort = 29000 + $node
    $discoveryPort = 27999 + $node
    $exporterPort = 29100 + $node
    $healthPort = 29080 + $node
    $lines.Add(@"
  broker-$node`:
    image: $GoImage
    container_name: $project-broker-$node
    working_dir: /app
    command: /app/broker
    cpus: 2
    mem_limit: 2g
    environment:
      CONFIG_PATH: /app/config.yaml
      ADVERTISED_HOST: broker-$node
      ADVERTISED_CLIENT_HOST: localhost
      ADVERTISED_BROKER_PORT: $brokerPort
      GOMAXPROCS: 2
      CURSUS_E2E_FAULTS: 1
    ports:
      - "127.0.0.1:$brokerPort`:9000"
      - "127.0.0.1:$discoveryPort`:8000"
      - "127.0.0.1:$exporterPort`:9100"
      - "127.0.0.1:$healthPort`:9080"
    volumes:
      - "$binary`:/app/broker:ro"
      - "$testPath/test/cluster/config.yaml:/app/config.yaml:ro"
      - data-$node`:/app/cluster-logs
"@)
}
$lines.Add("volumes:`n  data-1:`n  data-2:`n  data-3:")
[IO.File]::WriteAllText($composePath, ($lines -join "`n"), [Text.UTF8Encoding]::new($false))
$envNames = @('CURSUS_E2E_CONTAINER_PREFIX', 'CURSUS_E2E_PORT_SHIFT', 'CURSUS_RECOVERY_E2E_RESULT', 'GOMAXPROCS')
$saved = @{}
foreach ($name in $envNames) { $saved[$name] = [Environment]::GetEnvironmentVariable($name, 'Process') }
try {
    & docker compose -p $project -f $composePath up -d
    if ($LASTEXITCODE -ne 0) { throw 'Isolated cluster startup failed' }
    $env:CURSUS_E2E_CONTAINER_PREFIX = "$project-"
    $env:CURSUS_E2E_PORT_SHIFT = '20000'
    $env:CURSUS_RECOVERY_E2E_RESULT = "$resultsPath/$Variant-cluster.json"
    $env:GOMAXPROCS = '2'
    Push-Location $testPath
    try {
        & go test -p 1 -timeout 12m -count=1 -v -run '^TestRecoveryBudgetMixedLoad$' ./test/e2e-cluster 2>&1 | Tee-Object -FilePath "$resultsPath/$Variant-cluster.log"
        $testExit = $LASTEXITCODE
    } finally { Pop-Location }
    & docker compose -p $project -f $composePath logs --no-color --tail 200 > "$resultsPath/$Variant-brokers.log"
    if ($testExit -ne 0) { throw "Cluster experiment failed: $testExit" }
} finally {
    foreach ($name in $envNames) { [Environment]::SetEnvironmentVariable($name, $saved[$name], 'Process') }
    # Only this script's explicit isolated project owns these volumes.
    if ($project -notmatch '^cursus-recovery-(baseline|scheduler)$') { throw 'Invalid cleanup project' }
    & docker compose -p $project -f $composePath down -v
}
