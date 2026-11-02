[CmdletBinding()]
param(
    [ValidateSet('All', 'Start', 'Download', 'Upload', 'Verify', 'Status', 'Stop')]
    [string]$Action = 'All',
    [ValidatePattern('^\d{4}-(0[1-9]|1[0-2])$')]
    [string]$Month = '2023-01',
    [string]$DockerContext = 'desktop-linux'
)

$ErrorActionPreference = 'Stop'
$ProjectRoot = Split-Path -Parent $PSScriptRoot
$ComposePath = Join-Path $ProjectRoot 'compose.yaml'
$RunId = (Get-Date).ToUniversalTime().ToString('yyyyMMddTHHmmssfffZ')
$RunDirectory = Join-Path $ProjectRoot "logs\$RunId"
$Utf8 = New-Object System.Text.UTF8Encoding($false)
$ComposePrefix = @('compose', '--project-name', 'bigdata-taxi', '--file', $ComposePath)
$DockerPrefix = @('--context', $DockerContext)
$Stopwatch = [System.Diagnostics.Stopwatch]::StartNew()
New-Item -ItemType Directory -Force -Path $RunDirectory, (Join-Path $ProjectRoot 'logs\spark-events'), (Join-Path $ProjectRoot 'data\raw') | Out-Null

function Save-Json {
    param($Value, [string]$Name)
    [System.IO.File]::WriteAllText((Join-Path $RunDirectory $Name), ($Value | ConvertTo-Json -Depth 30), $Utf8)
}

function Invoke-DockerChecked {
    param([string[]]$CommandArguments, [string]$LogName, [switch]$Quiet)
    $oldPreference = $ErrorActionPreference
    # Windows PowerShell 5 represents native stderr as ErrorRecord objects.
    # Docker/Spark write normal progress there; success is decided by exit code.
    $ErrorActionPreference = 'Continue'
    try {
        $lines = @(& docker @DockerPrefix @CommandArguments 2>&1 | ForEach-Object {
            $text = $_.ToString()
            if (-not $Quiet) { Write-Host $text }
            $text
        })
        $exitCode = $LASTEXITCODE
    }
    finally { $ErrorActionPreference = $oldPreference }
    if ($LogName) { [System.IO.File]::WriteAllLines((Join-Path $RunDirectory $LogName), [string[]]$lines, $Utf8) }
    if ($exitCode -ne 0) { throw "Docker command failed (exit $exitCode): $($CommandArguments -join ' ')" }
    return $lines
}

function Invoke-ComposeChecked {
    param([string[]]$CommandArguments, [string]$LogName, [switch]$Quiet)
    Invoke-DockerChecked -CommandArguments ($ComposePrefix + $CommandArguments) -LogName $LogName -Quiet:$Quiet
}

function Wait-ForRegistrations {
    $deadline = (Get-Date).AddMinutes(3)
    do {
        try {
            $master = Invoke-RestMethod -Uri 'http://127.0.0.1:8080/json/' -TimeoutSec 5
            $nameNode = Invoke-RestMethod -Uri 'http://127.0.0.1:9870/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState' -TimeoutSec 5
            $workers = @($master.workers | Where-Object { $_.state -eq 'ALIVE' })
            if ($workers.Count -eq 1 -and $nameNode.beans[0].NumLiveDataNodes -eq 1) {
                Save-Json $master 'spark-master.json'
                Save-Json $nameNode 'namenode-jmx.json'
                Write-Host 'READY: one registered Spark worker and one live HDFS DataNode.'
                return
            }
        }
        catch { Write-Host 'Waiting for worker/DataNode registration...' }
        Start-Sleep -Seconds 2
    } while ((Get-Date) -lt $deadline)
    throw 'Services started but worker/DataNode registration did not complete within 180 seconds.'
}

function Download-Inputs {
    Write-Host "Downloading/verifying original TLC input for $Month..."
    $downloadScript = Join-Path $PSScriptRoot 'download_data.py'
    $oldPreference = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        & python $downloadScript --month $Month 2>&1 | ForEach-Object { Write-Host $_.ToString() }
        $exitCode = $LASTEXITCODE
    }
    finally { $ErrorActionPreference = $oldPreference }
    if ($exitCode -ne 0) { throw "Input download failed (exit $exitCode)." }
}

function Upload-Inputs {
    Wait-ForRegistrations
    $manifestPath = Join-Path $ProjectRoot "data\raw\manifest-$Month.json"
    if (-not (Test-Path -LiteralPath $manifestPath)) { throw 'Input manifest is missing. Run Download first.' }
    $manifest = Get-Content -Raw -LiteralPath $manifestPath | ConvertFrom-Json
    $trip = @($manifest.files | Where-Object { $_.name -eq "yellow_tripdata_$Month.parquet" })
    $lookup = @($manifest.files | Where-Object { $_.name -eq 'taxi_zone_lookup.csv' })
    if ($trip.Count -ne 1 -or $lookup.Count -ne 1) { throw 'Manifest must contain exactly one trip file and one lookup file.' }
    foreach ($name in @("yellow_tripdata_$Month.parquet", 'taxi_zone_lookup.csv', "manifest-$Month.json")) {
        $localPath = Join-Path $ProjectRoot "data\raw\$name"
        [void](Invoke-ComposeChecked -CommandArguments @('cp', $localPath, "namenode:/opt/project/data/raw/$name") -LogName "copy-$name.log")
    }
    [void](Invoke-ComposeChecked -CommandArguments @('exec', '-T', 'namenode', '/bin/bash', '/opt/project/import-hdfs.sh', $Month, $trip[0].sha256, $lookup[0].sha256) -LogName 'hdfs-import.log')
    Copy-Item -LiteralPath $manifestPath -Destination (Join-Path $RunDirectory 'input-manifest.json')
}

function Verify-Inputs {
    Wait-ForRegistrations
    [void](Invoke-ComposeChecked -CommandArguments @('exec', '-T', 'namenode', '/opt/hadoop/bin/hdfs', 'dfsadmin', '-report') -LogName 'hdfs-report.txt')
    [void](Invoke-ComposeChecked -CommandArguments @('exec', '-T', 'spark-master', '/opt/spark/bin/spark-submit', '--driver-memory', '1g', '--executor-memory', '1g', '--total-executor-cores', '1', '/opt/project/src/verify_input.py', '--month', $Month, '--run-id', $RunId) -LogName 'spark-submit.log')
    [void](Invoke-ComposeChecked -CommandArguments @('cp', "spark-master:/opt/project/logs/$RunId/.", $RunDirectory) -LogName 'copy-spark-results.log')
    [void](Invoke-ComposeChecked -CommandArguments @('cp', 'spark-master:/opt/project/logs/spark-events/.', (Join-Path $ProjectRoot 'logs\spark-events')) -LogName 'copy-event-logs.log')
    [void](Invoke-ComposeChecked -CommandArguments @('exec', '-T', 'namenode', '/opt/hadoop/bin/hdfs', 'fsck', '/taxi', '-files', '-blocks') -LogName 'hdfs-fsck.txt')
    $result = Get-Content -Raw -LiteralPath (Join-Path $RunDirectory 'spark-read.json') | ConvertFrom-Json
    if ($result.status -ne 'passed') { throw 'Spark read verification did not pass.' }
    Write-Host "VERIFIED: $($result.trip_rows) trip rows; $($result.lookup_rows) zone rows."
}

Start-Transcript -Path (Join-Path $RunDirectory 'transcript.txt') | Out-Null
try {
    Write-Host "Step 1: action=$Action month=$Month run_id=$RunId"
    $sourceFiles = @('compose.yaml', 'docker\hadoop.Dockerfile', 'docker\spark.Dockerfile', 'config\hadoop\core-site.xml', 'config\hadoop\hdfs-site.xml', 'config\spark\spark-defaults.conf', 'scripts\step1.ps1', 'scripts\download_data.py', 'scripts\hdfs-entrypoint.sh', 'scripts\import-hdfs.sh', 'src\verify_input.py')
    $sourceHashes = @($sourceFiles | ForEach-Object {
        [ordered]@{ path = $_; sha256 = (Get-FileHash -Algorithm SHA256 -LiteralPath (Join-Path $ProjectRoot $_)).Hash.ToLowerInvariant() }
    })
    Save-Json $sourceHashes 'source-hashes.json'
    $dockerInfo = Invoke-DockerChecked -CommandArguments @('info', '--format', '{{.NCPU}}|{{.MemTotal}}|{{.ServerVersion}}|{{.OSType}}') -LogName 'docker-capacity.txt' -Quiet
    $capacity = ($dockerInfo -join "").Split('|')
    if ($capacity.Count -ne 4) { throw 'Unexpected Docker capacity response.' }
    $engine = [pscustomobject]@{
        CPUs = [int]$capacity[0]; memory_bytes = [long]$capacity[1]
        server_version = $capacity[2]; os_type = $capacity[3]
    }
    Save-Json $engine 'docker-info.json'
    if ($engine.os_type -ne 'linux') { throw 'Select Linux containers in Docker Desktop first.' }
    Write-Host ('Docker capacity: {0} CPUs, {1:N2} GiB RAM.' -f $engine.CPUs, ($engine.memory_bytes / 1GB))
    if ($engine.memory_bytes -lt 3GB) { Write-Warning 'Docker has less than 3 GiB RAM; the complete verification may run out of memory.' }
    [void](Invoke-ComposeChecked -CommandArguments @('config', '--quiet') -LogName 'compose-validation.log')

    if ($Action -in @('All', 'Start')) {
        [void](Invoke-ComposeChecked -CommandArguments @('build') -LogName 'compose-build.log')
        [void](Invoke-ComposeChecked -CommandArguments @('up', '-d', '--wait', '--wait-timeout', '240') -LogName 'compose-up.log')
        Wait-ForRegistrations
    }
    if ($Action -in @('All', 'Download')) { Download-Inputs }
    if ($Action -in @('All', 'Upload')) { Upload-Inputs }
    if ($Action -in @('All', 'Verify')) { Verify-Inputs }
    if ($Action -eq 'Stop') {
        # No -v: preserve NameNode/DataNode volumes and original input.
        [void](Invoke-ComposeChecked -CommandArguments @('down') -LogName 'compose-down.log')
    }
    [void](Invoke-ComposeChecked -CommandArguments @('ps', '--all') -LogName 'compose-ps.txt')
    if ($Action -ne 'Stop') {
        [void](Invoke-ComposeChecked -CommandArguments @('logs', '--no-color', '--tail', '100') -LogName 'services.log' -Quiet)
        $ids = @(Invoke-ComposeChecked -CommandArguments @('ps', '-q') -Quiet)
        if ($ids.Count -gt 0) {
            [void](Invoke-DockerChecked -CommandArguments (@('stats', '--no-stream', '--format', '{{json .}}') + $ids) -LogName 'container-stats.jsonl' -Quiet)
        }
    }
    if ($Action -in @('All', 'Start', 'Verify')) {
        [void](Invoke-DockerChecked -CommandArguments @('image', 'inspect', 'apache/hadoop:3.4.1', 'apache/spark:3.5.8-scala2.12-java17-python3-ubuntu', '--format', '{{json .RepoDigests}}') -LogName 'image-digests.jsonl' -Quiet)
        [void](Invoke-DockerChecked -CommandArguments @('image', 'inspect', 'bigdata-taxi-hadoop:3.4.1', 'bigdata-taxi-spark:3.5.8', '--format', '{{.Id}}') -LogName 'project-image-ids.txt' -Quiet)
    }
    $Stopwatch.Stop()
    Save-Json ([ordered]@{
        status = 'passed'; action = $Action; month = $Month; run_id = $RunId
        finished_at_utc = (Get-Date).ToUniversalTime().ToString('o')
        wall_seconds = [math]::Round($Stopwatch.Elapsed.TotalSeconds, 3)
        compose_sha256 = (Get-FileHash -Algorithm SHA256 -LiteralPath $ComposePath).Hash.ToLowerInvariant()
        scope = 'Infrastructure and original data only; no cleaning or analytics'
    }) 'run-summary.json'
    Write-Host "DONE. Evidence: $RunDirectory"
}
catch {
    $failureMessage = $_.Exception.Message
    $Stopwatch.Stop()
    Save-Json ([ordered]@{
        status = 'failed'; action = $Action; month = $Month; run_id = $RunId
        error = $failureMessage
        finished_at_utc = (Get-Date).ToUniversalTime().ToString('o')
        wall_seconds = [math]::Round($Stopwatch.Elapsed.TotalSeconds, 3)
    }) 'run-summary.json'
    try { [void](Invoke-ComposeChecked -CommandArguments @('logs', '--no-color', '--tail', '150') -LogName 'services-failure.log' -Quiet) } catch { }
    Write-Host "FAILED: $failureMessage" -ForegroundColor Red
    Write-Host "Evidence: $RunDirectory"
    exit 1
}
finally { Stop-Transcript | Out-Null }
