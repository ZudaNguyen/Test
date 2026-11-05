[CmdletBinding()]
param(
    [ValidatePattern('^\d{4}-(0[1-9]|1[0-2])$')][string]$Month = '2023-01',
    [string]$DockerContext = 'desktop-linux',
    [switch]$SkipTests
)

$ErrorActionPreference = 'Stop'
$ProjectRoot = Split-Path -Parent $PSScriptRoot
$RunId = 'p2-' + $Month.Replace('-', '') + '-' + (Get-Date).ToUniversalTime().ToString('yyyyMMddTHHmmssfffZ') + '-' + [guid]::NewGuid().ToString('N').Substring(0, 6)
$ReportDirectory = Join-Path $ProjectRoot "reports\phase2\$RunId"
$ContainerReportDirectory = "/tmp/tlc-phase2/$RunId"
$Utf8 = New-Object System.Text.UTF8Encoding($false)
New-Item -ItemType Directory -Force -Path $ReportDirectory | Out-Null

function Invoke-Phase2Docker {
    param([string[]]$CommandArguments, [string]$LogName)
    $previousPreference = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        & docker --context $DockerContext @CommandArguments 2>&1 | ForEach-Object {
            $line = $_.ToString()
            Write-Host $line
            if ($LogName) { [System.IO.File]::AppendAllText((Join-Path $ReportDirectory $LogName), $line + [Environment]::NewLine, $Utf8) }
        }
        $commandExitCode = $LASTEXITCODE
    }
    finally { $ErrorActionPreference = $previousPreference }
    if ($commandExitCode -ne 0) { throw "Docker command failed (exit $commandExitCode): $($CommandArguments -join ' ')" }
}

function Save-RunnerStatus {
    param([string]$Status, [string]$Message)
    $record = [ordered]@{
        status = $Status; run_id = $RunId; month = $Month; message = $Message
        finished_at_utc = (Get-Date).ToUniversalTime().ToString('o')
    }
    [System.IO.File]::WriteAllText((Join-Path $ReportDirectory 'runner-status.json'), ($record | ConvertTo-Json), $Utf8)
}

try {
    Write-Host "Phase 2 run: $RunId"
    $master = Invoke-RestMethod -Uri 'http://127.0.0.1:8080/json/' -TimeoutSec 10
    $node = Invoke-RestMethod -Uri 'http://127.0.0.1:9870/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState' -TimeoutSec 10
    if (@($master.workers | Where-Object { $_.state -eq 'ALIVE' }).Count -lt 1 -or $node.beans[0].NumLiveDataNodes -lt 1) {
        throw 'Cluster not ready. Run scripts/start_windows.ps1 first.'
    }
    $sourcePaths = @('src\clean_data.py', 'src\test_cleaning_rules.py', 'scripts\run_phase2.ps1', 'docker-compose.yml', 'compose.windows.yml')
    $sourceHashes = @($sourcePaths | ForEach-Object {
        [ordered]@{ path = $_; sha256 = (Get-FileHash -LiteralPath (Join-Path $ProjectRoot $_) -Algorithm SHA256).Hash.ToLowerInvariant() }
    })
    $inputs = @("data\raw\yellow_tripdata_$Month.parquet", 'data\raw\taxi_zone_lookup.csv')
    $inputHashes = @($inputs | ForEach-Object {
        $path = Join-Path $ProjectRoot $_
        [ordered]@{ path = $_; bytes = (Get-Item -LiteralPath $path).Length; sha256 = (Get-FileHash -LiteralPath $path -Algorithm SHA256).Hash.ToLowerInvariant() }
    })
    [System.IO.File]::WriteAllText((Join-Path $ReportDirectory 'source-input-hashes.json'), ([ordered]@{ sources = $sourceHashes; inputs = $inputHashes } | ConvertTo-Json -Depth 8), $Utf8)

    Invoke-Phase2Docker -CommandArguments @('cp', (Join-Path $ProjectRoot 'src\.'), 'spark-master:/opt/spark/work-dir') -LogName 'prepare.log'
    if (-not $SkipTests) {
        Invoke-Phase2Docker -CommandArguments @('exec', 'spark-master', '/opt/spark/bin/spark-submit', '--master', 'local[1]', '--driver-memory', '512m', '/opt/spark/work-dir/test_cleaning_rules.py') -LogName 'synthetic-tests.log'
    }
    Invoke-Phase2Docker -CommandArguments @('cp', (Join-Path $ProjectRoot 'data\raw\taxi_zone_lookup.csv'), 'namenode:/local-data/raw/taxi_zone_lookup.csv') -LogName 'prepare.log'
    Invoke-Phase2Docker -CommandArguments @('exec', 'namenode', 'bash', '/scripts/upload_to_hdfs.sh', '/local-data/raw/taxi_zone_lookup.csv', '/user/tlc/reference') -LogName 'lookup-upload.log'
    Invoke-Phase2Docker -CommandArguments @(
        'exec', 'spark-master', '/opt/spark/bin/spark-submit',
        '--master', 'spark://spark-master:7077', '--driver-memory', '768m',
        '--executor-memory', '1g', '--total-executor-cores', '2',
        '--conf', 'spark.sql.shuffle.partitions=4', '--conf', 'spark.default.parallelism=2',
        '/opt/spark/work-dir/clean_data.py', '--month', $Month, '--run-id', $RunId,
        '--report-dir', $ContainerReportDirectory
    ) -LogName 'spark-cleaning.log'
    Invoke-Phase2Docker -CommandArguments @('cp', "spark-master:$ContainerReportDirectory/.", $ReportDirectory) -LogName 'export.log'
    $reportPath = Join-Path $ReportDirectory 'quality_report.json'
    $report = Get-Content -Raw -Encoding UTF8 -LiteralPath $reportPath | ConvertFrom-Json
    if ($report.status -ne 'passed') { throw 'Cleaning report did not pass.' }
    $report.quality.sequential_rule_counts | Select-Object step, rule, input_rows, removed_rows, output_rows |
        Export-Csv -LiteralPath (Join-Path $ReportDirectory 'rule-counts.csv') -NoTypeInformation -Encoding UTF8
    Copy-Item -LiteralPath $reportPath -Destination (Join-Path $ProjectRoot 'reports\phase2\latest.json') -Force
    Save-RunnerStatus -Status 'passed' -Message 'Cleaning, persisted output checks and report export passed.'
    Write-Host "PHASE 2 PASSED: input=$($report.quality.input_rows), cleaned=$($report.quality.cleaned_rows), rejected=$($report.quality.rejected_rows)"
    Write-Host "Report: $reportPath"
    Write-Host "Cleaned HDFS: $($report.outputs.cleaned)"
    Write-Host ($report.quality.sequential_rule_counts | Format-Table step, rule, input_rows, removed_rows, output_rows -AutoSize | Out-String)
}
catch {
    $failureMessage = $_.Exception.Message
    try { Invoke-Phase2Docker -CommandArguments @('cp', "spark-master:$ContainerReportDirectory/.", $ReportDirectory) -LogName 'failure-export.log' } catch { }
    Save-RunnerStatus -Status 'failed' -Message $failureMessage
    Write-Host "FAILED: $failureMessage" -ForegroundColor Red
    Write-Host "Logs: $ReportDirectory"
    exit 1
}
