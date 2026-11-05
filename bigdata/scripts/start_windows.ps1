[CmdletBinding()]
param([string]$DockerContext = 'desktop-linux')

$ErrorActionPreference = 'Stop'
$ProjectRoot = Split-Path -Parent $PSScriptRoot
$ComposeArguments = @(
    'compose', '--project-name', 'bigdata',
    '--file', (Join-Path $ProjectRoot 'docker-compose.yml'),
    '--file', (Join-Path $ProjectRoot 'compose.windows.yml')
)

function Invoke-CheckedDocker {
    param([string[]]$CommandArguments)
    $previousPreference = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        & docker --context $DockerContext @CommandArguments 2>&1 |
            ForEach-Object { Write-Host $_.ToString() }
        $commandExitCode = $LASTEXITCODE
    }
    finally { $ErrorActionPreference = $previousPreference }
    if ($commandExitCode -ne 0) {
        throw "Docker failed (exit $commandExitCode): $($CommandArguments -join ' ')"
    }
}

Invoke-CheckedDocker -CommandArguments @('info', '--format', 'Docker: {{.OSType}}, CPUs={{.NCPU}}, RAM bytes={{.MemTotal}}')
Invoke-CheckedDocker -CommandArguments ($ComposeArguments + @('config', '--quiet'))
Write-Host 'Recreating TLC containers; HDFS named volumes are preserved.'
Invoke-CheckedDocker -CommandArguments ($ComposeArguments + @('create', '--force-recreate'))

foreach ($serviceName in @('namenode', 'datanode')) {
    foreach ($fileName in @('core-site.xml', 'hdfs-site.xml')) {
        Invoke-CheckedDocker -CommandArguments @(
            'cp', (Join-Path $ProjectRoot "config\$fileName"),
            ($serviceName + ':/opt/hadoop/etc/hadoop/' + $fileName)
        )
    }
}
foreach ($serviceName in @('spark-master', 'spark-worker')) {
    Invoke-CheckedDocker -CommandArguments @(
        'cp', (Join-Path $ProjectRoot 'config'), ($serviceName + ':/opt/hadoop-conf')
    )
}
Invoke-CheckedDocker -CommandArguments @('cp', (Join-Path $ProjectRoot 'scripts'), 'namenode:/scripts')
Invoke-CheckedDocker -CommandArguments @('cp', (Join-Path $ProjectRoot 'data\.'), 'namenode:/local-data')
Invoke-CheckedDocker -CommandArguments @('cp', (Join-Path $ProjectRoot 'src\.'), 'spark-master:/opt/spark/work-dir')
Invoke-CheckedDocker -CommandArguments @('cp', (Join-Path $ProjectRoot 'data\.'), 'spark-master:/data')
Invoke-CheckedDocker -CommandArguments ($ComposeArguments + @('start'))

$readyDeadline = (Get-Date).AddMinutes(3)
do {
    try {
        $masterInfo = Invoke-RestMethod -Uri 'http://127.0.0.1:8080/json/' -TimeoutSec 5
        $nameNodeInfo = Invoke-RestMethod -Uri 'http://127.0.0.1:9870/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState' -TimeoutSec 5
        $liveWorkers = @($masterInfo.workers | Where-Object { $_.state -eq 'ALIVE' })
        if ($liveWorkers.Count -ge 1 -and $nameNodeInfo.beans[0].NumLiveDataNodes -ge 1) {
            Write-Host 'READY: Spark worker and HDFS DataNode registered.'
            Write-Host 'Next: run the upload and Spark test commands in README.md.'
            exit 0
        }
    }
    catch { }
    Start-Sleep -Seconds 3
} while ((Get-Date) -lt $readyDeadline)
throw 'Services did not become ready within 180 seconds. Check docker compose logs.'
