$running = $false
[int]$count = 1

if ($env:ERLANG_HOME) {
    $epmd = Join-Path -Path $env:ERLANG_HOME 'erts-8.3\bin\epmd.exe'
} else {
    $epmd = 'C:\Users\appveyor\erlang\erts-8.3\bin\epmd.exe'
}

Do {
    $running = & $epmd -names | Select-String -CaseSensitive -SimpleMatch -Quiet -Pattern 'name rabbit at port 25672'
    if ($running -eq $true) {
        Write-Host '[INFO] epmd reports that RabbitMQ is at port 25672'
        break
    }

    if ($count -gt 120) {
        throw '[ERROR] too many tries waiting for epmd to report RabbitMQ on port 25672'
    }

    Write-Host "[INFO] epmd NOT reporting yet that RabbitMQ is at port 25672, count: $count"
    $count = $count + 1
    Start-Sleep -Seconds 5

} While ($true)
