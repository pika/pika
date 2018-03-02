$running = $false
[int]$count = 1

$epmd = [System.IO.Path]::Combine($env:ERLANG_HOME, $env:erlang_erts_version, "bin", "epmd.exe")

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
