[int]$count = 1

Do {
    $proc_id = (Get-Process -Name erl).Id
    if (-Not ($proc_id -is [array])) {
        & "C:\Program Files\RabbitMQ Server\rabbitmq_server-$env:rabbitmq_version\sbin\rabbitmqctl.bat" wait -t 300000 -P $proc_id
        if ($LASTEXITCODE -ne 0) {
            throw "[ERROR] rabbitmqctl wait returned error: $LASTEXITCODE"
        }
        break
    }

    if ($count -gt 120) {
        throw '[ERROR] too many tries waiting for just one erl process to be running'
    }

    Write-Host '[INFO] multiple erl instances running still'
    $count = $count + 1
    Start-Sleep -Seconds 5

} While ($true)
