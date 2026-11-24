# Resolve the RabbitMQ and Erlang versions the Windows CI installs (#1857).
# A release is listed before its assets finish uploading, so take the newest
# release whose Windows installer is already attached.

function Get-NewestReleaseWithAsset
{
    param(
        [Parameter(Mandatory = $true)][string]$Repo,
        [Parameter(Mandatory = $true)][string]$TagPattern,
        [Parameter(Mandatory = $true)][string]$TagPrefix,
        [Parameter(Mandatory = $true)][string]$AssetFormat
    )
    $jq = '.[] | select((.draft or .prerelease) | not) | .tag_name as $t' +
        " | select(`$t | test(`"$TagPattern`"))" +
        " | (`$t | ltrimstr(`"$TagPrefix`")) as `$v" +
        " | select(any(.assets[]; .name == (`"$AssetFormat`" | sub(`"VERSION`"; `$v)) and .state == `"uploaded`"))" +
        ' | $v'
    $versions = @(gh api --paginate "repos/$Repo/releases?per_page=100" --jq $jq)
    if ($LASTEXITCODE -ne 0)
    {
        throw "[ERROR] listing $Repo releases failed"
    }
    $newest = $versions | Where-Object { $_ } | Sort-Object -Property { [version]$_ } | Select-Object -Last 1
    if (-Not $newest)
    {
        throw "[ERROR] no $Repo release matching '$TagPattern' has '$AssetFormat' attached"
    }
    return $newest
}

function Get-LatestRabbitMQVersion
{
    Get-NewestReleaseWithAsset -Repo 'rabbitmq/rabbitmq-server' `
        -TagPattern '^v[0-9]+\\.[0-9]+\\.[0-9]+$' -TagPrefix 'v' `
        -AssetFormat 'rabbitmq-server-VERSION.exe'
}

function Get-LatestErlang27Version
{
    Get-NewestReleaseWithAsset -Repo 'erlang/otp' `
        -TagPattern '^OTP-27\\.[0-9.]+$' -TagPrefix 'OTP-' `
        -AssetFormat 'otp_win64_VERSION.exe'
}
