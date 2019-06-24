function Init-Dummy {
    Param(
        [PSCustomObject]$InitParams
    )
    Write-Host "Init params: $($InitParams)"
    return [PSCustomObject]@{
        Parm = $InitParams
    }
}

function Check-Dummy {
    Param(
        [PSCustomObject]$Data
    )
    Write-Host "Check params: $($Data.Parm)"
    return 1
}

function Init-DummySleep {
    return [PSCustomObject]@{
    }
}

function Step0-DummySleep {
    Param(
        [PSCustomObject]$Data
    )
    if($Data.DELAY -eq $null -or $Data.DELAY -gt 2) {
        return [PSCustomObject]@{
            DELAY = 4 # Number of times we will delay max
            DELAY_Sleep = 3 # How many seconds we delay each time
        }
    } else {
        return [PSCustomObject]@{ }
    }
}

function Check-DummySleep {
    Param(
        [PSCustomObject]$Data
    )
    return $true
}

function Init-DummySleepForce {
    return [PSCustomObject]@{
    }
}

function Step0-DummySleepForce {
    Param(
        [PSCustomObject]$Data
    )
    if($Data.DELAY -eq $null) {
        return [PSCustomObject]@{
            DELAY = 4 # Number of times we will delay max
            DELAY_Sleep = -1 # How many seconds we delay each time - negative value means wait for the next block
        }
    } else {
        return [PSCustomObject]@{ }
    }
}

function Check-DummySleepForce {
    Param(
        [PSCustomObject]$Data
    )
    return $true
}