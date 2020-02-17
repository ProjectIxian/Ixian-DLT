function Determine-LocalIP {
    $ipAddresses = Get-NetIPAddress -AddressFamily IPv4 -Type Unicast -AddressState Preferred | Sort-Object ifIndex
    foreach($ip in $ipAddresses) {
        try {
            $ip_object = [System.Net.IPAddress]::Parse($ip.IPAddress)
            $octets = $ip_object.GetAddressBytes()
            if(($octets[0] -eq 10) -or 
                ($octets[0] -eq 172 -and $octets[1] -ge 16 -and $octets[1] -le 31) -or
                ($octets[0] -eq 192 -and $octets[1] -eq 168)) {

                # Is private address, we return the first one
                return $ip.IPAddress
            }
        } catch { }
    }
    # No private IP address
    return ""
}

function Invoke-DLTApi {
    Param(
        [int]$APIPort,
        [string]$Command,
        [hashtable]$CmdArgs
    )
    $url = "http://localhost:$($APIPort)/$($Command)"
    $args = ""
    foreach($k in $CmdArgs.Keys) {
        $args = $args + "$($k)=$($CmdArgs[$k])&"
    }
    if($args.Length -gt 0) {
        $url = "$($url)?$($args)"
    }
    # DEBUG
    #Write-Host -ForegroundColor Gray "Invoking DLT Api: $($url)"
    try {
        $r = Invoke-RestMethod -Method Get -Uri $url -TimeoutSec 5
        if($r.error -ne $null) {
            Write-Host -ForegroundColor Red "Invoke-DLTApi: Error returned from API: $($r.error)"
            return $null
        }
        return $r.result
    } catch {
        Write-Host -ForegroundColor Magenta "Invoke-DLTApi: Error while calling rest method $($Command): $($_.Message)"
        return $null
    }
}

function Send-TransactionTN {
    Param(
        [System.Collections.ArrayList]$Clients,
        [int]$FromClient,
        [int]$ToClient = -1,
        [string]$ToAddr = "",
        [int]$Amount = 0
    )
    if($FromClient -lt 0 -or $FromClient -ge $Clients.Count) {
        Write-Host -ForegroundColor Magenta "Send-Transaction: FromClient index $($FromClient) is invalid. Possible clients are: 0 - $($Clients.Count)"
        return $null
    }
    if($ToClient -eq -1 -and $ToAddr -eq "") {
        Write-Host -ForegroundColor Magenta "Send-Transaction: either ToClient or ToAddr must be specified!"
        return $null
    }
    $targetAddr = $ToAddr
    if($ToClient -lt 0 -or $ToClient -ge $Clients.Count) {
        Write-Host -ForegroundColor Magenta "Send-Transaction: ToClient index $($ToClient) is invalid. Possible clients are: 0 - $($Clients.Count)"
        return $null       
    } else {
        $targetAddr = $clients[$ToClient].Address
    }
    $cmdArgs = @{
        "to" = "$($targetAddr)_$($Amount)";
    }
    $reply = Invoke-DLTApi -APIPort $Clients[$FromClient].APIPort -Command "addtransaction" -CmdArgs $cmdArgs
    Write-Host -ForegroundColor Gray "Send-Transaction: Generated transaction txid: $($reply.id)"
    return $reply.id
}

function Send-Transaction {
    Param(
        [int]$FromNode,
        [System.Collections.ArrayList]$FromAddresses,
        [System.Collections.ArrayList]$FromAmounts,
        [System.Collections.ArrayList]$ToAddresses,
        [System.Collections.ArrayList]$ToAmounts
    )
    $fromArg = ""
    if($FromAddresses.Count -gt 0) {
        if($FromAddresses.Count -ne $FromAmounts.Count) {
            Write-Host -ForegroundColor Red "Number of FromAddresses and FromAmounts must be equal!"
            return $null
        }    
        for($i = 0; $i -lt $FromAddresses.Count; $i++) {
            $fromArg += "$($FromAddresses[$i])_$($FromAmounts[$i])-"
        }
        # remove trailing '-'
        $fromArg = $fromArg.Remove($fromArg.Length-1, 1)
    }
    $toArg = ""
    if($ToAddresses.Count -eq 0) {
        Write-Host -ForegroundColor Red "There must be at least one ToAddress."
        return $null
    }
    if($ToAddresses.Count -ne $ToAmounts.Count) {
        Write-Host -ForegroundColor Red "Number of ToAddresses and ToAmounts must be equal!"
        return $null
    }
    for($i = 0; $i -lt $ToAddresses.Count; $i++) {
        $toArg += "$($ToAddresses[$i])_$($ToAmounts[$i])-"
    }
    # remove trailing '-'
    $toArg = $toArg.Remove($toArg.Length-1, 1)
    $cmdArgs = $null
    if($fromArg.Length -gt 0) {
        $cmdArgs = @{
            "from" = $fromArg
            "to" = $toArg
            "autofee" = "true"
        }
    } else {
        $cmdArgs = @{
            "to" = $toArg
        }
    }
    $reply = Invoke-DLTApi -APIPort $FromNode -Command "addtransaction" -CmdArgs $cmdArgs
    if($reply -eq $null) {
        Write-Host -ForegroundColor Red "There was an error creating the transaction."
        return $null
    }
    return $reply.id
}

function Get-DLTNodeStatus {
    Param(
        [System.Collections.ArrayList]$Clients,
        [int]$NodeIdx
    )
    if($NodeIdx -lt 0 -or $NodeIdx -ge $Clients.Count) {
        Write-Host -ForegroundColor Magenta "Get-DLTNodeStatus: Invalid node index: $($NodeIdx). Values must be between 0 and $($Clients.Count)"
        return $null
    }
    try {
        $r = Invoke-DLTApi -APIPort $Clients[$NodeIdx].APIPort -Command "status?verbose=1"
    } catch {
        Write-Host -ForegroundColor Magenta "Get-DLTNodeStatus: Error calling api for client $($NodeIdx): $($_.Message)"
        return $null
    }
    return $r
}

function Get-DLTMinerStatus {
    Param(
        [System.Collections.ArrayList]$Clients,
        [int]$NodeIdx
    )
    if($NodeIdx -lt 0 -or $NodeIdx -ge $Clients.Count) {
        Write-Host -ForegroundColor Magenta "Get-DLTMinerStatus: Invalid node index: $($NodeIdx). Values must be between 0 and $($Clients.Count)"
        return $null
    }
    try {
        $r = Invoke-DLTApi -APIPort $Clients[$NodeIdx].APIPort -Command "minerstats"
    } catch {
        Write-Host -ForegroundColor Magenta "Get-DLTMinerStatus: Error calling api for client $($NodeIdx): $($_.Message)"
        return $null
    }
    return $r
}

function WaitConfirm-PendingTX {
    Param(
        [System.Collections.ArrayList]$Clients,
        [System.Collections.ArrayList]$TXList,
        [int]$Blocks = 5,
        [int]$ConfirmAtNode = 0
    )

    if($Blocks -lt 0) {
        $Blocks = 0
    }
    if($ConfirmAtNode -lt 0 -or $ConfirmAtNode -ge $Clients.Count) {
        Write-Host -ForegroundColor Magenta "WaitConfirm-PendingTX: Client idx $($ConfirmAtNode) is out of bounds."
        return $false
    }
    $ns = Get-DLTNodeStatus -Clients $Clients -NodeIdx $ConfirmAtNode
    if($ns -eq $null) {
        Write-Host -ForegroundColor Magenta "WaitConfirm-PendingTX: Unable to read status from confirmation node $($ConfirmAtNode)."
        return $false
    }
    $currentBlock = $ns.'Block Height'
    $waitUntil = $currentBlock + $Blocks
    while($currentBlock -lt $waitUntil) {
        $transactions = Invoke-DLTApi -APIPort $Clients[$ConfirmAtNode].APIPort -Command "tx"
        if($transactions -eq $null) {
            Write-Host -ForegroundColor Magenta "WaitConfirm-PendingTX: Unable to get a list of applied transactions from confirmation node $($ConfirmAtNode)."
            return $false
        }
        # check transaction output
        # $transactions are PSObject with properties=txids and values transaction objects
        $txids = $transactions.PSObject.Properties | foreach { $_.Name }
        $missing = $false
        foreach($txid in $TXList) {
            if($txids.Contains($txid) -eq $false) {
                $missing = $true
                break
            }
        }
        # if all from pending list are in, we return $true
        if($missing) {
            Write-Host -ForegroundColor Yellow "-> Some transactions have not been accepted yet..."
        } else {
            Write-Host -ForegroundColor Green "-> All transactions have been accepted..."
            return $true
        }
        $ns = Get-DLTNodeStatus -Clients $Clients -NodeIdx $ConfirmAtNode
        if($ns -eq $null) {
            Write-Host -ForegroundColor Magenta "WaitConfirm-PendingTX: Unable to read status from confirmation node $($ConfirmAtNode)."
            return $false
        }
        $currentBlock = $ns.'Block Height'
        Write-Host -ForegroundColor Gray -NoNewline "WaitConfirm-PendingTX: Waiting... Block height: $($currentBlock) / $($waitUntil)"
        Start-Sleep -Seconds 10
    }
    return $false
}

function Get-CurrentBlockHeight {
    Param(
        [int]$APIPort
    )
    $ns = Invoke-DLTApi -APIPort $APIPort -Command "status"
    if($ns -eq $null) {
        return $null
    }
    return $ns.'Block Height'
}

function Get-NumMasterNodes {
    Param(
        [int]$APIPort
    )
    $ns = Invoke-DLTApi -APIPort $APIPort -Command "status"
    if($ns -eq $null) {
        return $null
    }
    return $ns.'Masters'
}

function Choose-RandomNode {
    Param(
        [int]$APIPort,
        [int]$Offset = 0
    )
    $APIPort = $APIPort + $Offset
    $numMasterNodes = Get-NumMasterNodes -APIPort $APIPort
    $tries = 0
    while($true) {
        $r_apiport = Get-Random -Minimum $APIPort -Maximum ($APIPort + $numMasterNodes - $Offset)
        $ns = Invoke-DLTApi -APIPort $r_apiport -Command "status"
        if($ns -ne $null) {
            return $r_apiport
        }
        $tries++
        if($tries -ge 5) {
            Write-Host -ForegroundColor Yellow "Unable to find a working DLT node!"
            return $null
        }
    }
}

function Get-WalletBalance {
    Param(
        [int]$APIPort,
        [string]$address
    )
    $cmdArgs = @{
        "address" = $address
    }
    $balance = Invoke-DLTApi -APIPort $APIPort -Command "getbalance" -CmdArgs $cmdArgs
    if($balance -eq $null) {
        Write-Host -ForegroundColor Red "Error retrieving balance for wallet $($address)."
    }
    return $balance
}

function Get-Transaction {
    Param(
        [int]$APIPort,
        [string]$TXID
    )
    $cmdArgs = @{
        "id" = $TXID
    }
    $transaction = Invoke-DLTApi -APIPort $APIPort -Command "gettransaction" -CmdArgs $cmdArgs
    if($transaction -eq $null) {
        Write-Host -ForegroundColor Red "Error retrieving transaction with id $($TXID)."
    }
    return $transaction
}

function Check-TXExecuted {
    Param(
        [int]$APIPort,
        [string]$TXID
    )
    $transaction = Invoke-DLTApi -APIPort $APIPort -Command "gettransaction?id=$($TXID)"
    if($transaction -eq $null) {
        Write-Host -ForegroundColor Red "Error retrieving transaction with id $($TXID)."
        return $false
    }
    if($transaction.applied -gt 0) {
        return $true
    } else {
        return $false
    }
}