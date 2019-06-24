$DELAY = [PSCustomObject]@{
    DELAY = 2
}

function Init-MSAddKeyByOwner { # Generate new wallets on which we'll test
    $node_a = Choose-RandomNode -APIPort $APIStartPort
    $node_b = Choose-RandomNode -APIPort $APIStartPort
    while($node_b -eq $node_a) {
        $node_b = Choose-RandomNode -APIPort $APIStartPort
    }

    # Generate a new wallet for both a and b
    $new_wallet_a = Invoke-DLTApi -APIPort $node_a -Command "generatenewaddress"
    $new_wallet_b = Invoke-DLTApi -APIPort $node_a -Command "generatenewaddress"

    # Give wallet A some balance from one of $node_a's existing addresses
    $cmdargs = @{
        "to" = "$($new_wallet_a)_10000"
    }
    $initial_funds_tx = Invoke-DLTApi -APIPort $node_a -Command "addtransaction" -CmdArgs $cmdargs
    if($initial_funds_tx -eq $null) {
        Write-Host -ForegroundColor Red "Error creating initial funds transaction."
        return $null
    }

    return [PSCustomObject]@{
        Node_A = $node_a
        Wallet_A = $new_wallet_a
        Node_B = $node_b
        Wallet_B = $new_wallet_b
        Funds_TXID = ($initial_funds_tx.'id')
        WaitBlockChange = $true
    }
}

function Step0-MSAddKeyByOwner { # Execute the command to add Wallet_B as a signer to Wallet_A
    Param(
        [PSCustomObject]$Data
    )
    # make sure the TX was executed
    if((Check-TXExecuted -APIPort $APIStartPort -TXID $Data.Funds_TXID) -eq $false) {
        Write-Host -ForegroundColor Gray "Initial funds were not transferred to the new wallet yet."
        return $DELAY
    }
    # Add Wallet B as a signer for Wallet A
    $cmdArgs = @{
        "wallet" = $Data.Wallet_A
        "signer" = $Data.Wallet_B
    }
    $result = Invoke-DLTApi -APIPort $Data.Node_A -Command "addmultisigkey" -CmdArgs $cmdargs
    if($result -eq $null) {
        Write-Host -ForegroundColor Red " Error adding a multisig key $($Data.Wallet_B) to wallet $($Data.Wallet_A)."
        return $null
    }
    # if there was success, result will have a transaction object
    return [PSCustomObject]@{
        Node_A = $Data.Node_A
        Wallet_A = $Data.Wallet_A
        Node_B = $Data.Node_B
        Wallet_B = $Data.Wallet_B
        TXID = $result.'id'
        WaitBlockChange = $true
    }
}

function Step1-MSAddKeyByOwner { # Check that the transaction was completed
    Param(
        [PSCustomObject]$Data
    )
    if((Check-TXExecuted -APIPort $APIStartPort -TXID $Data.TXID) -eq $false) {
        Write-Host -ForegroundColor Gray "AddMultisigKey Transaction (ID: $($Data.TXID)) was not executed yet!"
        return $DELAY
    }
    # transaction was performed
    return [PSCustomObject]@{
        TargetWallet = $Data.Wallet_A
        SignerWallet = $Data.Wallet_B
    }
}

function Check-MSAddKeyByOwner { # Check that the wallet is now a multisig wallet with both keys
    Param(
        [PSCustomObject]$Data
    )
    $cmdargs = @{
        "id" = $Data.TargetWallet
    }
    $wallet = Invoke-DLTApi -APIPort $APIStartPort -Command "getwallet" -CmdArgs $cmdargs
    if($wallet -eq $null) {
        Write-Host -ForegroundColor Red "Error while attempting to wallet information from node $($APIStartPort)"
        return $null
    }
    if($wallet.'type' -ne "Multisig") {
        Write-Host -ForegroundColor Red "Wallet $($Data.TargetWallet) should be a multisih wallet, but is '$($wallet.'type')'."
        return $null
    }
    
    $allowedsigners = $wallet.'allowedSigners'
    if(($allowedsigners.Contains($Data.SignerWallet)) -eq $false) {
        Write-Host -ForegroundColor Red "Expecting wallet '$($Data.SignerWallet))' to be on the allowed signers list, but found instead: $($allowedsigners)"
        return $null
    }
    # all checks pass - it doesn't matter what we return, as long as it is not #null
    return 1
}