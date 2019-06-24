# Warning: functions in this file can be used only one function per batch
# Minimum 6 master nodes are required to perform this test
# MSPreparePrimaryWallets must be used on block #12
# After using MSPreparePrimaryWallets, network has to be restarted from genesis

$DELAY = [PSCustomObject]@{
    DELAY = 2
    DELAY_Sleep = 2
}

# global multisig vars
$global:multiSigData = $null
$global:multiSigSelectedNode = 'A'

# global task related multisig cache vars
[decimal] $global:multiSigTaskInitialBalance = 0
$global:multiSigTaskInitialAllowedSigners = $null
[int] $global:multiSigTaskInitialRequiredSigs = 1
$global:multiSigLastOrigTXID = $null
$global:multiSigLastTxType = $null
$global:multiSigLastTxData = $null
[decimal] $global:multiSigTotalSignerFee = 0

function Init-MSPreparePrimaryWallets { # Prepare existing primary wallets on which we'll test
    $global:multiSigTotalSignerFee = 0
    $node_a = Choose-RandomNode -APIPort $APIStartPort -Offset 2
    $node_b = Choose-RandomNode -APIPort $APIStartPort -Offset 2
    $node_c = Choose-RandomNode -APIPort $APIStartPort -Offset 2
    while($node_b -eq $node_a) {
        $node_b = Choose-RandomNode -APIPort $APIStartPort -Offset 2
    }
    while(($node_c -eq $node_b) -or ($node_c -eq $node_a)) {
        $node_c = Choose-RandomNode -APIPort $APIStartPort -Offset 2
    }

    # Generate a new wallet for both a and b
    $node_wallet_a = Invoke-DLTApi -APIPort $node_a -Command "mywallet"
    $node_wallet_b = Invoke-DLTApi -APIPort $node_b -Command "mywallet"
    $node_wallet_c = Invoke-DLTApi -APIPort $node_c -Command "mywallet"
    $new_wallet_c = Invoke-DLTApi -APIPort $node_c -Command "generatenewaddress"

    $primary_wallet_a = $($node_wallet_a.psobject.properties | Select -First 1).Name
    $primary_wallet_b = $($node_wallet_b.psobject.properties | Select -First 1).Name
    $primary_wallet_c = $($node_wallet_c.psobject.properties | Select -First 1).Name

    # Move funds to a different address to avoid becoming a master node
    $cmdargs = @{
        "to" = "$($new_wallet_c)_81000"
    }
    $initial_funds_tx = Invoke-DLTApi -APIPort $node_a -Command "addtransaction" -CmdArgs $cmdargs
    if($initial_funds_tx -eq $null) {
        Write-Host -ForegroundColor Red "Error creating initial funds transaction."
        return $null
    }

    $cmdargs = @{
        "to" = "$($new_wallet_c)_81000"
    }
    $initial_funds_tx2 = Invoke-DLTApi -APIPort $node_b -Command "addtransaction" -CmdArgs $cmdargs
    if($initial_funds_tx2 -eq $null) {
        Write-Host -ForegroundColor Red "Error creating initial funds transaction."
        return $null
    }

    $cmdargs = @{
        "to" = "$($new_wallet_c)_81000"
    }
    $initial_funds_tx3 = Invoke-DLTApi -APIPort $node_c -Command "addtransaction" -CmdArgs $cmdargs
    if($initial_funds_tx3 -eq $null) {
        Write-Host -ForegroundColor Red "Error creating initial funds transaction."
        return $null
    }

    $global:multiSigData = [PSCustomObject]@{
        Node_A = $node_a
        Wallet_A = $primary_wallet_a
        Node_B = $node_b
        Wallet_B = $primary_wallet_b
        Node_C = $node_c
        Wallet_C = $primary_wallet_c
        NewWallet_C = $new_wallet_c
        Funds_TXID1 = ($initial_funds_tx.'id')
        Funds_TXID2 = ($initial_funds_tx2.'id')
        Funds_TXID3 = ($initial_funds_tx3.'id')
        WaitBlockChange = $true
    }
    return 1
}

function Step0-MSPreparePrimaryWallets {
    $Data = $global:multiSigData

    # make sure the TX was executed
    if((Check-TXExecuted -APIPort $APIStartPort -TXID $Data.Funds_TXID1) -eq $false) {
        Write-Host -ForegroundColor Gray "Initial funds were not transferred to the new wallet yet."
        return $DELAY
    }
    if((Check-TXExecuted -APIPort $APIStartPort -TXID $Data.Funds_TXID2) -eq $false) {
        Write-Host -ForegroundColor Gray "Initial funds were not transferred to the new wallet yet."
        return $DELAY
    }
    if((Check-TXExecuted -APIPort $APIStartPort -TXID $Data.Funds_TXID3) -eq $false) {
        Write-Host -ForegroundColor Gray "Initial funds were not transferred to the new wallet yet."
        return $DELAY
    }
    return $Data
}

function Step1-MSPreparePrimaryWallets {
    $Data = $global:multiSigData
    return $Data
}

function Check-MSPreparePrimaryWallets {
    Param(
        [PSCustomObject]$Data
    )

    $cmdargs = @{
        "id" = $Data.Wallet_A
    }
    $wallet = Invoke-DLTApi -APIPort $APIStartPort -Command "getwallet" -CmdArgs $cmdargs
    if($wallet -eq $null) {
        Write-Host -ForegroundColor Red "Error while attempting to get wallet information from node $($APIStartPort)"
        return $null
    }

    return 1
}

function Init-MSGenerateSecondaryWallets { # Generate new wallets on which we'll test
    $global:multiSigTotalSignerFee = 0
    $node_a = Choose-RandomNode -APIPort $APIStartPort
    $node_b = Choose-RandomNode -APIPort $APIStartPort
    $node_c = Choose-RandomNode -APIPort $APIStartPort
    while($node_b -eq $node_a) {
        $node_b = Choose-RandomNode -APIPort $APIStartPort
    }
    while(($node_c -eq $node_b) -or ($node_c -eq $node_a)) {
        $node_c = Choose-RandomNode -APIPort $APIStartPort
    }

    # Generate a new wallet for both a and b
    $new_wallet_a = Invoke-DLTApi -APIPort $node_a -Command "generatenewaddress"
    $new_wallet_b = Invoke-DLTApi -APIPort $node_b -Command "generatenewaddress"
    $new_wallet_c = Invoke-DLTApi -APIPort $node_c -Command "generatenewaddress"

    # Give wallet A some balance from one of $node_a's existing addresses
    $cmdargs = @{
        "to" = "$($new_wallet_a)_10000"
    }
    $initial_funds_tx = Invoke-DLTApi -APIPort $node_a -Command "addtransaction" -CmdArgs $cmdargs
    if($initial_funds_tx -eq $null) {
        Write-Host -ForegroundColor Red "Error creating initial funds transaction."
        return $null
    }

    $global:multiSigData = [PSCustomObject]@{
        Node_A = $node_a
        Wallet_A = $new_wallet_a
        Node_B = $node_b
        Wallet_B = $new_wallet_b
        Node_C = $node_c
        Wallet_C = $new_wallet_c
        Funds_TXID = ($initial_funds_tx.'id')
        WaitBlockChange = $true
    }
    return 1
}

function Step0-MSGenerateSecondaryWallets {
    $Data = $global:multiSigData

    # make sure the TX was executed
    if((Check-TXExecuted -APIPort $APIStartPort -TXID $Data.Funds_TXID) -eq $false) {
        Write-Host -ForegroundColor Gray "Initial funds were not transferred to the new wallet yet."
        return $DELAY
    }
    return $Data
}

function Check-MSGenerateSecondaryWallets {
    Param(
        [PSCustomObject]$Data
    )

    $cmdargs = @{
        "id" = $Data.Wallet_A
    }
    $wallet = Invoke-DLTApi -APIPort $APIStartPort -Command "getwallet" -CmdArgs $cmdargs
    if($wallet -eq $null) {
        Write-Host -ForegroundColor Red "Error while attempting to get wallet information from node $($APIStartPort)"
        return $null
    }

    if($wallet.'balance' -ne '10000.00000000') {
        Write-Host -ForegroundColor Red "Wallet $($Data.Wallet_A) should have 10000 Ixis, but has '$($wallet.'balance')'."
        return $null
    }

    return 1
}

function Init-MSSetOwner {
    $global:multiSigSelectedNode = 'A'
    return 1
}

function Check-MSSetOwner {
    return 1
}

function Init-MSSetSigner1 {
    $global:multiSigSelectedNode = 'B'
    return 1
}

function Check-MSSetSigner1 {
    return 1
}

function Init-MSSetSigner2 {
    $global:multiSigSelectedNode = 'C'
    return 1
}

function Check-MSSetSigner2 {
    return 1
}


function Init-MSAddKey {  # Execute the command to add Wallet_B as a signer to Wallet_A
    Param(
        [PSCustomObject]$InputParams
    )

    $Data = $global:multiSigData

    $cmdargs = @{
        "id" = $Data.Wallet_A
    }
    $wallet = Invoke-DLTApi -APIPort $APIStartPort -Command "getwallet" -CmdArgs $cmdargs
    if($wallet -eq $null) {
        Write-Host -ForegroundColor Red "Error while attempting to get wallet information from node $($APIStartPort)"
        return $null
    }

    $global:multiSigTaskInitialBalance = $wallet.'balance'
    $global:multiSigTaskInitialAllowedSigners = $wallet.'allowedSigners'
    $global:multiSigTaskInitialRequiredSigs = $wallet.'requiredSigs'

    # Add Wallet B as a signer for Wallet A
    $cmdArgs = @{
        "wallet" = $Data.Wallet_A
        "signer" = $Data.Wallet_B
    }
    $node = $Data.Node_A
    if($global:multiSigSelectedNode -eq 'B')
    {
        $cmdArgs.'signer' = $Data.Wallet_C
        $node = $Data.Node_B
    }
    if($global:multiSigSelectedNode -eq 'C')
    {
        $cmdArgs.'signer' = $Data.Wallet_B
        $node = $Data.Node_C
    }
    $result = Invoke-DLTApi -APIPort $node -Command "addmultisigkey" -CmdArgs $cmdargs
    if($result -eq $null) {
        Write-Host -ForegroundColor Red " Error adding a multisig key $($cmdArgs.'signer') to wallet $($Data.Wallet_A)."
        if($InputParams.Fail -eq "InvalidSigner")
        {
            return $InputParams
        }
        return $null
    }elseif($InputParams.Fail -eq "InvalidSigner")
    {
        Write-Host -ForegroundColor Red " Expecting invalid signer for wallet $($Data.Wallet_A)."
        return $null
    }

    $global:multiSigTotalSignerFee = $result.'totalAmount'
    $global:multiSigLastOrigTXID = $result.'id'
    $global:multiSigLastTxType = "AddKey"
    $global:multiSigLastTxData = [PSCustomObject]@{
        TXID = $result.'id'
        Wallet = $Data.Wallet_A
        SignerWallet = $cmdArgs.'signer'
        WaitBlockChange = $true
        SigTXID = $null
        Fail = $InputParams.Fail
        }
    # if there was success, result will have a transaction object
    return $global:multiSigLastTxData
    
}

function Step0-MSAddKey { # Check that the transaction was completed
    Param(
        [PSCustomObject]$Data
    )

    if($Data.Fail -eq "InvalidSigner")
    {
        return $Data
    }

    if((Check-TXExecuted -APIPort $APIStartPort -TXID $Data.TXID) -eq $false) {
        Write-Host -ForegroundColor Gray "AddMultisigKey Transaction (ID: $($Data.TXID)) was not executed yet!"
        if($Data.Fail -eq "WaitForSig")
        {
            return $Data
        }
        return $DELAY
    }

    # transaction was performed
    return $Data
}

function Check-MSAddKey { # Check that the wallet is now a multisig wallet with both keys
    Param(
        [PSCustomObject]$Data
    )

    if($Data.Fail -eq "InvalidSigner")
    {
        return $Data
    }

    if($Data.Fail -eq "WaitForSig")
    {
        if((Check-TXExecuted -APIPort $APIStartPort -TXID $Data.TXID) -eq $false)
        {
            Write-Host -ForegroundColor Gray "AddMultisigKey Transaction (ID: $($Data.TXID)) was not executed yet!"
            return $Data
        }else
        {
            Write-Host -ForegroundColor Red " Expecting wait for sig but transaction got processed anyway $($Data.Wallet)."
            return $null
        }
    }

    $cmdargs = @{
        "id" = $Data.Wallet
    }
    $wallet = Invoke-DLTApi -APIPort $APIStartPort -Command "getwallet" -CmdArgs $cmdargs
    if($wallet -eq $null) {
        Write-Host -ForegroundColor Red "Error while attempting to get wallet information from node $($APIStartPort)"
        return $null
    }
    if($wallet.'type' -ne "Multisig") {
        Write-Host -ForegroundColor Red "Wallet $($Data.Wallet) should be a multisig wallet, but is '$($wallet.'type')'."
        return $null
    }
    
    if($global:multiSigTaskInitialAllowedSigners.Split(',').Count -ne ($wallet.'allowedSigners'.Split(',').Count - 1))
    {
        Write-Host -ForegroundColor Red "Wallet's $($Data.Wallet) allowed signers has changed incorrectly."
        return $null
    }

    if($wallet.'requiredSigs' -ne $global:multiSigTaskInitialRequiredSigs) {
        Write-Host -ForegroundColor Red "Wallet's $($Data.Wallet) required sigs has changed but shouldn't have."
        return $null
    }
    
    $allowedsigners = $wallet.'allowedSigners'
    if(($allowedsigners.Contains($Data.SignerWallet)) -eq $false) {
        Write-Host -ForegroundColor Red "Expecting wallet '$($Data.SignerWallet))' to be on the allowed signers list, but found instead: $($allowedsigners)"
        return $null
    }

    if($Data.SigTXID -ne $null)
    {
        $txData = Get-Transaction -APIPort $APIStartPort -TXID $Data.SigTXID
    }else
    {
        $txData = Get-Transaction -APIPort $APIStartPort -TXID $Data.TXID
    }
    if(([decimal]$txData.'totalAmount') -eq 0) {
        Write-Host -ForegroundColor Red "Transaction total amount should be bigger than 0, but is '$($txData.'totalAmount')'."
        return $null
    }
    if(([decimal]$txData.'fee') -eq 0) {
        Write-Host -ForegroundColor Red "Transaction fee should be bigger than 0, but is '$($txData.'fee')'."
        return $null
    }

    [decimal] $newExpBalance = [math]::round(($global:multiSigTaskInitialBalance - $global:multiSigTotalSignerFee), 8)
    if(([decimal]$wallet.'balance') -ne $newExpBalance) {
        Write-Host -ForegroundColor Red "Wallet $($global:multiSigData.Wallet_A) should have '$($newExpBalance)' Ixis, but has '$($wallet.'balance')'."
        return $null
    }

    # all checks pass - it doesn't matter what we return, as long as it is not $null
    return 1
}

function Init-MSDelKey {  # Execute the command to del Wallet_B as a signer from Wallet_A
    Param(
        [PSCustomObject]$InputParams
    )

    $Data = $global:multiSigData

    $cmdargs = @{
        "id" = $Data.Wallet_A
    }
    $wallet = Invoke-DLTApi -APIPort $APIStartPort -Command "getwallet" -CmdArgs $cmdargs
    if($wallet -eq $null) {
        Write-Host -ForegroundColor Red "Error while attempting to get wallet information from node $($APIStartPort)"
        return $null
    }

    $global:multiSigTaskInitialBalance = $wallet.'balance'
    $global:multiSigTaskInitialAllowedSigners = $wallet.'allowedSigners'
    $global:multiSigTaskInitialRequiredSigs = $wallet.'requiredSigs'

    # Add Wallet B as a signer for Wallet A
    $cmdArgs = @{
        "wallet" = $Data.Wallet_A
        "signer" = $Data.Wallet_B
    }
    $node = $Data.Node_A
    if($global:multiSigSelectedNode -eq 'B')
    {
        $cmdArgs.'signer' = $Data.Wallet_C
        $node = $Data.Node_B
    }
    if($global:multiSigSelectedNode -eq 'C')
    {
        $cmdArgs.'signer' = $Data.Wallet_B
        $node = $Data.Node_C
    }
    $result = Invoke-DLTApi -APIPort $node -Command "delmultisigkey" -CmdArgs $cmdargs
    if($result -eq $null) {
        Write-Host -ForegroundColor Red " Error deleting a multisig key $($cmdArgs.'signer') from wallet $($Data.Wallet_A)."
        if($InputParams.Fail -eq "InvalidSigner")
        {
            return $InputParams
        }
        return $null
    }elseif($InputParams.Fail -eq "InvalidSigner")
    {
        Write-Host -ForegroundColor Red " Expecting invalid signer for wallet $($Data.Wallet_A)."
        return $null
    }

    $global:multiSigTotalSignerFee = $result.'totalAmount'
    $global:multiSigLastOrigTXID = $result.'id'
    $global:multiSigLastTxType = "DelKey"

    # if there was success, result will have a transaction object
    $global:multiSigLastTxData = [PSCustomObject]@{
        TXID = $result.'id'
        Wallet = $Data.Wallet_A
        SignerWallet = $cmdArgs.'signer'
        WaitBlockChange = $true
        SigTXID = $null
        Fail = $InputParams.Fail
    }
    return $global:multiSigLastTxData
}

function Step0-MSDelKey { # Check that the transaction was completed
    Param(
        [PSCustomObject]$Data
    )

    if($Data.Fail -eq "InvalidSigner")
    {
        return $Data
    }

    if((Check-TXExecuted -APIPort $APIStartPort -TXID $Data.TXID) -eq $false) {
        Write-Host -ForegroundColor Gray "DelMultisigKey Transaction (ID: $($Data.TXID)) was not executed yet!"
        if($Data.Fail -eq "WaitForSig")
        {
            return $Data
        }
        return $DELAY
    }
    # transaction was performed
    return $Data
}

function Check-MSDelKey { # Check that the wallet is now a multisig wallet with both keys
    Param(
        [PSCustomObject]$Data
    )

    if($Data.Fail -eq "InvalidSigner")
    {
        return $Data
    }

    if($Data.Fail -eq "WaitForSig")
    {
        if((Check-TXExecuted -APIPort $APIStartPort -TXID $Data.TXID) -eq $false)
        {
            Write-Host -ForegroundColor Gray "DelMultisigKey Transaction (ID: $($Data.TXID)) was not executed yet!"
            return $Data
        }else
        {
            Write-Host -ForegroundColor Red " Expecting wait for sig but transaction got processed anyway $($Data.Wallet)."
            return $null
        }
    }

    $cmdargs = @{
        "id" = $Data.Wallet
    }
    $wallet = Invoke-DLTApi -APIPort $APIStartPort -Command "getwallet" -CmdArgs $cmdargs
    if($wallet -eq $null) {
        Write-Host -ForegroundColor Red "Error while attempting to get wallet information from node $($APIStartPort)"
        return $null
    }

    if($global:multiSigTaskInitialAllowedSigners.Split(',').Count -ne ($wallet.'allowedSigners'.Split(',').Count + 1))
    {
        Write-Host -ForegroundColor Red "Wallet's $($Data.Wallet) allowed signers has changed incorrectly."
        return $null
    }

    if($global:multiSigTaskInitialAllowedSigners.Split(',').Count -eq $global:multiSigTaskInitialRequiredSigs)
    {
        if($wallet.'requiredSigs' -ne ($global:multiSigTaskInitialRequiredSigs - 1)) {
            Write-Host -ForegroundColor Red "Wallet's $($Data.Wallet) required sigs didn't change correctly."
            return $null
        }
    }elseif($wallet.'requiredSigs' -ne $global:multiSigTaskInitialRequiredSigs) {
        Write-Host -ForegroundColor Red "Wallet's $($Data.Wallet) required sigs has changed but shouldn't have."
        return $null
    }

    $allowedsigners = $wallet.'allowedSigners'
    if(($allowedSigners.Split(',').Count -gt 1) -and ($wallet.'type' -ne "Multisig")) {
        Write-Host -ForegroundColor Red "Wallet $($Data.Wallet) should be a multisig wallet, but is '$($wallet.'type')'."
        return $null
    }

    if(($allowedSigners.Split(',').Count -lt 2) -and ($wallet.'type' -ne "Normal")) {
        Write-Host -ForegroundColor Red "Wallet $($Data.Wallet) should be a normal wallet, but is '$($wallet.'type')'."
        return $null
    }
    
    if(($allowedsigners.Contains($Data.SignerWallet)) -eq $true) {
        Write-Host -ForegroundColor Red "Expecting wallet '$($Data.SignerWallet))' to not be on the allowed signers list, but found instead: $($allowedsigners)"
        return $null
    }


    if($Data.SigTXID -ne $null)
    {
        $txData = Get-Transaction -APIPort $APIStartPort -TXID $Data.SigTXID
    }else
    {
        $txData = Get-Transaction -APIPort $APIStartPort -TXID $Data.TXID
    }

    if(([decimal]$txData.'totalAmount') -eq 0) {
        Write-Host -ForegroundColor Red "Transaction total amount should be bigger than 0, but is '$($txData.'totalAmount')'."
        return $null
    }
    if(([decimal]$txData.'fee') -eq 0) {
        Write-Host -ForegroundColor Red "Transaction fee should be bigger than 0, but is '$($txData.'fee')'."
        return $null
    }

    [decimal] $newExpBalance = [math]::round(($global:multiSigTaskInitialBalance - $global:multiSigTotalSignerFee), 8)

    if(([decimal]$wallet.'balance') -ne $newExpBalance) {
        Write-Host -ForegroundColor Red "Wallet $($global:multiSigData.Wallet_A) should have '$($newExpBalance)' Ixis, but has '$($wallet.'balance')'."
        return $null
    }

    # all checks pass - it doesn't matter what we return, as long as it is not $null
    return 1
}

function Init-MSChangeReqSigs {  # Execute the command to change required sigs
    Param(
        [PSCustomObject]$InputParams
    )

    $Data = $global:multiSigData

    $cmdargs = @{
        "id" = $Data.Wallet_A
    }
    $wallet = Invoke-DLTApi -APIPort $APIStartPort -Command "getwallet" -CmdArgs $cmdargs
    if($wallet -eq $null) {
        Write-Host -ForegroundColor Red "Error while attempting to get wallet information from node $($APIStartPort)"
        return $null
    }

    $global:multiSigTaskInitialBalance = $wallet.'balance'
    $global:multiSigTaskInitialAllowedSigners = $wallet.'allowedSigners'
    $global:multiSigTaskInitialRequiredSigs = $wallet.'requiredSigs'

    # Add Wallet B as a signer for Wallet A
    $cmdArgs = @{
        "wallet" = $Data.Wallet_A
        "sigs" = 2
    }
    $node = $Data.Node_A
    if($global:multiSigSelectedNode -eq 'B')
    {
        $cmdArgs.'signer' = $Data.Wallet_C
        $cmdArgs.'sigs' = 1
        $node = $Data.Node_B
    }
    if($global:multiSigSelectedNode -eq 'C')
    {
        $cmdArgs.'signer' = $Data.Wallet_B
        $cmdArgs.'sigs' = 3
        $node = $Data.Node_C
    }
    $result = Invoke-DLTApi -APIPort $node -Command "changemultisigs" -CmdArgs $cmdargs
    if($result -eq $null) {
        Write-Host -ForegroundColor Red " Error changing required signature on wallet $($Data.Wallet_A)."
        if($InputParams.Fail -eq "InvalidSigner")
        {
            return $InputParams
        }
        return $null
    }elseif($InputParams.Fail -eq "InvalidSigner")
    {
        Write-Host -ForegroundColor Red " Expecting invalid signer for wallet $($Data.Wallet_A)."
        return $null
    }

    $global:multiSigTotalSignerFee = $result.'totalAmount'
    $global:multiSigLastOrigTXID = $result.'id'
    $global:multiSigLastTxType = "ChangeReqSigs"

    # if there was success, result will have a transaction object
    $global:multiSigLastTxData = [PSCustomObject]@{
        TXID = $result.'id'
        Wallet = $Data.Wallet_A
        Sigs = $cmdArgs.'sigs'
        WaitBlockChange = $true
        SigTXID = $null
        Fail = $InputParams.Fail
    }
    return $global:multiSigLastTxData
}

function Step0-MSChangeReqSigs { # Check that the transaction was completed
    Param(
        [PSCustomObject]$Data
    )

    if($Data.Fail -eq "InvalidSigner")
    {
        return $Data
    }

    if((Check-TXExecuted -APIPort $APIStartPort -TXID $Data.TXID) -eq $false) {
        Write-Host -ForegroundColor Gray "ChangeReqSigs Transaction (ID: $($Data.TXID)) was not executed yet!"
        if($Data.Fail -eq "WaitForSig")
        {
            return $Data
        }
        return $DELAY
    }
    # transaction was performed
    return $Data
}

function Check-MSChangeReqSigs { # Check that the wallet is now a multisig wallet with both keys
    Param(
        [PSCustomObject]$Data
    )

    if($Data.Fail -eq "InvalidSigner")
    {
        return $Data
    }

    if($Data.Fail -eq "WaitForSig")
    {
        if((Check-TXExecuted -APIPort $APIStartPort -TXID $Data.TXID) -eq $false)
        {
            Write-Host -ForegroundColor Gray "ChangeReqSigs Transaction (ID: $($Data.TXID)) was not executed yet!"
            return $Data
        }else
        {
            Write-Host -ForegroundColor Red " Expecting wait for sig but transaction got processed anyway $($Data.Wallet)."
            return $null
        }
    }

    $cmdargs = @{
        "id" = $Data.Wallet
    }
    $wallet = Invoke-DLTApi -APIPort $APIStartPort -Command "getwallet" -CmdArgs $cmdargs
    if($wallet -eq $null) {
        Write-Host -ForegroundColor Red "Error while attempting to get wallet information from node $($APIStartPort)"
        return $null
    }

    if($global:multiSigTaskInitialAllowedSigners -ne $wallet.'allowedSigners')
    {
        Write-Host -ForegroundColor Red "Wallet's $($Data.Wallet) allowed signers has changed but shouldn't have."
        return $null
    }

    if($wallet.'requiredSigs' -ne $Data.Sigs) {
        Write-Host -ForegroundColor Red "Wallet's $($Data.Wallet) required sigs has changed incorrectly."
        return $null
    }

    if($wallet.'type' -ne "Multisig") {
        Write-Host -ForegroundColor Red "Wallet $($Data.Wallet) should be a multisig wallet, but is '$($wallet.'type')'."
        return $null
    }


    if($Data.SigTXID -ne $null)
    {
        $txData = Get-Transaction -APIPort $APIStartPort -TXID $Data.SigTXID
    }else
    {
        $txData = Get-Transaction -APIPort $APIStartPort -TXID $Data.TXID
    }

    if(([decimal]$txData.'totalAmount') -eq 0) {
        Write-Host -ForegroundColor Red "Transaction total amount should be bigger than 0, but is '$($txData.'totalAmount')'."
        return $null
    }
    if(([decimal]$txData.'fee') -eq 0) {
        Write-Host -ForegroundColor Red "Transaction fee should be bigger than 0, but is '$($txData.'fee')'."
        return $null
    }

    [decimal] $newExpBalance = [math]::round(($global:multiSigTaskInitialBalance - $global:multiSigTotalSignerFee), 8)

    if(([decimal]$wallet.'balance') -ne $newExpBalance) {
        Write-Host -ForegroundColor Red "Wallet $($global:multiSigData.Wallet_A) should have '$($newExpBalance)' Ixis, but has '$($wallet.'balance')'."
        return $null
    }

    # all checks pass - it doesn't matter what we return, as long as it is not $null
    return 1
}

function Init-MSAddSignature {  # Execute the command to add signature to a tx
    Param(
        [PSCustomObject]$InputParams
    )

    $Data = $global:multiSigData

    $cmdargs = @{
        "id" = $Data.Wallet_A
    }
    $wallet = Invoke-DLTApi -APIPort $APIStartPort -Command "getwallet" -CmdArgs $cmdargs
    if($wallet -eq $null) {
        Write-Host -ForegroundColor Red "Error while attempting to get wallet information from node $($APIStartPort)"
        return $null
    }

    $global:multiSigTaskInitialBalance = $wallet.'balance'
    $global:multiSigTaskInitialAllowedSigners = $wallet.'allowedSigners'
    $global:multiSigTaskInitialRequiredSigs = $wallet.'requiredSigs'

    # Add Wallet B as a signer for Wallet A
    $cmdArgs = @{
        "wallet" = $Data.Wallet_A
        "origtx" = $global:multiSigLastOrigTXID
    }

    $node = $Data.Node_A
    if($global:multiSigSelectedNode -eq 'B')
    {
        $node = $Data.Node_B
    }
    if($global:multiSigSelectedNode -eq 'C')
    {
        $node = $Data.Node_C
    }


    $result = Invoke-DLTApi -APIPort $node -Command "addmultisigtxsignature" -CmdArgs $cmdargs
    if($result -eq $null) {
        Write-Host -ForegroundColor Red " Error adding signature on txid $($global:multiSigLastOrigTXID)."
        if($InputParams.Fail -eq "InvalidSigner")
        {
            return $InputParams
        }
        return $null
    }elseif($InputParams.Fail -eq "InvalidSigner")
    {
        Write-Host -ForegroundColor Red " Expecting invalid signer for wallet $($Data.Wallet_A)."
        return $null
    }

    if($InputParams.SkipWalletBalance -ne $true)
    { 
        $global:multiSigTotalSignerFee = $global:multiSigTotalSignerFee + $result.'fee'
    }


    # if there was success, result will have a transaction object
    return [PSCustomObject]@{
        TXID = $result.'id'
        Wallet = $Data.Wallet_A
        WaitBlockChange = $true
        Fail = $InputParams.Fail
    }
}

function Step0-MSAddSignature { # Check that the transaction was completed
    Param(
        [PSCustomObject]$Data
    )

    if($Data.Fail -eq "InvalidSigner")
    {
        return $Data
    }

    if((Check-TXExecuted -APIPort $APIStartPort -TXID $Data.TXID) -eq $false) {
        Write-Host -ForegroundColor Gray "AddSignature Transaction (ID: $($Data.TXID)) was not executed yet!"
        if($Data.Fail -eq "WaitForSig")
        {
            return $Data
        }
        return $DELAY
    }
    # transaction was performed
    return $Data
}

function Check-MSAddSignature { # Check that the wallet is now a multisig wallet with both keys
    Param(
        [PSCustomObject]$Data
    )

    if($Data.Fail -eq "InvalidSigner")
    {
        return $Data
    }

    if($Data.Fail -eq "WaitForSig")
    {
        if((Check-TXExecuted -APIPort $APIStartPort -TXID $Data.TXID) -eq $false)
        {
            Write-Host -ForegroundColor Gray "AddSignature Transaction (ID: $($Data.TXID)) was not executed yet!"
            return $Data
        }else
        {
            Write-Host -ForegroundColor Red "Expecting wait for sig but transaction got processed anyway $($Data.Wallet)."
            return $null
        }
    }

    $lastTxData = $global:multiSigLastTxData
    $lastTxData.SigTXID = $Data.TXID

    $txData = Get-Transaction -APIPort $APIStartPort -TXID $Data.TXID
    if(([decimal]$txData.'totalAmount') -eq 0) {
        Write-Host -ForegroundColor Red "Transaction total amount should be bigger than 0, but is '$($txData.'totalAmount')'."
        return $null
    }
    if(([decimal]$txData.'fee') -eq 0) {
        Write-Host -ForegroundColor Red "Transaction fee should be bigger than 0, but is '$($txData.'fee')'."
        return $null
    }

    $lastTxData.Fail = $Data.Fail
    
    if($global:multiSigLastTxType -eq "AddKey")
    {
        return Check-MSAddKey -Data $lastTxData
    }
    if($global:multiSigLastTxType -eq "DelKey")
    {
        return Check-MSDelKey -Data $lastTxData
    }
    if($global:multiSigLastTxType -eq "ChangeReqSigs")
    {
        return Check-MSChangeReqSigs -Data $lastTxData
    }
    if($global:multiSigLastTxType -eq "SimpleTx")
    {
        return Check-MSSendTxSimple -Data $lastTxData
    }
    return $null
}

function Init-MSSendTxSimple {  # Execute the command to add Wallet_B as a signer to Wallet_A
    Param(
        [PSCustomObject]$InputParams
    )

    $Data = $global:multiSigData

    $cmdargs = @{
        "id" = $Data.Wallet_A
    }
    $wallet = Invoke-DLTApi -APIPort $APIStartPort -Command "getwallet" -CmdArgs $cmdargs
    if($wallet -eq $null) {
        Write-Host -ForegroundColor Red "Error while attempting to get wallet information from node $($APIStartPort)"
        return $null
    }

    $global:multiSigTaskInitialBalance = $wallet.'balance'
    $global:multiSigTaskInitialAllowedSigners = $wallet.'allowedSigners'
    $global:multiSigTaskInitialRequiredSigs = $wallet.'requiredSigs'

    $amount = 100;
    if($InputParams.Amount -ne $null)
    {
        $amount = $InputParams.Amount
    }

    # Add Wallet B as a signer for Wallet A
    $cmdArgs = @{
        "from" = $Data.Wallet_A
        "to" = "$($Data.Wallet_C)_$($amount)"
    }
    $node = $Data.Node_A
    if($global:multiSigSelectedNode -eq 'B')
    {
        $cmdArgs.'to' = "$($Data.Wallet_B)_$($amount)"
        $node = $Data.Node_B
    }
    if($global:multiSigSelectedNode -eq 'C')
    {
        $cmdArgs.'to' = "$($Data.Wallet_B)_$($amount)"
        $node = $Data.Node_C
    }
    $result = Invoke-DLTApi -APIPort $node -Command "addmultisigtransaction" -CmdArgs $cmdargs
    if($result -eq $null) {
        Write-Host -ForegroundColor Red " Error sending simple tx."
        if($InputParams.Fail -eq "InvalidSigner")
        {
            return $InputParams
        }
        return $null
    }elseif($InputParams.Fail -eq "InvalidSigner")
    {
        Write-Host -ForegroundColor Red " Expecting invalid signer for wallet $($Data.Wallet_A)."
        return $null
    }

    if(([decimal]$result.'amount') -lt 100) {
        Write-Host -ForegroundColor Red "Transaction amount should be bigger than 100, but is '$($result.'amount')'."
        return $null
    }

    $global:multiSigTotalSignerFee = $result.'totalAmount'
    $global:multiSigLastOrigTXID = $result.'id'
    $global:multiSigLastTxType = "SimpleTx"

    # if there was success, result will have a transaction object
    $global:multiSigLastTxData = [PSCustomObject]@{
        TXID = $result.'id'
        Wallet = $Data.Wallet_A
        SignerWallet = $cmdArgs.'signer'
        WaitBlockChange = $true
        SigTXID = $null
        Fail = $InputParams.Fail
    }
    return $global:multiSigLastTxData
}

function Step0-MSSendTxSimple { # Check that the transaction was completed
    Param(
        [PSCustomObject]$Data
    )

    if($Data.Fail -eq "InvalidSigner")
    {
        return $Data
    }

    if((Check-TXExecuted -APIPort $APIStartPort -TXID $Data.TXID) -eq $false) {
        Write-Host -ForegroundColor Gray "Simple multisig Transaction (ID: $($Data.TXID)) was not executed yet!"
        if($Data.Fail -eq "WaitForSig")
        {
            return $Data
        }
        return $DELAY
    }
    # transaction was performed
    return $Data
}

function Check-MSSendTxSimple { # Check that the wallet is now a multisig wallet with both keys
    Param(
        [PSCustomObject]$Data
    )

    if($Data.Fail -eq "InvalidSigner")
    {
        return $Data
    }

    if($Data.Fail -eq "WaitForSig")
    {
        if((Check-TXExecuted -APIPort $APIStartPort -TXID $Data.TXID) -eq $false)
        {
            Write-Host -ForegroundColor Gray "Simple multisig Transaction (ID: $($Data.TXID)) was not executed yet!"
            return $Data
        }else
        {
            Write-Host -ForegroundColor Red " Expecting wait for sig but transaction got processed anyway $($Data.Wallet)."
            return $null
        }
    }

    $cmdargs = @{
        "id" = $Data.Wallet
    }
    $wallet = Invoke-DLTApi -APIPort $APIStartPort -Command "getwallet" -CmdArgs $cmdargs
    if($wallet -eq $null) {
        Write-Host -ForegroundColor Red "Error while attempting to get wallet information from node $($APIStartPort)"
        return $null
    }
    if($wallet.'type' -ne "Multisig") {
        Write-Host -ForegroundColor Red "Wallet $($Data.Wallet) should be a multisig wallet, but is '$($wallet.'type')'."
        return $null
    }
    
    if($global:multiSigTaskInitialAllowedSigners -ne $wallet.'allowedSigners')
    {
        Write-Host -ForegroundColor Red "Wallet's $($Data.Wallet) allowed signers has changed incorrectly."
        return $null
    }

    if($wallet.'requiredSigs' -ne $global:multiSigTaskInitialRequiredSigs) {
        Write-Host -ForegroundColor Red "Wallet's $($Data.Wallet) required sigs has changed but shouldn't have."
        return $null
    }
    
    if($Data.SigTXID -ne $null)
    {
        $txData = Get-Transaction -APIPort $APIStartPort -TXID $Data.SigTXID
    }else
    {
        $txData = Get-Transaction -APIPort $APIStartPort -TXID $Data.TXID
    }

    if(([decimal]$txData.'totalAmount') -eq 0) {
        Write-Host -ForegroundColor Red "Transaction total amount should be bigger than 0, but is '$($txData.'totalAmount')'."
        return $null
    }
    if(([decimal]$txData.'fee') -eq 0) {
        Write-Host -ForegroundColor Red "Transaction fee should be bigger than 0, but is '$($txData.'fee')'."
        return $null
    }

    [decimal] $newExpBalance = [math]::round(($global:multiSigTaskInitialBalance - $global:multiSigTotalSignerFee), 8)

    if(([decimal]$wallet.'balance') -ne $newExpBalance) {
        Write-Host -ForegroundColor Red "Wallet $($global:multiSigData.Wallet_A) should have '$($newExpBalance)' Ixis, but has '$($wallet.'balance')'."
        return $null
    }

    # TODO Check to balances as well

    # all checks pass - it doesn't matter what we return, as long as it is not $null
    return 1
}

function Init-MSSendTxMulti {  # Execute the command to add Wallet_B as a signer to Wallet_A
    Param(
        [PSCustomObject]$InputParams
    )

    $Data = $global:multiSigData

    $cmdargs = @{
        "id" = $Data.Wallet_A
    }
    $wallet = Invoke-DLTApi -APIPort $APIStartPort -Command "getwallet" -CmdArgs $cmdargs
    if($wallet -eq $null) {
        Write-Host -ForegroundColor Red "Error while attempting to get wallet information from node $($APIStartPort)"
        return $null
    }

    $global:multiSigTaskInitialBalance = $wallet.'balance'
    $global:multiSigTaskInitialAllowedSigners = $wallet.'allowedSigners'
    $global:multiSigTaskInitialRequiredSigs = $wallet.'requiredSigs'

    # Add Wallet B as a signer for Wallet A
    $cmdArgs = @{
        "from" = $Data.Wallet_A
        "to" = "$($Data.Wallet_B)_100-$($Data.Wallet_C)_50"
    }
    $node = $Data.Node_A
    if($global:multiSigSelectedNode -eq 'B')
    {
        $cmdArgs.'to' = "$($Data.Wallet_B)_100-$($Data.Wallet_A)_50"
        $node = $Data.Node_B
    }
    if($global:multiSigSelectedNode -eq 'C')
    {
        $cmdArgs.'to' = "$($Data.Wallet_C)_50-$($Data.Wallet_A)_100"
        $node = $Data.Node_C
    }
    $result = Invoke-DLTApi -APIPort $node -Command "addmultisigtransaction" -CmdArgs $cmdargs
    if($result -eq $null) {
        Write-Host -ForegroundColor Red " Error sending simple tx."
        if($InputParams.Fail -eq "InvalidSigner")
        {
            return $InputParams
        }
        return $null
    }elseif($InputParams.Fail -eq "InvalidSigner")
    {
        Write-Host -ForegroundColor Red " Expecting invalid signer for wallet $($Data.Wallet_A)."
        return $null
    }

    if(([decimal]$result.'amount') -lt 150) {
        Write-Host -ForegroundColor Red "Transaction amount should be bigger than 150, but is '$($result.'amount')'."
        return $null
    }

    $global:multiSigTotalSignerFee = $result.'totalAmount'
    $global:multiSigLastOrigTXID = $result.'id'
    $global:multiSigLastTxType = "SimpleTx"

    # if there was success, result will have a transaction object
    $global:multiSigLastTxData = [PSCustomObject]@{
        TXID = $result.'id'
        Wallet = $Data.Wallet_A
        SignerWallet = $cmdArgs.'signer'
        WaitBlockChange = $true
        SigTXID = $null
        Fail = $InputParams.Fail
    }
    return $global:multiSigLastTxData
}

function Step0-MSSendTxMulti { # Check that the transaction was completed
    Param(
        [PSCustomObject]$Data
    )
    return Step0-MSSendTxSimple -Data $Data
}

function Check-MSSendTxMulti { # Check that the wallet is now a multisig wallet with both keys
    Param(
        [PSCustomObject]$Data
    )
    return Check-MSSendTxSimple -Data $Data
}