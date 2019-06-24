Param(
    [int]$APIStartPort = 11000
)

###### Libraries ######
. .\Testnet-Functions.ps1
. .\Ixian-MS-Tests.ps1
. .\Dummy-Tests.ps1


# Global stuff
$TestBatches = New-Object System.Collections.ArrayList
$InitError = $false

$FailedTestNames = New-Object System.Collections.ArrayList
$SucceededTestNames = New-Object System.Collections.ArrayList


## Test PSObject

# [PSCustomObject]@{
#     Name = [String]
#     Steps = [Number]
#     InitParams = [PSCustomObject]
# }
# Functions which must exist:
# `Init-Name`  , step initialization
# `StepX-Name` , where X [1..Steps] (inclusive). If Steps = 0, no Step functions need exist
# `Check-Name` , step final check for success/failure

## Return from each test step:
# [PSCustomObject]@{
#     Name = [String]
#     BH = [Number] , Block Height when the step was performed
#     Steps = [Number] , number of steps this test must go through
#     CurrentStep = [Number] , step this test is currently on
#     WaitBlockChange = [Boolean] , if $true, the next step won't be scheduled until the block number changes
#     ... , own data
# }

# All tests in a single batch are run in parallel, batches are completely one by one sequentially
# Batches are numbered from 0 forward

###### Functions ######

function Function-Exists {
    Param(
        [string]$Name
    )
    $old = $ErrorActionPreference
    $ErrorActionPreference = 'stop'
    try {
        Get-Command -Name $Name
        return $true
    } catch {
        return $false
    } finally {
        $ErrorActionPreference = $old
    }
}

function Extend-TestBatches {
    Param(
        [int]$Batch
    )
    while($TestBatches.Count -le $Batch) {
        $batchArray = New-Object System.Collections.ArrayList
        [void]$TestBatches.Add($batchArray)
    }
}

function Add-Test {
    Param(
        [string]$TestName,
        [int]$Batch,
        [int]$NumExtraSteps,
        [PSCustomObject]$InitParams = $null
    )
    Extend-TestBatches -Batch $Batch
    # Check if all required functions exist
    if((Function-Exists -Name "Init-$($TestName)") -eq $false) {
        Write-Host -ForegroundColor Red "Initialization function missing: 'Init-$($TestName)'"
        $InitError = $true
        return
    }
    if((Function-Exists -Name "Check-$($TestName)") -eq $false) {
        Write-Host -ForegroundColor Red "Check function missing: 'Check-$($TestName)'"
        $InitError = $true
        return
    }
    for($step = 0; $step -lt $NumExtraSteps; $step++) {
        if((Function-Exists -Name "Step$($step)-$($TestName)") -eq $false) {
            Write-Host -ForegroundColor Red "Step function missing: 'Step$($step)-$($TestName)'"
            $InitError = $false
            return
        }
    }

    # Create the appropriate test descriptor
    $td = [PSCustomObject]@{
        Name = $TestName
        Steps = $NumExtraSteps
        InitParams = $InitParams
    }
    [void]$TestBatches[$Batch].Add($td)
}


function WaitFor-NextBlock {
    $bh = Get-CurrentBlockHeight -APIPort $APIStartPort
    while($true) {
        $nbh = Get-CurrentBlockHeight -APIPort $APIStartPort
        if($nbh -gt $bh) {
            return
        }
        Start-Sleep -Seconds 2
    }
}

function Fill-ResultMembers {
    Param(
        [PSCustomObject]$Result,
        [string]$Name,
        [long]$BH,
        [int]$Steps,
        [int]$CurrentStep
    )

    if(([bool]($Result.PSObject.Properties.name -match "Name")) -eq $false) {
        $Result | Add-Member -Name 'Name' -Type NoteProperty -Value $Name
    } else {
        $Result.Name = $Name
    }
    if(([bool]($Result.PSObject.Properties.name -match "BH")) -eq $false) {
        $Result | Add-Member -Name 'BH' -Type NoteProperty -Value $BH
    } else {
        $Result.BH = $BH
    }
    if(([bool]($Result.PSObject.Properties.name -match "Steps")) -eq $false) {
        $Result | Add-Member -Name 'Steps' -Type NoteProperty -Value $Steps
    } else {
        $Result.Steps = $Steps
    }
    if(([bool]($Result.PSObject.Properties.name -match "CurrentStep")) -eq $false) {
        $Result | Add-Member -Name 'CurrentStep' -Type NoteProperty -Value $CurrentStep
    } else {
        $Result.CurrentStep = $CurrentStep
    }
    if(([bool]($Result.PSObject.Properties.name -match "WaitBlockChange")) -eq $false) {
        $Result | Add-Member -Name 'WaitBlockChange' -Type NoteProperty -Value $true
    }
    # We do not override WaitBlockChange, if it is already present
    return $Result
}

function Process-TestBatch {
    Param(
        [System.Collections.ArrayList]$Batch
    )
    Write-Host -ForegroundColor Cyan "-> Initializing..."
    $RemainingTests = New-Object System.Collections.ArrayList
    foreach($td in $Batch) {
        $test_return = $null
        if($td.InitParams -eq $null) {
            $test_return = & "Init-$($td.Name)"
        } else {
            $test_return = & "Init-$($td.Name)" $td.InitParams
        }
        if($test_return -eq $null) {
            Write-Host -ForegroundColor Yellow "-> $($td.Name)"
            [void]$FailedTestNames.Add($td.Name)
        }
        $test_return = Fill-ResultMembers -Result $test_return -Name $td.Name -BH (Get-CurrentBlockHeight -APIPort $APIStartPort) -Steps $td.Steps -CurrentStep 0
        [void]$RemainingTests.Add($test_return)
        Write-Host -ForegroundColor Green "-> $($td.Name)"
    }
    Write-Host -ForegroundColor Cyan "-> Performing intermediate steps..."
    $DoneSteps = New-Object System.Collections.ArrayList
    while($true) {
        if($RemainingTests.Count -eq 0) {
            # we are done with it all
            break
        }
        $tests_were_run = $false
        for($i = 0; $i -lt $RemainingTests.Count; $i++) {
            $tr1 = $RemainingTests[$i]
            if($tr1.CurrentStep -lt $tr1.Steps) {
                # Test has more intermediate steps to run
                if($tr1.WaitBlockChange -eq $false -or $tr1.BH -lt (Get-CurrentBlockHeight -APIPort $APIStartPort)) {
                    # it can be run right away
                    $tests_were_run = $true
                    $force_wait_next_block = $false
                    $tr2 = & "Step$($tr1.CurrentStep)-$($tr1.Name)" -Data $tr1
                    $delayed = $false
                    # a "DELAY" result means we repeat this step
                    if(([bool]($tr2.PSObject.Properties.name -match "DELAY")) -eq $true) {
                        $delayed = $true
                        # if $tr1 already has a delay, we decrement it
                        if(([bool]($tr1.PSObject.Properties.name -match "DELAY")) -eq $true) {
                            $tr1.DELAY--
                            if($tr1.DELAY -le 0) {
                                #time is up, so we fail the test
                                $tr2 = $null
                            }
                            # Write reduced delay value back into the test data
                            $RemainingTests[$i].DELAY = $tr1.DELAY
                        } else {
                            # this is the first time this step was delayed
                            $tr1 | Add-Member -Name 'DELAY' -Type NoteProperty -Value $tr2.DELAY
                        }
                        # Check if DELAY also has a sleep defined
                        if(([bool]($tr2.PSObject.Properties.name -match "DELAY_Sleep")) -ne $null) {
                            if($tr2.DELAY_Sleep -lt 0) {
                                # Negative sleep means we wait for the next block
                                # Use the force_wait flag
                                Write-Host -ForegroundColor Gray "Test $($tr1.Name) requires a delay until the next block..."
                                $force_wait_next_block = $true
                            } else {
                                Write-Host -ForegroundColor Gray "Test $($tr1.Name) requires a sleep of $($tr2.DELAY_Sleep) seconds..."
                                Start-Sleep -Seconds $tr2.DELAY_Sleep
                            }
                        }
                    }
                    # null return means a failure
                    if($tr2 -eq $null) {
                        Write-Host -ForegroundColor Yellow " -X $($tr1.Name)"
                        # remove failed test from the queue
                        $RemainingTests.RemoveAt($i)
                        $i--
                        [void]$FailedTestNames.Add($tr1.Name)
                    } elseif($delayed -eq $true) {
                        # basically do nothing, just go another circle - because a test was run, we'll wait for the next block automatically
                        Write-Host -ForegroundColor White " -> $($tr1.Name) DELAYED($($tr1.DELAY))"
                    } else {
                        Write-Host -ForegroundColor Green " -> $($tr1.Name), Step $($tr1.CurrentStep)"
                        $tr2 = Fill-ResultMembers -Result $tr2 -Name $tr1.Name -BH (Get-CurrentBlockHeight -APIPort $APIStartPort) -Steps $tr1.Steps -CurrentStep $tr1.CurrentStep
                        $tr2.CurrentStep++
                        $RemainingTests[$i] = $tr2
                    }
                }
            } else {
                # Test has no more intermediate steps
                $RemainingTests.RemoveAt($i)
                $i--
                [void]$DoneSteps.Add($tr1)
            }
        }
        # if no tests were run, we probably need to wait for the next block
        if($tests_were_run -eq $false -or $force_wait_next_block) {
            if($RemainingTests.Count -gt 0) {
                Write-Host -ForegroundColor Gray " -> Waiting for next block..."
                WaitFor-NextBlock
            }
        }
    }
    # Wait block?

    Write-Host -ForegroundColor Cyan "-> Performing final check..."
    foreach($tr1 in $DoneSteps) {
        $tr2 = & "Check-$($tr1.Name)" -Data $tr1
        if($tr2 -eq $null) {
            Write-Host -ForegroundColor Yellow " -X $($tr1.Name)"
            [void]$FailedTestNames.Add($tr1.Name)
        } else {
            # test is successful...
            Write-Host -ForegroundColor Green " -> $($tr1.Name)"
            [void]$SucceededTestNames.Add($tr1.Name)
        }
    }
}

function Add-MS-Tests()
{
    Param(
        [int]$StartBatch
    )
    Add-Test -TestName "MSSetOwner" -Batch ($startBatch + 1) -NumExtraSteps 0
    Add-Test -TestName "MSAddKey" -Batch ($startBatch + 2) -NumExtraSteps 1
    Add-Test -TestName "MSSendTxSimple" -Batch ($startBatch + 3) -NumExtraSteps 1
    Add-Test -TestName "MSSendTxMulti" -Batch ($startBatch + 4) -NumExtraSteps 1
    Add-Test -TestName "MSSendTxSimple" -Batch ($startBatch + 5) -NumExtraSteps 1 -InitParams ([PSCustomObject]@{ Fail = "InvalidSigner"
                                                                                                                  Amount = "10000000000" }) #overspend test
    Add-Test -TestName "MSDelKey" -Batch ($startBatch + 6) -NumExtraSteps 1
    Add-Test -TestName "MSAddKey" -Batch ($startBatch + 7) -NumExtraSteps 1
    Add-Test -TestName "MSChangeReqSigs" -Batch ($startBatch + 8) -NumExtraSteps 1
    Add-Test -TestName "MSSetSigner1" -Batch ($startBatch + 9) -NumExtraSteps 0
    Add-Test -TestName "MSAddKey" -Batch ($startBatch + 10) -NumExtraSteps 1 -InitParams ([PSCustomObject]@{ Fail = "WaitForSig" })
    Add-Test -TestName "MSAddSignature" -Batch ($startBatch + 11) -NumExtraSteps 1 -InitParams ([PSCustomObject]@{ Fail = "WaitForSig"
                                                                                                                   SkipWalletBalance = $true })
    Add-Test -TestName "MSSetOwner" -Batch ($startBatch + 12) -NumExtraSteps 0
    Add-Test -TestName "MSAddSignature" -Batch ($startBatch + 13) -NumExtraSteps 1
    Add-Test -TestName "MSSendTxSimple" -Batch ($startBatch + 14) -NumExtraSteps 1 -InitParams ([PSCustomObject]@{ Fail = "WaitForSig" })
    Add-Test -TestName "MSSetSigner1" -Batch ($startBatch + 15) -NumExtraSteps 0
    Add-Test -TestName "MSAddSignature" -Batch ($startBatch + 16) -NumExtraSteps 1
    Add-Test -TestName "MSSetOwner" -Batch ($startBatch + 17) -NumExtraSteps 0
    Add-Test -TestName "MSSendTxMulti" -Batch ($startBatch + 18) -NumExtraSteps 1 -InitParams ([PSCustomObject]@{ Fail = "WaitForSig" })
    Add-Test -TestName "MSSetSigner1" -Batch ($startBatch + 19) -NumExtraSteps 0
    Add-Test -TestName "MSAddSignature" -Batch ($startBatch + 20) -NumExtraSteps 1
    Add-Test -TestName "MSSetSigner2" -Batch ($startBatch + 21) -NumExtraSteps 0
    Add-Test -TestName "MSChangeReqSigs" -Batch ($startBatch + 22) -NumExtraSteps 1 -InitParams ([PSCustomObject]@{ Fail = "WaitForSig" })
    Add-Test -TestName "MSAddSignature" -Batch ($startBatch + 23) -NumExtraSteps 1 -InitParams ([PSCustomObject]@{ Fail = "WaitForSig"
                                                                                                                   SkipWalletBalance = $true })
    Add-Test -TestName "MSSetSigner1" -Batch ($startBatch + 24) -NumExtraSteps 0
    Add-Test -TestName "MSAddSignature" -Batch ($startBatch + 25) -NumExtraSteps 1
    Add-Test -TestName "MSSendTxSimple" -Batch ($startBatch + 26) -NumExtraSteps 1 -InitParams ([PSCustomObject]@{ Fail = "WaitForSig" })
    Add-Test -TestName "MSSetSigner2" -Batch ($startBatch + 27) -NumExtraSteps 0
    Add-Test -TestName "MSAddSignature" -Batch ($startBatch + 28) -NumExtraSteps 1 -InitParams ([PSCustomObject]@{ Fail = "WaitForSig" })
    Add-Test -TestName "MSSetOwner" -Batch ($startBatch + 29) -NumExtraSteps 0
    Add-Test -TestName "MSAddSignature" -Batch ($startBatch + 30) -NumExtraSteps 1
    Add-Test -TestName "MSSendTxMulti" -Batch ($startBatch + 31) -NumExtraSteps 1 -InitParams ([PSCustomObject]@{ Fail = "WaitForSig" })
    Add-Test -TestName "MSSetSigner2" -Batch ($startBatch + 32) -NumExtraSteps 0
    Add-Test -TestName "MSAddSignature" -Batch ($startBatch + 33) -NumExtraSteps 1 -InitParams ([PSCustomObject]@{ Fail = "WaitForSig" })
    Add-Test -TestName "MSSetSigner2" -Batch ($startBatch + 34) -NumExtraSteps 0
    Add-Test -TestName "MSAddSignature" -Batch ($startBatch + 35) -NumExtraSteps 1 -InitParams ([PSCustomObject]@{ Fail = "WaitForSig"
                                                                                                                   SkipWalletBalance = $true })
    Add-Test -TestName "MSSetSigner1" -Batch ($startBatch + 36) -NumExtraSteps 0
    Add-Test -TestName "MSAddSignature" -Batch ($startBatch + 37) -NumExtraSteps 1
    Add-Test -TestName "MSDelKey" -Batch ($startBatch + 38) -NumExtraSteps 1 -InitParams ([PSCustomObject]@{ Fail = "WaitForSig" })
    Add-Test -TestName "MSSetOwner" -Batch ($startBatch + 39) -NumExtraSteps 0
    Add-Test -TestName "MSAddSignature" -Batch ($startBatch + 40) -NumExtraSteps 1 -InitParams ([PSCustomObject]@{ Fail = "WaitForSig" })
    Add-Test -TestName "MSSetOwner" -Batch ($startBatch + 41) -NumExtraSteps 0
    Add-Test -TestName "MSAddSignature" -Batch ($startBatch + 42) -NumExtraSteps 1 -InitParams ([PSCustomObject]@{ Fail = "WaitForSig"
                                                                                                                   SkipWalletBalance = $true })
    Add-Test -TestName "MSSetSigner2" -Batch ($startBatch + 43) -NumExtraSteps 0
    Add-Test -TestName "MSAddSignature" -Batch ($startBatch + 44) -NumExtraSteps 1

    # invalid orig tx test
    Add-Test -TestName "MSSetSigner2" -Batch ($startBatch + 45)
    Add-Test -TestName "MSAddSignature" -Batch ($startBatch + 46) -NumExtraSteps 1 -InitParams ([PSCustomObject]@{ Fail = "InvalidSigner" })

    # test from here must totally fail
    Add-Test -TestName "MSSendTxMulti" -Batch ($startBatch + 47) -NumExtraSteps 1 -InitParams ([PSCustomObject]@{ Fail = "InvalidSigner" })
    Add-Test -TestName "MSDelKey" -Batch ($startBatch + 48) -NumExtraSteps 1 -InitParams ([PSCustomObject]@{ Fail = "InvalidSigner" })
    Add-Test -TestName "MSAddKey" -Batch ($startBatch + 49) -NumExtraSteps 1 -InitParams ([PSCustomObject]@{ Fail = "InvalidSigner" })
    Add-Test -TestName "MSChangeReqSigs" -Batch ($startBatch + 50) -NumExtraSteps 1 -InitParams ([PSCustomObject]@{ Fail = "InvalidSigner" })

    # test from here - first two in set must pass, second two must fail
    Add-Test -TestName "MSSetOwner" -Batch ($startBatch + 51) -NumExtraSteps 0
    Add-Test -TestName "MSSendTxMulti" -Batch ($startBatch + 52) -NumExtraSteps 1 -InitParams ([PSCustomObject]@{ Fail = "WaitForSig" })
    Add-Test -TestName "MSSetSigner2" -Batch ($startBatch + 53) -NumExtraSteps 0
    Add-Test -TestName "MSAddSignature" -Batch ($startBatch + 54) -NumExtraSteps 1 -InitParams ([PSCustomObject]@{ Fail = "InvalidSigner" })

    Add-Test -TestName "MSSetOwner" -Batch ($startBatch + 55) -NumExtraSteps 0
    Add-Test -TestName "MSChangeReqSigs" -Batch ($startBatch + 56) -NumExtraSteps 1 -InitParams ([PSCustomObject]@{ Fail = "WaitForSig" })
    Add-Test -TestName "MSSetSigner2" -Batch ($startBatch + 57) -NumExtraSteps 0
    Add-Test -TestName "MSAddSignature" -Batch ($startBatch + 58) -NumExtraSteps 1 -InitParams ([PSCustomObject]@{ Fail = "InvalidSigner" })

    Add-Test -TestName "MSSetOwner" -Batch ($startBatch + 59) -NumExtraSteps 0
    Add-Test -TestName "MSDelKey" -Batch ($startBatch + 60) -NumExtraSteps 1 -InitParams ([PSCustomObject]@{ Fail = "WaitForSig" })
    Add-Test -TestName "MSSetSigner2" -Batch ($startBatch + 61) -NumExtraSteps 0
    Add-Test -TestName "MSAddSignature" -Batch ($startBatch + 62) -NumExtraSteps 1 -InitParams ([PSCustomObject]@{ Fail = "InvalidSigner" })

    Add-Test -TestName "MSSetSigner1" -Batch ($startBatch + 63) -NumExtraSteps 0
    Add-Test -TestName "MSAddKey" -Batch ($startBatch + 64) -NumExtraSteps 1 -InitParams ([PSCustomObject]@{ Fail = "WaitForSig" })
    Add-Test -TestName "MSSetSigner2" -Batch ($startBatch + 65) -NumExtraSteps 0
    Add-Test -TestName "MSAddSignature" -Batch ($startBatch + 66) -NumExtraSteps 1 -InitParams ([PSCustomObject]@{ Fail = "InvalidSigner" })

    return $StartBatch + 66
}


###### Main ######

Write-Host -ForegroundColor White "Preparing tests..."

Add-Test -TestName "MSPreparePrimaryWallets" -Batch 0 -NumExtraSteps 2
$batchId = Add-MS-Tests -StartBatch 0
Add-Test -TestName "MSGenerateSecondaryWallets" -Batch ($batchId + 1) -NumExtraSteps 1
$batchId = Add-MS-Tests -StartBatch ($batchId + 1)


#Add-Test -TestName "Dummy" -Batch 0 -NumExtraSteps 0 -InitParams ([PSCustomObject]@{ SampleValue = 1 })
#Add-Test -TestName "DummySleep" -Batch 0 -NumExtraSteps 1
#Add-Test -TestName "DummySleepForce" -Batch 1 -NumExtraSteps 1


if($InitError) {
    Write-Host -ForegroundColor Magenta "Initialization error occured. Aborting."
    exit(-1)
}

$i = 0
foreach($Batch in $TestBatches) {
    Write-Host -ForegroundColor Cyan "Processing batch $($i)"
    Process-TestBatch -Batch $Batch
    $i++
    Start-Sleep -Seconds 5
}

Write-Host -ForegroundColor White -NoNewline "Results: Succeeded: "
Write-Host -ForegroundColor Green -NoNewline $SucceededTestNames.Count
Write-Host -ForegroundColor White -NoNewline " / Failed: "
Write-Host -ForegroundColor Red -NoNewline $FailedTestNames.Count
Write-Host -ForegroundColor White "."

if($FailedTestNames.Count -gt 0) {
    Write-Host -ForegroundColor White "Failing tests:"
    foreach($tn in $FailedTestNames) {
        Write-Host -ForegroundColor Red "-> $($tn)"
    }
}