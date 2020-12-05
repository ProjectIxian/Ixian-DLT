function Format-Human {
    Param(
        [int]$num
    )
    $suffix = ''
    if($num -ge 1000*1000*1000*1000) {
        $num = ($num*1.0) / (1000*1000*1000*1000)
        $suffix = ' T'
    } elseif($num -ge 1000*1000*1000) {
        $num = ($num*1.0) / (1000*1000*1000)
        $suffix = ' G'
    } elseif($num -ge 1000*1000) {
        $num = ($num*1.0) / (1000*1000)
        $suffix = ' M'
    } elseif($num -ge 1000) {
        $num = ($num*1.0) / (1000)
        $suffix = ' K'
    }

    return "$($num.ToString('0.00'))$($suffix)"
}


function Render-Header {
    Param(
        [int]$NumNodes
    )
    Write-Host -ForegroundColor White "------------------------------------------------------------------------"
    $now = Get-Date
    Write-Host -ForegroundColor White "Last refresh: $($now.ToString())"
    Write-Host -ForegroundColor Yellow "Testnet has $($NumNodes) nodes."
}

function Render-Indexes {
    Param(
        [System.Collections.ArrayList]$Clients
    )
    Write-Host -ForegroundColor White -NoNewline "PID:".PadRight(10)
    foreach($c in $Clients) {
        if($c.idx -ge 20) { break }
        if($c.Display -eq $false) { continue }
        Write-Host -ForegroundColor Green -NoNewline "$($c.Process.Id.ToString().PadLeft(18))"
    }
    Write-Host ""
}

function Render-Status {
    Param(
        [System.Collections.ArrayList]$dlt_statuses,
        [System.Collections.ArrayList]$bp_statuses
    )
    Write-Host -ForegroundColor White -NoNewline "STATUS:".PadRight(10)
    $num_elem = $dlt_statuses.Count
    if($num_elem -gt $bp_statuses.Count) {
        $num_elem = $bp_statuses.Count
    }
    for($i = 0; $i -lt $num_elem; $i++) {
        $dlt_s = $dlt_statuses[$i]
        $bp_s = $bp_statuses[$i]
        $dlt_c = [ConsoleColor]::Green
        $bp_c = [ConsoleColor]::Green
        if($dlt_s -eq "SYN") {
            $dlt_c = [ConsoleColor]::Yellow
        } elseif($dlt_s -eq "DEAD") {
            $dlt_c = [ConsoleColor]::Magenta
        }
        if($bp_s -eq "STOP") {
            $bp_c = [ConsoleColor]::Red
        } elseif($bp_s -eq "DEAD") {
            $bp_c = [ConsoleColor]::Magenta
        }
        $len = 18 - $dlt_s.Length - $bp_s.Length - 1
        Write-Host -NoNewline "$(New-Object String (' ', $len))"
        Write-Host -NoNewline -ForegroundColor $dlt_c $dlt_s
        Write-Host -NoNewline -ForegroundColor White "/"
        Write-Host -NoNewline -ForegroundColor $bp_c $bp_s
    }
    Write-Host ""
}

function Render-BlockHeight {
    Param(
        [System.Collections.ArrayList]$bhs
    )
    Write-Host -NoNewline -ForegroundColor White "B.HEIGHT:".PadRight(10)
    foreach($bh in $bhs) {
        Write-Host -NoNewline -ForegroundColor Cyan $bh.ToString().PadLeft(18)
    }
    Write-Host ""
}

function Render-BlockHeightSigs {
    Param(
        [System.Collections.ArrayList]$bhs,
        [int]$client_count
    )
    Write-Host -NoNewline -ForegroundColor White "B.SIGS:".PadRight(10)
    foreach($bh in $bhs) {
        if($client_count -eq $bh)
        {
            Write-Host -NoNewline -ForegroundColor Cyan $bh.ToString().PadLeft(18)
        }else
        {
            Write-Host -NoNewline -ForegroundColor Red $bh.ToString().PadLeft(18)
        }
    }
    Write-Host ""
}

function Render-Consensus {
    Param(
        [System.Collections.ArrayList]$consensus
    )
    Write-Host -NoNewline -ForegroundColor White "CONSENSUS:"
    foreach($c in $consensus) {
        Write-Host -NoNewline -ForegroundColor Yellow $c.ToString().PadLeft(18)
    }
    Write-Host ""
}

function Render-Presences {
    Param(
        [System.Collections.ArrayList]$presences
    )
    Write-Host -NoNewline -ForegroundColor White "PRESENCES:"
    foreach($p in $presences) {
        Write-Host -NoNewline -ForegroundColor Yellow $p.ToString().PadLeft(18)
    }
    Write-Host ""
}

function Render-Connections {
    Param(
        [System.Collections.ArrayList]$in_conn,
        [System.Collections.ArrayList]$out_conn
    )
    Write-Host -ForegroundColor White -NoNewline "N(In/Out):"
    $num_elem = $in_conn.Count
    if($num_elem -gt $out_conn.Count) {
        $num_elem = $out_conn.Count
    }
    for($i = 0; $i -lt $num_elem; $i++) {
        $in_s = $in_conn[$i].ToString()
        $out_s = $out_conn[$i].ToString()
        $len = 18 - $in_s.Length - $out_s.Length - 1
        Write-Host -NoNewline "$(New-Object String (' ', $len))"
        Write-Host -NoNewline -ForegroundColor Cyan $in_s
        Write-Host -NoNewline -ForegroundColor White "/"
        Write-Host -NoNewline -ForegroundColor Cyan $out_s
    }
    Write-Host ""
}

function Render-TXs {
    Param(
        [System.Collections.ArrayList]$applied,
        [System.Collections.ArrayList]$unapplied
    )
    Write-Host -ForegroundColor White -NoNewline "TX(A/U):  "
    $num_elem = $applied.Count
    if($num_elem -gt $unapplied.Count) {
        $num_elem = $unapplied.Count
    }
    for($i = 0; $i -lt $num_elem; $i++) {
        $a_s = $applied[$i].ToString()
        $u_s = $unapplied[$i].ToString()
        $len = 18 - $a_s.Length - $u_s.Length - 1
        Write-Host -NoNewline "$(New-Object String (' ', $len))"
        Write-Host -NoNewline -ForegroundColor Cyan $a_s
        Write-Host -NoNewline -ForegroundColor White "/"
        Write-Host -NoNewline -ForegroundColor Cyan $u_s
    }
    Write-Host ""
}

function Render-MinerHashrate {
    Param(
        [System.Collections.ArrayList]$hashrates
    )
    Write-Host -NoNewline -ForegroundColor White "HASH:".PadRight(10)
    foreach($hr in $hashrates) {
        if($hr -eq 0) {
            Write-Host -NoNewline -ForegroundColor Red "N/A".PadLeft(18)
        } else {
            $hr_t = Format-Human $hr
            Write-Host -NoNewline -ForegroundColor Green $hr_t.PadLeft(18)
        }
    }
    Write-Host ""
}

function Render-MinerSolved {
    Param(
        [System.Collections.ArrayList]$solved_counts
    )
    Write-Host -NoNewline -ForegroundColor White "SOLVED:".PadRight(10)
    foreach($s in $solved_counts) {
        Write-Host -NoNewline -ForegroundColor Green $s.ToString().PadLeft(18)
    }
    Write-Host ""
}

function Render-GlobalStats {
    Param(
        [int]$solved,
        [int]$unsolved,
        [int]$wallets,
        [uint64]$difficulty
    )
    Write-Host -ForegroundColor Gray "========================================================================"
    Write-Host -ForegroundColor White "Global stats:"
    # wallets
    Write-Host -ForegroundColor White -NoNewline "Wallets: "
    Write-Host -ForegroundColor Cyan "$($wallets)"
    # Block solution stats
    Write-Host -ForegroundColor White -NoNewline "Solved/Unsolved blocks: "
    Write-Host -ForegroundColor Cyan -NoNewline "$($solved)"
    Write-Host -ForegroundColor White -NoNewline "/"
    Write-Host -ForegroundColor Green -NoNewline "$($unsolved)"
    $ratio = 100.0 * $solved / $unsolved
    Write-Host -ForegroundColor White -NoNewline " ("
    $col = [ConsoleColor]::Cyan
    if($solved -eq $unsolved) {
        $col = [ConsoleColor]::Green
    } elseif($solved -gt $unsolved) {
        $col = [ConsoleColor]::Yellow
    }
    Write-Host -ForegroundColor $col -NoNewline "$($ratio.ToString('0.00'))"
    Write-Host -ForegroundColor White ")"
    # difficulty
    Write-Host -ForegroundColor White -NoNewline "Difficulty: "
    Write-Host -ForegroundColor Cyan -NoNewline "$($difficulty)"
    Write-Host -ForegroundColor White -NoNewline " ("
    $hex_diff = [System.BitConverter]::ToString([System.BitConverter]::GetBytes($difficulty)).Replace("-", "")
    Write-Host -ForegroundColor Green -NoNewline "$($hex_diff)"
    Write-Host -ForegroundColor White ")"

}

function Render-Footer {
    Write-Host -ForegroundColor White  "Keys:"
    Write-Host -ForegroundColor White  " - N : Add another DLT node to the network"
    Write-Host -ForegroundColor White  " - M : Add another Mining node to the network"
    Write-Host -ForegroundColor White  " - C : Cleanup dead nodes from the table"
    Write-Host -ForegroundColor White  " - E : Stop all nodes and exit"
    Write-Host -ForegroundColor Yellow " - X : DETACH FROM NODE PROCESSES AND LEAVE THE NETWORK RUNNING"
}


function Display-ClientStatus {
    Param(
        [System.Collections.ArrayList]$Clients
    )
    $count = 0
    $dlt_statuses = New-Object System.Collections.ArrayList (20)
    $bp_statuses = New-Object System.Collections.ArrayList (20)
    $node_bhs = New-Object System.Collections.ArrayList (20)
    $node_bh_sigs = New-Object System.Collections.ArrayList (20)
    $consensus = New-Object System.Collections.ArrayList (20)
    $presences = New-Object System.Collections.ArrayList (20)
    $in_connections = New-Object System.Collections.ArrayList (20)
    $out_connections = New-Object System.Collections.ArrayList (20)
    $applied_txs = New-Object System.Collections.ArrayList (20)
    $unapplied_txs = New-Object System.Collections.ArrayList (20)
    # Miner:
    $hashrates = New-Object System.Collections.ArrayList (20)
    $solved_counts = New-Object System.Collections.ArrayList (20)

    # Globals
    $global_solved = 0
    $global_unsolved = 0
    $global_wallets = 0
    $global_difficulty = 0
    $mining_globals_captured = $false

    foreach($node in $Clients) {
        if($node.Display -eq $false) { continue }
        $ns = $null
        if($node.Dead -eq $false) {
            $ns = Get-DLTNodeStatus -Clients $Clients -NodeIdx $node.idx
        }
        if($ns -eq $null) {
            # Node probably died
            #$node.Dead = $true
            [void]$dlt_statuses.Add("DEAD")
            [void]$bp_statuses.Add("DEAD")
            [void]$node_bhs.Add(0)
            [void]$node_bh_sigs.Add(0)
            [void]$consensus.Add(0)
            [void]$presences.Add(0)
            [void]$in_connections.Add(0)
            [void]$out_connections.Add(0)
            [void]$applied_txs.Add(0)
            [void]$unapplied_txs.Add(0)
            [void]$hashrates.Add(0)
            [void]$solved_counts.Add(0)
        } else {
            # DLT Status
            [void]$dlt_statuses.Add(($ns.'DLT Status').Substring(0,3).ToUpper())
            # Block processor status
            $bp_status = ($ns.'Block Processor Status')
            if($bp_status -eq "Stopped") { $bp_status = "STOP" }
            if($bp_status -eq "Running") { $bp_status = "OK" }
            [void]$bp_statuses.Add($bp_status)
            # Block Height
            [void]$node_bhs.Add(($ns.'Block Height'))
            # No. of sigs on last block height
            [void]$node_bh_sigs.Add(($ns.'Block Signature Count'))
            # Consensus
            [void]$consensus.Add(($ns.'Required Consensus'))
            # Presences
            [void]$presences.Add(($ns.'Presences'))
            # In / Out connections
            [void]$in_connections.Add(($ns.'Network Clients').Count)
            [void]$out_connections.Add(($ns.'Network Servers').Count)
            # Applied / Unapplied TXs
            [void]$applied_txs.Add(($ns.'Applied TX Count'))
            [void]$unapplied_txs.Add(($ns.'Unapplied TX Count'))
            # Miner
            $ms = Get-DLTMinerStatus -Clients $Clients -NodeIdx $node.idx
            if($ms -eq $null) {
                $node.Dead = $true
                [void]$hashrates.Add(0)
                [void]$solved_counts.Add(0)
            } else {
                # Hashrates
                [void]$hashrates.Add(($ms.'Hashrate'))
                # Solved
                [void]$solved_counts.Add(($ms.'Solved Blocks (Local)'))
                if($count -eq 0) {
                    # Caputer global stats from first node
                    $global_wallets = $ns.'Wallets'
                }
                if($node.Miner -eq $true -and $mining_globals_captured -eq $false) {
                    # Capture mining globals from the first miner
                    $global_solved = $ms.'Solved Blocks (Network)'
                    $global_unsolved = $ms.'Empty Blocks'
                    $global_difficulty = $ms.'Current Difficulty'
                    $mining_globals_captured = $true
                }
            }
            $count++
            if($Count -ge 20) {
                break
            }
        }
    }
    #
    Clear-Host
    Render-Header $Clients.Count
    Render-Indexes $Clients
    Render-Status $dlt_statuses $bp_statuses    
    Render-BlockHeight $node_bhs
    Render-BlockHeightSigs $node_bh_sigs $Clients.Count
    Render-Consensus $consensus
    Render-Presences $presences
    Render-Connections $in_connections $out_connections
    Render-TXs $applied_txs $unapplied_txs
    Render-MinerHashrate $hashrates
    Render-MinerSolved $solved_counts
    Render-GlobalStats $global_solved $global_unsolved $global_wallets $global_difficulty
    Render-Footer
}
