using DLT.Meta;
using DLT.Network;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Numerics;
using System.Threading;

namespace DLT
{
    enum BlockVerifyStatus
    {
        Valid,
        Invalid,
        Indeterminate
    }
    class BlockProcessor
    {
        public bool operating { get; private set; }
        public ulong firstSplitOccurence { get; private set; }

        Block localNewBlock; // Block being worked on currently
        readonly object localBlockLock = new object(); // used because localNewBlock can change while this lock should be held.
        DateTime lastBlockStartTime;

        int blockGenerationInterval = 30; // in seconds

        public bool firstBlockAfterSync;

        private static string[] splitter = { "::" };

        public BlockProcessor()
        {
            lastBlockStartTime = DateTime.Now;
            localNewBlock = null;
            operating = false;
            firstBlockAfterSync = false;
        }

        public void resumeOperation()
        {
            Logging.info("BlockProcessor resuming normal operation.");
            lastBlockStartTime = DateTime.Now;
            operating = true;
        }

        public bool onUpdate()
        {
            if(operating == false)
            {
                return true;
            }
            // check if it is time to generate a new block
            TimeSpan timeSinceLastBlock = DateTime.Now - lastBlockStartTime;
            //Logging.info(String.Format("Waiting for {0} to generate the next block #{1}. offset {2}", Node.blockChain.getLastElectedNodePubKey(getElectedNodeOffset()), Node.blockChain.getLastBlockNum()+1, getElectedNodeOffset()));
            if ((Node.isElectedToGenerateNextBlock(getElectedNodeOffset()) && timeSinceLastBlock.TotalSeconds > blockGenerationInterval) || Node.forceNextBlock)
            {
                if (Node.forceNextBlock)
                {
                    Logging.info("Forcing new block generation");
                    Node.forceNextBlock = false;
                }
                generateNewBlock();
            }
            else
            {
                verifyBlockAcceptance();
            }
            return true;
        }

        public int getElectedNodeOffset()
        {
            TimeSpan timeSinceLastBlock = DateTime.Now - lastBlockStartTime;
            if(timeSinceLastBlock.TotalSeconds > blockGenerationInterval * 15) // edge case, if network is stuck for more than 15 blocks always return 0 as the node offset.
            {
                return 0;
            }
            return (int)(timeSinceLastBlock.TotalSeconds / (blockGenerationInterval*3));
        }

        public List<string> getSignaturesWithoutPlEntry(Block b)
        {
            List<string> sigs = new List<string>();

            for (int i = 0; i < b.signatures.Count; i++)
            {
                string[] parts = b.signatures[i].Split(Block.splitter, StringSplitOptions.RemoveEmptyEntries);
                if (parts.Length != 2)
                {
                    sigs.Add(b.signatures[i]);
                    continue;
                }
                //Logging.info(String.Format("Searching for {0}", parts[1]));
                Presence p = PresenceList.presences.Find(x => x.metadata == parts[1]);
                if (p == null)
                {
                    sigs.Add(b.signatures[i]);
                    continue;
                }
            }
            return sigs;
        }

        public bool removeSignaturesWithoutPlEntry(Block b)
        {
            List<string> sigs = getSignaturesWithoutPlEntry(b);
            for (int i = 0; i < sigs.Count; i++)
            {
                b.signatures.Remove(sigs[i]);
            }
            if (sigs.Count > 0)
            {
                return true;
            }
            return false;
        }

        public List<string> getSignaturesWithLowBalance(Block b)
        {
            List<string> sigs = new List<string>();

            for (int i = 0; i < b.signatures.Count; i++)
            {
                string[] parts = b.signatures[i].Split(Block.splitter, StringSplitOptions.RemoveEmptyEntries);
                if (parts.Length != 2)
                {
                    sigs.Add(b.signatures[i]);
                    continue;
                }
                //Logging.info(String.Format("Searching for {0}", parts[1]));
                if(Node.walletState.getWalletBalance((new Address(parts[1])).ToString()) < Config.minimumMasterNodeFunds)
                {
                    sigs.Add(b.signatures[i]);
                    continue;
                }
            }
            return sigs;
        }

        public bool removeSignaturesWithLowBalance(Block b)
        {
            List<string> sigs = getSignaturesWithLowBalance(b);
            for (int i = 0; i < sigs.Count; i++)
            {
                b.signatures.Remove(sigs[i]);
            }
            if (sigs.Count > 0)
            {
                return true;
            }
            return false;
        }

        // Checks if the block has been sigFreezed and if all the hashes match and if removes sigs without a PL entry, returns false if the block should be discarded
        public bool handleSigFreezedBlock(Block b, Socket socket = null)
        {
            Block sigFreezingBlock = Node.blockChain.getBlock(b.blockNum + 5);
            if (sigFreezingBlock != null)
            {
                // this block already has a sigfreeze, don't tamper with the signatures
                Block targetBlock = Node.blockChain.getBlock(b.blockNum);
                if (targetBlock != null && sigFreezingBlock.signatureFreezeChecksum == targetBlock.calculateSignatureChecksum())
                {
                    if (b.calculateSignatureChecksum() != sigFreezingBlock.signatureFreezeChecksum)
                    {
                        // we already have the correct block but the sender does not, broadcast our block
                        //ProtocolMessage.broadcastNewBlock(targetBlock);
                        byte[] ba = ProtocolMessage.prepareProtocolMessage(ProtocolMessageCode.newBlock, targetBlock.getBytes());
                        socket.Send(ba, SocketFlags.None);
                    }
                    return false;
                }
                if (sigFreezingBlock.signatureFreezeChecksum == b.calculateSignatureChecksum())
                {
                    // this is likely the correct block, update and broadcast to others
                    targetBlock.signatures = b.signatures; // TODO TODO TODO, needs to be updated in storage as well
                    Meta.Storage.insertBlock(targetBlock);
                    ProtocolMessage.broadcastNewBlock(targetBlock, socket);
                    return false;
                }
                else
                {
                    ProtocolMessage.broadcastGetBlock(b.blockNum, socket);
                    Logging.warn(String.Format("Received block #{0} ({1}) which was sigFreezed and had an incorrect number of signatures, requesting the block from the network!", b.blockNum, b.blockChecksum));
                    return false;
                }
            }
            return true;
        }

        public void onBlockReceived(Block b, Socket socket = null)
        {
            if (operating == false) return;
            Logging.info(String.Format("Received block #{0} ({1} sigs) from the network.", b.blockNum, b.getUniqueSignatureCount()));
            if (verifyBlock(b) != BlockVerifyStatus.Valid)
            {
                Logging.warn(String.Format("Received block #{0} ({1}) which was invalid!", b.blockNum, b.blockChecksum));
                // TODO: Blacklisting point
                return;
            }
            if(!handleSigFreezedBlock(b, socket))
            {
                return;
            }
            if (removeSignaturesWithoutPlEntry(b))
            {
                Logging.warn(String.Format("Received block #{0} ({1}) which had a signature that wasn't found in the PL!", b.blockNum, b.blockChecksum));
                // TODO: Blacklisting point
            }
            if (removeSignaturesWithLowBalance(b))
            {
                Logging.warn(String.Format("Received block #{0} ({1}) which had a signature that had too low balance!", b.blockNum, b.blockChecksum));
                // TODO: Blacklisting point
            }
            // TODO TODO TODO verify sigs against WS as well?
            if (b.signatures.Count == 0)
            {
                Logging.warn(String.Format("Received block #{0} ({1}) which has no valid signatures!", b.blockNum, b.blockChecksum));
                // TODO: Blacklisting point
                return;
            }
            lock (localBlockLock)
            {
                if (b.blockNum > Node.blockChain.getLastBlockNum())
                {
                    onBlockReceived_currentBlock(b);
                }
                else
                {
                    if (Node.blockChain.refreshSignatures(b))
                    {
                        // if refreshSignatures returns true, it means that new signatures were added. re-broadcast to make sure the entire network gets this change.
                        Block updatedBlock = Node.blockChain.getBlock(b.blockNum);
                        ProtocolMessage.broadcastNewBlock(updatedBlock);
                    }
                }
            }
        }

        public BlockVerifyStatus verifyBlock(Block b, bool ignore_walletstate = false)
        {
            // first check if lastBlockChecksum and previous block's checksum match, so we can quickly discard an invalid block (possibly from a fork)
            Block prevBlock = Node.blockChain.getBlock(b.blockNum - 1);
            if (prevBlock == null && Node.blockChain.Count > 1) // block not found but blockChain is not empty, request the block
            {
                // Don't request block 0
                if (b.blockNum - 1 > 0)
                {
                    ProtocolMessage.broadcastGetBlock(b.blockNum - 1);
                }
                return BlockVerifyStatus.Indeterminate;
            }else if (prevBlock != null && b.lastBlockChecksum != prevBlock.blockChecksum) // block found but checksum doesn't match
            {
                Logging.warn(String.Format("Received block #{0} with invalid lastBlockChecksum!", b.blockNum));
                // TODO Blacklisting point?
                return BlockVerifyStatus.Invalid;
            }
            // Check all transactions in the block against our TXpool, make sure all is legal
            // Note: it is possible we don't have all the required TXs in our TXpool - in this case, request the missing ones and return Intederminate
            bool hasAllTransactions = true;
            Dictionary<string, IxiNumber> minusBalances = new Dictionary<string, IxiNumber>();
            foreach (string txid in b.transactions)
            {
                // Skip fetching staking txids if we're not synchronizing
                if(txid.StartsWith("stk"))
                {
                    if(Node.blockSync.synchronizing == false)
                        continue;
                }

                Transaction t = TransactionPool.getTransaction(txid);
                if (t == null)
                {                  
                    Logging.info(String.Format("Missing transaction '{0}'. Requesting.", txid));
                    ProtocolMessage.broadcastGetTransaction(txid);
                    hasAllTransactions = false;
                    continue;
                }
                if (!minusBalances.ContainsKey(t.from))
                {
                    minusBalances.Add(t.from, 0);
                }
                try
                {
                    // TODO: check to see if other transaction types need additional verification
                    if (t.type == (int)Transaction.Type.Normal)
                    {
                        IxiNumber new_minus_balance = minusBalances[t.from] + t.amount;
                        minusBalances[t.from] = new_minus_balance;
                    }
                }
                catch (OverflowException)
                {
                    // someone is doing something bad with this transaction, so we invalidate the block
                    // TODO: Blacklisting for the transaction originator node
                    Logging.warn(String.Format("Overflow caused by transaction {0}: amount: {1} from: {2}, to: {3}",
                        t.id, t.amount, t.from, t.to));
                    return BlockVerifyStatus.Invalid;
                }
            }
            // overspending
            foreach (string addr in minusBalances.Keys)
            {
                IxiNumber initial_balance = Node.walletState.getWalletBalance(addr);
                if (initial_balance < minusBalances[addr])
                {
                    Logging.warn(String.Format("Address {0} is attempting to overspend: Balance: {1}, Total Outgoing: {2}.",
                        addr, initial_balance, minusBalances[addr]));
                    return BlockVerifyStatus.Invalid;
                }
            }
            //
            if (!hasAllTransactions)
            {
                Logging.info(String.Format("Block #{0} is missing some transactions, which have been requested from the network.", b.blockNum));
                return BlockVerifyStatus.Indeterminate;
            }
            // Verify checksums
            string checksum = b.calculateChecksum();
            if (b.blockChecksum != checksum)
            {
                Logging.warn(String.Format("Block verification failed for #{0}. Checksum is {1}, but should be {2}.",
                    b.blockNum, b.blockChecksum, checksum));
                return BlockVerifyStatus.Invalid;
            }

            // Note: This part depends on no one else messing with WS while it runs.
            // Sometimes generateNewBlock is called from the other thread and this is invoked by network while
            // the generate thread is paused, so we need to lock
            // Note: This function is also called from BlockSync, which uses it to determine if the blocks it is syncing
            // from neighbors are OK.  However, BlockSync applies blocks before the current WS, so sometimes it doesn't
            // want to check WS checksums
            string ws_checksum = "";
            if (ignore_walletstate == false)
            {
                lock (localBlockLock)
                {
                    Node.walletState.snapshot();
                    applyAcceptedBlock(b, true);
                    ws_checksum = Node.walletState.calculateWalletStateChecksum(true);
                    Node.walletState.revert();
                }
                if (ws_checksum != b.walletStateChecksum)
                {
                    Logging.warn(String.Format("Block #{0} failed while verifying transactions: Invalid wallet state checksum! Block's WS checksum: {1}, actual WS checksum: {2}", b.blockNum, b.walletStateChecksum, ws_checksum));
                    return BlockVerifyStatus.Invalid;
                }
            }

            // Verify signatures
            if (!b.verifySignatures())
            {
                Logging.warn(String.Format("Block #{0} failed while verifying signatures. There are invalid signatures on the block.", b.blockNum));
                return BlockVerifyStatus.Invalid;
            }

            // Verify sigfreeze
            if (b.signatures.Count() >= Node.blockChain.getRequiredConsensus())
            {
                if(!verifySignatureFreezeChecksum(b))
                {
                    return BlockVerifyStatus.Indeterminate;
                }
            }

            // TODO: blacklisting would happen here - whoever sent us an invalid block is problematic
            //  Note: This will need a change in the Network code to tag incoming blocks with sender info.
            return BlockVerifyStatus.Valid;
        }

        private void onBlockReceived_currentBlock(Block b)
        {
            if (b.blockNum > Node.blockChain.getLastBlockNum() + 1)
            {
                Logging.warn(String.Format("Received block #{0}, but next block should be #{1}.", b.blockNum, Node.blockChain.getLastBlockNum() + 1));
                // TODO: keep a counter - if this happens too often, this node is falling behind the network
                for (ulong missingBlock = Node.blockChain.getLastBlockNum() + 1; missingBlock < b.blockNum; missingBlock++)
                {
                    ProtocolMessage.broadcastGetBlock(missingBlock);
                }
                return;
            }
            lock (localBlockLock)
            {
                if (localNewBlock != null)
                {
                    if(localNewBlock.blockChecksum == b.blockChecksum)
                    {
                        Logging.info("This is the block we are currently working on. Merging signatures.");
                        if(localNewBlock.addSignaturesFrom(b))
                        {
                            // if addSignaturesFrom returns true, that means signatures were increased, so we re-transmit
                            Logging.info(String.Format("Block #{0}: Number of signatures increased, re-transmitting. (total signatures: {1}).", b.blockNum, localNewBlock.getUniqueSignatureCount()));
                            ProtocolMessage.broadcastNewBlock(localNewBlock);
                        }else if(localNewBlock.signatures.Count != b.signatures.Count)
                        {
                            Logging.info(String.Format("Block #{0}: Received block has less signatures, re-transmitting local block. (total signatures: {1}).", b.blockNum, localNewBlock.getUniqueSignatureCount()));
                            ProtocolMessage.broadcastNewBlock(localNewBlock);
                        }
                    }
                    else
                    {
                        if (b.getUniqueSignatureCount() >= Node.blockChain.getRequiredConsensus() || (b.hasNodeSignature(Node.blockChain.getLastElectedNodePubKey(getElectedNodeOffset())) || firstBlockAfterSync == true))
                        {
                            Logging.info(String.Format("Incoming block #{0} has elected nodes sig or full consensus, accepting instead of our own. (total signatures: {1}, election offset: {2})", b.blockNum, b.signatures.Count, getElectedNodeOffset()));
                            localNewBlock = b;
                        }else if(b.getUniqueSignatureCount() > localNewBlock.getUniqueSignatureCount() && b.blockNum == localNewBlock.blockNum)
                        {
                            Logging.info(String.Format("Incoming block #{0} has more signatures and is the same block height, accepting instead of our own. (total signatures: {1}, election offset: {2})", b.blockNum, b.signatures.Count, getElectedNodeOffset()));
                            localNewBlock = b;
                        }
                        else
                        {
                            // discard with a warning, likely spam, resend our local block
                            Logging.info(String.Format("Incoming block #{0} doesn't have elected nodes sig, discarding and re-transmitting local block. (total signatures: {1}), election offset: {2}.", b.blockNum, b.signatures.Count, getElectedNodeOffset()));
                            ProtocolMessage.broadcastNewBlock(localNewBlock);
                        }
                        firstBlockAfterSync = false;
                    }
                }
                else // localNewBlock == null
                {
                    // this becomes the reference time for generating a new block
                    lastBlockStartTime = DateTime.Now;
                    localNewBlock = b;
                }
                if (localNewBlock.applySignature()) // applySignature() will return true, if signature was applied and false, if signature was already present from before
                {
                    ProtocolMessage.broadcastNewBlock(localNewBlock);
                }
            }
        }

        private void verifyBlockAcceptance()
        {
            bool requestBlockAgain = false;

            lock (localBlockLock)
            {
                if (localNewBlock == null) return;
                if (verifyBlock(localNewBlock) == BlockVerifyStatus.Valid)
                {
                    // TODO: we will need an edge case here in the event that too many nodes dropped and consensus
                    // can no longer be reached according to this number - I don't have a clean answer yet - MZ
                    if (localNewBlock.signatures.Count() >= Node.blockChain.getRequiredConsensus())
                    {
                        /*
                        // accept this block, apply its transactions, recalc consensus, etc
                        if (applyAcceptedBlock() == true)
                        {
                            Node.blockChain.appendBlock(localNewBlock);
                            Logging.info(String.Format("Accepted block #{0}.", localNewBlock.blockNum));
                            localNewBlock.logBlockDetails();
                            localNewBlock = null;
                        }
                        else
                        {
                            requestBlockAgain = true;
                        }*/
                        // accept this block, apply its transactions, recalc consensus, etc
                        applyAcceptedBlock(localNewBlock);
                        Node.blockChain.appendBlock(localNewBlock);
                        Logging.info(String.Format("Accepted block #{0}.", localNewBlock.blockNum));
                        localNewBlock.logBlockDetails();
                        localNewBlock = null;
                    }
                }
            }

            // Check if we should request the block again
            if (requestBlockAgain)
            {
                // Show a notification
                Console.ForegroundColor = ConsoleColor.Red;
                Logging.error(string.Format("Requesting block {0} again due to previous mismatch.", localNewBlock.blockNum));
                Console.ResetColor();
                // Sleep a bit to prevent spam
                Thread.Sleep(5000);
                // Request the block again
                ProtocolMessage.broadcastGetBlock(localNewBlock.blockNum);
            }
            return;

        }

        private bool verifySignatureFreezeChecksum(Block b)
        {
            if (b.signatureFreezeChecksum.Length > 3)
            {
                Block targetBlock = Node.blockChain.getBlock(b.blockNum - 5);
                if (targetBlock == null)
                {
                    // this shouldn't be possible
                    ProtocolMessage.broadcastGetBlock(b.blockNum - 5);
                    Logging.error(String.Format("Block verification can't be done since we are missing sigfreeze checksum target block {0}.", b.blockNum - 5));
                    return false;
                }
                string sigFreezeChecksum = targetBlock.calculateSignatureChecksum();
                if (b.signatureFreezeChecksum != sigFreezeChecksum)
                {
                    Logging.warn(String.Format("Block sigFreeze verification failed for #{0}. Checksum is {1}, but should be {2}. Requesting block #{3}",
                        b.blockNum, b.signatureFreezeChecksum, sigFreezeChecksum, b.blockNum - 5));
                    ProtocolMessage.broadcastGetBlock(b.blockNum - 5);
                    return false;
                }
            }
            else if (b.blockNum > 5)
            {
                Block targetBlock = Node.blockChain.getBlock(b.blockNum - 5);
                Logging.warn(String.Format("Block sigFreeze verification failed for #{0}. Checksum is empty but should be {1}. Requesting block #{2}",
                    b.blockNum, targetBlock.calculateSignatureChecksum(), b.blockNum - 5));
                ProtocolMessage.broadcastGetBlock(b.blockNum - 5);
                return false;
            }

            return true;
        }

        // Applies the block
        // Returns false if walletstate is not correct
        private bool applyAcceptedBlock(Block b, bool ws_snapshot = false)
        {
            // Distribute staking rewards first
            distributeStakingRewards(b, ws_snapshot);

            // Apply transactions from block
            TransactionPool.applyTransactionsFromBlock(b, ws_snapshot);

            // Apply transaction fees
            applyTransactionFeeRewards(b, ws_snapshot);

            // Save masternodes
            // TODO: find a better place for this
            Storage.savePresenceFile();

            return true;
        }

        public void applyTransactionFeeRewards(Block b, bool ws_snapshot = false)
        {
            string sigfreezechecksum = "0";
            lock (localBlockLock)
            {
                // Should never happen
                if (b == null)
                {
                    Logging.warn("Applying fee rewards: block is null.");
                    return;
                }

                sigfreezechecksum = b.signatureFreezeChecksum;
            }

            // Ignore blocks before #5
            if (b.blockNum < 5)
            {
                return;
            }

            if (sigfreezechecksum.Length < 3)
            {
                Logging.info("Current block does not have sigfreeze checksum.");
                return;
            }

            // Obtain the 5th last block, aka target block
            Block targetBlock = null;

            targetBlock = Node.blockChain.getBlock(b.blockNum - 5);
            if (targetBlock == null)
                return;

            string targetSigFreezeChecksum = targetBlock.calculateSignatureChecksum();

            if (sigfreezechecksum.Equals(targetSigFreezeChecksum, StringComparison.Ordinal) == false)
            {
                Logging.info(string.Format("Signature freeze mismatch for block {0}. Current block height: {1}", targetBlock.blockNum, b.blockNum));
                // TODO: fetch the block again or re-sync
                return;
            }

            // Calculate the total transactions amount and number of transactions in the target block
            IxiNumber tAmount = 0;
            IxiNumber tFeeAmount = 0;

            ulong txcount = 0;
            foreach(string txid in targetBlock.transactions)
            {
                Transaction tx = TransactionPool.getTransaction(txid);               
                if (tx != null)
                {
                    if (tx.type == (int)Transaction.Type.Normal)
                    {
                        tAmount += tx.amount;
                        tFeeAmount += tx.fee;
                        txcount++;
                    }
                }
            }

            // Check if there are any transactions processed in the target block
            if(txcount < 1)
            { 
                return;
            }

            // Check the amount
            if(tFeeAmount == (long) 0)
            {
                return;
            }

            // Calculate the total fee amount
            IxiNumber foundationAward = tFeeAmount * Config.foundationFeePercent / 100;

            // Award foundation fee
            Wallet foundation_wallet = Node.walletState.getWallet(Config.foundationAddress, ws_snapshot);
            IxiNumber foundation_balance_before = foundation_wallet.balance;
            IxiNumber foundation_balance_after = foundation_balance_before + foundationAward;
            Node.walletState.setWalletBalance(Config.foundationAddress, foundation_balance_after, ws_snapshot, foundation_wallet.nonce);
            Logging.info(string.Format("Awarded {0} IXI to foundation", foundationAward.ToString()));

            // Subtract the foundation award from total fee amount
            tFeeAmount = tFeeAmount - foundationAward;

            ulong numSigs = (ulong)targetBlock.signatures.Count();
            if(numSigs < 1)
            {
                // Something is not right, there are no signers on this block
                Logging.error("Transaction fee: no signatures on block!");
                return;
            }

            // Calculate the award per signer
            IxiNumber sigs = new IxiNumber(numSigs, false);

            IxiNumber tAward = IxiNumber.divRem(tFeeAmount, sigs, out IxiNumber remainder);

            // Division of fee amount and sigs left a remainder, distribute that to the foundation wallet
            if (remainder > (long) 0)
            {
                foundation_balance_after = foundation_balance_after + remainder;
                Node.walletState.setWalletBalance(Config.foundationAddress, foundation_balance_after, ws_snapshot, foundation_wallet.nonce);
                Logging.info(string.Format("Awarded {0} IXI to foundation from fee division remainder", foundationAward.ToString()));
            }

            // Go through each signature in the block
            foreach (string sig in targetBlock.signatures)
            {
                // Extract the public key
                string[] parts = sig.Split(splitter, StringSplitOptions.RemoveEmptyEntries);
                string pubkey = parts[1];
                if(pubkey.Length < 1)
                {
                    // TODO: find out what to do here. Perhaps send fee to the foundation wallet?
                    continue;
                }

                // Generate the corresponding Ixian address
                Address addr = new Address(pubkey);

                // Update the walletstate and deposit the award
                Wallet signer_wallet = Node.walletState.getWallet(addr.ToString(), ws_snapshot);
                IxiNumber balance_before = signer_wallet.balance;
                IxiNumber balance_after = balance_before + tAward;
                Node.walletState.setWalletBalance(addr.ToString(), balance_after, ws_snapshot, signer_wallet.nonce);

                Logging.info(string.Format("Awarded {0} IXI to {1}", tAward.ToString(), addr.ToString()));
            }

            // Output stats for this block's fee distribution
            Logging.info(string.Format("Total block TX amount: {0} Total TXs: {1} Reward per Signer: {2} Foundation Reward: {3}", tAmount.ToString(), txcount, 
                tAward.ToString(), foundationAward.ToString()));
          
        }

        // Generate a new block
        public void generateNewBlock()
        {
            lock (localBlockLock)
            {
                Console.ForegroundColor = ConsoleColor.Cyan;
                Console.WriteLine("GENERATING NEW BLOCK");
                Console.ResetColor();
                if (localNewBlock != null)
                {
                    Node.debugDumpState();
                    Logging.info(String.Format("Local new block #{0}, sigs: {1}, checksum: {2}, wsChecksum: {3}", localNewBlock.blockNum, localNewBlock.signatures.Count, localNewBlock.blockChecksum, localNewBlock.walletStateChecksum));
                    // either it arrived just before we started creating it, or previous block couldn't get signed in time
                    ulong current_block_num = localNewBlock.blockNum;
                    ulong supposed_block_num = Node.blockChain.getLastBlockNum() + 1;
                    if (current_block_num == supposed_block_num)
                    {
                        // this means the block currently being processed couldn't be signed in time.
                        TimeSpan since_last_blockgen = DateTime.Now - lastBlockStartTime;
                        if ((int)since_last_blockgen.TotalSeconds >= (2 * blockGenerationInterval))
                        {
                            // it has been two generation cycles without enough signatures
                            // we assume a network split (or a massive node drop) here and fix consensus to keep going
                            firstSplitOccurence = current_block_num; // This should be handled when network merges again, but that part isn't implemented yet
                            int consensus_number = Node.blockChain.getRequiredConsensus();
                            Logging.warn(String.Format("Unable to reach consensus. Maybe the network was split or too many nodes dropped at once. Split mode assumed and proceeding with consensus {0}.", consensus_number));
                            if (localNewBlock.signatures.Count < consensus_number)
                            {
                                // we have become isolated from the network, so we shutdown
                                Logging.error(String.Format("Currently generated block only has {0} signatures. Attempting to reconnect to the network...", localNewBlock.signatures.Count));
                                // TODO: notify network to reconnect to other nodes on the PL
                                // TODO TODO TODO : Split handling
                            }
                            //lastBlockStartTime = DateTime.Now;
                            return;
                        }
                        else //! since_last_blockgen.TotalSeconds < (2 * blockGenerationInterval)
                        {
                            Logging.warn(String.Format("It is taking too long to reach consensus! Re-broadcasting block."));
                            ProtocolMessage.broadcastNewBlock(localNewBlock);
                        }
                    }
                    else if (current_block_num < supposed_block_num)
                    {
                        // we are falling behind. Clear out current state and wait for the next network state
                        Logging.error(String.Format("We were processing #{0}, but that is already accepted. Lagging behind the network!", current_block_num));
                        localNewBlock = null;
                        lastBlockStartTime = DateTime.Now;
                    }
                    else
                    {
                        // we are too far ahead (this should never happen)
                        Logging.error(String.Format("Logic error detected. Current block num is #{0}, but should be #{1}. Clearing state and waiting for the network.", current_block_num, supposed_block_num));
                        localNewBlock = null;
                        lastBlockStartTime = DateTime.Now;
                    }
                    return;
                }

                // Create a new block and add all the transactions in the pool
                localNewBlock = new Block();
                lastBlockStartTime = DateTime.Now;
                localNewBlock.blockNum = Node.blockChain.getLastBlockNum() + 1;

                Console.WriteLine("\t\t|- Block Number: {0}", localNewBlock.blockNum);

                // Apply signature freeze
                localNewBlock.signatureFreezeChecksum = getSignatureFreeze();

                ulong total_transactions = 0;
                IxiNumber total_amount = 0;

                Transaction[] pool_transactions = TransactionPool.getUnappliedTransactions();
                foreach (var transaction in pool_transactions)
                {
                    //Console.WriteLine("\t\t|- tx: {0}, amount: {1}", transaction.id, transaction.amount);
                    // TODO: add an if check if adding the transaction failed ?

                    // Skip adding staking rewards
                    if(transaction.type == (int)Transaction.Type.StakingReward)
                    {
                        continue;
                    }

                    localNewBlock.addTransaction(transaction);
                    total_amount += transaction.amount;
                    total_transactions++;
                }

                // Apply staking transactions to block. 
                // Generate the staking transactions with the blockgen flag, as we are the current block generator
                List<Transaction> staking_transactions = generateStakingTransactions(true);
                foreach (Transaction transaction in staking_transactions)
                {
                    localNewBlock.addTransaction(transaction);
                    total_amount += transaction.amount;
                    total_transactions++;
                }
                staking_transactions.Clear();

                Console.WriteLine("\t\t|- Transactions: {0} \t\t Amount: {1}", total_transactions, total_amount);

                // Calculate mining difficulty
                localNewBlock.difficulty = calculateDifficulty();

                // Simulate applying a block to see what the walletstate would look like
                Node.walletState.snapshot();
                applyAcceptedBlock(localNewBlock, true);
                localNewBlock.setWalletStateChecksum(Node.walletState.calculateWalletStateChecksum(true));
                Node.walletState.revert();

                // Calculate the block checksums and sign it
                //localNewBlock.setWalletStateChecksum(Node.walletState.calculateWalletStateChecksum());
                localNewBlock.lastBlockChecksum = Node.blockChain.getLastBlockChecksum();
                localNewBlock.blockChecksum = localNewBlock.calculateChecksum();
                localNewBlock.applySignature();

                localNewBlock.logBlockDetails();

                // Broadcast the new block
                ProtocolMessage.broadcastNewBlock(localNewBlock);         

            }
        }

        // Calculate the current mining difficulty
        public ulong calculateDifficulty()
        {
            ulong current_difficulty = 14;
            if (localNewBlock.blockNum > 1)
            {
                Block previous_block = Node.blockChain.getBlock(Node.blockChain.getLastBlockNum());
                if (previous_block != null)
                    current_difficulty = previous_block.difficulty;

                // Increase or decrease the difficulty according to the number of solved blocks in the redacted window
                ulong solved_blocks = Node.blockChain.getSolvedBlocksCount();
                if (solved_blocks > Node.blockChain.redactedWindowSize / 2)
                {
                    current_difficulty++;
                }
                else
                {
                    current_difficulty--;
                }

                // Set some limits
                if (current_difficulty > 256)
                    current_difficulty = 256;
                else if (current_difficulty < 14)
                    current_difficulty = 14;

            }

            return current_difficulty;
        }

        // Retrieve the signature freeze of the 5th last block
        public string getSignatureFreeze()
        {
            // Prevent calculations if we don't have 5 fully generated blocks yet
            if(Node.blockChain.getLastBlockNum() < 5)
            {
                return "0";
            }

            // Last block num - 4 gets us the 5th last block
            Block targetBlock = Node.blockChain.getBlock(Node.blockChain.getLastBlockNum() - 4);
            if (targetBlock == null)
            {
                return "0";
            }

            // Calculate the signature checksum
            string sigFreezeChecksum = targetBlock.calculateSignatureChecksum();
            return sigFreezeChecksum;
        }

        // Generate all the staking transactions for this block
        private List<Transaction> generateStakingTransactions(bool blockgen = false)
        {
            List<Transaction> transactions = new List<Transaction>();
            // Prevent distribution if we don't have 10 fully generated blocks yet
            if (Node.blockChain.getLastBlockNum() < 10)
            {
                return transactions;
            }

            // Last block num - 4 gets us the 5th last block
            ulong targetBlockNum = Node.blockChain.getLastBlockNum() - 4;

            // Check if this is called from the block generator and if so, use the localNewBlock
            if (blockgen == true)
            {
                targetBlockNum = localNewBlock.blockNum - 5;
            }

            Block targetBlock = Node.blockChain.getBlock(targetBlockNum);
            if (targetBlock == null)
            {
                return transactions;
            }

            IxiNumber totalIxis = Node.walletState.calculateTotalSupply();
            IxiNumber inflationPA = new IxiNumber("10"); // 10% inflation per year

            // Set the anual inflation to 5% after 50bn IXIs in circulation 
            if (totalIxis > new IxiNumber("50000000000"))
            {
                inflationPA = new IxiNumber("5");
            }

            // Calculate the amount of new IXIs to be minted
            IxiNumber newIxis = totalIxis * inflationPA / new IxiNumber("100000000");
            Console.ForegroundColor = ConsoleColor.Magenta;
            Console.WriteLine("----STAKING REWARDS for #{0} TOTAL {1} IXIs----", targetBlock.blockNum, newIxis.ToString());
            // Retrieve the list of signature wallets
            List<string> signatureWallets = targetBlock.getSignaturesWalletAddresses();

            IxiNumber totalIxisStaked = new IxiNumber(0);
            int stakers = signatureWallets.Count;
            BigInteger[] stakes = new BigInteger[signatureWallets.Count];
            BigInteger[] awards = new BigInteger[signatureWallets.Count];
            BigInteger[] awardRemainders = new BigInteger[signatureWallets.Count];
            // First pass, go through each wallet to find its balance
            int idx = 0;
            foreach (string wallet_addr in signatureWallets)
            {
                Wallet wallet = Node.walletState.getWallet(wallet_addr);
                totalIxisStaked += wallet.balance;
                stakes[idx] = wallet.balance.getAmount();
                idx += 1;
            }

            // Second pass, determine awards by stake
            BigInteger totalAwarded = 0;
            for (int i = 0; i < stakes.Length; i++)
            {
                BigInteger p = (newIxis.getAmount() * stakes[i] * 100) / totalIxisStaked.getAmount();
                awardRemainders[i] = p % 100;
                p = p / 100;
                awards[i] = p;
                totalAwarded += p;
            }

            // Third pass, distribute remainders, if any
            // This essentially "rounds up" the awards for the stakers closest to the next whole amount,
            // until we bring the award difference down to zero.
            BigInteger diffAward = newIxis.getAmount() - totalAwarded;
            if (diffAward > 0)
            {
                int[] descRemaindersIndexes = awardRemainders
                    .Select((v, pos) => new KeyValuePair<BigInteger, int>(v, pos))
                    .OrderByDescending(x => x.Key)
                    .Select(x => x.Value).ToArray();
                int currRemainderAward = 0;
                while (diffAward > 0)
                {
                    awards[descRemaindersIndexes[currRemainderAward]] += 1;
                    currRemainderAward += 1;
                    diffAward -= 1;
                }
            }

            for (int i = 0; i < stakes.Length; i++)
            {
                IxiNumber award = new IxiNumber(awards[i]);
                if (award > (long)0)
                {
                    string wallet_addr = signatureWallets[i];
                    Console.WriteLine("----> Awarding {0} to {1}", award, wallet_addr);

                    Transaction tx = new Transaction();
                    tx.type = (int)Transaction.Type.StakingReward;
                    tx.to = wallet_addr;
                    tx.from = "IxianInfiniMine2342342342342342342342342342342342342342342342342db32";

                    tx.amount = award;

                    string data = string.Format("{0}||{1}||{2}", Node.walletStorage.publicKey, targetBlock.blockNum, "b");
                    tx.data = data;
                    tx.timeStamp = Clock.getTimestamp(DateTime.Now);
                    tx.id = "stk-" + targetBlock.blockNum + "-" + tx.generateID(); // Staking-specific txid
                    tx.checksum = Transaction.calculateChecksum(tx);
                    tx.signature = "Stake";

                    transactions.Add(tx);
                }

            }
            Console.WriteLine("------");
            Console.ResetColor();


            return transactions;
        }


        // Distribute the staking rewards according to the 5th last block signatures
        public bool distributeStakingRewards(Block b, bool ws_snapshot = false)
        {
            if (ws_snapshot)
                return true;
            // Prevent distribution if we don't have 10 fully generated blocks yet
            if (Node.blockChain.getLastBlockNum() < 10)
            {
                return false;
            }

            List<Transaction> transactions = generateStakingTransactions();      
            foreach(Transaction transaction in transactions)
            {
                if(!TransactionPool.addTransaction(transaction, true))
                {
                    Logging.warn("An error occured while trying to add staking transaction");
                }
            }
            
            return true;
        }

        public void storeStakingRewards()
        {
            // Prevent distribution if we don't have 10 fully generated blocks yet
            if (Node.blockChain.getLastBlockNum() < 10)
            {
                return;
            }

            List<Transaction> transactions = generateStakingTransactions();
            foreach (Transaction transaction in transactions)
            {
                Meta.Storage.insertTransaction(transaction);
            }


        }



        public bool hasNewBlock()
        {
            return localNewBlock != null;
        }

        public Block getLocalBlock()
        {
            return localNewBlock;
        }
    }
}
