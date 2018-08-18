using DLT.Meta;
using DLT.Network;
using DLTNode;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

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
        public bool synchronizing { get => inSyncMode; }
        public bool synchronized { get => !inSyncMode; }
        public ulong syncTarget { get => syncTargetBlockNum; }
        public int currentConsensus { get => consensusSignaturesRequired; }
        public ulong firstSplitOccurence { get; private set; }

        Block localNewBlock; // Block being worked on currently
        readonly Object localBlockLock = new object(); // used because localNewBlock can change while this lock should be held.
        DateTime lastBlockStartTime;

        bool inSyncMode = false;

        List<Block> pendingBlocks = new List<Block>();
        ulong syncTargetBlockNum;
        int consensusSignaturesRequired = 1;
        int blockGenerationInterval = 30; // in seconds
        int maxBlockRequests = 10;

        private static string[] splitter = { "::" };

        public BlockProcessor()
        {
            lastBlockStartTime = DateTime.Now;
            localNewBlock = null;
            // we start up in sync mode (except for genesis block)
            inSyncMode = Config.genesisFunds == "0";
            syncTargetBlockNum = 0;
        }

        // Returns true if a block was generated
        public bool onUpdate()
        {
            if (inSyncMode)
            {
                if (syncTargetBlockNum == 0)
                {
                    // we haven't connected to any clients yet
                    return true;
                }
                // attempt to merge pending blocks to the chain
                lock (pendingBlocks)
                {
                    while (pendingBlocks.Count > 0)
                    {
                        ulong nextRequired = Node.blockChain.getLastBlockNum() + 1;
                        int idx = pendingBlocks.FindIndex(x => x.blockNum == nextRequired);
                        if (idx > -1)
                        {
                            Block toAppend = pendingBlocks[idx];
                            pendingBlocks.RemoveAt(idx);
                            if (!Node.blockChain.appendBlock(toAppend))
                            {
                                Logging.warn(String.Format("Block #{0} could not be appended to the chain. It is possibly corrupt. Requesting a new copy...", toAppend.blockNum));
                                ProtocolMessage.broadcastGetBlock(toAppend.blockNum);
                                return true;
                            }
                        }
                        else // idx == -1
                        {
                            requestMissingBlocks();
                            break;
                        }
                    }
                    // we keep this locked so that onBlockReceive can't change syncTarget while we're processing
                    if (syncTargetBlockNum > 0 && Node.blockChain.getLastBlockNum() == syncTargetBlockNum)
                    {
                        // we cannot exit sync until wallet state is OK
                        if (WalletState.checkWalletStateChecksum(Node.blockChain.getCurrentWalletState()))
                        {
                            Logging.info(String.Format("Synchronization state achieved at block #{0}.", syncTargetBlockNum));
                            inSyncMode = false;
                            syncTargetBlockNum = 0;
                            lastBlockStartTime = DateTime.Now; // the network will probably generate a new block before we, but then we will take time from them.
                            lock (pendingBlocks)
                            {
                                pendingBlocks.Clear();
                            }
                            return true;
                        } // TODO: Possibly check if this has been going on too long and abort/restart
                    }
                }
            }
            else // !inSyncMode
            {
                // check if it is time to generate a new block
                TimeSpan timeSinceLastBlock = DateTime.Now - lastBlockStartTime;
                /*if ((int)timeSinceLastBlock.TotalSeconds % 5 == 0
                    && (timeSinceLastBlock.TotalSeconds - Math.Floor(timeSinceLastBlock.TotalSeconds) < 0.5))
                {
                    // spam only every 5 seconds
                    Logging.info(String.Format("Last block was at: {0}. That is {1} seconds in the past. {2} seconds to go.",
                        lastBlockStartTime.ToLongTimeString(),
                        timeSinceLastBlock.TotalSeconds,
                        blockGenerationInterval - timeSinceLastBlock.TotalSeconds));
                }*/
                if (timeSinceLastBlock.TotalSeconds > blockGenerationInterval || Node.forceNextBlock)
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
            }

            return true;
        }

        public void onBlockReceived(Block b)
        {
            Logging.info(String.Format("Received block #{0} ({1} sigs) from the network.", b.blockNum, b.getUniqueSignatureCount()));
            if (verifyBlock(b) == BlockVerifyStatus.Invalid)
            {
                Logging.warn(String.Format("Received block #{0} ({1}) which was invalid!", b.blockNum, b.blockChecksum));
                // TODO: Blacklisting point
                return;
            }
            //Logging.info(String.Format("Received valid block #{0} ({1}).", b.blockNum, b.blockChecksum));
            if (inSyncMode)
            {
                if (b.signatures.Count() < consensusSignaturesRequired)
                {
                    // ignore blocks which haven't been accepted while we're syncing.
                    return;
                }
                else
                {
                    lock (pendingBlocks)
                    {
                        int idx = pendingBlocks.FindIndex(x => x.blockNum == b.blockNum);
                        if (idx > -1)
                        {
                            if (pendingBlocks[idx].signatures.Count() < b.signatures.Count())
                            {
                                pendingBlocks[idx] = b;
                            }
                        }
                        else // idx <= -1
                        {
                            if (b.blockNum > syncTargetBlockNum)
                            {
                                // we move the goalpost to make sure we end up in the valid state
                                syncTargetBlockNum = b.blockNum;
                            }
                            pendingBlocks.Add(b);
                        }
                    }
                }
            }
            else // !inSyncMode
            {
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
        }

        public BlockVerifyStatus verifyBlock(Block b)
        {
            // Check all transactions in the block against our TXpool, make sure all is legal
            // Note: it is possible we don't have all the required TXs in our TXpool - in this case, request the missing ones and return Intederminate
            bool hasAllTransactions = true;
            Dictionary<string, IxiNumber> minusBalances = new Dictionary<string, IxiNumber>();
            foreach (string txid in b.transactions)
            {
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
                    IxiNumber new_minus_balance = minusBalances[t.from] + t.amount;
                    minusBalances[t.from] = new_minus_balance;
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
            // overspending:
            foreach (string addr in minusBalances.Keys)
            {
                IxiNumber initial_balance = WalletState.getBalanceForAddress(addr);
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
            // Verify signatures
            if (!b.verifySignatures())
            {
                Logging.warn(String.Format("Block #{0} failed while verifying signatures. There are invalid signatures on the block.", b.blockNum));
                return BlockVerifyStatus.Invalid;
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
                return;
            }
            lock (localBlockLock)
            {
                if (localNewBlock != null)
                {
                    if(localNewBlock.blockChecksum == b.blockChecksum)
                    {
                        //Logging.info("This is the block we are currently working on. Merging signatures.");
                        if(localNewBlock.addSignaturesFrom(b))
                        {
                            // if addSignaturesFrom returns true, that means signatures were increased, so we re-transmit
                            Logging.info(String.Format("Block #{0}: Number of signatures increased, re-transmitting. (total signatures: {1}).", b.blockNum, localNewBlock.getUniqueSignatureCount()));
                            ProtocolMessage.broadcastNewBlock(localNewBlock);
                        }
                    }
                    else
                    {
                        if(b.signatures.Count() > localNewBlock.signatures.Count())
                        {
                            Logging.info(String.Format("Incoming block #{0} has more signatures, accepting instead of our own.", b.blockNum));
                            localNewBlock = b;
                        }
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

        private void requestMissingBlocks()
        {
            if (syncTargetBlockNum == 0)
            {
                return;
            }
            lock (pendingBlocks)
            {
                ulong firstBlock = Node.blockChain.redactedWindow > syncTargetBlockNum ? 1 : syncTargetBlockNum - Node.blockChain.redactedWindow;
                ulong lastBlock = syncTargetBlockNum;
                List<ulong> missingBlocks = new List<ulong>(
                    Enumerable.Range(0, (int)(lastBlock - firstBlock)).Select(x => (ulong)x + firstBlock)
                    );
                foreach (Block b in pendingBlocks)
                {
                    missingBlocks.RemoveAll(x => x == b.blockNum);
                }
                // whatever is left in pendingBlocks is what we need to request
                Logging.info(String.Format("{0} blocks are missing before node is synchronized...", missingBlocks.Count()));
                int count = 0;
                foreach (ulong blockNum in missingBlocks)
                {
                    ProtocolMessage.broadcastGetBlock(blockNum);
                    count++;
                    if (count >= maxBlockRequests) break;
                }
            }
        }

        private void verifyBlockAcceptance()
        {
            lock (localBlockLock)
            {
                if (localNewBlock == null) return;
                if (verifyBlock(localNewBlock) == BlockVerifyStatus.Valid)
                {
                    // TODO: we will need an edge case here in the event that too many nodes dropped and consensus
                    // can no longer be reached according to this number - I don't have a clean answer yet - MZ
                    if (localNewBlock.signatures.Count() >= consensusSignaturesRequired)
                    {
                        // accept this block, apply its transactions, recalc consensus, etc
                        applyAcceptedBlock();
                        Node.blockChain.appendBlock(localNewBlock);
                        Logging.info(String.Format("Accepted block #{0}.", localNewBlock.blockNum));
                        localNewBlock.logBlockDetails();
                        localNewBlock = null;
                    }
                }
            }
        }

        private void applyAcceptedBlock()
        {
            TransactionPool.applyTransactionsFromBlock(localNewBlock);
            // recalculating consensus minimums: we use #n-5 and #n-6 to amortize potential sudden spikes
            int sigs5 = Node.blockChain.getBlockSignaturesReverse(4);
            int sigs6 = Node.blockChain.getBlockSignaturesReverse(5);
            if(sigs5 == 0 || sigs6 == 0)
            {
                // special case to bootstrap the network more smoothly
                if(Node.blockChain.getBlockSignaturesReverse(0) > 1)
                {
                    Logging.info(String.Format("Bootstrapping the consensus: Previous block had {0} signatures.", Node.blockChain.getBlockSignaturesReverse(0)));
                    consensusSignaturesRequired = Node.blockChain.getBlockSignaturesReverse(0);
                }
                return;
            }
            Logging.info(String.Format("Signatures (#{0}): {1}, Signatures ({2}): {3}",
                Node.blockChain.getLastBlockNum() - 5, sigs5,
                Node.blockChain.getLastBlockNum() - 6, sigs6));
            int avgSigs = (sigs5 + sigs6) / 2;
            int requiredSigs = (int)Math.Ceiling(avgSigs * 0.75);
            int deltaSigs = requiredSigs - consensusSignaturesRequired; // divide 2, so we amortize the change a little
            int prevConsensus = consensusSignaturesRequired;
            consensusSignaturesRequired = consensusSignaturesRequired + deltaSigs;
            if (consensusSignaturesRequired == 0)
            {
                consensusSignaturesRequired = 1;
            }
            if(consensusSignaturesRequired == 1 && Node.blockChain.getBlockSignaturesReverse(0) > 1)
            {
                Logging.info(String.Format("DLT network init: Forcing signatures to {0}.", Node.blockChain.getBlockSignaturesReverse(0)));
                consensusSignaturesRequired = Node.blockChain.getBlockSignaturesReverse(0);
            }
            if (consensusSignaturesRequired != prevConsensus)
            {
                Logging.info(String.Format("Consensus changed from {0} to {1} ({2}{3})",
                    prevConsensus,
                    consensusSignaturesRequired,
                    deltaSigs < 0 ? "-" : "+",
                    deltaSigs));
            }

            // Finally, apply transaction fee rewards
            applyTransactionFeeRewards();
        }

        // Deposits the transaction fees corresponding to the 5th last block
        public void applyTransactionFeeRewards()
        {
            string sigfreezechecksum = "0";
            lock (localNewBlock)
            {
                // Should never happen
                if (localNewBlock == null)
                    return;

                sigfreezechecksum = localNewBlock.signatureFreezeChecksum;
            }
            // Verify the checksum is valid
            if (sigfreezechecksum.Length < 3)
            {
                return;
            }

            // Obtain the 5th last block, aka target block
            // Last block num - 4 gets us the 5th last block
            Block targetBlock = Node.blockChain.getBlock(Node.blockChain.getLastBlockNum() - 4);

            // Calculate the signature checksum
            string targetSigFreezeChecksum = targetBlock.calculateSignatureChecksum();

            if (sigfreezechecksum.Equals(targetSigFreezeChecksum, StringComparison.Ordinal) == false)
            {
                Logging.info(string.Format("Signature freeze mismatch for block {0}. Current block height: {1}", targetBlock.blockNum, localNewBlock.blockNum));
                // TODO: fetch the block again or re-sync
                return;
            }

            // Calculate the total transactions amount and number of transactions in the target block
            IxiNumber tAmount = 0;
            ulong txcount = 0;
            foreach(string txid in targetBlock.transactions)
            {
                Transaction tx = TransactionStorage.getTransaction(txid);
                if (tx != null)
                {
                    tAmount += tx.amount;
                    txcount++;
                }
            }

            // Check if there are any transactions processed in the target block
            if(txcount < 1)
            { 
                return;
            }

            // Check the amount
            if(tAmount == (long) 0)
            {
                return;
            }

            // Calculate the total fee amount
            IxiNumber tFeeAmount =  Config.transactionPrice * txcount;
            IxiNumber foundationAward = tFeeAmount * Config.foundationFeePercent / 100;

            // Award foundation fee
            IxiNumber foundation_balance_before = WalletState.getBalanceForAddress(Config.foundationAddress);
            IxiNumber foundation_balance_after = foundation_balance_before + foundationAward;
            WalletState.setBalanceForAddress(Config.foundationAddress, foundation_balance_after);
            Logging.info(string.Format("Awarded {0} IXI to foundation", foundationAward.ToString()));

            // Subtract the foundation award from total fee amount
            tFeeAmount = tFeeAmount - foundationAward;

            long numSigs = targetBlock.signatures.Count();
            if(numSigs < 1)
            {
                // Something is not right, there are no signers on this block
                return;
            }

            // Calculate the award per signer
            IxiNumber sigs = new IxiNumber(numSigs);
            IxiNumber tAward = tFeeAmount / sigs; // TODO: use floor and distribute the remainder to the foundation wallet

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
                IxiNumber balance_before = WalletState.getBalanceForAddress(addr.ToString());
                IxiNumber balance_after = balance_before + tAward;
                WalletState.setBalanceForAddress(addr.ToString(), balance_after);

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
                            int consensus_number = (int)((double)localNewBlock.signatures.Count * 0.75);
                            if (consensus_number <= 0)
                            {
                                // we have become isolated from the network, so we shutdown
                                Logging.error(String.Format("Currently generated block only has {0} signaures. Attempting to reconnect to the network...", localNewBlock.signatures.Count));
                                // TODO: disable block generation and notify network to reconnect to other nodes on the PL
                                lastBlockStartTime = DateTime.MaxValue;
                                return;
                            }
                            Logging.warn(String.Format("Unable to achieve consensus. Maybe the network was split or too many nodes dropped at once. Split mode assumed and proceeding with consensus {0}.", consensus_number));
                            consensusSignaturesRequired = consensus_number;
                        }
                        else //! since_last_blockgen.TotalSeconds < (2 * blockGenerationInterval)
                        {
                            Logging.warn(String.Format("It is takign too long to achieve consensus! Re-broadcasting block."));
                            ProtocolMessage.broadcastNewBlock(localNewBlock);
                        }
                    }
                    else if (current_block_num < supposed_block_num)
                    {
                        // we are falling behind. Clear out current state and wait for the next network state
                        Logging.error(String.Format("We were processing #{0}, but that is already accepted. Lagging behind the network!", current_block_num));
                        localNewBlock = null;
                        lastBlockStartTime = DateTime.MaxValue;
                    }
                    else
                    {
                        // we are too far ahead (this should never happen)
                        Logging.error(String.Format("Logic error detected. Current block num is #{0}, but should be #{1}. Clearing state and waiting for the network.", current_block_num, supposed_block_num));
                        localNewBlock = null;
                        lastBlockStartTime = DateTime.MaxValue;
                    }
                    return;
                }

                // Create a new block and add all the transactions in the pool
                localNewBlock = new Block();
                lastBlockStartTime = DateTime.Now;
                localNewBlock.blockNum = Node.blockChain.getLastBlockNum() + 1;

                Console.WriteLine("\t\t|- Block Number: {0}", localNewBlock.blockNum);

                ulong total_transactions = 0;
                IxiNumber total_amount = 0;

                Transaction[] poolTransactions = TransactionPool.getUnappliedTransactions();
                foreach (var transaction in poolTransactions)
                {
                    //Console.WriteLine("\t\t|- tx: {0}, amount: {1}", transaction.id, transaction.amount);
                    localNewBlock.addTransaction(transaction);
                    total_amount += transaction.amount;
                    total_transactions++;
                }
                Console.WriteLine("\t\t|- Transactions: {0} \t\t Amount: {1}", total_transactions, total_amount);


                // Calculate the block checksums and sign it
                localNewBlock.setWalletStateChecksum(WalletState.calculateChecksum());
                localNewBlock.lastBlockChecksum = Node.blockChain.getLastBlockChecksum();
                localNewBlock.signatureFreezeChecksum = getSignatureFreeze();
                localNewBlock.blockChecksum = localNewBlock.calculateChecksum();
                localNewBlock.applySignature();

                localNewBlock.logBlockDetails();

                // Broadcast the new block
                ProtocolMessage.broadcastNewBlock(localNewBlock);         

            }
        }

        public string getSignatureFreeze()
        {
            // Prevent calculations if we don't have 5 fully generated blocks yet
            if(Node.blockChain.getLastBlockNum() < 5)
            {
                return "0";
            }

            // Last block num - 4 gets us the 5th last block
            Block targetBlock = Node.blockChain.getBlock(Node.blockChain.getLastBlockNum() - 4);

            // Calculate the signature checksum
            string sigFreezeChecksum = targetBlock.calculateSignatureChecksum();
            return sigFreezeChecksum;
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
