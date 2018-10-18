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
        public readonly object localBlockLock = new object(); // used because localNewBlock can change while this lock should be held.
        DateTime lastBlockStartTime;

        int blockGenerationInterval = 30; // in seconds

        public bool firstBlockAfterSync;

        private static string[] splitter = { "::" };

        private SortedList<ulong, int> fetchingTxForBlocks = new SortedList<ulong, int>();
        private SortedList<ulong, int> fetchingBulkTxForBlocks = new SortedList<ulong, int>();

        private Thread block_thread = null;

        private ulong highestNetworkBlockNum = 0;

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

            lock (localBlockLock)
            {

                // Abort the thread if it's already created
                if (block_thread != null)
                    block_thread.Abort();

                // Start the thread
                block_thread = new Thread(onUpdate);
                block_thread.Start();
            }
        }

        public void stopOperation()
        {
            operating = false;
            Logging.info("BlockProcessor stopped.");
        }

        // Check passed time since last block generation and if needed generate a new block
        public void onUpdate()
        {
            while (operating)
            {
                // check if it is time to generate a new block
                TimeSpan timeSinceLastBlock = DateTime.Now - lastBlockStartTime;
                if (Node.blockChain.getLastBlockNum() < 10)
                {
                    blockGenerationInterval = 5;
                }
                else
                {
                    blockGenerationInterval = 30;
                }

                bool forceNextBlock = Node.forceNextBlock;
                Random rnd = new Random();
                if (localNewBlock == null && timeSinceLastBlock.TotalSeconds > (blockGenerationInterval * 15) + rnd.Next(30)) // no block for 15 block times + random seconds, we don't want all nodes sending at once
                {
                    forceNextBlock = true;
                }

                //Logging.info(String.Format("Waiting for {0} to generate the next block #{1}. offset {2}", Node.blockChain.getLastElectedNodePubKey(getElectedNodeOffset()), Node.blockChain.getLastBlockNum()+1, getElectedNodeOffset()));
                if ((localNewBlock == null && (Node.isElectedToGenerateNextBlock(getElectedNodeOffset()) && timeSinceLastBlock.TotalSeconds >= blockGenerationInterval)) || forceNextBlock)
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

                // Sleep until next iteration
                Thread.Sleep(1000);
            }
            Thread.Yield();
            return;
        }

        // returns offset depending on time since last block and block generation interval. This function will return -1 if more than 10 block generation intervals have passed
        public int getElectedNodeOffset()
        {
            TimeSpan timeSinceLastBlock = DateTime.Now - lastBlockStartTime;
            if(timeSinceLastBlock.TotalSeconds > blockGenerationInterval * 10) // edge case, if network is stuck for more than 10 blocks always return 0 as the node offset.
            {
                return -1;
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

                Presence p = null;
                // Check if we have a public key instead of an address
                if (parts[1].Length > 70)
                {
                    p = PresenceList.presences.Find(x => x.metadata == parts[1]);
                }
                else
                {
                    p = PresenceList.presences.Find(x => x.wallet == parts[1]);
                }

                //Logging.info(String.Format("Searching for {0}", parts[1]));                 
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

                string address = parts[1];
                // Check if we have a public key instead of an address
                if (address.Length > 70)
                {
                    address = (new Address(parts[1])).ToString();
                }

                if (Node.walletState.getWalletBalance(address) < Config.minimumMasterNodeFunds)
                {
                    Logging.error(String.Format("LOW BALANCE ADDRESS: {0} FUNDS: {1}", address, Node.walletState.getWalletBalance(address)));
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

        // Checks if the block has been sigFreezed and if all the hashes match, returns false if the block should be discarded
        public bool handleSigFreezedBlock(Block b, RemoteEndpoint endpoint = null, RemoteEndpoint skipEndpoint = null)
        {
            Block sigFreezingBlock = Node.blockChain.getBlock(b.blockNum + 5);
            string sigFreezeChecksum = null;
            lock (localBlockLock)
            {
                if (sigFreezingBlock == null && localNewBlock != null && localNewBlock.blockNum == b.blockNum + 5)
                {
                    sigFreezingBlock = localNewBlock;
                }
            }
            if (sigFreezingBlock != null)
            {
                lock (localBlockLock)
                {
                    sigFreezeChecksum = sigFreezingBlock.signatureFreezeChecksum;
                }
                // this block already has a sigfreeze, don't tamper with the signatures
                Block targetBlock = Node.blockChain.getBlock(b.blockNum);
                if (targetBlock != null && sigFreezeChecksum == targetBlock.calculateSignatureChecksum())
                {
                    // we already have the correct block
                    if (b.calculateSignatureChecksum() != sigFreezeChecksum)
                    {
                        // we already have the correct block but the sender does not, broadcast our block
                        //ProtocolMessage.broadcastNewBlock(targetBlock);
                        endpoint.sendData(ProtocolMessageCode.newBlock, targetBlock.getBytes());
                    }
                    return false;
                }
                if (sigFreezeChecksum == b.calculateSignatureChecksum())
                {
                    Logging.warn(String.Format("Received block #{0} ({1}) which was sigFreezed with correct checksum, force updating signatures locally!", b.blockNum, b.blockChecksum));
                    // this is likely the correct block, update and broadcast to others
                    Node.blockChain.refreshSignatures(b, true);
                    ProtocolMessage.broadcastNewBlock(targetBlock, skipEndpoint);
                    return false;
                }
                else
                {
                    Logging.warn(String.Format("Received block #{0} ({1}) which was sigFreezed and had an incorrect number of signatures, requesting the block from the network!", b.blockNum, b.blockChecksum));
                    ProtocolMessage.broadcastGetBlock(b.blockNum, skipEndpoint);
                    return false;
                }
            }
            return true;
        }

        public void onBlockReceived(Block b, RemoteEndpoint endpoint = null)
        {
            if (operating == false) return;
            Logging.info(String.Format("Received block #{0} {1} ({2} sigs) from the network.", b.blockNum, b.blockChecksum, b.getUniqueSignatureCount()));

            // if historic block, only the sigs should be updated if not older than 5 blocks in history
            if (b.blockNum <= Node.blockChain.getLastBlockNum())
            {
                if (b.blockNum >= Node.blockChain.getLastBlockNum() - 5)
                {
                    Logging.info(String.Format("Already processed block #{0}, doing basic verification and collecting only sigs if relevant!", b.blockNum));
                    Block localBlock = Node.blockChain.getBlock(b.blockNum);
                    if (b.blockChecksum == localBlock.blockChecksum && verifyBlockBasic(b) == BlockVerifyStatus.Valid)
                    {
                        if (handleSigFreezedBlock(b, endpoint))
                        {
                            removeSignaturesWithoutPlEntry(b);
                            removeSignaturesWithLowBalance(b);
                            b.applySignature();
                            if (Node.blockChain.refreshSignatures(b))
                            {
                                // if refreshSignatures returns true, it means that new signatures were added. re-broadcast to make sure the entire network gets this change.
                                Block updatedBlock = Node.blockChain.getBlock(b.blockNum);
                                ProtocolMessage.broadcastNewBlock(updatedBlock);
                            }
                        }
                    }else
                    {
                        // we likely have to correct block, resend
                        // TODO TODO TODO this might go into an endless loop between 2 nodes
                        ProtocolMessage.broadcastNewBlock(localBlock);
                    }
                }
                return;
            }

            if (verifyBlock(b) != BlockVerifyStatus.Valid)
            {
                Logging.warn(String.Format("Received block #{0} ({1}) which was invalid!", b.blockNum, b.blockChecksum));
                // TODO: Blacklisting point
                return;
            }
            // remove signatures without PL entry but not if we're catching up with the network
            if (b.blockNum > highestNetworkBlockNum - 5 && removeSignaturesWithoutPlEntry(b))
            {
                Logging.warn(String.Format("Received block #{0} ({1}) which had a signature that wasn't found in the PL!", b.blockNum, b.blockChecksum));
                // TODO: Blacklisting point
            }
            if (removeSignaturesWithLowBalance(b))
            {
                Logging.warn(String.Format("Received block #{0} ({1}) which had a signature that had too low balance!", b.blockNum, b.blockChecksum));
                // TODO: Blacklisting point
            }
            if (b.signatures.Count == 0)
            {
                Logging.warn(String.Format("Received block #{0} ({1}) which has no valid signatures!", b.blockNum, b.blockChecksum));
                // TODO: Blacklisting point
                return;
            }

            // TODOBLOCK
            lock (localBlockLock)
            {
                if (b.blockNum > Node.blockChain.getLastBlockNum())
                {
                    onBlockReceived_currentBlock(b);
                }
            }
        }

        public BlockVerifyStatus verifyBlockBasic(Block b)
        {
            // first check if lastBlockChecksum and previous block's checksum match, so we can quickly discard an invalid block (possibly from a fork)
            Block prevBlock = Node.blockChain.getBlock(b.blockNum - 1);

            if (prevBlock != null && b.lastBlockChecksum != prevBlock.blockChecksum) // block found but checksum doesn't match
            {
                Logging.warn(String.Format("Received block #{0} with invalid lastBlockChecksum!", b.blockNum));
                // TODO Blacklisting point?
                return BlockVerifyStatus.Invalid;
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

            if (prevBlock == null && Node.blockChain.Count > 1) // block not found but blockChain is not empty, request the missing blocks
            {
                if (!Node.blockSync.synchronizing)
                {
                    // Don't request block 0
                    if (b.blockNum - 1 > 0 && highestNetworkBlockNum < b.blockNum)
                    {
                        if (removeSignaturesWithLowBalance(b))
                        {
                            Logging.warn(String.Format("Received block #{0} ({1}) which had a signature that had too low balance!", b.blockNum, b.blockChecksum));
                            // TODO: Blacklisting point
                        }
                        if (removeSignaturesWithoutPlEntry(b))
                        {
                            Logging.warn(String.Format("Received block #{0} ({1}) which had a signature that wasn't found in the PL!", b.blockNum, b.blockChecksum));
                            // TODO: Blacklisting point
                        }
                        // blocknum is higher than the network's, switching to catch-up mode, but only if full consensus is reached on the block
                        if (b.blockNum > Node.blockChain.getLastBlockNum() + 2 && b.getUniqueSignatureCount() >= Node.blockChain.getRequiredConsensus()) // if at least 2 blocks behind
                        {
                            highestNetworkBlockNum = b.blockNum;
                        }
                    }
                    ProtocolMessage.broadcastGetBlock(Node.blockChain.getLastBlockNum() + 1);
                }
                return BlockVerifyStatus.Indeterminate;
            }
            // Verify sigfreeze
            if (b.blockNum <= Node.blockChain.getLastBlockNum())
            {
                if (!verifySignatureFreezeChecksum(b))
                {
                    return BlockVerifyStatus.Indeterminate;
                }
            }

            // TODO: blacklisting would happen here - whoever sent us an invalid block is problematic
            //  Note: This will need a change in the Network code to tag incoming blocks with sender info.
            return BlockVerifyStatus.Valid;
        }

        public BlockVerifyStatus verifyBlock(Block b, bool ignore_walletstate = false)
        {

            var sw = new System.Diagnostics.Stopwatch();
            sw.Start();

            BlockVerifyStatus basicVerification = verifyBlockBasic(b);

            if (basicVerification != BlockVerifyStatus.Valid)
            {
                return basicVerification;
            }

            if (Node.blockChain.getLastBlockNum() + 1 != b.blockNum)
            {
                // TODO TODO TODO verify if the block matches the one in blockchain and validate it
                return BlockVerifyStatus.Indeterminate;
            }

            // Check all transactions in the block against our TXpool, make sure all is legal
            // Note: it is possible we don't have all the required TXs in our TXpool - in this case, request the missing ones and return Indeterminate
            bool hasAllTransactions = true;
            bool fetchTransactions = false;
            int txTimeout = 0;
            lock (fetchingTxForBlocks)
            {
                if (fetchingTxForBlocks.ContainsKey(b.blockNum))
                {
                    txTimeout = fetchingTxForBlocks[b.blockNum];
                }
            }
            if (txTimeout > 100) // TODO TODO TODO change this 100 to 20 for extra network buffer fun
            {
                Logging.info("fetchingTxTimeout EXPIRED");
                txTimeout = 0;
                fetchTransactions = true;
            }
            int txCount = 0;
            int missing = 0;
            Dictionary<string, IxiNumber> minusBalances = new Dictionary<string, IxiNumber>();
            foreach (string txid in b.transactions)
            {
                // Skip fetching staking txids if we're not synchronizing
                if (txid.StartsWith("stk"))
                {
                    if (Node.blockSync.synchronizing == false || Config.recoverFromFile)
                        continue;
                }

                Transaction t = TransactionPool.getTransaction(txid, Node.blockSync.synchronizing);
                if (t == null)
                {
                    if(fetchTransactions)
                    {
                        Logging.info(String.Format("Missing transaction '{0}'. Requesting.", txid));
                        ProtocolMessage.broadcastGetTransaction(txid);
                        hasAllTransactions = false;
                        missing++;
                    }
                    else
                    {
                        hasAllTransactions = false;
                        missing = b.transactions.Count;
                        break;
                    }
                    continue;
                }else if(Node.blockSync.synchronizing && TransactionPool.getTransaction(txid) == null)
                {
                    t.applied = 0;
                    TransactionPool.addTransaction(t, true);
                }
                if (!minusBalances.ContainsKey(t.from))
                {
                    minusBalances.Add(t.from, 0);
                }
                try
                {
                    // TODO TODO TODO verify nonces

                    // TODO: check to see if other transaction types need additional verification
                    if (t.type == (int)Transaction.Type.Normal)
                    {
                        txCount++;
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
            if((ulong)txCount > Config.maximumTransactionsPerBlock + 10)
            {
                Logging.warn(String.Format("Block has more transactions than the maximumTransactionsPerBlock setting {0}/{1}", txCount, Config.maximumTransactionsPerBlock + 10));
                return BlockVerifyStatus.Invalid;
            }
            //
            if (!hasAllTransactions)
            {
                lock (fetchingTxForBlocks)
                {
                    if (!fetchingBulkTxForBlocks.ContainsKey(b.blockNum))
                    {
                        fetchingBulkTxForBlocks.Add(b.blockNum, 0);
                        fetchingTxForBlocks.Add(b.blockNum, 0);
                        ProtocolMessage.broadcastGetBlockTransactions(b.blockNum, Node.blockSync.synchronizing);
                    }
                    fetchingBulkTxForBlocks.AddOrReplace(b.blockNum, txTimeout + 1);
                    fetchingTxForBlocks.AddOrReplace(b.blockNum, txTimeout + 1);
                }
                Thread.Sleep(100); // TODO TODO TODO hack, remove for fun with network buffers
                Logging.info(String.Format("Block #{0} is missing {1} transactions, which have been requested from the network.", b.blockNum, missing));
                return BlockVerifyStatus.Indeterminate;
            }
            lock (fetchingTxForBlocks)
            {
                fetchingBulkTxForBlocks.Remove(b.blockNum);
                fetchingTxForBlocks.Remove(b.blockNum);
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

                lock (localBlockLock)
                {
                    // ignore wallet state check if it isn't the current block
                    if (b.blockNum < Node.blockChain.getLastBlockNum())
                    {
                        Logging.info(String.Format("Not verifying wallet state for old block {0}", b.blockNum));
                    }else if(b.blockNum == Node.blockChain.getLastBlockNum())
                    {
                        ws_checksum = Node.walletState.calculateWalletStateChecksum();
                        // this should always be the same anyway, but just in case
                        if (b.walletStateChecksum != ws_checksum)
                        {
                            Logging.error(String.Format("Incorrect current wallet state checksum for the last block #{0} Block's WS checksum: {1}, actual WS checksum: {2}", b.blockNum, b.walletStateChecksum, ws_checksum));
                            return BlockVerifyStatus.Invalid;
                        }
                    }
                    else
                    {
                        Node.walletState.snapshot();
                        if (applyAcceptedBlock(b, true))
                        {
                            ws_checksum = Node.walletState.calculateWalletStateChecksum(true);
                        }
                        Node.walletState.revert();
                        if (ws_checksum != b.walletStateChecksum)
                        {
                            Logging.warn(String.Format("Block #{0} failed while verifying transactions: Invalid wallet state checksum! Block's WS checksum: {1}, actual WS checksum: {2}", b.blockNum, b.walletStateChecksum, ws_checksum));
                            return BlockVerifyStatus.Invalid;
                        }
                    }
                }
            }


            sw.Stop();
            TimeSpan elapsed = sw.Elapsed;
            Logging.info(string.Format("VerifyBlock duration: {0}ms", elapsed.TotalMilliseconds));


            // TODO: blacklisting would happen here - whoever sent us an invalid block is problematic
            //  Note: This will need a change in the Network code to tag incoming blocks with sender info.
            return BlockVerifyStatus.Valid;
        }

        private void onBlockReceived_currentBlock(Block b)
        {
            if (b.blockNum != Node.blockChain.getLastBlockNum() + 1)
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
                        Logging.info("This is the block we are currently working on. Merging signatures.");
                        if(localNewBlock.addSignaturesFrom(b))
                        {
                            if (Node.isWorkerNode())
                                return;
                            // if addSignaturesFrom returns true, that means signatures were increased, so we re-transmit
                            Logging.info(String.Format("Block #{0}: Number of signatures increased, re-transmitting. (total signatures: {1}).", b.blockNum, localNewBlock.getUniqueSignatureCount()));
                            ProtocolMessage.broadcastNewBlock(localNewBlock);
                        }else if(localNewBlock.signatures.Count != b.signatures.Count)
                        {
                            if (Node.isWorkerNode())
                                return;
                            Logging.info(String.Format("Block #{0}: Received block has less signatures, re-transmitting local block. (total signatures: {1}).", b.blockNum, localNewBlock.getUniqueSignatureCount()));
                            ProtocolMessage.broadcastNewBlock(localNewBlock);
                        }
                    }
                    else
                    {
                        int blockSigCount = b.getUniqueSignatureCount();
                        int localBlockSigCount = localNewBlock.getUniqueSignatureCount();
                        if(blockSigCount > localBlockSigCount && b.blockNum == localNewBlock.blockNum)
                        {
                            Logging.info(String.Format("Incoming block #{0} has more signatures and is the same block height, accepting instead of our own. (total signatures: {1}, election offset: {2})", b.blockNum, b.signatures.Count, getElectedNodeOffset()));
                            localNewBlock = b;
                        }
                        else
                        {
                            if (Node.isWorkerNode())
                                return;
                            // discard with a warning, likely spam, resend our local block
                            Logging.info(String.Format("Incoming block #{0} is different than our own and doesn't have more sigs, discarding and re-transmitting local block. (total signatures: {1}), election offset: {2}.", b.blockNum, b.signatures.Count, getElectedNodeOffset()));
                            ProtocolMessage.broadcastNewBlock(localNewBlock);
                        }
                    }
                }
                else // localNewBlock == null
                {
                    bool hasNodeSig = true;
                    if(getElectedNodeOffset() != -1)
                    {
                        hasNodeSig = b.hasNodeSignature(Node.blockChain.getLastElectedNodePubKey(getElectedNodeOffset()));
                    }
                    if (hasNodeSig
                        || b.getUniqueSignatureCount() >= Node.blockChain.getRequiredConsensus()/2 // TODO TODO TODO think about /2 thing
                        || firstBlockAfterSync)
                    {
                        localNewBlock = b;
                        firstBlockAfterSync = false;
                    }
                    else
                    {
                        Logging.warn(String.Format("Incoming block #{0} doesn't have elected node's sig, waiting for a new block. (total signatures: {1}), election offset: {2}.", b.blockNum, b.signatures.Count, getElectedNodeOffset()));
                    }
                }

                if (Node.isWorkerNode())
                    return;

                if (localNewBlock != null && localNewBlock.applySignature()) // applySignature() will return true, if signature was applied and false, if signature was already present from before
                {
                    ProtocolMessage.broadcastNewBlock(localNewBlock);
                }
            }
        }

        private void verifyBlockAcceptance()
        {
            bool requestBlockAgain = false;
            ulong requestBlockNum = 0;
            bool sleep = false;

            var sw = new System.Diagnostics.Stopwatch();
            sw.Start();

            lock (localBlockLock)
            {
                if (localNewBlock == null) return;
                if (verifyBlock(localNewBlock) == BlockVerifyStatus.Valid)
                {
                    // TODO: we will need an edge case here in the event that too many nodes dropped and consensus
                    // can no longer be reached according to this number - I don't have a clean answer yet - MZ
                    if (localNewBlock.signatures.Count() >= Node.blockChain.getRequiredConsensus())
                    {
                        if(!verifySignatureFreezeChecksum(localNewBlock))
                        {
                            Logging.warn(String.Format("Signature freeze checksum verification failed on current localNewBlock #{0}, waiting for the correct target block.", localNewBlock.blockNum));
                            return;
                        }
                        if (localNewBlock.blockNum != Node.blockChain.getLastBlockNum() + 1)
                        {
                            Logging.warn(String.Format("Tried to apply an unexpected block #{0}, expected #{1}. Stack trace: {2}", localNewBlock.blockNum, Node.blockChain.getLastBlockNum() + 1, Environment.StackTrace));
                            // block has already been applied or ahead, waiting for new blocks
                            localNewBlock = null;
                            return;
                        }
                        // accept this block, apply its transactions, recalc consensus, etc
                        if (applyAcceptedBlock(localNewBlock) == true)
                        {
                            string wsChecksum = Node.walletState.calculateWalletStateChecksum();
                            if (wsChecksum != localNewBlock.walletStateChecksum)
                            {
                                Logging.error(String.Format("After applying block #{0}, walletStateChecksum is incorrect, rolling back transactions!. Block's WS: {1}, actualy WS: {2}", localNewBlock.blockNum, localNewBlock.walletStateChecksum, wsChecksum));
                                rollBackAcceptedBlock(localNewBlock);
                                if (Node.walletState.calculateWalletStateChecksum() != Node.blockChain.getBlock(Node.blockChain.getLastBlockNum()).walletStateChecksum)
                                {
                                    Logging.error(String.Format("Fatal error occured while rolling back accepted block #{0}!.", localNewBlock.blockNum));
                                    // TODO TODO TODO maybe do something else instead?
                                    operating = false;
                                    Node.stop();
                                    return;
                                }
                                localNewBlock.logBlockDetails();
                                requestBlockNum = localNewBlock.blockNum;
                                localNewBlock = null;
                                requestBlockAgain = true;
                            }
                            else
                            {
                                Node.blockChain.appendBlock(localNewBlock);
                                Logging.info(String.Format("Accepted block #{0}.", localNewBlock.blockNum));
                                lastBlockStartTime = DateTime.Now;
                                localNewBlock.logBlockDetails();
                                localNewBlock = null;

                                // Reset transaction limits
                                //TransactionPool.resetSocketTransactionLimits();

                                if (highestNetworkBlockNum > Node.blockChain.getLastBlockNum())
                                {
                                    ProtocolMessage.broadcastGetBlock(Node.blockChain.getLastBlockNum() + 1);
                                }else
                                {
                                    highestNetworkBlockNum = 0;
                                }

                            }
                        }
                        else if(Node.blockChain.getBlock(localNewBlock.blockNum) == null)
                        {
                            // TODO TODO TODO Partial rollback may be needed here
                            Logging.error(String.Format("Couldn't apply accepted block #{0}.", localNewBlock.blockNum));
                            localNewBlock.logBlockDetails();
                            requestBlockNum = localNewBlock.blockNum;
                            localNewBlock = null;
                            requestBlockAgain = true;
                        }else
                        {
                            localNewBlock = null;
                        }
                    }
                    else
                    {
                        if (Node.isWorkerNode() == false)
                        {
                            ProtocolMessage.broadcastNewBlock(localNewBlock);
                            Logging.info(String.Format("Local block #{0} hasn't reached consensus yet {1}/{2}, resending.", localNewBlock.blockNum, localNewBlock.signatures.Count, Node.blockChain.getRequiredConsensus()));
                            sleep = true;
                        }
                    }
                }else if (Node.blockChain.getBlock(localNewBlock.blockNum) == null)
                {
                    Logging.error(String.Format("We have an invalid block #{0} in verifyBlockAcceptance, requesting the block again.", localNewBlock.blockNum));
                    requestBlockNum = localNewBlock.blockNum;
                    localNewBlock = null;
                    requestBlockAgain = true;
                }else
                {
                    localNewBlock = null;
                }
            }

            sw.Stop();
            TimeSpan elapsed = sw.Elapsed;
            Logging.info(string.Format("VerifyBlockAcceptance took: {0}ms", elapsed.TotalMilliseconds));


            if (sleep)
            {
                Thread.Sleep(5000);
            }

            // Check if we should request the block again
            if (requestBlockAgain && requestBlockNum > 0)
            {
                // Show a notification
                Logging.error(string.Format("Requesting block {0} again due to previous mismatch.", requestBlockNum));
                // Sleep a bit to prevent spam
                Thread.Sleep(5000);
                // Request the block again
                ProtocolMessage.broadcastGetBlock(requestBlockNum);
            }
            return;

        }

        public bool verifySignatureFreezeChecksum(Block b)
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
        public bool applyAcceptedBlock(Block b, bool ws_snapshot = false)
        {
            if (Node.blockChain.getBlock(b.blockNum) != null)
            {
                Logging.warn(String.Format("Block #{0} has already been applied. Stack trace: {1}", b.blockNum, Environment.StackTrace));
                return false;
            }

            // Distribute staking rewards first
            distributeStakingRewards(b, ws_snapshot);

            // Apply transactions from block
            if(!TransactionPool.applyTransactionsFromBlock(b, ws_snapshot))
            {
                return false;
            }

            // Apply transaction fees
            applyTransactionFeeRewards(b, ws_snapshot);

            // Update wallet state public keys
            b.updateWalletStatePublicKeys(ws_snapshot);

            // Reset the internal nonce
            if (!ws_snapshot)
            {
                TransactionPool.internalNonce = 0;
            }

            return true;
        }

        public bool rollBackAcceptedBlock(Block b, bool ws_snapshot = false)
        {
            return false; // TODO TODO TODO partially implemented

            /*for(int i = 0; i < b.transactions.Count; i++)
            {
                Transaction t = TransactionPool.getTransaction(b.transactions[i]);
                if(t == null)
                {
                    return false;
                }

                if (t.applied == b.blockNum)
                {
                    TransactionPool.rollBackNormalTransaction(t);
                    TransactionPool.rollBackPoWTransaction(t);
                    TransactionPool.rollBackStakingTransaction(t);
                    TransactionPool.rollBackTransactionFeeReward(t);
                    t.applied = 0;
                } // else the tx was either not applied - could be failed, or applied to some previous block
            }
            return true; */
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
                if (b.blockNum > 1)
                {
                    sigfreezechecksum = Node.blockChain.getBlock(b.blockNum - 1).signatureFreezeChecksum;
                }
            }

            // Ignore blocks before #6
            if (b.blockNum < 6)
            {
                return;
            }

            if (sigfreezechecksum.Length < 3)
            {
                Logging.warn("Current block does not have sigfreeze checksum.");
                return;
            }

            // Obtain the 6th last block, aka target block
            Block targetBlock = null;

            targetBlock = Node.blockChain.getBlock(b.blockNum - 6);
            if (targetBlock == null)
                return;

            string targetSigFreezeChecksum = targetBlock.calculateSignatureChecksum();

            if (sigfreezechecksum.Equals(targetSigFreezeChecksum, StringComparison.Ordinal) == false)
            {
                Logging.warn(string.Format("Signature freeze mismatch for block {0}. Current block height: {1}", targetBlock.blockNum, b.blockNum));
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
            //Logging.info(string.Format("Awarded {0} IXI to foundation", foundationAward.ToString()));

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
                //Logging.info(string.Format("Awarded {0} IXI to foundation from fee division remainder", foundationAward.ToString()));
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

                //Logging.info(string.Format("Awarded {0} IXI to {1}", tAward.ToString(), addr.ToString()));
            }

            // Output stats for this block's fee distribution
            Logging.info(string.Format("Total block TX amount: {0} Total TXs: {1} Reward per Signer: {2} Foundation Reward: {3}", tAmount.ToString(), txcount, 
                tAward.ToString(), foundationAward.ToString()));
          
        }

        // Generate a new block
        public void generateNewBlock()
        {
            if (Node.isWorkerNode())
                return;

            lock (localBlockLock)
            {
                Logging.info("GENERATING NEW BLOCK");

                // Create a new block and add all the transactions in the pool
                localNewBlock = new Block();
                localNewBlock.timestamp = Node.getCurrentTimestamp().ToString();
                localNewBlock.blockNum = Node.blockChain.getLastBlockNum() + 1;

                Logging.info(String.Format("\t\t|- Block Number: {0}", localNewBlock.blockNum));

                // Apply signature freeze
                localNewBlock.signatureFreezeChecksum = getSignatureFreeze();

                ulong total_transactions = 0;
                IxiNumber total_amount = 0;

                // Apply staking transactions to block. 
                // Generate the staking transactions with the blockgen flag, as we are the current block generator
                List<Transaction> staking_transactions = generateStakingTransactions(localNewBlock.blockNum - 6);
                foreach (Transaction transaction in staking_transactions)
                {
                    localNewBlock.addTransaction(transaction);
                    total_amount += transaction.amount;
                    total_transactions++;
                }
                staking_transactions.Clear();

                List<Transaction> pool_transactions = TransactionPool.getUnappliedTransactions().ToList<Transaction>();
                pool_transactions.OrderBy(x => x.blockHeight); // TODO add fee

                ulong normal_transactions = 0; // Keep a counter of normal transactions for the limiter

                foreach (var transaction in pool_transactions)
                {
                    // Check if we reached the transaction limit for this block
                    if(normal_transactions >= Config.maximumTransactionsPerBlock)
                    {
                        // Limit all other transactions
                        break;
                    }

                    // Verify that the transaction is actually valid at this point
                    // no need as the tx is already in the pool and was verified when received
                    //if (TransactionPool.verifyTransaction(transaction) == false)
                    //    continue;

                    // Skip adding staking rewards
                    if (transaction.type == (int)Transaction.Type.StakingReward)
                    {
                        continue;
                    }                    

                    localNewBlock.addTransaction(transaction);
                    total_amount += transaction.amount;
                    total_transactions++;
                    normal_transactions++;
                }


                Logging.info(String.Format("\t\t|- Transactions: {0} \t\t Amount: {1}", total_transactions, total_amount));

                // Calculate mining difficulty
                localNewBlock.difficulty = calculateDifficulty();

                // Simulate applying a block to see what the walletstate would look like
                Node.walletState.snapshot();
                applyAcceptedBlock(localNewBlock, true);
                localNewBlock.setWalletStateChecksum(Node.walletState.calculateWalletStateChecksum(true));
                Node.walletState.revert();

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
            ulong local_blocknum = 0;
            ulong current_difficulty = 14;
            lock (localBlockLock)
            {
                if (localNewBlock != null)
                {
                    local_blocknum = localNewBlock.blockNum;
                } 
            }
            if (local_blocknum > 1)
            {
                Block previous_block = Node.blockChain.getBlock(Node.blockChain.getLastBlockNum());
                if (previous_block != null)
                    current_difficulty = previous_block.difficulty;

                // Increase or decrease the difficulty according to the number of solved blocks in the redacted window
                ulong solved_blocks = Node.blockChain.getSolvedBlocksCount();
                if (solved_blocks > Config.redactedWindowSize / 2)
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
        public List<Transaction> generateStakingTransactions(ulong targetBlockNum, bool ws_snapshot = false)
        {
            List<Transaction> transactions = new List<Transaction>();
            /// WARNING WARNING WARNING
            //return transactions;

            // Prevent distribution if we don't have 10 fully generated blocks yet
            if (Node.blockChain.getLastBlockNum() < 10)
            {
                return transactions;
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
            IxiNumber newIxis = totalIxis * inflationPA / new IxiNumber("100000000"); // approximation of 2*60*24*365*100
            //Console.ForegroundColor = ConsoleColor.Magenta;
            //Console.WriteLine("----STAKING REWARDS for #{0} TOTAL {1} IXIs----", targetBlock.blockNum, newIxis.ToString());
            // Retrieve the list of signature wallets
            List<string> signatureWallets = targetBlock.getSignaturesWalletAddresses();

            IxiNumber totalIxisStaked = new IxiNumber(0);
            string[] stakeWallets = new string[signatureWallets.Count];
            BigInteger[] stakes = new BigInteger[signatureWallets.Count];
            BigInteger[] awards = new BigInteger[signatureWallets.Count];
            BigInteger[] awardRemainders = new BigInteger[signatureWallets.Count];
            // First pass, go through each wallet to find its balance
            int stakers = 0;
            foreach (string wallet_addr in signatureWallets)
            {
                Wallet wallet = Node.walletState.getWallet(wallet_addr, ws_snapshot);
                if (wallet.balance.getAmount() > 0)
                {
                    totalIxisStaked += wallet.balance;
                    stakes[stakers] = wallet.balance.getAmount();
                    stakeWallets[stakers] = wallet_addr;
                    stakers += 1;
                }
            }

            if(totalIxisStaked.getAmount() <= 0)
            {
                Logging.warn(String.Format("No Ixis were staked or a logic error occured - total ixi staked returned: {0}", totalIxisStaked.getAmount()));
                return transactions;
            }

            // Second pass, determine awards by stake
            BigInteger totalAwarded = 0;
            for (int i = 0; i < stakers; i++)
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

            for (int i = 0; i < stakers; i++)
            {
                IxiNumber award = new IxiNumber(awards[i]);
                if (award > (long)0)
                {
                    string wallet_addr = stakeWallets[i];
                    //Console.WriteLine("----> Awarding {0} to {1}", award, wallet_addr);

                    Transaction tx = new Transaction();
                    tx.type = (int)Transaction.Type.StakingReward;
                    tx.to = wallet_addr;
                    tx.from = "IxianInfiniMine2342342342342342342342342342342342342342342342342db32";
                    tx.amount = award;

                    
                    tx.blockHeight = Node.blockChain.getLastBlockNum();
                    // Handle legacy staking transactions
                    if (tx.blockHeight < Legacy.up20181017)
                        tx.blockHeight = 0;

                    string data = string.Format("{0}", targetBlock.blockNum);
                    tx.data = data;

                    tx.timeStamp = Node.getCurrentTimestamp().ToString();
                    tx.id = tx.generateID(); // Staking-specific txid
                    tx.checksum = Transaction.calculateChecksum(tx);
                    tx.signature = "Stake";

                    transactions.Add(tx);

                }

            }
            //Console.WriteLine("------");
            //Console.ResetColor();


            return transactions;
        }


        // Distribute the staking rewards according to the 5th last block signatures
        public bool distributeStakingRewards(Block b, bool ws_snapshot = false)
        {
            // Prevent distribution if we don't have 10 fully generated blocks yet
            if (Node.blockChain.getLastBlockNum() < 10)
            {
                return false;
            }

            if (ws_snapshot == false)
            {
                List<Transaction> transactions = generateStakingTransactions(b.blockNum - 6, ws_snapshot);
                foreach (Transaction transaction in transactions)
                {
                    if (!TransactionPool.addTransaction(transaction, true))
                    {
                        Logging.warn("An error occured while trying to add staking transaction");
                    }
                }
            }
            
            return true;
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
