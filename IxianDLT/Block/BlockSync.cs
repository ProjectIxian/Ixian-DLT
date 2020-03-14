using DLT.Meta;
using DLT.Network;
using IXICore;
using IXICore.Meta;
using IXICore.Network;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace DLT
{
    class BlockSync
    {
        public bool synchronizing { get; private set; }
        List<Block> pendingBlocks = new List<Block>();
        List<ulong> missingBlocks = null;
        
        public ulong pendingWsBlockNum { get; private set; }
        readonly List<WsChunk> pendingWsChunks = new List<WsChunk>();
        int wsSyncCount = 0;
        DateTime lastChunkRequested;
        Dictionary<ulong, long> requestedBlockTimes = new Dictionary<ulong, long>();

        public ulong lastBlockToReadFromStorage = 0;

        ulong syncTargetBlockNum;
        int maxBlockRequests = 50; // Maximum number of block requests per iteration
        bool receivedAllMissingBlocks = false;

        public ulong wsSyncConfirmedBlockNum = 0;
        int wsSyncConfirmedVersion;
        bool wsSynced = false;
        string syncNeighbor;
        HashSet<int> missingWsChunks = new HashSet<int>();

        bool canPerformWalletstateSync = false;

        private Thread sync_thread = null;

        private bool running = false;

        private ulong watchDogBlockNum = 0;
        private DateTime watchDogTime = DateTime.UtcNow;

        private bool noNetworkSynchronization = false; // Flag to determine if it ever started a network sync

        private bool syncDone = false;
        private ThreadLiveCheck TLC;

        public BlockSync()
        {
            synchronizing = false;
            receivedAllMissingBlocks = false;

            running = true;
            TLC = new ThreadLiveCheck();
            // Start the thread
            sync_thread = new Thread(onUpdate);
            sync_thread.Name = "Block_Sync_Update_Thread";
            sync_thread.Start();
        }

        public void onUpdate()
        {
            
            while (running)
            {
                TLC.Report();
                if (synchronizing == false || syncDone == true)
                {
                    Thread.Sleep(1000);
                    continue;
                }
                if (syncTargetBlockNum == 0)
                {
                    // we haven't connected to any clients yet
                    Thread.Sleep(1000);
                    continue;
                }

            //    Logging.info(String.Format("BlockSync: {0} blocks received, {1} walletstate chunks pending.",
              //      pendingBlocks.Count, pendingWsChunks.Count));
                if (!CoreConfig.preventNetworkOperations && !Config.storeFullHistory && !Config.recoverFromFile && wsSyncConfirmedBlockNum == 0)
                {
                    startWalletStateSync();
                    Thread.Sleep(1000);
                    continue;
                }
                if(CoreConfig.preventNetworkOperations || Config.storeFullHistory || Config.recoverFromFile || (wsSyncConfirmedBlockNum > 0 && wsSynced))
                {
                    // Request missing blocks if needed
                    if (receivedAllMissingBlocks == false)
                    {
                        // Proceed with rolling forward the chain
                        rollForward();

                        if (syncDone)
                        {
                            continue;
                        }

                        if (requestMissingBlocks())
                        {
                            // If blocks were requested, wait for next iteration
                            Thread.Sleep(100);
                            continue;
                        }
                    }
                }
                // Check if we can perform the walletstate synchronization
                if (canPerformWalletstateSync)
                {
                    performWalletStateSync();
                    Thread.Sleep(1000);
                }
                else
                {
                    // Proceed with rolling forward the chain
                    rollForward();
                }
                Thread.Yield();
            }
        }

        public void stop()
        {
            running = false;
            Logging.info("BlockSync stopped.");
        }


        private bool requestMissingBlocks()
        {
            if (syncDone)
            {
                return false;
            }

            if (syncTargetBlockNum == 0)
            {
                return false;
            }

            long currentTime = Clock.getTimestamp();

            // Check if the block has already been requested
            lock (requestedBlockTimes)
            {
                Dictionary<ulong, long> tmpRequestedBlockTimes = new Dictionary<ulong, long>(requestedBlockTimes);
                foreach (var entry in tmpRequestedBlockTimes)
                {
                    ulong blockNum = entry.Key;
                    // Check if the request expired (after 10 seconds)
                    if (currentTime - requestedBlockTimes[blockNum] > 10)
                    {
                        // Re-request block
                        if (ProtocolMessage.broadcastGetBlock(blockNum, null, null, 1, true) == false)
                        {
                            if (watchDogBlockNum > 0 && (blockNum == watchDogBlockNum - 4 || blockNum == watchDogBlockNum + 1))
                            {
                                watchDogTime = DateTime.UtcNow;
                            }
                            Logging.warn(string.Format("Failed to rebroadcast getBlock request for {0}", blockNum));
                            Thread.Sleep(500);
                        }
                        else
                        {
                            // Re-set the block request time
                            requestedBlockTimes[blockNum] = currentTime;
                        }
                    }
                }
            }


            ulong syncToBlock = syncTargetBlockNum;

            ulong firstBlock = getLowestBlockNum();


            lock (pendingBlocks)
            {
                ulong lastBlock = syncToBlock;
                if (missingBlocks == null)
                {
                    missingBlocks = new List<ulong>(Enumerable.Range(0, (int)(lastBlock - firstBlock + 1)).Select(x => (ulong)x + firstBlock));
                    missingBlocks.Sort();
                }

                int total_count = 0;
                int requested_count = 0;

                // whatever is left in missingBlocks is what we need to request
                if (missingBlocks.Count() == 0)
                {
                    receivedAllMissingBlocks = true;
                    return false;
                }

                List<ulong> tmpMissingBlocks = new List<ulong>(missingBlocks.Take(maxBlockRequests * 2));

                foreach (ulong blockNum in tmpMissingBlocks)
                {
                    total_count++;
                    lock (requestedBlockTimes)
                    {
                        if (requestedBlockTimes.ContainsKey(blockNum))
                        {
                            requested_count++;
                            continue;
                        }
                    }

                    bool readFromStorage = false;
                    if (blockNum <= lastBlockToReadFromStorage)
                    {
                        readFromStorage = true;
                    }

                    ulong last_block_height = IxianHandler.getLastBlockHeight();
                    if (blockNum > last_block_height  + (ulong)maxBlockRequests)
                    {
                        if (last_block_height > 0 || (last_block_height == 0 && total_count > 10))
                        {
                            break;
                        }
                    }

                    // First check if the missing block can be found in storage
                    Block block = Node.blockChain.getBlock(blockNum, readFromStorage);
                    if (block != null)
                    {
                        Node.blockSync.onBlockReceived(block, null);
                    }
                    else
                    {
                        if (readFromStorage)
                        {
                            Logging.warn("Expecting block {0} in storage but had to request it from network.", blockNum);
                        }
                        // Didn't find the block in storage, request it from the network
                        if (ProtocolMessage.broadcastGetBlock(blockNum, null, null, 1, true) == false)
                        {
                            if (watchDogBlockNum > 0 && (blockNum == watchDogBlockNum - 4 || blockNum == watchDogBlockNum + 1))
                            {
                                watchDogTime = DateTime.UtcNow;
                            }
                            Logging.warn(string.Format("Failed to broadcast getBlock request for {0}", blockNum));
                            Thread.Sleep(500);
                        }
                        else
                        {
                            requested_count++;
                            // Set the block request time
                            lock (requestedBlockTimes)
                            {
                                requestedBlockTimes.Add(blockNum, currentTime);
                            }
                        }
                    }
                }

                if (requested_count > 0)
                    return true;
            }

            return false;
        }

        private void performWalletStateSync()
        {
            Logging.info(String.Format("WS SYNC block: {0}", wsSyncConfirmedBlockNum));
            if (wsSyncConfirmedBlockNum > 0)
            {
                Logging.info(String.Format("We are synchronizing to block #{0}.", wsSyncConfirmedBlockNum));
                requestWalletChunks();
                if (missingWsChunks.Count == 0)
                {
                    Logging.info("All WalletState chunks have been received. Applying");
                    lock (pendingWsChunks)
                    {
                        if (pendingWsChunks.Count > 0)
                        {
                            Node.walletState.clear();
                            Node.walletState.version = wsSyncConfirmedVersion;
                            foreach (WsChunk c in pendingWsChunks)
                            {
                                Logging.info(String.Format("Applying chunk {0}.", c.chunkNum));
                                Node.walletState.setWalletChunk(c.wallets);
                            }
                            pendingWsChunks.Clear();
                            wsSynced = true;
                        }
                    }
                }
                else // misingWsChunks.Count > 0
                {
                    return;
                }
                Logging.info(String.Format("Verifying complete walletstate as of block #{0}", wsSyncConfirmedBlockNum));

                canPerformWalletstateSync = false;
            }
            else // wsSyncStartBlock == 0
            {
                Logging.info("WalletState is already synchronized. Skipping.");
            }
        }

        private ulong getLowestBlockNum()
        {
            ulong lowestBlockNum = 1;

            if(Config.fullStorageDataVerification)
            {
                return lowestBlockNum;
            }

            ulong syncToBlock = wsSyncConfirmedBlockNum;

            if (syncToBlock > ConsensusConfig.getRedactedWindowSize())
            {
                lowestBlockNum = syncToBlock - ConsensusConfig.getRedactedWindowSize();
            }
            return lowestBlockNum;
        }

        private bool requestBlockAgain(ulong blockNum)
        {
            lock (pendingBlocks)
            {
                if (missingBlocks != null)
                {
                    if (!missingBlocks.Contains(blockNum))
                    {
                        Logging.info(String.Format("Requesting missing block #{0} again.", blockNum));
                        missingBlocks.Add(blockNum);
                        missingBlocks.Sort();

                        requestedBlockTimes.Add(blockNum, Clock.getTimestamp() - 10);

                        receivedAllMissingBlocks = false;
                        return true;
                    }
                }
            }
            return false;
        }

        private void rollForward()
        {
            bool sleep = false;

            ulong lowestBlockNum = getLowestBlockNum();

            ulong syncToBlock = syncTargetBlockNum;

            if (Node.blockChain.Count > 5)
            {
                lock (pendingBlocks)
                {
                    pendingBlocks.RemoveAll(x => x.blockNum < Node.blockChain.getLastBlockNum() - 5);
                }
            }

            lock (pendingBlocks)
            {

                // Loop until we have no more pending blocks
                do
                {
                    handleWatchDog();

                    ulong next_to_apply = lowestBlockNum;
                    if (Node.blockChain.Count > 0)
                    {
                        next_to_apply = Node.blockChain.getLastBlockNum() + 1;
                    }

                    if (next_to_apply > syncToBlock)
                    {
                        // we have everything, clear pending blocks and break
                        pendingBlocks.Clear();
                        lock (requestedBlockTimes)
                        {
                            requestedBlockTimes.Clear();
                        }
                        break;
                    }
                    Block b = pendingBlocks.Find(x => x.blockNum == next_to_apply);
                    if (b == null)
                    {
                        lock (requestedBlockTimes)
                        {
                            if (requestBlockAgain(next_to_apply))
                            {
                                // the node isn't connected yet, wait a while
                                sleep = true;
                            }
                        }
                        break;
                    }
                    b = new Block(b);

                    if (b.version > Block.maxVersion)
                    {
                        Logging.error("Received block {0} with a version higher than this node can handle, discarding the block.", b.blockNum);
                        pendingBlocks.RemoveAll(x => x.blockNum == b.blockNum);
                        Node.blockProcessor.networkUpgraded = true;
                        sleep = true;
                        break;
                    }else
                    {
                        Node.blockProcessor.networkUpgraded = false;
                    }


                    if (next_to_apply > 5)
                    {
                        ulong targetBlock = next_to_apply - 5;

                        Block tb = pendingBlocks.Find(x => x.blockNum == targetBlock);
                        if (tb != null)
                        {
                            if (tb.blockChecksum.SequenceEqual(Node.blockChain.getBlock(tb.blockNum).blockChecksum) && Node.blockProcessor.verifyBlockBasic(tb) == BlockVerifyStatus.Valid)
                            {
                                if (Node.blockProcessor.verifyBlockSignatures(tb))
                                {
                                    Node.blockChain.refreshSignatures(tb, true);
                                }
                                else
                                {
                                    Logging.warn("Target block " + tb.blockNum + " does not have the required consensus.");
                                }
                            }
                            pendingBlocks.RemoveAll(x => x.blockNum == tb.blockNum);
                        }
                    }

                    try
                    {

                        Logging.info(String.Format("Sync: Applying block #{0}/{1}.",
                            b.blockNum, syncToBlock));

                        bool ignoreWalletState = true;

                        if (b.blockNum > wsSyncConfirmedBlockNum || Config.fullStorageDataVerification)
                        {
                            ignoreWalletState = false;
                        }

                        b.powField = null;

                        // wallet state is correct as of wsConfirmedBlockNumber, so before that we call
                        // verify with a parameter to ignore WS tests, but do all the others
                        BlockVerifyStatus b_status = BlockVerifyStatus.Valid;

                        if (b.fromLocalStorage)
                        {
                            bool missing = false;
                            foreach (string txid in b.transactions)
                            {
                                if(!running)
                                {
                                    break;
                                }

                                Transaction t = TransactionPool.getTransaction(txid, b.blockNum, true);
                                if (t != null)
                                {
                                    t.applied = 0;
                                    if(!TransactionPool.addTransaction(t, true, null, Config.fullStorageDataVerification))
                                    {
                                        Logging.error("Error adding a transaction {0} from storage", txid);
                                    }
                                }else
                                {
                                    CoreProtocolMessage.broadcastGetTransaction(txid, b.blockNum);
                                    missing = true;
                                }
                            }
                            if(missing)
                            {
                                sleep = true;
                                break;
                            }
                        }

                        if (b.blockNum > wsSyncConfirmedBlockNum || b.fromLocalStorage == false || Config.fullStorageDataVerification)
                        {
                            b_status = Node.blockProcessor.verifyBlock(b, ignoreWalletState);
                        }else
                        {
                            b_status = Node.blockProcessor.verifyBlockBasic(b, false);
                        }

                        if (b_status == BlockVerifyStatus.Indeterminate)
                        {
                            Logging.info(String.Format("Waiting for missing transactions from block #{0}...", b.blockNum));
                            Thread.Sleep(100);
                            return;
                        }
                        if (b_status != BlockVerifyStatus.Valid)
                        {
                            Logging.warn(String.Format("Block #{0} {1} is invalid. Discarding and requesting a new one.", b.blockNum, Crypto.hashToString(b.blockChecksum)));
                            pendingBlocks.RemoveAll(x => x.blockNum == b.blockNum);
                            requestBlockAgain(b.blockNum);
                            return;
                        }

                        if (!b.fromLocalStorage && !Node.blockProcessor.verifyBlockSignatures(b) && Node.blockChain.Count > 16)
                        {
                            Logging.warn(String.Format("Block #{0} {1} doesn't have the required consensus. Discarding and requesting a new one.", b.blockNum, Crypto.hashToString(b.blockChecksum)));
                            pendingBlocks.RemoveAll(x => x.blockNum == b.blockNum);
                            requestBlockAgain(b.blockNum);
                            return;
                        }

                        bool sigFreezeCheck = Node.blockProcessor.verifySignatureFreezeChecksum(b, null);

                        // Apply transactions when rolling forward from a recover file without a synced WS
                        if (b.blockNum > wsSyncConfirmedBlockNum)
                        {
                            if (Node.blockChain.Count <= 5 || sigFreezeCheck)
                            {
                                Node.blockProcessor.applyAcceptedBlock(b);

                                if (b.version >= BlockVer.v5 && b.lastSuperBlockChecksum == null)
                                {
                                    // skip WS checksum check
                                }
                                else
                                {
                                    if (b.lastSuperBlockChecksum != null || b.blockNum % Config.saveWalletStateEveryBlock == 0)
                                    {
                                        byte[] wsChecksum = Node.walletState.calculateWalletStateChecksum();
                                        if (wsChecksum == null || !wsChecksum.SequenceEqual(b.walletStateChecksum))
                                        {
                                            Logging.error(String.Format("After applying block #{0}, walletStateChecksum is incorrect!. Block's WS: {1}, actual WS: {2}", b.blockNum, Crypto.hashToString(b.walletStateChecksum), Crypto.hashToString(wsChecksum)));
                                            handleWatchDog(true);
                                            return;
                                        }
                                    }
                                }
                                if (b.blockNum % Config.saveWalletStateEveryBlock == 0)
                                {
                                    DLT.Meta.WalletStateStorage.saveWalletState(b.blockNum);
                                }
                            }
                        }
                        else
                        {
                            if (syncToBlock == b.blockNum)
                            {
                                if (b.version >= BlockVer.v5 && b.lastSuperBlockChecksum == null)
                                {
                                    // skip WS checksum check
                                }
                                else
                                {
                                    byte[] wsChecksum = Node.walletState.calculateWalletStateChecksum();
                                    if (wsChecksum == null || !wsChecksum.SequenceEqual(b.walletStateChecksum))
                                    {
                                        Logging.warn(String.Format("Block #{0} is last and has an invalid WSChecksum. Discarding and requesting a new one.", b.blockNum));
                                        pendingBlocks.RemoveAll(x => x.blockNum == b.blockNum);
                                        requestBlockAgain(b.blockNum);
                                        handleWatchDog(true);
                                        return;
                                    }
                                }
                            }
                        }

                        if (Node.blockChain.Count <= 5 || sigFreezeCheck)
                        {
                            //Logging.info(String.Format("Appending block #{0} to blockChain.", b.blockNum));
                            if (b.blockNum <= wsSyncConfirmedBlockNum)
                            {
                                if (!TransactionPool.setAppliedFlagToTransactionsFromBlock(b))
                                {
                                    pendingBlocks.RemoveAll(x => x.blockNum == b.blockNum);
                                    requestBlockAgain(b.blockNum);
                                    return;
                                }
                            }

                            if (b.blockNum > 12 && b.blockNum + 5 >= IxianHandler.getHighestKnownNetworkBlockHeight())
                            {
                                if (Node.isMasterNode())
                                {
                                    byte[][] signature_data = b.applySignature(); // applySignature() will return signature_data, if signature was applied and null, if signature was already present from before
                                    if (signature_data != null)
                                    {
                                        // ProtocolMessage.broadcastNewBlock(localNewBlock);
                                        ProtocolMessage.broadcastNewBlockSignature(b.blockNum, b.blockChecksum, signature_data[0], signature_data[1]);
                                    }
                                }
                            }

                            Node.blockChain.appendBlock(b, !b.fromLocalStorage);
                            resetWatchDog(b.blockNum);
                        }
                        else if (Node.blockChain.Count > 5 && !sigFreezeCheck)
                        {
                            // invalid sigfreeze, waiting for the correct block
                            pendingBlocks.RemoveAll(x => x.blockNum == b.blockNum);
                            return;
                        }
                    }
                    catch (Exception e)
                    {
                        Logging.error(String.Format("Exception occured while syncing block #{0}: {1}", b.blockNum, e));
                    }

                    pendingBlocks.RemoveAll(x => x.blockNum == b.blockNum);
                } while (pendingBlocks.Count > 0 && running);
            }

            if (!sleep && Node.blockChain.getLastBlockNum() >= syncToBlock)
            {
                if(verifyLastBlock())
                {
                    resetWatchDog(0);
                    sleep = false;
                }
                else
                {
                    handleWatchDog(true);
                    sleep = true;
                }
            }
            
            if(sleep)
            {
                Thread.Sleep(500);
            }
        }

        private void startWalletStateSync()
        {
            HashSet<string> all_neighbors = new HashSet<string>(NetworkClientManager.getConnectedClients(true).Concat(NetworkServer.getConnectedClients(true)));
            if (all_neighbors.Count < 1)
            {
                Logging.info(String.Format("Wallet state synchronization from storage."));
                return;
            }

            Random r = new Random();
            syncNeighbor = all_neighbors.ElementAt(r.Next(all_neighbors.Count));
            Logging.info(String.Format("Starting wallet state synchronization from {0}", syncNeighbor));       
            ProtocolMessage.syncWalletStateNeighbor(syncNeighbor);
        }

        // Verify the last block we have
        private bool verifyLastBlock()
        {
            Block b = Node.blockChain.getBlock(Node.blockChain.getLastBlockNum());
            if (b != null && b.version >= BlockVer.v5 && b.lastSuperBlockChecksum == null)
            {
                // skip WS checksum check
            }
            else
            {
                if (!b.walletStateChecksum.SequenceEqual(Node.walletState.calculateWalletStateChecksum()))
                {
                    // TODO TODO TODO resync?
                    Logging.error(String.Format("Wallet state synchronization failed, last block's WS checksum does not match actual WS Checksum, last block #{0}, wsSyncStartBlock: #{1}, block's WS: {2}, actual WS: {3}", Node.blockChain.getLastBlockNum(), wsSyncConfirmedBlockNum, Crypto.hashToString(b.walletStateChecksum), Crypto.hashToString(Node.walletState.calculateWalletStateChecksum())));
                    return false;
                }
            }

            resetWatchDog(0);

            stopSyncStartBlockProcessing();

            return true;
        }

        private void stopSyncStartBlockProcessing()
        {

            if(CoreConfig.preventNetworkOperations)
            {
                Logging.info("Data verification successfully completed.");
                IxianHandler.forceShutdown = true;
                return;
            }

            // Don't finish sync if we never synchronized from network
            if (noNetworkSynchronization == true)
            {
                Thread.Sleep(500);
                return;
            }

            IxianHandler.status = NodeStatus.ready;

            // if we reach here, we are synchronized
            syncDone = true;

            synchronizing = false;

            Node.blockProcessor.firstBlockAfterSync = true;
            Node.blockProcessor.resumeOperation();

            lock(pendingBlocks)
            {
                lock (requestedBlockTimes)
                {
                    requestedBlockTimes.Clear();
                }
                pendingBlocks.Clear();
                if (missingBlocks != null)
                {
                    missingBlocks.Clear();
                    missingBlocks = null;
                }
            }

            if (!Config.recoverFromFile)
            {
                CoreProtocolMessage.broadcastProtocolMessageToSingleRandomNode(new char[] { 'M' }, ProtocolMessageCode.getUnappliedTransactions, new byte[1], IxianHandler.getHighestKnownNetworkBlockHeight());

                Node.miner.start();

                Node.walletStorage.scanForLostAddresses();
            }

        }

        // Request missing walletstate chunks from network
        private void requestWalletChunks()
        {
            lock(missingWsChunks)
            {
                int count = 0;
                foreach(int c in missingWsChunks)
                {
                    bool request_sent = ProtocolMessage.getWalletStateChunkNeighbor(syncNeighbor, c);
                    if(request_sent == false)
                    {
                        Logging.warn(String.Format("Failed to request wallet chunk from {0}. Restarting WalletState synchronization.", syncNeighbor));
                        startWalletStateSync();
                        return;
                    }

                    count += 1;
                    if (count > maxBlockRequests) break;
                }
                if (count > 0)
                {
                    Logging.info(String.Format("{0} WalletState chunks are missing before WalletState is synchronized...", missingWsChunks.Count));
                }
                Thread.Sleep(2000);
            }
        }

        // Called when receiving a walletstate synchronization request
        public bool startOutgoingWSSync(RemoteEndpoint endpoint)
        {
            // TODO TODO TODO this function really should be done better

            if (synchronizing == true)
            {
                Logging.warn("Unable to perform outgoing walletstate sync until own blocksync is complete.");
                return false;
            }

            lock (pendingWsChunks)
            {
                if (wsSyncCount == 0 || (DateTime.UtcNow - lastChunkRequested).TotalSeconds > 150)
                {
                    wsSyncCount = 0;
                    pendingWsBlockNum = Node.blockChain.getLastBlockNum();
                    pendingWsChunks.Clear();
                    pendingWsChunks.AddRange(
                        Node.walletState.getWalletStateChunks(CoreConfig.walletStateChunkSplit, Node.blockChain.getLastBlockNum())
                        );
                }
                wsSyncCount++;
            }
            Logging.info("Started outgoing WalletState Sync.");
            return true;
        }

        public void outgoingSyncComplete()
        {
            // TODO TODO TODO this function really should be done better

            if (wsSyncCount > 0)
            {
                wsSyncCount--;
                if (wsSyncCount == 0)
                {
                    pendingWsChunks.Clear();
                }
            }
            Logging.info("Outgoing WalletState Sync finished.");
        }

        // passing endpoint through here is an ugly hack, which should be removed once network code is refactored.
        public void onRequestWalletChunk(int chunk_num, RemoteEndpoint endpoint)
        {
            // TODO TODO TODO this function really should be done better
            if (synchronizing == true)
            {
                Logging.warn("Neighbor is requesting WalletState chunks, but we are synchronizing!");
                return;
            }
            lastChunkRequested = DateTime.UtcNow;
            lock (pendingWsChunks)
            {
                if (chunk_num >= 0 && chunk_num < pendingWsChunks.Count)
                {
                    ProtocolMessage.sendWalletStateChunk(endpoint, pendingWsChunks[chunk_num]);
                    if (chunk_num + 1 == pendingWsChunks.Count)
                    {
                        outgoingSyncComplete();
                    }
                }
                else
                {
                    Logging.warn(String.Format("Neighbor requested an invalid WalletState chunk: {0}, but the pending array only has 0-{1}.",
                        chunk_num, pendingWsChunks.Count));
                }
            }
        }

        public void onWalletChunkReceived(WsChunk chunk)
        {
            if(synchronizing == false)
            {
                Logging.warn("Received WalletState chunk, but we are not synchronizing!");
                return;
            }
            lock(missingWsChunks)
            {
                if(missingWsChunks.Contains(chunk.chunkNum))
                {
                    pendingWsChunks.Add(chunk);
                    missingWsChunks.Remove(chunk.chunkNum);
                }
            }
        }

        public void onBlockReceived(Block b, RemoteEndpoint endpoint)
        {
            if (synchronizing == false) return;
            lock (pendingBlocks)
            {
                // Remove from requestedblocktimes, as the block has been received 
                lock (requestedBlockTimes)
                {
                    if (requestedBlockTimes.ContainsKey(b.blockNum))
                        requestedBlockTimes.Remove(b.blockNum);
                }

                if (missingBlocks != null)
                {
                    missingBlocks.Remove(b.blockNum);
                }

                if (b.blockNum > syncTargetBlockNum)
                {
                    return;
                }

                int idx = pendingBlocks.FindIndex(x => x.blockNum == b.blockNum);
                if (idx > -1)
                {
                    pendingBlocks[idx] = b;
                }
                else // idx <= -1
                {
                    pendingBlocks.Add(b);
                }
            }
        }
        
        public void startSync()
        {
            // clear out current state
            lock (requestedBlockTimes)
            {
                requestedBlockTimes.Clear();
            }

            lock (pendingBlocks)
            {
                pendingBlocks.Clear();
            }
            synchronizing = true;
            // select sync partner for walletstate
            receivedAllMissingBlocks = false;
        }

        public void onWalletStateHeader(int ws_version, ulong ws_block, long ws_count)
        {
            if(synchronizing == true && wsSyncConfirmedBlockNum == 0)
            {
                // If we reach this point, it means it started synchronization from network
                noNetworkSynchronization = false;

                long chunks = ws_count / CoreConfig.walletStateChunkSplit;
                if(ws_count % CoreConfig.walletStateChunkSplit > 0)
                {
                    chunks += 1;
                }
                Logging.info(String.Format("WalletState Starting block: #{0}. Wallets: {1} ({2} chunks)", 
                    ws_block, ws_count, chunks));
                wsSyncConfirmedBlockNum = ws_block;
                wsSyncConfirmedVersion = ws_version;
                lock (missingWsChunks)
                {
                    missingWsChunks.Clear();
                    for (int i = 0; i < chunks; i++)
                    {
                        missingWsChunks.Add(i);
                    }
                }

                // We can perform the walletstate sync now
                canPerformWalletstateSync = true;
            }
        }

        public void onHelloDataReceived(ulong block_height, byte[] block_checksum, int block_version, byte[] walletstate_checksum, int consensus, ulong last_block_to_read_from_storage = 0, bool from_network = false)
        {
            Logging.info("SYNC HEADER DATA");
            Logging.info(string.Format("\t|- Block Height:\t\t#{0}", block_height));
            Logging.info(string.Format("\t|- Block Checksum:\t\t{0}", Crypto.hashToString(block_checksum)));
            Logging.info(string.Format("\t|- WalletState checksum:\t{0}", Crypto.hashToString(walletstate_checksum)));
            Logging.info(string.Format("\t|- Currently reported consensus:\t{0}", consensus));

            if (synchronizing)
            {
                if (block_height > syncTargetBlockNum)
                {
                    Logging.info(String.Format("Sync target increased from {0} to {1}.",
                        syncTargetBlockNum, block_height));

                    Node.blockProcessor.highestNetworkBlockNum = block_height; // TODO TODO TODO TODO this has to be improved, to check the validity of the block height - it must have required signatures

                    // Start a wallet state synchronization if no network sync was done before
                    if (noNetworkSynchronization && !Config.storeFullHistory && !Config.recoverFromFile && wsSyncConfirmedBlockNum == 0)
                    {
                        startWalletStateSync();
                    }
                    noNetworkSynchronization = false;

                    ulong firstBlock = IxianHandler.getLastBlockHeight();

                    lock (pendingBlocks)
                    {
                        if (missingBlocks != null)
                        {
                            for (ulong i = 1; syncTargetBlockNum + i <= block_height; i++)
                            {
                                missingBlocks.Add(syncTargetBlockNum + i);
                            }
                            missingBlocks.Sort();
                        }
                        receivedAllMissingBlocks = false;
                        syncTargetBlockNum = block_height;
                    }

                }
            } else
            {
                if (Node.blockProcessor.operating == false && syncDone == false)
                {
                    if (last_block_to_read_from_storage > 0)
                    {
                        lastBlockToReadFromStorage = last_block_to_read_from_storage;
                    }
                    // This should happen when node first starts up.
                    Logging.info(String.Format("Network synchronization started. Target block height: #{0}.", block_height));

                    if (lastBlockToReadFromStorage > block_height)
                    {
                        Node.blockProcessor.highestNetworkBlockNum = lastBlockToReadFromStorage;
                        syncTargetBlockNum = lastBlockToReadFromStorage;
                    }
                    else
                    {
                        Node.blockProcessor.highestNetworkBlockNum = block_height;
                        syncTargetBlockNum = block_height;
                    }
                    if (Config.fullStorageDataVerification)
                    {
                        Node.walletState.clear();
                        wsSynced = true;
                    }
                    else if (Config.storeFullHistory)
                    {
                        Block b = Node.blockChain.getBlock(block_height, true, true);
                        if (b != null)
                        {
                            Node.walletState.setCachedBlockVersion(block_version);
                            if ((b.version >= BlockVer.v5 && b.lastSuperBlockChecksum == null) || Node.walletState.calculateWalletStateChecksum().SequenceEqual(walletstate_checksum))
                            {
                                wsSyncConfirmedBlockNum = block_height;
                                wsSynced = true;
                                wsSyncConfirmedVersion = Node.walletState.version;
                            }else
                            {
                                // TODO TODO TODO: this should be handled so that it reads previous WS from storage and so on until things match; for now this will do
                                // Seperate handling for non full history nodes
                                Logging.error("onHelloDataReceived: WS Checksum doesn't match the block.");
                                Node.walletState.clear();
                            }
                        }
                    }
                    startSync();

                    if (Config.recoverFromFile || Config.noNetworkSync || CoreConfig.preventNetworkOperations)
                    {
                        noNetworkSynchronization = false;
                    }else
                    {
                        noNetworkSynchronization = true;
                    }
                }
            }

            if (from_network)
            {
                noNetworkSynchronization = false;
            }
        }

        private void resetWatchDog(ulong blockNum)
        {
            watchDogBlockNum = blockNum;
            watchDogTime = DateTime.UtcNow;
        }

        private void handleWatchDog(bool forceWsUpdate = false)
        {
            if (syncDone)
            {
                return;
            }

            if (!forceWsUpdate && watchDogBlockNum == 0)
            {
                return;
            }

            if (forceWsUpdate || (DateTime.UtcNow - watchDogTime).TotalSeconds > 1200) // stuck on the same block for 1200 seconds
            {
                wsSyncConfirmedBlockNum = 0;
                if (Config.fullStorageDataVerification == false)
                {
                    ulong lastBlockHeight = IxianHandler.getLastBlockHeight();
                    if (lastBlockHeight > 100)
                    {
                        Logging.info("Restoring WS to " + (lastBlockHeight - 100));
                        wsSyncConfirmedBlockNum = WalletStateStorage.restoreWalletState(lastBlockHeight - 100);
                    }
                }

                if (wsSyncConfirmedBlockNum == 0)
                {
                    Logging.info("Resetting sync to begin from 0");
                    Node.walletState.clear();
                }
                else
                {
                    Block b = Node.blockChain.getBlock(wsSyncConfirmedBlockNum, true);
                    if (b != null)
                    {
                        if (b.version >= BlockVer.v5 && b.lastSuperBlockChecksum == null)
                        {
                            // skip WS checksum check
                        }else
                        {
                            int block_version = b.version;
                            Node.walletState.setCachedBlockVersion(block_version);
                            if (!Node.walletState.calculateWalletStateChecksum().SequenceEqual(b.walletStateChecksum))
                            {
                                b = null;
                            }
                        }
                    }
                    if(b == null)
                    {
                        Logging.error("BlockSync WatchDog: Wallet state mismatch");
                        return;
                    }
                }

                lastBlockToReadFromStorage = wsSyncConfirmedBlockNum;

                resetWatchDog(0);

                Node.blockChain.clear();

                TransactionPool.clear();

                lock (pendingBlocks)
                {
                    ulong firstBlock = getLowestBlockNum();
                    ulong lastBlock = lastBlockToReadFromStorage;
                    if(syncTargetBlockNum > lastBlock)
                    {
                        lastBlock = syncTargetBlockNum;
                    }
                    missingBlocks = new List<ulong>(Enumerable.Range(0, (int)(lastBlock - firstBlock + 1)).Select(x => (ulong)x + firstBlock));
                    missingBlocks.Sort();
                    pendingBlocks.Clear();
                    receivedAllMissingBlocks = false;
                    noNetworkSynchronization = true;
                }
                lock (requestedBlockTimes)
                {
                    requestedBlockTimes.Clear();
                }
            }
        }
    }
}
