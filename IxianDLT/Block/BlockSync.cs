﻿// Copyright (C) 2017-2020 Ixian OU
// This file is part of Ixian DLT - www.github.com/ProjectIxian/Ixian-DLT
//
// Ixian DLT is free software: you can redistribute it and/or modify
// it under the terms of the MIT License as published
// by the Open Source Initiative.
//
// Ixian DLT is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// MIT License for more details.

using DLT.Meta;
using DLT.Network;
using IXICore;
using IXICore.ExternalWallets;
using IXICore.Meta;
using IXICore.Network;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace DLT
{
    class BlockSync
    {
        public bool synchronizing { get; private set; }
        List<Block> pendingBlocks = new List<Block>();
        List<ulong> missingBlocks = null;
        ulong lastMissingBlock = 0;

        SortedDictionary<ulong, List<Transaction>> pendingTransactions = new();

        public ulong pendingWsBlockNum { get; private set; }
        readonly List<WsChunk> pendingWsChunks = new List<WsChunk>();
        int wsSyncCount = 0;
        DateTime lastChunkRequested;
        Dictionary<ulong, long> requestedBlockTimes = new Dictionary<ulong, long>();

        public ulong lastBlockToReadFromStorage = 0;

        public ulong syncTargetBlockNum;
        int maxBlockRequests = 50; // Maximum number of block requests per iteration

        public ulong wsSyncConfirmedBlockNum = 0;
        bool wsSynced = false;
        string syncNeighbor;
        HashSet<int> missingWsChunks = new HashSet<int>();

        bool canPerformWalletstateSync = false;

        private Thread fetchThread = null;
        private Thread syncThread = null;

        private bool running = false;

        private bool noNetworkSynchronization = false; // Flag to determine if it ever started a network sync

        private bool syncDone = false;
        private ThreadLiveCheck TLC;

        private ulong lastProcessedBlockNum = 0;
        private long lastProcessedBlockTime = 0;

        private List<Block> lastBlocks = new List<Block>();

        public bool paused = false;
        public BlockSync()
        {
            synchronizing = false;

            running = true;
            TLC = new ThreadLiveCheck();
            // Start the thread
            syncThread = new Thread(rollForwardLoop);
            syncThread.Name = "Block_Sync_RollForward_Thread";
            syncThread.Start();

            fetchThread = new Thread(fetchLoop);
            fetchThread.Name = "Block_Sync_Fetch_Thread";
            fetchThread.Start();
        }

        private WebSocketClientManager webSocketClientManager;
        public void SetupWebSocketMessageHandling()
        {
            this.webSocketClientManager = WebSocketClientManager.Instance; // Store the reference
            WebSocketClientManager.Instance.OnMessageReceived += HandleWebSocketMessage;
        }

        private async void HandleWebSocketMessage(string message)
        {
            var parsedMessage = webSocketClientManager.ParseMessage(message);

            switch (parsedMessage.command)
            {
                case "HandleSync":
                    await WsHandleSyncAsync(parsedMessage.id, parsedMessage.type, parsedMessage.message);
                    break;

            }
        }

        private async Task WsHandleSyncAsync(string id, string type, string message)
        {
            try
            {
                if (type == "ping")
                {

                    var syncData = new
                    {
                        synchronizing = (bool)synchronizing,
                        syncDone = (bool)syncDone

                    };

                    var syncResult = new
                    {
                        id = (string)null,
                        result = syncData,
                        error = (string)null
                    };
                    WebSocketClientManager.ParsedMessage pingMessage = new WebSocketClientManager.ParsedMessage
                    {
                        command = "HandleSync",
                        type = "pong",
                        data = (object)syncResult,
                        message = (string)"",
                        id = id
                    };

                    await webSocketClientManager.SendMessageAsync(pingMessage);
                }

            }
            catch (Exception ex)
            {
                Logging.error($"Failed to handle or send message: {ex.Message}");
            }
        }


        public void rollForwardLoop()
        {
            while (running)
            {
                TLC.Report();
                if (paused || synchronizing == false || syncDone == true)
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
                    Thread.Sleep(1000);
                    continue;
                }
                if (CoreConfig.preventNetworkOperations || Config.storeFullHistory || Config.recoverFromFile || (wsSyncConfirmedBlockNum > 0 && wsSynced))
                {
                    // Proceed with rolling forward the chain
                    try
                    {
                        var lastBh = IxianHandler.getLastBlockHeight();
                        rollForward();
                        if (lastBh == IxianHandler.getLastBlockHeight())
                        {
                            Thread.Sleep(100);
                        }
                    }
                    catch (Exception e)
                    {
                        Logging.error("Exception caught in rollForwardLoop: " + e);
                        Thread.Sleep(100);
                    }
                    continue;
                }
                Thread.Sleep(100);
            }
        }

        public void fetchLoop()
        {
            while (running)
            {
                TLC.Report();
                if (paused || synchronizing == false || syncDone == true)
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
                if (CoreConfig.preventNetworkOperations || Config.storeFullHistory || Config.recoverFromFile || (wsSyncConfirmedBlockNum > 0 && wsSynced))
                {
                    // Request missing blocks if needed
                    if (syncDone)
                    {
                        Thread.Sleep(100);
                        continue;
                    }

                    requestMissingBlocks();
                    Thread.Sleep(100);
                    continue;
                }
                // Check if we can perform the walletstate synchronization
                if (canPerformWalletstateSync)
                {
                    performWalletStateSync();
                    Thread.Sleep(1000);
                    continue;
                }
                Thread.Sleep(100);
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
                    if (!running)
                    {
                        return false;
                    }
                    ulong blockNum = entry.Key;
                    // Check if the request expired (after 10 seconds)
                    if (currentTime - requestedBlockTimes[blockNum] > 10)
                    {
                        // Re-request block
                        if (BlockProtocolMessages.broadcastGetBlock(blockNum, null, null, 1, true) == false)
                        {
                            Logging.warn("Failed to rebroadcast getBlock request for {0}", blockNum);
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

            int total_count = 0;
            int requested_count = 0;

            List<ulong> tmpMissingBlocks;

            lock (pendingBlocks)
            {
                ulong lastBlock = syncToBlock;
                if (missingBlocks == null)
                {
                    lastMissingBlock = lastBlock;
                    missingBlocks = new List<ulong>(Enumerable.Range(0, (int)(lastBlock - firstBlock + 1)).Select(x => (ulong)x + firstBlock));
                    missingBlocks.Sort();
                }


                // whatever is left in missingBlocks is what we need to request
                if (missingBlocks.Count() == 0)
                {
                    return false;
                }

                if (pendingBlocks.Count() > maxBlockRequests * 2)
                {
                    return false;
                }

                tmpMissingBlocks = new List<ulong>(missingBlocks.Take(maxBlockRequests * 2));
            }

            foreach (ulong blockNum in tmpMissingBlocks)
            {
                if (!running)
                {
                    return false;
                }
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
                if (blockNum > last_block_height + (ulong)maxBlockRequests)
                {
                    if (last_block_height > 0 || (last_block_height == 0 && total_count > 10))
                    {
                        break;
                    }
                }

                // var sw = new System.Diagnostics.Stopwatch();
                // sw.Start();

                // First check if the missing block can be found in storage
                Block block = Node.blockChain.getBlock(blockNum, readFromStorage);

                //sw.Stop();
                //Logging.info(string.Format("Get block #{0} took {1}ms", blockNum, sw.Elapsed.TotalMilliseconds));

                if (block != null)
                {
                    if (CoreConfig.preventNetworkOperations || Config.recoverFromFile)
                    {
                        if (!lastBlocks.Exists(x => x.blockNum == blockNum))
                        {
                            lastBlocks.Add(new Block(block));
                            if (lastBlocks.Count > maxBlockRequests * 2)
                            {
                                lastBlocks.RemoveAt(0);
                            }
                        }
                    }
                    onBlockReceived(block, null);
                }
                else
                {
                    if (readFromStorage)
                    {
                        Logging.warn("Expecting block {0} in storage but had to request it from network.", blockNum);
                    }
                    // Didn't find the block in storage, request it from the network
                    if (BlockProtocolMessages.broadcastGetBlock(blockNum, null, null, 1, true) == false)
                    {
                        Logging.warn("Failed to broadcast getBlock request for {0}", blockNum);
                        Thread.Sleep(500);
                        break;
                    }
                    else
                    {
                        requested_count++;
                        // Set the block request time
                        lock (requestedBlockTimes)
                        {
                            if (!requestedBlockTimes.ContainsKey(blockNum))
                            {
                                requestedBlockTimes.Add(blockNum, currentTime);
                            }
                        }
                    }
                }
            }

            if (requested_count > 0)
                return true;

            return false;
        }

        private void performWalletStateSync()
        {
            Logging.info("WS SYNC block: {0}", wsSyncConfirmedBlockNum);
            if (wsSyncConfirmedBlockNum > 0)
            {
                Logging.info("We are synchronizing to block #{0}.", wsSyncConfirmedBlockNum);
                requestWalletChunks();
                if (missingWsChunks.Count == 0)
                {
                    Logging.info("All WalletState chunks have been received. Applying");
                    lock (pendingWsChunks)
                    {
                        if (pendingWsChunks.Count > 0)
                        {
                            Node.walletState.clear();
                            Node.regNameState.clear();
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

            if (Config.fullStorageDataVerification)
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
                if (lastProcessedBlockNum == blockNum)
                {
                    lastProcessedBlockTime = Clock.getTimestamp();
                }

                if (missingBlocks != null)
                {
                    if (!missingBlocks.Contains(blockNum))
                    {
                        Logging.info(String.Format("Requesting missing block #{0} again.", blockNum));
                        //missingBlocks.Add(blockNum);
                        //missingBlocks.Sort();

                        lock (requestedBlockTimes)
                        {
                            if (!requestedBlockTimes.ContainsKey(blockNum))
                            {
                                requestedBlockTimes.Add(blockNum, Clock.getTimestamp() - 10);
                            }
                        }
                        return true;
                    }
                }
            }
            return false;
        }

        private bool fastBlockLoading(Block b)
        {
            if (Config.disableFastBlockLoading)
            {
                return false;
            }

            if (b == null)
            {
                return false;
            }

            if (b.fromLocalStorage == false)
            {
                return false;
            }

            if (wsSyncConfirmedBlockNum < 50
                || lastBlockToReadFromStorage < 50)
            {
                return false;
            }

            if (b.blockNum >= wsSyncConfirmedBlockNum - 50
                || b.blockNum >= lastBlockToReadFromStorage - 50)
            {
                return false;
            }

            if (!Node.blockProcessor.verifySignatureFreezeChecksum(b, null, false))
            {
                return false;
            }

            IEnumerable<Transaction> txs = Node.storage.getTransactionsInBlock(b.blockNum, (int)Transaction.Type.PoWSolution);

            // TODO Add a check of how many transactions are in storage and if the count
            // doesn't equal to the block tx count, process as normal block
            foreach (var txid in b.transactions)
            {
                TransactionPool.addTxId(txid);
            }

            foreach (var tx in txs)
            {
                if (tx.type != (int)Transaction.Type.PoWSolution)
                {
                    Logging.error("Sync error, received a non-PoWSolution transaction from storage.");
                    continue;
                }
                if (b.transactions.Contains(tx.id))
                {
                    ulong blockNum = tx.powSolution.blockNum;
                    Block solvedBlock = Node.blockChain.getBlock(blockNum, false, true);
                    if (solvedBlock != null && solvedBlock.powField == null)
                    {
                        Node.blockChain.increaseSolvedBlocksCount();
                        solvedBlock.powField = BitConverter.GetBytes(b.blockNum);
                        Node.blockChain.updateBlock(solvedBlock);
                    }
                }
            }

            Node.blockChain.appendBlock(b, false);

            return true;
        }

        private void rollForward()
        {
            bool sleep = false;

            ulong lowestBlockNum = getLowestBlockNum();

            if (Node.blockChain.Count > 5)
            {
                lock (pendingBlocks)
                {
                    pendingBlocks.RemoveAll(x => x.blockNum < Node.blockChain.getLastBlockNum() - 5);
                }
            }

            // Loop until we have no more pending blocks
            do
            {
                ulong next_to_apply = lowestBlockNum;
                if (Node.blockChain.Count > 0)
                {
                    next_to_apply = Node.blockChain.getLastBlockNum() + 1;
                }

                if (next_to_apply >= syncTargetBlockNum)
                {
                    // we have everything, clear pending blocks and break
                    lock (pendingBlocks)
                    {
                        pendingBlocks.Clear();
                        lock (requestedBlockTimes)
                        {
                            requestedBlockTimes.Clear();
                        }
                    }
                    break;
                }

                processPendingTransactions();

                Block b;
                lock (pendingBlocks)
                {
                    b = pendingBlocks.Find(x => x.blockNum == next_to_apply);
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
                }
                b = new Block(b);
                if (Node.blockChain.Count == 0 && b.blockNum > 1)
                {
                    Block tmp_b = Node.blockChain.getBlock(b.blockNum - 1, true, true);
                    if (tmp_b != null)
                    {
                        Node.blockChain.setLastBlockVersion(tmp_b.version);
                    }
                    else
                    {
                        Node.blockChain.setLastBlockVersion(b.version);
                    }
                }

                if (b.version > Block.maxVersion)
                {
                    Logging.error("Received block {0} with a version higher than this node can handle, discarding the block.", b.blockNum);
                    lock (pendingBlocks)
                    {
                        pendingBlocks.RemoveAll(x => x.blockNum == b.blockNum);
                    }
                    Node.blockProcessor.networkUpgraded = true;
                    sleep = true;
                    break;
                }
                else
                {
                    Node.blockProcessor.networkUpgraded = false;
                }


                if (next_to_apply > 5)
                {
                    ulong targetBlock = next_to_apply - 5;
                    Block tb;
                    lock (pendingBlocks)
                    {
                        tb = pendingBlocks.Find(x => x.blockNum == targetBlock);
                    }
                    if (tb != null)
                    {
                        Block local_block = Node.blockChain.getBlock(tb.blockNum);
                        bool verify_sigs = true;
                        if (tb.blockNum <= wsSyncConfirmedBlockNum)
                        {
                            verify_sigs = false;
                        }

                        if (tb.lastSuperBlockChecksum != null)
                        {
                            tb.superBlockSegments = local_block.superBlockSegments;
                            tb.blockChecksum = tb.calculateChecksum();
                        }

                        if (local_block != null && tb.blockChecksum.SequenceEqual(local_block.blockChecksum) && Node.blockProcessor.verifyBlockBasic(tb, verify_sigs) == BlockVerifyStatus.Valid)
                        {
                            if (Node.blockProcessor.verifyBlockSignatures(tb, null))
                            {
                                Node.blockChain.refreshSignatures(tb, true);
                            }
                            else
                            {
                                Logging.warn("Target block " + tb.blockNum + " does not have the required consensus.");
                            }
                        }
                        lock (pendingBlocks)
                        {
                            pendingBlocks.RemoveAll(x => x.blockNum == tb.blockNum);
                        }
                    }
                }

                try
                {

                    Logging.info("Sync: Applying block #{0}/{1}.",
                        b.blockNum, syncTargetBlockNum);

                    b.powField = null;

                    if (fastBlockLoading(b))
                    {
                        lock (pendingBlocks)
                        {
                            pendingBlocks.RemoveAll(x => x.blockNum == b.blockNum);
                        }
                        continue;
                    }

                    bool ignoreWalletState = true;

                    if (b.blockNum > wsSyncConfirmedBlockNum || Config.fullStorageDataVerification)
                    {
                        ignoreWalletState = false;
                    }

                    // wallet state is correct as of wsConfirmedBlockNumber, so before that we call
                    // verify with a parameter to ignore WS tests, but do all the others
                    BlockVerifyStatus b_status = BlockVerifyStatus.Valid;

                    if (b.fromLocalStorage)
                    {
                        bool missing = false;

                        var sw = new System.Diagnostics.Stopwatch();
                        sw.Start();

                        IEnumerable<Transaction> txs = Node.storage.getTransactionsInBlock(b.blockNum);

                        int missed_txs = 0;
                        int found_txs = 0;

                        List<byte[]> txs_to_fetch = new List<byte[]>();
                        foreach (byte[] txid in b.transactions)
                        {
                            if (!running)
                            {
                                break;
                            }

                            Transaction t = TransactionPool.getUnappliedTransaction(txid);
                            if (t == null)
                            {
                                foreach (Transaction t2 in txs)
                                {
                                    if (txid.SequenceEqual(t2.id))
                                    {
                                        t = t2;
                                        found_txs++;
                                        break;
                                    }
                                }

                                if (t == null)
                                {
                                    //t = Node.storage.getTransaction(txid, b.blockNum);
                                    missed_txs++;
                                }

                                if (t != null)
                                {
                                    t.applied = 0;
                                    TransactionPool.addTransaction(t, true, null, Config.fullStorageDataVerification);
                                }
                                else
                                {
                                    txs_to_fetch.Add(txid);
                                    missing = true;
                                }
                            }
                        }

                        if (missing)
                        {
                            Logging.info("Requesting missing transactions for block {0}", b.blockNum);
                            TransactionProtocolMessages.broadcastGetTransactions(txs_to_fetch, -(long)b.blockNum, null);
                            Thread.Sleep(100);
                            break;
                        }

                        sw.Stop();
                        Logging.info("|- Local TX fetch took: {0}ms. Missed: {1}  Found: {2}", sw.Elapsed.TotalMilliseconds, missed_txs, found_txs);
                    }

                    if (b.blockNum > wsSyncConfirmedBlockNum || b.fromLocalStorage == false || Config.fullStorageDataVerification)
                    {
                        b_status = Node.blockProcessor.verifyBlock(b, ignoreWalletState);
                    }
                    else
                    {
                        b_status = Node.blockProcessor.verifyBlockBasic(b, false);
                    }

                    if (b_status == BlockVerifyStatus.Indeterminate)
                    {
                        if (Node.blockChain.getLastBlockNum() > Node.storage.getHighestBlockInStorage())
                        {
                            Logging.info("Sync: Waiting for storage to be ready to continue processing indeterminate block #{0}...", b.blockNum);
                            return;
                        }

                        if (lastProcessedBlockNum < b.blockNum)
                        {
                            lastProcessedBlockNum = b.blockNum;
                            lastProcessedBlockTime = Clock.getTimestamp();
                        }

                        if (Clock.getTimestamp() - lastProcessedBlockTime > ConsensusConfig.blockGenerationInterval * 2 || b.getFrozenSignatureCount() == 0)
                        {
                            Logging.info("Sync: Discarding indeterminate block #{0}, due to timeout or signature count == 0...", b.blockNum);
                            Node.blockProcessor.blacklistBlock(b);
                            lock (pendingBlocks)
                            {
                                pendingBlocks.RemoveAll(x => x.blockNum == b.blockNum);
                                requestBlockAgain(b.blockNum);
                            }
                        }
                        else
                        {
                            Logging.info("Sync: Waiting for missing transactions from block #{0}...", b.blockNum);
                        }

                        Thread.Sleep(100);
                        return;
                    }
                    if (b_status != BlockVerifyStatus.Valid)
                    {
                        if (Node.blockChain.getLastBlockNum() > Node.storage.getHighestBlockInStorage())
                        {
                            Logging.info("Sync: Waiting for storage to be ready to continue processing block #{0}...", b.blockNum);
                            return;
                        }

                        Logging.warn("Block #{0} {1} is invalid. Discarding and requesting a new one.", b.blockNum, Crypto.hashToString(b.blockChecksum));
                        Node.blockProcessor.blacklistBlock(b);
                        lock (pendingBlocks)
                        {
                            pendingBlocks.RemoveAll(x => x.blockNum == b.blockNum);
                            requestBlockAgain(b.blockNum);
                        }
                        if (b_status == BlockVerifyStatus.PotentiallyForkedBlock && b.blockNum + 7 > lastBlockToReadFromStorage)
                        {
                            Node.blockProcessor.handleForkedFlag();
                        }
                        return;
                    }

                    if (!b.fromLocalStorage && !Node.blockProcessor.verifyBlockSignatures(b, null) && Node.blockChain.Count > 16)
                    {
                        Logging.warn("Block #{0} {1} doesn't have the required consensus. Discarding and requesting a new one.", b.blockNum, Crypto.hashToString(b.blockChecksum));
                        lock (pendingBlocks)
                        {
                            pendingBlocks.RemoveAll(x => x.blockNum == b.blockNum);
                            requestBlockAgain(b.blockNum);
                        }
                        return;
                    }

                    bool sigFreezeCheck = Node.blockProcessor.verifySignatureFreezeChecksum(b, null);
                    // Apply transactions when rolling forward from a recover file without a synced WS
                    if (b.blockNum > wsSyncConfirmedBlockNum)
                    {
                        if (Node.blockChain.Count <= 5 || sigFreezeCheck)
                        {
                            Node.walletState.beginTransaction(b.blockNum, false);
                            Node.regNameState.beginTransaction(b.blockNum, false);
                            bool applied = false;
                            try
                            {
                                applied = Node.blockProcessor.applyAcceptedBlock(b);

                                if (applied)
                                {
                                    Node.walletState.commitTransaction(b.blockNum);
                                    Node.regNameState.commitTransaction(b.blockNum);
                                }
                            }
                            catch (Exception e)
                            {
                                Logging.error("Error occurred during block sync, while applying/commiting transactions: " + e);
                            }
                            if (!applied)
                            {
                                Logging.warn("Error applying Block #{0} {1}. Reverting, discarding and requesting a new one.", b.blockNum, Crypto.hashToString(b.blockChecksum));
                                Node.walletState.revertTransaction(b.blockNum);
                                Node.regNameState.revertTransaction(b.blockNum);
                                Node.blockChain.revertBlockTransactions(b);
                                Node.blockProcessor.blacklistBlock(b);
                                lock (pendingBlocks)
                                {
                                    pendingBlocks.RemoveAll(x => x.blockNum == b.blockNum);
                                    requestBlockAgain(b.blockNum);
                                }
                                return;
                            }
                            else
                            {
                                if (b.version >= BlockVer.v5 && b.lastSuperBlockChecksum == null)
                                {
                                    // skip WS checksum check
                                }
                                else
                                {
                                    if (b.lastSuperBlockChecksum != null)
                                    {
                                        byte[] wsChecksum = Node.walletState.calculateWalletStateChecksum();
                                        if (wsChecksum == null || !wsChecksum.SequenceEqual(b.walletStateChecksum))
                                        {
                                            Logging.error("After applying block #{0}, walletStateChecksum is incorrect. Block's WS: {1}, actual WS: {2}", b.blockNum, Crypto.hashToString(b.walletStateChecksum), Crypto.hashToString(wsChecksum));
                                            Node.walletState.revertTransaction(b.blockNum);
                                            Node.regNameState.revertTransaction(b.blockNum);
                                            Node.blockChain.revertBlockTransactions(b);
                                            Node.blockProcessor.blacklistBlock(b);
                                            lock (pendingBlocks)
                                            {
                                                pendingBlocks.RemoveAll(x => x.blockNum == b.blockNum);
                                                requestBlockAgain(b.blockNum);
                                            }
                                            return;
                                        }
                                    }
                                }

                                if (b.version >= BlockVer.v11)
                                {
                                    if (b.lastSuperBlockChecksum != null)
                                    {
                                        byte[] rnChecksum = Node.regNameState.calculateRegNameStateChecksum(b.blockNum);
                                        if (rnChecksum == null || !rnChecksum.SequenceEqual(b.regNameStateChecksum))
                                        {
                                            Logging.error("After applying block #{0}, regNameStateChecksum is incorrect. Block's RN: {1}, actual RN: {2}", b.blockNum, Crypto.hashToString(b.regNameStateChecksum), Crypto.hashToString(rnChecksum));
                                            Node.walletState.revertTransaction(b.blockNum);
                                            Node.regNameState.revertTransaction(b.blockNum);
                                            Node.blockChain.revertBlockTransactions(b);
                                            Node.blockProcessor.blacklistBlock(b);
                                            lock (pendingBlocks)
                                            {
                                                pendingBlocks.RemoveAll(x => x.blockNum == b.blockNum);
                                                requestBlockAgain(b.blockNum);
                                            }
                                            return;
                                        }
                                    }
                                }

                                if (b.blockNum % Config.saveWalletStateEveryBlock == 0)
                                {
                                    if (b.version >= BlockVer.v11)
                                    {
                                        Node.regNamesMemoryStorage.saveToDisk(b.blockNum);
                                    }
                                    WalletStateStorage.saveWalletState(b.blockNum);
                                }
                            }
                        }
                    }
                    else
                    {
                        if (syncTargetBlockNum == b.blockNum)
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
                                    Logging.warn("Block #{0} is last and has an invalid WSChecksum. Discarding and requesting a new one.", b.blockNum);
                                    Node.blockProcessor.blacklistBlock(b);
                                    lock (pendingBlocks)
                                    {
                                        pendingBlocks.RemoveAll(x => x.blockNum == b.blockNum);
                                        requestBlockAgain(b.blockNum);
                                    }
                                    return;
                                }
                            }

                            if (b.version >= BlockVer.v11)
                            {
                                if (b.lastSuperBlockChecksum != null)
                                {
                                    byte[] rnChecksum = Node.regNameState.calculateRegNameStateChecksum(b.blockNum);
                                    if (rnChecksum == null || !rnChecksum.SequenceEqual(b.regNameStateChecksum))
                                    {
                                        Logging.warn("Block #{0} is last and has an invalid RNChecksum. Discarding and requesting a new one.", b.blockNum);
                                        Node.blockProcessor.blacklistBlock(b);
                                        lock (pendingBlocks)
                                        {
                                            pendingBlocks.RemoveAll(x => x.blockNum == b.blockNum);
                                            requestBlockAgain(b.blockNum);
                                        }
                                        return;
                                    }
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
                                Node.blockChain.revertBlockTransactions(b);
                                Node.blockProcessor.blacklistBlock(b);
                                lock (pendingBlocks)
                                {
                                    pendingBlocks.RemoveAll(x => x.blockNum == b.blockNum);
                                    requestBlockAgain(b.blockNum);
                                }
                                return;
                            }
                        }

                        Node.blockChain.appendBlock(b, !b.fromLocalStorage);
                    }
                    else if (Node.blockChain.Count > 5 && !sigFreezeCheck)
                    {
                        if (CoreConfig.preventNetworkOperations || Config.recoverFromFile)
                        {
                            var last_block = lastBlocks.Find(x => x.blockNum == b.blockNum - 5);
                            if (last_block != null)
                            {
                                lock (pendingBlocks)
                                {
                                    pendingBlocks.Add(last_block);
                                }
                            }
                            return;
                        }
                        // invalid sigfreeze, waiting for the correct block
                        Logging.warn("Block #{0} {1} doesn't have the correct sigfreezed block. Discarding and requesting a new one.", b.blockNum, Crypto.hashToString(b.blockChecksum));
                        lock (pendingBlocks)
                        {
                            pendingBlocks.RemoveAll(x => x.blockNum == b.blockNum);
                            requestBlockAgain(b.blockNum);
                        }
                        return;
                    }
                }
                catch (Exception e)
                {
                    Logging.error("Exception occurred while syncing block #{0}: {1}", b.blockNum, e);
                }
                if (Config.enableChainReorgTest)
                {
                    if (!Node.blockProcessor.chainReorgTest(b.blockNum))
                    {
                        lock (pendingBlocks)
                        {
                            pendingBlocks.RemoveAll(x => x.blockNum + 6 < b.blockNum);
                        }
                    }
                }
                else
                {
                    lock (pendingBlocks)
                    {
                        pendingBlocks.RemoveAll(x => x.blockNum == b.blockNum);
                    }
                }
                Node.blockProcessor.cleanupBlockBlacklist();
            } while (pendingBlocks.Count > 0 && running);

            if (!sleep && Node.blockChain.getLastBlockNum() + 1 >= syncTargetBlockNum)
            {
                if (verifyLastBlock())
                {
                    sleep = false;
                }
                else
                {
                    sleep = true;
                }
            }

            if (sleep)
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
            WalletStateProtocolMessages.syncWalletStateNeighbor(syncNeighbor);
        }

        // Verify the last block we have
        private bool verifyLastBlock()
        {
            Block b = Node.blockChain.getBlock(Node.blockChain.getLastBlockNum());
            if (b == null)
            {
                throw new Exception("Cannot find block #" + Node.blockChain.getLastBlockNum());
            }

            if (b.version >= BlockVer.v5 && b.lastSuperBlockChecksum == null)
            {
                // skip WS checksum check
            }
            else
            {
                byte[] wsChecksum = Node.walletState.calculateWalletStateChecksum();
                if (!b.walletStateChecksum.SequenceEqual(wsChecksum))
                {
                    // TODO TODO TODO resync?
                    Logging.error("Wallet state synchronization failed, last block's WS checksum does not match actual WS Checksum, last block #{0}, wsSyncStartBlock: #{1}, block's WS: {2}, actual WS: {3}", Node.blockChain.getLastBlockNum(), wsSyncConfirmedBlockNum, Crypto.hashToString(b.walletStateChecksum), Crypto.hashToString(wsChecksum));
                    return false;
                }
            }

            if (b.version >= BlockVer.v11)
            {
                if (b.lastSuperBlockChecksum != null)
                {
                    byte[] rnChecksum = Node.regNameState.calculateRegNameStateChecksum(b.blockNum);
                    if (rnChecksum == null || !rnChecksum.SequenceEqual(b.regNameStateChecksum))
                    {
                        // TODO TODO TODO resync?
                        Logging.error("RegName state synchronization failed, last block's RN checksum does not match actual RN Checksum, last block #{0}, wsSyncStartBlock: #{1}, block's RN: {2}, actual RN: {3}", Node.blockChain.getLastBlockNum(), wsSyncConfirmedBlockNum, Crypto.hashToString(b.regNameStateChecksum), Crypto.hashToString(rnChecksum));
                        return false;
                    }
                }
            }

            stopSyncStartBlockProcessing();

            return true;
        }

        private async void stopSyncStartBlockProcessing()
        {

            if (CoreConfig.preventNetworkOperations)
            {
                Logging.info("Data verification successfully completed.");

                IxianHandler.forceShutdown = true;

                syncDone = true;
                synchronizing = false;

                lock (pendingTransactions)
                {
                    pendingTransactions.Clear();
                }

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

            var syncResult = new
            {
                id = (string)null,
                result = "active",
                error = (string)null
            };

            WebSocketClientManager.ParsedMessage syncMessage = new WebSocketClientManager.ParsedMessage
            {
                command = "HandleSync",
                type = "request",
                data = (object)syncResult,
                message = (string)"",
                id = (string)null
            };

            await webSocketClientManager.SendMessageAsync(syncMessage);

            lock (pendingTransactions)
            {
                pendingTransactions.Clear();
            }

            Node.blockProcessor.firstBlockAfterSync = true;
            Node.blockProcessor.resumeOperation();
            Node.signerPowMiner.start();

            lock (pendingBlocks)
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
                CoreProtocolMessage.broadcastProtocolMessageToSingleRandomNode(new char[] { 'M', 'H' }, ProtocolMessageCode.getUnappliedTransactions, new byte[1], IxianHandler.getHighestKnownNetworkBlockHeight());

                Node.miner.start();

                IxianHandler.getWalletStorage().scanForLostAddresses();
            }
        }

        // Request missing walletstate chunks from network
        private void requestWalletChunks()
        {
            lock (missingWsChunks)
            {
                int count = 0;
                foreach (int c in missingWsChunks)
                {
                    bool request_sent = WalletStateProtocolMessages.getWalletStateChunkNeighbor(syncNeighbor, c);
                    if (request_sent == false)
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
                    WalletStateProtocolMessages.sendWalletStateChunk(endpoint, pendingWsChunks[chunk_num]);
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
            if (synchronizing == false)
            {
                Logging.warn("Received WalletState chunk, but we are not synchronizing!");
                return;
            }
            lock (missingWsChunks)
            {
                if (missingWsChunks.Contains(chunk.chunkNum))
                {
                    pendingWsChunks.Add(chunk);
                    missingWsChunks.Remove(chunk.chunkNum);
                }
            }
        }

        public void onTransactionReceived(Transaction tx, RemoteEndpoint endpoint)
        {
            if (synchronizing == false) return;

            if (tx.blockHeight > Node.blockChain.getLastBlockNum() + (ulong)maxBlockRequests + 5)
            {
                return;
            }

            ulong lastBlockNum = Node.storage.getHighestBlockInStorage();
            //ulong lastBlockNum = Node.blockChain.getLastBlockNum();

            if (tx.blockHeight > lastBlockNum)
            {
                lock (pendingTransactions)
                {
                    int idx = -1;
                    ulong txBlockHeight = tx.blockHeight;
                    if (tx.type == (int)Transaction.Type.StakingReward)
                    {
                        txBlockHeight += 1;
                    }

                    if (pendingTransactions.ContainsKey(txBlockHeight))
                    {
                        idx = pendingTransactions[txBlockHeight].FindIndex(x => x.id.SequenceEqual(tx.id));
                    }
                    else
                    {
                        pendingTransactions.Add(txBlockHeight, new());
                    }

                    if (idx > -1)
                    {
                        // pendingTransactions[idx] = tx;
                    }
                    else
                    {
                        pendingTransactions[txBlockHeight].Add(tx);
                    }
                }
            }
            else
            {
                if (!TransactionPool.addTransaction(tx, true, endpoint))
                {
                    Logging.error("Couldn't add transaction " + tx.getTxIdString());
                }
            }
        }

        private void processPendingTransactions()
        {
            lock (pendingTransactions)
            {
                List<ulong> txSectionsToRemove = new();
                foreach (var txs in pendingTransactions)
                {
                    ulong lastBlockNum = Node.storage.getHighestBlockInStorage();
                    //ulong lastBlockNum = Node.blockChain.getLastBlockNum();
                    if (txs.Key > lastBlockNum + 1)
                    {
                        break;
                    }

                    foreach (var tx in txs.Value)
                    {
                        try
                        {
                            if (!TransactionPool.addTransaction(tx, true, null))
                            {
                                Logging.error("Couldn't add transaction in processPendingTransactions: " + tx.getTxIdString());
                                // Retry but only once
                                if (tx.blockHeight == txs.Key)
                                {
                                    if (pendingTransactions.ContainsKey(txs.Key + 1))
                                    {
                                        pendingTransactions[txs.Key + 1].Add(tx);
                                    }
                                }
                            }
                        }
                        catch (Exception e)
                        {
                            Logging.error("Error occurred in processPendingTransactions: " + e);
                        }
                    }
                    txSectionsToRemove.Add(txs.Key);
                }
                foreach (var txs in txSectionsToRemove)
                {
                    pendingTransactions.Remove(txs);
                }
            }
        }
        public void onBlockReceived(Block b, RemoteEndpoint endpoint)
        {
            if (synchronizing == false) return;

            if (Node.blockProcessor.isBlockBlacklisted(b, ConsensusConfig.blockGenerationInterval * 3))
            {
                return;
            }

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
                    // pendingBlocks[idx] = b;
                }
                else // idx <= -1
                {
                    pendingBlocks.Add(b);
                    Logging.info("Added block #{0} from storage: {1}", b.blockNum, b.fromLocalStorage);
                }
            }
        }

        public async void startSync()
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

            var syncResult = new
            {
                id = (string)null,
                result = "synchronizing",
                error = (string)null
            };

            WebSocketClientManager.ParsedMessage syncMessage = new WebSocketClientManager.ParsedMessage
            {
                command = "HandleSync",
                type = "request",
                data = (object)syncResult,
                message = (string)"",
                id = (string)null
            };

            await webSocketClientManager.SendMessageAsync(syncMessage);
        }

        public void onWalletStateHeader(ulong ws_block, long ws_count)
        {
            if (synchronizing == true && wsSyncConfirmedBlockNum == 0)
            {
                // If we reach this point, it means it started synchronization from network
                noNetworkSynchronization = false;

                long chunks = ws_count / CoreConfig.walletStateChunkSplit;
                if (ws_count % CoreConfig.walletStateChunkSplit > 0)
                {
                    chunks += 1;
                }
                Logging.info(String.Format("WalletState Starting block: #{0}. Wallets: {1} ({2} chunks)",
                    ws_block, ws_count, chunks));
                wsSyncConfirmedBlockNum = ws_block;
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

        public void onHelloDataReceived(ulong block_height, byte[] block_checksum, int block_version, byte[] walletstate_checksum, byte[] regnamestate_checksum, int consensus, ulong last_block_to_read_from_storage = 0, bool from_network = false)
        {
            Logging.info("SYNC HEADER DATA");
            Logging.info("\t|- Block Height:\t\t#{0}", block_height);
            Logging.info("\t|- Block Checksum:\t\t{0}", Crypto.hashToString(block_checksum));

            if (block_height < 2)
            {
                block_height = 2;
            }

            if (synchronizing)
            {
                Node.blockProcessor.highestNetworkBlockNum = Node.blockProcessor.determineHighestNetworkBlockNum();
                if (Node.blockProcessor.highestNetworkBlockNum > 0)
                {
                    block_height = Node.blockProcessor.highestNetworkBlockNum;
                }
                if (block_height > syncTargetBlockNum)
                {
                    Logging.info("Sync target increased from {0} to {1}.",
                        syncTargetBlockNum, block_height);


                    // Start a wallet state synchronization if no network sync was done before
                    if (noNetworkSynchronization && !Config.storeFullHistory && !Config.recoverFromFile && wsSyncConfirmedBlockNum == 0)
                    {
                        startWalletStateSync();
                    }
                    noNetworkSynchronization = false;

                    lock (pendingBlocks)
                    {
                        if (missingBlocks != null
                            && block_height > lastMissingBlock)
                        {
                            for (ulong i = 1; lastMissingBlock + i <= block_height; i++)
                            {
                                missingBlocks.Add(lastMissingBlock + i);
                            }
                            lastMissingBlock = block_height;
                            missingBlocks.Sort();
                        }
                        determineSyncTargetBlockNum();
                    }
                }
                else
                {
                    noNetworkSynchronization = false;
                    determineSyncTargetBlockNum();
                }

            }
            else
            {
                if (Node.blockProcessor.operating == false && syncDone == false)
                {
                    if (last_block_to_read_from_storage > 0)
                    {
                        lastBlockToReadFromStorage = last_block_to_read_from_storage;
                    }
                    // This should happen when node first starts up.
                    Logging.info("Network synchronization started. Target block height: #{0}.", block_height);

                    if (CoreConfig.preventNetworkOperations)
                    {
                        Node.blockProcessor.highestNetworkBlockNum = last_block_to_read_from_storage;
                    }
                    else if (lastBlockToReadFromStorage > block_height)
                    {
                        Node.blockProcessor.highestNetworkBlockNum = lastBlockToReadFromStorage;
                    }
                    else
                    {
                        Node.blockProcessor.highestNetworkBlockNum = Node.blockProcessor.determineHighestNetworkBlockNum();
                    }
                    determineSyncTargetBlockNum();
                    if (Config.fullStorageDataVerification)
                    {
                        Node.walletState.clear();
                        Node.regNameState.clear();
                        wsSynced = true;
                    }
                    else if (Config.storeFullHistory)
                    {
                        if (!from_network)
                        {
                            Block b = Node.blockChain.getBlock(block_height, true, true);
                            if (b != null)
                            {
                                Node.walletState.setCachedBlockVersion(block_version);
                                Node.regNameState.setCachedBlockVersion(block_version);
                                if ((b.version >= BlockVer.v5 && b.lastSuperBlockChecksum == null)
                                    || (Node.walletState.calculateWalletStateChecksum().SequenceEqual(walletstate_checksum)
                                        && (b.version < BlockVer.v11 || Node.regNameState.calculateRegNameStateChecksum(b.blockNum).SequenceEqual(regnamestate_checksum))))
                                {
                                    wsSyncConfirmedBlockNum = block_height;
                                    wsSynced = true;
                                }
                                else
                                {
                                    // TODO TODO TODO: this should be handled so that it reads previous WS from storage and so on until things match; for now this will do
                                    // Seperate handling for non full history nodes
                                    Logging.error("onHelloDataReceived: WS Checksum doesn't match the block.");
                                    Node.walletState.clear();
                                    Node.regNameState.clear();
                                }
                            }
                        }
                    }
                    startSync();

                    if (Config.recoverFromFile || Config.noNetworkSync || CoreConfig.preventNetworkOperations)
                    {
                        noNetworkSynchronization = false;
                    }
                    else
                    {
                        noNetworkSynchronization = true;
                    }
                }
                else
                {
                    Node.blockProcessor.highestNetworkBlockNum = Node.blockProcessor.determineHighestNetworkBlockNum();
                }
            }

            if (from_network)
            {
                noNetworkSynchronization = false;
            }
        }

        public void determineSyncTargetBlockNum()
        {
            if (Config.forceSyncToBlock > 0)
            {
                syncTargetBlockNum = Config.forceSyncToBlock + 1;
            }
            else
            {
                if (Node.blockProcessor.highestNetworkBlockNum > syncTargetBlockNum)
                {
                    syncTargetBlockNum = Node.blockProcessor.highestNetworkBlockNum;
                }
            }
        }
    }
}
