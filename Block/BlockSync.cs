using DLT.Meta;
using DLT.Network;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DLT
{
    public class WsChunk
    {
        public ulong blockNum;
        public int chunkNum;
        public Wallet[] wallets;
    }

    class BlockSync
    {
        public bool synchronizing { get; private set; }
        List<Block> pendingBlocks = new List<Block>();
        readonly List<WsChunk> pendingWsChunks = new List<WsChunk>();
        ulong syncTargetBlockNum;
        int maxBlockRequests = 10;

        ulong wsSyncStartBlock;
        ulong wsConfirmedBlockNumber;
        string syncNeighbor;
        HashSet<int> missingWsChunks = new HashSet<int>();

        public BlockSync()
        {
            synchronizing = false;
        }

        public void onUpdate()
        {
            if (synchronizing == false) return;
            if (syncTargetBlockNum == 0)
            {
                // we haven't connected to any clients yet
                return;
            }
            Logging.info(String.Format("Sync running: {0} blocks, {1} walletstate chunks.",
                pendingBlocks.Count, pendingWsChunks.Count));

            requestMissingBlocks(); // TODO: this is a bad hack that just spams the network in the end

            if (wsSyncStartBlock > 0)
            {
                Logging.info(String.Format("We are synchronizing from block #{0}.", wsSyncStartBlock));
                requestWalletChunks();
                if (missingWsChunks.Count == 0)
                {
                    Logging.info("All WalletState chunks have been received. Applying");
                    lock (pendingWsChunks)
                    {
                        if (pendingWsChunks.Count > 0)
                        {
                            Node.walletState.clear();
                            foreach (WsChunk c in pendingWsChunks)
                            {
                                Logging.info(String.Format("Applying chunk {0}.", c.chunkNum));
                                Node.walletState.setWalletChunk(c.wallets);
                            }
                            pendingWsChunks.Clear();
                        }
                    }
                } else // misingWsChunks.Count > 0
                {
                    return;
                }
                Logging.info(String.Format("Verifying complete walletstate as of block #{0}", wsSyncStartBlock));
                lock (pendingBlocks)
                {
                    Block b = pendingBlocks.Find(x => x.blockNum == wsSyncStartBlock);
                    if (b == null)
                    {
                        Logging.warn(String.Format("Required block #{0} has not been received, sending requests.", wsSyncStartBlock));
                        ProtocolMessage.broadcastGetBlock(wsSyncStartBlock);
                        return;
                    }
                    string ws_checksum = Node.walletState.calculateWalletStateChecksum();
                    if (ws_checksum == b.walletStateChecksum)
                    {
                        Logging.info(String.Format("WalletState is correct at block #{0}", wsSyncStartBlock));
                        wsConfirmedBlockNumber = wsSyncStartBlock;
                        wsSyncStartBlock = 0;
                        ProtocolMessage.syncCompleteNeighbor(syncNeighbor);
                    } else
                    {
                        Console.ForegroundColor = ConsoleColor.Red;
                        Logging.warn(String.Format("Wallet state is not correct (block = {0}, WS = {1}).",
                            b.walletStateChecksum, ws_checksum));
                        Console.ResetColor();

                        //TODO : restart sync with another neighbor
                        syncTargetBlockNum = 0;
                    }
                }
            } else // wsSyncStartBlock == 0
            {
                Logging.info("WalletState is already synchronized. Skipping.");
            }
            // if we reach here, we can proceed with rolling forward the chain until we reach syncTargetBlockNum
            lock (pendingBlocks) {
                ulong lowestBlockNum = 1;
                if (Node.blockChain.redactedWindowSize < syncTargetBlockNum)
                {
                    lowestBlockNum = syncTargetBlockNum - Node.blockChain.redactedWindowSize + 1;
                }
                while (Node.blockChain.getLastBlockNum() < syncTargetBlockNum)
                {
                    ulong next_to_apply = Node.blockChain.getLastBlockNum() + 1;
                    if(next_to_apply < lowestBlockNum)
                    {
                        next_to_apply = lowestBlockNum;
                    }
                    Block b = pendingBlocks.Find(x => x.blockNum == next_to_apply);
                    if(b==null)
                    {
                        Logging.info(String.Format("Requesting missing block #{0}", next_to_apply));
                        ProtocolMessage.broadcastGetBlock(next_to_apply);
                        return;
                    }
                    Logging.info(String.Format("Applying pending block #{0}. Left to apply: {1}.",
                        b.blockNum, syncTargetBlockNum - Node.blockChain.getLastBlockNum()));

                    // wallet state is correct as of wsConfirmedBlockNumber, so before that we call
                    // verify with a parameter to ignore WS tests, but do all the others
                    BlockVerifyStatus b_status = BlockVerifyStatus.Invalid;
                    if (b.blockNum >= wsConfirmedBlockNumber)
                    {
                        b_status = Node.blockProcessor.verifyBlock(b);
                    }
                    else
                    {
                        // blocks earlier than wsConfirmedBlockNumber shouldn't check their transactions, since they are already included
                        // in the WS as of wsConfirmedBlockNumber
                        b_status = Node.blockProcessor.verifyBlock(b, true);
                    }

                    if (b_status == BlockVerifyStatus.Indeterminate)
                    {
                        Logging.info(String.Format("Waiting for missing transactions from block #{0}...", b.blockNum));
                        return;
                    }
                    if(b_status == BlockVerifyStatus.Invalid)
                    {
                        Logging.info(String.Format("Block #{0} is invalid. Discarding and requesting a new one.", b.blockNum));
                        pendingBlocks.RemoveAll(x => x.blockNum == b.blockNum);
                        ProtocolMessage.broadcastGetBlock(b.blockNum);
                        return;
                    }
                    if (b.blockNum > wsConfirmedBlockNumber)
                    {
                        // TODO: carefully verify this
                        // Apply transactions when rolling forward from a recover file without a synced WS
                        if (Config.recoverFromFile)
                        {
                            TransactionPool.applyTransactionsFromBlock(b);
                            // Apply transaction fees
                            Node.blockProcessor.applyTransactionFeeRewards(b);
                            // Apply staking rewards
                            Node.blockProcessor.distributeStakingRewards(b);
                        }
                    }
                   // else
                    {
         //               Node.blockProcessor.storeStakingRewards();
                    }
                    TransactionPool.setAppliedFlagToTransactionsFromBlock(b); // TODO TODO TODO this is a hack, do it properly
                    Node.blockChain.appendBlock(b);
                    // if last block doesn't have enough sigs, set as local block, get more sigs
                    if (Node.blockChain.getBlock(Node.blockChain.getLastBlockNum()).signatures.Count < Node.blockChain.getRequiredConsensus())
                    {
                        if (next_to_apply == syncTargetBlockNum) // if last block
                        {
                            Node.blockProcessor.onBlockReceived(b);
                        }
                        else
                        {
                            Logging.info(String.Format("Block #{0} has less than the required sigs. Discarding and requesting a new one.", b.blockNum));
                            pendingBlocks.RemoveAll(x => x.blockNum == b.blockNum);
                            ProtocolMessage.broadcastGetBlock(b.blockNum);
                            return;
                        }
                    }
                    pendingBlocks.RemoveAll(x => x.blockNum == b.blockNum);
                }
                // if we reach here, we are synchronized
                synchronizing = false;
                syncTargetBlockNum = 0;

                Node.blockProcessor.firstBlockAfterSync = true;
                Node.blockProcessor.resumeOperation();
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
                ulong firstBlock = Node.blockChain.redactedWindow > syncTargetBlockNum ? 1 : syncTargetBlockNum - Node.blockChain.redactedWindow + 1;
                ulong lastBlock = syncTargetBlockNum;
                List<ulong> missingBlocks = new List<ulong>(
                    Enumerable.Range(0, (int)(lastBlock - firstBlock + 1)).Select(x => (ulong)x + firstBlock));
                foreach (Block b in pendingBlocks)
                {
                    missingBlocks.RemoveAll(x => x == b.blockNum);
                }
                // whatever is left in missingBlocks is what we need to request
                Logging.info(String.Format("{0} blocks are missing before node is synchronized...", missingBlocks.Count()));
                int count = 0;
                foreach (ulong blockNum in missingBlocks)
                {
                    // First check if the missing block can be found in storage
                    Block block = Node.blockChain.getBlock(blockNum);
                    if (block != null)
                    {
                        Node.blockSync.onBlockReceived(block);
                        continue;
                    }

                    // Didn't find the block in storage, request it from the network
                    ProtocolMessage.broadcastGetBlock(blockNum);
                    count++;
                    if (count >= maxBlockRequests) break;
                }
            }
        }

        private void requestWalletChunks()
        {
            lock(missingWsChunks)
            {
                int count = 0;
                foreach(int c in missingWsChunks)
                {
                    ProtocolMessage.getWalletStateChunkNeighbor(syncNeighbor, c);
                    count += 1;
                    if (count > maxBlockRequests) break;
                }
                Logging.info(String.Format("{0} WalletState chunks are missing before WalletState is synchronized...", missingWsChunks.Count));
            }
        }

        public bool startOutgoingWSSync()
        {
            if(synchronizing)
            {
                Logging.info("Unable to outgoing sync until own sync is complete.");
                return false;
            }
            if(pendingWsChunks.Count > 0)
            {
                  Logging.info("Unable to outgoing sync, because another outgoing sync is still in progress.");
                  return false;
            }
            lock (pendingWsChunks)
            {
                pendingWsChunks.AddRange(Node.walletState.getWalletStateChunks(Config.walletStateChunkSplit));
            }
            Logging.info("Started outgoing WalletState Sync.");
            return true;
        }

        public void outgoingSyncComplete()
        {
            lock (pendingWsChunks)
            {
                pendingWsChunks.Clear();
            }
            Logging.info("Outgoing WalletState Sync finished.");
        }

        // passing endpoint through here is an ugly hack, which should be removed once network code is refactored.
        public void onRequestWalletChunk(int chunk_num, RemoteEndpoint endpoint)
        {
            if(synchronizing == true)
            {
                Logging.warn("Neighbor is requesting WalletState chunks, but we are synchronizing!");
                return;
            }
            if (chunk_num >= 0 && chunk_num < pendingWsChunks.Count)
            {
                ProtocolMessage.sendWalletStateChunk(endpoint, pendingWsChunks[chunk_num]);
            } else
            {
                Logging.warn(String.Format("Neighbor requested an invalid WalletState chunk: {0}, but the pending array only has 0-{1}.",
                    chunk_num, pendingWsChunks.Count));
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

        public void onBlockReceived(Block b)
        {
            if (synchronizing == false) return;
            if(b.blockNum >= syncTargetBlockNum)
            {
                if (b.signatures.Count < Node.blockChain.getRequiredConsensus())
                {
                    Logging.info(String.Format("Block is currently being calculated and does not meet consensus."));
                    return;
                }
            }
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
        
        public void startSync()
        {
            // clear out current state
            lock (pendingBlocks)
            {
                lock (pendingWsChunks)
                {
                    pendingBlocks.Clear();
                    pendingWsChunks.Clear();
                    Node.walletState.clear();
                }
            }
            synchronizing = true;
            // select sync partner for walletstate
            wsSyncStartBlock = 0;
            HashSet<string> all_neighbors = new HashSet<string>(NetworkClientManager.getConnectedClients().Concat(NetworkServer.getConnectedClients()));
            if (all_neighbors.Count < 1)
            {
                Logging.info(String.Format("Starting node synchronization from storage."));
                return;
            }
            Random r = new Random();
            syncNeighbor = all_neighbors.ElementAt(r.Next(all_neighbors.Count));
            Logging.info(String.Format("Starting node synchronization. Neighbor {0} chosen.", syncNeighbor));           
            ProtocolMessage.syncWalletStateNeighbor(syncNeighbor);
        }

        public void onWalletStateHeader(ulong ws_block, long ws_count)
        {
            if(synchronizing == true && wsSyncStartBlock == 0)
            {
                long chunks = ws_count / Config.walletStateChunkSplit;
                if(ws_count % Config.walletStateChunkSplit > 0)
                {
                    chunks += 1;
                }
                Logging.info(String.Format("Starting Wallet State synchronization. Starting block: #{0}. Wallets: {1} ({2} chunks)", 
                    ws_block, ws_count, chunks));
                wsSyncStartBlock = ws_block;
                lock (missingWsChunks)
                {
                    missingWsChunks.Clear();
                    for (int i = 0; i < chunks; i++)
                    {
                        missingWsChunks.Add(i);
                    }
                }
            }
        }

        public void onHelloDataReceived(ulong block_height, string block_checksum, string walletstate_checksum, int consensus)
        {
            Logging.info(string.Format("\t|- Block Height:\t\t#{0}", block_height));
            Logging.info(string.Format("\t|- Block Checksum:\t\t{0}", block_checksum));
            Logging.info(string.Format("\t|- WalletState checksum:\t{0}", walletstate_checksum));
            Logging.info(string.Format("\t|- Currently reported consensus:\t{0}", consensus));

            if (synchronizing)
            {
                if(block_height > syncTargetBlockNum)
                {
                    Logging.info(String.Format("Sync target increased from {0} to {1}.",
                        syncTargetBlockNum, block_height));
                    syncTargetBlockNum = block_height;
                }
            } else
            {
                if(Node.blockProcessor.operating == false)
                {
                    // This should happen when node first starts up.
                    // Node: some of the chain might be loaded from disk, so we might need to walk through that. This case is ignored for now.
                    Logging.info(String.Format("Network synchronization started. Target block height: #{0}.", block_height));

                    // TODO: this can get very slow depending on number of transactions.
                    // Find a better way to handle it
                    Storage.redactBlockStorage(block_height); // Redact block storage if needed

                    syncTargetBlockNum = block_height;
                    startSync();
                }
            }
        }
    }
}
