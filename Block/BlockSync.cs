using DLT.Meta;
using DLT.Network;
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
        public long chunkNum;
        public Wallet[] wallets;
    }

    class BlockSync
    {
        public bool synchronizing { get; private set; }
        List<Block> pendingBlocks = new List<Block>();
        List<WsChunk> pendingWsChunks = new List<WsChunk>();
        ulong syncTargetBlockNum;
        int maxBlockRequests = 10;

        ulong wsSyncStartBlock;
        long wsTargetNum;
        string syncNeighbor;
        HashSet<long> missingWsChunks = new HashSet<long>();


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
            requestMissingBlocks();
            requestWalletChunks();
            // TODO:
            // Apply wallet chunks in order, until all are applied
            // before applying wallet chunk, reverse it using 'pending blocks' to the state as of wsSyncStartBlock
            // should end up with the entire walletstate as of wsSyncStartBlock (checksum)
            // once that is done, apply pending blocks in order 
            //  -this should work, because these pending blocks and their transactions were required for block reversal
            //  -the only possible edge case here is wsSyncStartBlock (if TXs are missing from that, request them and sleep)
            // if we hit the syncTargetBlockNum and all checksums check out, we are synced
            //  -in this case, toggle "synchronizing" off and tell BlockProcessor to start normal operations
            //  -there is a potential edge case here: if a new block is accepted by the network *after* we turn sync off and *before* we turn processing on
            //    - this should be fine, because the accepted block will be re-broadcast a few times (to add late signatures) and BlockProcessor 
            //      should catch it before the new block is started
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

        private void requestWalletChunks()
        {
            lock(missingWsChunks)
            {
                int count = 0;
                foreach(long c in missingWsChunks)
                {
                    ProtocolMessage.getWalletStateChunkNeighbor(syncNeighbor, c);
                    count += 1;
                    if (count > maxBlockRequests) break;
                }
            }
        }

        // passing endpoint through here is an ugly hack, which should be removed once network code is refactored.
        public void onRequestWalletChunk(long chunk_num, RemoteEndpoint endpoint)
        {
            if(synchronizing == true)
            {
                Logging.warn("Neighbor is requesting WalletState chunks, but we are synchronizing!");
                return;
            }
            Wallet[] wallets = Node.walletState.getWalletStateChunk(chunk_num, Config.walletStateChunkSplit);
            WsChunk chunk = new WsChunk
            {
                chunkNum = chunk_num,
                blockNum = Node.blockChain.getLastBlockNum(),
                wallets = wallets
            };
            ProtocolMessage.sendWalletStateChunk(endpoint, chunk);
        }

        public void onWalletChunkReceived(WsChunk chunk)
        {
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
            if (b.signatures.Count() < Node.blockProcessor.currentConsensus)
            {
                // ignore blocks which haven't been accepted while we're syncing.
                return;
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
                pendingBlocks.Clear();
                Node.walletState.clear();
            }
            synchronizing = true;
            // select sync partner for walletstate
            HashSet<string> all_neighbors = new HashSet<string>(NetworkClientManager.getConnectedClients().Concat(NetworkServer.getConnectedClients()));
            Random r = new Random();
            syncNeighbor = all_neighbors.ElementAt(r.Next(all_neighbors.Count));
            wsSyncStartBlock = 0;
            ProtocolMessage.syncWalletStateNeighbor(syncNeighbor);
        }

        public void onWalletStateHeader(ulong ws_block, long ws_count)
        {
            if(synchronizing == true && wsSyncStartBlock == 0)
            {
                long chunks = ws_count / Config.walletStateChunkSplit;
                Logging.info(String.Format("Starting Wallet State synchronization. Starting block: #{0}. Wallets: {1} ({2} chunks)", 
                    ws_block, ws_count, chunks));
                wsSyncStartBlock = ws_block;
                wsTargetNum = ws_count;
                lock (missingWsChunks)
                {
                    missingWsChunks.Clear();
                    for (long i = 0; i < chunks; i++)
                    {
                        missingWsChunks.Add(i);
                    }
                }
            }
        }

        public void onHelloDataReceived(ulong block_height, string block_checksum, string walletstate_checksum, int consensus)
        {
            Console.WriteLine(string.Format("\t|- Block Height:\t\t#{0}", block_height));
            Console.WriteLine(string.Format("\t|- Block Checksum:\t\t{0}", block_checksum));
            Console.WriteLine(string.Format("\t|- WalletState checksum:\t{0}", walletstate_checksum));
            Console.WriteLine(string.Format("\t|- Current Consensus:\t{0}", consensus));

            if (synchronizing)
            {
                if(block_height > syncTargetBlockNum)
                {
                    Logging.info(String.Format("Sync target increased from {0} to {1}.",
                        syncTargetBlockNum, block_height));
                    syncTargetBlockNum = block_height;
                    Logging.info(String.Format("Consensus changed from {0} to {1}.",
                        Node.blockProcessor.currentConsensus, consensus));
                    Node.blockProcessor.setSyncConsensus(consensus);
                }
            } else
            {
                if(Node.blockProcessor.operating == false)
                {
                    // This should happen when node first starts up.
                    // Node: some of the chain might be loaded from disk, so we might need to walk through that. This case is ignored for now.
                    Logging.info(String.Format("Network synchronization started. Target block height: #{0}.", block_height));
                    syncTargetBlockNum = block_height;
                    Node.blockProcessor.setSyncConsensus(consensus);
                    startSync();
                }
            }
        }
    }
}
