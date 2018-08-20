using DLT.Meta;
using DLT.Network;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DLT
{
    class BlockSync
    {
        public bool synchronizing { get; private set; }
        List<Block> pendingBlocks = new List<Block>();
        ulong syncTargetBlockNum;
        int maxBlockRequests = 10;


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
                            return;
                        }
                    }
                    else // idx == -1
                    {
                        requestMissingBlocks();
                        break;
                    }
                }
                if (syncTargetBlockNum > 0 && Node.blockChain.getLastBlockNum() == syncTargetBlockNum)
                {
                    // we cannot exit sync until wallet state is OK
                    if (Node.blockChain.getCurrentWalletState() == Node.walletState.calculateWalletStateChecksum())
                    {
                        Logging.info(String.Format("Synchronization state achieved at block #{0}.", syncTargetBlockNum));
                        synchronizing = false;
                        syncTargetBlockNum = 0;
                        Node.blockProcessor.resumeOperation();
                        lock (pendingBlocks)
                        {
                            pendingBlocks.Clear();
                        }
                    } // TODO: Possibly check if this has been going on too long and abort/restart
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

        public void onHelloDataReceived(ulong block_height, string block_checksum, string walletstate_checksum)
        {
            Console.WriteLine(string.Format("\t|- Block Height:\t\t#{0}", block_height));
            Console.WriteLine(string.Format("\t|- Block Checksum:\t\t{0}", block_checksum));
            Console.WriteLine(string.Format("\t|- WalletState checksum:\t{0}", walletstate_checksum));

            if(synchronizing)
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
                    // Node: some of the chain might be loaded from disk
                    Logging.info(String.Format("Network synchronization started. Target block height: #{0}.", block_height));
                    synchronizing = true;
                    syncTargetBlockNum = block_height;
                }
            }
        }
    }
}
