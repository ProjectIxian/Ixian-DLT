using DLT.Meta;
using DLT.Network;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DLT
{
    class BlockChain
    {
        int redactedWindowSize = 3000; // approx 25 hours

        List<Block> blocks = new List<Block>();
        ulong currentBlockNum = 0;
        int minimumConsensusSignatures = 2;
        bool synchronizing = false;

        public bool isSynchronizing { get => synchronizing; }

        List<Block> pendingBlocks = new List<Block>();

        public BlockChain()
        {
        }

        public void onUpdate() {
            lock (blocks)
            {
                int begin_size = blocks.Count();
                while (blocks.Count() > redactedWindowSize)
                {
                    blocks.RemoveAt(0);
                }
                if (begin_size > blocks.Count())
                {
                    Console.WriteLine(String.Format("REDACTED {0} blocks to keep the chain length appropriate.", begin_size - blocks.Count()));
                }
            }
            if(synchronizing)
            {
                // attempt to move blocks from temporary storage to the blockchain proper
                chainPendingBlocks();
            }
        }

        private void chainPendingBlocks()
        {
            int pending_blocks_chained = 0;
            lock (blocks)
            {
                lock (pendingBlocks)
                {
                    while (pendingBlocks.Exists(b => b.blockNum == currentBlockNum + 1))
                    {
                        int idx = pendingBlocks.FindIndex(b => b.blockNum == currentBlockNum + 1);
                        Block chained_block = pendingBlocks[idx];
                        pendingBlocks.RemoveAt(idx);
                        // verify if the block logically follows - discard ones that don't
                        if (verifyBlock(ref chained_block) && chained_block.lastBlockChecksum == blocks.Last().blockChecksum)
                        {
                            blocks.Add(chained_block);
                            currentBlockNum = chained_block.blockNum;
                            pending_blocks_chained += 1;
                        }
                    }
                }
            }
            Logging.info(String.Format("Blockchain gap filled. {0} blocks added to chain. New head: {1}", pending_blocks_chained, currentBlockNum));
        }

        public bool appendBlockchain(Block block) {
            // verify that the block is internally consistent
            if(verifyBlock(ref block) == false)
            {
                Logging.warn(String.Format("New block #{0} has invalid checksums or signatures!", block.blockNum));
                return false;
            }
            if(blocks.Count() == 0)
            {
                // we ignore previous block checksum and signature counts for genesis block
                blocks.Add(block);
            } else
            {
                if(block.blockNum == currentBlockNum+1)
                {
                    lock(blocks)
                    {
                        blocks.Add(block);                        
                    }
                    // try to append more of the pending blocks
                    // primarily useful in synchronizing, but it will also help out if the node temporarily lags.
                    chainPendingBlocks();
                } else
                {
                    // new block is disjointed (does not follow the chain). We will ignore old blocks
                    if(block.blockNum > currentBlockNum)
                    {
                        // it is a future block
                        // we put it in the pending pile and request the block we actually need, so we ensure we get it.
                        if (!pendingBlocks.Exists(b => b.blockNum == block.blockNum))
                        {
                            lock (pendingBlocks)
                            {
                                pendingBlocks.Add(block);
                            }
                        }
                        ProtocolMessage.broadcastGetBlock(currentBlockNum + 1);
                    }
                }
            }
            // TODO: Ask Storage to persist the current chain
            return true;
        }

        bool verifyBlock(ref Block block) {
            if(block.calculateChecksum() == block.blockChecksum)
            {
                // TODO: Verify all signatures are valid
                return true;
            }
            return false;
        }

        // Insert an older block into the blockchain, provided it passes validation
        public bool insertOldBlock(Block block)
        {
            // Check if we already have the blocknum in the local chain
            foreach(Block local_block in blocks)
            {
                if(local_block.blockNum == block.blockNum)
                {
                    // We already have the block in our local blockchain

                    // Check the signatures
                    if(checkBlockSignatures(block) == false)
                    {
                        return false;
                    }

                    return false;
                }
            }

            // If we're here, it means the block is not already in the local blockchain.
            // Check the signatures
            if (checkBlockSignatures(block))
            {
                addToBlockchain(block);
                return true;
            }

            return false;
        }

        // Insert a block into the temporary list, until synchronization is complete
        public bool insertTemporaryBlock(Block block)
        {
            if (block == null)
                return false;

            // Check if we already have the blocknum in the temporary list
            foreach (Block temporary_block in temporaryBlocks)
            {
                if (temporary_block.blockNum == block.blockNum)
                {
                    // We already have the block in our local blockchain

                    // Check the signatures
                    if (checkBlockSignatures(block) == false)
                    {
                        return false;
                    }

                    // Check wallet state checksum
                    if (temporary_block.walletStateChecksum.Equals(block.walletStateChecksum, StringComparison.Ordinal) == false)
                    {
                        // Wallet state is different, request a new wallet state
                        // Todo: figure out what to do in this case
                        return false;
                    }

                    return false;
                }
            }

            // Check the signatures
            if (checkBlockSignatures(block))
            {
                lock (temporaryBlocks)
                {
                    temporaryBlocks.Add(block);
                }
                Console.WriteLine("Added block {0} to temporary blockchain.", block.blockNum);

                return true;
            }
            return true;
        }

        // Check and validate a block's signature
        public bool checkBlockSignatures(Block block)
        {
            // Block 1 is always the genesis block
            if (block.blockNum == 1)
                return true;

            // TODO: add additional verification based on signatures
            if (block.getUniqueSignatureCount() >= minimumConsensusSignatures)
            {
                return true;
            }

            return false;
        }

        // Call this to perform a merge operation of temporary blocks into the active blockchain.
        // This will empty the temporary blocks list
        public bool mergeTemporaryBlocks()
        {
            lock (temporaryBlocks)
            {
                // Sort the temporary block list according to blockNum ascending
                List<Block> sorted_List = temporaryBlocks.OrderBy(o => o.blockNum).ToList();

                foreach (Block temporary_block in sorted_List)
                {
                    insertBlock(temporary_block);
                }

                // Clear the temporary block list
                temporaryBlocks.Clear();
            }

            return true;
        }

        public string getLastBlockChecksum()
        {
            if (blocks.Count() < 1)
                return Crypto.sha256("IXIAN-GENESIS");

            return blocks.Last().blockChecksum;
        }

        public Block getLastBlock()
        {
            if (blocks.Count() < 1)
                return null;

            return blocks.Last();
        }

        public Block getBlock(ulong block_num)
        {
            if (blocks.Count() < 1)
                return null;

            if (block_num < 0)
            {
                return null;
            }

            // Return the block based on corresponding block number
            foreach (Block block in blocks)
            {
                if (block.blockNum == block_num)
                {
                    return block;
                }
            }

            // If the block is still no found and the node is a full history node
            if(Config.noHistory == false)
            {
                // TODO: quickly scan the storage for blocks not stored in memory
            }

            return null;
        }




    }
}
