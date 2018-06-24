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
        public List<Block> blocks = new List<Block> { };
        public bool synchronized = false;
        public ulong currentBlockNum = 0;
        public int minimumConsensusSignatures = 2;



        // Maintain a list of temporary blocks to be processed during synchronization
        public List<Block> temporaryBlocks = new List<Block> { };


        public BlockChain()
        {
            synchronized = false;
        }

        public bool insertBlock(Block new_block)
        {
            lock(blocks)
            {
                // Verify the block's checksums and signatures
                if (new_block.blockNum > 2 && blocks.Count() > 2)
                {
                    // Retrieve the latest block in the local chain
                    Block lastBlock = blocks.Last();

                    // Verify if the checksums match
                    if (lastBlock.blockChecksum.Equals(new_block.lastBlockChecksum))
                    {
                        // Checksum matches, continue
                    }
                    else
                    {
                        // The checksums didn't match. 
                        if (new_block.blockNum > lastBlock.blockNum + 1)
                        {
                            // A block has been skipped at this point
                            // TODO: fetch the correct missing block(s)
                        }
                        else
                        {
                            // Discard this block
                            Logging.log(LogSeverity.info, String.Format("Invalid block {0} checksum! Block has not been added to blockchain.",
                            new_block.blockNum));
                            return false;
                        }
                    }
                }

                // Check signatures before adding the block
                if (checkBlockSignatures(new_block))
                {
                    // Add to the blockchain
                    addToBlockchain(new_block);
                    currentBlockNum = new_block.blockNum;
                    
                    // Set the next minimum consensus to 75% of previous block's signatures
                    minimumConsensusSignatures = (int)((float)new_block.signatures.Count() * 0.75);

                    // Require at least two signatures to proceed
                    if (minimumConsensusSignatures < 2)
                        minimumConsensusSignatures = 2;

                    Console.WriteLine("Added block {0} to blockchain. Minimum consensus is {1} / {2}\n", 
                        currentBlockNum, minimumConsensusSignatures, new_block.signatures.Count());
                    return true;
                }

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

            // TODO: add verification based on signatures. Do not just accept the block if it has at least x signatures
            if (block.signatures.Count() >= minimumConsensusSignatures)
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

        // Adds the block to the memory-stored blockchain and writes to storage if history is enabled
        private bool addToBlockchain(Block block)
        {
            blocks.Add(block);

            //Storage.appendToStorage(block.getBytes());
            Storage.insertBlock(block);

            /* // For debugging block signatures
            Console.WriteLine("### BLOCK {0}", block.blockNum);
            // Write each signature
            foreach (string signature in block.signatures)
            {
                Console.WriteLine(signature);
            }
            Console.WriteLine("######");
            */
            return true;
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
