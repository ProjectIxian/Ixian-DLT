using DLT.Meta;
using IXICore;
using IXICore.Utils;
using System;
using System.Collections.Generic;
using System.Linq;

namespace DLT
{
    class BlockChain
    {
        List<Block> blocks = new List<Block>((int)ConsensusConfig.getRedactedWindowSize());

        Dictionary<ulong, Block> blocksDictionary = new Dictionary<ulong, Block>(); // A secondary storage for quick lookups

        long lastBlockReceivedTime = Clock.getTimestamp();

        Block lastBlock = null;
        ulong lastBlockNum = 0;
        int lastBlockVersion = 0;

        ulong lastSuperBlockNum = 0;
        byte[] lastSuperBlockChecksum = null;
        Dictionary<ulong, Block> pendingSuperBlocks = new Dictionary<ulong, Block>();

        Block genesisBlock = null;

        public long Count
        {
            get
            {
                lock (blocks)
                {
                    return blocks.LongCount();
                }
            }
        }

        public BlockChain()
        {
        }

        public int redactChain()
        {

            lock (blocks)
            {
                // redaction
                int begin_size = blocks.Count();
                while ((ulong)blocks.Count() > ConsensusConfig.redactedWindowSize)
                {
                    Block block = getBlock(blocks[0].blockNum);

                    if(block == null)
                    {
                        break;
                    }

                    TransactionPool.redactTransactionsForBlock(block); // Remove from Transaction Pool

                    // Check if this is a full history node
                    if (Config.storeFullHistory == false)
                    {
                        Storage.removeBlock(block); // Remove from storage
                    }

                    lock (blocksDictionary)
                    {
                        blocksDictionary.Remove(block.blockNum);
                    }
                    blocks.RemoveAt(0); // Remove from memory
                }
                if (begin_size > blocks.Count())
                {
                    Logging.info(String.Format("REDACTED {0} blocks to keep the chain length appropriate.", begin_size - blocks.Count()));
                }
                return begin_size - blocks.Count();
            }
        }

        public bool appendBlock(Block b, bool add_to_storage = true)
        {
            lock (blocks)
            {
                if (b.blockNum > lastBlockNum)
                {
                    lastBlock = b;
                    lastBlockNum = b.blockNum;
                    if (b.version != lastBlockVersion)
                    {
                        lastBlockVersion = b.version;
                    }
                }

                if (b.lastSuperBlockChecksum != null || b.blockNum == 1)
                {
                    pendingSuperBlocks.Remove(b.blockNum);

                    lastSuperBlockNum = b.blockNum;
                    lastSuperBlockChecksum = b.blockChecksum;
                }

                // special case when we are starting up and have an empty chain
                if (blocks.Count == 0)
                {
                    blocks.Add(b);
                    lock (blocksDictionary)
                    {
                        blocksDictionary.Add(b.blockNum, b);
                    }
                    Storage.insertBlock(b);
                    return true;
                }
                // check for invalid block appending
                if (b.blockNum != blocks[blocks.Count - 1].blockNum + 1)
                {
                    Logging.warn(String.Format("Attempting to add non-sequential block #{0} after block #{1}.",
                        b.blockNum,
                        blocks[blocks.Count - 1].blockNum));
                    return false;
                }
                if (!b.lastBlockChecksum.SequenceEqual(blocks[blocks.Count - 1].blockChecksum))
                {
                    Logging.error(String.Format("Attempting to add a block #{0} with invalid lastBlockChecksum!", b.blockNum));
                    return false;
                }
                if (b.signatureFreezeChecksum != null && blocks.Count > 5 && !blocks[blocks.Count - 5].calculateSignatureChecksum().SequenceEqual(b.signatureFreezeChecksum))
                {
                    Logging.error(String.Format("Attempting to add a block #{0} with invalid sigFreezeChecksum!", b.blockNum));
                    return false;
                }
                blocks.Add(b);
                lock (blocksDictionary)
                {
                    blocksDictionary.Add(b.blockNum, b);
                }
            }

            if (add_to_storage)
            {
                // Add block to storage
                Storage.insertBlock(b);
            }

            ConsensusConfig.redactedWindowSize = ConsensusConfig.getRedactedWindowSize(b.version);
            ConsensusConfig.minRedactedWindowSize = ConsensusConfig.getRedactedWindowSize(b.version);

            redactChain();
            lock (blocks)
            {
                if (blocks.Count > 20)
                {
                    Block tmp_block = getBlock(b.blockNum - 20);
                    if (tmp_block != null)
                    {
                        TransactionPool.compactTransactionsForBlock(tmp_block);
                        tmp_block.compact();
                    }
                }
                compactBlockSigs(b);
            }

            lastBlockReceivedTime = Clock.getTimestamp();

            return true;
        }

        // Attempts to retrieve a block from memory or from storage
        // Returns null if no block is found
        public Block getBlock(ulong blocknum, bool search_in_storage = false, bool return_full_block = true)
        {
            Block block = null;

            bool compacted_block = false;

            byte[] pow_field = null;

            // Search memory
            lock (blocksDictionary)
            {
                if (blocksDictionary.ContainsKey(blocknum))
                {
                    block = blocksDictionary[blocknum];
                    pow_field = block.powField;

                    if (block.compacted && return_full_block)
                    {
                        compacted_block = true;
                        block = null;
                    }
                }
            }

            if (block != null)
                return block;

            // Search storage
            if (search_in_storage || compacted_block)
            {
                block = Storage.getBlock(blocknum);
                if (block != null && compacted_block)
                {
                    block.powField = pow_field;
                }
            }

            return block;
        }

        public bool removeBlock(ulong blockNum)
        {
            lock (blocks)
            {
                if (blocksDictionary.Remove(blockNum))
                {
                    if (blocks.RemoveAll(x => x.blockNum == blockNum) > 0)
                    {
                        return true;
                    }
                }
                return false;
            }
        }

        public List<Block> getBlocks(int fromIndex = 0, int count = 0)
        {
            lock (blocks)
            {
                List<Block> blockList = blocks.Skip(fromIndex).ToList();
                if (count == 0)
                {
                    return blockList;
                }
                return blockList.Take(count).ToList();
            }
        }

        // Attempts to retrieve a block from memory or from storage
        // Returns null if no block is found
        public Block getBlockByHash(byte[] hash, bool search_in_storage = false, bool return_full_block = true)
        {
            Block block = null;

            bool compacted_block = false;

            byte[] pow_field = null;

            // Search memory
            lock (blocks)
            {
                block = blocks.Find(x => x.blockChecksum.SequenceEqual(hash));

                if (block != null)
                {
                    pow_field = block.powField;
                    if (block.compacted && return_full_block)
                    {
                        compacted_block = true;
                        block = null;
                    }
                }
            }

            if (block != null)
                return block;

            // Search storage
            if (search_in_storage || compacted_block)
            {
                block = Storage.getBlockByHash(hash);
                if (block != null && compacted_block)
                {
                    block.powField = pow_field;
                }
            }

            return block;
        }

        public ulong getLastBlockNum()
        {
            return lastBlockNum;
        }

        public int getLastBlockVersion()
        {
            return lastBlockVersion;
        }

        public void  setLastBlockVersion(int version)
        {
            lastBlockVersion = version;
        }

        public ulong getLastSuperBlockNum()
        {
            return lastSuperBlockNum;
        }

        public byte[] getLastSuperBlockChecksum()
        {
            return lastSuperBlockChecksum;
        }

        public void setGenesisBlock(Block genesis)
        {
            genesisBlock = genesis;
        }

        public int getRequiredConsensus()
        {
            // TODO TODO TODO cache
            return getRequiredConsensus(lastBlockNum + 1);
        }

        public int getRequiredConsensus(ulong block_num, bool adjusted_to_ratio = true)
        {
            int block_offset = 6;
            if (block_num < (ulong)block_offset + 2) return 1; // special case for first X blocks - since sigFreeze happens n-5 blocks
            lock (blocks)
            {
                int total_consensus = 0;
                int block_count = 0;
                for (int i = 0; i < 10; i++)
                {
                    ulong consensus_block_num = block_num - (ulong)i - (ulong)block_offset - 1;
                    Block b = blocks.Find(x => x.blockNum == consensus_block_num);
                    if(b == null)
                    {
                        break;
                    }
                    total_consensus += b.getFrozenSignatureCount();
                    block_count++;
                }

                if (block_count == 0)
                {
                    return ConsensusConfig.maximumBlockSigners;
                }

                int consensus = (int)Math.Ceiling((double)total_consensus / block_count);

                if (consensus > ConsensusConfig.maximumBlockSigners)
                {
                    consensus = ConsensusConfig.maximumBlockSigners;
                    if (adjusted_to_ratio)
                    {
                        consensus = (int)Math.Ceiling(consensus * ConsensusConfig.networkConsensusRatio);
                    }
                }
                else
                {
                    if (adjusted_to_ratio)
                    {
                        consensus = (int)Math.Ceiling(total_consensus / block_count * ConsensusConfig.networkConsensusRatio);
                    }
                }



                if (consensus < 2)
                {
                    consensus = 2;
                }

                return consensus;
            }
        }

        public byte[] getLastBlockChecksum()
        {
            if(lastBlock != null)
            {
                return lastBlock.blockChecksum;
            }
            return null;
        }

        public Block getLastBlock()
        {
            return lastBlock;
        }

        public byte[] getCurrentWalletState()
        {
            if (lastBlock != null)
            {
                return lastBlock.walletStateChecksum;
            }
            return null;
        }

        public bool refreshSignatures(Block b, bool forceRefresh = false)
        {
            if (!forceRefresh)
            {
                // we refuse to change sig numbers older than 4 blocks
                ulong sigLockHeight = getLastBlockNum() > 5 ? getLastBlockNum() - 3 : 1;
                if (b.blockNum <= sigLockHeight)
                {
                    return false;
                }
            }
            Block updatestorage_block = null;
            int beforeSigs = 0;
            int afterSigs = 0;

            lock (blocks)
            {
                int idx = blocks.FindIndex(x => x.blockNum == b.blockNum && x.blockChecksum.SequenceEqual(b.blockChecksum));
                if (idx > 0)
                {
                    if(blocks[idx].compacted)
                    {
                        Logging.error("Trying to refresh signatures on compacted block {0}", blocks[idx].blockNum);
                        return false;
                    }

                    byte[] beforeSigsChecksum = blocks[idx].calculateSignatureChecksum();
                    beforeSigs = blocks[idx].getFrozenSignatureCount();

                    blocks[idx].addSignaturesFrom(b);

                    if (forceRefresh)
                    {
                        blocks[idx].setFrozenSignatures(b.signatures);
                        afterSigs = b.signatures.Count;
                    }
                    else
                    {
                        afterSigs = blocks[idx].signatures.Count;
                    }

                    byte[] afterSigsChecksum = blocks[idx].calculateSignatureChecksum();
                    if (!beforeSigsChecksum.SequenceEqual(afterSigsChecksum))
                    {
                        updatestorage_block = blocks[idx];
                    }
                }
            }

            // Check if the block needs to be refreshed
            if (updatestorage_block != null)
            {
                Node.blockChain.updateBlock(updatestorage_block);

                Logging.info(String.Format("Refreshed block #{0}: Updated signatures {1} -> {2}", b.blockNum, beforeSigs, afterSigs));
                return true;
            }

            return false;
        }

        // Gets the elected node's pub key from the last sigFreeze; offset defines which entry to pick from the sigs
        public byte[] getLastElectedNodePubKey(int offset = 0)
        {
            Block targetBlock = getBlock(getLastBlockNum() - 6);
            Block curBlock = getBlock(getLastBlockNum());
            if (targetBlock != null && curBlock != null)
            {
                byte[] sigFreezeChecksum = curBlock.signatureFreezeChecksum;
                int sigNr = BitConverter.ToInt32(sigFreezeChecksum, 0) + offset;

                // Sort the signatures first
                List<byte[][]> sortedSigs = new List<byte[][]>(targetBlock.signatures);
                sortedSigs.Sort((x, y) => _ByteArrayComparer.Compare(x[1], y[1]));

                byte[][] sig = sortedSigs[(int)((uint)sigNr % sortedSigs.Count)];

                // Note: we don't need any further validation, since this block has already passed through BlockProcessor.verifyBlock() at this point.
                byte[] address = sig[1];

                // Check if we have a public key instead of an address
                if (address.Length > 70)
                {
                    return address;
                }

                Wallet signerWallet = Node.walletState.getWallet(address);
                return signerWallet.publicKey; // signer pub key
            }
            return null;
        }

        // Get the number of PoW solved blocks
        public ulong getSolvedBlocksCount(ulong redacted_window_size)
        {
            // TODO TODO TODO TODO cache
            ulong solved_blocks = 0;

            ulong firstBlockNum = 1;
            if (Node.blockChain.getLastBlockNum() > redacted_window_size)
            {
                firstBlockNum = Node.blockChain.getLastBlockNum() - redacted_window_size;
            }
            lock (blocksDictionary)
            {
                foreach (KeyValuePair<ulong, Block> entry in blocksDictionary)
                {
                    if (entry.Key < firstBlockNum)
                    {
                        continue;
                    }
                    if (entry.Value.powField != null)
                    {
                        // TODO: an additional verification would be nice
                        solved_blocks++;
                    }
                }
            }
            return solved_blocks;
        }

        public long getTimeSinceLastBLock()
        {
            return Clock.getTimestamp() - lastBlockReceivedTime;
        }

        public Block getPendingSuperBlock(ulong block_num)
        {
            lock(pendingSuperBlocks)
            {
                if (pendingSuperBlocks.ContainsKey(block_num))
                {
                    return pendingSuperBlocks[block_num];
                }else
                {
                    return null;
                }
            }
        }

        public Block getSuperBlock(ulong block_num)
        {
            return getBlock(block_num, true);
        }

        public Block getSuperBlock(byte[] block_checksum)
        {
            return getBlockByHash(block_checksum, true);
        }

        // Clears all the transactions in the pool
        public void clear()
        {
            lastBlock = null;
            lastBlockNum = 0;
            lastBlockVersion = 0;
            lock (blocksDictionary)
            {
                blocksDictionary.Clear();
            }
            lock (blocks)
            {
                blocks.Clear();
            }
        }

        // this function prunes un-needed sigs from blocks
        private void compactBlockSigs(Block last_block)
        {
            if(last_block.version < BlockVer.v5)
            {
                return;
            }

            if (last_block.lastSuperBlockChecksum == null)
            {
                return;
            }

            // superblock was just generated, prune all block sigs, except sigs within the superblock window

            ulong prev_superblock_num = last_block.lastSuperBlockNum;

            for(ulong block_num = prev_superblock_num; block_num > 1; block_num--)
            {
                Block block = getBlock(block_num, true, true);

                if (block == null)
                {
                    Logging.error("Block {0} was null while compacting sigs", block_num);
                    break;
                }

                if (block.version < BlockVer.v4)
                {
                    break;
                }

                if (block.compactedSigs == true)
                {
                    break;
                }

                block.pruneSignatures();
                updateBlock(block);
            }
        }

        public void updateBlock(Block block)
        {
            bool compacted = false;
            bool compacted_sigs = false;

            lock (blocksDictionary)
            {
                if(blocksDictionary.ContainsKey(block.blockNum))
                {
                    Block old_block = blocksDictionary[block.blockNum];
                    if (old_block.compacted)
                    {
                        compacted = true;
                    }
                    if (old_block.compactedSigs)
                    {
                        compacted_sigs = true;
                    }
                }
            }

            if (compacted_sigs)
            {
                block.pruneSignatures();
            }

            Meta.Storage.insertBlock(block);

            if (compacted)
            {
                Block new_block = new Block(block);

                new_block.compact();

                lock(blocks)
                {
                    int block_idx = blocks.FindIndex(x => x.blockNum == new_block.blockNum);
                    if (block_idx >= 0)
                    {
                        blocks[block_idx] = new_block;
                        lock (blocksDictionary)
                        {
                            blocksDictionary[new_block.blockNum] = new_block;
                        }
                    }
                    else
                    {
                        Logging.error("Error updating block {0}", new_block.blockNum);
                    }
                }
            }
        }
    }
}
