using DLT.Meta;
using IXICore;
using IXICore.Utils;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace DLT
{
    class BlockChain
    {
        List<Block> blocks = new List<Block>((int)CoreConfig.getRedactedWindowSize());

        Dictionary<ulong, Block> blocksDictionary = new Dictionary<ulong, Block>(); // A secondary storage for quick lookups


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
                while ((ulong)blocks.Count() > CoreConfig.redactedWindowSize)
                {
                    TransactionPool.redactTransactionsForBlock(blocks[0]); // Remove from Transaction Pool

                    // Check if this is a full history node
                    if (Config.storeFullHistory == false)
                    {
                        Storage.removeBlock(blocks[0]); // Remove from storage
                    }
                    lock (blocksDictionary)
                    {
                        blocksDictionary.Remove(blocks[0].blockNum);
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

            CoreConfig.redactedWindowSize = CoreConfig.getRedactedWindowSize(b.version);
            CoreConfig.minRedactedWindowSize = CoreConfig.getRedactedWindowSize(b.version);

            redactChain();
            lock (blocks)
            {
                if (blocks.Count > 20)
                {
                    Block tmp_block = getBlock(b.blockNum - 20);
                    if (tmp_block != null)
                    {
                        TransactionPool.compactTransactionsForBlock(tmp_block);
                    }
                }
            }
            return true;
        }

        // Attempts to retrieve a block from memory or from storage
        // Returns null if no block is found
        public Block getBlock(ulong blocknum, bool search_in_storage = false)
        {
            Block block = null;

            // Search memory
            lock (blocksDictionary)
            {
                if (blocksDictionary.ContainsKey(blocknum))
                    block = blocksDictionary[blocknum];
            }

            if (block != null)
                return block;

            // Search storage
            if (search_in_storage)
                block = Storage.getBlock(blocknum);

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
        public Block getBlockByHash(byte[] hash, bool search_in_storage = false)
        {
            Block block = null;

            // Search memory
            lock (blocks)
            {
                block = blocks.Find(x => x.blockChecksum.SequenceEqual(hash));
            }

            if (block != null)
                return block;

            // Search storage
            if (search_in_storage)
                block = Storage.getBlockByHash(hash);

            return block;
        }

        public ulong getLastBlockNum()
        {
            lock (blocks)
            {
                if (blocks.Count == 0) return 0;
                return blocks[blocks.Count - 1].blockNum;
            }
        }

        public int getLastBlockVersion()
        {
            lock (blocks)
            {
                if (blocks.Count == 0) return 0;
                return blocks[blocks.Count - 1].version;
            }
        }

        public int getRequiredConsensus()
        {
            // TODO TODO TODO cache
            int blockOffset = 6;
            lock (blocks)
            {
                if (blocks.Count < blockOffset + 1) return 1; // special case for first X blocks - since sigFreeze happens n-X blocks
                int totalConsensus = 0;
                int blockCount = blocks.Count - blockOffset < 10 ? blocks.Count - blockOffset : 10;
                for (int i = 0; i < blockCount; i++)
                {
                    totalConsensus += blocks[blocks.Count - i - blockOffset - 1].signatures.Count;
                }
                int consensus = (int)Math.Ceiling(totalConsensus / blockCount * CoreConfig.networkConsensusRatio);
                if (consensus < 2)
                {
                    consensus = 2;
                }
                return consensus;
            }
        }

        public int getRequiredConsensus(ulong block_num)
        {
            int block_offset = 6;
            if (block_num < (ulong)block_offset + 1) return 1; // special case for first X blocks - since sigFreeze happens n-X blocks
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
                    total_consensus += b.signatures.Count;
                    block_count++;
                }
                int consensus = (int)Math.Ceiling(total_consensus / block_count * CoreConfig.networkConsensusRatio);
                if (consensus < 2)
                {
                    consensus = 2;
                }
                return consensus;
            }
        }

        public byte[] getLastBlockChecksum()
        {
            lock (blocks)
            {
                if (blocks.Count == 0) return null;
                return blocks[blocks.Count - 1].blockChecksum;
            }
        }

        public byte[] getCurrentWalletState()
        {
            lock (blocks)
            {
                if (blocks.Count == 0) return null;
                return blocks[blocks.Count - 1].walletStateChecksum;
            }
        }

        public int getBlockSignaturesReverse(int index)
        {
            lock (blocks)
            {
                if (blocks.Count - index - 1 < 0) return 0;
                return blocks[blocks.Count - index - 1].signatures.Count();
            }
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
                    byte[] beforeSigsChecksum = blocks[idx].calculateSignatureChecksum();
                    beforeSigs = blocks[idx].signatures.Count;
                    if (forceRefresh)
                    {
                        blocks[idx].signatures = b.signatures;
                    }
                    else
                    {
                        blocks[idx].addSignaturesFrom(b);
                    }
                    byte[] afterSigsChecksum = blocks[idx].calculateSignatureChecksum();
                    afterSigs = blocks[idx].signatures.Count;
                    if (!beforeSigsChecksum.SequenceEqual(afterSigsChecksum))
                    {
                        updatestorage_block = blocks[idx];
                    }
                }
            }

            // Check if the block needs to be refreshed
            if (updatestorage_block != null)
            {
                Storage.insertBlock(updatestorage_block); // Update the block

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

        // Clears all the transactions in the pool
        public void clear()
        {
            lock (blocksDictionary)
            {
                blocksDictionary.Clear();
            }
            lock (blocks)
            {
                blocks.Clear();
            }
        }
    }
}
