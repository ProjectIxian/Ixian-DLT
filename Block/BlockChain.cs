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
        SortedDictionary<ulong, Block> blocks = new SortedDictionary<ulong, Block>();

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

        public void redactChain()
        { 

            lock (blocks)
            {
                // redaction
                int begin_size = blocks.Count();
                while ((ulong)blocks.Count() > CoreConfig.redactedWindowSize)
                {
                    Block block = blocks.ElementAt(0).Value;
                    TransactionPool.redactTransactionsForBlock(block); // Remove from Transaction Pool

                    // Check if this is a full history node
                    if (Config.storeFullHistory == false)
                    {
                        Storage.removeBlock(block); // Remove from storage
                    }
                    blocks.Remove(block.blockNum); // Remove from memory
                }
                if (begin_size > blocks.Count())
                {
                    Logging.info(String.Format("REDACTED {0} blocks to keep the chain length appropriate.", begin_size - blocks.Count()));
                }
            }
        }

        public bool appendBlock(Block b)
        {
            lock (blocks)
            {
                // special case when we are starting up and have an empty chain
                if (blocks.Count == 0)
                {
                    blocks.Add(b.blockNum, b);
                    Storage.insertBlock(b);
                    return true;
                }
                // check for invalid block appending
                if (b.blockNum != blocks.Last().Value.blockNum + 1)
                {
                    Logging.warn(String.Format("Attempting to add non-sequential block #{0} after block #{1}.",
                        b.blockNum,
                        blocks.Last().Value.blockNum));
                    return false;
                }
                if (!b.lastBlockChecksum.SequenceEqual(blocks.Last().Value.blockChecksum))
                {
                    Logging.error(String.Format("Attempting to add a block #{0} with invalid lastBlockChecksum!", b.blockNum));
                    return false;
                }
                if (b.signatureFreezeChecksum != null && blocks.Count > 5 && !blocks.ElementAt(blocks.Count - 5).Value.calculateSignatureChecksum().SequenceEqual(b.signatureFreezeChecksum))
                {
                    Logging.error(String.Format("Attempting to add a block #{0} with invalid sigFreezeChecksum!", b.blockNum));
                    return false;
                }
                blocks.Add(b.blockNum, b);
            }
            // Add block to storage
            Storage.insertBlock(b);
            redactChain();
            return true;           
        }

        public bool removeBlock(ulong blockNum)
        {
            lock (blocks)
            {
                return blocks.Remove(blockNum);
            }
        }

        // Attempts to retrieve a block from memory or from storage
        // Returns null if no block is found
        public Block getBlock(ulong blocknum, bool search_in_storage = false)
        {
            Block block = null;

            // Search memory
            lock (blocks)
            {
                if(blocks.ContainsKey(blocknum))
                    block = blocks[blocknum];
            }

            if (block != null)
                return block;

            // Search storage
            if (search_in_storage)
                block = Storage.getBlock(blocknum);

            return block;
        }

        public List<Block> getBlocks(int fromIndex = 0, int count = 0)
        {
            lock(blocks)
            {
                List<Block> blockList = blocks.Select(x => x.Value).Skip(fromIndex).ToList();
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
                block = blocks.Select(x => x.Value).Where(x => x.blockChecksum.SequenceEqual(hash)).First();
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
                return blocks.Last().Value.blockNum;
            }
        }

        public int getRequiredConsensus()
        {
            int blockOffset = 6;
            lock (blocks)
            {
                if (blocks.Count < blockOffset + 1) return 1; // special case for first X blocks - since sigFreeze happens n-X blocks
                int totalConsensus = 0;
                int blockCount = blocks.Count - blockOffset < 10 ? blocks.Count - blockOffset : 10;
                for (int i = 0; i < blockCount; i++)
                {
                    totalConsensus += blocks.ElementAt(blocks.Count - i - blockOffset - 1).Value.signatures.Count;
                }
                int consensus = (int)Math.Ceiling(totalConsensus / blockCount * CoreConfig.networkConsensusRatio);
                if (consensus < 2)
                {
                    consensus = 2;
                }
                return consensus;
            }
        }

        public byte[] getLastBlockChecksum()
        {
            lock(blocks)
            {
                if (blocks.Count == 0) return null;
                return blocks.Last().Value.blockChecksum;
            }
        }

        public byte[] getCurrentWalletState()
        {
            lock(blocks)
            {
                if (blocks.Count == 0) return null;
                return blocks.Last().Value.walletStateChecksum;
            }
        }

        public int getBlockSignaturesReverse(int index)
        {
            lock(blocks)
            {
                if (blocks.Count - index - 1 < 0) return 0;
                return blocks.ElementAt(blocks.Count - index - 1).Value.signatures.Count();
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
                Block block = blocks[b.blockNum];
                if (block != null && block.blockChecksum.SequenceEqual(b.blockChecksum))
                {
                    byte[] beforeSigsChecksum = block.calculateSignatureChecksum();
                    beforeSigs = block.signatures.Count;
                    if (forceRefresh)
                    {
                        block.signatures = b.signatures;
                    }
                    else
                    {
                        block.addSignaturesFrom(b);
                    }
                    byte[] afterSigsChecksum = block.calculateSignatureChecksum();
                    afterSigs = block.signatures.Count;
                    if (!beforeSigsChecksum.SequenceEqual(afterSigsChecksum))
                    {
                        updatestorage_block = block;
                    }
                }
            }

            // Check if the block needs to be refreshed
            if(updatestorage_block != null)
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
        public ulong getSolvedBlocksCount()
        {
            ulong solved_blocks = 0;

            ulong firstBlockNum = 1;
            if (Node.blockChain.getLastBlockNum() > CoreConfig.redactedWindowSize)
            {
                firstBlockNum = Node.blockChain.getLastBlockNum() - CoreConfig.redactedWindowSize;
            }
            lock (blocks)
            {
                foreach (KeyValuePair<ulong,Block> entry in blocks)
                {
                    if(entry.Key < firstBlockNum)
                    {
                        continue;
                    }
                    if(entry.Value.powField != null)
                    {
                        // TODO: an additional verification would be nice
                        solved_blocks++;
                    }
                }
            }
            return solved_blocks;
        }

    }
}
