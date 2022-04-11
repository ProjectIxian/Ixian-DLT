// Copyright (C) 2017-2020 Ixian OU
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
using IXICore.Inventory;
using IXICore.Meta;
using IXICore.Network;
using IXICore.Utils;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Numerics;

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

        Block preLastSuperBlock = null;
        Block lastSuperBlock = null;
        ulong lastSuperBlockNum = 0;
        byte[] lastSuperBlockChecksum = null;
        Dictionary<ulong, Block> pendingSuperBlocks = new Dictionary<ulong, Block>();

        Block genesisBlock = null;

        ulong reorgBlockStart = 0;

        ulong solvedBlocksCount = 0;
        ulong solvedBlocksRedactedWindowSize = 0;

        private int blockCount = 0;
        public long Count
        {
            get
            {
                return blockCount;
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
                        Node.storage.removeBlock(block.blockNum); // Remove from storage
                    }

                    if (block.powField != null)
                    {
                        decreaseSolvedBlocksCount();
                    }

                    lock (blocksDictionary)
                    {
                        blocksDictionary.Remove(block.blockNum);
                    }
                    blocks.RemoveAt(0); // Remove from memory
                }
                blockCount = blocks.Count;
                int redacted_block_count = begin_size - blockCount;
                if (redacted_block_count > 0)
                {
                    Logging.info("REDACTED {0} blocks to keep the chain length appropriate.", redacted_block_count);
                }
                if(redacted_block_count > 1)
                {
                    solvedBlocksRedactedWindowSize = 0;
                }
                return redacted_block_count;
            }
        }

        // Reverts redaction for a single block
        private bool unredactChain()
        {
            lock(blocks)
            {
                int redacted_window_size = (int)ConsensusConfig.getRedactedWindowSize(getLastBlockVersion());
                if (blocks.Count() == redacted_window_size)
                {
                    Logging.warn("Won't unredact chain, block count is already correct.");
                    return false;
                }

                while(blocks.Count() < redacted_window_size)
                {
                    if (lastBlockNum < (ulong)redacted_window_size)
                    {
                        return false;
                    }

                    ulong block_num_to_unredact = lastBlockNum - (ulong)blocks.Count();

                    Block b = getBlock(block_num_to_unredact, true, true);

                    lock (blocksDictionary)
                    {
                        if (blocksDictionary.ContainsKey(block_num_to_unredact))
                        {
                            Logging.warn("Won't unredact chain, block #{0} is already in memory.", block_num_to_unredact);
                            return false;
                        }
                    }

                    if (!TransactionPool.unredactTransactionsForBlock(b))
                    {
                        TransactionPool.redactTransactionsForBlock(b);
                        return false;
                    }

                    blocks.Insert(0, b);
                    lock (blocksDictionary)
                    {
                        blocksDictionary.Add(block_num_to_unredact, b);
                    }
                    blockCount = blocks.Count;

                    if (b.powField != null)
                    {
                        increaseSolvedBlocksCount();
                    }

                    Logging.info("UNREDACTED block #{0} to keep the chain length appropriate.", block_num_to_unredact);
                }
            }

            return true;
        }

        public bool appendBlock(Block b, bool add_to_storage = true)
        {
            lock (blocks)
            {
                if(blocks.Count > 0)
                {
                    Block prev_block = blocks[blocks.Count - 1];
                    // check for invalid block appending
                    if (b.blockNum != prev_block.blockNum + 1)
                    {
                        Logging.warn(String.Format("Attempting to add non-sequential block #{0} after block #{1}.",
                            b.blockNum,
                            prev_block.blockNum));
                        return false;
                    }
                    if (!b.lastBlockChecksum.SequenceEqual(prev_block.blockChecksum))
                    {
                        Logging.error(String.Format("Attempting to add a block #{0} with invalid lastBlockChecksum!", b.blockNum));
                        return false;
                    }
                    if (b.signatureFreezeChecksum != null && blocks.Count > 5 && !blocks[blocks.Count - 5].calculateSignatureChecksum().SequenceEqual(b.signatureFreezeChecksum))
                    {
                        Logging.error(String.Format("Attempting to add a block #{0} with invalid sigFreezeChecksum!", b.blockNum));
                        return false;
                    }
                }
                lastBlock = b;
                lastBlockNum = b.blockNum;
                if (b.version != lastBlockVersion)
                {
                    lastBlockVersion = b.version;
                }

                if (b.lastSuperBlockChecksum != null || b.blockNum == 1)
                {
                    pendingSuperBlocks.Remove(b.blockNum);

                    preLastSuperBlock = lastSuperBlock;

                    lastSuperBlock = b;
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
                    blockCount = blocks.Count;
                    Node.storage.insertBlock(b);
                    return true;
                }

                blocks.Add(b);
                lock (blocksDictionary)
                {
                    blocksDictionary.Add(b.blockNum, b);
                }
                blockCount = blocks.Count;

                if (reorgBlockStart <= b.blockNum)
                {
                    reorgBlockStart = 0;
                }
            }

            if (add_to_storage)
            {
                // Add block to storage
                Node.storage.insertBlock(b);
            }

            ConsensusConfig.redactedWindowSize = ConsensusConfig.getRedactedWindowSize(b.version);
            ConsensusConfig.minRedactedWindowSize = ConsensusConfig.getRedactedWindowSize(b.version);

            redactChain();
            lock (blocks)
            {
                if (blocks.Count > 30)
                {
                    Block tmp_block = getBlock(b.blockNum - 30);
                    if (tmp_block != null)
                    {
                        TransactionPool.compactTransactionsForBlock(tmp_block);
                        tmp_block.compact();
                    }
                }
                compactBlockSigs(b);
            }

            // Cleanup transaction pool
            TransactionPool.performCleanup();

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
                block = Node.storage.getBlock(blocknum);
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
                lock(blocksDictionary)
                {
                    if (blocksDictionary.Remove(blockNum))
                    {
                        if (blocks.RemoveAll(x => x.blockNum == blockNum) > 0)
                        {
                            blockCount = blocks.Count;
                            return true;
                        }
                    }
                }
                blockCount = blocks.Count;
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
                block = Node.storage.getBlockByHash(hash);
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
            // TODO TODO TODO TODO TODO there is an issue with calculating required consensus after blocks are compacted, for now this is resolved by increasing the compacting window
            int block_offset = 7;
            if (block_num < (ulong)block_offset + 1) return 1; // special case for first X blocks - since sigFreeze happens n-5 blocks
            lock (blocks)
            {
                int total_consensus = 0;
                int block_count = 0;
                for (int i = 0; i < 10; i++)
                {
                    ulong consensus_block_num = block_num - (ulong)i - (ulong)block_offset;
                    Block b = null;
                    if (blocksDictionary.ContainsKey(consensus_block_num))
                    {
                        b = blocksDictionary[consensus_block_num];
                    }
                    if (b == null)
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
                        consensus = (int)Math.Floor(consensus * ConsensusConfig.networkConsensusRatio);
                    }
                }
                else
                {
                    if (adjusted_to_ratio)
                    {
                        consensus = (int)Math.Floor(total_consensus / block_count * ConsensusConfig.networkConsensusRatio);
                    }
                }



                if (consensus < 2)
                {
                    consensus = 2;
                }

                return consensus;
            }
        }

        public BigInteger getRequiredSignerDifficulty(bool adjustToRatio)
        {
            // TODO TODO TODO cache
            return getRequiredSignerDifficulty(lastBlockNum + 1, adjustToRatio);
        }

        public BigInteger getRequiredSignerDifficulty(ulong blockNum, bool adjustToRatio)
        {
            // TODO TODO TODO cache
            BigInteger difficulty = SignerPowSolution.bitsToDifficulty(getRequiredSignerBits(blockNum));
            if (adjustToRatio)
            {
                difficulty = (difficulty * ConsensusConfig.networkConsensusRatioNew) / 100;
            }
            return difficulty;
        }

        public uint getRequiredSignerBits()
        {
            // TODO TODO TODO cache
            return getRequiredSignerBits(lastBlockNum + 1);
        }

        public uint getRequiredSignerBits(ulong blockNum)
        {
            if (blockNum == 1)
            {
                return SignerPowSolution.difficultyToBits(ConsensusConfig.minBlockSignerPowDifficulty);
            }
            if (lastSuperBlock == null || lastSuperBlock.signerBits == 0)
            {
                return SignerPowSolution.difficultyToBits(ConsensusConfig.minBlockSignerPowDifficulty);
            }
            if (blockNum <= lastSuperBlock.blockNum)
            {
                if(preLastSuperBlock == null || preLastSuperBlock.signerBits == 0)
                {
                    return SignerPowSolution.difficultyToBits(ConsensusConfig.minBlockSignerPowDifficulty);
                }
                return preLastSuperBlock.signerBits;
            }
            return lastSuperBlock.signerBits;
        }

        public uint calculateRequiredSignerBits(bool adjustToRatio)
        {
            // TODO TODO TODO cache
            return SignerPowSolution.difficultyToBits(calculateRequiredSignerDifficulty(adjustToRatio));
        }

        public BigInteger calculateRequiredSignerDifficulty(bool adjustToRatio)
        {
            // TODO TODO TODO TODO TODO there is an issue with calculating required consensus after blocks are compacted, for now this is resolved by increasing the compacting window
            ulong blockNum = getLastBlockNum() + 1;
            ulong blockOffset = 7;
            if (blockNum < blockOffset + 1) return ConsensusConfig.minBlockSignerPowDifficulty; // special case for first X blocks - since sigFreeze happens n-5 blocks
            lock (blocks)
            {
                BigInteger totalDifficulty = 0;
                int blockCount = 0;
                for (ulong i = 0; i < ConsensusConfig.superblockInterval; i++)
                {
                    ulong consensusBlockNum = blockNum - i - blockOffset;
                    Block b = null;
                    if (blocksDictionary.ContainsKey(consensusBlockNum))
                    {
                        b = blocksDictionary[consensusBlockNum];
                    }
                    if (b == null)
                    {
                        break;
                    }
                    totalDifficulty += b.getTotalSignerDifficulty();
                    blockCount++;
                }

                if (blockCount == 0)
                {
                    return ConsensusConfig.minBlockSignerPowDifficulty;
                }
                BigInteger newDifficulty = totalDifficulty / blockCount;

                if (adjustToRatio)
                {
                    // Adjust to ratio
                    newDifficulty = (newDifficulty * ConsensusConfig.networkConsensusRatioNew) / 100;
                }

                // Limit to max *2, /2
                BigInteger maxDifficulty = SignerPowSolution.bitsToDifficulty(lastSuperBlock.signerBits) * 2;
                if (newDifficulty > maxDifficulty)
                {
                    newDifficulty = maxDifficulty;
                }else
                {
                    BigInteger minDifficulty = SignerPowSolution.bitsToDifficulty(lastSuperBlock.signerBits) / 2;
                    if (newDifficulty < minDifficulty)
                    {
                        newDifficulty = minDifficulty;
                    }
                }

                if (newDifficulty < ConsensusConfig.minBlockSignerPowDifficulty)
                {
                    newDifficulty = ConsensusConfig.minBlockSignerPowDifficulty;
                }

                return newDifficulty;
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

        public bool refreshSignatures(Block b, bool forceRefresh = false, RemoteEndpoint endpoint = null)
        {
            if (!forceRefresh)
            {
                // we refuse to change sig numbers older than 4 blocks
                ulong lastBlockNum = getLastBlockNum();
                ulong sigLockHeight = lastBlockNum > 5 ? lastBlockNum - 4 : 1;
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

                    var added_sigs = blocks[idx].addSignaturesFrom(b, false);

                    if (added_sigs != null && Node.isMasterNode())
                    {
                        foreach (var sig in added_sigs)
                        {
                            Node.inventoryCache.setProcessedFlag(InventoryItemTypes.blockSignature, InventoryItemSignature.getHash(sig.signerAddress.addressNoChecksum, b.blockChecksum), true);

                            SignatureProtocolMessages.broadcastBlockSignature(sig, b.blockNum, b.blockChecksum, endpoint, null);
                        }
                    }

                    if (forceRefresh)
                    {
                        if(!b.verifyBlockProposer())
                        {
                            Logging.error("Error verifying block proposer while force refreshing signatures on block {0} ({1})", b.blockNum, Crypto.hashToString(b.blockChecksum));
                            return false;
                        }
                        Node.blockChain.setFrozenSignatures(blocks[idx], b.signatures);
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
                updateBlock(updatestorage_block);

                Logging.info("Refreshed block #{0}: Updated signatures {1} -> {2}", b.blockNum, beforeSigs, afterSigs);
                return true;
            }

            return false;
        }

        public bool setFrozenSignatures(Block b, List<BlockSignature> signatures)
        {
            lock(Node.blockProcessor.localBlockLock)
            {
                if(Node.blockProcessor.localNewBlock != null 
                    && Node.blockProcessor.localNewBlock.signatureFreezeChecksum != null
                    && b.frozenSignatures != null
                    && Node.blockProcessor.localNewBlock.signatureFreezeChecksum.SequenceEqual(b.calculateSignatureChecksum()))
                {
                    return false;
                }
                int sig_count = 0;
                if (signatures != null)
                    sig_count = signatures.Count;
                Logging.info("Setting {0} frozen signatures for {1}", sig_count, b.blockNum);
                b.setFrozenSignatures(signatures);
                return true;
            }
        }

        // Gets the elected nodes's pub key from the last sigFreeze; offset defines which entry to pick from the sigs
        public List<byte[]> getElectedNodesPubKeys(int offset)
        {
            List<byte[]> pubKeys = new List<byte[]>();
            Block targetBlock = getBlock(getLastBlockNum() - 6);
            Block curBlock = getBlock(getLastBlockNum());
            if (targetBlock != null && curBlock != null)
            {
                byte[] sigFreezeChecksum = curBlock.signatureFreezeChecksum;
                int sigNr = BitConverter.ToInt32(sigFreezeChecksum, 0);

                // Sort the signatures first
                List<BlockSignature> sortedSigs = new List<BlockSignature>(targetBlock.frozenSignatures??targetBlock.signatures);
                sortedSigs.Sort((x, y) => _ByteArrayComparer.Compare(x.signerAddress.addressNoChecksum, y.signerAddress.addressNoChecksum));
                int maxElectedNodes = 3;
                if(sortedSigs.Count < 100)
                {
                    maxElectedNodes = 1;
                }else
                {
                    offset = 0;
                }
                for(int i = offset; i < maxElectedNodes; i++)
                {
                    BlockSignature sig = sortedSigs[(int)((uint)(sigNr + i) % sortedSigs.Count)];

                    // Check if we have a public key instead of an address
                    if (sig.signerAddress.pubKey != null)
                    {
                        pubKeys.Add(sig.signerAddress.pubKey);
                        continue;
                    }

                    Wallet signerWallet = Node.walletState.getWallet(sig.signerAddress);
                    pubKeys.Add(signerWallet.publicKey); // signer pub key
                }
            }
            return pubKeys;
        }

        // Get the number of PoW solved blocks
        public ulong getSolvedBlocksCount(ulong redacted_window_size)
        {
            lock(blocks)
            {
                if(redacted_window_size != solvedBlocksRedactedWindowSize)
                {
                    ulong solved_blocks = 0;

                    ulong firstBlockNum = 1;
                    if (getLastBlockNum() > redacted_window_size)
                    {
                        firstBlockNum = getLastBlockNum() - redacted_window_size;
                    }

                    foreach (Block b in blocks)
                    {
                        if (b.blockNum < firstBlockNum)
                        {
                            continue;
                        }
                        if (b.powField != null)
                        {
                            solved_blocks++;
                        }
                    }

                    solvedBlocksRedactedWindowSize = redacted_window_size;
                    solvedBlocksCount = solved_blocks;
                    return solved_blocks;
                }
                else
                {
                    return solvedBlocksCount;
                }
            }
        }

        public long getTimeSinceLastBlock()
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
            lock (blocks)
            {
                lock (blocksDictionary)
                {
                    blocksDictionary.Clear();
                }
                blocks.Clear();
            }
            blockCount = blocks.Count;
        }

        // this function prunes un-needed sigs from blocks
        private void compactBlockSigs(Block last_block)
        {
            return; // TODO enable after enabling sig pruning

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

        public void updateBlock(Block block, bool update_storage = true)
        {
            // TODO TODO Omega prevent updating block older than sigfreezed block
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

            if(update_storage)
            {
                Node.storage.insertBlock(block);
            }

            Block new_block = new Block(block);

            if (compacted)
            {
                new_block.compact();
            }

            lock (blocks)
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
                    Logging.error("Error updating block #{0}", new_block.blockNum);
                }
            }
        }

        public Block getGenesisBlock()
        {
            return genesisBlock;
        }

        public bool revertLastBlock(bool blacklist = true, bool legacy_dual_revert = true)
        {
            if(lastBlockNum == 1)
            {
                Logging.error("Cannot revert block #1.");
                return false;
            }

            Block block_to_revert = lastBlock;
            ulong block_num_to_revert = block_to_revert.blockNum;
            if(!Node.walletState.canRevertTransaction(block_num_to_revert))
            {
                Logging.error("Cannot revert block #" + block_num_to_revert + ", WSJ transaction is missing.");
                return false;
            }

            if (reorgBlockStart == 0)
            {
                reorgBlockStart = block_num_to_revert;
            }

            // Re-org blockchain for max 7 blocks
            if (block_num_to_revert + 7 < reorgBlockStart)
            {
                Logging.error("Cannot revert block #" + block_num_to_revert + ", blockchain re-org started on " + reorgBlockStart);
                return false;
            }

            Logging.info("Reverting block #" + block_num_to_revert);

            if (blacklist)
            {
                Node.blockProcessor.blacklistBlock(block_to_revert);
            }

            removeBlock(block_num_to_revert);

            lastBlock = getBlock(block_num_to_revert - 1, true, true);
            lastBlockVersion = lastBlock.version;
            lastBlockReceivedTime = lastBlock.timestamp;
            lastBlockNum = lastBlock.blockNum;

            if (lastSuperBlockNum == block_num_to_revert)
            {
                Block super_block = block_to_revert;
                lastSuperBlockNum = super_block.lastSuperBlockNum;
                lastSuperBlockChecksum = super_block.lastSuperBlockChecksum;
            }

            ConsensusConfig.redactedWindowSize = ConsensusConfig.getRedactedWindowSize(lastBlockVersion);
            ConsensusConfig.minRedactedWindowSize = ConsensusConfig.getRedactedWindowSize(lastBlockVersion);

            // edge case for first block of block_version 3
            if (lastBlockVersion == BlockVer.v3 && getBlock(lastBlockNum - 1, true, true).version == BlockVer.v2)
            {
                Node.walletState.setCachedBlockVersion(2);
            }
            else
            {
                Node.walletState.setCachedBlockVersion(lastBlock.version);
            }

            unredactChain();

            Node.walletState.revertTransaction(block_num_to_revert);

            revertBlockTransactions(block_to_revert);

            Node.blockProcessor.resetSuperBlockCache();

            if (lastBlock.version >= BlockVer.v5 && lastBlock.lastSuperBlockChecksum == null)
            {
                if (lastBlock.version >= BlockVer.v8)
                {
                    if (!Node.walletState.calculateWalletStateDeltaChecksum(lastBlock.blockNum, lastBlock.version).SequenceEqual(lastBlock.walletStateChecksum))
                    {
                        Logging.error("Fatal error occurred: Delta Wallet state is incorrect after reverting block #{0} - Block's WS Checksum: {1}, WS Checksum: {2}, Wallets: {3}", block_num_to_revert, Crypto.hashToString(lastBlock.walletStateChecksum), Crypto.hashToString(Node.walletState.calculateWalletStateDeltaChecksum(lastBlock.blockNum, lastBlock.version)), Node.walletState.numWallets);
                        Node.stop();
                        return false;
                    }
                }else if(legacy_dual_revert)
                {
                    revertLastBlock(false, false);
                    return true;
                }
            }
            else
            {
                if (!Node.walletState.calculateWalletStateChecksum().SequenceEqual(lastBlock.walletStateChecksum))
                {
                    Logging.error("Fatal error occurred: Wallet state is incorrect after reverting block #{0} - Block's WS Checksum: {1}, WS Checksum: {2}, Wallets: {3}", block_num_to_revert, Crypto.hashToString(lastBlock.walletStateChecksum), Crypto.hashToString(Node.walletState.calculateWalletStateChecksum()), Node.walletState.numWallets);
                    Node.stop();
                    return false;
                }
            }
            return true;
        }

        public bool revertBlockTransactions(Block block)
        {
            foreach(var tx_id in block.transactions)
            {
                Transaction tx = TransactionPool.getAppliedTransaction(tx_id, block.blockNum, true);
                if(tx == null)
                {
                    Logging.error("Cannot revert transaction " + Transaction.getTxIdString(tx_id) + ", transaction doesn't exist.");
                    continue;
                }
                if (IxianHandler.isMyAddress(new Address(tx.pubKey)) || IxianHandler.extractMyAddressesFromAddressList(tx.toList) != null)
                {
                    ActivityStorage.updateStatus(tx.id, ActivityStatus.Error, block.blockNum);
                    tx.fromLocalStorage = false;
                }
                if (tx.type == (int)Transaction.Type.StakingReward)
                {
                    TransactionPool.removeAppliedTransaction(tx.id);
                }else
                {
                    TransactionPool.unapplyTransaction(tx.id);
                    if (tx.type == (int)Transaction.Type.PoWSolution)
                    {
                        ulong pow_blocknum = tx.powSolution.blockNum;

                        Block pow_block = getBlock(pow_blocknum, true, true);
                        // Check if the block is valid
                        if (pow_block == null)
                        {
                            Logging.error("PoW target block {0} not found", pow_blocknum);
                            continue;
                        }
                        if (pow_block.powField != null)
                        {
                            decreaseSolvedBlocksCount();
                            pow_block.powField = null;
                            updateBlock(pow_block);
                        }
                    }
                }
            }
            return true;
        }

        public void increaseSolvedBlocksCount()
        {
            lock (blocks)
            {
                solvedBlocksCount++;
            }
        }

        public void decreaseSolvedBlocksCount()
        {
            lock (blocks)
            {
                solvedBlocksCount--;
            }
        }

        public BigInteger getMinSignerPowDifficulty(ulong blockNum)
        {
            if (Count < 8)
            {
                return ConsensusConfig.minBlockSignerPowDifficulty;
            }
            Block tb = getBlock(blockNum - 6, true, true);
            if (tb == null)
            {
                return SignerPowSolution.bitsToDifficulty(SignerPowSolution.maxTargetBits);
            }
            var difficulty = getRequiredSignerDifficulty(blockNum, true) / ((ulong)tb.getFrozenSignatureCount() * 10);
            if (difficulty < ConsensusConfig.minBlockSignerPowDifficulty)
            {
                difficulty = ConsensusConfig.minBlockSignerPowDifficulty;
            }
            return difficulty;
        }
    }
}
