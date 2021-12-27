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
using IXICore;
using IXICore.Meta;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Numerics;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;

namespace DLT
{
    public enum BlockSearchMode
    {
        lowestDifficulty,
        randomLowestDifficulty,
        latestBlock,
        random
    }

    class Miner
    {
        public bool pause = true; // Flag to toggle miner activity

        public long lastHashRate = 0; // Last reported hash rate
        public ulong currentBlockNum = 0; // Mining block number
        public int currentBlockVersion = 0;
        public ulong currentBlockDifficulty = 0; // Current block difficulty
        public byte[] currentHashCeil { get; private set; }
        public ulong lastSolvedBlockNum = 0; // Last solved block number
        private DateTime lastSolvedTime = DateTime.MinValue; // Last locally solved block time
        public BlockSearchMode searchMode = BlockSearchMode.randomLowestDifficulty;

        private long hashesPerSecond = 0; // Total number of hashes per second
        private DateTime lastStatTime; // Last statistics output time
        private bool shouldStop = false; // flag to signal shutdown of threads
        private ThreadLiveCheck TLC;


        Block activeBlock = null;
        bool blockFound = false;

        byte[] activeBlockChallenge = null;

        private static Random random = new Random(); // used to seed initial curNonce's
        [ThreadStatic] private static byte[] curNonce = null; // Used for random nonce
        [ThreadStatic] private static byte[] dummyExpandedNonce = null;
        [ThreadStatic] private static int lastNonceLength = 0;

        private static List<ulong> solvedBlocks = new List<ulong>(); // Maintain a list of solved blocks to prevent duplicate work
        private static long solvedBlockCount = 0;

        static object activePoolBlockLock = new object();
        static Block activePoolBlock = null;

        public Miner()
        {
            lastStatTime = DateTime.UtcNow;

        }

        // Starts the mining threads
        public bool start()
        {
            // Calculate the allowed number of threads based on logical processor count
            Config.miningThreads = calculateMiningThreadsCount(Config.miningThreads);
            Logging.info(String.Format("Starting miner with {0} threads on {1} logical processors.", Config.miningThreads, Environment.ProcessorCount));

            shouldStop = false;

            TLC = new ThreadLiveCheck();
            // Start primary mining thread
            Thread miner_thread = new Thread(threadLoop);
            miner_thread.Name = "Miner_Main_Thread";
            miner_thread.Start();

            // Start secondary worker threads
            for (int i = 0; i < Config.miningThreads - 1; i++)
            {
                Thread worker_thread = new Thread(secondaryThreadLoop);
                worker_thread.Name = "Miner_Worker_Thread_#" + i.ToString();
                worker_thread.Start();
            }

            return true;
        }

        // Signals all the mining threads to stop
        public bool stop()
        {
            shouldStop = true;
            return true;
        }

        // Returns the allowed number of mining threads based on amount of logical processors detected
        public static uint calculateMiningThreadsCount(uint miningThreads)
        {
            uint vcpus = (uint)Environment.ProcessorCount;

            // Single logical processor detected, force one mining thread maximum
            if (vcpus <= 1)
            {
                Logging.info("Single logical processor detected, forcing one mining thread maximum.");
                return 1;
            }

            // Calculate the maximum number of threads allowed
            uint maxThreads = (vcpus / 2) - 1;
            if (maxThreads < 1)
            {
                return 1;
            }

            // Provided mining thread count exceeds maximum
            if (miningThreads > maxThreads)
            {
                Logging.warn("Provided mining thread count ({0}) exceeds maximum allowed ({1})", miningThreads, maxThreads);
                return maxThreads;
            }

            // Provided mining thread count is allowed
            return miningThreads;
        }

        private void threadLoop(object data)
        {
            while (!shouldStop)
            {
                TLC.Report();
                Thread.Sleep(1000);

                // Wait for blockprocessor network synchronization
                if (Node.blockProcessor.operating == false)
                {
                    continue;
                }

                // Edge case for seeds
                if (Node.blockChain.getLastBlockNum() > 10)
                {
                    break;
                }
            }

            while (!shouldStop)
            {               
                if (pause)
                {
                    lastStatTime = DateTime.UtcNow;
                    lastHashRate = hashesPerSecond;
                    hashesPerSecond = 0;
                    Thread.Sleep(500);
                    continue;
                }

                if (blockFound == false)
                {
                    searchForBlock();
                }
                else
                {
                    calculatePow_v3(currentHashCeil);
                }

                // Output mining stats
                TimeSpan timeSinceLastStat = DateTime.UtcNow - lastStatTime;
                if (timeSinceLastStat.TotalSeconds > 5)
                {
                    printMinerStatus();
                }
            }
        }

        private void secondaryThreadLoop(object data)
        {
            while (!shouldStop)
            {
                TLC.Report();
                Thread.Sleep(1000);

                // Wait for blockprocessor network synchronization
                if (Node.blockProcessor.operating == false)
                {
                    continue;
                }

                // Edge case for seeds
                if (Node.blockChain.getLastBlockNum() > 10)
                {
                    break;
                }
            }

            while (!shouldStop)
            {
                if (pause)
                {
                    Thread.Sleep(500);
                    continue;
                }

                if (blockFound == false)
                {
                    Thread.Sleep(10);
                    continue;
                }

                calculatePow_v3(currentHashCeil);
            }
        }

        public void forceSearchForBlock()
        {
            blockFound = false;
        }

        public void checkActiveBlockSolved()
        {
            if (currentBlockNum > 0)
            {
                if (currentBlockNum > ConsensusConfig.getRedactedWindowSize()
                    && currentBlockNum + ConsensusConfig.getRedactedWindowSize() - 100 <= Node.blockChain.getLastBlockNum())
                {
                    blockFound = false;
                }else
                {
                    Block tmpBlock = Node.blockChain.getBlock(currentBlockNum, false, false);
                    if (tmpBlock == null || tmpBlock.powField != null)
                    {
                        blockFound = false;
                    }
                }
            }

            lock (activePoolBlockLock)
            {
                if (activePoolBlock != null)
                {
                    if (activePoolBlock.blockNum > ConsensusConfig.getRedactedWindowSize()
                        && activePoolBlock.blockNum + ConsensusConfig.getRedactedWindowSize() - 100 <= Node.blockChain.getLastBlockNum())
                    {
                        activePoolBlock = null;
                    }
                    else
                    {
                        Block tmpBlock = Node.blockChain.getBlock(activePoolBlock.blockNum, false, false);
                        if (tmpBlock == null || tmpBlock.powField != null)
                        {
                            activePoolBlock = null;
                        }
                    }
                }
            }
        }

        // Static function used by the getMiningBlock API call
        public static Block getMiningBlock(BlockSearchMode searchMode)
        {
            Block candidate_block = null;

            lock (activePoolBlockLock)
            {
                if (activePoolBlock != null)
                {
                    return activePoolBlock;
                }
            }

            List<Block> blockList = null;

            int block_offset = 1;
            if (Node.blockChain.Count >= (long)ConsensusConfig.getRedactedWindowSize())
            {
                block_offset = 1000;
            }

            if (searchMode == BlockSearchMode.lowestDifficulty)
            {
                blockList = Node.blockChain.getBlocks(block_offset, (int)Node.blockChain.Count - block_offset).Where(x => x.powField == null).OrderBy(x => x.difficulty).ToList();
            }
            else if (searchMode == BlockSearchMode.randomLowestDifficulty)
            {
                Random rnd = new Random();
                blockList = Node.blockChain.getBlocks(block_offset, (int)Node.blockChain.Count - block_offset).Where(x => x.powField == null).OrderBy(x => x.difficulty).Skip(rnd.Next(200)).ToList();
            }
            else if (searchMode == BlockSearchMode.latestBlock)
            {
                blockList = Node.blockChain.getBlocks(block_offset, (int)Node.blockChain.Count - block_offset).Where(x => x.powField == null).OrderByDescending(x => x.blockNum).ToList();
            }
            else if (searchMode == BlockSearchMode.random)
            {
                Random rnd = new Random();
                blockList = Node.blockChain.getBlocks(block_offset, (int)Node.blockChain.Count - block_offset).Where(x => x.powField == null).OrderBy(x => rnd.Next()).ToList();
            }
            // Check if the block list exists
            if (blockList == null)
            {
                Logging.error("No block list found while searching.");
                return null;
            }

            // Go through each block in the list
            foreach (Block block in blockList)
            {
                if (block.powField == null)
                {
                    ulong solved = 0;
                    lock (solvedBlocks)
                    {
                        solved = solvedBlocks.Find(x => x == block.blockNum);
                    }

                    // Check if this block is in the solved list
                    if (solved > 0)
                    {
                        // Do nothing at this point
                    }
                    else
                    {
                        // Block is not solved, select it
                        candidate_block = block;

                        if(searchMode == BlockSearchMode.random || searchMode == BlockSearchMode.randomLowestDifficulty)
                        {
                            lock (activePoolBlockLock)
                            {
                                activePoolBlock = candidate_block;
                            }
                        }
                        break;
                    }
                }
            }

            return candidate_block;
        }

        // Returns the most recent block without a PoW flag in the redacted blockchain
        private void searchForBlock()
        {
            lock (solvedBlocks)
            {
                List<ulong> tmpSolvedBlocks = new List<ulong>(solvedBlocks);
                foreach (ulong blockNum in tmpSolvedBlocks)
                {
                    Block b = Node.blockChain.getBlock(blockNum, false, false);
                    if (b == null || b.powField != null)
                    {
                        solvedBlocks.Remove(blockNum);
                    }
                }
            }

            Block candidate_block = null;

            List<Block> blockList = null;

            int block_offset = 1;
            if(Node.blockChain.Count > (long)ConsensusConfig.getRedactedWindowSize())
            {
                block_offset = 1000;
            }

            if (searchMode == BlockSearchMode.lowestDifficulty)
            {
                blockList = Node.blockChain.getBlocks(block_offset, (int)Node.blockChain.Count - block_offset).Where(x => x.powField == null).OrderBy(x => x.difficulty).ToList();
            }
            else if (searchMode == BlockSearchMode.randomLowestDifficulty)
            {
                Random rnd = new Random();
                blockList = Node.blockChain.getBlocks(block_offset, (int)Node.blockChain.Count - block_offset).Where(x => x.powField == null).OrderBy(x => x.difficulty).Skip(rnd.Next(500)).ToList();
            }
            else if (searchMode == BlockSearchMode.latestBlock)
            {
                blockList = Node.blockChain.getBlocks(block_offset, (int)Node.blockChain.Count - block_offset).Where(x => x.powField == null).OrderByDescending(x => x.blockNum).ToList();
            }
            else if (searchMode == BlockSearchMode.random)
            {
                Random rnd = new Random();
                blockList = Node.blockChain.getBlocks(block_offset, (int)Node.blockChain.Count - block_offset).Where(x => x.powField == null).OrderBy(x => rnd.Next()).ToList();
            }

            // Check if the block list exists
            if (blockList == null)
            {
                Logging.error("No block list found while searching. Likely an incorrect miner block search mode.");
                return;
            }

            // Go through each block in the list
            foreach (Block block in blockList)
            {
                if (block.powField == null)
                {
                    ulong solved = 0;
                    lock(solvedBlocks)
                    {
                        solved = solvedBlocks.Find(x => x == block.blockNum);
                    }

                    // Check if this block is in the solved list
                    if (solved > 0)
                    {
                        // Do nothing at this point
                    }
                    else
                    {
                        // Block is not solved, select it
                        candidate_block = block;
                        break;
                    }
                }

            }

            if (candidate_block == null)
            {
                // No blocks with empty PoW field found, wait a bit
                Thread.Sleep(1000);
                return;
            }

            currentBlockNum = candidate_block.blockNum;
            currentBlockDifficulty = candidate_block.difficulty;
            currentBlockVersion = candidate_block.version;
            currentHashCeil = MiningUtils.getHashCeilFromDifficulty(currentBlockDifficulty);

            activeBlock = candidate_block;
            byte[] block_checksum = activeBlock.blockChecksum;
            byte[] solver_address = IxianHandler.getWalletStorage().getPrimaryAddress();
            activeBlockChallenge = new byte[block_checksum.Length + solver_address.Length];
            System.Buffer.BlockCopy(block_checksum, 0, activeBlockChallenge, 0, block_checksum.Length);
            System.Buffer.BlockCopy(solver_address, 0, activeBlockChallenge, block_checksum.Length, solver_address.Length);

            blockFound = true;

            return;
        }

        private byte[] randomNonce(int length)
        {
            if (curNonce == null)
            {
                curNonce = new byte[length];
                lock (random)
                {
                    random.NextBytes(curNonce);
                }
            }
            bool inc_next = true;
            length = curNonce.Length;
            for (int pos = length - 1; inc_next == true && pos > 0; pos--)
            {
                if (curNonce[pos] < 0xFF)
                {
                    inc_next = false;
                    curNonce[pos]++;
                }
                else
                {
                    curNonce[pos] = 0;
                }
            }
            return curNonce;
        }

        // Expand a provided nonce up to expand_length bytes by appending a suffix of fixed-value bytes
        private static byte[] expandNonce(byte[] nonce, int expand_length)
        {
            if (dummyExpandedNonce == null)
            {
                dummyExpandedNonce = new byte[expand_length];
                for (int i = 0; i < dummyExpandedNonce.Length; i++)
                {
                    dummyExpandedNonce[i] = 0x23;
                }
            }

            // set dummy with nonce
            for (int i = 0; i < nonce.Length; i++)
            {
                dummyExpandedNonce[i] = nonce[i];
            }

            // clear any bytes from last nonce
            for(int i = nonce.Length; i < lastNonceLength; i++)
            {
                dummyExpandedNonce[i] = 0x23;
            }

            lastNonceLength = nonce.Length;

            return dummyExpandedNonce;
        }

        private void calculatePow_v1(byte[] hash_ceil)
        {
            // PoW = Argon2id( BlockChecksum + SolverAddress, Nonce)
            byte[] nonce = ASCIIEncoding.ASCII.GetBytes(ASCIIEncoding.ASCII.GetString(randomNonce(64)));
            byte[] hash = Argon2id.getHash(activeBlockChallenge, nonce, 1, 1024, 2);

            if (hash.Length < 1)
            {
                Logging.error("Stopping miner due to invalid hash.");
                stop();
                return;
            }

            hashesPerSecond++;

            // We have a valid hash, update the corresponding block
            if (Miner.validateHashInternal_v1(hash, hash_ceil) == true)
            {
                Logging.info(String.Format("SOLUTION FOUND FOR BLOCK #{0}", activeBlock.blockNum));

                // Broadcast the nonce to the network
                sendSolution(nonce);

                // Add this block number to the list of solved blocks
                lock (solvedBlocks)
                {
                    solvedBlocks.Add(activeBlock.blockNum);
                    solvedBlockCount++;
                }

                lastSolvedBlockNum = activeBlock.blockNum;
                lastSolvedTime = DateTime.UtcNow;

                // Reset the block found flag so we can search for another block
                blockFound = false;
            }
        }

        private void calculatePow_v2(byte[] hash_ceil)
        {
            // PoW = Argon2id( BlockChecksum + SolverAddress, Nonce)
            byte[] nonce = randomNonce(64);
            byte[] hash = Argon2id.getHash(activeBlockChallenge, nonce, 1, 1024, 2);

            if (hash.Length < 1)
            {
                Logging.error("Stopping miner due to invalid hash.");
                stop();
                return;
            }

            hashesPerSecond++;

            // We have a valid hash, update the corresponding block
            if (Miner.validateHashInternal_v2(hash, hash_ceil) == true)
            {
                Logging.info(String.Format("SOLUTION FOUND FOR BLOCK #{0}", activeBlock.blockNum));

                // Broadcast the nonce to the network
                sendSolution(nonce);

                // Add this block number to the list of solved blocks
                lock (solvedBlocks)
                {
                    solvedBlocks.Add(activeBlock.blockNum);
                    solvedBlockCount++;
                }

                lastSolvedBlockNum = activeBlock.blockNum;
                lastSolvedTime = DateTime.UtcNow;

                // Reset the block found flag so we can search for another block
                blockFound = false;
            }
        }


        private void calculatePow_v3(byte[] hash_ceil)
        {
            // PoW = Argon2id( BlockChecksum + SolverAddress, Nonce)
            byte[] nonce_bytes = randomNonce(64);
            byte[] fullnonce = expandNonce(nonce_bytes, 234236);

            byte[] hash = Argon2id.getHash(activeBlockChallenge, fullnonce, 2, 2048, 2);

            if (hash.Length < 1)
            {
                Logging.error("Stopping miner due to invalid hash.");
                stop();
                return;
            }

            hashesPerSecond++;

            // We have a valid hash, update the corresponding block
            if (Miner.validateHashInternal_v2(hash, hash_ceil) == true)
            {
                Logging.info(String.Format("SOLUTION FOUND FOR BLOCK #{0}", activeBlock.blockNum));

                // Broadcast the nonce to the network
                sendSolution(nonce_bytes);

                // Add this block number to the list of solved blocks
                lock (solvedBlocks)
                {
                    solvedBlocks.Add(activeBlock.blockNum);
                    solvedBlockCount++;
                }

                lastSolvedBlockNum = activeBlock.blockNum;
                lastSolvedTime = DateTime.UtcNow;

                // Reset the block found flag so we can search for another block
                blockFound = false;
            }
        }

        // difficulty is number of consecutive starting bits which must be 0 in the calculated hash
        public static byte[] setDifficulty_v0(int difficulty)
        {
            if (difficulty < 14)
            {
                difficulty = 14;
            }
            if (difficulty > 256)
            {
                difficulty = 256;
            }
            List<byte> diff_temp = new List<byte>();
            while (difficulty >= 8)
            {
                diff_temp.Add(0xff);
                difficulty -= 8;
            }
            if (difficulty > 0)
            {
                byte lastbyte = (byte)(0xff << (8 - difficulty));
                diff_temp.Add(lastbyte);
            }
            return diff_temp.ToArray();
        }

        // Check if a hash is valid based on the current difficulty
        public static bool validateHash_v0(string hash, ulong difficulty = 0)
        {
            // Set the difficulty for verification purposes   
            byte[] hashStartDifficulty = setDifficulty_v0((int)difficulty);
            
            if (hash.Length < hashStartDifficulty.Length)
            {
                return false;
            }

            for (int i = 0; i < hashStartDifficulty.Length; i++)
            {
                byte hash_byte = byte.Parse(hash.Substring(2 * i, 2), System.Globalization.NumberStyles.HexNumber);
                if ((hash_byte & hashStartDifficulty[i]) != 0)
                {
                    return false;
                }
            }

            return true;
        }

        private static bool validateHashInternal_v1(byte[] hash, byte[] hash_ceil)
        {
            if(hash == null || hash.Length < 1)
            {
                return false;
            }
            for (int i = 0; i < hash.Length; i++)
            {
                byte cb = i < hash_ceil.Length ? hash_ceil[i] : (byte)0xff;
                if (hash_ceil[i] > hash[i]) return true;
                if (hash_ceil[i] < hash[i]) return false;
            }
            // if we reach this point, the hash is exactly equal to the ceiling we consider this a 'passing hash'
            return true;
        }

        private static bool validateHashInternal_v2(byte[] hash, byte[] hash_ceil)
        {
            if (hash == null || hash.Length < 32)
            {
                return false;
            }
            for (int i = 0; i < hash.Length; i++)
            {
                byte cb = i < hash_ceil.Length ? hash_ceil[i] : (byte)0xff;
                if (cb > hash[i]) return true;
                if (cb < hash[i]) return false;
            }
            // if we reach this point, the hash is exactly equal to the ceiling we consider this a 'passing hash'
            return true;
        }

        // Check if a hash is valid based on the current difficulty
        public static bool validateHash_v1(byte[] hash, ulong difficulty)
        {
            return validateHashInternal_v1(hash, MiningUtils.getHashCeilFromDifficulty(difficulty));
        }

        public static bool validateHash_v2(byte[] hash, ulong difficulty)
        {
            return validateHashInternal_v2(hash, MiningUtils.getHashCeilFromDifficulty(difficulty));
        }

        // Verify nonce
        public static bool verifyNonce_v0(string nonce, ulong block_num, byte[] solver_address, ulong difficulty)
        {
            if (nonce == null || nonce.Length < 1)
            {
                return false;
            }

            Block block = Node.blockChain.getBlock(block_num, false, false);
            if (block == null)
                return false;

            // TODO checksum the solver_address just in case it's not valid
            // also protect against spamming with invalid nonce/block_num
            Byte[] p1 = new Byte[block.blockChecksum.Length + solver_address.Length];
            System.Buffer.BlockCopy(block.blockChecksum, 0, p1, 0, block.blockChecksum.Length);
            System.Buffer.BlockCopy(solver_address, 0, p1, block.blockChecksum.Length, solver_address.Length);

            byte[] nonce_bytes = ASCIIEncoding.ASCII.GetBytes(nonce);
            string hash = Argon2id.getHashString(p1, nonce_bytes, 1, 1024, 4);

            if (Miner.validateHash_v0(hash, difficulty) == true)
            {
                // Hash is valid
                return true;
            }

            return false;
        }

        // Verify nonce
        public static bool verifyNonce_v1(string nonce, ulong block_num, byte[] solver_address, ulong difficulty)
        {
            if(nonce == null || nonce.Length < 1)
            {
                return false;
            }

            Block block = Node.blockChain.getBlock(block_num, false, false);
            if (block == null)
                return false;

            // TODO checksum the solver_address just in case it's not valid
            // also protect against spamming with invalid nonce/block_num
            byte[] p1 = new byte[block.blockChecksum.Length + solver_address.Length];
            System.Buffer.BlockCopy(block.blockChecksum, 0, p1, 0, block.blockChecksum.Length);
            System.Buffer.BlockCopy(solver_address, 0, p1, block.blockChecksum.Length, solver_address.Length);

            byte[] nonce_bytes = ASCIIEncoding.ASCII.GetBytes(nonce);
            byte[] hash = Argon2id.getHash(p1, nonce_bytes, 1, 1024, 2);

            if (Miner.validateHash_v1(hash, difficulty) == true)
            {
                // Hash is valid
                return true;
            }

            return false;
        }

        // Verify nonce
        public static bool verifyNonce_v2(string nonce, ulong block_num, byte[] solver_address, ulong difficulty)
        {
            if (nonce == null || nonce.Length < 1 || nonce.Length > 128)
            {
                return false;
            }

            Block block = Node.blockChain.getBlock(block_num, false, false);
            if (block == null)
                return false;

            // TODO checksum the solver_address just in case it's not valid
            // also protect against spamming with invalid nonce/block_num
            byte[] p1 = new byte[block.blockChecksum.Length + solver_address.Length];
            System.Buffer.BlockCopy(block.blockChecksum, 0, p1, 0, block.blockChecksum.Length);
            System.Buffer.BlockCopy(solver_address, 0, p1, block.blockChecksum.Length, solver_address.Length);

            byte[] nonce_bytes = Crypto.stringToHash(nonce);
            byte[] hash = Argon2id.getHash(p1, nonce_bytes, 1, 1024, 2);

            if (Miner.validateHash_v2(hash, difficulty) == true)
            {
                // Hash is valid
                return true;
            }

            return false;
        }

        // Verify nonce
        public static bool verifyNonce_v3(string nonce, ulong block_num, byte[] solver_address, ulong difficulty)
        {
            if (nonce == null || nonce.Length < 1 || nonce.Length > 128)
            {
                return false;
            }

            Block block = Node.blockChain.getBlock(block_num, false, false);
            if (block == null)
                return false;

            // TODO protect against spamming with invalid nonce/block_num
            byte[] p1 = new byte[block.blockChecksum.Length + solver_address.Length];
            System.Buffer.BlockCopy(block.blockChecksum, 0, p1, 0, block.blockChecksum.Length);
            System.Buffer.BlockCopy(solver_address, 0, p1, block.blockChecksum.Length, solver_address.Length);

            byte[] nonce_bytes = Crypto.stringToHash(nonce);
            byte[] fullnonce = expandNonce(nonce_bytes, 234236);
            byte[] hash = Argon2id.getHash(p1, fullnonce, 2, 2048, 2);

            if (Miner.validateHash_v2(hash, difficulty) == true)
            {
                // Hash is valid
                return true;
            }

            return false;
        }

        // Submit solution with a provided blocknum
        // This is normally called from the API, as it is a static function
        public static bool sendSolution(byte[] nonce, ulong blocknum)
        {
            WalletStorage ws = IxianHandler.getWalletStorage();
            byte[] pubkey = ws.getPrimaryPublicKey();
            // Check if this wallet's public key is already in the WalletState
            Wallet mywallet = Node.walletState.getWallet(ws.getPrimaryAddress());
            if (mywallet.publicKey != null && mywallet.publicKey.SequenceEqual(pubkey))
            {
                // Walletstate public key matches, we don't need to send the public key in the transaction
                pubkey = null;
            }

            byte[] data = null;

            using (MemoryStream mw = new MemoryStream())
            {
                using (BinaryWriter writerw = new BinaryWriter(mw))
                {
                    writerw.Write(blocknum);
                    writerw.Write(Crypto.hashToString(nonce));                   
                    data = mw.ToArray();
                }
            }

            Transaction tx = new Transaction((int)Transaction.Type.PoWSolution, new IxiNumber(0), new IxiNumber(0), ConsensusConfig.ixianInfiniMineAddress, IxianHandler.getWalletStorage().getPrimaryAddress(), data, pubkey, Node.blockChain.getLastBlockNum());

            if (TransactionPool.addTransaction(tx))
            {
                PendingTransactions.addPendingLocalTransaction(tx);
            }
            else
            {
                Logging.error("An unknown error occurred while sending API PoW solution.");
                return false;
            }

            return true;
        }

        // Broadcasts the solution to the network
        public void sendSolution(byte[] nonce)
        {
            WalletStorage ws = IxianHandler.getWalletStorage();
            byte[] pubkey = ws.getPrimaryPublicKey();
            // Check if this wallet's public key is already in the WalletState
            Wallet mywallet = Node.walletState.getWallet(ws.getPrimaryAddress());
            if (mywallet.publicKey != null && mywallet.publicKey.SequenceEqual(pubkey))
            {
                // Walletstate public key matches, we don't need to send the public key in the transaction
                pubkey = null;
            }

            byte[] data = null;

            using (MemoryStream mw = new MemoryStream())
            {
                using (BinaryWriter writerw = new BinaryWriter(mw))
                {
                    writerw.Write(activeBlock.blockNum);
                    writerw.Write(Crypto.hashToString(nonce));
                    data = mw.ToArray();
                }
            }

            Transaction tx = new Transaction((int)Transaction.Type.PoWSolution, new IxiNumber(0), new IxiNumber(0), ConsensusConfig.ixianInfiniMineAddress, IxianHandler.getWalletStorage().getPrimaryAddress(), data, pubkey, Node.blockChain.getLastBlockNum());

            if (TransactionPool.addTransaction(tx))
            {
                PendingTransactions.addPendingLocalTransaction(tx);
            }
            else
            {
                Logging.error("An unknown error occurred while sending PoW solution.");
            }
        }

        // Output the miner status
        private void printMinerStatus()
        {
            // Console.WriteLine("Miner: Block #{0} | Hashes per second: {1}", currentBlockNum, hashesPerSecond);
            lastStatTime = DateTime.UtcNow;
            lastHashRate = hashesPerSecond / 5;
            hashesPerSecond = 0;
        }

        // Returns the number of locally solved blocks
        public long getSolvedBlocksCount()
        {
            lock(solvedBlocks)
            {
                return solvedBlockCount;
            }
        }

        // Returns the number of empty and full blocks, based on PoW field
        public List<int> getBlocksCount()
        {
            int empty_blocks = 0;
            int full_blocks = 0;

            ulong lastBlockNum = Node.blockChain.getLastBlockNum();
            ulong oldestRedactedBlock = 0;
            if (lastBlockNum > ConsensusConfig.getRedactedWindowSize())
                oldestRedactedBlock = lastBlockNum - ConsensusConfig.getRedactedWindowSize();

            for (ulong i = lastBlockNum; i > oldestRedactedBlock; i--)
            {
                Block block = Node.blockChain.getBlock(i, false, false);
                if(block == null)
                {
                    continue;
                }
                if (block.powField == null)
                {
                    empty_blocks++;
                }
                else
                {
                    full_blocks++;
                }
            }
            List<int> result = new List<int>();
            result.Add(empty_blocks);
            result.Add(full_blocks);
            return result;
        }

        // Returns the relative time since the last block was solved
        public string getLastSolvedBlockRelativeTime()
        {
            if (lastSolvedTime == DateTime.MinValue)
                return "Never";

            return Clock.getRelativeTime(lastSolvedTime);
        }

        public void test()
        {
            while (1 == 1)
            {
                byte[] nonce = ASCIIEncoding.ASCII.GetBytes(ASCIIEncoding.ASCII.GetString(randomNonce(64)));
                byte[] hash = Argon2id.getHash(new byte[3]{ 1, 2, 3 }, nonce, 1, 1024, 2);

                // We have a valid hash, update the corresponding block
                if (Miner.validateHashInternal_v1(hash, BitConverter.GetBytes(80)) == true)
                {
                    byte[] data = null;

                    using (MemoryStream mw = new MemoryStream())
                    {
                        using (BinaryWriter writerw = new BinaryWriter(mw))
                        {
                            string nonce_hex = ASCIIEncoding.ASCII.GetString(nonce);
                            writerw.Write(nonce_hex);
                            data = mw.ToArray();
                        }
                    }

                    string nonce_str = "";

                    // Extract the block number and nonce
                    using (MemoryStream m = new MemoryStream(data))
                    {
                        using (BinaryReader reader = new BinaryReader(m))
                        {
                            nonce_str = reader.ReadString();
                        }
                    }

                    byte[] nonce_bytes = ASCIIEncoding.ASCII.GetBytes(nonce_str);
                    byte[] hash_to_test = Argon2id.getHash(new byte[3] { 1, 2, 3 }, nonce_bytes, 1, 1024, 2);

                    if (Miner.validateHashInternal_v1(hash_to_test, BitConverter.GetBytes(80)) == true)
                    {
                        // Hash is valid
                        Logging.error("Found correct PoW");
                        //break;
                    }else
                    {
                        Logging.error("PoW solution incorrect");
                    }
                }
            }
        }
    }
}
