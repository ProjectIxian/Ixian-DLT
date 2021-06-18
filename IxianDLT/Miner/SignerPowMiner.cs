// Copyright (C) 2017-2021 Ixian OU
// This file is part of Ixian Core - www.github.com/ProjectIxian/Ixian-Core
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
using System.IO;
using System.Threading;

namespace DLT
{
    class SignerPowMiner
    {
        public bool pause = false; // Flag to toggle miner activity

        public long lastHashRate = 0; // Last reported hash rate
        public ulong currentBlockNum = 0; // Mining block number
        public int currentBlockVersion = 0;
        public ulong currentBlockDifficulty = 0; // Current block difficulty
        public byte[] currentHashCeil { get; private set; }
        public ulong lastSolvedBlockNum = 0; // Last solved block number
        private DateTime lastSolvedTime = DateTime.MinValue; // Last locally solved block time

        private long hashesPerSecond = 0; // Total number of hashes per second
        private DateTime lastStatTime; // Last statistics output time
        private bool shouldStop = false; // flag to signal shutdown of threads
        private ThreadLiveCheck TLC;


        Block activeBlock = null;
        bool blockFound = false;

        byte[] activeBlockChallenge = null;

        private static Random random = new Random(); // used to seed initial curNonce's
        [ThreadStatic] private static byte[] curNonce = null; // Used for random nonce

        private static long solvedBlockCount = 0;

        public SignerPowMiner()
        {
            lastStatTime = DateTime.UtcNow;

        }

        // Starts the mining threads
        public bool start()
        {
            // Calculate the allowed number of threads based on logical processor count
            uint miningThreads = calculateMiningThreadsCount();
            Logging.info("Starting Presence List miner with {0} threads on {1} logical processors.", miningThreads, Environment.ProcessorCount);

            shouldStop = false;

            TLC = new ThreadLiveCheck();
            // Start primary mining thread
            Thread miner_thread = new Thread(threadLoop);
            miner_thread.Name = "PresenceListMiner_Main_Thread";
            miner_thread.Start();

            // Start secondary worker threads
            for (int i = 0; i < miningThreads - 1; i++)
            {
                Thread worker_thread = new Thread(secondaryThreadLoop);
                worker_thread.Name = "PresenceListMiner_Worker_Thread_#" + i.ToString();
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
        public static uint calculateMiningThreadsCount()
        {
            uint vcpus = Config.cpuThreads;

            // Calculate the maximum number of threads allowed
            uint maxThreads = vcpus / 2;
            if (maxThreads < 1)
            {
                return 1;
            }

            // Provided mining thread count is allowed
            return maxThreads;
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
                    calculatePow(currentHashCeil);
                }

                // Output mining stats
                TimeSpan timeSinceLastStat = DateTime.UtcNow - lastStatTime;
                if (timeSinceLastStat.TotalSeconds > 5)
                {
                    lastStatTime = DateTime.UtcNow;
                    lastHashRate = hashesPerSecond / 5;
                    hashesPerSecond = 0;
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

                calculatePow(currentHashCeil);
            }
        }

        public void forceSearchForBlock()
        {
            blockFound = false;
        }


        // Returns the most recent fully accepted block
        private void searchForBlock()
        {
            Block candidate_block = Node.blockChain.getLastBlock();

            if (candidate_block == null)
            {
                // No blocks with empty PoW field found, wait a bit
                Thread.Sleep(1000);
                return;
            }

            if (candidate_block.blockNum > 6)
            {
                candidate_block = Node.blockChain.getBlock(candidate_block.blockNum - 6, true, true);
            }

            currentBlockNum = candidate_block.blockNum;
            currentBlockDifficulty = candidate_block.difficulty;
            currentBlockVersion = candidate_block.version;
            currentHashCeil = SignerPowSolution.getHashCeilFromDifficulty(currentBlockDifficulty);

            activeBlock = candidate_block;
            byte[] block_checksum = activeBlock.blockChecksum;
            byte[] solver_address = Node.walletStorage.getPrimaryAddress();
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

        private void calculatePow(byte[] hash_ceil)
        {
            // PoW = Argon2id( BlockChecksum + SolverAddress, Nonce)
            byte[] nonce_bytes = randomNonce(64);
            byte[] fullnonce = SignerPowSolution.expandNonce(nonce_bytes, 234234);

            byte[] hash = SignerPowSolution.getArgon2idHash(activeBlockChallenge, fullnonce);
            if (hash.Length < 1)
            {
                Logging.error("Stopping miner due to invalid hash.");
                stop();
                return;
            }

            hashesPerSecond++;

            // We have a valid hash, update the corresponding block
            if (SignerPowSolution.validateHash(hash, hash_ceil) == true)
            {
                Logging.info("SOLUTION FOUND FOR BLOCK #{0}", activeBlock.blockNum);

                // Broadcast the nonce to the network
                sendSolution(nonce_bytes);

                solvedBlockCount++;

                lastSolvedBlockNum = activeBlock.blockNum;
                lastSolvedTime = DateTime.UtcNow;

                // Reset the block found flag so we can search for another block
                blockFound = false;
            }
        }

        // Broadcasts the solution to the network
        public void sendSolution(byte[] nonce)
        {
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
        }

        public void test()
        {
            start();
            Block b = new Block() { blockNum = 11, blockChecksum = new byte[3] { 1, 2, 3 }, difficulty = 1 };
            Node.blockChain.appendBlock(b, false);
            while (!shouldStop)
            {
                Logging.info("Solved block count: " + solvedBlockCount + ", " + lastHashRate + " h/s");
                Thread.Sleep(5000);
            }
        }
    }
}
