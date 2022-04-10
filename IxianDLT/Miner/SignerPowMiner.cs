// Copyright (C) 2017-2022 Ixian OU
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
using IXICore.Utils;
using System;
using System.Linq;
using System.Numerics;
using System.Security.Cryptography;
using System.Threading;

namespace DLT
{
    class SignerPowMiner
    {
        public bool pause = false; // Flag to toggle miner activity

        Block activeBlock = null;
        private ulong currentBlockNum = 0; // Mining block number
        public SignerPowSolution lastSignerPowSolution { get; private set; } = null;
        private long startedSolvingTime = 0; // Started solving time
        private BigInteger solvingDifficulty = 0;

        public ulong lastHashRate { get; private set; } = 0; // Last reported hash rate
        private ulong hashesPerSecond = 0; // Total number of hashes per second
        private DateTime lastStatTime; // Last statistics output time

        private bool started = false;
        private bool shouldStop = false; // flag to signal shutdown of threads


        bool blockReadyForMining = false;

        [ThreadStatic] private static byte[] activeBlockChallenge = null;
        [ThreadStatic] private static ulong activeBlockChallengeBlockNum = 0;

        [ThreadStatic] private static byte[] curNonce = null; // Used for random nonce

        public static long solutionsFound { get; private set; } = 0;

        public SignerPowMiner()
        {
            lastStatTime = DateTime.UtcNow;

        }

        // Starts the mining threads
        public bool start()
        {
            if (started)
            {
                return false;
            }

            started = true;

            // Calculate the allowed number of threads based on logical processor count
            uint miningThreads = calculateMiningThreadsCount() / 2;
            Logging.info("Starting Presence List miner with {0} threads on {1} logical processors.", miningThreads, Environment.ProcessorCount);

            shouldStop = false;

            // Start primary mining thread
            Thread manager_thread = new Thread(threadLoop);
            manager_thread.Name = "PresenceListMiner_Manager_Thread";
            manager_thread.Start();

            // Start secondary worker threads
            for (int i = 0; i < miningThreads; i++)
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
            started = false;
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



        private void threadLoop()
        {
            while (!shouldStop)
            {
                try
                {
                    searchForBlock();

                    // Output mining stats
                    TimeSpan timeSinceLastStat = DateTime.UtcNow - lastStatTime;
                    if (timeSinceLastStat.TotalSeconds > 5)
                    {
                        lastStatTime = DateTime.UtcNow;
                        lastHashRate = hashesPerSecond / (ulong)timeSinceLastStat.TotalSeconds;
                        hashesPerSecond = 0;
                    }

                    sendFoundSolution();
                }
                catch (Exception e)
                {
                    Logging.error("Exception occured in SignerPowMiner.threadLoop(): " + e);
                }
                Thread.Sleep(5000);
            }
        }

        private void secondaryThreadLoop()
        {
            while (!shouldStop)
            {
                try
                {
                    if (blockReadyForMining == false)
                    {
                        Thread.Sleep(500);
                        continue;
                    }

                    calculatePow();
                }
                catch (Exception e)
                {
                    Thread.Sleep(500);
                    Logging.error("Exception occured in SignerPowMiner.secondaryThreadLoop(): " + e);
                }
            }
        }

        public void forceSearchForBlock()
        {
            blockReadyForMining = false;
            lastSignerPowSolution = null;
        }


        // Returns the most recent fully accepted block
        private void searchForBlock()
        {
            Block candidateBlock = IxianHandler.getLastBlock();

            if (candidateBlock == null)
            {
                // No blocks, Not ready yet
                return;
            }

            if (pause
                || PresenceList.myPresenceType == 'W'
                || candidateBlock.version < BlockVer.v10)
            {
                // paused or not synced/worker node or lower than v10
                blockReadyForMining = false;
                currentBlockNum = 0;
                lastStatTime = DateTime.UtcNow;
                lastHashRate = hashesPerSecond;
                hashesPerSecond = 0;
                return;
            }

            if (candidateBlock.blockNum <= 14)
            {
                candidateBlock = Node.blockChain.getBlock(1, false, false);
            }
            else
            {
                candidateBlock = Node.blockChain.getBlock(candidateBlock.blockNum - 7, false, false);
                // TODO TODO Omega find first v10 block
            }

            if (candidateBlock == null)
            {
                // Not ready yet
                return;
            }

            solvingDifficulty = Node.blockChain.getMinSignerPowDifficulty(IxianHandler.getLastBlockHeight() + 1);

            if (solvingDifficulty < 0)
            {
                Logging.error("SignerPowMiner: Solving difficulty is negative.");
                return;
            }

            if (currentBlockNum == candidateBlock.blockNum
                && activeBlock.blockChecksum.SequenceEqual(candidateBlock.blockChecksum))
            {
                // already mining this block
                return;
            }

            ulong highestNetworkBlockHeight = IxianHandler.getHighestKnownNetworkBlockHeight();
            if (candidateBlock.blockNum + ConsensusConfig.plPowCalculationInterval < highestNetworkBlockHeight)
            {
                // Catching up to the network
                return;
            }

            if (lastSignerPowSolution != null
                && Node.blockChain.getTimeSinceLastBlock() < 1800
                && currentBlockNum + ConsensusConfig.plPowCalculationInterval + ConsensusConfig.plPowMinCalculationBlockTime > highestNetworkBlockHeight)
            {
                // If the chain isn't stuck and we've already processed PoW within the interval
                return;
            }

            startedSolvingTime = Clock.getTimestamp();

            activeBlock = candidateBlock;
            currentBlockNum = candidateBlock.blockNum;

            blockReadyForMining = true;

            return;
        }

        private byte[] randomNonce(int length)
        {
            if (curNonce == null)
            {
                curNonce = new byte[length];
                RandomNumberGenerator.Create().GetBytes(curNonce);
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

        [ThreadStatic] private Random rnd = null;
        private int rndInt = 10000 - new Random().Next(100);
        private void calculatePow()
        {
            // TODO TODO Omega remove this, for testing purposes only
            if (rnd == null)
            {
                rnd = new Random();
            }
            int rndNr = rnd.Next(10000);
            if (rndNr > rndInt)
            {
                Thread.Sleep(10);
            }

            // PoW = sha3_512sq(BlockNum + BlockChecksum + SolverAddress + Nonce)
            byte[] nonce = randomNonce(64);
            if (activeBlockChallengeBlockNum != currentBlockNum)
            {
                var block = activeBlock;
                byte[] blockNumBytes = block.blockNum.GetIxiVarIntBytes();
                byte[] blockChecksum = block.blockChecksum;

                byte[] solverAddress = IxianHandler.getWalletStorage().getPrimaryAddress().addressNoChecksum;

                activeBlockChallengeBlockNum = currentBlockNum;
                activeBlockChallenge = new byte[blockNumBytes.Length + blockChecksum.Length + solverAddress.Length + 64];

                System.Buffer.BlockCopy(blockNumBytes, 0, activeBlockChallenge, 0, blockNumBytes.Length);
                System.Buffer.BlockCopy(blockChecksum, 0, activeBlockChallenge, blockNumBytes.Length, blockChecksum.Length);
                System.Buffer.BlockCopy(solverAddress, 0, activeBlockChallenge, blockNumBytes.Length + blockChecksum.Length, solverAddress.Length);
            }
            System.Buffer.BlockCopy(nonce, 0, activeBlockChallenge, activeBlockChallenge.Length - 64, nonce.Length);
            byte[] hash = CryptoManager.lib.sha3_512sqTrunc(activeBlockChallenge);

            hashesPerSecond++;

            handleFoundSolution(hash, nonce);
        }

        // Process found solution and pass it to main thread for further processing
        private void handleFoundSolution(byte[] hash, byte[] nonce)
        {
            // pre-validate hash
            if (hash[hash.Length - 1] != 0
                || hash[hash.Length - 2] != 0)
            {
                return;
            }

            BigInteger hashDifficulty = SignerPowSolution.hashToDifficulty(hash);

            if (hashDifficulty >= solvingDifficulty)
            {
                // valid hash
                var lastSolution = lastSignerPowSolution;
                if (lastSolution == null
                    || lastSolution.blockNum != activeBlockChallengeBlockNum
                    || (lastSolution.blockNum == activeBlockChallengeBlockNum && hashDifficulty > lastSolution.difficulty))
                {
                    Logging.info("SOLUTION FOUND FOR BLOCK #{0} - {1} > {2} - {3}", activeBlock.blockNum, hashDifficulty, solvingDifficulty, Crypto.hashToString(hash));

                    byte[] nonceCopy = new byte[nonce.Length];
                    Array.Copy(nonce, nonceCopy, nonce.Length);

                    SignerPowSolution signerPow = new SignerPowSolution(IxianHandler.primaryWalletAddress)
                    {
                        blockNum = activeBlockChallengeBlockNum,
                        solution = nonceCopy
                    };

                    lastSignerPowSolution = signerPow;
                    solvingDifficulty = hashDifficulty;
                }

                solutionsFound++;
            }
        }

        // Broadcasts the solution to the network
        private void sendFoundSolution()
        {
            var newSolution = lastSignerPowSolution;
            if (newSolution == null)
            {
                return;
            }

            // Check if we're mining for at least X minutes and that the blockchain isn't stuck
            if (Clock.getTimestamp() - startedSolvingTime > ConsensusConfig.plPowMinCalculationTime
                && Node.blockChain.getTimeSinceLastBlock() < 1800) // TODO move 1800 to CoreConfig
            {
                // Reset the blockReadyForMining, to stop mining on all threads
                blockReadyForMining = false;
            }

            var solution = PresenceList.getPowSolution();
            if (solution != null)
            {
                ulong lastBlockHeight = IxianHandler.getLastBlockHeight();

                if (newSolution.difficulty <= solution.difficulty
                    && solution.blockNum + ConsensusConfig.plPowBlocksValidity - ConsensusConfig.plPowMinCalculationBlockTime > lastBlockHeight
                    && solution.difficulty > solvingDifficulty
                )
                {
                    // If the new solution has a lower difficulty than the previously submitted solution and the previously submitted solution is still valid
                    return;
                }
            }

            PresenceList.setPowSolution(newSolution);
            // TODO TODO Omega - reapply signature with new solution on sigfreezed block up to the current block <---- do this only if the chain is stuck
        }
    }
}
