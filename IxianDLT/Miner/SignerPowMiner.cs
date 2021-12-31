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
using System.Threading;

namespace DLT
{
    class SignerPowMiner
    {
        public bool pause = false; // Flag to toggle miner activity

        Block activeBlock = null;
        public ulong currentBlockNum = 0; // Mining block number
        public ulong lastSolvedBlockNum = 0; // Last solved block number
        public SignerPowSolution lastSignerPowSolution = null;
        private long startedSolvingTime = 0; // Started solving time
        private ulong solvingDifficulty = 0;

        public long lastHashRate = 0; // Last reported hash rate
        private long hashesPerSecond = 0; // Total number of hashes per second
        private DateTime lastStatTime; // Last statistics output time

        private bool started = false;
        private bool shouldStop = false; // flag to signal shutdown of threads


        bool blockFound = false;

        [ThreadStatic] private static byte[] activeBlockChallenge = null;
        [ThreadStatic] private static ulong activeBlockChallengeBlockNum = 0;

        private static Random random = new Random(); // used to seed initial curNonce's
        [ThreadStatic] private static byte[] curNonce = null; // Used for random nonce

        private static long solutionsFound = 0;


        private object sendSolutionLock = new object();

        public SignerPowMiner()
        {
            lastStatTime = DateTime.UtcNow;

        }

        // Starts the mining threads
        public bool start()
        {
            if(started)
            {
                return false;
            }
            
            started = true;

            // Calculate the allowed number of threads based on logical processor count
            uint miningThreads = calculateMiningThreadsCount();
            Logging.info("Starting Presence List miner with {0} threads on {1} logical processors.", miningThreads, Environment.ProcessorCount);

            shouldStop = false;

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
                    if (blockFound == false)
                    {
                        searchForBlock();
                    }
                    else
                    {
                        // TODO Omega increase diff when no block for a long time
                        calculatePow(solvingDifficulty);
                    }

                    // Output mining stats
                    TimeSpan timeSinceLastStat = DateTime.UtcNow - lastStatTime;
                    if (timeSinceLastStat.TotalSeconds > 5)
                    {
                        lastStatTime = DateTime.UtcNow;
                        lastHashRate = hashesPerSecond / 5;
                        hashesPerSecond = 0;
                    }
                }catch(Exception e)
                {
                    Thread.Sleep(500);
                    Logging.error("Exception occured in SignerPowMiner.threadLoop(): " + e);
                }
            }
        }

        private void secondaryThreadLoop()
        {
            while (!shouldStop)
            {
                try
                {
                    if (blockFound == false)
                    {
                        Thread.Sleep(500);
                        continue;
                    }
                    // TODO Omega increase diff when no block for a long time
                    calculatePow(solvingDifficulty);
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
            blockFound = false;
        }


        // Returns the most recent fully accepted block
        private void searchForBlock()
        {
            Block candidateBlock = IxianHandler.getLastBlock();

            if (candidateBlock == null)
            {
                // Not ready yet
                Thread.Sleep(1000);
                return;
            }

            if (pause
                || PresenceList.myPresenceType == 'W'
                || candidateBlock.version < BlockVer.v10)
            {
                lastStatTime = DateTime.UtcNow;
                lastHashRate = hashesPerSecond;
                hashesPerSecond = 0;
                Thread.Sleep(500);
                return;
            }

            if (candidateBlock.blockNum > 7)
            {
                candidateBlock = Node.blockChain.getBlock(candidateBlock.blockNum - 7, true, true);
            }

            if (candidateBlock == null)
            {
                // Not ready yet
                Thread.Sleep(1000);
                return;
            }

            ulong highestNetworkBlockHeight = IxianHandler.getHighestKnownNetworkBlockHeight();

            if (candidateBlock.blockNum + 50 < highestNetworkBlockHeight)
            {
                // Not ready yet
                Thread.Sleep(1000);
                return;
            }

            // TODO Omega mine for 10 minutes, send the best solution
            // TODO Omega make sure to find a min. required difficulty and mine for longer than 10 minutes if necessary; needs computing total PoW of the previously discarded block height
            // TODO Omega handle below if according, to the above 2 TODOs
            if (highestNetworkBlockHeight > 50 &&
                lastSolvedBlockNum + 7 + ConsensusConfig.plPowCalculationInterval > highestNetworkBlockHeight)
            {
                // Not ready yet
                Thread.Sleep(1000);
                return;
            }

            startedSolvingTime = Clock.getTimestamp();

            solvingDifficulty = Node.blockChain.getMinSignerPowDifficulty();
            if(solvingDifficulty < 10000)
            {
                solvingDifficulty = 10000;
            }

            activeBlock = candidateBlock;
            currentBlockNum = candidateBlock.blockNum;

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

        private void calculatePow(ulong difficulty)
        {
            // PoW = sha512sq( BlockChecksum + SolverAddress + Nonce)
            byte[] nonce = randomNonce(64);
            if(activeBlockChallengeBlockNum != currentBlockNum)
            {
                byte[] blockChecksum = activeBlock.blockChecksum;
                byte[] solverAddress = IxianHandler.getWalletStorage().getPrimaryAddress();
                activeBlockChallengeBlockNum = currentBlockNum;
                activeBlockChallenge = new byte[blockChecksum.Length + solverAddress.Length + 64];
                System.Buffer.BlockCopy(blockChecksum, 0, activeBlockChallenge, 0, blockChecksum.Length);
                System.Buffer.BlockCopy(solverAddress, 0, activeBlockChallenge, blockChecksum.Length, solverAddress.Length);
            }
            System.Buffer.BlockCopy(nonce, 0, activeBlockChallenge, activeBlockChallenge.Length - 64, nonce.Length);
            byte[] hash = Crypto.sha512sq(activeBlockChallenge, 0, 0);
            if (hash.Length < 1)
            {
                Logging.error("Stopping signing miner due to invalid hash.");
                stop();
                return;
            }

            hashesPerSecond++;

            // We have a valid hash, update the corresponding block
            if (SignerPowSolution.validateHash(hash, difficulty) == true)
            {
                Logging.info("SOLUTION FOUND FOR BLOCK #{0} - {1} > {2} - {3}", activeBlock.blockNum, SignerPowSolution.hashToDifficulty(hash), difficulty, Crypto.hashToString(hash));

                // Broadcast the nonce to the network
                handleFoundSolution(nonce, hash, difficulty);

                solutionsFound++;

                lastSolvedBlockNum = activeBlock.blockNum;
                solvingDifficulty = SignerPowSolution.hashToDifficulty(hash) + 1;
                if(Clock.getTimestamp() - startedSolvingTime > ConsensusConfig.plPowMinCalculationTime)
                {
                    // Reset the block found flag so we can search for another block
                    blockFound = false;
                }
            }
        }

        // Broadcasts the solution to the network
        public void handleFoundSolution(byte[] nonce, byte[] hash, ulong difficulty)
        {
            lock(sendSolutionLock)
            {
                if(lastSignerPowSolution != null 
                    && (activeBlockChallengeBlockNum < lastSignerPowSolution.blockNum || (activeBlockChallengeBlockNum == lastSignerPowSolution.blockNum && difficulty <= lastSignerPowSolution.difficulty)))
                {
                    return;
                }
                byte[] nonceCopy = new byte[nonce.Length];
                Array.Copy(nonce, nonceCopy, nonce.Length);
                SignerPowSolution signerPow = new SignerPowSolution(IxianHandler.primaryWalletAddress)
                {
                    blockNum = activeBlockChallengeBlockNum,
                    solution = nonceCopy
                };
                lastSignerPowSolution = signerPow;
                PresenceList.setPowSolution(signerPow);
            }
        }

        public void test()
        {
            WalletStorage ws = IxianHandler.getWalletStorage();
            start();
            Block b = new Block() { version = Block.maxVersion, blockProposer = ws.getPrimaryAddress(), blockNum = 1, difficulty = 0x00ff000000000000 };
            b.blockChecksum = b.calculateChecksum();
            b.walletStateChecksum = Node.walletState.calculateWalletStateChecksum();
            b.applySignature();
            byte[] sf1 = b.calculateSignatureChecksum();
            Node.blockChain.appendBlock(b);

            b = new Block() { version = Block.maxVersion, blockProposer = ws.getPrimaryAddress(), blockNum = 2, lastBlockChecksum = b.blockChecksum, difficulty = 0x00ff000000000000, signatureFreezeChecksum = sf1 };
            b.blockChecksum = b.calculateChecksum();
            b.applySignature();
            byte[] sf2 = b.calculateSignatureChecksum();
            Node.blockChain.appendBlock(b);

            b = new Block() { version = Block.maxVersion, blockProposer = ws.getPrimaryAddress(), blockNum = 3, lastBlockChecksum = b.blockChecksum, difficulty = 0x00ff000000000000, signatureFreezeChecksum = sf1 };
            b.blockChecksum = b.calculateChecksum();
            b.applySignature();
            byte[] sf3 = b.calculateSignatureChecksum();
            Node.blockChain.appendBlock(b);

            b = new Block() { version = Block.maxVersion, blockProposer = ws.getPrimaryAddress(), blockNum = 4, lastBlockChecksum = b.blockChecksum, difficulty = 0x00ff000000000000, signatureFreezeChecksum = sf1 };
            b.blockChecksum = b.calculateChecksum();
            b.applySignature();
            byte[] sf4 = b.calculateSignatureChecksum();
            Node.blockChain.appendBlock(b);

            b = new Block() { version = Block.maxVersion, blockProposer = ws.getPrimaryAddress(), blockNum = 5, lastBlockChecksum = b.blockChecksum, difficulty = 0xff00000000000000, signatureFreezeChecksum = sf1 };
            b.blockChecksum = b.calculateChecksum();
            b.applySignature();
            byte[] sf5 = b.calculateSignatureChecksum();
            Node.blockChain.appendBlock(b);

            b = new Block() { version = Block.maxVersion, blockProposer = ws.getPrimaryAddress(), blockNum = 6, lastBlockChecksum = b.blockChecksum, difficulty = 0x00ff000000000000, signatureFreezeChecksum = sf1 };
            b.blockChecksum = b.calculateChecksum();
            b.applySignature();
            byte[] sf6 = b.calculateSignatureChecksum();
            Node.blockChain.appendBlock(b);

            b = new Block() { version = Block.maxVersion, blockProposer = ws.getPrimaryAddress(), blockNum = 7, lastBlockChecksum = b.blockChecksum, difficulty = 0x00ff000000000000, signatureFreezeChecksum = sf2 };
            b.blockChecksum = b.calculateChecksum();
            b.applySignature();
            Node.blockChain.appendBlock(b);

            b = new Block() { version = Block.maxVersion, blockProposer = ws.getPrimaryAddress(), blockNum = 8, lastBlockChecksum = b.blockChecksum, difficulty = 0x00ff000000000000, signatureFreezeChecksum = sf3 };
            b.blockChecksum = b.calculateChecksum();
            b.applySignature();
            Node.blockChain.appendBlock(b);

            b = new Block() { version = Block.maxVersion, blockProposer = ws.getPrimaryAddress(), blockNum = 9, lastBlockChecksum = b.blockChecksum, difficulty = 0x00ff000000000000, signatureFreezeChecksum = sf4 };
            b.blockChecksum = b.calculateChecksum();
            b.applySignature();
            Node.blockChain.appendBlock(b);

            b = new Block() { version = Block.maxVersion, blockProposer = ws.getPrimaryAddress(), blockNum = 10, lastBlockChecksum = b.blockChecksum, difficulty = 0x00ff000000000000, signatureFreezeChecksum = sf5 };
            b.blockChecksum = b.calculateChecksum();
            b.applySignature();
            Node.blockChain.appendBlock(b);

            b = new Block() { version = Block.maxVersion, blockProposer = ws.getPrimaryAddress(), blockNum = 11, lastBlockChecksum = b.blockChecksum, difficulty = 0x00ff0000000000001, signatureFreezeChecksum = sf6 };
            b.blockChecksum = b.calculateChecksum();
            b.applySignature();
            Node.blockChain.appendBlock(b);

            while (!shouldStop)
            {
                Logging.info("Solved block count: " + solutionsFound + ", " + lastHashRate + " h/s");
                Thread.Sleep(5000);
            }
        }
    }
}
