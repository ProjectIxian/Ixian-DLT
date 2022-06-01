// Copyright (C) 2017-2022 Ixian OU
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

using DLT;
using DLT.Meta;
using DLT.Storage;
using IXICore;
using IXICore.Meta;
using IXICore.Utils;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DLTNode
{
    class Benchmark
    {
        private static Miner miner = null;

        private static DateTime lastStatTime; // Last statistics output time
        private static bool running = false; // Benchmark running flag

        private static PerformanceCounter cpuCounter;
        private static PerformanceCounter ramCounter;

        private static BenchmarkNode benchmarkNode;

        public static void start(string mode)
        {
            cpuCounter = new PerformanceCounter("Processor", "% Processor Time","_Total", true);
            if (IXICore.Platform.onMono() == false)
            {
                ramCounter = new PerformanceCounter("Memory", "Available Bytes", true);
            }
            else
            {
                ramCounter = new PerformanceCounter("Mono Memory", "Available Physical Memory", true);
            }

            lastStatTime = DateTime.UtcNow;
            running = true;

            switch (mode)
            {
                // Argon2id hashing benchmark
                case "argon2id":
                    benchmarkArgon2id();
                    break;

                // SHA hashing benchmark
                case "sha":
                    benchmarkSHA();
                    break;

                // Signing + verification benchmark
                case "rsa":
                    benchmarkRSA();
                    break;

                // Storage benchmark
                case "storage":
                    benchmarkStorage();
                    break;

                // Transaction object benchmark
                case "tx":
                    benchmarkTx();
                    break;

                // Key derivation generation benchmarks
                case "keys1024":
                    benchmarkKeyGeneration(1024);
                    break;
                case "keys2048":
                    benchmarkKeyGeneration(1024);
                    break;
                case "keys4096":
                    benchmarkKeyGeneration(1024);
                    break;

                default:
                    Logging.error("Unknown benchmark mode: {0}", mode);
                    return;
            }

            // Wait for benchmark to finish
            while (running)
            {
                Thread.Sleep(100);
            }

        }

        public static void printSystemStatus()
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();

            Process currentProcess = System.Diagnostics.Process.GetCurrentProcess();
            Logging.info("DLT Memory Usage:\t {0} bytes", currentProcess.WorkingSet64.ToString("N0"));

            Logging.info("Available Memory:\t {0} bytes", Convert.ToInt64(ramCounter.NextValue()).ToString("N0"));
            Logging.info("CPU Usage: {0}%", Convert.ToInt32(cpuCounter.NextValue()).ToString());
        }

        // Benchmark for argon2id hashing
        public static void benchmarkArgon2id()
        {
            miner = new Miner();

            Thread argond2id_thread = new Thread(argon2idThreadLoop);
            argond2id_thread.Name = "Benchmark_Argon2id_Thread";
            argond2id_thread.Start();
        }
        private static void argon2idThreadLoop()
        {
            miner.benchmark();
            running = false;
        }

        // Benchmark for SHA hashing
        public static void benchmarkSHA()
        {
            Thread sha_thread = new Thread(shaThreadLoop);
            sha_thread.Name = "Benchmark_SHA_Thread";
            sha_thread.Start();
        }

        private static void shaThreadLoop()
        {
            Logging.info("Starting SHA benchmark, single thread.");
            printSystemStatus();

            bool shouldStop = false;
            long lastHashRate = 0; // Last reported hash rate
            long hashesPerSecond = 0; // Total number of hashes per second
            int iterations = 0;
            int mode = 0; // Start with sha256

            Random rnd = new Random();
            Byte[] random_data = new Byte[48]; // 48 bytes worth of data

            lastStatTime = DateTime.UtcNow;

            while (!shouldStop)
            {
                byte[] hash = null;
                rnd.NextBytes(random_data); // Randomize content for each iteration

                // Set hashing mode
                switch (mode)
                {
                    case 0:
                        hash = Crypto.sha256(random_data);
                        break;
                    case 1:
                        hash = Crypto.sha512(random_data);
                        break;
                    case 2:
                        hash = Crypto.sha512sq(random_data);
                        break;
                    case 3:
                        hash = Crypto.sha512qu(random_data);
                        break;
                    default:
                        break;
                }

                if (hash.Length < 1)
                {
                    Logging.error("Stopping benchmark due to invalid SHA hash.");
                    running = false;
                    return;
                }

                hashesPerSecond++;

                TimeSpan timeSinceLastStat = DateTime.UtcNow - lastStatTime;
                if (timeSinceLastStat.TotalSeconds > 3)
                {
                    lastHashRate = (long)(hashesPerSecond / timeSinceLastStat.TotalSeconds);
                    hashesPerSecond = 0;
                    iterations++;
                    string mode_text = "SHA-256";
                    switch (mode)
                    {
                        case 0:
                            mode_text = "SHA-256";
                            break;
                        case 1:
                            mode_text = "SHA-512";
                            break;
                        case 2:
                            mode_text = "SHA-512SQ";
                            break;
                        case 3:
                            mode_text = "SHA-512QU";
                            break;
                        default:
                            break;
                    }

                    Logging.info("{0}: {1} h/s", mode_text, lastHashRate.ToString("N0"));
                    lastStatTime = DateTime.UtcNow;

                    // Run 5 iterations for each algorithm
                    if (iterations >= 5)
                    {
                        // Proceed to the next mode
                        mode++;
                        if (mode > 3)
                            shouldStop = true;
                        iterations = 0;
                        hashesPerSecond = 0;
                    }
                }
            }

            Logging.info("SHA benchmark complete.");
            printSystemStatus();

            running = false;
        }

        // Benchmark for RSA signing + verification
        public static void benchmarkRSA()
        {
            Thread rsa_thread = new Thread(rsaThreadLoop);
            rsa_thread.Name = "Benchmark_RSA_Thread";
            rsa_thread.Start();
        }

        private static void rsaThreadLoop()
        {
            Logging.info("Starting RSA benchmark, single thread.");
            printSystemStatus();

            int max_iterations = 5;

            Random rnd = new Random();
            Byte[] random_data = new Byte[1024 * 1024]; // 1MB worth of data
            rnd.NextBytes(random_data); // Random content

            lastStatTime = DateTime.UtcNow;

            // Benchmark key pair generation time
            Logging.info("Generating key pairs. RSA key size: {0}, iterations: {1}", ConsensusConfig.defaultRsaKeySize, max_iterations);
            IxianKeyPair kp = null;
            // Perform multiple iterations
            for (int i = 0; i < max_iterations; i++)
            {
                kp = CryptoManager.lib.generateKeys(ConsensusConfig.defaultRsaKeySize, 2);
                if (kp == null)
                {
                    Logging.error("Error during RSA benchmark, unable to generate a keypair.");
                    running = false;
                    return;
                }

                TimeSpan timeSinceLastStat = DateTime.UtcNow - lastStatTime;
                lastStatTime = DateTime.UtcNow;
                Logging.info("Generate RSA key pair took {0} ms", timeSinceLastStat.TotalMilliseconds.ToString("N0"));
            }


            // Benchmark signature time
            Logging.info("Signing 1MB of data using last generated keypair ({0} iterations):", max_iterations);
            lastStatTime = DateTime.UtcNow;
            byte[] signature = null;
            // Perform multiple iterations
            for (int i = 0; i < max_iterations; i++)
            {
                signature = CryptoManager.lib.getSignature(random_data, kp.privateKeyBytes);
                if (signature == null)
                {
                    Logging.error("Error during RSA benchmark, unable to sign data.");
                    running = false;
                    return;
                }
                TimeSpan timeSinceLastStat = DateTime.UtcNow - lastStatTime;
                lastStatTime = DateTime.UtcNow;
                Logging.info("Signing took {0} ms", timeSinceLastStat.TotalMilliseconds.ToString("N0"));
            }

            // Benchmark verification time
            Logging.info("Verifying last signature ({0} iterations):", max_iterations);
            lastStatTime = DateTime.UtcNow;
            // Perform multiple iterations
            for (int i = 0; i < max_iterations; i++)
            {
                bool valid = CryptoManager.lib.verifySignature(random_data, kp.publicKeyBytes, signature);
                if (!valid)
                {
                    Logging.error("Error during RSA benchmark, unable to verify signature.");
                    running = false;
                    return;
                }
                TimeSpan timeSinceLastStat = DateTime.UtcNow - lastStatTime;
                lastStatTime = DateTime.UtcNow;
                Logging.info("Verification took {0} ms", timeSinceLastStat.TotalMilliseconds.ToString("N0"));
            }

            Logging.info("RSA benchmark complete.");
            printSystemStatus();

            running = false;
        }

        // Benchmark for storage
        public static void benchmarkStorage()
        {
            Thread storage_thread = new Thread(storageThreadLoop);
            storage_thread.Name = "Benchmark_Storage_Thread";
            storage_thread.Start();
        }

        private static void storageThreadLoop()
        {
            IStorage storage = null;

            // Initialize storage
            if (storage is null)
            {
                storage = IStorage.create(Config.blockStorageProvider);
            }
            if (!storage.prepareStorage())
            {
                Logging.error("Error while preparing block storage! Aborting.");
                Program.noStart = true;
                return;
            }

            // Initialize the benchmark node
            benchmarkNode = new BenchmarkNode();

            // Start the node
            benchmarkNode.start();

            Logging.info("Starting Storage benchmark.");
            printSystemStatus();


            ulong highest_block = storage.getHighestBlockInStorage();
            Logging.info("Highest block is storage is #{0}", highest_block);
            if(highest_block < 101)
            {
                Logging.error("Too few blocks in blockchain storage to perform benchmark.");

                storage.stopStorage();
                Logging.info("Storage benchmark aborted.");
                printSystemStatus();

                running = false;

                // Shutdown the node
                benchmarkNode.shutdown();
                Program.noStart = true;
                return;
            }

            List<Block> blockList = new List<Block>();
            Logging.info("Fetching last 100 blocks from storage:");
            Stopwatch s1 = Stopwatch.StartNew();
            Stopwatch s2 = Stopwatch.StartNew();
            for (ulong i = 0; i < 100; i++)
            {
                s2.Restart();
                Block block = storage.getBlock(highest_block - i);
                blockList.Add(block);
                s2.Stop();
                Logging.info("Block #{0} fetch took {1} ms", highest_block - i, s2.ElapsedMilliseconds);
            }
            s1.Stop();
            Logging.info("Fetching {0} blocks took {1} ms", blockList.Count, s1.ElapsedMilliseconds);

            blockList.Clear();

            storage.stopStorage();
            Logging.info("Storage benchmark complete.");
            printSystemStatus();

            running = false;

            // Shutdown the node
            benchmarkNode.shutdown();
        }

        // Benchmark for transaction objects
        public static void benchmarkTx()
        {
            Thread tx_thread = new Thread(txThreadLoop);
            tx_thread.Name = "Benchmark_Tx_Thread";
            tx_thread.Start();
        }

        private static void txThreadLoop()
        {
            Logging.info("Starting TX benchmark.");
            printSystemStatus();

            // Initialize the benchmark node
            benchmarkNode = new BenchmarkNode();

            // Start the node
            benchmarkNode.start();

            int maxtx = 1000000; // Number of transactions to generate

            IxiNumber amount = ConsensusConfig.transactionPrice;
            IxiNumber fee = ConsensusConfig.transactionPrice;
            Random rnd = new Random();
            byte[] from = new Byte[48];
            byte[] to = new Byte[48];
            byte[] pubKey = new Byte[256];

            Dictionary<byte[], Transaction> transactions = new Dictionary<byte[], Transaction>(new ByteArrayComparer());
            List<byte[]> lookupList = new List<byte[]>();

            Logging.info("Generating {0} transactions", maxtx.ToString("N0"));
            int txcount = 0;

            Stopwatch s1 = Stopwatch.StartNew();
            Stopwatch s2 = Stopwatch.StartNew();
            while (txcount < maxtx)
            {
                rnd.NextBytes(from);
                rnd.NextBytes(to);
                rnd.NextBytes(pubKey);
                pubKey[0] = 1; // Set address version to 1

                Transaction transaction = new Transaction((int)Transaction.Type.Normal, amount, fee, new Address(to), new Address(from), null, new Address(pubKey), 1236547);
                transactions.AddOrReplace(transaction.id, transaction);
                txcount++;

                if (txcount % 10000 == 0)
                {
                    s1.Stop();
                    Logging.info("{0}/{1} transactions in {2} ms", txcount.ToString("N0"), maxtx.ToString("N0"), s1.ElapsedMilliseconds);
                    s1.Restart();
                }

                // Add 12 transactions to the lookup list for later
                if(txcount % (maxtx / 12) == 0)
                {
                    lookupList.Add(transaction.id);
                }
            }
            s1.Stop();
            s2.Stop();
            Logging.info("Generated a total of {0} transactions in {1} ms", txcount.ToString("N0"), s2.ElapsedMilliseconds.ToString("N0"));
            printSystemStatus();

            Logging.info("Preparing lookup benchmark");
            lookupList.Add(from); // Add a random invalid txid

            Logging.info("Transactions in lookup list: {0}", lookupList.Count);
            
            for (int i = 0; i < lookupList.Count; i++)
            {
                s1.Reset();
                if (transactions.ContainsKey(lookupList[i]))
                {
                    s1.Stop();
                    Logging.info("Tx #{0} lookup time took {1} ms", i, s1.ElapsedMilliseconds);
                    s1.Start();
                }              
            }
            s1.Stop();
            Logging.info("Total lookup time {0} ms", s1.ElapsedMilliseconds);
            printSystemStatus();

            Logging.info("Preparing to remove {0} transactions from memory", lookupList.Count);
            s1.Start();
            foreach (byte[] txid in lookupList)
            {
                transactions.Remove(txid);
            }
            lookupList.Clear();
            s1.Stop();
            Logging.info("Total transaction removal time {0} ms", s1.ElapsedMilliseconds);
            Thread.Sleep(1000);
            printSystemStatus();

            Logging.info("Preparing to remove remaining {0} transactions from memory", transactions.Count.ToString("N0"));
            s1.Start();
            transactions.Clear();
            s1.Stop();
            Logging.info("Total transaction removal time {0} ms", s1.ElapsedMilliseconds);
            Thread.Sleep(1000);
            printSystemStatus();


            Logging.info("Objects benchmark complete.");
            printSystemStatus();

            running = false;

            // Shutdown the node
            benchmarkNode.shutdown();
        }

        // Benchmark for key generation
        public static void benchmarkKeyGeneration(int key_size)
        {
            if (key_size != 1024 && key_size != 2048 && key_size != 4096)
            {
                Logging.error("Invalid key bit length: {0}. Allowed values are 1024, 2048 or 4096!", key_size);
            }
            else
            {
                IXICore.CryptoKey.KeyDerivation.BenchmarkKeyGeneration(10000, key_size, "bench_keys.out");
            }
            running = false;
        }




    }
}
