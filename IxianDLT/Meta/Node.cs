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

using DLT.Network;
using DLTNode;
using DLTNode.Inventory;
using IXICore;
using IXICore.Meta;
using IXICore.Network;
using IXICore.Utils;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;

namespace DLT.Meta
{
    class Node: IxianNode
    {
        // Public
        public static BlockChain blockChain = null;
        public static BlockProcessor blockProcessor = null;
        public static BlockSync blockSync = null;
        public static WalletStorage walletStorage = null;
        public static Miner miner = null;
        public static WalletState walletState = null;
        public static IStorage storage = null;
        public static InventoryCacheDLT inventoryCache = null;

        public static StatsConsoleScreen statsConsoleScreen = null;

        public static APIServer apiServer;

        public static bool genesisNode = false;
        public static bool forceNextBlock = false;

        public static bool serverStarted = false;

        // Private
        private static Thread maintenanceThread;

        private static bool running = false;


        private static bool floodPause = false;

        private static DateTime lastIsolateTime;
        private static ThreadLiveCheck TLC;

        public static string shutdownMessage = "";

        private static ulong lastMasterNodeConversionBlockNum = 0;

        public Node()
        {
            IxianHandler.init(Config.version, this, Config.networkType, !Config.disableSetTitle);
            init();
        }

        // Perform basic initialization of node
        public void init()
        {
            running = true;

            // First create the data folder if it does not already exist
            checkDataFolder();

            renameStorageFiles(); // this function will be here temporarily for the next few version, then it will be removed to keep a cleaner code base

            // debug
            if (Config.networkDumpFile != "")
            {
                NetDump.Instance.start(Config.networkDumpFile);
            }

            UpdateVerify.init(Config.checkVersionUrl, Config.checkVersionSeconds);

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

            NetworkUtils.configureNetwork(Config.externalIp, Config.serverPort);

            // Load or Generate the wallet
            if (!initWallet())
            {
                storage.stopStorage();
                running = false;
                DLTNode.Program.noStart = true;
                return;
            }

            // Setup the stats console
            statsConsoleScreen = new StatsConsoleScreen();

            // Initialize the wallet state
            walletState = new WalletState();

            inventoryCache = new InventoryCacheDLT();

            PeerStorage.init("");
        }

        private bool initWallet()
        {
            walletStorage = new WalletStorage(Config.walletFile);

            Logging.flush();

            if (!walletStorage.walletExists())
            {
                ConsoleHelpers.displayBackupText();

                // Request a password
                // NOTE: This can only be done in testnet to enable automatic testing!
                string password = "";
                if (Config.dangerCommandlinePasswordCleartextUnsafe != "")
                {
                    Logging.warn("Warning: Wallet password has been specified on the command line!");
                    password = Config.dangerCommandlinePasswordCleartextUnsafe;
                    // Also note that the commandline password still has to be >= 10 characters
                }
                while (password.Length < 10)
                {
                    Logging.flush();
                    password = ConsoleHelpers.requestNewPassword("Enter a password for your new wallet: ");
                    if (IxianHandler.forceShutdown)
                    {
                        return false;
                    }
                }
                walletStorage.generateWallet(password);
            }
            else
            {
                ConsoleHelpers.displayBackupText();

                bool success = false;
                while (!success)
                {

                    // NOTE: This is only permitted on the testnet for dev/testing purposes!
                    string password = "";
                    if (Config.dangerCommandlinePasswordCleartextUnsafe != "")
                    {
                        Logging.warn("Warning: Attempting to unlock the wallet with a password from commandline!");
                        password = Config.dangerCommandlinePasswordCleartextUnsafe;
                    }
                    if (password.Length < 10)
                    {
                        Logging.flush();
                        Console.Write("Enter wallet password: ");
                        password = ConsoleHelpers.getPasswordInput();
                    }
                    if (IxianHandler.forceShutdown)
                    {
                        return false;
                    }
                    if (walletStorage.readWallet(password))
                    {
                        success = true;
                    }
                }
            }


            if (walletStorage.getPrimaryPublicKey() == null)
            {
                return false;
            }

            // Wait for any pending log messages to be written
            Logging.flush();

            Console.WriteLine();
            Console.WriteLine("Your IXIAN addresses are: ");
            Console.ForegroundColor = ConsoleColor.Green;
            foreach(var entry in walletStorage.getMyAddressesBase58())
            {
                Console.WriteLine(entry);
            }
            Console.ResetColor();
            Console.WriteLine();

            if(Config.onlyShowAddresses)
            {
                return false;
            }

            // Check if we should change the password of the wallet
            if (Config.changePass == true)
            {
                // Request a new password
                string new_password = "";
                while (new_password.Length < 10)
                {
                    new_password = ConsoleHelpers.requestNewPassword("Enter a new password for your wallet: ");
                    if (IxianHandler.forceShutdown)
                    {
                        return false;
                    }
                }
                walletStorage.writeWallet(new_password);
            }

            Logging.info("Wallet Version: {0}", walletStorage.walletVersion);
            Logging.info("Public Node Address: {0}", Base58Check.Base58CheckEncoding.EncodePlain(walletStorage.getPrimaryAddress()));

            return true;
        }

        // this function will be here temporarily for the next few version, then it will be removed to keep a cleaner code base
        private void renameStorageFiles()
        {
            if (File.Exists("data" + Path.DirectorySeparatorChar + "ws" + Path.DirectorySeparatorChar + "0000" + Path.DirectorySeparatorChar + "wsStorage.dat.1000"))
            {
                var files = Directory.GetFiles("data" + Path.DirectorySeparatorChar + "ws" + Path.DirectorySeparatorChar + "0000");
                foreach (var filename in files)
                {
                    var split_filenane = filename.Split('.');
                    string path = filename.Substring(0, filename.LastIndexOf(Path.DirectorySeparatorChar));
                    File.Move(filename, path + Path.DirectorySeparatorChar + split_filenane[2] + ".dat");
                }
            }

            if (File.Exists("data" + Path.DirectorySeparatorChar + "blocks" + Path.DirectorySeparatorChar + "0000" + Path.DirectorySeparatorChar + "blockchain.dat.0"))
            {
                var files = Directory.GetFiles("data" + Path.DirectorySeparatorChar + "blocks" + Path.DirectorySeparatorChar + "0000");
                foreach (var filename in files)
                {
                    if (filename.EndsWith("-shm") || filename.EndsWith("-wal"))
                    {
                        File.Delete(filename);
                        continue;
                    }
                    var split_filenane = filename.Split('.');
                    string path = filename.Substring(0, filename.LastIndexOf(Path.DirectorySeparatorChar));
                    File.Move(filename, path + Path.DirectorySeparatorChar + split_filenane[2] + ".dat");
                }
            }
            
            if (File.Exists("data" + Path.DirectorySeparatorChar + "blocks" + Path.DirectorySeparatorChar + "0000" + Path.DirectorySeparatorChar + "testnet-blockchain.dat.0"))
            {
                var files = Directory.GetFiles("data" + Path.DirectorySeparatorChar + "blocks" + Path.DirectorySeparatorChar + "0000");
                foreach (var filename in files)
                {
                    if (filename.EndsWith("-shm") || filename.EndsWith("-wal"))
                    {
                        File.Delete(filename);
                        continue;
                    }
                    var split_filenane = filename.Split('.');
                    string path = "data-testnet" + Path.DirectorySeparatorChar + "blocks" + Path.DirectorySeparatorChar + "0000";
                    File.Move(filename, path + Path.DirectorySeparatorChar + split_filenane[2] + ".dat");
                }
            }
        }

        static private void distributeGenesisFunds(IxiNumber genesisFunds)
        {
            blockChain.setLastBlockVersion(Config.maxBlockVersionToGenerate);

            byte[] from = ConsensusConfig.ixianInfiniMineAddress;

            int tx_type = (int)Transaction.Type.Genesis;

            Transaction tx = new Transaction(tx_type, genesisFunds, new IxiNumber(0), walletStorage.getPrimaryAddress(), from, null, null, 1);
            TransactionPool.addTransaction(tx);

            if (Config.genesis2Address != "")
            {
                Transaction txGen2 = new Transaction(tx_type, genesisFunds, new IxiNumber(0), Base58Check.Base58CheckEncoding.DecodePlain(Config.genesis2Address), from, null, null, 1);
                TransactionPool.addTransaction(txGen2);
            }

            if (!IxianHandler.isTestNet)
            {
                // seed2
                Transaction tx2 = new Transaction(tx_type, ConsensusConfig.minimumMasterNodeFunds, 0, Base58Check.Base58CheckEncoding.DecodePlain("1NpizdRi5rmw586Aw883CoQ7THUT528CU5JGhGomgaG9hC3EF"), from, null, null, 1);
                TransactionPool.addTransaction(tx2);

                // seed3
                Transaction tx3 = new Transaction(tx_type, ConsensusConfig.minimumMasterNodeFunds, 0, Base58Check.Base58CheckEncoding.DecodePlain("1Dp9bEFkymhN8PcN7QBzKCg2buz4njjp4eJeFngh769H4vUWi"), from, null, null, 1);
                TransactionPool.addTransaction(tx3);

                // seed4
                Transaction tx4 = new Transaction(tx_type, ConsensusConfig.minimumMasterNodeFunds, 0, Base58Check.Base58CheckEncoding.DecodePlain("1SWy7jYky8xkuN5dnr3aVMJiNiQVh4GSLggZ9hBD3q7ALVEYY"), from, null, null, 1);
                TransactionPool.addTransaction(tx4);

                // seed5
                Transaction tx5 = new Transaction(tx_type, ConsensusConfig.minimumMasterNodeFunds, 0, Base58Check.Base58CheckEncoding.DecodePlain("1R2WxZ7rmQhMTt5mCFTPhPe9Ltw8pTPY6uTsWHCvVd3GvWupC"), from, null, null, 1);
                TransactionPool.addTransaction(tx5);

                // Team Reward
                Transaction tx6 = new Transaction(tx_type, new IxiNumber("1000000000"), 0, Base58Check.Base58CheckEncoding.DecodePlain("13fiCRZHPqcCFvQvuggKEjDvFsVLmwoavaBw1ng5PdSKvCUGp"), from, null, null, 1);
                TransactionPool.addTransaction(tx6);

                // Development
                Transaction tx7 = new Transaction(tx_type, new IxiNumber("1000000000"), 0, Base58Check.Base58CheckEncoding.DecodePlain("16LUmwUnU9M4Wn92nrvCStj83LDCRwvAaSio6Xtb3yvqqqCCz"), from, null, null, 1);
                TransactionPool.addTransaction(tx7);
            }
        }

        // Start the node
        public void start(bool verboseConsoleOutput)
        {
            char node_type = 'W';

            // Check if we're in worker-only mode
            if (Config.workerOnly)
            {
                CoreConfig.simultaneousConnectedNeighbors = 4;
            }

            // Generate presence list
            PresenceList.init(IxianHandler.publicIP, Config.serverPort, node_type);

            ActivityStorage.prepareStorage();

            // Initialize the block chain
            blockChain = new BlockChain();

            //runDiffTests();
            //return;

            // Create the block processor and sync
            blockProcessor = new BlockProcessor();
            blockSync = new BlockSync();


            if (Config.devInsertFromJson)
            {
                Console.WriteLine("Inserting from JSON");
                devInsertFromJson();
                Program.noStart = true;
                return;
            }

            if (Config.apiBinds.Count == 0)
            {
                Config.apiBinds.Add("http://localhost:" + Config.apiPort + "/");
            }

            // Start the HTTP JSON API server
            apiServer = new APIServer(Config.apiBinds, Config.apiUsers, Config.apiAllowedIps);

            if (IXICore.Platform.onMono() == false && !Config.disableWebStart)
            {
                System.Diagnostics.Process.Start(Config.apiBinds[0]);
            }

            miner = new Miner();

            // Start the network queue
            NetworkQueue.start();

            // prepare stats screen
            ConsoleHelpers.verboseConsoleOutput = verboseConsoleOutput;
            Logging.consoleOutput = verboseConsoleOutput;
            Logging.flush();
            if (ConsoleHelpers.verboseConsoleOutput == false)
            {
                statsConsoleScreen.clearScreen();
            }

            // Distribute genesis funds
            IxiNumber genesisFunds = new IxiNumber(Config.genesisFunds);

            // Check if this is a genesis node
            if (genesisFunds > (long)0)
            {
                Logging.info(String.Format("Genesis {0} specified. Starting operation.", genesisFunds));

                distributeGenesisFunds(genesisFunds);

                genesisNode = true;
                PresenceList.myPresenceType = 'M';
                blockProcessor.resumeOperation();
                serverStarted = true;
                if (!isMasterNode())
                {
                    Logging.info("Network server is not enabled in modes other than master node.");
                }
                else
                {
                    NetworkServer.beginNetworkOperations();
                }
            }
            else
            {
                if(File.Exists(Config.genesisFile))
                {
                    Block genesis = new Block(Crypto.stringToHash(File.ReadAllText(Config.genesisFile)));
                    blockChain.setGenesisBlock(genesis);
                }
                ulong lastLocalBlockNum = storage.getHighestBlockInStorage();
                if (lastLocalBlockNum > 6)
                {
                    lastLocalBlockNum = lastLocalBlockNum - 6;
                }
                if(Config.lastGoodBlock > 0 && Config.lastGoodBlock < lastLocalBlockNum)
                {
                    lastLocalBlockNum = Config.lastGoodBlock;
                }
                if (lastLocalBlockNum > 0)
                {
                    Block b = blockChain.getBlock(lastLocalBlockNum, true);
                    if (b != null)
                    {
                        ConsensusConfig.minRedactedWindowSize = ConsensusConfig.getRedactedWindowSize(b.version);
                        ConsensusConfig.redactedWindowSize = ConsensusConfig.getRedactedWindowSize(b.version);
                    }

                }

                // Start block sync
                ulong blockNum = WalletStateStorage.restoreWalletState(lastLocalBlockNum);
                if(blockNum > 0)
                {
                    Block b = blockChain.getBlock(blockNum, true);
                    if (b != null)
                    {
                        blockSync.onHelloDataReceived(blockNum, b.blockChecksum, b.version, b.walletStateChecksum, b.getSignatureCount(), lastLocalBlockNum);
                    }else
                    {
                        walletState.clear();
                    }
                }else
                {
                    blockSync.lastBlockToReadFromStorage = lastLocalBlockNum;

                    walletState.clear();

                    if (CoreConfig.preventNetworkOperations)
                    {
                        Block b = storage.getBlock(lastLocalBlockNum);
                        blockSync.onHelloDataReceived(b.blockNum, b.blockChecksum, b.version, b.walletStateChecksum, b.getSignatureCount(), lastLocalBlockNum);
                    }
                }

                // Start the server for ping purposes
                serverStarted = true;
                if (!isMasterNode())
                {
                    Logging.info("Network server is not enabled in modes other than master node.");
                }
                else
                {
                    NetworkServer.beginNetworkOperations();
                }

                // Start the network client manager
                if (Config.recoverFromFile)
                {
                    NetworkClientManager.start(0);
                }else
                {
                    NetworkClientManager.start(1);
                }
            }

            PresenceList.startKeepAlive();

            TLC = new ThreadLiveCheck();
            // Start the maintenance thread
            maintenanceThread = new Thread(performMaintenance);
            maintenanceThread.Name = "Node_Maintenance_Thread";
            maintenanceThread.Start();
        }

        static public bool update()
        {
            if(serverStarted == false)
            {
                /*if(Node.blockProcessor.operating == true)
                {*/
                    Logging.info("Starting Network Server now.");

                    // Start the node server
                    if (!isMasterNode())
                    {
                        Logging.info("Network server is not enabled in modes other than master node.");
                    }
                    else
                    {
                        NetworkServer.beginNetworkOperations();
                    }

                    serverStarted = true;
                //}
            }

            // Check for node deprecation
            if (checkCurrentBlockDeprecation(Node.blockChain.getLastBlockNum()) == false)
            {
                ConsoleHelpers.verboseConsoleOutput = true;
                Logging.consoleOutput = true;
                shutdownMessage = string.Format("Your DLT node can only handle blocks up to #{0}. Please update to the latest version from www.ixian.io", Config.nodeDeprecationBlock);
                Logging.error(shutdownMessage);
                IxianHandler.forceShutdown = true;
                running = false;
                return running;
            }

            // Check for sufficient node balance
            if (checkMasternodeBalance() == false)
            {
                //running = false;
            }

            TimeSpan last_isolate_time_diff = DateTime.UtcNow - lastIsolateTime;
            if (Node.blockChain.getTimeSinceLastBLock() > 900 && (last_isolate_time_diff.TotalSeconds < 0 || last_isolate_time_diff.TotalSeconds > 1800)) // if no block for over 900 seconds with cooldown of 1800 seconds
            {
                CoreNetworkUtils.isolate();
                lastIsolateTime = DateTime.UtcNow;
            }

            // TODO TODO TODO TODO this is a global flood control and should be also done per node to detect attacks
            // I propose getting average traffic from different types of nodes and detect a node that's sending 
            // disproportionally more messages than the other nodes, provided thatthe network queue is over a certain limit
            int total_queued_messages = NetworkQueue.getQueuedMessageCount();
            if(floodPause == false && total_queued_messages > Config.floodMaxQueuedMessages)
            {
                Logging.warn("Flooding detected, isolating the node.");
                NetworkClientManager.stop();
                if (isMasterNode())
                {
                    NetworkServer.stopNetworkOperations();
                }
                floodPause = true;
            }else if(floodPause == true && total_queued_messages < Config.floodDisableMaxQueuedMessages)
            {
                Logging.warn("Data after flooding processed, reconnecting the node.");
                if (isMasterNode())
                {
                    NetworkServer.beginNetworkOperations();
                }
                NetworkClientManager.start();
                floodPause = false;
            }


            return running;
        }

        static public void stop()
        {
            Program.noStart = true;
            IxianHandler.forceShutdown = true;

            // Stop the keepalive thread
            PresenceList.stopKeepAlive();

            // Stop the block processor
            blockProcessor.stopOperation();

            // Stop the block sync
            blockSync.stop();

            // Stop the API server
            if (apiServer != null)
            {
                apiServer.stop();
                apiServer = null;
            }

            // Stop the miner
            if (miner != null)
            {
                miner.stop();
                miner = null;
            }

            if (maintenanceThread != null)
            {
                maintenanceThread.Abort();
                maintenanceThread = null;
            }

            // Stop the block storage
            storage.stopStorage();

            // stop activity storage
            ActivityStorage.stopStorage();

            // Stop the network queue
            NetworkQueue.stop();

            // Stop all network clients
            NetworkClientManager.stop();
            
            // Stop the network server
            NetworkServer.stopNetworkOperations();

            // Stop the console stats screen
            // Console screen has a thread running even if we are in verbose mode
            statsConsoleScreen.stop();

            NetDump.Instance.shutdown();
        }

        static public void synchronize()
        {
            // Clear everything and force a resynchronization
            Logging.info("\n\n\tSynchronizing to network...\n");

            blockProcessor.stopOperation();

            blockProcessor = new BlockProcessor();
            blockChain = new BlockChain();
            walletState.clear();
            TransactionPool.clear();

            NetworkQueue.stop();
            NetworkQueue.start();

            // Finally, reconnect to the network
            CoreNetworkUtils.reconnect();
        }

        // Checks to see if this node can handle the block number
        static public bool checkCurrentBlockDeprecation(ulong block)
        {
            ulong block_limit = Config.nodeDeprecationBlock;

            if(block > block_limit)
            {
                return false;
            }

            return true;
        }

        // Checks the current balance of the masternode
        static public bool checkMasternodeBalance()
        {
            if (Config.workerOnly)
                return false;

            // First check if the block processor is running
            if (blockProcessor.operating == true)
            {
                ulong last_block_num = blockChain.getLastBlockNum();
                if (last_block_num > 2)
                {
                    IxiNumber nodeBalance = walletState.getWalletBalance(walletStorage.getPrimaryAddress());
                    if(!isMasterNode())
                    {
                        if (nodeBalance >= ConsensusConfig.minimumMasterNodeFunds)
                        {
                            if (lastMasterNodeConversionBlockNum != last_block_num)
                            {
                                lastMasterNodeConversionBlockNum = last_block_num;
                                Logging.info(string.Format("Your balance is more than the minimum {0} IXIs needed to operate a masternode. Reconnecting as a masternode.", ConsensusConfig.minimumMasterNodeFunds));
                                convertToMasterNode();
                            }
                        }
                    }
                    else
                    if (nodeBalance < ConsensusConfig.minimumMasterNodeFunds)
                    {
                        if (!isWorkerNode())
                        {
                            Logging.error(string.Format("Your balance is less than the minimum {0} IXIs needed to operate a masternode. Reconnecting as a worker node.",
                                ConsensusConfig.minimumMasterNodeFunds));
                            convertToWorkerNode();
                        }
                        return false;
                    }
                }
            }
            // Masternode has enough IXIs to proceed
            return true;
        }

        public static void debugDumpState()
        {
            Console.ForegroundColor = ConsoleColor.Yellow;
            Logging.trace("===================== Dumping Node State: =====================");
            Logging.trace(String.Format(" -> Current blockchain height: #{0}. In redacted chain: #{1}.", Node.blockChain.getLastBlockNum(), Node.blockChain.Count));
            Logging.trace(String.Format(" -> Last Block Checksum: {0}.", Crypto.hashToString(Node.blockChain.getLastBlockChecksum())));
            Logging.trace("Last five signature counts:");
            for (int i = 0; i < 6; i++)
            {
                ulong blockNum = Node.blockChain.getLastBlockNum() - (ulong)i;
                Block block = Node.blockChain.getBlock(blockNum);

                if(block != null)
                    Logging.trace(String.Format(" -> block #{0}, signatures: {1}, checksum: {2}, wsChecksum: {3}.", blockNum, Node.blockChain.getBlock(blockNum - (ulong)i).getSignatureCount(), 
                        Crypto.hashToString(block.blockChecksum), Crypto.hashToString(block.walletStateChecksum)));
            }
            Logging.trace(String.Format(" -> Block processor is operating: {0}.", Node.blockProcessor.operating));
            Logging.trace(String.Format(" -> Block processor is synchronizing: {0}.", Node.blockSync.synchronizing));
            Logging.trace(String.Format(" -> Current consensus number: {0}.", Node.blockChain.getRequiredConsensus()));
            Console.ResetColor();
        }

        public static bool isElectedToGenerateNextBlock(int offset = 0)
        {
            if(offset == -1)
            {
                return false;
            }

            byte[] pubKey = blockChain.getLastElectedNodePubKey(offset);
            if(pubKey != null && pubKey.SequenceEqual(walletStorage.getPrimaryPublicKey()))
            {
                return true;
            }

            return false;
        }

        // Cleans the storage cache and logs
        public static bool cleanCacheAndLogs()
        {
            ActivityStorage.deleteCache();

            // deleting block storage is a special case
            // we have to instantiate whatever implementation we are using and remove its data files
            if (storage is null)
            {
                storage = IStorage.create(Config.blockStorageProvider);
            }
            storage.deleteData();

            WalletStateStorage.deleteCache();

            PeerStorage.deletePeersFile();

            Logging.clear();

            Logging.info("Cleaned cache and logs.");
            return true;
        }


        // Perform periodic cleanup tasks
        private static void performMaintenance()
        {
            while (running)
            {
                TLC.Report();
                // Sleep a while to prevent cpu usage
                Thread.Sleep(10000);

                try
                {
                    TransactionPool.processPendingTransactions();

                    // Cleanup the presence list
                    PresenceList.performCleanup();

                    inventoryCache.processCache();

                    if (update() == false)
                    {
                        IxianHandler.forceShutdown = true;
                    }

                    // Remove expired peers from blacklist
                    PeerStorage.updateBlacklist();
                }
                catch (Exception e)
                {
                    Logging.error("Exception occured in Node.performMaintenance: " + e);
                }
            }
        }

        // Convert this masternode to a worker node
        public static void convertToWorkerNode()
        {
            if (PresenceList.myPresenceType == 'W')
                return;

            CoreConfig.simultaneousConnectedNeighbors = 4;

            PresenceList.myPresenceType = 'W';

            NetworkClientManager.restartClients();
            NetworkServer.stopNetworkOperations();
        }

        // Convert this worker node to a masternode
        public static void convertToMasterNode()
        {
            if (PresenceList.myPresenceType == 'M' || PresenceList.myPresenceType == 'H')
                return;

            CoreConfig.simultaneousConnectedNeighbors = 6;

            if (Config.storeFullHistory)
            {
                PresenceList.myPresenceType = 'M'; // TODO TODO TODO TODO this is only temporary until all nodes upgrade, changes this to 'H' later
            }
            else
            {
                PresenceList.myPresenceType = 'M';
            }

            if (!Node.isMasterNode())
            {
                Logging.info("Network server is not enabled in modes other than master node.");
                NetworkServer.stopNetworkOperations();
            }
            else
            {
                NetworkServer.restartNetworkOperations();
            }

            NetworkClientManager.restartClients();
        }

        public static bool isWorkerNode()
        {
            if (PresenceList.myPresenceType == 'W')
                return true;
            return false;
        }

        public static bool isMasterNode()
        {
            if (PresenceList.myPresenceType == 'M' || PresenceList.myPresenceType == 'H')
                return true;
            return false;
        }

        public override ulong getLastBlockHeight()
        {
            return blockChain.getLastBlockNum();
        }

        public override int getLastBlockVersion()
        {
            return blockChain.getLastBlockVersion();
        }

        // Check if the data folder exists. Otherwise it creates it
        public static void checkDataFolder()
        {
            if (!Directory.Exists(Config.dataFolderPath))
            {
                Directory.CreateDirectory(Config.dataFolderPath);
            }
            File.SetAttributes(Config.dataFolderPath, FileAttributes.NotContentIndexed);


            if (!Directory.Exists(Config.dataFolderPath + Path.DirectorySeparatorChar + "ws"))
            {
                Directory.CreateDirectory(Config.dataFolderPath + Path.DirectorySeparatorChar + "ws");
            }

            if (!Directory.Exists(Config.dataFolderPath + Path.DirectorySeparatorChar + "ws" + Path.DirectorySeparatorChar + "0000"))
            {
                Directory.CreateDirectory(Config.dataFolderPath + Path.DirectorySeparatorChar + "ws" + Path.DirectorySeparatorChar + "0000");
            }

            if (!Directory.Exists(Config.dataFolderPath + Path.DirectorySeparatorChar + "blocks"))
            {
                Directory.CreateDirectory(Config.dataFolderPath + Path.DirectorySeparatorChar + "blocks");
            }

            if (!Directory.Exists(Config.dataFolderPath + Path.DirectorySeparatorChar + "blocks" + Path.DirectorySeparatorChar + "0000"))
            {
                Directory.CreateDirectory(Config.dataFolderPath + Path.DirectorySeparatorChar + "blocks" + Path.DirectorySeparatorChar + "0000");
            }
        }

        public override ulong getHighestKnownNetworkBlockHeight()
        {
            ulong bh = getLastBlockHeight();

            if(bh < blockProcessor.highestNetworkBlockNum)
            {
                bh = blockProcessor.highestNetworkBlockNum;
            }

            return bh;
        }

        public override Block getLastBlock()
        {
            return blockChain.getLastBlock();
        }

        public override bool isAcceptingConnections()
        {
            if(Node.blockProcessor.operating)
            {
                return true;
            }
            return false;
        }

        public override bool addTransaction(Transaction transaction, bool force_broadcast)
        {
            return TransactionPool.addTransaction(transaction, false, null, true, force_broadcast);
        }

        public override Wallet getWallet(byte[] id)
        {
            return Node.walletState.getWallet(id);
        }

        public override IxiNumber getWalletBalance(byte[] id)
        {
            return Node.walletState.getWalletBalance(id);
        }

        public override void shutdown()
        {
            IxianHandler.forceShutdown = true;
        }

        public override WalletStorage getWalletStorage()
        {
            return walletStorage;
        }

        public override void parseProtocolMessage(ProtocolMessageCode code, byte[] data, RemoteEndpoint endpoint)
        {
            ProtocolMessage.parseProtocolMessage(code, data, endpoint);
        }

        /*static void runDiffTests()
        {
            Logging.info("Running difficulty tests");
            CoreConfig.redactedWindowSize = CoreConfig.getRedactedWindowSize(2);
            ulong redactedWindow = CoreConfig.getRedactedWindowSize(2);
            ulong prevDiff = 0;

            ulong block_step = 1;  // Number of blocks to increase in one step. The less, the more precise.
            int cycle_count = 5;    // Number of cycles to run this test


            List<dataPoint> diffs = new List<dataPoint>();

            ulong block_num = 1;

            Random rnd = new Random();

            ulong hash_rate = 2000; // starting hashrate
            BigInteger max_hash_rate = 0;

            for (int c = 0; c < cycle_count; c++)
            {
                for (ulong i = 0; i < redactedWindow; i += block_step)
                {
                    prevDiff = BlockProcessor.calculateDifficulty_v3();
                    Block b = new Block();
                    b.version = 2;
                    b.blockNum = block_num;
                    block_num++;
                    b.difficulty = prevDiff;

                    if (i > 10)
                    {
                        if (i > 1000 && i < 2000)
                        {
                            // increase hashrate by 100
                            hash_rate += 100;
                        }
                        else if (i > 2000 && i < 5000)
                        {
                            // spike the hashrate to 50k
                            hash_rate = 50000;
                        }
                        else if (i > 5000 && i < 10000)
                        {
                            // drop hash rate to 4k
                            hash_rate = 4000;
                        }
                        else if(i > 10000)
                        {
                            ulong next_rnd = (ulong)rnd.Next(1000);
                            // randomize hash rate
                            if (rnd.Next(2) == 1)
                            {
                                hash_rate += next_rnd;
                                if (hash_rate > 100000)
                                {
                                    hash_rate = 5000;
                                }
                            }
                            else
                            {
                                if (hash_rate < next_rnd)
                                {
                                    hash_rate = 5000;
                                }
                                else
                                {
                                    hash_rate -= next_rnd;
                                }
                                if(hash_rate < 5000)
                                {
                                    hash_rate = 5000;
                                }
                            }
                        }
                        ulong max_difficulty = Miner.calculateTargetDifficulty(max_hash_rate);
                        List<Block> blocks = blockChain.getBlocks().ToList().FindAll(x => x.powField == null && x.difficulty < max_difficulty).OrderBy(x => x.difficulty).ToList();
                        if (blocks.Count == 0)
                        {
                            max_hash_rate += hash_rate;
                        }
                        else
                        {
                            BigInteger hash_rate_used = 0;
                            int tmp_nonce_counter = 0;
                            foreach (Block pow_block in blocks)
                            {
                                hash_rate_used += Miner.getTargetHashcountPerBlock(pow_block.difficulty);
                                Transaction t = new Transaction((int)Transaction.Type.PoWSolution);
                                t.data = BitConverter.GetBytes(pow_block.blockNum);
                                t.applied = b.blockNum;
                                t.fromList.Add(new byte[1] { 0 }, 0);
                                t.pubKey = Node.walletStorage.getPrimaryAddress();
                                t.blockHeight = b.blockNum - 1;
                                t.nonce += tmp_nonce_counter;
                                tmp_nonce_counter++;
                                t.generateChecksums();

                                TransactionPool.transactions.Add(t.id, t);

                                b.transactions.Add(t.id);
                                blockChain.blocksDictionary[pow_block.blockNum].powField = new byte[8];

                                if (hash_rate_used >= max_hash_rate)
                                {
                                    max_hash_rate = hash_rate;
                                    break;
                                }
                            }
                        }
                    }else
                    {

                    }

                    blockChain.blocks.Add(b);
                    blockChain.blocksDictionary.Add(b.blockNum, b);
                    blockChain.redactChain();

                    Logging.info("[generated {0}\t/{1}] Diff: {2}", block_num, redactedWindow, prevDiff);

                    dataPoint datap = new dataPoint();
                    datap.diff = prevDiff;
                    datap.solved = ((blockChain.getSolvedBlocksCount(redactedWindow) * 100) / (ulong)blockChain.blocks.Count()) + "% - " + block_num;
                    diffs.Add(datap);
                }
            }


            string text = JsonConvert.SerializeObject(diffs);
            System.IO.File.WriteAllText(@"chart.json", text);

            Logging.info("Test done, you can open chart.html now");
        }*/

        private void devInsertFromJson()
        {
            string json_file = "block.txt";

            Dictionary<string, string> response = JsonConvert.DeserializeObject<Dictionary<string, string>>(File.ReadAllText(json_file));
            ulong blockNum = ulong.Parse(response["Block Number"]);
            List<byte[][]> signatures = JsonConvert.DeserializeObject<List<byte[][]>>(response["Signatures"]);
            Block b = storage.getBlock(blockNum);
            b.signatures = signatures;
            storage.insertBlock(b);
        }
    }

    class dataPoint
    {
        [JsonProperty(PropertyName = "diff")]
        public ulong diff { get; set; }

        [JsonProperty(PropertyName = "solved")]
        public string solved { get; set; }
    }
}
