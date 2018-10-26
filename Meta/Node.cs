using DLT.Network;
using IXICore;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;

namespace DLT.Meta
{
    class Node
    {
        // Public
        public static BlockChain blockChain;
        public static BlockProcessor blockProcessor;
        public static BlockSync blockSync;
        public static WalletStorage walletStorage;
        public static Miner miner;
        public static WalletState walletState;

        public static UPnP upnp;

        public static bool genesisNode = false;
        public static bool forceNextBlock = false;

        public static bool serverStarted = false;
        public static bool presenceListActive = false;


        public static int keepAliveVersion = 0;

        // Private
        private static Thread keepAliveThread;
        private static bool autoKeepalive = false;
        private static bool workerMode = false;

        private static bool running = false;


        static public void start()
        {
            running = true;

            // Upgrade any legacy files
            NodeLegacy.upgrade();

            // Load or Generate the wallet
            walletStorage = new WalletStorage(Config.walletFile);
            if(walletStorage.publicKey == null)
            {
                running = false;
                DLTNode.Program.noStart = true;
                return;
            }

            // Initialize the wallet state
            walletState = new WalletState();

            // debug
            if (Config.networkDumpFile != "")
            {
                NetDump.Instance.start(Config.networkDumpFile);
            }

            // Network configuration
            upnp = new UPnP();
            if (Config.externalIp != "" && IPAddress.TryParse(Config.externalIp, out _))
            {
                Config.publicServerIP = Config.externalIp;
            }
            else
            {
                Config.publicServerIP = "";
                List<IPAndMask> local_ips = CoreNetworkUtils.GetAllLocalIPAddressesAndMasks();
                foreach (IPAndMask local_ip in local_ips)
                {
                    if(IPv4Subnet.IsPublicIP(local_ip.Address))
                    {
                        Logging.info(String.Format("Public IP detected: {0}, mask {1}.", local_ip.Address.ToString(), local_ip.SubnetMask.ToString()));
                        Config.publicServerIP = local_ip.Address.ToString();
                    }
                }
                if(Config.publicServerIP == "")
                {
                    IPAddress primary_local = CoreNetworkUtils.GetPrimaryIPAddress();
                    if(primary_local == null)
                    {
                        Logging.warn("Unable to determine primary IP address.");
                        showIPmenu();
                    } else
                    {
                        Logging.warn(String.Format("None of the locally configured IP addresses are public. Attempting UPnP..."));
                        IPAddress public_ip = upnp.GetExternalIPAddress();
                        if(public_ip == null)
                        {
                            Logging.warn("UPnP failed.");
                            showIPmenu();
                        } else
                        {
                            Logging.info(String.Format("UPNP-determined public IP: {0}. Attempting to configure a port-forwarding rule.", public_ip.ToString()));
                            if(upnp.MapPublicPort(Config.serverPort, primary_local))
                            {
                                Config.publicServerIP = upnp.getMappedIP();
                                Logging.info(string.Format("Network configured. Public IP is: {0}", Config.publicServerIP));
                            } else
                            {
                                Logging.warn("UPnP configuration failed.");
                                showIPmenu();
                            }
                        }
                    }
                }
            }

            // Generate presence list
            PresenceList.generatePresenceList(Config.publicServerIP);

            // Initialize storage
            Storage.prepareStorage();

            // Initialize the block chain
            blockChain = new BlockChain();

            // Create the block processor and sync
            blockProcessor = new BlockProcessor();
            blockSync = new BlockSync();

            miner = new Miner();

            // Start the network queue
            NetworkQueue.start();

            // Distribute genesis funds
            IxiNumber genesisFunds = new IxiNumber(Config.genesisFunds);

            // Check if this is a genesis node
            if (genesisFunds > (long)0)
            {
                Logging.info(String.Format("Genesis {0} specified. Starting operation.", genesisFunds));

                byte[] from = CoreConfig.ixianInfiniMineAddress;

                Transaction tx = new Transaction();
                tx.type = (int)Transaction.Type.Genesis;
                tx.to = walletStorage.address;
                tx.from = from;
                tx.amount = genesisFunds;
                tx.data = null;
                tx.blockHeight = 1;
                tx.timeStamp = Core.getCurrentTimestamp();
                tx.id = tx.generateID();
                tx.checksum = Transaction.calculateChecksum(tx);
                tx.signature = Transaction.getSignature(tx.checksum);
                TransactionPool.addTransaction(tx);

                if(Config.genesis2Address != "")
                {
                    Transaction txGen2 = new Transaction();
                    txGen2.type = (int)Transaction.Type.Genesis;
                    txGen2.to = Base58Check.Base58CheckEncoding.DecodePlain(Config.genesis2Address);
                    txGen2.from = from;
                    txGen2.amount = genesisFunds;
                    txGen2.data = null;
                    tx.blockHeight = 1;
                    txGen2.timeStamp = Core.getCurrentTimestamp();
                    txGen2.id = txGen2.generateID();
                    txGen2.checksum = Transaction.calculateChecksum(txGen2);
                    txGen2.signature = Transaction.getSignature(txGen2.checksum);
                    TransactionPool.addTransaction(txGen2);
                }

                if (Config.isTestNet)
                {
                    // testnet seed2
                    Transaction tx2 = new Transaction(CoreConfig.minimumMasterNodeFunds, 0, Base58Check.Base58CheckEncoding.DecodePlain("16NBHjLGJnmWGWjoRj1Tz5TebgwhAtN2ewDThrDp1HfKuhJBo"), from, null, null, 1);
                    tx2.type = (int)Transaction.Type.Genesis;
                    tx2.data = null;
                    tx2.timeStamp = Core.getCurrentTimestamp();
                    tx2.id = tx2.generateID();
                    tx2.checksum = Transaction.calculateChecksum(tx2);
                    tx2.signature = Transaction.getSignature(tx2.checksum);
                    TransactionPool.addTransaction(tx2);
                }
                else
                {
                    // seed2
                    Transaction tx2 = new Transaction(CoreConfig.minimumMasterNodeFunds, 0, Base58Check.Base58CheckEncoding.DecodePlain("1NpizdRi5rmw586Aw883CoQ7THUT528CU5JGhGomgaG9hC3EF"), from, null, null, 1);
                    tx2.type = (int)Transaction.Type.Genesis;
                    tx2.data = null;
                    tx2.timeStamp = Core.getCurrentTimestamp();
                    tx2.id = tx2.generateID();
                    tx2.checksum = Transaction.calculateChecksum(tx2);
                    tx2.signature = Transaction.getSignature(tx2.checksum);
                    TransactionPool.addTransaction(tx2);

                    // seed3
                    Transaction tx3 = new Transaction(CoreConfig.minimumMasterNodeFunds, 0, Base58Check.Base58CheckEncoding.DecodePlain("1Dp9bEFkymhN8PcN7QBzKCg2buz4njjp4eJeFngh769H4vUWi"), from, null, null, 1);
                    tx3.type = (int)Transaction.Type.Genesis;
                    tx3.data = null;
                    tx3.timeStamp = Core.getCurrentTimestamp();
                    tx3.id = tx3.generateID();
                    tx3.checksum = Transaction.calculateChecksum(tx3);
                    tx3.signature = Transaction.getSignature(tx3.checksum);
                    TransactionPool.addTransaction(tx3);

                    // seed4
                    Transaction tx4 = new Transaction(CoreConfig.minimumMasterNodeFunds, 0, Base58Check.Base58CheckEncoding.DecodePlain("1SWy7jYky8xkuN5dnr3aVMJiNiQVh4GSLggZ9hBD3q7ALVEYY"), from, null, null, 1);
                    tx4.type = (int)Transaction.Type.Genesis;
                    tx4.data = null;
                    tx4.timeStamp = Core.getCurrentTimestamp();
                    tx4.id = tx4.generateID();
                    tx4.checksum = Transaction.calculateChecksum(tx4);
                    tx4.signature = Transaction.getSignature(tx4.checksum);
                    TransactionPool.addTransaction(tx4);

                    // seed5
                    Transaction tx5 = new Transaction(CoreConfig.minimumMasterNodeFunds, 0, Base58Check.Base58CheckEncoding.DecodePlain("1R2WxZ7rmQhMTt5mCFTPhPe9Ltw8pTPY6uTsWHCvVd3GvWupC"), from, null, null, 1);
                    tx5.type = (int)Transaction.Type.Genesis;
                    tx5.data = null;
                    tx5.timeStamp = Core.getCurrentTimestamp();
                    tx5.id = tx5.generateID();
                    tx5.checksum = Transaction.calculateChecksum(tx5);
                    tx5.signature = Transaction.getSignature(tx5.checksum);
                    TransactionPool.addTransaction(tx5);

                    // Team Reward
                    Transaction tx6 = new Transaction(new IxiNumber("1000000000"), 0, Base58Check.Base58CheckEncoding.DecodePlain("13fiCRZHPqcCFvQvuggKEjDvFsVLmwoavaBw1ng5PdSKvCUGp"), from, null, null, 1);
                    tx6.type = (int)Transaction.Type.Genesis;
                    tx6.data = null;
                    tx6.timeStamp = Core.getCurrentTimestamp();
                    tx6.id = tx6.generateID();
                    tx6.checksum = Transaction.calculateChecksum(tx6);
                    tx6.signature = Transaction.getSignature(tx6.checksum);
                    TransactionPool.addTransaction(tx6);

                    // Development
                    Transaction tx7 = new Transaction(new IxiNumber("1000000000"), 0, Base58Check.Base58CheckEncoding.DecodePlain("16LUmwUnU9M4Wn92nrvCStj83LDCRwvAaSio6Xtb3yvqqqCCz"), from, null, null, 1);
                    tx7.type = (int)Transaction.Type.Genesis;
                    tx7.data = null;
                    tx7.timeStamp = Core.getCurrentTimestamp();
                    tx7.id = tx7.generateID();
                    tx7.checksum = Transaction.calculateChecksum(tx7);
                    tx7.signature = Transaction.getSignature(tx7.checksum);
                    TransactionPool.addTransaction(tx7);
                }

                genesisNode = true;
                blockProcessor.resumeOperation();
                serverStarted = true;
                NetworkServer.beginNetworkOperations();
            }
            else
            {
                if (Config.recoverFromFile)
                {
                    Storage.readFromStorage();
                }
                else
                {
                    // Start the network client manager
                    NetworkClientManager.start();
                }
            }

            // Start the keepalive thread
            autoKeepalive = true;
            keepAliveThread = new Thread(keepAlive);
            keepAliveThread.Start();
        }

        static public bool update()
        {
            // Update redacted blockchain
            blockChain.onUpdate();

            // Cleanup the presence list
            // TODO: optimize this by using a different thread perhaps
            PresenceList.performCleanup();

            if(serverStarted == false)
            {
                if(Node.blockProcessor.operating == true)
                {
                    Logging.info("Starting Network Server now.");

                    // Start the node server
                    NetworkServer.beginNetworkOperations();

                    serverStarted = true;
                }
            }

            // Check for node deprecation
            if (checkCurrentBlockDeprecation(Node.blockChain.getLastBlockNum()) == false)
            {
                Logging.error(string.Format("Your DLT node can only handle blocks up to #{0}. Please update to the latest version from www.ixian.io", Config.compileTimeBlockNumber + Config.deprecationBlockOffset));
                running = false;
                return running;
            }

            // Check for sufficient node balance
            if (checkMasternodeBalance() == false)
            {
                running = false;
            }

            return running;
        }

        static public void stop()
        {
            // Stop the miner
            if (miner != null)
            {
                miner.stop();
            }

            autoKeepalive = false;
            if (keepAliveThread != null)
            {
                keepAliveThread.Abort();
                keepAliveThread = null;
            }

            // Stop the network queue
            NetworkQueue.stop();

            // Stop the block processor
            blockProcessor.stopOperation();

            // Stop the block sync
            blockSync.stop();

            // Stop all network clients
            NetworkClientManager.stop();
            
            // Stop the network server
            NetworkServer.stopNetworkOperations();

            // Stop the storage thread
            Storage.stopStorage();

            NetDump.Instance.shutdown();

            presenceListActive = false;
        }

        static public void reconnect()
        {
            // Reconnect server and clients
            presenceListActive = false;

            // Reset the network receive queue
            NetworkQueue.reset();

            NetworkServer.restartNetworkOperations();
            NetworkClientManager.restartClients();
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
            reconnect();
        }

        // Isolates the node from the network.
        static public void isolate()
        {
            NetworkClientManager.isolate();
            NetworkServer.restartNetworkOperations();

        }

        // Shows an IP selector menu
        static public void showIPmenu()
        {
            Thread.Sleep(1000); // sleep a bit to allow logging to do it's thing
            Console.WriteLine("This node needs to be reachable from the internet. Please select a valid IP address.");
            Console.WriteLine();

            List<string> ips = CoreNetworkUtils.GetAllLocalIPAddresses();

            uint counter = 0;
            foreach (string ip in ips)
            {
                Console.WriteLine("\t{0}) {1}", counter, ip);
                counter++;
            }
            Console.WriteLine("\tM) Manual Entry");
            Console.WriteLine();

            Console.Write("Choose option [default 0]: ");

            int option = 0;
            try
            {
                string result = Console.ReadLine();
                if (result.Equals("m", StringComparison.OrdinalIgnoreCase))
                {
                    option = -1;
                }
                else
                {
                    option = Convert.ToInt32(result);
                }
            }
            catch(Exception)
            {
                // Handle exceptions
                option = 0;
            }

            if (option == -1)
            {
                showManualIPEntry();
            }
            else
            {
                if (option > ips.Count || option < 0)
                    option = 0;

                string chosenIP = ips[option];
                Config.publicServerIP = chosenIP;
                Console.WriteLine("Using option {0}) {1} as the default external IP for this node.", option, chosenIP);
            }
        }

        static public void showManualIPEntry()
        {
            Console.Write("Type Manual IP: ");
            string chosenIP = Console.ReadLine();

            // Validate the IP
            if (chosenIP.Length > 255 || validateIPv4(chosenIP) == false)
            {
                Console.WriteLine("Incorrect IP. Please try again.");
                showManualIPEntry();
                return;
            }

            Config.publicServerIP = chosenIP;
            Console.WriteLine("Using option M) {0} as the default external IP for this node.", chosenIP);           
        }

        // Checks to see if this node can handle the block number
        static public bool checkCurrentBlockDeprecation(ulong block)
        {
            ulong block_limit = Config.compileTimeBlockNumber + Config.deprecationBlockOffset;

            if(block > block_limit)
            {
                return false;
            }

            return true;
        }

        // Checks the current balance of the masternode
        static public bool checkMasternodeBalance()
        {
            // First check if the block processor is running
            if (blockProcessor.operating == true)
            {
                if (Node.blockChain.getLastBlockNum() > 2)
                {
                    IxiNumber nodeBalance = walletState.getWalletBalance(walletStorage.address);
                    if(isWorkerNode())
                    {
                        if (nodeBalance > CoreConfig.minimumMasterNodeFunds)
                        {
                            Logging.info(string.Format("Your balance is more than the minimum {0} IXIs needed to operate a masternode. Reconnecting as a masternode.", nodeBalance));
                            convertToMasterNode();
                        }
                    }
                    else
                    if (nodeBalance < CoreConfig.minimumMasterNodeFunds)
                    {
                        Logging.error(string.Format("Your balance is less than the minimum {0} IXIs needed to operate a masternode.\nSend more IXIs to {1} and restart the node.", CoreConfig.minimumMasterNodeFunds, walletStorage.address));
                        Node.stop();
                        running = false;
                        return false;
                    }
                }
            }
            // Masternode has enough IXIs to proceed
            return true;
        }

        // Helper for validating IPv4 addresses
        static private bool validateIPv4(string ipString)
        {
            if (String.IsNullOrWhiteSpace(ipString))
            {
                return false;
            }

            string[] splitValues = ipString.Split('.');
            if (splitValues.Length != 4)
            {
                return false;
            }

            byte tempForParsing;
            return splitValues.All(r => byte.TryParse(r, out tempForParsing));
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
                    Logging.trace(String.Format(" -> block #{0}, signatures: {1}, checksum: {2}, wsChecksum: {3}.", blockNum, Node.blockChain.getBlockSignaturesReverse(i), 
                        Crypto.hashToString(block.blockChecksum), Crypto.hashToString(block.walletStateChecksum)));
            }
            Logging.trace(String.Format(" -> Block processor is operating: {0}.", Node.blockProcessor.operating));
            Logging.trace(String.Format(" -> Block processor is synchronizing: {0}.", Node.blockSync.synchronizing));
            Logging.trace(String.Format(" -> Current consensus number: {0}.", Node.blockChain.getRequiredConsensus()));
            Console.ResetColor();
        }

        public static bool isElectedToGenerateNextBlock(int offset = 0)
        {
            byte[] pubKey = blockChain.getLastElectedNodePubKey(offset);
            if(pubKey == null || pubKey.SequenceEqual(walletStorage.publicKey))
            {
                return true;
            }

            return false;
        }

        // Cleans the storage cache and logs
        public static bool cleanCacheAndLogs()
        {
            if(File.Exists(Storage.filename))
            {
                File.Delete(Storage.filename);
            }

            PeerStorage.deletePeersFile();

            Logging.clear();

            Logging.info("Cleaned cache and logs.");
            return true;
        }

        // Sends perioding keepalive network messages
        private static void keepAlive()
        {
            while (autoKeepalive)
            {
                // Wait x seconds before rechecking
                for (int i = 0; i < CoreConfig.keepAliveInterval; i++)
                {
                    if (autoKeepalive == false)
                    {
                        Thread.Yield();
                        return;
                    }
                    // Sleep for one second
                    Thread.Sleep(1000);
                }


                try
                {
                    // Prepare the keepalive message
                    using (MemoryStream m = new MemoryStream())
                    {
                        using (BinaryWriter writer = new BinaryWriter(m))
                        {
                            writer.Write(keepAliveVersion);

                            byte[] wallet = walletStorage.address;
                            writer.Write(wallet.Length);
                            writer.Write(wallet);

                            writer.Write(Config.device_id);

                            // Add the unix timestamp
                            long timestamp = Core.getCurrentTimestamp();
                            writer.Write(timestamp);

                            string hostname = Node.getFullAddress();
                            writer.Write(hostname);

                            // Add a verifiable signature
                            byte[] private_key = walletStorage.privateKey;
                            byte[] signature = CryptoManager.lib.getSignature(Encoding.UTF8.GetBytes(CoreConfig.ixianChecksumLockString + "-" + Config.device_id + "-" + timestamp + "-" + hostname), private_key);
                            writer.Write(signature.Length);
                            writer.Write(signature);

                            PresenceList.curNodePresenceAddress.lastSeenTime = timestamp;
                            PresenceList.curNodePresenceAddress.signature = signature;
                        }


                        // Update self presence
                        PresenceList.receiveKeepAlive(m.ToArray());

                        // Send this keepalive message to all connected clients
                        ProtocolMessage.broadcastProtocolMessage(ProtocolMessageCode.keepAlivePresence, m.ToArray());
                    }
                }
                catch (Exception)
                {
                    continue;
                }

            }

            Thread.Yield();
        }



        public static string getFullAddress()
        {
            return Config.publicServerIP + ":" + Config.serverPort;
        }

        // Convert this masternode to a worker node
        public static void convertToWorkerNode()
        {
            if (workerMode)
                return;

            workerMode = true;
            PresenceList.curNodePresenceAddress.type = 'W';

            NetworkClientManager.restartClients();
            NetworkServer.stopNetworkOperations();
        }

        // Convert this worker node to a masternode
        public static void convertToMasterNode()
        {
            if (!workerMode)
                return;

            workerMode = false;
            if (Config.storeFullHistory)
            {
                PresenceList.curNodePresenceAddress.type = 'H';
            }
            else
            {
                PresenceList.curNodePresenceAddress.type = 'M';
            }

            NetworkClientManager.restartClients();
            NetworkServer.restartNetworkOperations();
            
        }

        public static bool isWorkerNode()
        {
            return workerMode;
        }


    }
}
