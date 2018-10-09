using DLT.Network;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;

namespace DLT.Meta
{
    class Node
    {

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

        private static bool running = false;

        private static Thread keepAliveThread;
        private static bool autoKeepalive = false;

        public static long networkTimeDifference = 0;

        static public void start()
        {
            running = true;

            // Load or Generate the wallet
            walletStorage = new WalletStorage(Config.walletFile);


            // Initialize the wallet state
            walletState = new WalletState();

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

            // Start the network queue
            NetworkQueue.start();

            // Distribute genesis funds
            IxiNumber genesisFunds = new IxiNumber(Config.genesisFunds);

            // Check if this is a genesis node
            if (genesisFunds > (long)0)
            {
                Logging.info(String.Format("Genesis {0} specified. Starting operation.", genesisFunds));

                Transaction tx = new Transaction();
                tx.type = (int)Transaction.Type.Genesis;
                tx.to = walletStorage.address;
                tx.from = "IxianInfiniMine2342342342342342342342342342342342342342342342342db32";
                tx.amount = genesisFunds;
                tx.data = "";
                tx.timeStamp = getCurrentTimestamp().ToString();
                tx.id = tx.generateID();
                tx.checksum = Transaction.calculateChecksum(tx);
                tx.signature = Transaction.getSignature(tx.checksum);
                TransactionPool.addTransaction(tx);

                Transaction tx2 = new Transaction(new IxiNumber("1000000"), 0, "b0052ef5407605f9bd668fc699a550ca2dc5de4720017f1ad813c8e06b6235697849", "IxianInfiniMine2342342342342342342342342342342342342342342342342db32");
                tx2.type = (int)Transaction.Type.Genesis;
                tx2.data = "";
                tx2.timeStamp = getCurrentTimestamp().ToString();
                tx2.id = tx2.generateID();
                tx2.checksum = Transaction.calculateChecksum(tx2);
                tx2.signature = Transaction.getSignature(tx2.checksum);
                TransactionPool.addTransaction(tx2);

                Transaction tx3 = new Transaction(new IxiNumber("1000000"), 0, "2bd883f4db2535879132e67d762f8871708927beb2a0fcc8f9f715125fc9a6c4b567", "IxianInfiniMine2342342342342342342342342342342342342342342342342db32");
                tx3.type = (int)Transaction.Type.Genesis;
                tx3.data = "";
                tx3.timeStamp = getCurrentTimestamp().ToString();
                tx3.id = tx3.generateID();
                tx3.checksum = Transaction.calculateChecksum(tx3);
                tx3.signature = Transaction.getSignature(tx3.checksum);
                TransactionPool.addTransaction(tx3);

                Transaction tx4 = new Transaction(new IxiNumber("1000000"), 0, "9f51590772405d88278c160df993892148cb44ba40ab0785fd5afbba7a0ddce0bdc9", "IxianInfiniMine2342342342342342342342342342342342342342342342342db32");
                tx4.type = (int)Transaction.Type.Genesis;
                tx4.data = "";
                tx4.timeStamp = getCurrentTimestamp().ToString();
                tx4.id = tx4.generateID();
                tx4.checksum = Transaction.calculateChecksum(tx4);
                tx4.signature = Transaction.getSignature(tx4.checksum);
                TransactionPool.addTransaction(tx4);

                genesisNode = true;
                Node.blockProcessor.resumeOperation();
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

                    // Start the miner
                    miner = new Miner();
                    miner.start();
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
                Logging.error(string.Format("Your DLT node can only handle blocks up to #{0}. Please update to the latest version from www.ixian.io", block_limit));
                Node.stop();
                running = false;
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
                    if (nodeBalance < Config.minimumMasterNodeFunds)
                    {
                        Logging.error(string.Format("Your balance is less than the minimum {0} IXIs needed to operate a masternode.\nSend more IXIs to {1} and restart the node.", Config.minimumMasterNodeFunds, walletStorage.address));
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
            Logging.trace(String.Format(" -> Last Block Checksum: {0}.", Node.blockChain.getLastBlockChecksum()));
            Logging.trace("Last five signature counts:");
            for (int i = 0; i < 6; i++)
            {
                ulong blockNum = Node.blockChain.getLastBlockNum() - (ulong)i;
                Block block = Node.blockChain.getBlock(blockNum);

                if(block != null)
                    Logging.trace(String.Format(" -> block #{0}, signatures: {1}, checksum: {2}, wsChecksum: {3}.", blockNum, Node.blockChain.getBlockSignaturesReverse(i), block.blockChecksum, block.walletStateChecksum));
            }
            Logging.trace(String.Format(" -> Block processor is operating: {0}.", Node.blockProcessor.operating));
            Logging.trace(String.Format(" -> Block processor is synchronizing: {0}.", Node.blockSync.synchronizing));
            Logging.trace(String.Format(" -> Current consensus number: {0}.", Node.blockChain.getRequiredConsensus()));
            Console.ResetColor();
        }

        public static bool isElectedToGenerateNextBlock(int offset = 0)
        {
            string pubKey = blockChain.getLastElectedNodePubKey(offset);
            if(pubKey == null || pubKey == walletStorage.publicKey)
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
                for (int i = 0; i < Config.keepAliveInterval; i++)
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

                            string publicHostname = string.Format("{0}:{1}", Config.publicServerIP, Config.serverPort);
                            string wallet = Node.walletStorage.address;
                            writer.Write(wallet);
                            writer.Write(Config.device_id);
                            writer.Write(publicHostname);

                            // Add the unix timestamp
                            string timestamp = Node.getCurrentTimestamp().ToString();
                            writer.Write(timestamp);

                            // Add a verifiable signature
                            string private_key = Node.walletStorage.privateKey;
                            string signature = CryptoManager.lib.getSignature(Config.device_id + "-" + timestamp + "-" + publicHostname, private_key);
                            writer.Write(signature);

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

        // TODO everything connected to networkTimeDifference can probably be solved better
        public static long getCurrentTimestamp()
        {
            return (long)(Clock.getTimestamp(DateTime.Now) - networkTimeDifference);
        }

    }
}
