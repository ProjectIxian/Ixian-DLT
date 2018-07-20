using DLT.Network;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DLT.Meta
{
    class Node
    {

        public static BlockChain blockChain;
        public static BlockProcessor blockProcessor;
        public static WalletStorage walletStorage;


        public static bool genesisNode = false;


        public static bool serverStarted = false;
        public static bool presenceListActive = false;

        private static bool running = false;

        static public void start()
        {
            running = true;

            // Load or Generate the wallet
            walletStorage = new WalletStorage(Config.walletFile);

            // Initialize the wallet state
            WalletState.generateWalletState();

            // Initialize storage
            //Storage.prepareStorage();

            // Initialize the block chain
            blockChain = new BlockChain();

            // Create the block processor
            blockProcessor = new BlockProcessor();

            // Show the IP selector menu
            showIPmenu();

            /*if (Config.recoverFromFile)
            {
                Storage.readFromStorage();

                // Generate the initial presence list
                PresenceList.generatePresenceList(Config.publicServerIP);
            }
            else*/
            // Check if this is a genesis node
            if (Config.genesisFunds > 0)
            {
                genesisNode = true;

                // Start the node server if genesis
                NetworkServer.beginNetworkOperations();

                // Stop at here since it's a genesis node
                return;
            }
            PresenceList.generatePresenceList(Config.publicServerIP);

            // Start the network client manager
            NetworkClientManager.startClients();
        }

        static public bool update()
        {
            // Check passed time since last block generation and if needed generate a new block
            blockProcessor.onUpdate();

            // Update redacted blockchain
            blockChain.onUpdate();

            // Cleanup the presence list
            // TODO: optimize this by using a different thread perhaps
            PresenceList.performCleanup();

            if(serverStarted == false)
            {
                if(Node.blockProcessor.synchronized == true)
                {
                    Console.WriteLine("Resuming client connections.");

                    // Connect to the rest of the clients
                    NetworkClientManager.resumeConnections();

                    Console.WriteLine("Starting Network Server now.");

                    // Start the node server
                    NetworkServer.beginNetworkOperations();
                    serverStarted = true;
                }
            }

            return running;
        }

        static public void stop()
        {
            // Stop all network clients
            NetworkClientManager.stopClients();
            
            // Stop the network server
            NetworkServer.stopNetworkOperations();

            presenceListActive = false;
        }

        static public void reconnect()
        {
            // Reconnect server and clients
            presenceListActive = false;
            NetworkServer.restartNetworkOperations();
            NetworkClientManager.restartClients();
        }

        static public void synchronize()
        {
            // Clear everything and force a resynchronization
            Console.WriteLine("\n\n\tSynchronizing to network...\n");

            blockProcessor = new BlockProcessor();
            blockChain = new BlockChain();
            WalletState.clear();
            TransactionPool.clear();

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

    }
}
