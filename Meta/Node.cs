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

        static public void start()
        {
            // Load or Generate the wallet
            walletStorage = new WalletStorage(Config.walletFile);

            // Initialize the wallet state
            WalletState.generateWalletState();

            // Initialize storage
            Storage.prepareStorage();

            // Initialize the block chain
            blockChain = new BlockChain();

            // Create the block processor
            blockProcessor = new BlockProcessor();

            // Show the IP selector menu
            showIPmenu();


            // Check if this is a genesis node
            if (Config.genesisFunds > 0)
            {
                genesisNode = true;

                // Start the node server if genesis
                NetworkServer.beginNetworkOperations();

                // Generate the initial presence list
                PresenceList.generatePresenceList(Config.publicServerIP);

                // Stop at here since it's a genesis node
                return;
            }

            // Start the network client manager
            NetworkClientManager.startClients();
        }

        static public void update()
        {
            // Check passed time since last block generation and if needed generate a new block
            blockProcessor.onUpdate();

            // Cleanup the presence list
            // TODO: optimize this by using a different thread perhaps
            PresenceList.performCleanup();

            if(serverStarted == false)
            {
                if(Node.blockProcessor.synchronized == true)
                {
                    Console.WriteLine("Starting Network Server now.");
                    // Start the node server
                    NetworkServer.beginNetworkOperations();
                    serverStarted = true;
                }
            }
        }

        static public void stop()
        {
            // Stop all network clients
            NetworkClientManager.stopClients();
            
            // Stop the network server
            NetworkServer.stopNetworkOperations();
        }

        static public void reconnect()
        {
            // Reconnect server and clients
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
                Console.Write("Type Manual IP: ");
                string chosenIP = Console.ReadLine();
                if (chosenIP != null && chosenIP.Length < 255)
                {
                    Config.publicServerIP = chosenIP;
                    Console.WriteLine("Using option M) {0} as the default external IP for this node.", chosenIP);
                }
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

    }
}
