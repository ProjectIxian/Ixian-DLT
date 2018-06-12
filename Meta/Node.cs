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

            // Start the node server
            NetworkServer.beginNetworkOperations();

            // Check if this is a genesis node
            if (Config.genesisFunds > 0)
            {
                genesisNode = true;

                // Generate the initial presence list
                PresenceList.generatePresenceList();

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


    }
}
