using DLT.Meta;
using DLT.Network;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DLT
{
    class NetworkClientManager
    {
        private static List<NetworkClient> networkClients = new List<NetworkClient>();
        private static Thread reconnectThread;
        private static bool autoReconnect = true;

        private static Thread keepAliveThread;
        private static bool autoKeepalive = false;

        private static NetworkClientManager singletonInstance = new NetworkClientManager();
        static NetworkClientManager()
        {
        }

        private NetworkClientManager()
        {
        }

        public static NetworkClientManager singleton
        {
            get
            {
                return singletonInstance;
            }
        }


        public static void startClients()
        {
            networkClients = new List<NetworkClient>();

            lock (networkClients)
            {
                // Create clients and connect to various nodes
                for (int i = 0; i < CoreNetworkUtils.seedNodes.Length; i++)
                {
                    // Connect client immediately
                    connectTo(CoreNetworkUtils.seedNodes[i]);
                }
            }

            // Start the reconnect thread
            reconnectThread = new Thread(reconnectClients);
            autoReconnect = true;
            reconnectThread.Start();

            // Start the keepalive thread
            autoKeepalive = true;
            keepAliveThread = new Thread(keepAlive);
            keepAliveThread.Start();

        }

        public static void stopClients()
        {
            autoKeepalive = false;
            autoReconnect = false;
            isolate();

            // Force stopping of reconnect thread
            if (reconnectThread == null)
                return;
            reconnectThread.Abort();
        }

        // Immediately disconnects all clients
        public static void isolate()
        {
            Logging.info("Isolating network clients...");

            lock (networkClients)
            {
                // Disconnect each client
                foreach (NetworkClient client in networkClients)
                {
                    client.disconnect();
                }

                // Empty the client list
                networkClients.Clear();
            }
        }

        // Reconnects to network clients
        public static void restartClients()
        {
            Logging.info("Stopping network clients...");
            stopClients();
            Thread.Sleep(100);
            Logging.info("Starting network clients...");
            startClients();
        }

        // Connects to a specified node, with the syntax host:port
        private static void threadConnectTo(object data)
        {
            if (data is string)
            {

            }
            else
            {
                throw new Exception(String.Format("Exception in client connection thread {0}", data.GetType().ToString()));
            }
            string host = (string)data;

            string[] server = host.Split(':');
            if (server.Count() < 2)
            {
                Logging.warn(string.Format("Cannot connect to invalid hostname: {0}", host));
                return;
            }

            // Resolve the hostname first
            string resolved_server_name = NetworkUtils.resolveHostname(server[0]);

            // Get all self addresses and run through them
            List<string> self_addresses = CoreNetworkUtils.GetAllLocalIPAddresses();
            foreach (string self_address in self_addresses)
            {
                // Don't connect to self
                if (resolved_server_name.Equals(self_address, StringComparison.Ordinal))
                {
                    if (server[1].Equals(string.Format("{0}", Config.serverPort), StringComparison.Ordinal))
                    {
                        Logging.info(string.Format("Skipping connection to self seed node {0}", host));
                        return;
                    }
                }
            }

            // Check if nodes is already in the client list
            lock (networkClients)
            {
                foreach (NetworkClient client in networkClients)
                {
                    if(client.address.Equals(host, StringComparison.Ordinal))
                    {
                        // Address is already in the client list
                        return;
                    }
                }
            }

            // Connect to the specified node
            NetworkClient new_client = new NetworkClient();
            bool result = new_client.connectToServer(server[0], Convert.ToInt32(server[1]));

            // Add this node to the client list if connection was successfull
            if (result == true)
            {
                lock(networkClients)
                {
                    networkClients.Add(new_client);
                }
            }

            Thread.Yield();
        }

        // Connects to a specified node, with the syntax host:port
        // It does so by spawning a temporary thread
        public static void connectTo(string host)
        {
            Thread conn_thread = new Thread(threadConnectTo);
            conn_thread.Start(host);
        }

        // Send data to all connected nodes
        public static void broadcastData(ProtocolMessageCode code, byte[] data, Socket skipSocket = null)
        {
            lock (networkClients)
            {
                foreach (NetworkClient client in networkClients)
                {
                    if (client.isConnected())
                    {
                        if (skipSocket != null)
                        {
                            if (client.tcpClient.Client == skipSocket)
                                continue;
                        }

                        client.sendData(code, data);
                        //Console.WriteLine("CLNMGR-BROADCAST SENT: {0}", code);
                    }
                }
            }
        }

        // Returns all the connected clients
        public static string[] getConnectedClients()
        {
            List<String> result = new List<String>();

            lock (networkClients)
            {
                foreach (NetworkClient client in networkClients)
                {
                    if (client.isConnected())
                    {
                        try
                        {
                            string client_name = client.getFullAddress();
                            result.Add(client_name);
                        }
                        catch (Exception e)
                        {
                            Logging.warn(string.Format("NetworkClientManager->getConnectedClients: {0}", e.ToString()));
                        }
                    }
                }
            }

            return result.ToArray();
        }


        // Checks for missing clients
        private static void reconnectClients()
        {
            // Wait 5 seconds before starting the loop
            Thread.Sleep(Config.networkClientReconnectInterval);

            while (autoReconnect)
            {
                lock (networkClients)
                {
                    foreach (NetworkClient client in networkClients)
                    {
                        client.sendPing();
                    }
                }

                // Wait 5 seconds before rechecking
                Thread.Sleep(Config.networkClientReconnectInterval);
            }

            Thread.Yield();
        }

        // Sends perioding keepalive network messages
        private static void keepAlive()
        {
            while (autoKeepalive)
            {
                // Wait x seconds before rechecking
                for (int i = 0; i < Config.keepAliveInterval; i++)
                {
                    if(autoKeepalive == false)
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

                            string publicHostname = string.Format("{0}:{1}", NetworkServer.publicIPAddress, Config.serverPort);
                            string wallet = Node.walletStorage.address;
                            writer.Write(wallet);
                            writer.Write(Config.device_id);
                            writer.Write(publicHostname);

                            // Add the unix timestamp
                            string timestamp = Clock.getTimestamp(DateTime.Now);
                            writer.Write(timestamp);

                            // Add a verifiable signature
                            string private_key = Node.walletStorage.privateKey;
                            string signature = CryptoManager.lib.getSignature(timestamp, private_key);
                            writer.Write(signature);

                        }

                        // Update self presence
                        PresenceList.receiveKeepAlive(m.ToArray());

                        // Send this keepalive message to all connected clients
                        lock (networkClients)
                        {
                            foreach (NetworkClient client in networkClients)
                            {
                                if (client.isConnected())
                                {
                                    client.sendKeepAlive(m.ToArray());
                                }
                            }
                        }
                    }
                }
                catch (Exception)
                {
                    continue;
                }

            }

            Thread.Yield();
        }

    }
}
