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

        // Starts the Network Client Manager. First it connects to one of the seed nodes in order to fetch the Presence List.
        // Afterwards, it starts the reconnect and keepalive threads
        public static void start()
        {
            networkClients = new List<NetworkClient>();

            // Connect to a random seed node first
            Random rnd = new Random();
            bool firstSeedConnected = false;
            while (firstSeedConnected == false)
            {
                firstSeedConnected = connectTo(CoreNetworkUtils.seedNodes[rnd.Next(CoreNetworkUtils.seedNodes.Length)]);
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

        // Resume the initial connection handling
        public static void resumeConnections()
        {
            Thread th = new Thread(() =>
            {
                // Create clients and connect to various nodes
                for (int i = 0; i < CoreNetworkUtils.seedNodes.Length; i++)
                {
                    // Connect client immediately
                    connectTo(CoreNetworkUtils.seedNodes[i]);

                    // Wait before connecting to another client
                    Thread.Sleep(2000);
                }
                Thread.Yield();
            });
            th.Start();
        }

        public static void stop()
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
            stop();
            Thread.Sleep(100);
            Logging.info("Starting network clients...");
            start();
        }

        // Connects to a specified node, with the syntax host:port
        private static bool threadConnectTo(object data)
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
                return false;
            }

            // Resolve the hostname first
            string resolved_server_name = NetworkUtils.resolveHostname(server[0]);

            // Verify against the publicly disclosed ip
            // Don't connect to self
            if (resolved_server_name.Equals(Config.publicServerIP, StringComparison.Ordinal))
            {
                if (server[1].Equals(string.Format("{0}", Config.serverPort), StringComparison.Ordinal))
                {
                    Logging.info(string.Format("Skipping connection to public self seed node {0}", host));
                    return false;
                }
            }

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
                        return false;
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
                        return false;
                    }
                }
            }

            // Connect to the specified node
            NetworkClient new_client = new NetworkClient();
            // Recompose the connection address from the resolved IP and the original port
            bool result = new_client.connectToServer(resolved_server_name, Convert.ToInt32(server[1]));

            // Add this node to the client list if connection was successfull
            if (result == true)
            {
                lock(networkClients)
                {
                    networkClients.Add(new_client);
                }
            }

            return result;
        }

        // Connects to a specified node, with the syntax host:port
        // It does so by spawning a temporary thread
        public static bool connectTo(string host)
        {
            //Thread conn_thread = new Thread(threadConnectTo);
            //conn_thread.Start(host);
            return threadConnectTo(host);
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

        public static bool sendToClient(string neighbor, ProtocolMessageCode code, byte[] data)
        {
            lock(networkClients)
            {
                foreach(NetworkClient c in networkClients)
                {
                    if(c.getFullAddress() == neighbor)
                    {
                        c.sendData(code, data);
                        return true;
                    }
                }
            }
            return false;
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

        // Scans the Presence List for a new potential neighbor. Returns null if no new neighbor is found.
        public static string scanForNeighbor()
        {
            // Cache the connected clients string array first for faster comparisons
            string[] connectedClients = getConnectedClients();

            // Prepare a list of candidate nodes
            List<string> candidates = new List<string>();
            Random rnd = new Random();

            lock (PresenceList.presences)
            {
                foreach (Presence presence in PresenceList.presences)
                {
                    // Find only masternodes
                    foreach (PresenceAddress addr in presence.addresses)
                    {
                        if (addr.type == 'M')
                        {
                            // Check if the address format is correct
                            string[] server = addr.address.Split(':');
                            if (server.Count() < 2)
                            {
                                continue;
                            }

                            bool addr_valid = true;

                            // Check if we are already connected to this address
                            lock (networkClients)
                            {
                                foreach (NetworkClient client in networkClients)
                                {
                                    if (client.address.Equals(addr.address, StringComparison.Ordinal))
                                    {
                                        // Address is already in the client list
                                        addr_valid = false;
                                        break;
                                    }
                                }
                            }

                            // Next, check if we're connecting to a self address of this node

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
                                        addr_valid = false;
                                    }
                                }
                            }

                            // If the address is valid, add it to the candidates
                            if (addr_valid)
                            {
                                candidates.Add(addr.address);
                            }
                        }
                    }
                }
            }


            if (candidates.Count < 1)
                    return null;

            string candidate = candidates[rnd.Next(candidates.Count)];
            return candidate;
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
                    // Check if we need to connect to more neighbors
                    if(networkClients.Count < Config.simultaneousConnectedNeighbors)
                    {
                        // Scan for and connect to a new neighbor
                        string neighbor = scanForNeighbor();
                        if(neighbor != null)
                        {
                            Logging.info(string.Format("Attempting to add new neighbor: {0}", neighbor));
                            connectTo(neighbor);
                        }
                    }

                    // Prepare a list of failed clients
                    List<NetworkClient> failed_clients = new List<NetworkClient>();

                    foreach (NetworkClient client in networkClients)
                    {
                        // Check if we exceeded the maximum reconnect count
                        if (client.getFailedReconnectsCount() >= Config.maximumNeighborReconnectCount)
                        {
                            // Remove this client so we can search for a new neighbor
                            failed_clients.Add(client);
                        }
                        else
                        {
                            // Everything is in order, send a ping message
                            client.sendPing();
                        }
                    }

                    // Go through the list of failed clients and remove them
                    foreach (NetworkClient client in failed_clients)
                    {
                        networkClients.Remove(client);
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

                            string publicHostname = string.Format("{0}:{1}", Config.publicServerIP, Config.serverPort);
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
