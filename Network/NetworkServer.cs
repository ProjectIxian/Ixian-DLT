using System;
using System.Collections.Generic;
using System.Threading;
using System.Net;
using System.Net.Sockets;
using DLT.Meta;
using System.Linq;
using System.IO;

namespace DLT
{
    namespace Network
    {
        public struct NetOpsData
        {
            public IPEndPoint listenAddress;
        }

        public class NetworkServer
        {
            public static string publicIPAddress = "127.0.0.1";


            private static bool continueRunning;
            private static Thread netControllerThread = null;
            private static TcpListener listener;
            private static List<RemoteEndpoint> connectedClients = new List<RemoteEndpoint>();
            private static Thread pingThread;
            private static NetworkServer singletonInstance = new NetworkServer();

            static NetworkServer()
            {
            }

            private NetworkServer()
            {
            }

            public static NetworkServer singleton
            {
                get
                {
                    return singletonInstance;
                }
            }




            public static void beginNetworkOperations()
            {
                if (netControllerThread != null)
                {
                    // already running
                    Logging.info("Network server thread is already running.");
                    return;
                }

                netControllerThread = new Thread(networkOpsLoop);
                connectedClients = new List<RemoteEndpoint>();
                continueRunning = true;

                // Read the server port from the configuration
                NetOpsData nod = new NetOpsData();
                nod.listenAddress = new IPEndPoint(IPAddress.Any, Config.serverPort);
                netControllerThread.Start(nod);

                // Retrieve the public-accessible IP address
                publicIPAddress = Config.publicServerIP; // CoreNetworkUtils.GetLocalIPAddress();

              //  WebClient client = new WebClient();
            //    publicIPAddress = client.DownloadString("http://seed1.ixian.io/myip");

                Logging.info(string.Format("Public network node address: {0} port {1}", publicIPAddress, Config.serverPort));

                // Finally, start the ping thread
                pingThread = new Thread(pingLoop);
                pingThread.Start();
            }

            public static void stopNetworkOperations()
            {
                if (netControllerThread == null)
                {
                    // not running
                    Logging.info("Network server thread was already halted.");
                    return;
                }
                continueRunning = false;

                // Close blocking socket
                listener.Stop();

                Logging.info("Closing network server connected clients");
                // Clear all clients
                lock(connectedClients)
                {
                    // Immediately close all connected client sockets
                    foreach(RemoteEndpoint client in connectedClients)
                    {
                        client.abort();
                    }

                    connectedClients.Clear();
                }

                netControllerThread.Abort();
                netControllerThread = null;
                pingThread.Abort();
            }

            // Restart the network server
            public static void restartNetworkOperations()
            {
                Logging.info("Stopping network server...");
                stopNetworkOperations();
                Thread.Sleep(1000);
                Logging.info("Restarting network server...");
                beginNetworkOperations();
            }

            private static void networkOpsLoop(object data)
            {
                if (data is NetOpsData)
                {
                    try
                    {
                        NetOpsData netOpsData = (NetOpsData)data;
                        listener = new TcpListener(netOpsData.listenAddress);
                        listener.Start();
                    }
                    catch(Exception e)
                    {
                        Logging.error(string.Format("Exception starting server: {0}", e.ToString()));
                        return;
                    }
                }
                else
                {
                    Logging.error(String.Format("NetworkServer.networkOpsLoop called with incorrect data object. Expected 'NetOpsData', got '{0}'", data.GetType().ToString()));
                    return;
                }
                // housekeeping tasks
                while (continueRunning)
                {
                    if (connectedClients.Count < Config.maximumServerClients)
                    {
                        // Use a blocking mechanism
                        try
                        {
                            Socket handlerSocket = listener.AcceptSocket();
                            acceptConnection(handlerSocket);
                        }
                        catch (SocketException)
                        {
                            // Could be an interupt request
                        }
                    }

                    // Sleep to prevent cpu usage
                    Thread.Sleep(100);

                }
                Logging.info("Server listener thread ended.");
                Thread.Yield();
            }

            // Unified pinging thread for all connected stream clients
            private static void pingLoop()
            {
                // Only ping while networkops loop is active
                while (continueRunning)
                {
                    // Wait x seconds before rechecking
                    for (int i = 0; i < Config.keepAliveInterval; i++)
                    {
                        if (continueRunning == false)
                        {
                            Thread.Yield();
                            return;
                        }
                        // Sleep for one second
                        Thread.Sleep(1000);
                    }

                    // Prepare the keepalive message
                    using (MemoryStream m = new MemoryStream())
                    {
                        using (BinaryWriter writer = new BinaryWriter(m))
                        {
                            try
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
                            catch(Exception)
                            {
                                continue;

                            }

                            List<RemoteEndpoint> netClients = null;
                            lock (connectedClients)
                            {
                                netClients = new List<RemoteEndpoint>(connectedClients);
                            }
                            try
                            {
                                foreach (RemoteEndpoint endpoint in netClients)
                                {
                                    sendPing(endpoint, m.ToArray());
                                }
                            }
                            catch (Exception)
                            {

                            }
                        }
                    }

                }
                Thread.Yield();
            }

            // Send data to all connected clients
            public static void broadcastData(ProtocolMessageCode code, byte[] data, Socket skipSocket = null)
            {
                List<RemoteEndpoint> netClients = null;
                lock (connectedClients)
                {
                    netClients = new List<RemoteEndpoint>(connectedClients);
                }
                foreach (RemoteEndpoint endpoint in netClients)
                {
                    // TODO: filter messages based on presence node type
                    if(skipSocket != null)
                    {
                        if (endpoint.clientSocket == skipSocket)
                            continue;
                    }

                    endpoint.sendData(code, data);
                }
            }

            public static bool sendToClient(string neighbor, ProtocolMessageCode code, byte[] data)
            {
                RemoteEndpoint client = null;
                lock (connectedClients)
                {
                    foreach (RemoteEndpoint ep in connectedClients)
                    {
                        foreach(PresenceAddress addr in ep.presence.addresses)
                        {
                            if(addr.address == neighbor)
                            {
                                client = ep;
                                break;
                            }
                        }
                    }
                }
                if (client != null)
                {
                    client.sendData(code, data);
                    return true;
                }
                return false;
            }

            // Send a ping packet to verify the connection status
            private static void sendPing(RemoteEndpoint endpoint, byte[] data)
            {
                if (endpoint == null)
                    return;

                endpoint.sendData(ProtocolMessageCode.keepAlivePresence, data);
            }

            // Returns all the connected clients
            public static string[] getConnectedClients()
            {
                List<String> result = new List<String>();

                lock (connectedClients)
                {
                    foreach (RemoteEndpoint rclient in connectedClients)
                    {
                        if (rclient.presence is null) continue; // ignore, if the clients are in the process of connecting and have not yet sent their identity
                        foreach(PresenceAddress addr in rclient.presence.addresses)
                        {
                            try
                            {
                                string client_name = addr.address; //client.getFullAddress();
                                result.Add(client_name);
                            }
                            catch (Exception e)
                            {
                                Logging.warn(string.Format("NetworkServer->getConnectedClients: {0}", e.ToString()));
                            }
                        }
                    }
                }

                return result.ToArray();
            }

            private static void acceptConnection(Socket clientSocket)
            {
                IPEndPoint clientEndpoint = (IPEndPoint)clientSocket.RemoteEndPoint;

                // Add timeouts and set socket options
                //clientSocket.ReceiveTimeout = 5000;
                //clientSocket.SendTimeout = 5000;
                clientSocket.LingerState = new LingerOption(true, 3);
                clientSocket.NoDelay = true;
                clientSocket.Blocking = true;

                // Setup the remote endpoint
                RemoteEndpoint remoteEndpoint = new RemoteEndpoint();
                remoteEndpoint.remoteIP = clientEndpoint;
                remoteEndpoint.clientSocket = clientSocket;
                remoteEndpoint.presence = null;
                remoteEndpoint.presenceAddress = null;
                remoteEndpoint.state = RemoteEndpointState.Initial;

                lock (connectedClients)
                {
                    if (connectedClients.Count + 1 > Config.maximumServerClients)
                    {
                        Logging.warn(string.Format("Maximum number of connected clients reached. Disconnecting client: {0}:{1}",
                            clientEndpoint.Address.ToString(), clientEndpoint.Port));
                        clientSocket.Disconnect(true);
                        clientSocket.Shutdown(SocketShutdown.Both);
                        return;
                    }

                    var existing_clients = connectedClients.Where(re => re.remoteIP.Address == clientEndpoint.Address);
                    if (existing_clients.Count() > 0)
                    {
                        Logging.warn(String.Format("Client {0}:{1} already connected as {2}.",
                            clientEndpoint.Address.ToString(), clientEndpoint.Port, existing_clients.First().ToString()));
                        // TODO: handle this situation (client already connected)
                    }

                    connectedClients.Add(remoteEndpoint);
                }

                Logging.info(String.Format("Client connection accepted: {0} | #{1}/{2}", clientEndpoint.ToString(), connectedClients.Count + 1, Config.maximumServerClients));

                remoteEndpoint.start();
            }

            // Removes an endpoint from the connected clients list
            public static bool removeEndpoint(RemoteEndpoint endpoint)
            {
                bool result = false;
                lock (connectedClients)
                {
                    result = connectedClients.Remove(endpoint);
                }
                return result;
            }
    

        }
    }
}
