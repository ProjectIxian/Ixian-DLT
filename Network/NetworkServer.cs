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
            public static List<String> neighborClients = new List<String>();


            private static bool continueRunning;
            private static Thread netControllerThread;
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


            public static bool addNeighbor(string hostname)
            {
                // Detect if neighbor hostname syntax is valid
                string[] server = hostname.Split(':');
                if (server.Count() < 2)
                {
                    Logging.warn(string.Format("Cannot connect to invalid potential neightbor hostname: {0}", hostname));
                    return false;
                }

                // Don't connect to self
                if (server[0].Equals(CoreNetworkUtils.GetLocalIPAddress(), StringComparison.Ordinal))
                {
                    if (server[1].Equals(string.Format("{0}", Config.serverPort), StringComparison.Ordinal))
                    {
                        // Silently ignore
                        return false;
                    }
                }

                // Check if neighbor is connected as client already
                foreach(string client in NetworkClientManager.getConnectedClients())
                {
                    if (client.Equals(hostname, StringComparison.Ordinal))
                        return false;
                }

                lock (neighborClients)
                {
                    // Check for duplicates
                    foreach (string neighbor in neighborClients)
                    {
                        // Ignore duplicates
                        if (neighbor.Equals(hostname, StringComparison.Ordinal))
                            return false;
                    }

                    // Add to neighbors list
                    neighborClients.Add(hostname);
                    return true;
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
                    Logging.info("Network server thread was halted.");
                    return;
                }
                continueRunning = false;

                // Close blocking socket
                listener.Stop();

                Logging.info("Closing network server connected clients");
                // Clear all clients
                //lock(connectedClients)
                {
                    // Immediately close all connected client sockets
                    foreach(RemoteEndpoint client in connectedClients)
                    {
                        client.state = RemoteEndpointState.Closed;
                    }
                }

                // Clear all neighbors
                //lock (neighborClients)
                {
                    neighborClients.Clear();
                }
                
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
                    
                    // check for new incoming connections
                    /*if (listener.Pending())
                    {
                        
                        try
                        {
                              listener.BeginAcceptSocket(new AsyncCallback(doAcceptConnection), null);
                        }
                        catch (SocketException socketException)
                        {
                            // log error while accepting connection, then simply drop it
                            Logging.warn("Error while accepting client connection: " + socketException.Message);
                        }
                    }
                    else
                    {
                        Thread.Sleep(100);
                    }*/

                    // Use a blocking mechanism
                    try
                    {
                        Socket handlerSocket = listener.AcceptSocket();
                        acceptConnection(handlerSocket);
                    }
                    catch(SocketException)
                    {
                        // Could be an interupt request
                    }

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

                            lock (connectedClients)
                            {
                                try
                                {
                                    //Console.WriteLine("~~~~");
                                    foreach (RemoteEndpoint endpoint in connectedClients)
                                    {
                                        //   Console.WriteLine("Ping {0}", endpoint.presence.addresses[0].address);
                                        Thread th = new Thread(() =>
                                        {
                                            sendPing(endpoint, m.ToArray());
                                            Thread.Yield();
                                        });
                                        th.Start();
                                    }
                                    // Console.WriteLine("~==~~");
                                }
                                catch (Exception)
                                {

                                }
                            }
                        }
                    }

                }
                Thread.Yield();
            }

            // Send data to all connected clients
            public static void broadcastData(ProtocolMessageCode code, byte[] data, Socket skipSocket = null)
            {
                lock (connectedClients)
                {
                    foreach (RemoteEndpoint endpoint in connectedClients)
                    {
                        // TODO: filter messages based on presence node type
                        if(skipSocket != null)
                        {
                            if (endpoint.clientSocket == skipSocket)
                                continue;
                        }

                        byte[] ba = ProtocolMessage.prepareProtocolMessage(code, data);
                        try
                        {
                            endpoint.clientSocket.Send(ba, SocketFlags.None);
                        }
                        catch (Exception)
                        {
                            // Report any issues related to sockets
                            // Logging.warn(string.Format("SRV: Socket exception for {0}. Info: {1}", endpoint.remoteIP, e.ToString()));
                        }
                    }
                }
            }

            // Send a ping packet to verify the connection status
            private static void sendPing(RemoteEndpoint endpoint, byte[] data)
            {
                if (endpoint == null)
                    return;

                // TODO: find a better way to detect near-instant disconnects
                byte[] ba = ProtocolMessage.prepareProtocolMessage(ProtocolMessageCode.keepAlivePresence, data);
                try
                {
                    endpoint.clientSocket.Send(ba, SocketFlags.None);
                    if (endpoint.clientSocket.Connected == false)
                    {
                        //Console.WriteLine("!!! Failed to ping remote endpoint.");
                        endpoint.state = RemoteEndpointState.Closed;
                    }
                }
                catch (Exception)
                {
                    //Console.WriteLine("Failed ping to client: {0}", e.ToString());
                    endpoint.state = RemoteEndpointState.Closed;
                }


            }

            // Returns all the connected clients
            public static string[] getConnectedClients()
            {
                List<String> result = new List<String>();

                lock (connectedClients)
                {
                    foreach (RemoteEndpoint rclient in connectedClients)
                    {
                      //  if (client.isConnected())
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

            //private static void doAcceptConnection(IAsyncResult ar)
            private static void acceptConnection(Socket clientSocket)
            {
                //Socket clientSocket = listener.EndAcceptSocket(ar);
                IPEndPoint clientEndpoint = (IPEndPoint)clientSocket.RemoteEndPoint;
                Logging.info(String.Format("Client connection accepted: {0}", clientEndpoint.ToString()));
                lock (connectedClients)
                {
                    var existing_clients = connectedClients.Where(re => re.remoteIP.Address == clientEndpoint.Address);
                    if (existing_clients.Count() > 0)
                    {
                        Logging.warn(String.Format("Client {0}:{1} already connected as {2}.",
                            clientEndpoint.Address.ToString(), clientEndpoint.Port, existing_clients.First().ToString()));
                        // TODO: handle this situation (client already connected)
                    }

                    // Setup socket keepalive mechanism
                    int size = sizeof(UInt32);
                    UInt32 on = 1;
                    UInt32 keepAliveInterval = 10000; // send a packet once every x seconds.
                    UInt32 retryInterval = 1000; // if no response, resend every second.
                    byte[] inArray = new byte[size * 3];
                    Array.Copy(BitConverter.GetBytes(on), 0, inArray, 0, size);
                    Array.Copy(BitConverter.GetBytes(keepAliveInterval), 0, inArray, size, size);
                    Array.Copy(BitConverter.GetBytes(retryInterval), 0, inArray, size * 2, size);
                    clientSocket.IOControl(IOControlCode.KeepAliveValues, inArray, null);

                    // Setup the remote endpoint
                    RemoteEndpoint remoteEndpoint = new RemoteEndpoint();
                    remoteEndpoint.remoteIP = clientEndpoint;
                    remoteEndpoint.clientSocket = clientSocket;
                    remoteEndpoint.presence = null;
                    remoteEndpoint.presenceAddress = null;
                    remoteEndpoint.state = RemoteEndpointState.Initial;
                    remoteEndpoint.thread = new Thread(clientLoop);
                    remoteEndpoint.thread.Start(remoteEndpoint);

                    connectedClients.Add(remoteEndpoint);

                }
            }

            private static void clientLoop(object data)
            {
                RemoteEndpoint client = null;
                if (data is RemoteEndpoint)
                {
                    client = (RemoteEndpoint)data;
                }
                else
                {
                    throw new Exception(String.Format("NetworkServer.clientLoop called with incorrect data object. Expected 'RemoteEndpoint', got '{0}'", data.GetType().ToString()));
                }

                bool clientActive = true;
                while (clientActive)
                {
                    // Check if the socket is active
                    {
                        // Let the protocol handler receive and handle messages
                        try
                        {
                            ProtocolMessage.readProtocolMessage(client.clientSocket, client);
                        }
                        catch (Exception)
                        {
                            //Console.WriteLine("Disconnected client: {0}", e.ToString());
                            client.state = RemoteEndpointState.Closed;
                        }
                    }
                    
                    // Check if the client disconnected
                    if (client.state == RemoteEndpointState.Closed)
                    {
                        clientActive = false;
                    }
                }

                // Remove corresponding address from presence list
                if(client.presence != null && client.presenceAddress != null)
                {
                    PresenceList.removeAddressEntry(client.presence.wallet, client.presenceAddress);
                }

                // Close the client socket
                if (client != null && client.clientSocket != null)
                {
                    try
                    {
                        client.clientSocket.Shutdown(SocketShutdown.Both);
                        client.clientSocket.Close();
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine("Could not shutdown client socket: {0}", e.ToString());
                    }
                }

                lock (connectedClients)
                {
                    connectedClients.Remove(client);
                }

                Thread.Yield();
            }


        }
    }
}
