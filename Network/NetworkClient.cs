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
    class NetworkClient
    {
        public TcpClient tcpClient = null;
        public bool running = false;
        public string address = "127.0.0.1:10000";

        private string tcpHostname = "";
        private int tcpPort = 0;
        private int failedReconnects = 0;

        // Maintain two threads for handling data receiving and sending
        private Thread recvThread = null;
        private Thread sendThread = null;

        // Maintain a queue of messages to send
        private static List<QueueMessage> sendQueueMessages = new List<QueueMessage>();


        public NetworkClient()
        {
            prepareClient();
        }

        // Prepare the client socket
        public void prepareClient()
        {
            tcpClient = new TcpClient();

            // Don't allow another socket to bind to this port.
            tcpClient.Client.ExclusiveAddressUse = true;

            // The socket will linger for 3 seconds after 
            // Socket.Close is called.
            tcpClient.Client.LingerState = new LingerOption(true, 3);

            // Disable the Nagle Algorithm for this tcp socket.
            tcpClient.Client.NoDelay = true;

            tcpClient.Client.ReceiveTimeout = 5000;
            //tcpClient.Client.ReceiveBufferSize = 1024 * 64;
            //tcpClient.Client.SendBufferSize = 1024 * 64;
            tcpClient.Client.SendTimeout = 5000;

            // Reset the failed reconnects count
            failedReconnects = 0;

        }

        public bool connectToServer(string hostname, int port)
        {
            tcpHostname = hostname;
            tcpPort = port;
            address = string.Format("{0}:{1}", hostname, port);

            // Prepare the TCP client
            prepareClient();

            try
            {
                tcpClient.Connect(hostname, port);
            }
            catch (SocketException se)
            {
                SocketError errorCode = (SocketError)se.ErrorCode;

                switch (errorCode)
                {
                    case SocketError.IsConnected:
                        break;

                    case SocketError.AddressAlreadyInUse:
                        Logging.warn(string.Format("Socket exception for {0}:{1} has failed. Address already in use.", hostname, port));
                        break;

                    default:
                        {
                            Logging.warn(string.Format("Socket connection for {0}:{1} has failed.", hostname, port));
                        }
                        break;
                }

                // Todo: make this more elegant
                try
                {
                    tcpClient.Client.Close();
                }
                catch (Exception)
                {
                    Logging.warn(string.Format("Socket exception for {0}:{1} when closing.", hostname, port));
                }

                running = false;
                failedReconnects++;
                return false;
            }
            catch (Exception)
            {
                Logging.warn(string.Format("Network client connection to {0}:{1} has failed.", hostname, port));
                running = false;
                failedReconnects++;
                return false;
            }

            Logging.info(string.Format("Network client connected to {0}:{1}", hostname, port));

            // Reset the failed reconnects count
            failedReconnects = 0;

            running = true;

            // Start receive thread
            recvThread = new Thread(new ThreadStart(recvLoop));
            recvThread.Start();

            // Start send thread
            sendThread = new Thread(new ThreadStart(sendLoop));
            sendThread.Start();

            return true;
        }

        // Reconnect with the previous settings
        public bool reconnect()
        {
            if (tcpHostname.Length < 1)
            {
                Logging.warn("Network client reconnect failed due to invalid hostname.");
                failedReconnects++;
                return false;
            }

            // Check if socket already disconnected
            if (tcpClient == null)
            {
                // TODO: handle this scenario
            }
            else if (tcpClient.Client == null)
            {
                // TODO: handle this scenario
            }
            else if (tcpClient.Client.Connected)
            {
                tcpClient.Client.Shutdown(SocketShutdown.Both);
                tcpClient.Close();
            }

            Logging.info(string.Format("--> Reconnecting to {0}", address));
            return connectToServer(tcpHostname, tcpPort);
        }


        // Sends data over the network
        public void sendData(ProtocolMessageCode code, byte[] data)
        {
            if (data == null)
            {
                Logging.warn(string.Format("Invalid protocol message data for {0}", code));
                return;
            }

            QueueMessage message = new QueueMessage();
            message.code = code;
            message.data = data;
            message.skipSocket = null;

            lock (sendQueueMessages)
            {
                if (sendQueueMessages.Exists(x => x.code == message.code && message.data.SequenceEqual(x.data)))
                {
                    Logging.warn(string.Format("Attempting to add a duplicate message (code: {0}) to the network queue", code));
                }
                else
                {
                    sendQueueMessages.Add(message);
                }
            }

        }

        // Internal function that sends data through the socket
        private void sendDataInternal(ProtocolMessageCode code, byte[] data)
        {
            byte[] ba = ProtocolMessage.prepareProtocolMessage(code, data);
            try
            {
                for (int sentBytes = 0; sentBytes < ba.Length;)
                {
                    sentBytes += tcpClient.Client.Send(ba, sentBytes, ba.Length - sentBytes, SocketFlags.None);
                    if (sentBytes < ba.Length)
                    {
                        Thread.Sleep(5);
                    }
                    // TODO TODO TODO timeout
                }
                if (tcpClient.Client.Connected == false)
                {
                    Console.WriteLine("Failed senddata to client: {0}. Reconnecting.", address);
                    reconnect();

                }
            }
            catch (Exception)
            {
                Console.WriteLine("CLN: Socket exception, attempting to reconnect");
                reconnect();
            }
        }

        // Receive thread
        private void recvLoop()
        {
            // Send a hello message containing the public ip and port of this node
            List<string> ips = CoreNetworkUtils.GetAllLocalIPAddresses();

            foreach (string ip in ips)
            {
                using (MemoryStream m = new MemoryStream())
                {
                    using (BinaryWriter writer = new BinaryWriter(m))
                    {
                        string publicHostname = string.Format("{0}:{1}", Config.publicServerIP, Config.serverPort);
                        // Send the public IP address and port
                        writer.Write(publicHostname);

                        // Send the public node address
                        string address = Node.walletStorage.address;
                        writer.Write(address);

                        // Send the testnet designator
                        writer.Write(Config.isTestNet);

                        // Send the node type
                        char node_type = 'M'; // This is a Master node
                        writer.Write(node_type);

                        // Send the version
                        writer.Write(Config.version);

                        // Send the node device id
                        writer.Write(Config.device_id);

                        // Send the S2 public key
                        writer.Write(Node.walletStorage.encPublicKey);

                        // Send the wallet public key
                        writer.Write(Node.walletStorage.publicKey);

                        sendData(ProtocolMessageCode.hello, m.ToArray());
                    }
                }
                // TODO: multi-ip presence issue
                // For now limit to first ip
                break;
            }

            while (running)
            {
                try
                {
                    // Let the protocol handler receive and handle messages
                    ProtocolMessage.readProtocolMessage(tcpClient.Client, null);
                }
                catch(Exception)
                {
                    disconnect();
                    Thread.Yield();
                    return;
                }

                // Sleep a while to prevent cpu cycle waste
                Thread.Sleep(10);
            }

            disconnect();
            Thread.Yield();
        }

        // Send thread
        private void sendLoop()
        {
            // Prepare an special message object to use while sending, without locking up the queue messages
            QueueMessage active_message = new QueueMessage();

            while (running)
            {
                bool message_found = false;
                lock (sendQueueMessages)
                {
                    if (sendQueueMessages.Count > 0)
                    {
                        // Pick the oldest message
                        QueueMessage candidate = sendQueueMessages[0];
                        active_message.code = candidate.code;
                        active_message.data = candidate.data;
                        active_message.skipSocket = candidate.skipSocket;
                        // Remove it from the queue
                        sendQueueMessages.Remove(candidate);
                        message_found = true;
                    }
                }

                if (message_found)
                {
                    // Active message set, attempt to send it
                    sendDataInternal(active_message.code, active_message.data);
                }
                else
                {
                    // No active message
                    // Sleep for 100ms to prevent cpu waste
                    Thread.Sleep(100);
                }
            }

            Thread.Yield();
        }

        public void disconnect()
        {
            // Check if socket already disconnected
            if(tcpClient == null)
            {
                return;
            }
            if(tcpClient.Client == null)
            {
                return;
            }

            // Stop reading protocol messages
            running = false;

            if (tcpClient.Client.Connected)
            {
                tcpClient.Client.Shutdown(SocketShutdown.Both);
                // tcpClient.Client.Disconnect(true);
                tcpClient.Close();
            }
        }
        
        // Get the ip/hostname and port
        public string getFullAddress()
        {
            return string.Format("{0}:{1}", tcpHostname, tcpPort);
        }

        // Broadcasts a keepalive network message for this node PL address
        public bool sendKeepAlive(byte[] data)
        {
            sendData(ProtocolMessageCode.keepAlivePresence, data);
            return true;
        }

        // Send a ping message to this server
        public void sendPing()
        {
            byte[] tmp = new byte[1];
            sendData(ProtocolMessageCode.ping, tmp);
        }

        public bool isConnected()
        {
            try
            {
                if (tcpClient == null)
                {
                    return false;
                }

                if (tcpClient.Client == null)
                {
                    return false;
                }

                return tcpClient.Connected && running;
            }
            catch (Exception)
            {
                return false;
            }
        }

        // Returns the number of failed reconnects
        public int getFailedReconnectsCount()
        {
            return failedReconnects;
        }

    }

}
