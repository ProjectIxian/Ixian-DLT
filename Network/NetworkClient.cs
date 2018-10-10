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
    public class NetworkClient : RemoteEndpoint
    {
        public TcpClient tcpClient = null;

        private string tcpHostname = "";
        private int tcpPort = 0;
        private int failedReconnects = 0;

        private object reconnectLock = new object();

        public NetworkClient()
        {
            prepareClient();
        }

        // Prepare the client socket
        public void prepareClient()
        {
            tcpClient = new TcpClient();

            Socket tmpSocket = tcpClient.Client;

            // Don't allow another socket to bind to this port.
            tmpSocket.ExclusiveAddressUse = true;

            // The socket will linger for 3 seconds after 
            // Socket.Close is called.
            tmpSocket.LingerState = new LingerOption(true, 3);

            // Disable the Nagle Algorithm for this tcp socket.
            tmpSocket.NoDelay = true;

            //tcpClient.Client.ReceiveTimeout = 5000;
            //tcpClient.Client.ReceiveBufferSize = 1024 * 64;
            //tcpClient.Client.SendBufferSize = 1024 * 64;
            //tcpClient.Client.SendTimeout = 5000;

            tmpSocket.Blocking = true;

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
                    tcpClient.Close();
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

            start(tcpClient.Client);
            return true;
        }

        // Reconnect with the previous settings
        public bool reconnect()
        {
            lock (reconnectLock)
            {
                if (tcpHostname.Length < 1)
                {
                    Logging.warn("Network client reconnect failed due to invalid hostname.");
                    failedReconnects++;
                    return false;
                }

                // Safely close the threads
                running = false;

                // Check if socket already disconnected
                if (clientSocket == null)
                {
                    // TODO: handle this scenario
                }
                else if (clientSocket.Connected)
                {
                    clientSocket.Shutdown(SocketShutdown.Both);
                    tcpClient.Close();
                }

                Logging.info(string.Format("--> Reconnecting to {0}", address));
                return connectToServer(tcpHostname, tcpPort);
            }
        }

        private void sendHello()
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
        }

        // Receive thread
        protected override void recvLoop()
        {
            sendHello();

            base.recvLoop();
        }

        // Send a ping message to this server
        public void sendPing()
        {
            byte[] tmp = new byte[1];
            sendData(ProtocolMessageCode.ping, tmp);
        }

        // Returns the number of failed reconnects
        public int getFailedReconnectsCount()
        {
            return failedReconnects;
        }
    }

}
