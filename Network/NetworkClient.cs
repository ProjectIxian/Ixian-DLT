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
        public bool running;
        public string address = "127.0.0.1:10000";

        private string tcpHostname = "";
        private int tcpPort = 0;

        public NetworkClient()
        {
            tcpClient = new TcpClient();

            // Don't allow another socket to bind to this port.
            tcpClient.Client.ExclusiveAddressUse = true;

            // The socket will linger for 3 seconds after 
            // Socket.Close is called.
            tcpClient.Client.LingerState = new LingerOption(true, 3);

            // Disable the Nagle Algorithm for this tcp socket.
            tcpClient.Client.NoDelay = true;

            tcpClient.Client.ReceiveBufferSize = 1024 * 64;// 8192;
//            tcpClient.Client.ReceiveTimeout = 1000;

            tcpClient.Client.SendBufferSize = 1024 * 64;// 8192;
 //           tcpClient.Client.SendTimeout = 1000;

   //         tcpClient.Client.Ttl = 42;

        }

        public bool connectToServer(string hostname, int port)
        {
            tcpHostname = hostname;
            tcpPort = port;
            address = string.Format("{0}:{1}", hostname, port);

            if (tcpClient.Client.Connected)
            {
                tcpClient.Client.Shutdown(SocketShutdown.Both);
                tcpClient.Client.Disconnect(true);
            }

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
                            Logging.warn(string.Format("Socket exception for {0}:{1} has failed. Reason {2}", hostname, port, se.ToString()));
                        }
                        break;
                }

                // Todo: make this more elegant
                try
                {
                    tcpClient.Client.Close();
                }
                catch(Exception e)
                {
                    Logging.warn(string.Format("Socket exception when closing. Reason {0}", e.ToString()));
                }

                running = false;
                return false;
            }
            catch (Exception e)
            {
                Logging.warn(string.Format("Network client connection to {0}:{1} has failed. Reason {2}", hostname, port, e.ToString()));
                running = false;
                return false;
            }

            Logging.info(string.Format("Network client connected to {0}:{1}", hostname, port));



            running = true;
            Thread thread = new Thread(new ThreadStart(onUpdate));
            thread.Start();

            return true;
        }

        // Reconnect with the previous settings
        public bool reconnect()
        {
            if(tcpHostname.Length < 1 )
            {
                Logging.warn("Network client reconnect failed due to invalid hostname.");
                return false;
            }

            return connectToServer(tcpHostname, tcpPort);
        }


        // Sends data over the network
        public void sendData(ProtocolMessageCode code, byte[] data)
        {
            if (!isConnected())
            {
                Logging.info(string.Format("Attempted to senddata to disconnected client {0}", address));
                reconnect();
                return;
            }

            byte[] ba = ProtocolMessage.prepareProtocolMessage(code, data);
            try
            {
                tcpClient.Client.Send(ba, SocketFlags.None);
                if (tcpClient.Client.Connected == false)
                {
                    Console.WriteLine("Failed senddata to client: {0}. Reconnecting.", address);
                    reconnect();

                }
            }
            catch(Exception)
            {
                Console.WriteLine("CLN: Socket exception, attempting to reconnect");
                reconnect();
            }
        }


        private void onUpdate()
        {
            // Send a hello message containing the public ip and port of this node
            List<string> ips = CoreNetworkUtils.GetAllLocalIPAddresses();

            foreach (string ip in ips)
            {
                using (MemoryStream m = new MemoryStream())
                {
                    using (BinaryWriter writer = new BinaryWriter(m))
                    {
                        string publicHostname = string.Format("{0}:{1}", NetworkServer.publicIPAddress, Config.serverPort);
                        // Send the public IP address and port
                        writer.Write(publicHostname);

                        // Send the public node address
                        string address = Node.walletStorage.address;
                        writer.Write(address);

                        // Send the node type
                        char node_type = 'M'; // This is a Master node
                        writer.Write(node_type);

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
                catch(Exception e)
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


    }

}
