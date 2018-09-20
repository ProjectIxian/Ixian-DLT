using DLT;
using DLT.Meta;
using DLT.Network;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace DLTNode
{
    class StressTest
    {
        public static TcpClient tcpClient = null;

        static string hostname = "192.168.1.102";
        static int port = 10515;

        public static void start()
        {
            // Run protocol spam
            //  startProtocolTest();

            // Run the spam connect test
            //  startSpamConnectTest();

            // Run transaction spam test
            startTxSpamTest();
        }

        private static bool connect()
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
                        Logging.warn(string.Format("Address already in use."));
                        break;

                    default:
                        {
                            Logging.warn(string.Format("Socket connection has failed."));
                        }
                        break;
                }

                try
                {
                    tcpClient.Client.Close();
                }
                catch (Exception)
                {
                    Logging.warn(string.Format("Socket exception when closing."));
                }

                disconnect();
                return false;
            }
            return true;
        }

        public static void disconnect()
        {
            if (tcpClient == null)
                return;

            if (tcpClient.Client.Connected)
            {
                tcpClient.Client.Shutdown(SocketShutdown.Both);
                // tcpClient.Client.Disconnect(true);
                tcpClient.Close();
            }
        }


        public static void startSpamConnectTest()
        {
            Console.ForegroundColor = ConsoleColor.DarkYellow;
            Logging.info("Starting spam connect test");

            for (int i = 0; i < 100; i++)
            {
                if (connect())
                    Logging.info("Connected.");

            //    disconnect();
            }

            Console.ForegroundColor = ConsoleColor.DarkYellow;
            Logging.info("Ending spam connect test");
            Console.ResetColor();
        }
     
        public static void startProtocolTest()
        {
            Console.ForegroundColor = ConsoleColor.DarkYellow;
            Logging.info("Starting spam connect test");

            if(connect())
                    Logging.info("Connected.");



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

                           // Send the node device id
                           writer.Write(Config.device_id);

                           // Send the S2 public key
                           writer.Write(Node.walletStorage.encPublicKey);

                           // Send the wallet public key
                           writer.Write(Node.walletStorage.publicKey);

                           sendData(ProtocolMessageCode.hello, m.ToArray());
                       }
                   }

            Transaction tx = new Transaction();
            tx.type = (int)Transaction.Type.PoWSolution;
            tx.from = Node.walletStorage.getWalletAddress();
            tx.to = "IxianInfiniMine2342342342342342342342342342342342342342342342342db32";
            tx.amount = "0";

            string data = string.Format("{0}||{1}||{2}", Node.walletStorage.publicKey, 0, 1);
            tx.data = data;

            tx.timeStamp = Clock.getTimestamp(DateTime.Now);
            tx.id = tx.generateID();
            tx.checksum = Transaction.calculateChecksum(tx);
            tx.signature = Transaction.getSignature(tx.checksum);

            sendData(ProtocolMessageCode.newTransaction, tx.getBytes());


            disconnect();
            

            Console.ForegroundColor = ConsoleColor.DarkYellow;
            Logging.info("Ending spam connect test");
            Console.ResetColor();
        }

        public static void startTxSpamTest()
        {
            Console.ForegroundColor = ConsoleColor.DarkYellow;
            Logging.info("Starting tx spam test");

            ulong nonce = 1;
            for(int i = 0; i < 1000; i++)
            {
                IxiNumber amount = new IxiNumber("0.01");
                IxiNumber fee = Config.transactionPrice;
                string to = "08a4a1d8bae813dc2cfb0185175f02bd8da5d9cec470e99ec3b010794605c854a481";
                string from = Node.walletStorage.getWalletAddress();
                Transaction transaction = new Transaction(amount, fee, to, from, nonce);
                ProtocolMessage.broadcastProtocolMessage(ProtocolMessageCode.newTransaction, transaction.getBytes());

                nonce++;
            }

            Console.ForegroundColor = ConsoleColor.DarkYellow;
            Logging.info("Ending tx spam test");
            Console.ResetColor();
        }

        // Sends data over the network
        public static void sendData(ProtocolMessageCode code, byte[] data)
        {
            byte[] ba = ProtocolMessage.prepareProtocolMessage(code, data);
            try
            {
                tcpClient.Client.Send(ba, SocketFlags.None);
                if (tcpClient.Client.Connected == false)
                {
                    Console.WriteLine("Failed senddata to client. Reconnecting.");

                }
            }
            catch (Exception)
            {
                Console.WriteLine("CLN: Socket exception, attempting to reconnect");
            }
            Console.WriteLine("sendData done");
        }
    }
}
