using DLT;
using DLT.Meta;
using DLT.Network;
using DLTNode.Network;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DLTNode
{
    class StressTest
    {
        public static TcpClient tcpClient = null;

        static string txfilename = "txspam.file";
        static string hostname = "192.168.1.101";
        static int port = 10515;

        static int txspamNum = 1000; // Set the initial number of spam transactions

        public static void start(string type, int txnum = 0)
        {
            if (txnum != 0)
                txspamNum = txnum;

            new Thread(() =>
            {
                // Run protocol spam
                if (type.Equals("protocol", StringComparison.Ordinal))
                    startProtocolTest();

                // Run the spam connect test
                if (type.Equals("connect", StringComparison.Ordinal))
                    startSpamConnectTest();

                // Run transaction spam test
                if (type.Equals("txspam", StringComparison.Ordinal))
                    startTxSpamTest();

                // Run the transaction spam file generation test
                if (type.Equals("txfilegen", StringComparison.Ordinal))
                    startTxFileGenTest();

                // Run the transaction spam file test
                if (type.Equals("txfilespam", StringComparison.Ordinal))
                    startTxFileSpamTest();

            }).Start();
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

            tx.timeStamp = Node.getCurrentTimestamp().ToString();
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

            ulong nonce = Node.walletState.getWallet(Node.walletStorage.getWalletAddress()).nonce;

            for (int i = 0; i < txspamNum; i++)
            {
                IxiNumber amount = new IxiNumber("0.01");
                IxiNumber fee = Config.transactionPrice;
                string to = "08a4a1d8bae813dc2cfb0185175f02bd8da5d9cec470e99ec3b010794605c854a481";
                string from = Node.walletStorage.getWalletAddress();

                string data = Node.walletStorage.publicKey;
                // Check if this wallet's public key is already in the WalletState
                Wallet mywallet = Node.walletState.getWallet(from, true);
                if (mywallet.publicKey.Equals(data, StringComparison.Ordinal))
                {
                    // Walletstate public key matches, we don't need to send the public key in the transaction
                    data = "";
                }

                Transaction transaction = new Transaction(amount, fee, to, from, data, nonce);
                // Console.WriteLine("> sending {0}", transaction.id);
                TransactionPool.addTransaction(transaction);
                nonce++;
            }

            Console.ForegroundColor = ConsoleColor.DarkYellow;
            Logging.info("Ending tx spam test");
            Console.ResetColor();
        }

        public static void startTxFileGenTest()
        {
            Console.ForegroundColor = ConsoleColor.DarkYellow;
            Logging.info("Starting tx file gen test");
            Console.ResetColor();

            BinaryWriter writer;
            try
            {
                writer = new BinaryWriter(new FileStream(txfilename, FileMode.Create));
            }
            catch (IOException e)
            {
                Logging.error(String.Format("Cannot create txspam file. {0}", e.Message));
                return;
            }

            ulong nonce = 0; // Set the starting nonce

            writer.Write(txspamNum);
            for (int i = 0; i < txspamNum; i++)
            {
                IxiNumber amount = new IxiNumber("0.01");
                IxiNumber fee = Config.transactionPrice;
                string to = "08a4a1d8bae813dc2cfb0185175f02bd8da5d9cec470e99ec3b010794605c854a481";
                string from = Node.walletStorage.getWalletAddress();

                string data = Node.walletStorage.publicKey;
                // Check if this wallet's public key is already in the WalletState
                Wallet mywallet = Node.walletState.getWallet(from, true);
                if (mywallet.publicKey.Equals(data, StringComparison.Ordinal))
                {
                    // Walletstate public key matches, we don't need to send the public key in the transaction
                    data = "";
                }

                Transaction transaction = new Transaction(amount, fee, to, from, data, nonce);
                byte[] bytes = transaction.getBytes();
                
                Console.WriteLine("> writing tx {0}", transaction.id);
                writer.Write(bytes.Length);
                writer.Write(bytes);

                nonce++;
            }

            writer.Close();

            Console.ForegroundColor = ConsoleColor.DarkYellow;
            Logging.info("Ending tx file gen test");
            Console.ResetColor();
        }

        public static void startTxFileSpamTest()
        {
            Console.ForegroundColor = ConsoleColor.DarkYellow;
            Logging.info("Starting tx file spam test");
            Console.ResetColor();

            if (File.Exists(txfilename) == false)
            {
                Logging.error("Cannot start tx file spam test. Missing tx spam file!");
                return;
            }

            ulong nonce = Node.walletState.getWallet(Node.walletStorage.getWalletAddress()).nonce;
            if(nonce != 0)
            {
                Logging.error("Cannot start tx file spam test. Initial nonce is not 0!");
                return;
            }

            BinaryReader reader;
            try
            {
                reader = new BinaryReader(new FileStream(txfilename, FileMode.Open));
            }
            catch (IOException e)
            {
                Logging.log(LogSeverity.error, String.Format("Cannot open txspam file. {0}", e.Message));
                return;
            }

            try
            {
                int spam_num = reader.ReadInt32();
                Logging.info(string.Format("Reading {0} spam transactions from file.", spam_num));
                for (int i = 0; i < spam_num; i++)
                {
                    int length = reader.ReadInt32();
                    byte[] bytes = reader.ReadBytes(length);
                    Transaction transaction = new Transaction(bytes);
                    Console.WriteLine("> adding tx {0}", transaction.id);
                    TransactionPool.addTransaction(transaction);
                }
            }
            catch (IOException e)
            {
                Logging.log(LogSeverity.error, String.Format("Cannot read from txspam file. {0}", e.Message));
                return;
            }
            reader.Close();

            Console.ForegroundColor = ConsoleColor.DarkYellow;
            Logging.info("Ending tx file spam test");
            Console.ResetColor();
        }



        // Sends data over the network
        public static void sendData(ProtocolMessageCode code, byte[] data)
        {
            byte[] ba = ProtocolMessage.prepareProtocolMessage(code, data);
            NetDump.Instance.appendSent(tcpClient.Client, ba, ba.Length);
            try
            {
                tcpClient.Client.Send(ba, SocketFlags.None);
                if (tcpClient.Client.Connected == false)
                {
                    Logging.error("Failed senddata to client. Reconnecting.");

                }
            }
            catch (Exception e)
            {
                Logging.error(String.Format("CLN: Socket exception, attempting to reconnect {0}", e));
            }
            //Console.WriteLine("sendData done");
        }
    }
}
