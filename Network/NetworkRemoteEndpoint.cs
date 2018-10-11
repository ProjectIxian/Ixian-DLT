using DLT.Meta;
using DLT.Network;
using DLTNode.Network;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DLT
{
    public class RemoteEndpoint
    {
        public string fullAddress = "127.0.0.1:10000";
        public string address = "";
        public int incomingPort = Config.serverPort;


        protected long lastDataReceivedTime = 0;
        protected long lastPing = 0;

        public IPEndPoint remoteIP;
        public Socket clientSocket;
        public RemoteEndpointState state;
        public bool inIO;
        // Maintain two threads for handling data receiving and sending
        protected Thread recvThread = null;
        protected Thread sendThread = null;
        protected Thread parseThread = null;

        public Presence presence = null;
        public PresenceAddress presenceAddress = null;

        protected bool running = false;

        // Maintain a queue of messages to send
        private List<QueueMessage> sendQueueMessages = new List<QueueMessage>();

        // Maintain a queue of raw received data
        private List<QueueMessageRaw> recvRawQueueMessages = new List<QueueMessageRaw>();


        public void start(Socket socket = null)
        {
            if(running)
            {
                return;
            }

            if (socket != null)
            {
                clientSocket = socket;
            }
            if (clientSocket == null)
            {
                Logging.error("Could not start NetworkRemoteEndpoint, socket is null");
                return;
            }
            remoteIP = (IPEndPoint)clientSocket.RemoteEndPoint;
            address = remoteIP.Address.ToString();
            fullAddress = address + ":" + remoteIP.Port;
            presence = null;
            presenceAddress = null;

            lastPing = 0;
            lastDataReceivedTime = Clock.getTimestamp(DateTime.Now);

            state = RemoteEndpointState.Established;

            running = true;
            clientSocket.Blocking = true;

            // Abort all related threads
            if (recvThread != null)
            {
                recvThread.Abort();
            }
            if (sendThread != null)
            {
                sendThread.Abort();
            }
            if (parseThread != null)
            {
                parseThread.Abort();
            }

            // Start receive thread
            recvThread = new Thread(new ThreadStart(recvLoop));
            recvThread.Start();

            // Start send thread
            sendThread = new Thread(new ThreadStart(sendLoop));
            sendThread.Start();

            // Start parse thread
            parseThread = new Thread(new ThreadStart(parseLoop));
            parseThread.Start();
        }

        // Aborts all related endpoint threads and data
        public void stop()
        {
            Thread.Sleep(50); // clear any last messages

            state = RemoteEndpointState.Closed;
            running = false;

            Thread.Sleep(50); // wait for threads to stop

            lock (sendQueueMessages)
            {
                sendQueueMessages.Clear();
            }

            lock (recvRawQueueMessages)
            {
                recvRawQueueMessages.Clear();
            }

            // Abort all related threads
            if (recvThread != null)
            {
                recvThread.Abort();
            }
            if (sendThread != null)
            {
                sendThread.Abort();
            }
            if (parseThread != null)
            {
                parseThread.Abort();
            }

            disconnect();
        }

        // Receive thread
        protected virtual void recvLoop()
        {
            while (running)
            {
                // Let the protocol handler receive and handle messages
                try
                {
                    byte[] data = ProtocolMessage.readSocketData(clientSocket);
                    if (data != null)
                    {
                        lastDataReceivedTime = Clock.getTimestamp(DateTime.Now);
                        lastPing = 0;
                        parseDataInternal(data, this);
                    }
                }
                catch (Exception e)
                {
                    Logging.warn(string.Format("recvRE: Disconnected client: {0}", e.ToString()));
                    state = RemoteEndpointState.Closed;
                }

                // Sleep a while to throttle the client
                // Check if there are too many messages
                // TODO TODO TODO should we include txqueue here as well?
                if (NetworkQueue.getQueuedMessageCount() > Config.maxNetworkQueue)
                {
                    Thread.Sleep(50);
                }else
                {
                    Thread.Sleep(1);
                }

                // Check if the client disconnected
                if (state == RemoteEndpointState.Closed)
                {
                    running = false;
                }
            }

            disconnect();

            // Remove this endpoint from the network server
            NetworkServer.removeEndpoint(this);
        }

        public virtual void disconnect()
        {
            // Close the client socket
            if (clientSocket != null)
            {
                try
                {
                    clientSocket.Shutdown(SocketShutdown.Both);
                    clientSocket.Close();
                    clientSocket = null;
                }
                catch (Exception e)
                {
                    Logging.warn(string.Format("recvRE: Could not shutdown client socket: {0}", e.ToString()));
                }
            }
        }


        // Send thread
        protected void sendLoop()
        {
            // Prepare an special message object to use while sending, without locking up the queue messages
            QueueMessage active_message = new QueueMessage();

            while (running)
            {
                long curTime = Clock.getTimestamp(DateTime.Now);
                if (curTime - lastDataReceivedTime > Config.pingInterval)
                {
                    if (lastPing == 0)
                    {
                        lastPing = curTime;
                        byte[] pingBytes = new byte[1];
                        sendDataInternal(ProtocolMessageCode.ping, pingBytes, Crypto.sha256(pingBytes));
                        Thread.Sleep(1);
                        continue;
                    }else if(curTime - lastPing > Config.pingTimeout)
                    {
                        // haven't received any data for 30 seconds, stop running
                        Logging.error("Node hasn't received any data from remote endpoint for over 30 seconds, disconnecting.");
                        running = false;
                        break;
                    }
                }

                bool message_found = false;
                lock (sendQueueMessages)
                {
                    if (sendQueueMessages.Count > 0)
                    {
                        // Pick the oldest message
                        QueueMessage candidate = sendQueueMessages[0];
                        active_message.code = candidate.code;
                        active_message.data = candidate.data;
                        active_message.checksum = candidate.checksum;
                        active_message.skipEndpoint = candidate.skipEndpoint;
                        // Remove it from the queue
                        sendQueueMessages.Remove(candidate);
                        message_found = true;
                    }
                }

                if (message_found)
                {
                    // Active message set, attempt to send it
                    sendDataInternal(active_message.code, active_message.data, active_message.checksum);
                    Thread.Sleep(1);
                }
                else
                {
                    // Sleep for 10ms to prevent cpu waste
                    Thread.Sleep(10);
                }
            }
        }

        // Parse thread
        protected void parseLoop()
        {
            // Prepare an special message object to use while sending, without locking up the queue messages
            QueueMessageRaw active_message = new QueueMessageRaw();

            while (running)
            {
                try
                {
                    bool message_found = false;
                    lock (recvRawQueueMessages)
                    {
                        if (recvRawQueueMessages.Count > 0)
                        {
                            // Pick the oldest message
                            QueueMessageRaw candidate = recvRawQueueMessages[0];
                            active_message.data = candidate.data;
                            active_message.endpoint = candidate.endpoint;
                            // Remove it from the queue
                            recvRawQueueMessages.Remove(candidate);
                            message_found = true;
                        }
                    }

                    if (message_found)
                    {
                        // Active message set, add it to Network Queue
                        ProtocolMessage.readProtocolMessage(active_message.data, this);
                    }
                    else
                    {
                        Thread.Sleep(10);
                    }

                }
                catch (Exception e)
                {
                    Logging.error("Exception occured in parseLoopRE: " + e);
                }
                // Sleep a bit to prevent cpu waste
                Thread.Yield();
            }

        }

        protected void parseDataInternal(byte[] data, RemoteEndpoint endpoint)
        {
            QueueMessageRaw message = new QueueMessageRaw();
            message.data = data;
            message.endpoint = endpoint;

            lock (recvRawQueueMessages)
            {
                recvRawQueueMessages.Add(message);
            }
        }


        // Internal function that sends data through the socket
        protected void sendDataInternal(ProtocolMessageCode code, byte[] data, byte[] checksum)
        {
            byte[] ba = ProtocolMessage.prepareProtocolMessage(code, data, checksum);
            NetDump.Instance.appendSent(ba);
            try
            {
                for (int sentBytes = 0; sentBytes < ba.Length;)
                {
                    int bytesToSendCount = ba.Length - sentBytes;
                    if (bytesToSendCount > 8000)
                    {
                        bytesToSendCount = 8000;
                    }
                    int curSentBytes = clientSocket.Send(ba, sentBytes, ba.Length - sentBytes, SocketFlags.None);
                    if (curSentBytes < bytesToSendCount)
                    {
                        Thread.Sleep(10);
                    }
                    else
                    {
                        // Sleep a bit to allow other threads to do their thing
                        Thread.Yield();
                    }
                    sentBytes += curSentBytes;
                    // TODO TODO TODO timeout
                }
                if (clientSocket.Connected == false)
                {
                    Logging.warn("sendRE: Failed senddata to remote endpoint. Closing.");
                    state = RemoteEndpointState.Closed;
                }
            }
            catch (Exception e)
            {
                Logging.warn(String.Format("sendRE: Socket exception, closing {0}", e));
                state = RemoteEndpointState.Closed;

            }
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
            message.checksum = Crypto.sha256(data);
            message.skipEndpoint = null;

            lock (sendQueueMessages)
            {
                if (sendQueueMessages.Exists(x => x.code == message.code && message.checksum.SequenceEqual(x.checksum)))
                {
                    Logging.warn(string.Format("Attempting to add a duplicate message (code: {0}) to the network queue", code));
                }
                else
                {
                    if (sendQueueMessages.Count > 6)
                    {
                        // Prioritize certain messages if the queue is large
                        if (message.code != ProtocolMessageCode.newTransaction)
                        {
                            sendQueueMessages.Insert(3, message);
                        }
                        else
                        {
                            // Check if there are too many messages
                            if (sendQueueMessages.Count > Config.maxSendQueue)
                            {
                                sendQueueMessages.RemoveAt(1);
                            }

                            sendQueueMessages.Add(message);
                        }
                    }
                    else
                    {
                        sendQueueMessages.Add(message);
                    }
                }
            }

        }

        public int getQueuedMessageCount()
        {
            lock (sendQueueMessages)
            {
                return sendQueueMessages.Count;
            }
        }

        public bool isConnected()
        {
            try
            {
                if (clientSocket == null)
                {
                    return false;
                }

                return clientSocket.Connected && running;
            }
            catch (Exception)
            {
                return false;
            }
        }

        // Get the ip/hostname and port
        public string getFullAddress(bool useIncomingPorts = false)
        {
            if(useIncomingPorts)
            {
                return address + ":" + incomingPort;
            }
            return fullAddress;
        }


        // Handle the getBlockTransactions message
        // This is called from NetworkProtocol
        public void handleGetBlockTransactions(byte[] data)
        {
            using (MemoryStream m = new MemoryStream(data))
            {
                using (BinaryReader reader = new BinaryReader(m))
                {
                    ulong blockNum = reader.ReadUInt64();
                    bool requestAllTransactions = reader.ReadBoolean();
                    Logging.info(String.Format("Received request for transactions in block {0}.", blockNum));

                    // Get the requested block and corresponding transactions
                    Block b = Node.blockChain.getBlock(blockNum, Config.storeFullHistory);
                    List<string> txIdArr = null;
                    if (b != null)
                    {
                        txIdArr = new List<string>(b.transactions);
                    }
                    else
                    {
                        // Block is likely local, fetch the transactions
                        lock (Node.blockProcessor.localBlockLock)
                        {
                            Block tmp = Node.blockProcessor.getLocalBlock();
                            if (tmp != null && tmp.blockNum == blockNum)
                            {
                                txIdArr = new List<string>(tmp.transactions);
                            }
                        }
                    }

                    if (txIdArr == null)
                        return;

                    int tx_count = txIdArr.Count();

                    if (tx_count == 0)
                        return;

                    int num_chunks = tx_count / Config.maximumTransactionsPerChunk + 1;

                    // Go through each chunk
                    for (int i = 0; i < num_chunks; i++)
                    {
                        using (MemoryStream mOut = new MemoryStream())
                        {
                            using (BinaryWriter writer = new BinaryWriter(mOut))
                            {
                                // Generate a chunk of transactions
                                for (int j = 0; j < Config.maximumTransactionsPerChunk; j++)
                                {
                                    int tx_index = i * Config.maximumTransactionsPerChunk + j;
                                    if (tx_index > tx_count - 1)
                                        break;

                                    if (!requestAllTransactions)
                                    {
                                        if (txIdArr[tx_index].StartsWith("stk"))
                                        {
                                            continue;
                                        }
                                    }
                                    Transaction tx = TransactionPool.getTransaction(txIdArr[tx_index], Config.storeFullHistory);
                                    if (tx != null)
                                    {
                                        byte[] txBytes = tx.getBytes();

                                        writer.Write(txBytes.Length);
                                        writer.Write(txBytes);
                                    }
                                }

                                // Send a chunk
                                sendData(ProtocolMessageCode.transactionsChunk, mOut.ToArray());
                            }
                        }
                    }
                }
            }
        }

        // Handle the getUnappliedTransactions message
        // This is called from NetworkProtocol
        public void handleGetUnappliedTransactions(byte[] data)
        {
            Transaction[] txIdArr = TransactionPool.getUnappliedTransactions();
            int tx_count = txIdArr.Count();

            if (tx_count == 0)
                return;

            int num_chunks = tx_count / Config.maximumTransactionsPerChunk + 1;

            // Go through each chunk
            for (int i = 0; i < num_chunks; i++)
            {
                using (MemoryStream mOut = new MemoryStream())
                {
                    using (BinaryWriter writer = new BinaryWriter(mOut))
                    {
                        // Generate a chunk of transactions
                        for (int j = 0; j < Config.maximumTransactionsPerChunk; j++)
                        {
                            int tx_index = i * Config.maximumTransactionsPerChunk + j;
                            if (tx_index > tx_count - 1)
                                break;

                            byte[] txBytes = txIdArr[tx_index].getBytes();
                            writer.Write(txBytes.Length);
                            writer.Write(txBytes);                         
                        }

                        // Send a chunk
                        sendData(ProtocolMessageCode.transactionsChunk, mOut.ToArray());
                    }
                }
            }            
        }


    }
}