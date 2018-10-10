using DLT.Meta;
using DLTNode;
using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Linq;

namespace DLT
{
    namespace Network
    {
        public class ProtocolMessage
        {

            [ThreadStatic] static byte[] currentBuffer = null;
            public static readonly ulong[] recvByteHist = new ulong[100];


            public static byte getHeaderChecksum(byte[] header)
            {
                byte sum = 0x7F;
                for(int i = 0; i < header.Length; i++)
                {
                    sum ^= header[i];
                }
                return sum;
            }

            // Prepare a network protocol message. Works for both client-side and server-side
            public static byte[] prepareProtocolMessage(ProtocolMessageCode code, byte[] data, byte[] checksum = null)
            {
                byte[] result = null;

                // Prepare the protocol sections
                int data_length = data.Length;

                if (data_length > 10000000)
                {
                    Logging.error(String.Format("Tried to send data bigger than 10MB - {0} with code {1}.", data_length, code));
                    return null;
                }

                byte[] data_checksum = checksum;

                if(checksum == null)
                {
                    data_checksum = Crypto.sha256(data);
                }

                using (MemoryStream m = new MemoryStream())
                {
                    using (BinaryWriter writer = new BinaryWriter(m))
                    {
                        // Protocol sections are code, length, checksum, data
                        // Write each section in binary, in that specific order
                        writer.Write((byte)'X');
                        writer.Write((int)code);
                        writer.Write(data_length);
                        writer.Write(data_checksum);

                        writer.Flush();
                        m.Flush();

                        byte header_checksum = getHeaderChecksum(m.ToArray());
                        writer.Write(header_checksum);

                        writer.Write((byte)'I');
                        writer.Write(data);
                    }
                    result = m.ToArray();
                }

                return result;
            }

            public static bool broadcastGetBlock(ulong block_num, RemoteEndpoint skipEndpoint = null)
            {
                using (MemoryStream mw = new MemoryStream())
                {
                    using (BinaryWriter writerw = new BinaryWriter(mw))
                    {
                        writerw.Write(block_num);

                        return broadcastProtocolMessage(ProtocolMessageCode.getBlock, mw.ToArray(), skipEndpoint);
                    }
                }
            }

            public static bool broadcastNewBlock(Block b, RemoteEndpoint skipEndpoint = null)
            {
                //Logging.info(String.Format("Broadcasting block #{0} : {1}.", b.blockNum, b.blockChecksum));
                return broadcastProtocolMessage(ProtocolMessageCode.newBlock, b.getBytes(), skipEndpoint);
            }

            public static bool broadcastGetTransaction(string txid)
            {
                using (MemoryStream mw = new MemoryStream())
                {
                    using (BinaryWriter writerw = new BinaryWriter(mw))
                    {
                        writerw.Write(txid);

                        return broadcastProtocolMessage(ProtocolMessageCode.getTransaction, mw.ToArray());
                    }
                }
            }

            public static void broadcastGetBlockTransactions(ulong blockNum, bool requestAllTransactions)
            {
                using (MemoryStream mw = new MemoryStream())
                {
                    using (BinaryWriter writerw = new BinaryWriter(mw))
                    {
                        writerw.Write(blockNum);
                        writerw.Write(requestAllTransactions);

                        broadcastProtocolMessage(ProtocolMessageCode.getBlockTransactions, mw.ToArray());
                    }
                }
            }

            public static void broadcastSyncWalletState()
            {
                broadcastProtocolMessage(ProtocolMessageCode.syncWalletState, new byte[1]);
            }

            // Broadcast a protocol message across clients and nodes
            // Returns true if it sent the message at least one endpoint. Returns false if the message couldn't be sent to any endpoints
            public static bool broadcastProtocolMessage(ProtocolMessageCode code, byte[] data, RemoteEndpoint skipEndpoint = null)
            {
                if(data == null)
                {
                    Logging.warn(string.Format("Invalid protocol message data for {0}", code));
                    return false;
                }

                bool c_result = NetworkClientManager.broadcastData(code, data, skipEndpoint);                
                bool s_result = NetworkServer.broadcastData(code, data, skipEndpoint);

                if (!c_result && !s_result)
                    return false;

                return true;
            }

            public static void syncWalletStateNeighbor(string neighbor)
            {
                if(NetworkClientManager.sendToClient(neighbor, ProtocolMessageCode.syncWalletState, new byte[1]) == false)
                {
                    NetworkServer.sendToClient(neighbor, ProtocolMessageCode.syncWalletState, new byte[1]);
                }
            }

            // Requests a specific wallet chunk from a specified neighbor
            // Returns true if request was sent. Returns false if the request could not be sent (socket error, missing neighbor, etc)
            public static bool getWalletStateChunkNeighbor(string neighbor, int chunk)
            {
                using (MemoryStream m = new MemoryStream())
                {
                    using (BinaryWriter writer = new BinaryWriter(m))
                    {
                        writer.Write(chunk);

                        if (NetworkClientManager.sendToClient(neighbor, ProtocolMessageCode.getWalletStateChunk, m.ToArray()) == false)
                        {
                            if (NetworkServer.sendToClient(neighbor, ProtocolMessageCode.getWalletStateChunk, m.ToArray()) == false)
                                return false;
                        }
                    }
                }
                return true;
            }

            public static void sendWalletStateChunk(RemoteEndpoint endpoint, WsChunk chunk)
            {
                using (MemoryStream m = new MemoryStream())
                {
                    using (BinaryWriter writer = new BinaryWriter(m))
                    {
                        writer.Write(chunk.blockNum);
                        writer.Write(chunk.chunkNum);
                        writer.Write(chunk.wallets.Length);
                        foreach(Wallet w in chunk.wallets)
                        {
                            writer.Write(w.id);
                            writer.Write(w.balance.ToString());
                            writer.Write(w.data);
                            writer.Write(w.nonce);
                        }
                        //
                        endpoint.sendData(ProtocolMessageCode.walletStateChunk, m.ToArray());
                    }
                }
            }

            private static int getDataLengthFromMessageHeader(List<byte> header)
            {
                int data_length = -1;
                // we should have the full header, save the data length
                using (MemoryStream m = new MemoryStream(header.ToArray()))
                {
                    using (BinaryReader reader = new BinaryReader(m))
                    {
                        reader.ReadByte(); // skip start byte
                        int code = reader.ReadInt32(); // skip message code
                        data_length = reader.ReadInt32(); // finally read data length
                        byte[] data_checksum = reader.ReadBytes(32); // skip checksum sha256, 32 bytes
                        byte checksum = reader.ReadByte(); // header checksum byte
                        byte endByte = reader.ReadByte(); // end byte

                        if (endByte != 'I')
                        {
                            Logging.warn("Header end byte was not 'I'");
                            return -1;
                        }

                        if(getHeaderChecksum(header.Take(41).ToArray()) != checksum)
                        {
                            Logging.warn(String.Format("Header checksum mismatch"));
                            return -1;
                        }

                        if (data_length <= 0)
                        {
                            Logging.warn(String.Format("Data length was {0}, code {1}", data_length, code));
                            return -1;
                        }

                        if (data_length > 10000000)
                        {
                            Logging.warn(String.Format("Data length was bigger than 10MB - {0}, code {1}.", data_length, code));
                            return -1;
                        }
                    }
                }
                return data_length;
            }

            // Reads data from a socket and returns a byte array
            public static byte[] readSocketData(Socket socket)
            {
                if(currentBuffer == null)
                {
                    currentBuffer = new byte[8192];
                }

                byte[] data = null;

                // Check for socket availability
                if (socket.Connected == false)
                {
                    throw new Exception("Socket already disconnected at other end");
                }

                if (socket.Available < 1)
                {
                    // Sleep a while to prevent cpu cycle waste
                    Thread.Sleep(10);
                    return data;
                }

                // Read multi-packet messages
                // TODO: optimize this as it's not very efficient
                List<byte> big_buffer = new List<byte>();

                bool message_found = false;

                try
                {
                    int data_length = 0;
                    int header_length = 43; // start byte + int32 (4 bytes) + int32 (4 bytes) + checksum (32 bytes) + header checksum (1 byte) + end byte
                    int bytesToRead = 1;
                    while (message_found == false && socket.Connected)
                    {
                        //int pos = bytesToRead > recvByteHist.Length ? recvByteHist.Length - 1 : bytesToRead;
                        /*lock (recvByteHist)
                        {
                            recvByteHist[pos]++;
                        }*/
                        int byteCounter = socket.Receive(currentBuffer, bytesToRead, SocketFlags.None);

                        if (byteCounter > 0)
                        {
                            if (big_buffer.Count > 0)
                            {
                                big_buffer.AddRange(currentBuffer.Take(byteCounter));
                                if (big_buffer.Count == header_length)
                                {
                                    data_length = getDataLengthFromMessageHeader(big_buffer);
                                    if (data_length <= 0)
                                    {
                                        data_length = 0;
                                        big_buffer.Clear();
                                        bytesToRead = 1;
                                    }
                                }
                                else if (big_buffer.Count == data_length + header_length)
                                {
                                    // we have everything that we need, save the last byte and break
                                    message_found = true;
                                }
                                if (data_length > 0)
                                {
                                    bytesToRead = data_length + header_length - big_buffer.Count;
                                    if (bytesToRead > 8000)
                                    {
                                        bytesToRead = 8000;
                                    }
                                }
                            }
                            else
                            {
                                if (currentBuffer[0] == 'X') // X is the message start byte
                                {
                                    big_buffer.Add(currentBuffer[0]);
                                    bytesToRead = header_length - 1; // header length - start byte
                                }
                            }
                            Thread.Yield();
                        }
                        else
                        {
                            // sleep a litte while waiting for bytes
                            Thread.Sleep(10);
                            // TODO TODO TODO, should reset the big_buffer if a timeout occurs
                        }
                    }
                }
                catch (Exception e)
                {
                    Logging.error(String.Format("NET: endpoint disconnected {0}", e));
                    throw;
                }
                if (message_found)
                {
                    data = big_buffer.ToArray();
                }
                return data;
            }

            // Read a protocol message from a byte array
            public static void readProtocolMessage(byte[] recv_buffer, RemoteEndpoint endpoint)
            {
                if (endpoint == null)
                {
                    Logging.error("Endpoint was null. readProtocolMessage");
                    return;
                }

                ProtocolMessageCode code = ProtocolMessageCode.hello;
                byte[] data = null;

                using (MemoryStream m = new MemoryStream(recv_buffer))
                {
                    using (BinaryReader reader = new BinaryReader(m))
                    {
                        // Check for multi-message packets. One packet can contain multiple network messages.
                        while (reader.BaseStream.Position < reader.BaseStream.Length)
                        {
                            byte[] data_checksum;
                            try
                            {
                                byte startByte = reader.ReadByte();

                                int message_code = reader.ReadInt32();
                                code = (ProtocolMessageCode)message_code;

                                int data_length = reader.ReadInt32();

                                // If this is a connected client, filter messages
                                if (endpoint.GetType() == typeof(RemoteEndpoint) )
                                {
                                    if (endpoint.presence == null)
                                    {
                                        // Check for presence and only accept hello and syncPL messages if there is no presence.
                                        if (code == ProtocolMessageCode.hello || code == ProtocolMessageCode.syncPresenceList || code == ProtocolMessageCode.getBalance || code == ProtocolMessageCode.newTransaction)
                                        {

                                        }
                                        else
                                        {
                                            // Ignore anything else
                                            return;
                                        }
                                    }
                                }




                                data_checksum = reader.ReadBytes(32); // sha256, 8 bits per byte
                                byte header_checksum = reader.ReadByte();
                                byte endByte = reader.ReadByte();
                                data = reader.ReadBytes(data_length);
                            }
                            catch (Exception e)
                            {
                                Logging.error(String.Format("NET: dropped packet. {0}", e));
                                return;
                            }
                            // Compute checksum of received data
                            byte[] local_checksum = Crypto.sha256(data);

                            // Verify the checksum before proceeding
                            if (Crypto.byteArrayCompare(local_checksum, data_checksum) == false)
                            {
                                Logging.error("Dropped message (invalid checksum)");
                                continue;
                            }

                            // For development purposes, output the proper protocol message
                            //Console.WriteLine(string.Format("NET: {0} | {1} | {2}", code, data_length, Crypto.hashToString(data_checksum)));

                            // Can proceed to parse the data parameter based on the protocol message code.
                            // Data can contain multiple elements.
                            //parseProtocolMessage(code, data, socket, endpoint);
                            NetworkQueue.receiveProtocolMessage(code, data, Crypto.hashToString(data_checksum), endpoint);
                        }
                    }
                }




            }

            // Unified protocol message parsing
            public static void parseProtocolMessage(ProtocolMessageCode code, byte[] data, RemoteEndpoint endpoint)
            {
                if (endpoint == null)
                {
                    Logging.error("Endpoint was null. parseProtocolMessage");
                    return;
                }

                try
                {
                    switch (code)
                    {
                        case ProtocolMessageCode.hello:
                            using (MemoryStream m = new MemoryStream(data))
                            {
                                using (BinaryReader reader = new BinaryReader(m))
                                {
                                    // Check for hello messages that don't originate from RemoteEndpoints
                                    if(endpoint == null)
                                    {
                                        return;
                                    }

                                    // Node already has a presence
                                    if(endpoint != null && endpoint.presence != null )
                                    {
                                        // Ignore the hello message in this case
                                        return;
                                    }

                                    /*Logging.info(String.Format("New node connected with advertised address {0}", hostname));
                                    if(CoreNetworkUtils.PingAddressReachable(hostname) == false)
                                    {
                                        Logging.warn("New node was not reachable on the advertised address.");
                                        using (MemoryStream reply_stream = new MemoryStream())
                                        {
                                            using (BinaryWriter w = new BinaryWriter(reply_stream))
                                            {
                                                w.Write("External IP:Port not reachable!");
                                                socket.Send(reply_stream.ToArray(), SocketFlags.None);
                                                socket.Disconnect(true);
                                                return;
                                            }
                                        }
                                    }*/
                                    //Console.WriteLine("Received IP: {0}", hostname);

                                    // Verify that the reported hostname matches the actual socket's IP
                                    //endpoint.remoteIP;


                                    // Another layer to catch any incompatible node exceptions for the hello message
                                    try
                                    {
                                        string addr = reader.ReadString();
                                        bool test_net = reader.ReadBoolean();
                                        char node_type = reader.ReadChar();
                                        string node_version = reader.ReadString();
                                        string device_id = reader.ReadString();
                                        string s2pubkey = reader.ReadString();
                                        string pubkey = reader.ReadString();
                                        int port = reader.ReadInt32();


                                        // Check the testnet designator and disconnect on mismatch
                                        if (test_net != Config.isTestNet)
                                        {
                                            using (MemoryStream m2 = new MemoryStream())
                                            {
                                                using (BinaryWriter writer = new BinaryWriter(m2))
                                                {
                                                    writer.Write(string.Format("Incorrect testnet designator: {0}. Should be {1}", test_net, Config.isTestNet));
                                                    Logging.warn(string.Format("Rejected master node {0} due to incorrect testnet designator: {1}", endpoint.fullAddress, test_net));
                                                    endpoint.sendData(ProtocolMessageCode.bye, m2.ToArray());
                                                    endpoint.stop();
                                                    return;
                                                }
                                            }
                                        }

                                        //Console.WriteLine("Received Address: {0} of type {1}", addr, node_type);

                                        endpoint.incomingPort = port;

                                        // Store the presence address for this remote endpoint
                                        endpoint.presenceAddress = new PresenceAddress(device_id, endpoint.getFullAddress(true), node_type, node_version);

                                        // Create a temporary presence with the client's address and device id
                                        Presence presence = new Presence(addr, s2pubkey, pubkey, endpoint.presenceAddress);



                                        // Connect to this node only if it's a master node
                                        if(node_type == 'M')
                                        {
                                            // Check the wallet balance for the minimum amount of coins
                                            IxiNumber balance = Node.walletState.getWalletBalance(addr);
                                            if(balance < Config.minimumMasterNodeFunds)
                                            {
                                                using (MemoryStream m2 = new MemoryStream())
                                                {
                                                    using (BinaryWriter writer = new BinaryWriter(m2))
                                                    {
                                                        writer.Write(string.Format("Insufficient funds. Minimum is {0}", Config.minimumMasterNodeFunds));
                                                        Logging.warn(string.Format("Rejected master node {0} due to insufficient funds: {1}", endpoint.getFullAddress(), balance.ToString()));
                                                        endpoint.sendData(ProtocolMessageCode.bye, m2.ToArray());
                                                        endpoint.stop();
                                                        return;
                                                    }
                                                }
                                            }
                                           
                                            // Limit to one IP per masternode
                                            // TODO TODO TODO - think about this and do it properly
                                            /*string[] hostname_split = hostname.Split(':');
                                            if (PresenceList.containsIP(hostname_split[0], 'M'))
                                            {
                                                using (MemoryStream m2 = new MemoryStream())
                                                {
                                                    using (BinaryWriter writer = new BinaryWriter(m2))
                                                    {
                                                        writer.Write(string.Format("This IP address ( {0} ) already has a masternode connected.", hostname_split[0]));
                                                        Logging.info(string.Format("Rejected master node {0} due to duplicate IP address", hostname));
                                                        socket.Send(prepareProtocolMessage(ProtocolMessageCode.bye, m2.ToArray()), SocketFlags.None);
                                                        socket.Disconnect(true);
                                                        return;
                                                    }
                                                }
                                            }*/
                                            
                                        }


                                        // Retrieve the final presence entry from the list (or create a fresh one)
                                        endpoint.presence = PresenceList.updateEntry(presence);

                                    }
                                    catch(Exception e)
                                    {
                                        // Disconnect the node in case of any reading errors
                                        Logging.warn(string.Format("Older node connected. {0}", e.ToString()));
                                        using (MemoryStream m2 = new MemoryStream())
                                        {
                                            using (BinaryWriter writer = new BinaryWriter(m2))
                                            {
                                                writer.Write(string.Format("Please update your Ixian node to connect."));
                                                endpoint.sendData(ProtocolMessageCode.bye, m2.ToArray());
                                                endpoint.stop();
                                                return;
                                            }
                                        }
                                    }

                                }
                            }


                            using (MemoryStream m = new MemoryStream())
                            {
                                using (BinaryWriter writer = new BinaryWriter(m))
                                {
                                    // Send the node version
                                    writer.Write(Config.nodeVersion);

                                    ulong lastBlock = Node.blockChain.getLastBlockNum();
                                    Block block = Node.blockChain.getBlock(lastBlock);
                                    if(block == null)
                                    {
                                        Logging.warn("Clients are connecting, but we have no blocks yet to send them!");
                                        return;
                                    }
                                    lastBlock = block.blockNum;
                                    writer.Write(lastBlock);
                                    writer.Write(block.blockChecksum);
                                    writer.Write(block.walletStateChecksum);
                                    writer.Write(Node.blockChain.getRequiredConsensus());
                                    writer.Write(Node.getCurrentTimestamp());

                                    endpoint.sendData(ProtocolMessageCode.helloData, m.ToArray());
                                }
                            }
                            break;

                        case ProtocolMessageCode.helloData:
                            using (MemoryStream m = new MemoryStream(data))
                            {
                                using (BinaryReader reader = new BinaryReader(m))
                                {
                                    int node_version = reader.ReadInt32();
                                    Logging.info(string.Format("Received Hello: Node version {0}", node_version));
                                    // Check for incompatible nodes
                                    if (node_version < Config.nodeVersion)
                                    {
                                        Logging.warn(String.Format("Hello: Connected node version ({0}) is too old! Upgrade the node.", node_version));
                                        endpoint.stop();
                                        return;
                                    }

                                    ulong last_block_num = reader.ReadUInt64();
                                    string block_checksum = reader.ReadString();
                                    string walletstate_checksum = reader.ReadString();
                                    int consensus = reader.ReadInt32();
                                    long timestamp = reader.ReadInt64();

                                    long myTimestamp = Node.getCurrentTimestamp();

                                    if (timestamp > myTimestamp + 100 || timestamp < myTimestamp - 100)
                                    {
                                        Logging.warn("This node's time is very different from network's time.");
                                    }

                                    if (Node.checkCurrentBlockDeprecation(last_block_num) == false)
                                    {
                                        endpoint.stop();
                                        return;
                                    }

                                    Node.blockSync.onHelloDataReceived(last_block_num, block_checksum, walletstate_checksum, consensus, timestamp);
                                }
                            }
                            break;

                        case ProtocolMessageCode.getBlock:
                            {
                                using (MemoryStream m = new MemoryStream(data))
                                {
                                    using (BinaryReader reader = new BinaryReader(m))
                                    {
                                        ulong block_number = reader.ReadUInt64();

                                        Logging.info(String.Format("Block #{0} has been requested.", block_number));
                                        // TODO TODO TODO full history node
                                        Block block = Node.blockChain.getBlock(block_number, Config.storeFullHistory);
                                        if (block == null)
                                        {
                                            Logging.warn(String.Format("Unable to find block #{0} in the chain!", block_number));
                                            return;
                                        }
                                        Logging.info(String.Format("Block #{0} ({1}) found, transmitting...", block_number, block.blockChecksum.Substring(4)));
                                        // Send the block
                                        endpoint.sendData(ProtocolMessageCode.blockData, block.getBytes());

                                        // if somebody requested last block from the chain, re-broadcast the localNewBlock as well
                                        // TODO: looking for a better solution but will likely need an updated network subsystem
                                        if (Node.blockChain.getLastBlockNum() == block_number)
                                        {
                                            Block localNewBlock = Node.blockProcessor.getLocalBlock();
                                            if (localNewBlock != null)
                                            {
                                                ProtocolMessage.broadcastNewBlock(localNewBlock);
                                            }
                                        }
                                    }
                                }
                            }
                            break;

                        case ProtocolMessageCode.getBalance:
                            {
                                using (MemoryStream m = new MemoryStream(data))
                                {
                                    using (BinaryReader reader = new BinaryReader(m))
                                    {
                                        string address = reader.ReadString();

                                        // Retrieve the latest balance
                                        IxiNumber balance = Node.walletState.getWalletBalance(address);

                                        // Return the balance for the matching address
                                        using (MemoryStream mw = new MemoryStream())
                                        {
                                            using (BinaryWriter writerw = new BinaryWriter(mw))
                                            {
                                                writerw.Write(address);
                                                writerw.Write(balance.ToString());

 
                                                endpoint.sendData(ProtocolMessageCode.balance, mw.ToArray());
                                            }
                                        }
                                    }
                                }
                            }
                            break;

                        case ProtocolMessageCode.getTransaction:
                            {
                                using (MemoryStream m = new MemoryStream(data))
                                {
                                    using (BinaryReader reader = new BinaryReader(m))
                                    {
                                        // Retrieve the transaction id
                                        string txid = reader.ReadString();

                                        // Check for a transaction corresponding to this id
                                        Transaction transaction = TransactionPool.getTransaction(txid, Config.storeFullHistory);
                                        if (transaction == null)
                                        {
                                            Logging.warn(String.Format("I do not have txid '{0}.", txid));
                                            return;
                                        }

                                        Logging.info(String.Format("Sending transaction {0} - {1} - {2} - {3}.", txid, transaction.id, transaction.checksum, transaction.amount));

                                        endpoint.sendData(ProtocolMessageCode.transactionData, transaction.getBytes());
                                    }
                                }
                            }
                            break;

                        case ProtocolMessageCode.newTransaction:
                            {
                                /*if(TransactionPool.checkSocketTransactionLimits(socket) == true)
                                {
                                    // Throttled, ignore this transaction
                                    return;
                                }*/

                                Transaction transaction = new Transaction(data);
                                if (transaction == null)
                                    return;
                                TransactionPool.addTransaction(transaction, false, endpoint);
                            }
                            break;

                        /*case ProtocolMessageCode.updateTransaction:
                            {
                                Transaction transaction = new Transaction(data);         
                                TransactionPool.updateTransaction(transaction);
                            }
                            break;*/

                        case ProtocolMessageCode.transactionData:
                            {
                                Transaction transaction = new Transaction(data);
                                if (transaction == null)
                                    return;

                                //
                                if (!Node.blockSync.synchronizing)
                                {
                                    if (transaction.type == (int)Transaction.Type.StakingReward)
                                    {
                                        // Skip received staking transactions if we're not synchronizing
                                        return;
                                    }
                                }

                                // Add the transaction to the pool
                                TransactionPool.addTransaction(transaction, true, endpoint);                               
                            }
                            break;
                        case ProtocolMessageCode.bye:
                            {
                                using (MemoryStream m = new MemoryStream(data))
                                {
                                    using (BinaryReader reader = new BinaryReader(m))
                                    {
                                        // Retrieve the message
                                        string message = reader.ReadString();
                                        Logging.error(string.Format("Disconnected with message: {0}", message));
                                    }
                                }
                            }
                            break;

                        case ProtocolMessageCode.newBlock:
                            {
                                Block block = new Block(data);
                                //Logging.info(String.Format("Network: Received block #{0} from {1}.", block.blockNum, socket.RemoteEndPoint.ToString()));
                                Node.blockSync.onBlockReceived(block);
                                Node.blockProcessor.onBlockReceived(block, endpoint);
                            }
                            break;

                        case ProtocolMessageCode.blockData:
                            {
                                Block block = new Block(data);
                                Node.blockSync.onBlockReceived(block);
                                Node.blockProcessor.onBlockReceived(block, endpoint);
                            }
                            break;

                        case ProtocolMessageCode.syncWalletState:
                            {
                                if(Node.blockSync.startOutgoingWSSync(endpoint) == false)
                                {
                                    Logging.warn(String.Format("Unable to start synchronizing with neighbor {0}",
                                        endpoint.presence.addresses[0].address));
                                    return;
                                }

                                // Request the latest walletstate header
                                using (MemoryStream m = new MemoryStream())
                                {
                                    using (BinaryWriter writer = new BinaryWriter(m))
                                    {
                                        ulong walletstate_block = Node.blockSync.pendingWsBlockNum;
                                        long walletstate_count = Node.walletState.numWallets;

                                        // Return the current walletstate block and walletstate count
                                        writer.Write(walletstate_block);
                                        writer.Write(walletstate_count);

                                        endpoint.sendData(ProtocolMessageCode.walletState, m.ToArray());
                                    }
                                }

                            }
                            break;

                        case ProtocolMessageCode.walletState:
                            {
                                using (MemoryStream m = new MemoryStream(data))
                                {
                                    using (BinaryReader reader = new BinaryReader(m))
                                    {
                                        ulong walletstate_block = 0;
                                        long walletstate_count = 0;
                                        try
                                        {
                                            walletstate_block = reader.ReadUInt64();
                                            walletstate_count = reader.ReadInt64();
                                        } catch(Exception e)
                                        {
                                            Logging.warn(String.Format("Error while receiving the WalletState header: {0}.", e.Message));
                                            return;
                                        }
                                        Node.blockSync.onWalletStateHeader(walletstate_block,walletstate_count);
                                    }
                                }
                            }
                            
                            break;

                        case ProtocolMessageCode.getWalletStateChunk:
                            {
                                using (MemoryStream m = new MemoryStream(data))
                                {
                                    using (BinaryReader reader = new BinaryReader(m))
                                    {
                                        int chunk_num = reader.ReadInt32();
                                        Node.blockSync.onRequestWalletChunk(chunk_num, endpoint);
                                    }
                                }
                            }
                            break;

                        case ProtocolMessageCode.walletStateChunk:
                            {
                                using (MemoryStream m = new MemoryStream(data))
                                {
                                    using (BinaryReader reader = new BinaryReader(m))
                                    {
                                        ulong block_num = reader.ReadUInt64();
                                        int chunk_num = reader.ReadInt32();
                                        int num_wallets = reader.ReadInt32();
                                        if(num_wallets > Config.walletStateChunkSplit)
                                        {
                                            Logging.error(String.Format("Received {0} wallets in a chunk. ( > {1}).",
                                                num_wallets, Config.walletStateChunkSplit));
                                            return;
                                        }
                                        Wallet[] wallets = new Wallet[num_wallets];
                                        for(int i =0;i<num_wallets;i++)
                                        {
                                            string w_id = reader.ReadString();
                                            IxiNumber w_balance = new IxiNumber(reader.ReadString());
                                            string w_data = reader.ReadString();
                                            ulong w_nonce = reader.ReadUInt64();
                                            wallets[i] = new Wallet(w_id, w_balance);
                                            wallets[i].data = w_data;
                                            wallets[i].nonce = w_nonce;
                                        }
                                        WsChunk c = new WsChunk
                                        {
                                            chunkNum = chunk_num,
                                            blockNum = block_num,
                                            wallets = wallets
                                        };
                                        Node.blockSync.onWalletChunkReceived(c);
                                    }
                                }
                            }
                            break;


                        case ProtocolMessageCode.syncPresenceList:
                            {
                                byte[] pdata = PresenceList.getBytes();
                                byte[] ba = prepareProtocolMessage(ProtocolMessageCode.presenceList, pdata);
                                endpoint.sendData(ProtocolMessageCode.presenceList, pdata);
                            }
                            break;

                        case ProtocolMessageCode.presenceList:
                            {
                                // TODO TODO TODO secure this further
                                if(isAuthoritativeNode(endpoint))
                                {
                                    Logging.info("NET: Receiving complete presence list");
                                    if (Node.presenceListActive == false)
                                    {
                                        Logging.info("Synchronizing complete presence list.");
                                        PresenceList.syncFromBytes(data);
                                        Node.presenceListActive = true;
                                    }
                                }
                            }
                            break;

                        case ProtocolMessageCode.updatePresence:
                            {
                                // TODO TODO TODO secure this further
                                if (isAuthoritativeNode(endpoint))
                                {
                                    // Parse the data and update entries in the presence list
                                    PresenceList.updateFromBytes(data);
                                }
                            }
                            break;

                        case ProtocolMessageCode.keepAlivePresence:
                            {
                                bool updated = PresenceList.receiveKeepAlive(data, endpoint.getFullAddress(true));
                                // If a presence entry was updated, broadcast this message again
                                if (updated)
                                {
                                    broadcastProtocolMessage(ProtocolMessageCode.keepAlivePresence, data, endpoint);
                                }
                                
                            }
                            break;

                        case ProtocolMessageCode.getPresence:
                            {
                                using (MemoryStream m = new MemoryStream(data))
                                {
                                    using (BinaryReader reader = new BinaryReader(m))
                                    {
                                        string wallet = reader.ReadString();
                                        // TODO re-verify this
                                        Presence p = PresenceList.presences.Find(x => x.wallet == wallet);
                                        if (p != null)
                                        {
                                            endpoint.sendData(ProtocolMessageCode.updatePresence, p.getBytes());
                                        }
                                        else
                                        {
                                            // TODO blacklisting point
                                            Logging.warn(string.Format("Node has requested presence information about {0} that is not in our PL.", wallet));
                                        }
                                    }
                                }
                            }
                            break;

                        case ProtocolMessageCode.getBlockTransactions:
                            {
                                // TODO TODO TODO split
                                using (MemoryStream m = new MemoryStream(data))
                                {
                                    using (BinaryReader reader = new BinaryReader(m))
                                    {
                                        ulong blockNum = reader.ReadUInt64();
                                        bool requestAllTransactions = reader.ReadBoolean();
                                        Logging.info(String.Format("Received request for transactions in block {0}.", blockNum));
                                        using (MemoryStream mOut = new MemoryStream())
                                        {
                                            using (BinaryWriter writer = new BinaryWriter(mOut))
                                            {
                                                // TODO TODO TODO full history node
                                                Block b = Node.blockChain.getBlock(blockNum, Config.storeFullHistory);
                                                List<string> txIdArr = null;
                                                if(b != null)
                                                {
                                                    txIdArr = new List<string>(b.transactions);
                                                }
                                                else
                                                {
                                                    lock(Node.blockProcessor.localBlockLock)
                                                    {
                                                        Block tmp = Node.blockProcessor.getLocalBlock();
                                                        if(tmp != null && tmp.blockNum == blockNum)
                                                        {
                                                            txIdArr = new List<string>(tmp.transactions);
                                                        }
                                                    }
                                                }
                                                if (txIdArr != null)
                                                {
                                                    for (int i = 0; i < txIdArr.Count; i++)
                                                    {
                                                        if (!requestAllTransactions)
                                                        {
                                                            if (txIdArr[i].StartsWith("stk"))
                                                            {
                                                                continue;
                                                            }
                                                        }
                                                        Transaction tx = TransactionPool.getTransaction(txIdArr[i], Config.storeFullHistory);
                                                        if (tx != null)
                                                        {
                                                            byte[] txBytes = tx.getBytes();

                                                            writer.Write(txBytes.Length);
                                                            writer.Write(txBytes);
                                                        }
                                                    }

                                                    endpoint.sendData(ProtocolMessageCode.transactionsChunk, mOut.ToArray());
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            break;

                        case ProtocolMessageCode.getUnappliedTransactions:
                            {
                                // TODO TODO TODO split
                                using (MemoryStream m = new MemoryStream(data))
                                {
                                    using (BinaryReader reader = new BinaryReader(m))
                                    {
                                        using (MemoryStream mOut = new MemoryStream())
                                        {
                                            using (BinaryWriter writer = new BinaryWriter(mOut))
                                            {
                                                Transaction[] txIdArr = TransactionPool.getUnappliedTransactions();
                                                foreach(Transaction tx in txIdArr)
                                                {
                                                    byte[] txBytes = tx.getBytes();

                                                    writer.Write(txBytes.Length);
                                                    writer.Write(txBytes);
                                                }

                                                endpoint.sendData(ProtocolMessageCode.transactionsChunk, mOut.ToArray());
                                            }
                                        }
                                    }
                                }
                            }
                            break;

                        case ProtocolMessageCode.transactionsChunk:
                            {
                                using (MemoryStream m = new MemoryStream(data))
                                {
                                    using (BinaryReader reader = new BinaryReader(m))
                                    {
                                        var sw = new System.Diagnostics.Stopwatch();
                                        sw.Start();
                                        int processedTxCount = 0;
                                        while (m.Length > m.Position)
                                        {
                                            int len = reader.ReadInt32();
                                            if (m.Position + len > m.Length)
                                            {
                                                // TODO blacklist
                                                Logging.warn(String.Format("A node is sending invalid transaction chunks (tx byte len > received data len)."));
                                                break;
                                            }
                                            byte[] txData = reader.ReadBytes(len);
                                            Transaction tx = new Transaction(txData);
                                            if(tx.type == (int)Transaction.Type.StakingReward && !Node.blockSync.synchronizing)
                                            {
                                                continue;
                                            }
                                            if (TransactionPool.getTransaction(tx.id) != null)
                                            {
                                                continue;
                                            }
                                            if (!TransactionPool.addTransaction(tx, true))
                                            {
                                                Logging.error(String.Format("Error adding transaction {0} received in a chunk to the transaction pool.", tx.id));
                                            }else
                                            {
                                                processedTxCount++;
                                            }
                                        }
                                        sw.Stop();
                                        TimeSpan elapsed = sw.Elapsed;
                                        Logging.info(string.Format("Processed {0} txs in {1}ms", processedTxCount, elapsed.TotalMilliseconds));
                                    }
                                }
                            }
                            break;
                        
                        default:
                            break;
                    }

                }
                catch(Exception e)
                {
                    Logging.error(string.Format("Error parsing network message. Details: {0} [ {1} ]", e.ToString(), e.StackTrace));
                }
                
            }

            // Check if the remote endpoint provided is authoritative
            private static bool isAuthoritativeNode(RemoteEndpoint endpoint)
            {
                // Disabled for dev purposes.
                // TODO: re-enable
                return true;

                if(endpoint == null)
                {
                    return false;
                }

                if(endpoint.presence == null || endpoint.presenceAddress == null)
                {
                    // This means the endpoint is neither a masternode nor a relay node
                    return false;
                }

                // Check if it's a master node
                if (endpoint.presenceAddress.type == 'M')
                {
                    return true;
                }

                // Check if it's a relay node
                if (endpoint.presenceAddress.type == 'R')
                {
                    return true;
                }

                // Otherwise it's a usual client
                return false;
            }

        }
    }
}