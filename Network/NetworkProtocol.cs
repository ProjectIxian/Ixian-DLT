using DLT.Meta;
using DLTNode;
using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Sockets;
using System.Text;
using System.Threading;

namespace DLT
{
    namespace Network
    {
        public class ProtocolMessage
        {

            // Prepare a network protocol message. Works for both client-side and server-side
            public static byte[] prepareProtocolMessage(ProtocolMessageCode code, byte[] data)
            {
                byte[] result = null;

                // Prepare the protocol sections
                int data_length = data.Length;
                byte[] data_checksum = Crypto.sha256(data);

                using (MemoryStream m = new MemoryStream())
                {
                    using (BinaryWriter writer = new BinaryWriter(m))
                    {
                        // Protocol sections are code, length, checksum, data
                        // Write each section in binary, in that specific order
                        writer.Write((int)code);
                        writer.Write(data_length);
                        writer.Write(data_checksum);
                        writer.Write(data);
                    }
                    result = m.ToArray();
                }

                return result;
            }

            public static void broadcastGetBlock(ulong block_num)
            {
                using (MemoryStream mw = new MemoryStream())
                {
                    using (BinaryWriter writerw = new BinaryWriter(mw))
                    {
                        writerw.Write(block_num);
                        writerw.Write(false);

                        broadcastProtocolMessage(ProtocolMessageCode.getBlock, mw.ToArray());
                    }
                }
            }

            public static void broadcastNewBlock(Block b)
            {
                broadcastProtocolMessage(ProtocolMessageCode.newBlock, b.getBytes());
            }

            public static void broadcastGetTransaction(string txid)
            {
                using (MemoryStream mw = new MemoryStream())
                {
                    using (BinaryWriter writerw = new BinaryWriter(mw))
                    {
                        writerw.Write(txid);

                        broadcastProtocolMessage(ProtocolMessageCode.getTransaction, mw.ToArray());
                    }
                }
            }

            // Broadcast a protocol message across clients and nodes
            public static void broadcastProtocolMessage(ProtocolMessageCode code, byte[] data, Socket skipSocket = null)
            {
                if(data == null)
                {
                    Logging.warn(string.Format("Invalid protocol message data for {0}", code));
                    return;
                }

                NetworkClientManager.broadcastData(code, data, skipSocket);
                NetworkServer.broadcastData(code, data, skipSocket);
            }

            // Server-side protocol reading
            public static void readProtocolMessage(Socket socket, RemoteEndpoint endpoint)
            {
                // Check for socket availability
                if(socket.Connected == false)
                {
                    throw new Exception("Socket already disconnected at other end");
                }

                if (socket.Available < 1)
                {
                    // Sleep a while to prevent cpu cycle waste
                    Thread.Sleep(100);
                    return;
                }

                // Read multi-packet messages
                // TODO: optimize this as it's not very efficient
                var big_buffer = new List<byte>();

                try
                {
                    while (socket.Available > 0)
                    {
                        var current_byte = new Byte[1];
                        var byteCounter = socket.Receive(current_byte, current_byte.Length, SocketFlags.None);

                        if (byteCounter.Equals(1))
                        {
                            big_buffer.Add(current_byte[0]);
                        }
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine("NET: endpoint disconnected " + e);
                    throw e;
                }

                byte[] recv_buffer = big_buffer.ToArray();

                ProtocolMessageCode code = ProtocolMessageCode.hello;
                byte[] data = null;

                using (MemoryStream m = new MemoryStream(recv_buffer))
                {
                    using (BinaryReader reader = new BinaryReader(m))
                    {
                        // Check for multi-message packets. One packet can contain multiple network messages.
                        while (reader.BaseStream.Position < reader.BaseStream.Length)
                        {
                            int message_code = reader.ReadInt32();
                            code = (ProtocolMessageCode)message_code;

                            int data_length = reader.ReadInt32();
                            if (data_length < 0)
                                return;

                            // If this is a connected client, filter messages
                            if (endpoint != null)
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

                            byte[] data_checksum;

                            try
                            {
                                data_checksum = reader.ReadBytes(32); // sha256, 8 bits per byte
                                data = reader.ReadBytes(data_length);
                            }
                            catch (Exception e)
                            {
                                Console.WriteLine("NET: dropped packet. " + e);
                                return;
                            }
                            // Compute checksum of received data
                            byte[] local_checksum = Crypto.sha256(data);

                            // Verify the checksum before proceeding
                            if (Crypto.byteArrayCompare(local_checksum, data_checksum) == false)
                            {
                                Logging.warn("Dropped message (invalid checksum)");
                                continue;
                            }

                            // For development purposes, output the proper protocol message
                            //Console.WriteLine(string.Format("NET: {0} | {1} | {2}", code, data_length, Crypto.hashToString(data_checksum)));

                            // Can proceed to parse the data parameter based on the protocol message code.
                            // Data can contain multiple elements.
                            parseProtocolMessage(code, data, socket, endpoint);
                        }
                    }
                }




            }

            // Unified protocol message parsing
            public static void parseProtocolMessage(ProtocolMessageCode code, byte[] data, Socket socket, RemoteEndpoint endpoint)
            {
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

                                    string hostname = reader.ReadString();
                                    //Console.WriteLine("Received IP: {0}", hostname);

                                    // Verify that the reported hostname matches the actual socket's IP
                                    //endpoint.remoteIP;


                                    // Another layer to catch any incompatible node exceptions for the hello message
                                    try
                                    {
                                        string addr = reader.ReadString();
                                        char node_type = reader.ReadChar();
                                        string device_id = reader.ReadString();
                                        string pubkey = reader.ReadString();

                                        // Read the metadata and provide backward compatibility with older nodes
                                        string meta = " ";
                                        try
                                        {
                                            meta = reader.ReadString();
                                        }
                                        catch (Exception)
                                        {

                                        }
                                        //Console.WriteLine("Received Address: {0} of type {1}", addr, node_type);

                                        // Store the presence address for this remote endpoint
                                        endpoint.presenceAddress = new PresenceAddress(device_id, hostname, node_type);

                                        // Create a temporary presence with the client's address and device id
                                        Presence presence = new Presence(addr, pubkey, meta, endpoint.presenceAddress);



                                        // Connect to this node only if it's a master node
                                        if(node_type == 'M')
                                        {
                                            // Check the wallet balance for the minimum amount of coins
                                            ulong balance = WalletState.getDeltaBalanceForAddress(addr);
                                            if(balance < Config.minimumMasterNodeFunds)
                                            {
                                                using (MemoryStream m2 = new MemoryStream())
                                                {
                                                    using (BinaryWriter writer = new BinaryWriter(m2))
                                                    {
                                                        writer.Write(string.Format("Insufficient funds. Minimum is {0}", Config.minimumMasterNodeFunds));
                                                        Logging.info(string.Format("Rejected master node {0} due to insufficient funds: {1}", hostname, balance));
                                                        socket.Send(prepareProtocolMessage(ProtocolMessageCode.bye, m2.ToArray()), SocketFlags.None);
                                                        socket.Disconnect(true);
                                                        return;
                                                    }
                                                }
                                            }

                                            if (NetworkServer.addNeighbor(hostname))
                                                Logging.info(string.Format("Master node detected at {0}. Added as neighbor.", hostname));
                                        }


                                        // Retrieve the final presence entry from the list (or create a fresh one)
                                        endpoint.presence = PresenceList.updateEntry(presence);

                                    }
                                    catch(Exception e)
                                    {
                                        Logging.info(string.Format("Older node connected. Please update node. {0}", e.ToString()));
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
                                    lastBlock = block.blockNum;
                                    writer.Write(lastBlock);
                                    writer.Write(block.blockChecksum);
                                    writer.Write(block.walletStateChecksum);

                                    byte[] ba = prepareProtocolMessage(ProtocolMessageCode.helloData, m.ToArray());
                                    socket.Send(ba, SocketFlags.None);
                                }
                            }
                            break;

                        case ProtocolMessageCode.helloData:
                            using (MemoryStream m = new MemoryStream(data))
                            {
                                using (BinaryReader reader = new BinaryReader(m))
                                {
                                    int node_version = reader.ReadInt32();

                                    // Check for incompatible nodes
                                    if (node_version < Config.nodeVersion)
                                    {
                                        Console.WriteLine("Hello: Connected node version ({0}) is too old! Upgrade the node.", node_version);
                                        socket.Disconnect(true);
                                        return;
                                    }
                                   

                                    ulong last_block_num = reader.ReadUInt64();
                                    string block_checksum = reader.ReadString();
                                    string walletstate_checksum = reader.ReadString();

                                    Console.WriteLine(string.Format("Received Hello: Node version {0}", node_version));
                                    Console.WriteLine(string.Format("\t|- Block Height:\t\t#{0}", last_block_num));
                                    Console.WriteLine(string.Format("\t|- Block Checksum:\t\t{0}", block_checksum));
                                    Console.WriteLine(string.Format("\t|- WalletState checksum:\t{0}", walletstate_checksum));

                                    if(Node.checkCurrentBlockDeprecation(last_block_num) == false)
                                    {
                                        socket.Disconnect(true);
                                        return;
                                    }


                                    // Check if this node has a newer block ready
                                    /*if (last_block_num > Node.blockChain.currentBlockNum + 1)
                                    {
                                        // Check if we're already in synchronization mode
                                        if (Node.blockProcessor.synchronizing)
                                            return;

                                        // Enter synchronization mode
                                        Node.blockProcessor.enterSyncMode(last_block_num, block_checksum, walletstate_checksum);

                                        // Synchronize the transaction pool and the wallet state
                                        socket.Send(prepareProtocolMessage(ProtocolMessageCode.syncPoolState, new byte[1]), SocketFlags.None);
                                        //socket.Send(prepareProtocolMessage(ProtocolMessageCode.syncWalletState, new byte[1]), SocketFlags.None);


                                        // Request the latest block
                                        broadcastGetBlock(last_block_num);

                                        // Get neighbors
                                        socket.Send(prepareProtocolMessage(ProtocolMessageCode.getNeighbors, new byte[1]), SocketFlags.None);

                                        // Get presences
                                        socket.Send(prepareProtocolMessage(ProtocolMessageCode.syncPresenceList, new byte[1]), SocketFlags.None);
                                    }/*/

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
                                        bool fetch_walletstate = reader.ReadBoolean(); // Deprecated as of version 4

                                        Block block = Node.blockChain.getBlock(block_number);
                                        if (block == null)
                                        {
                                            // If it's not in the blockchain, it's likely the local block
                                            block = Node.blockProcessor.getLocalBlock();

                                            // No localblock
                                            if (block == null || block.blockNum != block_number)
                                                return;

                                        }

                                        // Send the block
                                        socket.Send(prepareProtocolMessage(ProtocolMessageCode.blockData, block.getBytes()), SocketFlags.None);
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
                                        ulong balance = WalletState.getDeltaBalanceForAddress(address);

                                        // Return the balance for the matching address
                                        using (MemoryStream mw = new MemoryStream())
                                        {
                                            using (BinaryWriter writerw = new BinaryWriter(mw))
                                            {
                                                writerw.Write(address);
                                                writerw.Write(balance);

 
                                                byte[] ba = prepareProtocolMessage(ProtocolMessageCode.balance, mw.ToArray());
                                                socket.Send(ba, SocketFlags.None);
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
                                        Transaction transaction = TransactionStorage.getTransaction(txid);
                                        if (transaction == null)
                                            return;
                                        
                                        byte[] ba = prepareProtocolMessage(ProtocolMessageCode.transactionData, transaction.getBytes());
                                        socket.Send(ba, SocketFlags.None);
                                    }
                                }
                            }
                            break;

                        case ProtocolMessageCode.newTransaction:
                            {
                                Transaction transaction = new Transaction(data);
                                TransactionPool.addTransaction(transaction);
                            }
                            break;

                        case ProtocolMessageCode.updateTransaction:
                            {
                                Transaction transaction = new Transaction(data);         
                                TransactionPool.updateTransaction(transaction);
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
                                        Logging.warn(string.Format("Disconnected with message: {0}", message));
                                    }
                                }
                            }
                            break;

                        case ProtocolMessageCode.newBlock:
                            {
                                Block block = new Block(data);
                                Node.blockProcessor.onBlockReceived(block);
                            }
                            break;

                        case ProtocolMessageCode.blockData:
                            {
                                Block block = new Block(data);
                                Node.blockProcessor.onBlockReceived(block);
                            }
                            break;

                        case ProtocolMessageCode.syncPoolState:
                            {
                                byte[] tdata = TransactionPool.getBytes();
                                byte[] ba = prepareProtocolMessage(ProtocolMessageCode.poolState, tdata);
                                socket.Send(ba, SocketFlags.None);
                            }
                            break;

                        case ProtocolMessageCode.poolState:
                            {
                                if (isAuthoritativeNode(endpoint, socket))
                                {
                                    Console.WriteLine("NET: Received a new transaction pool state");
                                    TransactionPool.syncFromBytes(data);
                                }
                            }
                            break;

                        case ProtocolMessageCode.syncWalletState:
                            {
                                // Request the latest walletstate header
                                using (MemoryStream m = new MemoryStream())
                                {
                                    using (BinaryWriter writer = new BinaryWriter(m))
                                    {
                                        ulong walletstate_block = 0;
                                        long walletstate_count = WalletState.getTotalWallets();
                                        string walletstate_checksum = WalletState.calculateChecksum();

                                        // Return the current walletstate block and walletstate count
                                        writer.Write(walletstate_block);
                                        writer.Write(walletstate_count);
                                        writer.Write(walletstate_checksum);

                                        byte[] ba = prepareProtocolMessage(ProtocolMessageCode.walletState, m.ToArray());
                                        socket.Send(ba, SocketFlags.None);
                                    }
                                }

                            }
                            break;

                        case ProtocolMessageCode.walletState:
                            {
                                // Check if we're in synchronization mode
                                if (Node.blockProcessor.synchronizing == false)
                                {
                                    Logging.warn(string.Format("Received walletstate while not in sync mode from {0}", socket.RemoteEndPoint.ToString()));
                                    return;
                                }

                                Console.WriteLine("NET: Received a new wallet state header");
                                using (MemoryStream m = new MemoryStream(data))
                                {
                                    using (BinaryReader reader = new BinaryReader(m))
                                    {
                                        ulong walletstate_block = 0;
                                        long walletstate_count = 0;
                                        string walletstate_checksum = "";
                                        try
                                        {
                                            walletstate_block = reader.ReadUInt64();
                                            walletstate_count = reader.ReadInt64();
                                            walletstate_checksum = reader.ReadString();
                                        }
                                        catch (Exception e)
                                        {
                                            Logging.warn(e.ToString());
                                            return;
                                        }

                                        // Clear the walletstate first
                                        WalletState.clear();

                                        // Loop this based on walletstate_count and Config.walletStateChunkSplit
                                        int requestCount = 0;
                                        if (walletstate_count > 0)
                                            requestCount = (int)(walletstate_count / Config.walletStateChunkSplit) + 1;

                                        if (walletstate_count == 0)
                                            Logging.error("Received walletstate count is 0. Possibly corrupted walletstate. Ignoring...");

                                        for (int i = 0; i < requestCount; i++)
                                        {
                                            // Request the latest block
                                            using (MemoryStream mw = new MemoryStream())
                                            {
                                                using (BinaryWriter writerw = new BinaryWriter(mw))
                                                {
                                                    long startOffset = i * Config.walletStateChunkSplit;
                                                    long walletCount = Config.walletStateChunkSplit;

                                                    // Don't request more wallets than we need
                                                    // TODO: this might be a problem if the walletstate increases in size while fetching.
                                                    if (startOffset + walletCount > walletstate_count)
                                                        walletCount = walletstate_count - startOffset;

                                                    writerw.Write(startOffset);
                                                    writerw.Write(walletCount);
                                                    //Console.WriteLine("\t\t\tRequesting WalletState Chunk: {0} - {1}", startOffset, walletCount);

                                                    // Either broadcast the request
                                                    //  broadcastProtocolMessage(ProtocolMessageCode.getBlock, mw.ToArray());
                                                    // or send it to this specific node only
                                                    byte[] ba = prepareProtocolMessage(ProtocolMessageCode.getWalletStateChunk, mw.ToArray());
                                                    socket.Send(ba, SocketFlags.None);

                                                }
                                            }
                                        }
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
                                        long startOffset = 0;
                                        long walletCount = 0;
                                        try
                                        {
                                            startOffset = reader.ReadInt64();
                                            walletCount = reader.ReadInt64();
                                        }
                                        catch (Exception e)
                                        {
                                            Logging.warn(e.ToString());
                                            return;
                                        }

                                        ulong blockNum = Node.blockChain.getLastBlockNum();
                                        byte[] pdata = WalletState.getChunkBytes(startOffset, walletCount, blockNum);
                                        if (pdata == null)
                                            return;

                                        byte[] ba = prepareProtocolMessage(ProtocolMessageCode.walletStateChunk, pdata);
                                        socket.Send(ba, SocketFlags.None);
                                    }
                                }
                            }
                            break;

                        case ProtocolMessageCode.walletStateChunk:
                            {                                
                                WalletState.processChunk(data);
                            }
                            break;

                        case ProtocolMessageCode.getNeighbors:
                            {
                                //Console.WriteLine("Get neighbor data");
                                byte[] ndata = NetworkUtils.getNeighborsData();
                                if (ndata == null)
                                    return;
                                byte[] ba = prepareProtocolMessage(ProtocolMessageCode.neighborData, ndata);
                                socket.Send(ba, SocketFlags.None);
                            }
                            break;

                        case ProtocolMessageCode.neighborData:
                            {
                                if (isAuthoritativeNode(endpoint, socket))
                                {
                                    //Console.WriteLine("Received neighbor data");
                                    NetworkUtils.processNeighborsData(data);
                                }
                            }
                            break;

                        case ProtocolMessageCode.syncPresenceList:
                            {
                                byte[] pdata = PresenceList.getBytes();
                                byte[] ba = prepareProtocolMessage(ProtocolMessageCode.presenceList, pdata);
                                socket.Send(ba, SocketFlags.None);                               
                            }
                            break;

                        case ProtocolMessageCode.presenceList:
                            {
                                if(isAuthoritativeNode(endpoint, socket))
                                {
                                    Console.WriteLine("NET: Receiving complete presence list");
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
                                if (isAuthoritativeNode(endpoint, socket))
                                {
                                    // Parse the data and update entries in the presence list
                                    PresenceList.updateFromBytes(data);
                                }
                            }
                            break;

                        case ProtocolMessageCode.removePresence:
                            {
                                if (isAuthoritativeNode(endpoint, socket))
                                {
                                    // Parse the data and remove the entry from the presence list
                                    Presence presence = new Presence(data);
                                    if (presence.wallet.Equals(Node.walletStorage.address, StringComparison.Ordinal))
                                    {
                                        Console.WriteLine("[PL] Received removal of self from PL, ignoring.");
                                        return;
                                    }
                                    PresenceList.removeEntry(presence);
                                }
                            }
                            break;

                        case ProtocolMessageCode.keepAlivePresence:
                            {
                                bool updated = PresenceList.receiveKeepAlive(data);
                                // If a presence entry was updated, broadcast this message again
                                if (updated)
                                {
                                    broadcastProtocolMessage(ProtocolMessageCode.keepAlivePresence, data, socket);
                                }
                                
                            }
                            break;
                        
                        default:
                            break;
                    }

                }
                catch(Exception e)
                {
                    Logging.error(string.Format("Error parsing network message. Details: {0}", e.ToString()));
                }
                
            }

            // Check if the remote endpoint provided is authoritative
            private static bool isAuthoritativeNode(RemoteEndpoint endpoint, Socket socket)
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