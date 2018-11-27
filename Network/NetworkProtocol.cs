using DLT.Meta;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Linq;
using IXICore;

namespace DLT
{
    namespace Network
    {
        public class ProtocolMessage
        {
            public static readonly ulong[] recvByteHist = new ulong[100];

            // Handle the getBlockTransactions message
            // This is called from NetworkProtocol
            private static void handleGetBlockTransactions(byte[] data, RemoteEndpoint endpoint)
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
                                    b = tmp;
                                    txIdArr = new List<string>(tmp.transactions);
                                }
                            }
                        }

                        if (txIdArr == null)
                            return;

                        int tx_count = txIdArr.Count();

                        if (tx_count == 0)
                            return;

                        int num_chunks = tx_count / CoreConfig.maximumTransactionsPerChunk + 1;

                        // Go through each chunk
                        for (int i = 0; i < num_chunks; i++)
                        {
                            using (MemoryStream mOut = new MemoryStream())
                            {
                                using (BinaryWriter writer = new BinaryWriter(mOut))
                                {
                                    // Generate a chunk of transactions
                                    for (int j = 0; j < CoreConfig.maximumTransactionsPerChunk; j++)
                                    {
                                        int tx_index = i * CoreConfig.maximumTransactionsPerChunk + j;
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
                                    endpoint.sendData(ProtocolMessageCode.transactionsChunk, mOut.ToArray());
                                }
                            }
                        }

                        // Send the block
                        endpoint.sendData(ProtocolMessageCode.blockData, b.getBytes());

                    }
                }
            }

            // Handle the getUnappliedTransactions message
            // This is called from NetworkProtocol
            private static void handleGetUnappliedTransactions(byte[] data, RemoteEndpoint endpoint)
            {
                Transaction[] txIdArr = TransactionPool.getUnappliedTransactions();
                int tx_count = txIdArr.Count();

                if (tx_count == 0)
                    return;

                int num_chunks = tx_count / CoreConfig.maximumTransactionsPerChunk + 1;

                // Go through each chunk
                for (int i = 0; i < num_chunks; i++)
                {
                    using (MemoryStream mOut = new MemoryStream())
                    {
                        using (BinaryWriter writer = new BinaryWriter(mOut))
                        {
                            // Generate a chunk of transactions
                            for (int j = 0; j < CoreConfig.maximumTransactionsPerChunk; j++)
                            {
                                int tx_index = i * CoreConfig.maximumTransactionsPerChunk + j;
                                if (tx_index > tx_count - 1)
                                    break;

                                byte[] txBytes = txIdArr[tx_index].getBytes();
                                writer.Write(txBytes.Length);
                                writer.Write(txBytes);
                            }

                            // Send a chunk
                            endpoint.sendData(ProtocolMessageCode.transactionsChunk, mOut.ToArray());
                        }
                    }
                }
            }


            public static bool broadcastGetBlock(ulong block_num, RemoteEndpoint skipEndpoint = null)
            {
                using (MemoryStream mw = new MemoryStream())
                {
                    using (BinaryWriter writerw = new BinaryWriter(mw))
                    {
                        writerw.Write(block_num);

                        return broadcastProtocolMessage(ProtocolMessageCode.getBlock, mw.ToArray(), skipEndpoint, true);
                    }
                }
            }

            public static bool broadcastNewBlock(Block b, RemoteEndpoint skipEndpoint = null)
            {
                //Logging.info(String.Format("Broadcasting block #{0} : {1}.", b.blockNum, b.blockChecksum));
                return broadcastProtocolMessage(ProtocolMessageCode.newBlock, b.getBytes(), skipEndpoint);
            }

            public static bool broadcastGetTransaction(string txid, RemoteEndpoint endpoint = null)
            {
                using (MemoryStream mw = new MemoryStream())
                {
                    using (BinaryWriter writerw = new BinaryWriter(mw))
                    {
                        writerw.Write(txid);

                        if (endpoint != null)
                        {
                            if (endpoint.isConnected())
                            {
                                endpoint.sendData(ProtocolMessageCode.getTransaction, mw.ToArray());
                                return true;
                            }
                            return false;
                        }
                        else
                        {
                            return broadcastProtocolMessage(ProtocolMessageCode.getTransaction, mw.ToArray(), null, true);
                        }
                    }
                }
            }

            public static void broadcastGetBlockTransactions(ulong blockNum, bool requestAllTransactions, RemoteEndpoint endpoint)
            {
                using (MemoryStream mw = new MemoryStream())
                {
                    using (BinaryWriter writerw = new BinaryWriter(mw))
                    {
                        writerw.Write(blockNum);
                        writerw.Write(requestAllTransactions);

                        if (endpoint != null)
                        {
                            endpoint.sendData(ProtocolMessageCode.getBlockTransactions, mw.ToArray());
                        }else
                        {
                            broadcastProtocolMessage(ProtocolMessageCode.getBlockTransactions, mw.ToArray(), null, true);
                        }
                    }
                }
            }

            // Broadcast a protocol message across clients and nodes
            // Returns true if it sent the message at least one endpoint. Returns false if the message couldn't be sent to any endpoints
            public static bool broadcastProtocolMessage(ProtocolMessageCode code, byte[] data, RemoteEndpoint skipEndpoint = null, bool sendToSingleRandomNode = false)
            {
                if(data == null)
                {
                    Logging.warn(string.Format("Invalid protocol message data for {0}", code));
                    return false;
                }

                if(sendToSingleRandomNode)
                {
                    int serverCount = NetworkClientManager.getConnectedClients().Count();
                    int clientCount = NetworkServer.getConnectedClients().Count();

                    Random r = new Random();
                    int rIdx = r.Next(serverCount + clientCount);

                    RemoteEndpoint re = null;

                    if (rIdx < serverCount)
                    {
                        re = NetworkClientManager.getClient(rIdx);
                    }else
                    {
                        re = NetworkServer.getClient(rIdx - serverCount);
                    }
                    if (re != null && re.isConnected())
                    {
                        re.sendData(code, data);
                        return true;
                    }
                    return false;
                }
                else
                {
                    bool c_result = NetworkClientManager.broadcastData(code, data, skipEndpoint);
                    bool s_result = NetworkServer.broadcastData(code, data, skipEndpoint);

                    if (!c_result && !s_result)
                        return false;
                }



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

            // Sends a single wallet chunk
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
                            writer.Write(w.id.Length);
                            writer.Write(w.id);
                            writer.Write(w.balance.ToString());

                            if (w.data != null)
                            {
                                writer.Write(w.data.Length);
                                writer.Write(w.data);
                            }else
                            {
                                writer.Write((int)0);
                            }

                            if (w.publicKey != null)
                            {
                                writer.Write(w.publicKey.Length);
                                writer.Write(w.publicKey);
                            }
                            else
                            {
                                writer.Write((int)0);
                            }
                        }
                        //
                        endpoint.sendData(ProtocolMessageCode.walletStateChunk, m.ToArray());
                    }
                }
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
                            byte[] local_checksum = Crypto.sha512sqTrunc(data);

                            // Verify the checksum before proceeding
                            if (local_checksum.SequenceEqual(data_checksum) == false)
                            {
                                Logging.error("Dropped message (invalid checksum)");
                                continue;
                            }

                            // For development purposes, output the proper protocol message
                            //Console.WriteLine(string.Format("NET: {0} | {1} | {2}", code, data_length, Crypto.hashToString(data_checksum)));

                            // Can proceed to parse the data parameter based on the protocol message code.
                            // Data can contain multiple elements.
                            //parseProtocolMessage(code, data, socket, endpoint);
                            NetworkQueue.receiveProtocolMessage(code, data, data_checksum, endpoint);
                        }
                    }
                }
            }

            public static bool checkNodeConnectivity(RemoteEndpoint endpoint)
            {
                // TODO TODO TODO TODO we should put this in a separate thread
                string hostname = endpoint.getFullAddress(true);
                if (CoreNetworkUtils.PingAddressReachable(hostname) == false)
                {
                    Logging.warn("New node was not reachable on the advertised address.");
                    using (MemoryStream reply_stream = new MemoryStream())
                    {
                        using (BinaryWriter w = new BinaryWriter(reply_stream))
                        {
                            w.Write(601);
                            w.Write("External IP:Port not reachable!");
                            w.Write("");
                            endpoint.sendData(ProtocolMessageCode.bye, reply_stream.ToArray());
                        }
                    }
                    return false;
                }
                return true;
            }

            public static bool processHelloMessage(RemoteEndpoint endpoint, BinaryReader reader)
            {
                // Node already has a presence
                if (endpoint.presence != null)
                {
                    // Ignore the hello message in this case
                    return false;
                }

                // Another layer to catch any incompatible node exceptions for the hello message
                try
                {
                    int protocol_version = reader.ReadInt32();

                    Logging.info(string.Format("Received Hello: Node version {0}", protocol_version));
                    // Check for incompatible nodes
                    if (protocol_version < CoreConfig.protocolVersion)
                    {
                        using (MemoryStream m2 = new MemoryStream())
                        {
                            using (BinaryWriter writer = new BinaryWriter(m2))
                            {
                                Logging.warn(String.Format("Hello: Connected node version ({0}) is too old! Upgrade the node.", protocol_version));
                                writer.Write(string.Format("Your node version is too old. Should be at least {0} is {1}", CoreConfig.protocolVersion, protocol_version));
                                endpoint.sendData(ProtocolMessageCode.bye, m2.ToArray());
                                return false;
                            }
                        }
                    }

                    int addrLen = reader.ReadInt32();
                    byte[] addr = reader.ReadBytes(addrLen);

                    bool test_net = reader.ReadBoolean();
                    char node_type = reader.ReadChar();
                    string node_version = reader.ReadString();
                    string device_id = reader.ReadString();

                    int pkLen = reader.ReadInt32();
                    byte[] pubkey = reader.ReadBytes(pkLen);

                    int port = reader.ReadInt32();
                    long timestamp = reader.ReadInt64();

                    int sigLen = reader.ReadInt32();
                    byte[] signature = reader.ReadBytes(sigLen);

                    // Check the testnet designator and disconnect on mismatch
                    if (test_net != Config.isTestNet)
                    {
                        using (MemoryStream m2 = new MemoryStream())
                        {
                            using (BinaryWriter writer = new BinaryWriter(m2))
                            {
                                writer.Write(string.Format("Incorrect testnet designator: {0}. Should be {1}", test_net, Config.isTestNet));
                                Logging.warn(string.Format("Rejected node {0} due to incorrect testnet designator: {1}", endpoint.fullAddress, test_net));
                                endpoint.sendData(ProtocolMessageCode.bye, m2.ToArray());
                                return false;
                            }
                        }
                    }

                    // Check the address and pubkey and disconnect on mismatch
                    if (!addr.SequenceEqual((new Address(pubkey)).address))
                    {
                        using (MemoryStream m2 = new MemoryStream())
                        {
                            using (BinaryWriter writer = new BinaryWriter(m2))
                            {
                                writer.Write(string.Format("Pubkey and address do not match."));
                                Logging.warn(string.Format("Pubkey and address do not match."));
                                endpoint.sendData(ProtocolMessageCode.bye, m2.ToArray());
                                return false;
                            }
                        }
                    }


                    //Console.WriteLine("Received Address: {0} of type {1}", addr, node_type);

                    endpoint.incomingPort = port;

                    // Verify the signature
                    if (node_type == 'C')
                    {
                        // TODO: verify if the client is connectable and if so, add the presence

                        // Client is not connectable, don't add a presence
                        return true;
                    }
                    else
                    if (CryptoManager.lib.verifySignature(Encoding.UTF8.GetBytes(CoreConfig.ixianChecksumLockString + "-" + device_id + "-" + timestamp + "-" + endpoint.getFullAddress(true)), pubkey, signature) == false)
                    {
                        using (MemoryStream m2 = new MemoryStream())
                        {
                            using (BinaryWriter writer = new BinaryWriter(m2))
                            {
                                writer.Write(600);
                                writer.Write("Verify signature failed in hello message, likely an incorrect IP was specified. Detected IP:");
                                writer.Write(endpoint.address);
                                Logging.warn(string.Format("Verify signature failed in hello message, likely an incorrect IP was specified. Detected IP: {0}", endpoint.address));
                                endpoint.sendData(ProtocolMessageCode.bye, m2.ToArray());
                                return false;
                            }
                        }
                    }

                    // if we're a client update the network time difference
                    if(endpoint.GetType() == typeof(NetworkClient))
                    {
                        long curTime = Clock.getTimestamp(DateTime.Now);

                        long timeDiff = 0;

                        // amortize +- 5 seconds
                        if (curTime - timestamp < -5 || curTime - timestamp > 5)
                        {
                            timeDiff = curTime - timestamp;
                        }else
                        {
                            timeDiff = 0;
                        }

                        ((NetworkClient)endpoint).timeDifference = timeDiff;
                    }else
                    {
                        if (node_type == 'M' || node_type == 'H' || node_type == 'R')
                        {
                            if (!checkNodeConnectivity(endpoint))
                            {
                                return false;
                            }
                        }
                    }

                    // Store the presence address for this remote endpoint
                    endpoint.presenceAddress = new PresenceAddress(device_id, endpoint.getFullAddress(true), node_type, node_version, Core.getCurrentTimestamp(), null);

                    // Create a temporary presence with the client's address and device id
                    Presence presence = new Presence(addr, pubkey, null, endpoint.presenceAddress);



                    // Connect to this node only if it's a master node or a full history node
                    if (node_type == 'M' || node_type == 'H')
                    {
                        if (endpoint.GetType() == typeof(RemoteEndpoint))
                        {
                            // Check the wallet balance for the minimum amount of coins
                            IxiNumber balance = Node.walletState.getWalletBalance(addr);
                            if (balance < CoreConfig.minimumMasterNodeFunds)
                            {
                                using (MemoryStream m2 = new MemoryStream())
                                {
                                    using (BinaryWriter writer = new BinaryWriter(m2))
                                    {
                                        writer.Write(string.Format("Insufficient funds. Minimum is {0}", CoreConfig.minimumMasterNodeFunds));
                                        Logging.warn(string.Format("Rejected master node {0} due to insufficient funds: {1}", endpoint.getFullAddress(), balance.ToString()));
                                        endpoint.sendData(ProtocolMessageCode.bye, m2.ToArray());
                                        return false;
                                    }
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
                catch (Exception e)
                {
                    // Disconnect the node in case of any reading errors
                    Logging.warn(string.Format("Older node connected. {0}", e.ToString()));
                    using (MemoryStream m2 = new MemoryStream())
                    {
                        using (BinaryWriter writer = new BinaryWriter(m2))
                        {
                            writer.Write(string.Format("Please update your Ixian node to connect."));
                            endpoint.sendData(ProtocolMessageCode.bye, m2.ToArray());
                            return false;
                        }
                    }
                }
                return true;
            }

            public static void sendHelloMessage(RemoteEndpoint endpoint, bool sendHelloData)
            {
                using (MemoryStream m = new MemoryStream())
                {
                    using (BinaryWriter writer = new BinaryWriter(m))
                    {
                        string publicHostname = string.Format("{0}:{1}", Config.publicServerIP, Config.serverPort);

                        // Send the node version
                        writer.Write(CoreConfig.protocolVersion);

                        // Send the public node address
                        byte[] address = Node.walletStorage.address;
                        writer.Write(address.Length);
                        writer.Write(address);

                        // Send the testnet designator
                        writer.Write(Config.isTestNet);

                        // Send the node type
                        char node_type = 'M'; // This is a Master node

                        if(Config.storeFullHistory)
                        {
                            node_type = 'M'; // TODO TODO TODO TODO this is only temporary until all nodes upgrade, changes this to 'H' later
                        }

                        if (Node.isWorkerNode())
                            node_type = 'W'; // This is a Worker node

                        writer.Write(node_type);

                        // Send the version
                        writer.Write(Config.version);

                        // Send the node device id
                        writer.Write(Config.device_id);

                        // Send the wallet public key
                        writer.Write(Node.walletStorage.publicKey.Length);
                        writer.Write(Node.walletStorage.publicKey);

                        // Send listening port
                        writer.Write(Config.serverPort);

                        // Send timestamp
                        long timestamp = Core.getCurrentTimestamp();
                        writer.Write(timestamp);

                        // send signature
                        byte[] signature = CryptoManager.lib.getSignature(Encoding.UTF8.GetBytes(CoreConfig.ixianChecksumLockString + "-" + Config.device_id + "-" + timestamp + "-" + publicHostname), Node.walletStorage.privateKey);
                        writer.Write(signature.Length);
                        writer.Write(signature);


                        if (sendHelloData)
                        {
                            ulong lastBlock = Node.blockChain.getLastBlockNum();
                            Block block = Node.blockChain.getBlock(lastBlock);
                            if (block == null)
                            {
                                Logging.warn("Clients are connecting, but we have no blocks yet to send them!");
                                return;
                            }


                            lastBlock = block.blockNum;
                            writer.Write(lastBlock);

                            writer.Write(block.blockChecksum.Length);
                            writer.Write(block.blockChecksum);

                            writer.Write(block.walletStateChecksum.Length);
                            writer.Write(block.walletStateChecksum);

                            writer.Write(Node.blockChain.getRequiredConsensus());

                            // Write the legacy level
                            writer.Write(Legacy.getLegacyLevel());

                            endpoint.sendData(ProtocolMessageCode.helloData, m.ToArray());

                        }
                        else
                        {
                            endpoint.sendData(ProtocolMessageCode.hello, m.ToArray());
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
                                    if (processHelloMessage(endpoint, reader))
                                    {
                                        sendHelloMessage(endpoint, true);
                                        endpoint.helloReceived = true;
                                        return;
                                    }
                                }
                            }
                            break;

                        case ProtocolMessageCode.helloData:
                            using (MemoryStream m = new MemoryStream(data))
                            {
                                using (BinaryReader reader = new BinaryReader(m))
                                {
                                    if (processHelloMessage(endpoint, reader))
                                    {
                                        ulong last_block_num = reader.ReadUInt64();
                                        int bcLen = reader.ReadInt32();
                                        byte[] block_checksum = reader.ReadBytes(bcLen);
                                        int wsLen = reader.ReadInt32();
                                        byte[] walletstate_checksum = reader.ReadBytes(wsLen);
                                        int consensus = reader.ReadInt32();

                                        long myTimestamp = Core.getCurrentTimestamp();


                                        if (Node.checkCurrentBlockDeprecation(last_block_num) == false)
                                        {
                                            using (MemoryStream m2 = new MemoryStream())
                                            {
                                                using (BinaryWriter writer = new BinaryWriter(m2))
                                                {
                                                    writer.Write(string.Format("This node deprecated or will deprecate on block {0}, your block height is {1}, disconnecting.", Config.compileTimeBlockNumber + Config.deprecationBlockOffset, last_block_num));
                                                    endpoint.sendData(ProtocolMessageCode.bye, m2.ToArray());
                                                }
                                            }
                                            return;
                                        }

                                        // Check for legacy level
                                        ulong legacy_level = last_block_num;
                                        try
                                        {
                                            ulong level = reader.ReadUInt64();
                                            legacy_level = level;
                                        }
                                        catch(Exception)
                                        {
                                            legacy_level = 0;
                                        }

                                        // Check for legacy node
                                        if(Legacy.isLegacy(legacy_level))
                                        {
                                            // TODO TODO TODO TODO check this out
                                            //endpoint.setLegacy(true);
                                        }

                                        // Process the hello data
                                        Node.blockSync.onHelloDataReceived(last_block_num, block_checksum, walletstate_checksum, consensus);
                                        endpoint.helloReceived = true;
                                        NetworkClientManager.recalculateLocalTimeDifference();
                                    }
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
                                        Logging.info(String.Format("Block #{0} ({1}) found, transmitting...", block_number, Crypto.hashToString(block.blockChecksum.Take(4).ToArray())));
                                        // Send the block
                                        endpoint.sendData(ProtocolMessageCode.blockData, block.getBytes());

                                        // if somebody requested last block from the chain, re-broadcast the localNewBlock as well
                                        if (Node.blockChain.getLastBlockNum() == block_number)
                                        {
                                            Block localNewBlock = Node.blockProcessor.getLocalBlock();
                                            if (localNewBlock != null)
                                            {
                                                endpoint.sendData(ProtocolMessageCode.newBlock, localNewBlock.getBytes());
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
                                        int addrLen = reader.ReadInt32();
                                        byte[] address = reader.ReadBytes(addrLen);

                                        // Retrieve the latest balance
                                        IxiNumber balance = Node.walletState.getWalletBalance(address);

                                        // Return the balance for the matching address
                                        using (MemoryStream mw = new MemoryStream())
                                        {
                                            using (BinaryWriter writerw = new BinaryWriter(mw))
                                            {
                                                // Send the address
                                                writerw.Write(address.Length);
                                                writerw.Write(address);
                                                // Send the balance
                                                writerw.Write(balance.ToString());
                                                // Send the block height for this balance
                                                writerw.Write(Node.blockChain.getLastBlockNum());
 
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

                                        Logging.info(String.Format("Sending transaction {0} - {1} - {2}.", transaction.id, Crypto.hashToString(transaction.checksum), transaction.amount));

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
                                        endpoint.stop();

                                        bool byeV1 = false;
                                        try
                                        {
                                            int byeCode = reader.ReadInt32();
                                            string byeMessage = reader.ReadString();
                                            string byeData = reader.ReadString();

                                            byeV1 = true;

                                            Logging.warn(string.Format("Disconnected with message: {0} {1}", byeMessage, byeData));

                                            if (byeCode == 600)
                                            {
                                                if (Node.validateIPv4(byeData))
                                                {
                                                    if (NetworkClientManager.getConnectedClients().Length == 1)
                                                    {
                                                        Config.publicServerIP = byeData;
                                                        Logging.info("Changed internal IP Address to " + byeData + ", reconnecting");
                                                    }
                                                }
                                            }else if(byeCode == 601)
                                            {
                                                Logging.error("This node must be connectable from the internet, to connect to the network.");
                                                Logging.error("Please setup uPNP and/or port forwarding on your router for port "+Config.serverPort+".");
                                            }

                                        }
                                        catch (Exception e)
                                        {

                                        }
                                        if(byeV1)
                                        {
                                            return;
                                        }

                                        reader.BaseStream.Seek(0, SeekOrigin.Begin);

                                        // Retrieve the message
                                        string message = reader.ReadString();

                                        // Convert to Worker node if possible
                                        if(message.StartsWith("Insufficient funds"))
                                        {
                                            Logging.warn(string.Format("Disconnected with message: {0}", message));

                                            if (Config.disableMiner == false)
                                            {
                                                Logging.info("Reconnecting in Worker mode.");
                                                Node.convertToWorkerNode();
                                            }
                                            return;
                                        }

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
                                        int walletstate_version = Node.walletState.version;

                                        // Return the current walletstate block and walletstate count
                                        writer.Write(walletstate_version);
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
                                        int walletstate_version = 0;
                                        try
                                        {
                                            walletstate_version = reader.ReadInt32();
                                            walletstate_block = reader.ReadUInt64();
                                            walletstate_count = reader.ReadInt64();
                                        }
                                        catch (Exception e)
                                        {
                                            Logging.warn(String.Format("Error while receiving the WalletState header: {0}.", e.Message));
                                            return;
                                        }
                                        Node.blockSync.onWalletStateHeader(walletstate_version, walletstate_block, walletstate_count);
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
                                        if(num_wallets > CoreConfig.walletStateChunkSplit)
                                        {
                                            Logging.error(String.Format("Received {0} wallets in a chunk. ( > {1}).",
                                                num_wallets, CoreConfig.walletStateChunkSplit));
                                            return;
                                        }
                                        Wallet[] wallets = new Wallet[num_wallets];
                                        for(int i =0;i<num_wallets;i++)
                                        {
                                            int w_idLen = reader.ReadInt32();
                                            byte[] w_id = reader.ReadBytes(w_idLen);

                                            IxiNumber w_balance = new IxiNumber(reader.ReadString());

                                            wallets[i] = new Wallet(w_id, w_balance);

                                            int w_dataLen = reader.ReadInt32();
                                            if (w_dataLen > 0)
                                            {
                                                byte[] w_data = reader.ReadBytes(w_dataLen);
                                                wallets[i].data = w_data;
                                            }

                                            int w_publickeyLen = reader.ReadInt32();
                                            if (w_publickeyLen > 0)
                                            {
                                                byte[] w_publickey = reader.ReadBytes(w_publickeyLen);
                                                wallets[i].publicKey = w_publickey;
                                            }

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
                                byte[] ba = CoreProtocolMessage.prepareProtocolMessage(ProtocolMessageCode.presenceList, pdata);
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
                                bool updated = PresenceList.receiveKeepAlive(data);
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
                                        int walletLen = reader.ReadInt32();
                                        byte[] wallet = reader.ReadBytes(walletLen);
                                        // TODO re-verify this
                                        Presence p = PresenceList.presences.Find(x => x.wallet.SequenceEqual(wallet));
                                        if (p != null)
                                        {
                                            endpoint.sendData(ProtocolMessageCode.updatePresence, p.getBytes());
                                        }
                                        else
                                        {
                                            // TODO blacklisting point
                                            Logging.warn(string.Format("Node has requested presence information about {0} that is not in our PL.", Base58Check.Base58CheckEncoding.EncodePlain(wallet)));
                                        }
                                    }
                                }
                            }
                            break;

                        case ProtocolMessageCode.getBlockTransactions:
                            {
                                handleGetBlockTransactions(data, endpoint);                              
                            }
                            break;

                        case ProtocolMessageCode.getUnappliedTransactions:
                            {
                                handleGetUnappliedTransactions(data, endpoint);
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
                                        int totalTxCount = 0;
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
                                            totalTxCount++;
                                            if (tx.type == (int)Transaction.Type.StakingReward && !Node.blockSync.synchronizing)
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
                                        Logging.info(string.Format("Processed {0}/{1} txs in {2}ms", processedTxCount, totalTxCount, elapsed.TotalMilliseconds));
                                    }
                                }
                            }
                            break;

                        case ProtocolMessageCode.ping:
                            {
                                endpoint.sendData(ProtocolMessageCode.pong, new byte[1]);
                            }
                            break;

                        case ProtocolMessageCode.pong:
                            {
                                // do nothing
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
            private static bool isAuthoritativeNode(RemoteEndpoint endpoint)
            {
                // Disabled for dev purposes.
                // TODO: re-enable
                return true;
            }

        }
    }
}