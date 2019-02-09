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
            private static void handleGetBlockTransactions(ulong blockNum, bool requestAllTransactions, RemoteEndpoint endpoint)
            {
                //Logging.info(String.Format("Received request for transactions in block {0}.", blockNum));

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
                            int txs_in_chunk = 0;
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
                                Transaction tx = TransactionPool.getTransaction(txIdArr[tx_index], blockNum, true);
                                if (tx != null)
                                {
                                    byte[] txBytes = tx.getBytes();

                                    writer.Write(txBytes.Length);
                                    writer.Write(txBytes);
                                    txs_in_chunk++;
                                }
                            }

                            if (txs_in_chunk > 0)
                            {
                                // Send a chunk
                                endpoint.sendData(ProtocolMessageCode.transactionsChunk, mOut.ToArray());
                            }
                        }
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

            // Adds event subscriptions for the provided endpoint
            private static void handleAttachEvent(byte[] data, RemoteEndpoint endpoint)
            {
                if (data == null)
                {
                    Logging.warn(string.Format("Invalid protocol message event data"));
                    return;
                }

                using (MemoryStream m = new MemoryStream(data))
                {
                    using (BinaryReader reader = new BinaryReader(m))
                    {
                        int type = reader.ReadInt32();
                        int addrLen = reader.ReadInt32();
                        byte[] addresses = reader.ReadBytes(addrLen);

                        endpoint.attachEvent(type, addresses);
                    }
                }
            }

            // Removes event subscriptions for the provided endpoint
            private static void handleDetachEvent(byte[] data, RemoteEndpoint endpoint)
            {
                if (data == null)
                {
                    Logging.warn(string.Format("Invalid protocol message event data"));
                    return;
                }

                using (MemoryStream m = new MemoryStream(data))
                {
                    using (BinaryReader reader = new BinaryReader(m))
                    {
                        int type = reader.ReadInt32();
                        int addrLen = reader.ReadInt32();
                        byte[] addresses = reader.ReadBytes(addrLen);

                        endpoint.detachEvent(type, addresses);
                    }
                }
            }

            public static bool broadcastNewBlockSignature(byte[] signature_data, RemoteEndpoint skipEndpoint = null, RemoteEndpoint endpoint = null)
            {
                if (endpoint != null)
                {
                    if (endpoint.isConnected())
                    {
                        endpoint.sendData(ProtocolMessageCode.newBlockSignature, signature_data);
                        return true;
                    }
                    return false;
                }
                else
                {
                    return broadcastProtocolMessage(new char[] { 'M', 'H' }, ProtocolMessageCode.newBlockSignature, signature_data, skipEndpoint);
                }
            }


            // Removes event subscriptions for the provided endpoint
            private static void handleNewBlockSignature(byte[] data, RemoteEndpoint endpoint)
            {
                if (data == null)
                {
                    Logging.warn(string.Format("Invalid protocol message signature data"));
                    return;
                }

                using (MemoryStream m = new MemoryStream(data))
                {
                    using (BinaryReader reader = new BinaryReader(m))
                    {
                        ulong block_num = reader.ReadUInt64();

                        int checksum_len = reader.ReadInt32();
                        byte[] checksum = reader.ReadBytes(checksum_len);

                        int sig_len = reader.ReadInt32();
                        byte[] sig = reader.ReadBytes(sig_len);

                        int sig_addr_len = reader.ReadInt32();
                        byte[] sig_addr = reader.ReadBytes(sig_addr_len);

                        ulong last_bh = Node.getLastBlockHeight();

                        lock (Node.blockProcessor.localBlockLock)
                        {
                            if (last_bh < block_num || (last_bh + 1 == block_num && Node.blockProcessor.getLocalBlock() == null))
                            {
                                // future block, request the next block
                                broadcastGetBlock(last_bh + 1, null, endpoint);
                                return;
                            }
                        }

                        if(Node.blockProcessor.addSignatureToBlock(block_num, checksum, sig, sig_addr))
                        {
                            broadcastNewBlockSignature(data, endpoint);
                        }else
                        {
                            Logging.warn("Received an invalid signature for block {0}", block_num);
                        }
                    }
                }
            }

            // Handle the getBlockTransactions message
            // This is called from NetworkProtocol
            private static void handleGetBlockSignatures(ulong blockNum, byte[] checksum, RemoteEndpoint endpoint)
            {
                //Logging.info(String.Format("Received request for signatures in block {0}.", blockNum));

                // Get the requested block and corresponding signatures
                Block b = Node.blockChain.getBlock(blockNum, Config.storeFullHistory);

                if(b == null || !b.blockChecksum.SequenceEqual(checksum))
                {
                    return;
                }

                int sig_count = b.signatures.Count();

                int num_chunks = sig_count / CoreConfig.maximumTransactionsPerChunk + 1;
                // Go through each chunk
                for (int i = 0; i < num_chunks; i++)
                {
                    using (MemoryStream mOut = new MemoryStream())
                    {
                        using (BinaryWriter writer = new BinaryWriter(mOut))
                        {
                            int sigs_in_chunk = 0;
                            // Generate a chunk of transactions
                            for (int j = 0; j < CoreConfig.maximumTransactionsPerChunk; j++)
                            {
                                int sig_index = i * CoreConfig.maximumTransactionsPerChunk + j;
                                if (sig_index > sig_count - 1)
                                    break;

                                byte[][] sig = b.signatures[i];
                                if (sig != null)
                                {
                                    // sig
                                    writer.Write(sig[0].Length);
                                    writer.Write(sig[0]);

                                    // address/pubkey
                                    writer.Write(sig[1].Length);
                                    writer.Write(sig[1]);

                                    sigs_in_chunk++;
                                }
                            }

                            if (sigs_in_chunk > 0)
                            {
                                // Send a chunk
                                endpoint.sendData(ProtocolMessageCode.blockSignatureChunk, mOut.ToArray());
                            }
                        }
                    }
                }
            }

            // Requests block with specified block height from the network, include_transactions value can be 0 - don't include transactions, 1 - include all but staking transactions or 2 - include all, including staking transactions
            public static bool broadcastGetBlock(ulong block_num, RemoteEndpoint skipEndpoint = null, RemoteEndpoint endpoint = null, byte include_transactions = 0)
            {
                using (MemoryStream mw = new MemoryStream())
                {
                    using (BinaryWriter writerw = new BinaryWriter(mw))
                    {
                        writerw.Write(block_num);
                        writerw.Write(include_transactions);

                        if (endpoint != null)
                        {
                            if (endpoint.isConnected())
                            {
                                endpoint.sendData(ProtocolMessageCode.getBlock, mw.ToArray());
                                return true;
                            }
                        }
                        return broadcastProtocolMessageToSingleRandomNode(new char[] { 'M' }, ProtocolMessageCode.getBlock, mw.ToArray(), block_num, skipEndpoint);
                    }
                }
            }

            public static bool broadcastNewBlock(Block b, RemoteEndpoint skipEndpoint = null, RemoteEndpoint endpoint = null)
            {
                if (endpoint != null)
                {
                    if (endpoint.isConnected())
                    {
                        endpoint.sendData(ProtocolMessageCode.newBlock, b.getBytes());
                        return true;
                    }
                    return false;
                }
                else
                {
                    return broadcastProtocolMessage(new char[] { 'M', 'H' }, ProtocolMessageCode.newBlock, b.getBytes(), skipEndpoint);
                }
            }

            public static bool broadcastGetTransaction(string txid, ulong block_num, RemoteEndpoint endpoint = null)
            {
                using (MemoryStream mw = new MemoryStream())
                {
                    using (BinaryWriter writerw = new BinaryWriter(mw))
                    {
                        writerw.Write(txid);
                        writerw.Write(block_num);

                        if (endpoint != null)
                        {
                            if (endpoint.isConnected())
                            {
                                endpoint.sendData(ProtocolMessageCode.getTransaction, mw.ToArray());
                                return true;
                            }
                        }
                        // TODO TODO TODO TODO TODO determine if historic transaction and send to 'H' instead of 'M'
                        return broadcastProtocolMessageToSingleRandomNode(new char[] { 'M' }, ProtocolMessageCode.getTransaction, mw.ToArray(), block_num);
                    }
                }
            }

            public static bool broadcastGetBlockTransactions(ulong blockNum, bool requestAllTransactions, RemoteEndpoint endpoint)
            {
                using (MemoryStream mw = new MemoryStream())
                {
                    using (BinaryWriter writerw = new BinaryWriter(mw))
                    {
                        writerw.Write(blockNum);
                        writerw.Write(requestAllTransactions);

                        if (endpoint != null)
                        {
                            if (endpoint.isConnected())
                            {
                                endpoint.sendData(ProtocolMessageCode.getBlockTransactions, mw.ToArray());
                                return true;
                            }
                        }
                        return broadcastProtocolMessageToSingleRandomNode(new char[] { 'M' }, ProtocolMessageCode.getBlockTransactions, mw.ToArray(), blockNum);
                    }
                }
            }

            // Broadcasts protocol message to a single random node with block height higher than the one specified with parameter block_num
            public static bool broadcastProtocolMessageToSingleRandomNode(char[] types, ProtocolMessageCode code, byte[] data, ulong block_num, RemoteEndpoint skipEndpoint = null)
            {
                if (data == null)
                {
                    Logging.warn(string.Format("Invalid protocol message data for {0}", code));
                    return false;
                }

                lock (NetworkClientManager.networkClients)
                {
                    lock (NetworkServer.connectedClients)
                    {
                        int serverCount = 0;
                        int clientCount = 0;
                        List<NetworkClient> servers = null;
                        List<RemoteEndpoint> clients = null;

                        if (types == null)
                        {
                            servers = NetworkClientManager.networkClients.FindAll(x => x.blockHeight > block_num);
                            clients = NetworkServer.connectedClients.FindAll(x => x.blockHeight > block_num);

                            serverCount = servers.Count();
                            clientCount = clients.Count();

                            if (serverCount == 0 && clientCount == 0)
                            {
                                servers = NetworkClientManager.networkClients.FindAll(x => x.blockHeight == block_num);
                                clients = NetworkServer.connectedClients.FindAll(x => x.blockHeight == block_num);
                            }
                        }else
                        {
                            servers = NetworkClientManager.networkClients.FindAll(x => x.blockHeight > block_num && x.presenceAddress != null && types.Contains(x.presenceAddress.type));
                            clients = NetworkServer.connectedClients.FindAll(x => x.blockHeight > block_num && x.presenceAddress != null && types.Contains(x.presenceAddress.type));

                            serverCount = servers.Count();
                            clientCount = clients.Count();

                            if (serverCount == 0 && clientCount == 0)
                            {
                                servers = NetworkClientManager.networkClients.FindAll(x => x.blockHeight == block_num && x.presenceAddress != null && types.Contains(x.presenceAddress.type));
                                clients = NetworkServer.connectedClients.FindAll(x => x.blockHeight == block_num && x.presenceAddress != null && types.Contains(x.presenceAddress.type));
                            }
                        }

                        serverCount = servers.Count();
                        clientCount = clients.Count();

                        if(serverCount == 0 && clientCount == 0)
                        {
                            return false;
                        }

                        Random r = new Random();
                        int rIdx = r.Next(serverCount + clientCount);

                        RemoteEndpoint re = null;

                        if (rIdx < serverCount)
                        {
                            re = servers[rIdx];
                        }
                        else
                        {
                            re = clients[rIdx - serverCount];
                        }

                        if (re == skipEndpoint && serverCount + clientCount > 1)
                        {
                            if (rIdx + 1 < serverCount)
                            {
                                re = servers[rIdx + 1];
                            }
                            else if(rIdx + 1 < serverCount + clientCount)
                            {
                                re = clients[rIdx + 1 - serverCount];
                            }else if(serverCount > 0)
                            {
                                re = servers[0];
                            }else if(clientCount > 0)
                            {
                                re = clients[0];
                            }
                        }

                        if (re != null && re.isConnected())
                        {
                            re.sendData(code, data);
                            return true;
                        }
                        return false;
                    }
                }
            }

            // Broadcast a protocol message across clients and nodes
            // Returns true if it sent the message at least one endpoint. Returns false if the message couldn't be sent to any endpoints
            public static bool broadcastProtocolMessage(char[] types, ProtocolMessageCode code, byte[] data, RemoteEndpoint skipEndpoint = null)
            {
                if(data == null)
                {
                    Logging.warn(string.Format("Invalid protocol message data for {0}", code));
                    return false;
                }

                bool c_result = NetworkClientManager.broadcastData(types, code, data, skipEndpoint);
                bool s_result = NetworkServer.broadcastData(types, code, data, skipEndpoint);

                if (!c_result && !s_result)
                    return false;

                return true;
            }

            // Broadcast an event-specific protocol message across clients and nodes
            // Returns true if it sent the message at least one endpoint. Returns false if the message couldn't be sent to any endpoints
            public static bool broadcastEventBasedMessage(ProtocolMessageCode code, byte[] data, byte[] address, RemoteEndpoint skipEndpoint = null)
            {
                // Broadcast the event to all non-C nodes
                bool b_result = broadcastProtocolMessage(new char[] { 'M', 'R', 'H', 'W' }, code, data, skipEndpoint);

                // Now send it to subscribed C nodes
                bool f_result = NetworkServer.broadcastEventData(code, data, address, skipEndpoint);

                if (!b_result && !f_result)
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




                                data_checksum = reader.ReadBytes(32); // sha512qu, 32 bytes
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
                            byte[] local_checksum = Crypto.sha512sqTrunc(data, 0, 0, 32);

                            // Verify the checksum before proceeding
                            if (local_checksum.SequenceEqual(data_checksum) == false)
                            {
                                // TODO TODO TODO TODO TODO remove nested if after network upgrade
                                local_checksum = Crypto.sha512quTrunc(data);
                                if (local_checksum.SequenceEqual(data_checksum) == false)
                                {
                                    Logging.error("Dropped message (invalid checksum)");
                                    continue;
                                }
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
                    Logging.warn("Node {0} was not reachable on the advertised address.", hostname);
                    CoreProtocolMessage.sendBye(endpoint, 601, "External " + hostname + " not reachable!", "");
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
                        CoreProtocolMessage.sendBye(endpoint, 600, "Verify signature failed in hello message, likely an incorrect IP was specified. Detected IP:", endpoint.address);
                        Logging.warn(string.Format("Connected node used an incorrect signature in hello message, likely an incorrect IP was specified. Detected IP: {0}", endpoint.address));
                        return false;
                    }

                    // if we're a client update the network time difference
                    if(endpoint.GetType() == typeof(NetworkClient))
                    {
                        long timeDiff = endpoint.calculateTimeDifference();

                        // amortize +- 2 seconds
                        if (timeDiff >= -2 && timeDiff <= 2)
                        {
                            timeDiff = 0;
                        }

                        ((NetworkClient)endpoint).timeDifference = timeDiff;
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

                    if (endpoint.GetType() != typeof(NetworkClient))
                    {
                        if (node_type == 'M' || node_type == 'H' || node_type == 'R')
                        {
                            if (!checkNodeConnectivity(endpoint))
                            {
                                return false;
                            }
                        }
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
                        byte[] address = Node.walletStorage.getPrimaryAddress();
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
                        writer.Write(Node.walletStorage.getPrimaryPublicKey().Length);
                        writer.Write(Node.walletStorage.getPrimaryPublicKey());

                        // Send listening port
                        writer.Write(Config.serverPort);

                        // Send timestamp
                        long timestamp = Core.getCurrentTimestamp();
                        writer.Write(timestamp);

                        // send signature
                        byte[] signature = CryptoManager.lib.getSignature(Encoding.UTF8.GetBytes(CoreConfig.ixianChecksumLockString + "-" + Config.device_id + "-" + timestamp + "-" + publicHostname), Node.walletStorage.getPrimaryPrivateKey());
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

                            writer.Write(block.version);

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

                                        endpoint.blockHeight = last_block_num;

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

                                        int block_version = 1;
                                        try
                                        {
                                            block_version = reader.ReadInt32();
                                        }
                                        catch (Exception)
                                        {
                                            block_version = 1;
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
                                        Node.blockSync.onHelloDataReceived(last_block_num, block_checksum, walletstate_checksum, consensus, 0, true);
                                        endpoint.helloReceived = true;
                                        NetworkClientManager.recalculateLocalTimeDifference();
                                    }
                                }
                            }
                            break;

                        case ProtocolMessageCode.getBlock:
                            {
                                if (Node.blockSync.synchronizing)
                                {
                                    return;
                                }
                                using (MemoryStream m = new MemoryStream(data))
                                {
                                    using (BinaryReader reader = new BinaryReader(m))
                                    {
                                        ulong block_number = reader.ReadUInt64();
                                        byte include_transactions = reader.ReadByte();

                                        //Logging.info(String.Format("Block #{0} has been requested.", block_number));

                                        if (block_number > Node.getLastBlockHeight())
                                        {
                                            return;
                                        }

                                        Block block = Node.blockChain.getBlock(block_number, Config.storeFullHistory);
                                        if (block == null)
                                        {
                                            Logging.warn(String.Format("Unable to find block #{0} in the chain!", block_number));
                                            return;
                                        }
                                        //Logging.info(String.Format("Block #{0} ({1}) found, transmitting...", block_number, Crypto.hashToString(block.blockChecksum.Take(4).ToArray())));
                                        // Send the block

                                        if(include_transactions == 1)
                                        {
                                            handleGetBlockTransactions(block_number, false, endpoint);
                                        }
                                        else if(include_transactions == 2)
                                        {
                                            handleGetBlockTransactions(block_number, true, endpoint);
                                        }

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
                                if (Node.blockSync.synchronizing)
                                {
                                    return;
                                }
                                using (MemoryStream m = new MemoryStream(data))
                                {
                                    using (BinaryReader reader = new BinaryReader(m))
                                    {
                                        // Retrieve the transaction id
                                        string txid = reader.ReadString();
                                        ulong block_num = reader.ReadUInt64();

                                        Transaction transaction = null;

                                        // Check for a transaction corresponding to this id
                                        if(block_num == 0)
                                        {
                                            transaction = TransactionPool.getTransaction(txid, 0, false);
                                        }
                                        else
                                        {
                                            transaction = TransactionPool.getTransaction(txid, block_num, true);
                                        }

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

                                            if (byeCode != 200)
                                            {
                                                Logging.warn(string.Format("Disconnected with message: {0} {1}", byeMessage, byeData));
                                            }

                                            if (byeCode == 600)
                                            {
                                                if (Node.validateIPv4(byeData))
                                                {
                                                    if (NetworkClientManager.getConnectedClients().Length < 2)
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
                                        catch (Exception)
                                        {

                                        }
                                        if(byeV1)
                                        {
                                            return;
                                        }

                                        reader.BaseStream.Seek(0, SeekOrigin.Begin);

                                        // Retrieve the message
                                        string message = reader.ReadString();

                                        if (message.Length > 0)
                                            Logging.info(string.Format("Disconnected with message: {0}", message));
                                        else
                                            Logging.info("Disconnected");

                                        // Convert to Worker node if possible
                                        if (message.StartsWith("Insufficient funds"))
                                        {

                                            if (Config.disableMiner == false)
                                            {
                                                Logging.info("Reconnecting in Worker mode.");
                                                Node.convertToWorkerNode();
                                            }
                                            return;
                                        }

                                        
                                        
                                    }
                                }
                            }
                            break;

                        case ProtocolMessageCode.newBlock:
                            {
                                Block block = new Block(data);
                                if (endpoint.blockHeight < block.blockNum)
                                {
                                    endpoint.blockHeight = block.blockNum;
                                }

                                //Logging.info(String.Format("Network: Received block #{0} from {1}.", block.blockNum, socket.RemoteEndPoint.ToString()));
                                Node.blockSync.onBlockReceived(block, endpoint);
                                Node.blockProcessor.onBlockReceived(block, endpoint);
                            }
                            break;

                        case ProtocolMessageCode.blockData:
                            {
                                Block block = new Block(data);
                                if (endpoint.blockHeight < block.blockNum)
                                {
                                    endpoint.blockHeight = block.blockNum;
                                }

                                Node.blockSync.onBlockReceived(block, endpoint);
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
                                byte[] address = null;
                                bool updated = PresenceList.receiveKeepAlive(data, out address);

                                // If a presence entry was updated, broadcast this message again
                                if (updated)
                                {
                                    broadcastEventBasedMessage(ProtocolMessageCode.keepAlivePresence, data, address, endpoint);
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
                                        lock (PresenceList.presences)
                                        {
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
                            }
                            break;

                        case ProtocolMessageCode.getBlockTransactions:
                            {
                                using (MemoryStream m = new MemoryStream(data))
                                {
                                    using (BinaryReader reader = new BinaryReader(m))
                                    {
                                        ulong blockNum = reader.ReadUInt64();
                                        bool requestAllTransactions = reader.ReadBoolean();

                                        handleGetBlockTransactions(blockNum, requestAllTransactions, endpoint);
                                    }
                                }
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
                                            if (TransactionPool.hasTransaction(tx.id))
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

                        case ProtocolMessageCode.attachEvent:
                            {
                                handleAttachEvent(data, endpoint);
                            }
                            break;

                        case ProtocolMessageCode.detachEvent:
                            {
                                handleDetachEvent(data, endpoint);
                            }
                            break;

                        case ProtocolMessageCode.newBlockSignature:
                            {
                                handleNewBlockSignature(data, endpoint);
                            }
                            break;

                        case ProtocolMessageCode.getBlockSignatures:
                            {
                                using (MemoryStream m = new MemoryStream(data))
                                {
                                    using (BinaryReader reader = new BinaryReader(m))
                                    {
                                        ulong block_num = reader.ReadUInt64();

                                        int checksum_len = reader.ReadInt32();
                                        byte[] checksum = reader.ReadBytes(checksum_len);

                                        handleGetBlockSignatures(block_num, checksum, endpoint);
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