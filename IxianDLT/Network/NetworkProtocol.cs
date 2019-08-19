using DLT.Meta;
using IXICore;
using IXICore.Meta;
using IXICore.Network;
using IXICore.Utils;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;

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

                    bool haveLock = false;
                    try
                    {
                        Monitor.TryEnter(Node.blockProcessor.localBlockLock, 1000, ref haveLock);
                        if (!haveLock)
                        {
                            throw new TimeoutException();
                        }

                        Block tmp = Node.blockProcessor.getLocalBlock();
                        if (tmp != null && tmp.blockNum == blockNum)
                        {
                            b = tmp;
                            txIdArr = new List<string>(tmp.transactions);
                        }
                    }
                    finally
                    {
                        if (haveLock)
                        {
                            Monitor.Exit(Node.blockProcessor.localBlockLock);
                        }
                    }
                }

                if (txIdArr == null)
                    return;

                int tx_count = txIdArr.Count();

                if (tx_count == 0)
                    return;

                int num_chunks = tx_count / ConsensusConfig.maximumTransactionsPerChunk + 1;
                // Go through each chunk
                for (int i = 0; i < num_chunks; i++)
                {
                    using (MemoryStream mOut = new MemoryStream(4096))
                    {
                        using (BinaryWriter writer = new BinaryWriter(mOut))
                        {
                            int txs_in_chunk = 0;
                            // Generate a chunk of transactions
                            for (int j = 0; j < ConsensusConfig.maximumTransactionsPerChunk; j++)
                            {
                                int tx_index = i * ConsensusConfig.maximumTransactionsPerChunk + j;
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

#if TRACE_MEMSTREAM_SIZES
                            Logging.info(String.Format("NetworkProtocol::handleGetBlockTransactions: {0}", mOut.Length));
#endif
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

                int num_chunks = tx_count / ConsensusConfig.maximumTransactionsPerChunk + 1;

                // Go through each chunk
                for (int i = 0; i < num_chunks; i++)
                {
                    using (MemoryStream mOut = new MemoryStream())
                    {
                        using (BinaryWriter writer = new BinaryWriter(mOut))
                        {
                            // Generate a chunk of transactions
                            for (int j = 0; j < ConsensusConfig.maximumTransactionsPerChunk; j++)
                            {
                                int tx_index = i * ConsensusConfig.maximumTransactionsPerChunk + j;
                                if (tx_index > tx_count - 1)
                                    break;

                                byte[] txBytes = txIdArr[tx_index].getBytes();
                                writer.Write(txBytes.Length);
                                writer.Write(txBytes);
                            }

                            // Send a chunk
#if TRACE_MEMSTREAM_SIZES
                        Logging.info(String.Format("NetworkProtocol::handleGetUnappliedTransactions: {0}", mOut.Length));
#endif
                            endpoint.sendData(ProtocolMessageCode.transactionsChunk, mOut.ToArray());
                        }
                    }
                }
            }

            // Broadcast the current block height. Called after accepting a new block once the node is fully synced
            // Returns false when no RemoteEndpoints found to send the message to
            public static bool broadcastBlockHeight(ulong blockNum, byte[] checksum)
            {
                using (MemoryStream mw = new MemoryStream())
                {
                    using (BinaryWriter writerw = new BinaryWriter(mw))
                    {
                        Block tmp_block = IxianHandler.getLastBlock();

                        // Send the block height
                        writerw.Write(blockNum);

                        // Send the block checksum for this balance
                        writerw.Write(checksum.Length);
                        writerw.Write(checksum);

                        return CoreProtocolMessage.broadcastProtocolMessage(new char[] { 'C' }, ProtocolMessageCode.blockHeight, mw.ToArray(), null, null);
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
                    return CoreProtocolMessage.broadcastProtocolMessage(new char[] { 'M', 'H' }, ProtocolMessageCode.newBlockSignature, signature_data, null, skipEndpoint);
                }
            }


            public static bool broadcastNewBlockSignature(ulong block_num, byte[] block_checksum, byte[] signature, byte[] signer_address, RemoteEndpoint skipEndpoint = null, RemoteEndpoint endpoint = null)
            {
                byte[] signature_data = null;

                using (MemoryStream m = new MemoryStream(1152))
                {
                    using (BinaryWriter writer = new BinaryWriter(m))
                    {
                        writer.Write(block_num);

                        writer.Write(block_checksum.Length);
                        writer.Write(block_checksum);

                        writer.Write(signature.Length);
                        writer.Write(signature);

                        writer.Write(signer_address.Length);
                        writer.Write(signer_address);
#if TRACE_MEMSTREAM_SIZES
                        Logging.info(String.Format("NetworkProtocol::broadcastNewBlockSignature: {0}", m.Length));
#endif

                        signature_data = m.ToArray();
                    }
                }
                if (signature_data != null)
                {
                    return broadcastNewBlockSignature(signature_data, skipEndpoint, endpoint);
                }

                return false;
            }


            // Removes event subscriptions for the provided endpoint
            private static void handleNewBlockSignature(byte[] data, RemoteEndpoint endpoint)
            {
                if(Node.blockSync.synchronizing)
                {
                    return;
                }

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

                        ulong last_bh = IxianHandler.getLastBlockHeight();

                        lock (Node.blockProcessor.localBlockLock)
                        {
                            if (last_bh + 1 < block_num || (last_bh + 1 == block_num && Node.blockProcessor.getLocalBlock() == null))
                            {
                                Logging.info("Received signature for block {0} which is missing", block_num);
                                // future block, request the next block
                                broadcastGetBlock(last_bh + 1, null, endpoint);
                                return;
                            }
                        }

                        if(PresenceList.getPresenceByAddress(sig_addr) == null)
                        {
                            Logging.info("Received signature for block {0} whose signer isn't in the PL", block_num);
                            return;
                        }

                        if (Node.blockProcessor.addSignatureToBlock(block_num, checksum, sig, sig_addr, endpoint))
                        {
                            Node.blockProcessor.acceptLocalNewBlock();
                            if (Node.isMasterNode())
                            {
                                broadcastNewBlockSignature(data, endpoint);
                            }
                        }else
                        {
                            // discard - it might have already been applied
                        }
                    }
                }
            }

            public static void broadcastBlockSignatures(ulong block_num, byte[] block_checksum, List<byte[][]> signatures, RemoteEndpoint skip_endpoint = null, RemoteEndpoint endpoint = null)
            {
                int sig_count = signatures.Count();

                if (sig_count == 0)
                {
                    return;
                }

                using (MemoryStream mOut = new MemoryStream())
                {
                    using (BinaryWriter writer = new BinaryWriter(mOut))
                    {
                        writer.Write(block_num);

                        writer.Write(block_checksum.Length);
                        writer.Write(block_checksum);

                        writer.Write(sig_count);

                        for (int i = 0; i < sig_count; i++)
                        {
                            byte[][] sig = signatures[i];
                            if (sig != null)
                            {
                                // sig
                                writer.Write(sig[0].Length);
                                writer.Write(sig[0]);

                                // address/pubkey
                                writer.Write(sig[1].Length);
                                writer.Write(sig[1]);
                            }
                        }
#if TRACE_MEMSTREAM_SIZES
                        Logging.info(String.Format("NetworkProtocol::broadcastBlockSignatures: {0}", mOut.Length));
#endif

                        // Send a chunk
                        if (endpoint != null)
                        {
                            if (endpoint.isConnected())
                            {
                                endpoint.sendData(ProtocolMessageCode.blockSignatures, mOut.ToArray());
                                return;
                            }
                            return;
                        }
                        else
                        {
                            CoreProtocolMessage.broadcastProtocolMessage(new char[] { 'M', 'H' }, ProtocolMessageCode.blockSignatures, mOut.ToArray(), BitConverter.GetBytes(block_num), skip_endpoint);
                        }
                    }
                }
            }

            private static void handleGetRandomPresences(byte[] data, RemoteEndpoint endpoint)
            {
                if (!endpoint.isConnected())
                {
                    return;
                }

                using (MemoryStream m = new MemoryStream(data))
                {
                    using (BinaryReader reader = new BinaryReader(m))
                    {
                        char type = reader.ReadChar();

                        List<Presence> presences = PresenceList.getPresencesByType(type);
                        int presence_count = presences.Count();
                        if (presence_count > 10)
                        {
                            Random rnd = new Random();
                            presences = presences.Skip(rnd.Next(presence_count - 10)).Take(10).ToList();
                        }

                        foreach (Presence presence in presences)
                        {
                            byte[][] presence_chunks = presence.getByteChunks();
                            foreach (byte[] presence_chunk in presence_chunks)
                            {
                                endpoint.sendData(ProtocolMessageCode.updatePresence, presence_chunk, null);
                            }
                        }
                    }
                }
            }

            public static void broadcastBlockSignatures(Block b, RemoteEndpoint skip_endpoint = null, RemoteEndpoint endpoint = null)
            {
                if (b.frozenSignatures != null)
                {
                    broadcastBlockSignatures(b.blockNum, b.blockChecksum, b.frozenSignatures, skip_endpoint, endpoint);
                }else
                {
                    broadcastBlockSignatures(b.blockNum, b.blockChecksum, b.signatures, skip_endpoint, endpoint);
                }
            }

            private static void handleGetBlockSignatures(ulong blockNum, byte[] checksum, RemoteEndpoint endpoint)
            {
                //Logging.info(String.Format("Received request for signatures in block {0}.", blockNum));

                // Get the requested block and corresponding signatures
                Block b = Node.blockChain.getBlock(blockNum, Config.storeFullHistory);

                if(b == null || !b.blockChecksum.SequenceEqual(checksum))
                {
                    // likely forked
                    return;
                }

                broadcastBlockSignatures(b, null, endpoint);
            }

            
            private static void handleSigfreezedBlockSignatures(byte[] data, RemoteEndpoint endpoint)
            {
                using (MemoryStream m = new MemoryStream(data))
                {
                    using (BinaryReader reader = new BinaryReader(m))
                    {
                        ulong block_num = reader.ReadUInt64();

                        int checksum_len = reader.ReadInt32();
                        byte[] checksum = reader.ReadBytes(checksum_len);

                        ulong last_block_height = IxianHandler.getLastBlockHeight();

                        Block target_block = Node.blockChain.getBlock(block_num, true);
                        if(target_block == null)
                        {
                            if (block_num == last_block_height + 1)
                            {
                                // target block missing, request the next block
                                Logging.warn("Target block {0} missing, requesting...", block_num);
                                broadcastGetBlock(block_num, null, endpoint);
                            }
                            else
                            {
                                // target block missing
                                Logging.warn("Target block {0} missing", block_num);
                            }
                            return;
                        }else if(!target_block.blockChecksum.SequenceEqual(checksum))
                        {
                            // incorrect target block
                            Logging.warn("Incorrect target block {0} - {1}, possibly forked", block_num, checksum);
                            return;
                        }


                        Block sf_block = null;
                        if (block_num + 4 == last_block_height)
                        {
                            sf_block = Node.blockProcessor.getLocalBlock();
                        }
                        else if (block_num + 4 > last_block_height)
                        {
                            Logging.warn("Sigfreezing block {0} missing", block_num + 5);
                            return;
                        }
                        else
                        {
                            // block already sigfreezed, do nothing
                            return;
                        }

                        lock (target_block)
                        {

                            if (sf_block != null)
                            {
                                if (target_block.calculateSignatureChecksum().SequenceEqual(sf_block.signatureFreezeChecksum))
                                {
                                    // we already have the correct sigfreeze
                                    return;
                                }
                            }
                            else
                            {
                                // sf_block missing
                                Logging.warn("Sigfreezing block {0} missing", block_num + 5);
                                return;
                            }


                            int sig_count = reader.ReadInt32();

                            Block dummy_block = new Block();
                            dummy_block.blockNum = block_num;
                            dummy_block.blockChecksum = checksum;

                            for (int i = 0; i < sig_count; i++)
                            {
                                int sig_len = reader.ReadInt32();
                                byte[] sig = reader.ReadBytes(sig_len);

                                int addr_len = reader.ReadInt32();
                                byte[] addr = reader.ReadBytes(addr_len);

                                dummy_block.addSignature(sig, addr);
                            }

                            Node.blockProcessor.handleSigFreezedBlock(dummy_block, endpoint);
                        }
                    }
                }
            }

            private static void handleGetNextSuperBlock(byte[] data, RemoteEndpoint endpoint)
            {
                using (MemoryStream m = new MemoryStream(data))
                {
                    using (BinaryReader reader = new BinaryReader(m))
                    {
                        ulong include_segments = reader.ReadUInt64();

                        bool full_header = reader.ReadBoolean();

                        Block block = null;

                        int checksum_len = reader.ReadInt32();
                        byte[] checksum = reader.ReadBytes(checksum_len);

                        block = Storage.getNextSuperBlock(checksum);

                        if (block != null)
                        {
                            endpoint.sendData(ProtocolMessageCode.newBlock, block.getBytes(full_header), BitConverter.GetBytes(block.blockNum));

                            if (include_segments > 0)
                            {
                                foreach (var entry in block.superBlockSegments.OrderBy(x => x.Key))
                                {
                                    SuperBlockSegment segment = entry.Value;
                                    if (segment.blockNum < include_segments)
                                    {
                                        continue;
                                    }

                                    Block segment_block = Node.blockChain.getBlock(segment.blockNum, true);

                                    endpoint.sendData(ProtocolMessageCode.newBlock, segment_block.getBytes(), BitConverter.GetBytes(segment.blockNum));
                                }
                            }
                        }
                    }
                }
            }

            private static void handleGetBlockHeaders(byte[] data, RemoteEndpoint endpoint)
            {
                using (MemoryStream m = new MemoryStream(data))
                {
                    using (BinaryReader reader = new BinaryReader(m))
                    {
                        ulong from = reader.ReadUInt64();
                        ulong to = reader.ReadUInt64();

                        ulong totalCount = to - from;
                        if (totalCount < 1)
                            return;

                        ulong lastBlockNum = Node.blockChain.getLastBlockNum();

                        if (from > lastBlockNum - 1)
                            return;

                        if (to > lastBlockNum)
                            to = lastBlockNum;

                        // Adjust total count if necessary
                        totalCount = to - from;
                        if (totalCount < 1)
                            return;

                        // Cap total block headers sent
                        if (totalCount > 100)
                            totalCount = 100;

                        using (MemoryStream mOut = new MemoryStream())
                        {
                            using (BinaryWriter writer = new BinaryWriter(mOut))
                            {
                                writer.Write(totalCount);
                                for (ulong i = 0; i < totalCount; i++)
                                {
                                    Block block = Node.blockChain.getBlock(from + i);
                                    if (block == null)
                                        continue;

                                    BlockHeader header = new BlockHeader(block);
                                    byte[] headerBytes = header.getBytes();
                                    writer.Write(headerBytes.Length);
                                    writer.Write(headerBytes);
                                }
                            }

                            // Send the blockheaders
                            if (endpoint != null)
                            {
                                if (endpoint.isConnected())
                                {
                                    endpoint.sendData(ProtocolMessageCode.blockHeaders, mOut.ToArray());
                                }
                            }
                        }
                    }
                }
            }


            // Requests block with specified block height from the network, include_segments_from_block value can be 0 for no segments, and a positive value for segments bigger than the specified value
            public static bool broadcastGetNextSuperBlock(ulong block_num, byte[] block_checksum, ulong include_segments = 0, bool full_header = false, RemoteEndpoint skipEndpoint = null, RemoteEndpoint endpoint = null)
            {
                using (MemoryStream mw = new MemoryStream())
                {
                    using (BinaryWriter writerw = new BinaryWriter(mw))
                    {
                        writerw.Write(include_segments);
                        writerw.Write(full_header);
                        writerw.Write(block_checksum.Length);
                        writerw.Write(block_checksum);
#if TRACE_MEMSTREAM_SIZES
                        Logging.info(String.Format("NetworkProtocol::broadcastGetNextSuperBlock: {0}", mw.Length));
#endif

                        if (endpoint != null)
                        {
                            if (endpoint.isConnected())
                            {
                                endpoint.sendData(ProtocolMessageCode.getNextSuperBlock, mw.ToArray());
                                return true;
                            }
                        }
                        return CoreProtocolMessage.broadcastProtocolMessageToSingleRandomNode(new char[] { 'M', 'H' }, ProtocolMessageCode.getNextSuperBlock, mw.ToArray(), block_num, skipEndpoint);
                    }
                }
            }

            // Requests block with specified block height from the network, include_transactions value can be 0 - don't include transactions, 1 - include all but staking transactions or 2 - include all, including staking transactions
            public static bool broadcastGetBlock(ulong block_num, RemoteEndpoint skipEndpoint = null, RemoteEndpoint endpoint = null, byte include_transactions = 0, bool full_header = false)
            {
                using (MemoryStream mw = new MemoryStream())
                {
                    using (BinaryWriter writerw = new BinaryWriter(mw))
                    {
                        writerw.Write(block_num);
                        writerw.Write(include_transactions);
                        writerw.Write(full_header);
#if TRACE_MEMSTREAM_SIZES
                        Logging.info(String.Format("NetworkProtocol::broadcastGetBlock: {0}", mw.Length));
#endif

                        if (endpoint != null)
                        {
                            if (endpoint.isConnected())
                            {
                                endpoint.sendData(ProtocolMessageCode.getBlock, mw.ToArray());
                                return true;
                            }
                        }
                        return CoreProtocolMessage.broadcastProtocolMessageToSingleRandomNode(new char[] { 'M', 'H' }, ProtocolMessageCode.getBlock, mw.ToArray(), block_num, skipEndpoint);
                    }
                }
            }

            public static bool broadcastNewBlock(Block b, RemoteEndpoint skipEndpoint = null, RemoteEndpoint endpoint = null)
            {
                if(!Node.isMasterNode())
                {
                    return true;
                }
                if (endpoint != null)
                {
                    if (endpoint.isConnected())
                    {
                        endpoint.sendData(ProtocolMessageCode.newBlock, b.getBytes(false), BitConverter.GetBytes(b.blockNum));
                        return true;
                    }
                    return false;
                }
                else
                {
                    return CoreProtocolMessage.broadcastProtocolMessage(new char[] { 'M', 'H' }, ProtocolMessageCode.newBlock, b.getBytes(false), BitConverter.GetBytes(b.blockNum), skipEndpoint);
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
#if TRACE_MEMSTREAM_SIZES
                        Logging.info(String.Format("NetworkProtocol::broadcastGetTransaction: {0}", mw.Length));
#endif

                        if (endpoint != null)
                        {
                            if (endpoint.isConnected())
                            {
                                endpoint.sendData(ProtocolMessageCode.getTransaction, mw.ToArray());
                                return true;
                            }
                        }
                        // TODO TODO TODO TODO TODO determine if historic transaction and send to 'H' instead of 'M'
                        return CoreProtocolMessage.broadcastProtocolMessageToSingleRandomNode(new char[] { 'M', 'H' }, ProtocolMessageCode.getTransaction, mw.ToArray(), block_num);
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
#if TRACE_MEMSTREAM_SIZES
                        Logging.info(String.Format("NetworkProtocol::broadcastGetBlockTransactions: {0}", mw.Length));
#endif

                        if (endpoint != null)
                        {
                            if (endpoint.isConnected())
                            {
                                endpoint.sendData(ProtocolMessageCode.getBlockTransactions, mw.ToArray());
                                return true;
                            }
                        }
                        return CoreProtocolMessage.broadcastProtocolMessageToSingleRandomNode(new char[] { 'M', 'H' }, ProtocolMessageCode.getBlockTransactions, mw.ToArray(), blockNum);
                    }
                }
            }

            public static bool broadcastGetBlockSignatures(ulong block_num, byte[] block_checksum, RemoteEndpoint endpoint)
            {
                using (MemoryStream mw = new MemoryStream())
                {
                    using (BinaryWriter writerw = new BinaryWriter(mw))
                    {
                        writerw.Write(block_num);

                        writerw.Write(block_checksum.Length);
                        writerw.Write(block_checksum);
#if TRACE_MEMSTREAM_SIZES
                        Logging.info(String.Format("NetworkProtocol::broadcastGetBlockSignatures: {0}", mw.Length));
#endif

                        if (endpoint != null)
                        {
                            if (endpoint.isConnected())
                            {
                                endpoint.sendData(ProtocolMessageCode.getBlockSignatures, mw.ToArray());
                                return true;
                            }
                        }
                        return CoreProtocolMessage.broadcastProtocolMessageToSingleRandomNode(new char[] { 'M', 'H' }, ProtocolMessageCode.getBlockSignatures, mw.ToArray(), block_num);
                    }
                }
            }

            public static void syncWalletStateNeighbor(string neighbor)
            {
                if(NetworkClientManager.sendToClient(neighbor, ProtocolMessageCode.syncWalletState, new byte[1], null) == false)
                {
                    NetworkServer.sendToClient(neighbor, ProtocolMessageCode.syncWalletState, new byte[1], null);
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
#if TRACE_MEMSTREAM_SIZES
                        Logging.info(String.Format("NetworkProtocol::getWalletStateChunkNeighbor: {0}", m.Length));
#endif

                        if (NetworkClientManager.sendToClient(neighbor, ProtocolMessageCode.getWalletStateChunk, m.ToArray(), null) == false)
                        {
                            if (NetworkServer.sendToClient(neighbor, ProtocolMessageCode.getWalletStateChunk, m.ToArray(), null) == false)
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
#if TRACE_MEMSTREAM_SIZES
                        Logging.info(String.Format("NetworkProtocol::sendWalletStateChunk: {0}", m.Length));
#endif

                        endpoint.sendData(ProtocolMessageCode.walletStateChunk, m.ToArray());
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
                                    if (CoreProtocolMessage.processHelloMessage(endpoint, reader))
                                    {
                                        byte[] challenge_response = null;

                                        int challenge_len = reader.ReadInt32();
                                        byte[] challenge = reader.ReadBytes(challenge_len);

                                        challenge_response = CryptoManager.lib.getSignature(challenge, Node.walletStorage.getPrimaryPrivateKey());

                                        CoreProtocolMessage.sendHelloMessage(endpoint, true, challenge_response);
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
                                    if (CoreProtocolMessage.processHelloMessage(endpoint, reader))
                                    {
                                        char node_type = endpoint.presenceAddress.type;
                                        if (node_type != 'M' && node_type != 'H')
                                        {
                                            CoreProtocolMessage.sendBye(endpoint, ProtocolByeCode.expectingMaster, string.Format("Expecting master node."), "", true);
                                            return;
                                        }

                                        ulong last_block_num = reader.ReadUInt64();

                                        int bcLen = reader.ReadInt32();
                                        byte[] block_checksum = reader.ReadBytes(bcLen);

                                        int wsLen = reader.ReadInt32();

                                        if(last_block_num <= IxianHandler.getLastBlockHeight())
                                        {
                                            Block b = Node.blockChain.getBlock(last_block_num);
                                            if(b != null)
                                            {
                                                if(!b.blockChecksum.SequenceEqual(block_checksum))
                                                {
                                                    CoreProtocolMessage.sendBye(endpoint, ProtocolByeCode.forked, string.Format("This node is on a forked network on block {0}, disconnecting.", last_block_num), last_block_num.ToString(), true);
                                                    return;
                                                }
                                            }
                                        }

                                        byte[] walletstate_checksum = reader.ReadBytes(wsLen);
                                        int consensus = reader.ReadInt32(); // deprecated

                                        endpoint.blockHeight = last_block_num;

                                        if (Node.checkCurrentBlockDeprecation(last_block_num) == false)
                                        {
                                            CoreProtocolMessage.sendBye(endpoint, ProtocolByeCode.deprecated, string.Format("This node deprecated or will deprecate on block {0}, your block height is {1}, disconnecting.", Config.nodeDeprecationBlock, last_block_num), last_block_num.ToString(), true);
                                            return;
                                        }

                                        int block_version = reader.ReadInt32();

                                        // Check for legacy level
                                        ulong legacy_level = reader.ReadUInt64(); // deprecated


                                        int challenge_response_len = reader.ReadInt32();
                                        byte[] challenge_response = reader.ReadBytes(challenge_response_len);
                                        if (!CryptoManager.lib.verifySignature(endpoint.challenge, endpoint.serverPubKey, challenge_response))
                                        {
                                            CoreProtocolMessage.sendBye(endpoint, ProtocolByeCode.authFailed, string.Format("Invalid challenge response."), "", true);
                                            return;
                                        }

                                        ulong highest_block_height = IxianHandler.getHighestKnownNetworkBlockHeight();
                                        if (last_block_num + 10 < highest_block_height)
                                        {
                                            CoreProtocolMessage.sendBye(endpoint, ProtocolByeCode.tooFarBehind, string.Format("Your node is too far behind, your block height is {0}, highest network block height is {1}.", last_block_num, highest_block_height), highest_block_height.ToString(), true);
                                            return;
                                        }

                                        // Process the hello data
                                        Node.blockSync.onHelloDataReceived(last_block_num, block_checksum, block_version, walletstate_checksum, consensus, 0, true);
                                        endpoint.helloReceived = true;
                                        NetworkClientManager.recalculateLocalTimeDifference();
                                    }
                                }
                            }
                            break;

                        case ProtocolMessageCode.getBlock:
                            {
                                if (!Node.isMasterNode())
                                {
                                    Logging.warn("Block data was requested, but this node isn't a master node");
                                    return;
                                }

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
                                        bool full_header = false;
                                        try
                                        {
                                            full_header = reader.ReadBoolean();
                                        }catch(Exception)
                                        {

                                        }

                                        //Logging.info(String.Format("Block #{0} has been requested.", block_number));

                                        if (block_number > IxianHandler.getLastBlockHeight())
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

                                        if (!Node.blockProcessor.verifySigFreezedBlock(block))
                                        {
                                            Logging.warn("Sigfreezed block {0} was requested. but we don't have the correct sigfreeze!", block.blockNum);
                                        }

                                        bool frozen_sigs_only = true;

                                        if(block_number + 5 > IxianHandler.getLastBlockHeight())
                                        {
                                            if (block.getFrozenSignatureCount() < Node.blockChain.getRequiredConsensus(block_number))
                                            {
                                                frozen_sigs_only = false;
                                            }
                                        }

                                        endpoint.sendData(ProtocolMessageCode.blockData, block.getBytes(full_header, frozen_sigs_only), BitConverter.GetBytes(block.blockNum));
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

                                                Block tmp_block = IxianHandler.getLastBlock();

                                                // Send the block height for this balance
                                                writerw.Write(tmp_block.blockNum);
                                                // Send the block checksum for this balance
                                                writerw.Write(tmp_block.blockChecksum.Length);
                                                writerw.Write(tmp_block.blockChecksum);

#if TRACE_MEMSTREAM_SIZES
                                                Logging.info(String.Format("NetworkProtocol::parseProtocolMessage: {0}", mw.Length));
#endif

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

                                        endpoint.sendData(ProtocolMessageCode.transactionData, transaction.getBytes(true));
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
                                            ProtocolByeCode byeCode = (ProtocolByeCode) reader.ReadInt32();
                                            string byeMessage = reader.ReadString();
                                            string byeData = reader.ReadString();

                                            byeV1 = true;

                                            switch(byeCode)
                                            {
                                                case ProtocolByeCode.bye: // all good
                                                    break;

                                                case ProtocolByeCode.forked: // forked node disconnected
                                                    Logging.info(string.Format("Disconnected with message: {0} {1}", byeMessage, byeData));
                                                    break;

                                                case ProtocolByeCode.deprecated: // deprecated node disconnected
                                                    Logging.info(string.Format("Disconnected with message: {0} {1}", byeMessage, byeData));
                                                    break;

                                                case ProtocolByeCode.incorrectIp: // incorrect IP
                                                    if (IxiUtils.validateIPv4(byeData))
                                                    {
                                                        if (NetworkClientManager.getConnectedClients(true).Length < 2)
                                                        {
                                                            IxianHandler.publicIP = byeData;
                                                            Logging.info("Changed internal IP Address to " + byeData + ", reconnecting");
                                                        }
                                                    }
                                                    break;

                                                case ProtocolByeCode.notConnectable: // not connectable from the internet
                                                    Logging.error("This node must be connectable from the internet, to connect to the network.");
                                                    Logging.error("Please setup uPNP and/or port forwarding on your router for port " + IxianHandler.publicPort + ".");
                                                    NetworkServer.connectable = false;
                                                    break;

                                                case ProtocolByeCode.insufficientFunds:
                                                    if (Config.disableMiner == false)
                                                    {
                                                        Logging.info("Reconnecting in Worker mode.");
                                                        Node.convertToWorkerNode();
                                                    }
                                                    break;

                                                default:
                                                    Logging.warn(string.Format("Disconnected with message: {0} {1}", byeMessage, byeData));
                                                    break;
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
#if TRACE_MEMSTREAM_SIZES
                                        Logging.info(String.Format("NetworkProtocol::parseProtocolMessage2: {0}", m.Length));
#endif

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

                        case ProtocolMessageCode.updatePresence:
                            {
                                // Parse the data and update entries in the presence list
                                Presence updated_presence = PresenceList.updateFromBytes(data);

                                // If a presence entry was updated, broadcast this message again
                                if (updated_presence != null)
                                {
                                    CoreProtocolMessage.broadcastProtocolMessage(new char[] { 'M', 'R', 'H', 'W' }, ProtocolMessageCode.updatePresence, data, updated_presence.wallet, endpoint);

                                    // Send this keepalive message to all subscribed clients
                                    CoreProtocolMessage.broadcastEventDataMessage(NetworkEvents.Type.keepAlive, updated_presence.wallet, ProtocolMessageCode.updatePresence, data, updated_presence.wallet, endpoint);
                                }
                            }
                            break;

                        case ProtocolMessageCode.keepAlivePresence:
                            {
                                byte[] address = null;
                                bool updated = PresenceList.receiveKeepAlive(data, out address, endpoint);

                                // If a presence entry was updated, broadcast this message again
                                if (updated)
                                {
                                    CoreProtocolMessage.broadcastProtocolMessage(new char[] { 'M', 'R', 'H', 'W' }, ProtocolMessageCode.keepAlivePresence, data, address, endpoint);

                                    // Send this keepalive message to all subscribed clients
                                    CoreProtocolMessage.broadcastEventDataMessage(NetworkEvents.Type.keepAlive, address, ProtocolMessageCode.keepAlivePresence, data, address, endpoint);
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
                                        Presence p = PresenceList.getPresenceByAddress(wallet);
                                        if (p != null)
                                        {
                                            lock (p)
                                            {
                                                byte[][] presence_chunks = p.getByteChunks();
                                                foreach (byte[] presence_chunk in presence_chunks)
                                                {
                                                    endpoint.sendData(ProtocolMessageCode.updatePresence, presence_chunk, null);
                                                }
                                            }
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

                        // return 10 random presences of the selected type
                        case ProtocolMessageCode.getRandomPresences:
                            {
                                handleGetRandomPresences(data, endpoint);
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
                                NetworkEvents.handleAttachEventMessage(data, endpoint);
                            }
                            break;

                        case ProtocolMessageCode.detachEvent:
                            {
                                NetworkEvents.handleDetachEventMessage(data, endpoint);
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

                        case ProtocolMessageCode.blockSignatures:
                            {
                                handleSigfreezedBlockSignatures(data, endpoint);
                            }
                            break;

                        case ProtocolMessageCode.getNextSuperBlock:
                            {
                                handleGetNextSuperBlock(data, endpoint);
                            }
                            break;

                        case ProtocolMessageCode.getBlockHeaders:
                            {
                                handleGetBlockHeaders(data, endpoint);
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
        }
    }
}