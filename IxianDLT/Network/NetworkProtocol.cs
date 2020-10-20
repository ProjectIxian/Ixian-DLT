using DLT.Meta;
using IXICore;
using IXICore.Inventory;
using IXICore.Meta;
using IXICore.Network;
using IXICore.Utils;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
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
                                Transaction tx = TransactionPool.getAppliedTransaction(txIdArr[tx_index], blockNum, true);
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
                                endpoint.sendData(ProtocolMessageCode.blockTransactionsChunk, mOut.ToArray());
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
                            endpoint.sendData(ProtocolMessageCode.blockTransactionsChunk, mOut.ToArray());
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


            public static bool broadcastBlockSignature(byte[] signature_data, byte[] sig_address, ulong block_num, byte[] block_hash, RemoteEndpoint skipEndpoint = null, RemoteEndpoint endpoint = null)
            {
                if (endpoint != null)
                {
                    if (endpoint.isConnected())
                    {
                        endpoint.sendData(ProtocolMessageCode.blockSignature, signature_data);
                        return true;
                    }
                    return false;
                }
                else
                {
                    return CoreProtocolMessage.addToInventory(new char[] { 'M', 'H' }, new InventoryItemSignature(sig_address, block_num, block_hash), skipEndpoint, ProtocolMessageCode.blockSignature, signature_data, null);
                }
            }


            public static bool broadcastBlockSignature(ulong block_num, byte[] block_checksum, byte[] signature, byte[] signer_address, RemoteEndpoint skipEndpoint = null, RemoteEndpoint endpoint = null)
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
                    return broadcastBlockSignature(signature_data, signer_address, block_num, block_checksum, skipEndpoint, endpoint);
                }

                return false;
            }


            // Removes event subscriptions for the provided endpoint
            private static void handleBlockSignature(byte[] data, RemoteEndpoint endpoint)
            {
                if (Node.blockSync.synchronizing)
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

                        Node.inventoryCache.setProcessedFlag(InventoryItemTypes.blockSignature, InventoryItemSignature.getHash(sig_addr, checksum), true);

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

                        if (PresenceList.getPresenceByAddress(sig_addr) == null)
                        {
                            Logging.info("Received signature for block {0} whose signer isn't in the PL", block_num);
                            return;
                        }

                        if (Node.blockProcessor.addSignatureToBlock(block_num, checksum, sig, sig_addr, endpoint))
                        {
                            Node.blockProcessor.acceptLocalNewBlock();
                            if (Node.isMasterNode())
                            {
                                broadcastBlockSignature(data, sig_addr, block_num, checksum, endpoint);
                            }
                        }
                        else
                        {
                            // discard - it might have already been applied
                        }
                    }
                }
            }

            public static void broadcastBlockSignatures(ulong block_num, byte[] block_checksum, List<byte[][]> signatures, RemoteEndpoint skip_endpoint = null, RemoteEndpoint endpoint = null)
            {
                int max_sigs_per_chunk = ConsensusConfig.maximumBlockSigners;

                int sig_count = signatures.Count();

                if (sig_count == 0)
                {
                    return;
                }

                using (MemoryStream mOut = new MemoryStream())
                {
                    for(int i = 0; i < sig_count;)
                    {
                        using (BinaryWriter writer = new BinaryWriter(mOut))
                        {
                            writer.Write(block_num);

                            writer.Write(block_checksum.Length);
                            writer.Write(block_checksum);

                            int next_sig_count;
                            if (sig_count - i > max_sigs_per_chunk)
                            {
                                next_sig_count = max_sigs_per_chunk;
                            }
                            else
                            {
                                next_sig_count = sig_count - i;
                            }
                            writer.Write(next_sig_count);

                            for (int j = 0; j < next_sig_count; i++, j++)
                            {
                                byte[][] sig = signatures[i];
                                if (sig == null)
                                {
                                    continue;
                                }
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
                            if (!endpoint.isConnected())
                            {
                                return;
                            }
                            endpoint.sendData(ProtocolMessageCode.blockSignatures, mOut.ToArray(), BitConverter.GetBytes(block_num));
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
                }
                else
                {
                    broadcastBlockSignatures(b.blockNum, b.blockChecksum, b.signatures, skip_endpoint, endpoint);
                }
            }

            private static void handleGetBlock(byte[] data, RemoteEndpoint endpoint)
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
                        }
                        catch (Exception)
                        {

                        }

                        //Logging.info(String.Format("Block #{0} has been requested.", block_number));

                        ulong last_block_height = IxianHandler.getLastBlockHeight() + 1;

                        if (block_number > last_block_height)
                        {
                            return;
                        }

                        Block block = null;
                        if (block_number == last_block_height)
                        {
                            bool haveLock = false;
                            try
                            {
                                Monitor.TryEnter(Node.blockProcessor.localBlockLock, 1000, ref haveLock);
                                if (!haveLock)
                                {
                                    throw new TimeoutException();
                                }

                                Block tmp = Node.blockProcessor.getLocalBlock();
                                if (tmp != null && tmp.blockNum == last_block_height)
                                {
                                    block = tmp;
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
                        else
                        {
                            block = Node.blockChain.getBlock(block_number, Config.storeFullHistory);
                        }

                        if (block == null)
                        {
                            Logging.warn("Unable to find block #{0} in the chain!", block_number);
                            return;
                        }
                        //Logging.info(String.Format("Block #{0} ({1}) found, transmitting...", block_number, Crypto.hashToString(block.blockChecksum.Take(4).ToArray())));
                        // Send the block

                        if (include_transactions == 1)
                        {
                            handleGetBlockTransactions(block_number, false, endpoint);
                        }
                        else if (include_transactions == 2)
                        {
                            handleGetBlockTransactions(block_number, true, endpoint);
                        }

                        if (!Node.blockProcessor.verifySigFreezedBlock(block))
                        {
                            Logging.warn("Sigfreezed block {0} was requested. but we don't have the correct sigfreeze!", block.blockNum);
                        }

                        bool frozen_sigs_only = true;

                        if (block_number + 5 > IxianHandler.getLastBlockHeight())
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

            private static void handleGetBlockSignatures(ulong blockNum, byte[] checksum, RemoteEndpoint endpoint)
            {
                //Logging.info(String.Format("Received request for signatures in block {0}.", blockNum));

                // Get the requested block and corresponding signatures
                Block b = Node.blockChain.getBlock(blockNum, Config.storeFullHistory);

                if (b == null || !b.blockChecksum.SequenceEqual(checksum))
                {
                    // likely forked
                    if(b != null)
                    {
                        Logging.warn("Received forked block signature for block {0}", blockNum);
                    }
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
                        if (target_block == null)
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
                        }
                        else if (!target_block.blockChecksum.SequenceEqual(checksum))
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

                            if(sig_count > ConsensusConfig.maximumBlockSigners)
                            {
                                sig_count = ConsensusConfig.maximumBlockSigners;
                            }

                            Block dummy_block = new Block();
                            dummy_block.blockNum = block_num;
                            dummy_block.blockChecksum = checksum;

                            for (int i = 0; i < sig_count; i++)
                            {
                                int sig_len = reader.ReadInt32();
                                byte[] sig = reader.ReadBytes(sig_len);

                                int addr_len = reader.ReadInt32();
                                byte[] addr = reader.ReadBytes(addr_len);

                                Node.inventoryCache.setProcessedFlag(InventoryItemTypes.blockSignature, InventoryItemSignature.getHash(addr, checksum), true);

                                dummy_block.addSignature(sig, addr);

                                if(m.Position == m.Length)
                                {
                                    break;
                                }
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

                        block = Node.storage.getBlockByLastSBHash(checksum);

                        if (block != null)
                        {
                            endpoint.sendData(ProtocolMessageCode.blockData, block.getBytes(full_header), BitConverter.GetBytes(block.blockNum));

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

                                    endpoint.sendData(ProtocolMessageCode.blockData, segment_block.getBytes(), BitConverter.GetBytes(segment.blockNum));
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
                        if (totalCount > 1000)
                            totalCount = 1000;

                        if (endpoint != null)
                        {
                            if (endpoint.isConnected())
                            {
                                using (MemoryStream mOut = new MemoryStream())
                                {
                                    bool found = false;
                                    using (BinaryWriter writer = new BinaryWriter(mOut))
                                    {
                                        for (ulong i = 0; i < totalCount; i++)
                                        {
                                            // TODO TODO TODO block headers should be read from a separate storage and every node should keep a full copy
                                            Block block = Node.blockChain.getBlock(from + i, true, true);
                                            if (block == null)
                                                break;

                                            found = true;
                                            BlockHeader header = new BlockHeader(block);
                                            byte[] headerBytes = header.getBytes();
                                            writer.Write(headerBytes.Length);
                                            writer.Write(headerBytes);

                                            broadcastBlockHeaderTransactions(block, endpoint);
                                        }
                                    }
                                    if (found)
                                    {
                                        // Send the blockheaders
                                        endpoint.sendData(ProtocolMessageCode.blockHeaders, mOut.ToArray());
                                    }
                                }
                            }
                        }
                    }
                }
            }

            private static void handleGetPIT(byte[] data, RemoteEndpoint endpoint)
            {
                MemoryStream ms = new MemoryStream(data);
                using (BinaryReader r = new BinaryReader(ms))
                {
                    ulong block_num = r.ReadUInt64();
                    int filter_len = r.ReadInt32();
                    byte[] filter = r.ReadBytes(filter_len);
                    Cuckoo cf;
                    try
                    {
                        cf = new Cuckoo(filter);
                    }
                    catch (Exception)
                    {
                        Logging.warn("The Cuckoo filter in the getPIT message was invalid or corrupted!");
                        return;
                    }
                    Block b = Node.blockChain.getBlock(block_num, true, true);
                    if (b is null)
                    {
                        return;
                    }
                    if (b.version < BlockVer.v6)
                    {
                        Logging.warn("Neighbor {0} requested PIT information for block {0}, which was below the minimal PIT version.", endpoint.fullAddress, block_num);
                        return;
                    }
                    PrefixInclusionTree pit = new PrefixInclusionTree(44, 3);
                    List<string> interesting_transactions = new List<string>();
                    foreach (var tx in b.transactions)
                    {
                        pit.add(tx);
                        if (cf.Contains(Encoding.UTF8.GetBytes(tx)))
                        {
                            interesting_transactions.Add(tx);
                        }
                    }
                    // make sure we ended up with the correct PIT
                    if (!b.pitChecksum.SequenceEqual(pit.calculateTreeHash()))
                    {
                        // This is a serious error, but I am not sure how to respond to it right now.
                        Logging.error("Reconstructed PIT for block {0} does not match the checksum in block header!", block_num);
                        return;
                    }
                    byte[] minimal_pit = pit.getMinimumTreeTXList(interesting_transactions);
                    MemoryStream mOut = new MemoryStream(minimal_pit.Length + 12);
                    using (BinaryWriter w = new BinaryWriter(mOut, Encoding.UTF8, true))
                    {
                        w.Write(block_num);
                        w.Write(minimal_pit.Length);
                        w.Write(minimal_pit);
                    }
                    endpoint.sendData(ProtocolMessageCode.pitData, mOut.ToArray());
                }
            }

            private static void broadcastBlockHeaderTransactions(Block b, RemoteEndpoint endpoint)
            {
                if (!endpoint.isConnected())
                {
                    return;
                }

                foreach (var txid in b.transactions)
                {
                    Transaction t = TransactionPool.getAppliedTransaction(txid, b.blockNum, true);

                    if (endpoint.isSubscribedToAddress(NetworkEvents.Type.transactionFrom, new Address(t.pubKey).address))
                    {
                        endpoint.sendData(ProtocolMessageCode.transactionData, t.getBytes(true), null);
                    }
                    else
                    {
                        foreach (var entry in t.toList)
                        {
                            if (endpoint.isSubscribedToAddress(NetworkEvents.Type.transactionTo, entry.Key))
                            {
                                endpoint.sendData(ProtocolMessageCode.transactionData, t.getBytes(true), null);
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
                if (!Node.isMasterNode())
                {
                    return true;
                }
                if (endpoint != null)
                {
                    if (endpoint.isConnected())
                    {
                        endpoint.sendData(ProtocolMessageCode.blockData, b.getBytes(false), BitConverter.GetBytes(b.blockNum));
                        return true;
                    }
                    return false;
                }
                else
                {
                    return CoreProtocolMessage.addToInventory(new char[] { 'M', 'H' }, new InventoryItemBlock(b.blockChecksum, b.blockNum), skipEndpoint, ProtocolMessageCode.blockData, b.getBytes(false), BitConverter.GetBytes(b.blockNum));
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
                if (NetworkClientManager.sendToClient(neighbor, ProtocolMessageCode.syncWalletState, new byte[1], null) == false)
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
                        foreach (Wallet w in chunk.wallets)
                        {
                            writer.Write(w.id.Length);
                            writer.Write(w.id);
                            writer.Write(w.balance.ToString());

                            if (w.data != null)
                            {
                                writer.Write(w.data.Length);
                                writer.Write(w.data);
                            }
                            else
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
                                        if (last_block_num + 15 < highest_block_height)
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
                                handleGetBlock(data, endpoint);
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
                                        if (block_num == 0)
                                        {
                                            transaction = TransactionPool.getUnappliedTransaction(txid);
                                        }
                                        if (transaction == null)
                                        {
                                            transaction = TransactionPool.getAppliedTransaction(txid, block_num, true);
                                        }

                                        if (transaction == null)
                                        {
                                            Logging.warn("I do not have txid '{0}.", txid);
                                            return;
                                        }

                                        Logging.info("Sending transaction {0} - {1} - {2}.", transaction.id, Crypto.hashToString(transaction.checksum), transaction.amount);

                                        endpoint.sendData(ProtocolMessageCode.transactionData, transaction.getBytes(true));
                                    }
                                }
                            }
                            break;

                        case ProtocolMessageCode.newTransaction:
                        case ProtocolMessageCode.transactionData:
                            {
                                /*if(TransactionPool.checkSocketTransactionLimits(socket) == true)
                                {
                                    // Throttled, ignore this transaction
                                    return;
                                }*/

                                Transaction transaction = new Transaction(data);
                                Node.inventoryCache.setProcessedFlag(InventoryItemTypes.transaction, UTF8Encoding.UTF8.GetBytes(transaction.id), true);
                                if (transaction == null)
                                    return;

                                bool no_broadcast = false;
                                if (!Node.blockSync.synchronizing)
                                {
                                    if (transaction.type == (int)Transaction.Type.StakingReward)
                                    {
                                        // Skip received staking transactions if we're not synchronizing
                                        return;
                                    }
                                }else
                                {
                                    no_broadcast = true;
                                }

                                // Add the transaction to the pool
                                TransactionPool.addTransaction(transaction, no_broadcast, endpoint);
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
                                            ProtocolByeCode byeCode = (ProtocolByeCode)reader.ReadInt32();
                                            string byeMessage = reader.ReadString();
                                            string byeData = reader.ReadString();

                                            byeV1 = true;

                                            switch (byeCode)
                                            {
                                                case ProtocolByeCode.bye: // all good
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
                                                    Logging.info("Insufficient funds to connect as master node.");
                                                    break;

                                                default:
                                                    Logging.warn(string.Format("Disconnected with message: {0} {1}", byeMessage, byeData));
                                                    break;
                                            }
                                        }
                                        catch (Exception)
                                        {

                                        }
                                        if (byeV1)
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
                        case ProtocolMessageCode.blockData:
                            {
                                Block block = new Block(data);
                                Node.inventoryCache.setProcessedFlag(InventoryItemTypes.block, block.blockChecksum, true);
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
                                if (Node.blockSync.startOutgoingWSSync(endpoint) == false)
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
                                        if (num_wallets > CoreConfig.walletStateChunkSplit)
                                        {
                                            Logging.error(String.Format("Received {0} wallets in a chunk. ( > {1}).",
                                                num_wallets, CoreConfig.walletStateChunkSplit));
                                            return;
                                        }
                                        Wallet[] wallets = new Wallet[num_wallets];
                                        for (int i = 0; i < num_wallets; i++)
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
                                    CoreProtocolMessage.broadcastProtocolMessage(new char[] { 'M', 'H', 'W' }, ProtocolMessageCode.updatePresence, data, updated_presence.wallet, endpoint);

                                    // Send this keepalive message to all subscribed clients
                                    CoreProtocolMessage.broadcastEventDataMessage(NetworkEvents.Type.keepAlive, updated_presence.wallet, ProtocolMessageCode.updatePresence, data, updated_presence.wallet, endpoint);
                                }
                            }
                            break;

                        case ProtocolMessageCode.keepAlivePresence:
                            {
                                byte[] address = null;
                                long last_seen = 0;
                                byte[] device_id = null;

                                Node.inventoryCache.setProcessedFlag(InventoryItemTypes.keepAlive, Crypto.sha512sqTrunc(data), true);

                                bool updated = PresenceList.receiveKeepAlive(data, out address, out last_seen, out device_id, endpoint);

                                // If a presence entry was updated, broadcast this message again
                                if (updated)
                                {
                                    CoreProtocolMessage.addToInventory(new char[] { 'M', 'H', 'W' }, new InventoryItemKeepAlive(Crypto.sha512sqTrunc(data), last_seen, address, device_id), endpoint, ProtocolMessageCode.keepAlivePresence, data, address);

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

                        case ProtocolMessageCode.getKeepAlives:
                            handleGetKeepAlives(data, endpoint);
                            break;

                        case ProtocolMessageCode.keepAlivesChunk:
                            handleKeepAlivesChunk(data, endpoint);
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

                        case ProtocolMessageCode.blockTransactionsChunk:
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
                                            Node.inventoryCache.setProcessedFlag(InventoryItemTypes.transaction, UTF8Encoding.UTF8.GetBytes(tx.id), true);
                                            totalTxCount++;
                                            if (tx.type == (int)Transaction.Type.StakingReward && !Node.blockSync.synchronizing)
                                            {
                                                continue;
                                            }
                                            if (!TransactionPool.addTransaction(tx, true))
                                            {
                                                Logging.error(String.Format("Error adding transaction {0} received in a chunk to the transaction pool.", tx.id));
                                            }
                                            else
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

                        case ProtocolMessageCode.blockSignature:
                            {
                                handleBlockSignature(data, endpoint);
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

                        case ProtocolMessageCode.getPIT:
                            {
                                handleGetPIT(data, endpoint);
                            }
                            break;

                        case ProtocolMessageCode.inventory:
                            handleInventory(data, endpoint);
                            break;

                        case ProtocolMessageCode.getSignatures:
                            handleGetSignatures(data, endpoint);
                            break;

                        case ProtocolMessageCode.signaturesChunk:
                            handleSignaturesChunk(data, endpoint);
                            break;

                        case ProtocolMessageCode.getTransactions:
                            handleGetTransactions(data, endpoint);
                            break;

                        case ProtocolMessageCode.transactionsChunk:
                            handleTransactionsChunk(data, endpoint);
                            break;

                        default:
                            break;
                    }

                }
                catch (Exception e)
                {
                    Logging.error(string.Format("Error parsing network message. Details: {0}", e.ToString()));
                }
            }

            static void broadcastGetSignatures(ulong block_num, List<InventoryItemSignature> sig_list, RemoteEndpoint endpoint)
            {
                int sig_count = sig_list.Count;
                int max_sig_per_chunk = ConsensusConfig.maximumBlockSigners;
                using (MemoryStream mOut = new MemoryStream(max_sig_per_chunk * 570))
                {
                    for (int i = 0; i < sig_count;)
                    {
                        using (BinaryWriter writer = new BinaryWriter(mOut))
                        {
                            writer.WriteVarInt(block_num);

                            int next_sig_count;
                            if (sig_count - i > max_sig_per_chunk)
                            {
                                next_sig_count = max_sig_per_chunk;
                            }
                            else
                            {
                                next_sig_count = sig_count - i;
                            }

                            writer.WriteVarInt(next_sig_count);

                            for (int j = 0; j < next_sig_count; i++, j++)
                            {
                                InventoryItemSignature sig = sig_list[i];

                                long out_rollback_len = mOut.Length;

                                writer.WriteVarInt(sig.address.Length);
                                writer.Write(sig.address);

                                if (mOut.Length > CoreConfig.maxMessageSize)
                                {
                                    mOut.SetLength(out_rollback_len);
                                    i--;
                                    break;
                                }
                            }
                        }
                        endpoint.sendData(ProtocolMessageCode.getSignatures, mOut.ToArray(), null);
                    }
                }
            }

            static void handleGetSignatures(byte[] data, RemoteEndpoint endpoint)
            {
                if (Node.blockSync.synchronizing)
                {
                    return;
                }
                using (MemoryStream m = new MemoryStream(data))
                {
                    using (BinaryReader reader = new BinaryReader(m))
                    {
                        ulong block_number = reader.ReadVarUInt();

                        ulong last_block_height = IxianHandler.getLastBlockHeight() + 1;

                        if (block_number > last_block_height)
                        {
                            return;
                        }

                        Block block = null;
                        if (block_number == last_block_height)
                        {
                            bool haveLock = false;
                            try
                            {
                                Monitor.TryEnter(Node.blockProcessor.localBlockLock, 1000, ref haveLock);
                                if (!haveLock)
                                {
                                    throw new TimeoutException();
                                }

                                Block tmp = Node.blockProcessor.getLocalBlock();
                                if (tmp != null && tmp.blockNum == last_block_height)
                                {
                                    block = tmp;
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
                        else
                        {
                            block = Node.blockChain.getBlock(block_number, Config.storeFullHistory);
                        }

                        if (block == null)
                        {
                            Logging.warn("Unable to find block #{0} in the chain for fetching signatures!", block_number);
                            return;
                        }

                        int sig_count = (int)reader.ReadVarUInt();

                        int max_sigs_per_chunk = ConsensusConfig.maximumBlockSigners;

                        using (MemoryStream mOut = new MemoryStream(max_sigs_per_chunk * 570))
                        {
                            for(int i = 0; i < sig_count;)
                            {
                                using (BinaryWriter writer = new BinaryWriter(mOut))
                                {
                                    writer.WriteVarInt(block.blockNum);

                                    writer.WriteVarInt(block.blockChecksum.Length);
                                    writer.Write(block.blockChecksum);

                                    int next_sig_count;
                                    if (sig_count - i > max_sigs_per_chunk)
                                    {
                                        next_sig_count = max_sigs_per_chunk;
                                    }
                                    else
                                    {
                                        next_sig_count = sig_count - i;
                                    }
                                    writer.WriteVarInt(next_sig_count);

                                    for (int j = 0; j < next_sig_count; i++, j++)
                                    {
                                        int address_len = (int)reader.ReadVarInt();
                                        byte[] address = reader.ReadBytes(address_len);

                                        byte[] signature = block.getNodeSignature(address);
                                        if (signature == null)
                                        {
                                            continue;
                                        }

                                        writer.WriteVarInt(signature.Length);
                                        writer.Write(signature);

                                        writer.WriteVarInt(address_len);
                                        writer.Write(address);
                                    }
                                }
                                endpoint.sendData(ProtocolMessageCode.signaturesChunk, mOut.ToArray(), null);
                            }
                        }
                    }
                }
            }

            private static void handleSignaturesChunk(byte[] data, RemoteEndpoint endpoint)
            {
                using (MemoryStream m = new MemoryStream(data))
                {
                    using (BinaryReader reader = new BinaryReader(m))
                    {
                        ulong block_num = reader.ReadVarUInt();

                        int checksum_len = (int)reader.ReadVarUInt();
                        byte[] checksum = reader.ReadBytes(checksum_len);

                        ulong last_block_height = IxianHandler.getLastBlockHeight() + 1;

                        if (block_num > last_block_height)
                        {
                            return;
                        }

                        Block block = null;
                        if (block_num == last_block_height)
                        {
                            bool haveLock = false;
                            try
                            {
                                Monitor.TryEnter(Node.blockProcessor.localBlockLock, 1000, ref haveLock);
                                if (!haveLock)
                                {
                                    throw new TimeoutException();
                                }

                                Block tmp = Node.blockProcessor.getLocalBlock();
                                if (tmp != null && tmp.blockNum == last_block_height)
                                {
                                    block = tmp;
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
                        else
                        {
                            block = Node.blockChain.getBlock(block_num, false, false);
                        }


                        if (block == null)
                        {
                            // target block missing
                            Logging.warn("Target block {0} for adding sigs is missing", block_num);
                            return;
                        }
                        else if (!block.blockChecksum.SequenceEqual(checksum))
                        {
                            // incorrect target block
                            Logging.warn("Incorrect target block {0} - {1}, possibly forked", block_num, checksum);
                            return;
                        }


                        if (block_num + 5 < last_block_height)
                        {
                            // block already sigfreezed, do nothing
                            return;
                        }

                        int sig_count = (int)reader.ReadVarUInt();

                        if (sig_count > ConsensusConfig.maximumBlockSigners)
                        {
                            sig_count = ConsensusConfig.maximumBlockSigners;
                        }

                        if(block_num + 5 == last_block_height)
                        {
                            // handle currently sigfreezing block differently

                            Block dummy_block = new Block();
                            dummy_block.blockNum = block_num;
                            dummy_block.blockChecksum = checksum;

                            for (int i = 0; i < sig_count; i++)
                            {
                                int sig_len = (int)reader.ReadVarUInt();
                                byte[] sig = reader.ReadBytes(sig_len);

                                int addr_len = (int)reader.ReadVarUInt();
                                byte[] addr = reader.ReadBytes(addr_len);

                                Node.inventoryCache.setProcessedFlag(InventoryItemTypes.blockSignature, InventoryItemSignature.getHash(addr, checksum), true);

                                dummy_block.addSignature(sig, addr);

                                if (m.Position == m.Length)
                                {
                                    break;
                                }
                            }

                            Node.blockProcessor.handleSigFreezedBlock(dummy_block, endpoint);
                        }
                        else
                        {
                            for (int i = 0; i < sig_count; i++)
                            {
                                int sig_len = (int)reader.ReadVarUInt();
                                byte[] sig = reader.ReadBytes(sig_len);

                                int addr_len = (int)reader.ReadVarUInt();
                                byte[] addr = reader.ReadBytes(addr_len);

                                Node.inventoryCache.setProcessedFlag(InventoryItemTypes.blockSignature, InventoryItemSignature.getHash(addr, checksum), true);

                                if (PresenceList.getPresenceByAddress(addr) == null)
                                {
                                    Logging.info("Received signature for block {0} whose signer isn't in the PL", block_num);
                                    continue;
                                }

                                if (Node.blockProcessor.addSignatureToBlock(block_num, checksum, sig, addr, endpoint))
                                {
                                    if (Node.isMasterNode())
                                    {
                                        broadcastBlockSignature(data, addr, block_num, checksum, endpoint);
                                    }
                                }

                                if (m.Position == m.Length)
                                {
                                    break;
                                }
                            }
                        }
                        Node.blockProcessor.acceptLocalNewBlock();
                    }
                }
            }

            static void handleInventory(byte[] data, RemoteEndpoint endpoint)
            {
                using (MemoryStream m = new MemoryStream(data))
                {
                    using (BinaryReader reader = new BinaryReader(m))
                    {
                        ulong item_count = reader.ReadVarUInt();
                        if(item_count > (ulong)CoreConfig.maxInventoryItems)
                        {
                            Logging.warn("Received {0} inventory items, max items is {1}", item_count, CoreConfig.maxInventoryItems);
                            item_count = (ulong)CoreConfig.maxInventoryItems;
                        }

                        ulong last_block_height = IxianHandler.getLastBlockHeight();

                        Dictionary<ulong, List<InventoryItemSignature>> sig_lists = new Dictionary<ulong, List<InventoryItemSignature>>();
                        List<InventoryItemKeepAlive> ka_list = new List<InventoryItemKeepAlive>();
                        List<byte[]> tx_list = new List<byte[]>();
                        bool request_next_block = false;
                        for (ulong i = 0; i < item_count; i++)
                        {
                            ulong len = reader.ReadVarUInt();
                            byte[] item_bytes = reader.ReadBytes((int)len);
                            InventoryItem item = InventoryCache.decodeInventoryItem(item_bytes);
                            if(item.type == InventoryItemTypes.transaction)
                            {
                                PendingTransactions.increaseReceivedCount(UTF8Encoding.UTF8.GetString(item.hash), endpoint.presence.wallet);
                            }
                            PendingInventoryItem pii = Node.inventoryCache.add(item, endpoint);
                            if (!pii.processed && pii.lastRequested == 0)
                            {
                                // first time we're seeing this inventory item
                                switch(item.type)
                                {
                                    case InventoryItemTypes.keepAlive:
                                        ka_list.Add((InventoryItemKeepAlive)item);
                                        break;

                                    case InventoryItemTypes.transaction:
                                        tx_list.Add(item.hash);
                                        break;

                                    case InventoryItemTypes.blockSignature:
                                        var iis = (InventoryItemSignature)item;
                                        if (!sig_lists.ContainsKey(iis.blockNum))
                                        {
                                            sig_lists.Add(iis.blockNum, new List<InventoryItemSignature>());
                                        }
                                        sig_lists[iis.blockNum].Add(iis);
                                        break;

                                    case InventoryItemTypes.block:
                                        if (((InventoryItemBlock)item).blockNum <= last_block_height)
                                        {
                                            Node.inventoryCache.processInventoryItem(pii);
                                        }else
                                        {
                                            request_next_block = true;
                                        }
                                        break;

                                    default:
                                        Node.inventoryCache.processInventoryItem(pii);
                                        break;
                                }
                            }
                        }
                        broadcastGetTransactions(tx_list, endpoint);
                        broadcastGetKeepAlives(ka_list, endpoint);
                        foreach(var sig_list in sig_lists)
                        {
                            broadcastGetSignatures(sig_list.Key, sig_list.Value, endpoint);
                        }
                        if (request_next_block)
                        {
                            byte include_tx = 2;
                            if (Node.isMasterNode())
                            {
                                include_tx = 0;
                            }
                            broadcastGetBlock(last_block_height + 1, null, endpoint, include_tx, true);
                        }
                    }
                }
            }

            static void broadcastGetKeepAlives(List<InventoryItemKeepAlive> ka_list, RemoteEndpoint endpoint)
            {
                int ka_count = ka_list.Count;
                int max_ka_per_chunk = CoreConfig.maximumKeepAlivesPerChunk;
                using (MemoryStream mOut = new MemoryStream(max_ka_per_chunk * 570))
                {
                    for (int i = 0; i < ka_count;)
                    {
                        using (BinaryWriter writer = new BinaryWriter(mOut))
                        {
                            int next_ka_count;
                            if (ka_count - i > max_ka_per_chunk)
                            {
                                next_ka_count = max_ka_per_chunk;
                            }
                            else
                            {
                                next_ka_count = ka_count - i;
                            }
                            writer.WriteVarInt(next_ka_count);

                            for (int j = 0; j < next_ka_count; i++, j++)
                            {
                                InventoryItemKeepAlive ka = ka_list[i];

                                long rollback_pos = mOut.Position;

                                writer.WriteVarInt(ka.address.Length);
                                writer.Write(ka.address);

                                writer.WriteVarInt(ka.deviceId.Length);
                                writer.Write(ka.deviceId);

                                if (mOut.Length > CoreConfig.maxMessageSize)
                                {
                                    mOut.Position = rollback_pos;
                                    i--;
                                    break;
                                }
                            }
                        }
                        endpoint.sendData(ProtocolMessageCode.getKeepAlives, mOut.ToArray(), null);
                    }
                }
            }

            static void handleGetKeepAlives(byte[] data, RemoteEndpoint endpoint)
            {
                using (MemoryStream m = new MemoryStream(data))
                {
                    using (BinaryReader reader = new BinaryReader(m))
                    {
                        int ka_count = (int)reader.ReadVarUInt();

                        int max_ka_per_chunk = CoreConfig.maximumKeepAlivesPerChunk;

                        using (MemoryStream mOut = new MemoryStream(max_ka_per_chunk * 570))
                        {
                            for (int i = 0; i < ka_count;)
                            {
                                using (BinaryWriter writer = new BinaryWriter(mOut))
                                {
                                    int next_ka_count;
                                    if (ka_count - i > max_ka_per_chunk)
                                    {
                                        next_ka_count = max_ka_per_chunk;
                                    }
                                    else
                                    {
                                        next_ka_count = ka_count - i;
                                    }
                                    writer.WriteVarInt(next_ka_count);

                                    for (int j = 0; j < next_ka_count; i++, j++)
                                    {
                                        long in_rollback_pos = reader.BaseStream.Position;
                                        long out_rollback_len = mOut.Length;

                                        int address_len = (int)reader.ReadVarUInt();
                                        byte[] address = reader.ReadBytes(address_len);

                                        int device_len = (int)reader.ReadVarUInt();
                                        byte[] device = reader.ReadBytes(device_len);

                                        Presence p = PresenceList.getPresenceByAddress(address);
                                        if(p == null)
                                        {
                                            continue;
                                        }

                                        PresenceAddress pa = p.addresses.Find(x => x.device.SequenceEqual(device));
                                        if(pa == null)
                                        {
                                            continue;
                                        }

                                        byte[] ka_bytes = pa.getKeepAliveBytes(address);
                                        byte[] ka_len = VarInt.GetVarIntBytes(ka_bytes.Length);
                                        writer.Write(ka_len);
                                        writer.Write(ka_bytes);

                                        if (mOut.Length > CoreConfig.maxMessageSize)
                                        {
                                            reader.BaseStream.Position = in_rollback_pos;
                                            mOut.SetLength(out_rollback_len);
                                            i--;
                                            break;
                                        }
                                    }
                                }
                                endpoint.sendData(ProtocolMessageCode.keepAlivesChunk, mOut.ToArray(), null);
                            }
                        }
                    }
                }
            }


            private static void handleKeepAlivesChunk(byte[] data, RemoteEndpoint endpoint)
            {
                using (MemoryStream m = new MemoryStream(data))
                {
                    using (BinaryReader reader = new BinaryReader(m))
                    {
                        int ka_count = (int)reader.ReadVarUInt();

                        int max_ka_per_chunk = CoreConfig.maximumKeepAlivesPerChunk;

                        for (int i = 0; i < ka_count; i++)
                        {
                            int ka_len = (int)reader.ReadVarUInt();
                            byte[] ka_bytes = reader.ReadBytes(ka_len);

                            Node.inventoryCache.setProcessedFlag(InventoryItemTypes.keepAlive, Crypto.sha512sqTrunc(ka_bytes), true);

                            byte[] address;
                            long last_seen;
                            byte[] device_id;
                            bool updated = PresenceList.receiveKeepAlive(ka_bytes, out address, out last_seen, out device_id, endpoint);

                            // If a presence entry was updated, broadcast this message again
                            if (updated)
                            {
                                CoreProtocolMessage.addToInventory(new char[] { 'M', 'H', 'W' }, new InventoryItemKeepAlive(Crypto.sha512sqTrunc(data), last_seen, address, device_id), endpoint, ProtocolMessageCode.keepAlivePresence, data, address);

                                // Send this keepalive message to all subscribed clients
                                CoreProtocolMessage.broadcastEventDataMessage(NetworkEvents.Type.keepAlive, address, ProtocolMessageCode.keepAlivePresence, data, address, endpoint);
                            }

                            if (m.Position == m.Length)
                            {
                                break;
                            }
                        }
                    }
                }
            }

            static void broadcastGetTransactions(List<byte[]> tx_list, RemoteEndpoint endpoint)
            {
                int tx_count = tx_list.Count;
                int max_tx_per_chunk = CoreConfig.maximumTransactionsPerChunk;
                using (MemoryStream mOut = new MemoryStream(max_tx_per_chunk * 570))
                {
                    for (int i = 0; i < tx_count;)
                    {
                        using (BinaryWriter writer = new BinaryWriter(mOut))
                        {
                            int next_ka_count;
                            if (tx_count - i > max_tx_per_chunk)
                            {
                                next_ka_count = max_tx_per_chunk;
                            }
                            else
                            {
                                next_ka_count = tx_count - i;
                            }
                            writer.WriteVarInt(next_ka_count);

                            for (int j = 0; j < next_ka_count; i++, j++)
                            {
                                long rollback_pos = mOut.Position;

                                writer.WriteVarInt(tx_list[i].Length);
                                writer.Write(tx_list[i]);

                                if (mOut.Length > CoreConfig.maxMessageSize)
                                {
                                    mOut.Position = rollback_pos;
                                    i--;
                                    break;
                                }
                            }
                        }
                        endpoint.sendData(ProtocolMessageCode.getTransactions, mOut.ToArray(), null);
                    }
                }
            }

            static void handleGetTransactions(byte[] data, RemoteEndpoint endpoint)
            {
                using (MemoryStream m = new MemoryStream(data))
                {
                    using (BinaryReader reader = new BinaryReader(m))
                    {
                        int tx_count = (int)reader.ReadVarUInt();

                        int max_tx_per_chunk = CoreConfig.maximumTransactionsPerChunk;

                        using (MemoryStream mOut = new MemoryStream(max_tx_per_chunk * 570))
                        {
                            for (int i = 0; i < tx_count;)
                            {
                                using (BinaryWriter writer = new BinaryWriter(mOut))
                                {
                                    int next_tx_count;
                                    if (tx_count - i > max_tx_per_chunk)
                                    {
                                        next_tx_count = max_tx_per_chunk;
                                    }
                                    else
                                    {
                                        next_tx_count = tx_count - i;
                                    }
                                    writer.WriteVarInt(next_tx_count);

                                    for (int j = 0; j < next_tx_count; i++, j++)
                                    {
                                        long in_rollback_pos = reader.BaseStream.Position;
                                        long out_rollback_len = mOut.Length;

                                        int txid_len = (int)reader.ReadVarUInt();
                                        byte[] txid = reader.ReadBytes(txid_len);
                                        string txid_str = UTF8Encoding.UTF8.GetString(txid);

                                        Transaction tx = TransactionPool.getUnappliedTransaction(txid_str);
                                        if (tx == null)
                                        {
                                            tx = TransactionPool.getAppliedTransaction(txid_str);
                                            if (tx == null)
                                            {
                                                continue;
                                            }
                                        }

                                        byte[] tx_bytes = tx.getBytes();
                                        byte[] tx_len = VarInt.GetVarIntBytes(tx_bytes.Length);
                                        writer.Write(tx_len);
                                        writer.Write(tx_bytes);

                                        if (mOut.Length > CoreConfig.maxMessageSize)
                                        {
                                            reader.BaseStream.Position = in_rollback_pos;
                                            mOut.SetLength(out_rollback_len);
                                            i--;
                                            break;
                                        }
                                    }
                                }
                                endpoint.sendData(ProtocolMessageCode.transactionsChunk, mOut.ToArray(), null);
                            }
                        }
                    }
                }
            }

            private static void handleTransactionsChunk(byte[] data, RemoteEndpoint endpoint)
            {
                using (MemoryStream m = new MemoryStream(data))
                {
                    using (BinaryReader reader = new BinaryReader(m))
                    {
                        int tx_count = (int)reader.ReadVarUInt();

                        int max_tx_per_chunk = CoreConfig.maximumTransactionsPerChunk;
                        if(tx_count > max_tx_per_chunk)
                        {
                            tx_count = max_tx_per_chunk;
                        }

                        var sw = new System.Diagnostics.Stopwatch();
                        sw.Start();
                        int processedTxCount = 0;
                        int totalTxCount = 0;
                        for (int i = 0; i < tx_count; i++)
                        {
                            int tx_len = (int)reader.ReadVarUInt();
                            byte[] tx_bytes = reader.ReadBytes(tx_len);

                            Transaction tx = new Transaction(tx_bytes);

                            Node.inventoryCache.setProcessedFlag(InventoryItemTypes.transaction, UTF8Encoding.UTF8.GetBytes(tx.id), true);

                            totalTxCount++;
                            if (tx.type == (int)Transaction.Type.StakingReward && !Node.blockSync.synchronizing)
                            {
                                continue;
                            }
                            if (!TransactionPool.addTransaction(tx, false, endpoint))
                            {
                                Logging.error("Error adding transaction {0} received in a chunk to the transaction pool.", tx.id);
                            }
                            else
                            {
                                processedTxCount++;
                            }

                            if (m.Position == m.Length)
                            {
                                break;
                            }
                        }
                        sw.Stop();
                        TimeSpan elapsed = sw.Elapsed;
                        Logging.info("Processed {0}/{1} txs in {2}ms", processedTxCount, totalTxCount, elapsed.TotalMilliseconds);
                    }
                }
            }
        }
    }
}