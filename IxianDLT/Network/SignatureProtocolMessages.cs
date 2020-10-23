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
using System.Threading;

namespace DLT
{
    namespace Network
    {
        class SignatureProtocolMessages
        {

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
            public static void handleBlockSignature(byte[] data, RemoteEndpoint endpoint)
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
                                BlockProtocolMessages.broadcastGetBlock(last_bh + 1, null, endpoint);
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

            // Removes event subscriptions for the provided endpoint
            public static void handleBlockSignature2(byte[] data, RemoteEndpoint endpoint)
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
                        ulong block_num = reader.ReadIxiVarUInt();

                        int checksum_len = (int)reader.ReadIxiVarUInt();
                        byte[] checksum = reader.ReadBytes(checksum_len);

                        int sig_len = (int)reader.ReadIxiVarUInt();
                        byte[] sig = reader.ReadBytes(sig_len);

                        int sig_addr_len = (int)reader.ReadIxiVarUInt();
                        byte[] sig_addr = reader.ReadBytes(sig_addr_len);

                        Node.inventoryCache.setProcessedFlag(InventoryItemTypes.blockSignature, InventoryItemSignature.getHash(sig_addr, checksum), true);

                        ulong last_bh = IxianHandler.getLastBlockHeight();

                        lock (Node.blockProcessor.localBlockLock)
                        {
                            if (last_bh + 1 < block_num || (last_bh + 1 == block_num && Node.blockProcessor.getLocalBlock() == null))
                            {
                                Logging.info("Received signature for block {0} which is missing", block_num);
                                // future block, request the next block
                                BlockProtocolMessages.broadcastGetBlock2(last_bh + 1, null, endpoint);
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

            public static void broadcastBlockSignatures2(Block b, RemoteEndpoint skip_endpoint = null, RemoteEndpoint endpoint = null)
            {
                if (b.frozenSignatures != null)
                {
                    broadcastBlockSignatures2(b.blockNum, b.blockChecksum, b.frozenSignatures, skip_endpoint, endpoint);
                }
                else
                {
                    broadcastBlockSignatures2(b.blockNum, b.blockChecksum, b.signatures, skip_endpoint, endpoint);
                }
            }


            public static void handleGetBlockSignatures(ulong blockNum, byte[] checksum, RemoteEndpoint endpoint)
            {
                //Logging.info(String.Format("Received request for signatures in block {0}.", blockNum));

                // Get the requested block and corresponding signatures
                Block b = Node.blockChain.getBlock(blockNum, Config.storeFullHistory);

                if (b == null || !b.blockChecksum.SequenceEqual(checksum))
                {
                    // likely forked
                    if (b != null)
                    {
                        Logging.warn("Received forked block signature for block {0}", blockNum);
                    }
                    return;
                }

                broadcastBlockSignatures(b, null, endpoint);
            }

            public static void handleGetBlockSignatures2(ulong blockNum, byte[] checksum, RemoteEndpoint endpoint)
            {
                //Logging.info(String.Format("Received request for signatures in block {0}.", blockNum));

                // Get the requested block and corresponding signatures
                Block b = Node.blockChain.getBlock(blockNum, Config.storeFullHistory);

                if (b == null || !b.blockChecksum.SequenceEqual(checksum))
                {
                    // likely forked
                    if (b != null)
                    {
                        Logging.warn("Received forked block signature for block {0}", blockNum);
                    }
                    return;
                }

                broadcastBlockSignatures2(b, null, endpoint);
            }


            public static void handleSigfreezedBlockSignatures(byte[] data, RemoteEndpoint endpoint)
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
                                BlockProtocolMessages.broadcastGetBlock(block_num, null, endpoint);
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

                            if (sig_count > ConsensusConfig.maximumBlockSigners)
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

                                if (m.Position == m.Length)
                                {
                                    break;
                                }
                            }

                            Node.blockProcessor.handleSigFreezedBlock(dummy_block, endpoint);
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
                    for (int i = 0; i < sig_count;)
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

            public static void broadcastBlockSignatures2(ulong block_num, byte[] block_checksum, List<byte[][]> signatures, RemoteEndpoint skip_endpoint = null, RemoteEndpoint endpoint = null)
            {
                int max_sigs_per_chunk = ConsensusConfig.maximumBlockSigners;

                int sig_count = signatures.Count();

                if (sig_count == 0)
                {
                    return;
                }

                using (MemoryStream mOut = new MemoryStream())
                {
                    for (int i = 0; i < sig_count;)
                    {
                        using (BinaryWriter writer = new BinaryWriter(mOut))
                        {
                            writer.WriteIxiVarInt(block_num);

                            writer.WriteIxiVarInt(block_checksum.Length);
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
                            writer.WriteIxiVarInt(next_sig_count);

                            for (int j = 0; j < next_sig_count; i++, j++)
                            {
                                byte[][] sig = signatures[i];
                                if (sig == null)
                                {
                                    continue;
                                }
                                // sig
                                writer.WriteIxiVarInt(sig[0].Length);
                                writer.Write(sig[0]);

                                // address/pubkey
                                writer.WriteIxiVarInt(sig[1].Length);
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
                            endpoint.sendData(ProtocolMessageCode.signaturesChunk, mOut.ToArray(), BitConverter.GetBytes(block_num));
                        }
                        else
                        {
                            CoreProtocolMessage.broadcastProtocolMessage(new char[] { 'M', 'H' }, ProtocolMessageCode.signaturesChunk, mOut.ToArray(), BitConverter.GetBytes(block_num), skip_endpoint);
                        }
                    }
                }
            }


            public static void broadcastGetSignatures(ulong block_num, List<InventoryItemSignature> sig_list, RemoteEndpoint endpoint)
            {
                int sig_count = sig_list.Count;
                int max_sig_per_chunk = ConsensusConfig.maximumBlockSigners;
                using (MemoryStream mOut = new MemoryStream(max_sig_per_chunk * 570))
                {
                    for (int i = 0; i < sig_count;)
                    {
                        using (BinaryWriter writer = new BinaryWriter(mOut))
                        {
                            writer.WriteIxiVarInt(block_num);

                            int next_sig_count;
                            if (sig_count - i > max_sig_per_chunk)
                            {
                                next_sig_count = max_sig_per_chunk;
                            }
                            else
                            {
                                next_sig_count = sig_count - i;
                            }

                            writer.WriteIxiVarInt(next_sig_count);

                            for (int j = 0; j < next_sig_count; i++, j++)
                            {
                                InventoryItemSignature sig = sig_list[i];

                                long out_rollback_len = mOut.Length;

                                writer.WriteIxiVarInt(sig.address.Length);
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

            public static void handleGetSignatures(byte[] data, RemoteEndpoint endpoint)
            {
                if (Node.blockSync.synchronizing)
                {
                    return;
                }
                using (MemoryStream m = new MemoryStream(data))
                {
                    using (BinaryReader reader = new BinaryReader(m))
                    {
                        ulong block_number = reader.ReadIxiVarUInt();

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

                        int sig_count = (int)reader.ReadIxiVarUInt();

                        int max_sigs_per_chunk = ConsensusConfig.maximumBlockSigners;

                        using (MemoryStream mOut = new MemoryStream(max_sigs_per_chunk * 570))
                        {
                            for (int i = 0; i < sig_count;)
                            {
                                using (BinaryWriter writer = new BinaryWriter(mOut))
                                {
                                    writer.WriteIxiVarInt(block.blockNum);

                                    writer.WriteIxiVarInt(block.blockChecksum.Length);
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
                                    writer.WriteIxiVarInt(next_sig_count);

                                    for (int j = 0; j < next_sig_count; i++, j++)
                                    {
                                        int address_len = (int)reader.ReadIxiVarUInt();
                                        byte[] address = reader.ReadBytes(address_len);

                                        byte[] signature = block.getNodeSignature(address);
                                        if (signature == null)
                                        {
                                            continue;
                                        }

                                        writer.WriteIxiVarInt(signature.Length);
                                        writer.Write(signature);

                                        writer.WriteIxiVarInt(address_len);
                                        writer.Write(address);
                                    }
                                }
                                endpoint.sendData(ProtocolMessageCode.signaturesChunk, mOut.ToArray(), null);
                            }
                        }
                    }
                }
            }

            public static void handleSignaturesChunk(byte[] data, RemoteEndpoint endpoint)
            {
                using (MemoryStream m = new MemoryStream(data))
                {
                    using (BinaryReader reader = new BinaryReader(m))
                    {
                        ulong block_num = reader.ReadIxiVarUInt();

                        int checksum_len = (int)reader.ReadIxiVarUInt();
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

                        int sig_count = (int)reader.ReadIxiVarUInt();

                        if (sig_count > ConsensusConfig.maximumBlockSigners)
                        {
                            sig_count = ConsensusConfig.maximumBlockSigners;
                        }

                        if (block_num + 5 == last_block_height)
                        {
                            // handle currently sigfreezing block differently

                            Block dummy_block = new Block();
                            dummy_block.blockNum = block_num;
                            dummy_block.blockChecksum = checksum;

                            for (int i = 0; i < sig_count; i++)
                            {
                                int sig_len = (int)reader.ReadIxiVarUInt();
                                byte[] sig = reader.ReadBytes(sig_len);

                                int addr_len = (int)reader.ReadIxiVarUInt();
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
                                int sig_len = (int)reader.ReadIxiVarUInt();
                                byte[] sig = reader.ReadBytes(sig_len);

                                int addr_len = (int)reader.ReadIxiVarUInt();
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

            public static bool broadcastGetBlockSignatures2(ulong block_num, byte[] block_checksum, RemoteEndpoint endpoint)
            {
                using (MemoryStream mw = new MemoryStream())
                {
                    using (BinaryWriter writerw = new BinaryWriter(mw))
                    {
                        writerw.WriteIxiVarInt(block_num);

                        writerw.WriteIxiVarInt(block_checksum.Length);
                        writerw.Write(block_checksum);
#if TRACE_MEMSTREAM_SIZES
                        Logging.info(String.Format("NetworkProtocol::broadcastGetBlockSignatures: {0}", mw.Length));
#endif

                        if (endpoint != null)
                        {
                            if (endpoint.isConnected())
                            {
                                endpoint.sendData(ProtocolMessageCode.getBlockSignatures2, mw.ToArray());
                                return true;
                            }
                        }
                        return CoreProtocolMessage.broadcastProtocolMessageToSingleRandomNode(new char[] { 'M', 'H' }, ProtocolMessageCode.getBlockSignatures2, mw.ToArray(), block_num);
                    }
                }
            }
        }
    }
}