// Copyright (C) 2017-2020 Ixian OU
// This file is part of Ixian DLT - www.github.com/ProjectIxian/Ixian-DLT
//
// Ixian DLT is free software: you can redistribute it and/or modify
// it under the terms of the MIT License as published
// by the Open Source Initiative.
//
// Ixian DLT is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// MIT License for more details.

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
            public static bool broadcastBlockSignature(BlockSignature signature, ulong blockNum, byte[] blockHash, RemoteEndpoint skipEndpoint = null, RemoteEndpoint endpoint = null)
            {
                signature.blockNum = blockNum;
                signature.blockHash = blockHash;
                if (endpoint != null)
                {
                    if (endpoint.isConnected())
                    {
                        byte[] signature_data = signature.getBytesForBroadcast();
                        endpoint.sendData(ProtocolMessageCode.blockSignature2, signature_data);
                        return true;
                    }
                    return false;
                }
                else
                {
                    return CoreProtocolMessage.addToInventory(new char[] { 'M', 'H' }, new InventoryItemSignature(signature.recipientPubKeyOrAddress, signature.blockNum, signature.blockHash), skipEndpoint);
                }
            }

            public static void handleBlockSignature2(byte[] data, RemoteEndpoint endpoint)
            {
                if (Node.blockSync.synchronizing)
                {
                    return;
                }

                if (data == null)
                {
                    Logging.warn("Invalid protocol message signature data");
                    return;
                }

                BlockSignature blockSig = new BlockSignature(data, true);

                lock (Node.blockProcessor.localBlockLock)
                {
                    ulong last_bh = IxianHandler.getLastBlockHeight();

                    if (last_bh + 1 < blockSig.blockNum || (last_bh + 1 == blockSig.blockNum && Node.blockProcessor.getLocalBlock() == null))
                    {
                        Logging.info("Received signature for block {0} which is missing", blockSig.blockNum);
                        // future block, request the next block
                        BlockProtocolMessages.broadcastGetBlock(last_bh + 1, null, endpoint);
                        return;
                    }
                }

                Node.inventoryCache.setProcessedFlag(InventoryItemTypes.blockSignature, InventoryItemSignature.getHash(blockSig.recipientPubKeyOrAddress.addressNoChecksum, blockSig.blockHash), true);

                if (PresenceList.getPresenceByAddress(blockSig.recipientPubKeyOrAddress) == null)
                {
                    Logging.info("Received signature for block {0} whose signer isn't in the PL", blockSig.blockNum);
                    return;
                }

                if (Node.blockProcessor.addSignatureToBlock(blockSig, endpoint))
                {
                    Node.blockProcessor.acceptLocalNewBlock();
                    if (Node.isMasterNode())
                    {
                        broadcastBlockSignature(blockSig, blockSig.blockNum, blockSig.blockHash, endpoint);
                    }
                }
                else
                {
                    // discard - it might have already been applied
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

                broadcastBlockSignatures(b, null, endpoint);
            }

            public static void broadcastBlockSignatures(ulong block_num, byte[] block_checksum, List<BlockSignature> signatures, RemoteEndpoint skip_endpoint = null, RemoteEndpoint endpoint = null)
            {
                int max_sigs_per_chunk = ConsensusConfig.maximumBlockSigners;

                int sig_count = signatures.Count();

                if (sig_count == 0)
                {
                    return;
                }

                for (int i = 0; i < sig_count;)
                {
                    using (MemoryStream mOut = new MemoryStream())
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

                            for (int j = 0; j < next_sig_count && i < sig_count; j++)
                            {
                                BlockSignature sig = signatures[i];
                                i++;
                                if (sig == null)
                                {
                                    continue;
                                }
                                byte[] sig_bytes = sig.getBytesForBlock();
                                // sig
                                writer.WriteIxiVarInt(sig_bytes.Length);
                                writer.Write(sig_bytes);
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
                            endpoint.sendData(ProtocolMessageCode.signaturesChunk2, mOut.ToArray(), BitConverter.GetBytes(block_num));
                        }
                        else
                        {
                            CoreProtocolMessage.broadcastProtocolMessage(new char[] { 'M', 'H' }, ProtocolMessageCode.signaturesChunk2, mOut.ToArray(), BitConverter.GetBytes(block_num), skip_endpoint);
                        }
                    }
                }
            }

            public static void broadcastGetSignatures(ulong block_num, List<InventoryItemSignature> sig_list, RemoteEndpoint endpoint)
            {
                int sig_count = sig_list.Count;
                int max_sig_per_chunk = ConsensusConfig.maximumBlockSigners;
                for (int i = 0; i < sig_count;)
                {
                    using (MemoryStream mOut = new MemoryStream(max_sig_per_chunk * 570))
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

                            for (int j = 0; j < next_sig_count && i < sig_count; j++)
                            {
                                InventoryItemSignature sig = sig_list[i];
                                i++;

                                long out_rollback_len = mOut.Length;

                                writer.WriteIxiVarInt(sig.address.addressNoChecksum.Length);
                                writer.Write(sig.address.addressNoChecksum);

                                if (mOut.Length > CoreConfig.maxMessageSize)
                                {
                                    mOut.SetLength(out_rollback_len);
                                    i--;
                                    break;
                                }
                            }
                        }
                        endpoint.sendData(ProtocolMessageCode.getSignatures2, mOut.ToArray(), null);
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
                        
                        if(block == null)
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
                        if(sig_count > max_sigs_per_chunk)
                        {
                            sig_count = max_sigs_per_chunk;
                        }

                        for (int i = 0; i < sig_count;)
                        {
                            using (MemoryStream mOut = new MemoryStream(max_sigs_per_chunk * 570))
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

                                    for (int j = 0; j < next_sig_count && i < sig_count; j++)
                                    {
                                        i++;
                                        if (m.Position == m.Length)
                                        {
                                            break;
                                        }

                                        int address_len = (int)reader.ReadIxiVarUInt();
                                        Address address = new Address(reader.ReadBytes(address_len));

                                        BlockSignature signature = block.getNodeSignature(address);
                                        if (signature == null)
                                        {
                                            continue;
                                        }

                                        writer.WriteIxiVarInt(signature.signature.Length);
                                        writer.Write(signature.signature);

                                        writer.WriteIxiVarInt(address_len);
                                        writer.Write(address.getInputBytes(true));
                                    }
                                }
                                endpoint.sendData(ProtocolMessageCode.signaturesChunk, mOut.ToArray(), null);
                            }
                        }
                    }
                }
            }


            public static void handleGetSignatures2(byte[] data, RemoteEndpoint endpoint)
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

                        if (block == null)
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
                        if (sig_count > max_sigs_per_chunk)
                        {
                            sig_count = max_sigs_per_chunk;
                        }

                        for (int i = 0; i < sig_count;)
                        {
                            using (MemoryStream mOut = new MemoryStream(max_sigs_per_chunk * 570))
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

                                    for (int j = 0; j < next_sig_count && i < sig_count; j++)
                                    {
                                        i++;
                                        if (m.Position == m.Length)
                                        {
                                            break;
                                        }

                                        int address_len = (int)reader.ReadIxiVarUInt();
                                        Address address = new Address(reader.ReadBytes(address_len));

                                        BlockSignature signature = block.getNodeSignature(address);
                                        if (signature == null)
                                        {
                                            continue;
                                        }

                                        byte[] sig_bytes = signature.getBytesForBlock();

                                        writer.WriteIxiVarInt(sig_bytes.Length);
                                        writer.Write(sig_bytes);
                                    }
                                }
                                endpoint.sendData(ProtocolMessageCode.signaturesChunk2, mOut.ToArray(), null);
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

                        if (block == null)
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
                            Logging.warn("Incorrect target block {0} - {1}, possibly forked", block_num, Crypto.hashToString(checksum));
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
                            dummy_block.blockProposer = block.blockProposer;

                            for (int i = 0; i < sig_count; i++)
                            {
                                if (m.Position == m.Length)
                                {
                                    break;
                                }

                                int sig_len = (int)reader.ReadIxiVarUInt();
                                byte[] sig = reader.ReadBytes(sig_len);

                                int addr_len = (int)reader.ReadIxiVarUInt();
                                byte[] addr = reader.ReadBytes(addr_len);

                                BlockSignature blockSig = new BlockSignature() { signature = sig, recipientPubKeyOrAddress = new Address(addr) };
                                dummy_block.signatures.Add(blockSig);

                                Node.inventoryCache.setProcessedFlag(InventoryItemTypes.blockSignature, InventoryItemSignature.getHash(blockSig.recipientPubKeyOrAddress.addressNoChecksum, checksum), true);
                            }

                            Node.blockProcessor.handleSigFreezedBlock(dummy_block, true, endpoint);
                        }
                        else
                        {
                            for (int i = 0; i < sig_count; i++)
                            {
                                if (m.Position == m.Length)
                                {
                                    break;
                                }

                                int sig_len = (int)reader.ReadIxiVarUInt();
                                byte[] sig = reader.ReadBytes(sig_len);

                                int addr_len = (int)reader.ReadIxiVarUInt();
                                byte[] addr = reader.ReadBytes(addr_len);

                                Address signerAddress = new Address(addr);

                                Node.inventoryCache.setProcessedFlag(InventoryItemTypes.blockSignature, InventoryItemSignature.getHash(signerAddress.addressNoChecksum, checksum), true);

                                if (PresenceList.getPresenceByAddress(signerAddress) == null)
                                {
                                    Logging.info("Received signature for block {0} whose signer isn't in the PL", block_num);
                                    continue;
                                }
                                BlockSignature blockSig = new BlockSignature() { blockNum = block_num, blockHash = checksum, signature = sig, recipientPubKeyOrAddress = signerAddress };
                                if (Node.blockProcessor.addSignatureToBlock(blockSig, endpoint))
                                {
                                    if (Node.isMasterNode())
                                    {
                                        broadcastBlockSignature(blockSig, block_num, checksum, endpoint);
                                    }
                                }
                            }
                        }
                        Node.blockProcessor.acceptLocalNewBlock();
                    }
                }
            }

            public static void handleSignaturesChunk2(byte[] data, RemoteEndpoint endpoint)
            {
                using (MemoryStream m = new MemoryStream(data))
                {
                    using (BinaryReader reader = new BinaryReader(m))
                    {
                        ulong block_num = reader.ReadIxiVarUInt();

                        int checksum_len = (int)reader.ReadIxiVarUInt();
                        byte[] checksum = reader.ReadBytes(checksum_len);

                        ulong last_block_height = IxianHandler.getLastBlockHeight() + 1;

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

                        if (block == null)
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
                            Logging.warn("Incorrect target block {0} - {1}, possibly forked", block_num, Crypto.hashToString(checksum));
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
                            dummy_block.blockProposer = block.blockProposer;

                            for (int i = 0; i < sig_count; i++)
                            {
                                if (m.Position == m.Length)
                                {
                                    break;
                                }

                                int sig_len = (int)reader.ReadIxiVarUInt();
                                byte[] sig = reader.ReadBytes(sig_len);

                                BlockSignature blockSig = new BlockSignature(sig, false);

                                Node.inventoryCache.setProcessedFlag(InventoryItemTypes.blockSignature, InventoryItemSignature.getHash(blockSig.recipientPubKeyOrAddress.addressNoChecksum, checksum), true);

                                dummy_block.signatures.Add(blockSig);
                            }

                            Node.blockProcessor.handleSigFreezedBlock(dummy_block, true, endpoint);
                        }
                        else
                        {
                            for (int i = 0; i < sig_count; i++)
                            {
                                if (m.Position == m.Length)
                                {
                                    break;
                                }

                                int sig_len = (int)reader.ReadIxiVarUInt();
                                byte[] sig = reader.ReadBytes(sig_len);

                                BlockSignature blockSig = new BlockSignature(sig, false);
                                blockSig.blockHash = checksum;
                                blockSig.blockNum = block_num;

                                Node.inventoryCache.setProcessedFlag(InventoryItemTypes.blockSignature, InventoryItemSignature.getHash(blockSig.recipientPubKeyOrAddress.addressNoChecksum, checksum), true);

                                if (PresenceList.getPresenceByAddress(blockSig.recipientPubKeyOrAddress) == null)
                                {
                                    Logging.info("Received signature for block {0} whose signer isn't in the PL", block_num);
                                    continue;
                                }
                                if (Node.blockProcessor.addSignatureToBlock(blockSig, endpoint))
                                {
                                    if (Node.isMasterNode())
                                    {
                                        broadcastBlockSignature(blockSig, block_num, checksum, endpoint);
                                    }
                                }else
                                {
                                    Logging.info("SignaturesChunk2 error adding sig");
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