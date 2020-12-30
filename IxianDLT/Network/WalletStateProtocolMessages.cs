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
using IXICore.Meta;
using IXICore.Network;
using IXICore.Utils;
using System;
using System.IO;
using System.Numerics;

namespace DLT
{
    namespace Network
    {
        class WalletStateProtocolMessages
        {
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
                        writer.WriteIxiVarInt(chunk.blockNum);
                        writer.WriteIxiVarInt(chunk.chunkNum);
                        writer.WriteIxiVarInt(chunk.wallets.Length);
                        foreach (Wallet w in chunk.wallets)
                        {
                            writer.WriteIxiVarInt(w.id.Length);
                            writer.Write(w.id);
                            byte[] balance_bytes = w.balance.getAmount().ToByteArray();
                            writer.WriteIxiVarInt(balance_bytes.Length);
                            writer.Write(balance_bytes);

                            if (w.data != null)
                            {
                                writer.WriteIxiVarInt(w.data.Length);
                                writer.Write(w.data);
                            }
                            else
                            {
                                writer.WriteIxiVarInt((int)0);
                            }

                            if (w.publicKey != null)
                            {
                                writer.WriteIxiVarInt(w.publicKey.Length);
                                writer.Write(w.publicKey);
                            }
                            else
                            {
                                writer.WriteIxiVarInt((int)0);
                            }
                        }
#if TRACE_MEMSTREAM_SIZES
                        Logging.info(String.Format("NetworkProtocol::sendWalletStateChunk: {0}", m.Length));
#endif

                        endpoint.sendData(ProtocolMessageCode.walletStateChunk, m.ToArray());
                    }
                }
            }

            public static void handleGetBalance(byte[] data, RemoteEndpoint endpoint)
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
            public static void handleGetBalance2(byte[] data, RemoteEndpoint endpoint)
            {
                using (MemoryStream m = new MemoryStream(data))
                {
                    using (BinaryReader reader = new BinaryReader(m))
                    {
                        int addrLen = (int)reader.ReadIxiVarUInt();
                        byte[] address = reader.ReadBytes(addrLen);

                        // Retrieve the latest balance
                        IxiNumber balance = Node.walletState.getWalletBalance(address);

                        // Return the balance for the matching address
                        using (MemoryStream mw = new MemoryStream())
                        {
                            using (BinaryWriter writerw = new BinaryWriter(mw))
                            {
                                // Send the address
                                writerw.WriteIxiVarInt(address.Length);
                                writerw.Write(address);
                                // Send the balance
                                byte[] balance_bytes = balance.getAmount().ToByteArray();
                                writerw.WriteIxiVarInt(balance_bytes.Length);
                                writerw.Write(balance_bytes);

                                Block tmp_block = IxianHandler.getLastBlock();

                                // Send the block height for this balance
                                writerw.WriteIxiVarInt(tmp_block.blockNum);
                                // Send the block checksum for this balance
                                writerw.WriteIxiVarInt(tmp_block.blockChecksum.Length);
                                writerw.Write(tmp_block.blockChecksum);

#if TRACE_MEMSTREAM_SIZES
                                                Logging.info(String.Format("NetworkProtocol::parseProtocolMessage: {0}", mw.Length));
#endif

                                endpoint.sendData(ProtocolMessageCode.balance2, mw.ToArray());
                            }
                        }
                    }
                }
            }

            static public void handleWalletStateChunk(byte[] data, RemoteEndpoint endpoint)
            {
                using (MemoryStream m = new MemoryStream(data))
                {
                    using (BinaryReader reader = new BinaryReader(m))
                    {
                        ulong block_num = reader.ReadIxiVarUInt();
                        int chunk_num = (int)reader.ReadIxiVarUInt();
                        int num_wallets = (int)reader.ReadIxiVarUInt();
                        if (num_wallets > CoreConfig.walletStateChunkSplit)
                        {
                            Logging.error(String.Format("Received {0} wallets in a chunk. ( > {1}).",
                                num_wallets, CoreConfig.walletStateChunkSplit));
                            return;
                        }
                        Wallet[] wallets = new Wallet[num_wallets];
                        for (int i = 0; i < num_wallets; i++)
                        {
                            int w_idLen = (int)reader.ReadIxiVarUInt();
                            byte[] w_id = reader.ReadBytes(w_idLen);

                            int w_balanceLen = (int)reader.ReadIxiVarUInt();
                            byte[] w_balance_bytes = reader.ReadBytes(w_balanceLen);
                            IxiNumber w_balance = new IxiNumber(new BigInteger(w_balance_bytes));

                            wallets[i] = new Wallet(w_id, w_balance);

                            int w_dataLen = (int)reader.ReadIxiVarUInt();
                            if (w_dataLen > 0)
                            {
                                byte[] w_data = reader.ReadBytes(w_dataLen);
                                wallets[i].data = w_data;
                            }

                            int w_publickeyLen = (int)reader.ReadIxiVarUInt();
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

            static public void handleSyncWalletState(byte[] data, RemoteEndpoint endpoint)
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
                        writer.WriteIxiVarInt(walletstate_version);
                        writer.WriteIxiVarInt(walletstate_block);
                        writer.WriteIxiVarInt(walletstate_count);
#if TRACE_MEMSTREAM_SIZES
                                        Logging.info(String.Format("NetworkProtocol::parseProtocolMessage2: {0}", m.Length));
#endif

                        endpoint.sendData(ProtocolMessageCode.walletState, m.ToArray());
                    }
                }
            }

            static public void handleWalletState(byte[] data, RemoteEndpoint endpoint)
            {
                using (MemoryStream m = new MemoryStream(data))
                {
                    using (BinaryReader reader = new BinaryReader(m))
                    {
                        ulong walletstate_block = 0;
                        ulong walletstate_count = 0;
                        int walletstate_version = 0;
                        try
                        {
                            walletstate_version = (int)reader.ReadIxiVarUInt();
                            walletstate_block = reader.ReadIxiVarUInt();
                            walletstate_count = reader.ReadIxiVarUInt();
                        }
                        catch (Exception e)
                        {
                            Logging.warn("Error while receiving the WalletState header: {0}.", e.Message);
                            return;
                        }
                        Node.blockSync.onWalletStateHeader(walletstate_version, walletstate_block, (long)walletstate_count);
                    }
                }
            }

            static public void handleGetWalletStateChunk(byte[] data, RemoteEndpoint endpoint)
            {
                using (MemoryStream m = new MemoryStream(data))
                {
                    using (BinaryReader reader = new BinaryReader(m))
                    {
                        int chunk_num = (int)reader.ReadIxiVarUInt();
                        Node.blockSync.onRequestWalletChunk(chunk_num, endpoint);
                    }
                }
            }
        }
    }
}