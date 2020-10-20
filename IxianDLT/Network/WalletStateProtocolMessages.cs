using DLT.Meta;
using IXICore;
using IXICore.Meta;
using IXICore.Network;
using System;
using System.IO;

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

            static public void handleWalletStateChunk(byte[] data, RemoteEndpoint endpoint)
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

            static public void handleWalletState(byte[] data, RemoteEndpoint endpoint)
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

            static public void handleGetWalletStateChunk(byte[] data, RemoteEndpoint endpoint)
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
        }
    }
}