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

namespace DLT
{
    namespace Network
    {
        class PresenceProtocolMessages
        {
            public static void broadcastGetKeepAlives(List<InventoryItemKeepAlive> ka_list, RemoteEndpoint endpoint)
            {
                int ka_count = ka_list.Count;
                int max_ka_per_chunk = CoreConfig.maximumKeepAlivesPerChunk;
                for (int i = 0; i < ka_count;)
                {
                    using (MemoryStream mOut = new MemoryStream(max_ka_per_chunk * 570))
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
                            writer.WriteIxiVarInt(next_ka_count);

                            for (int j = 0; j < next_ka_count && i < ka_count; j++)
                            {
                                InventoryItemKeepAlive ka = ka_list[i];
                                i++;

                                if (ka == null)
                                {
                                    break;
                                }

                                long rollback_len = mOut.Length;

                                writer.WriteIxiVarInt(ka.address.Length);
                                writer.Write(ka.address);

                                writer.WriteIxiVarInt(ka.deviceId.Length);
                                writer.Write(ka.deviceId);

                                if (mOut.Length > CoreConfig.maxMessageSize)
                                {
                                    mOut.SetLength(rollback_len);
                                    i--;
                                    break;
                                }
                            }
                        }
                        endpoint.sendData(ProtocolMessageCode.getKeepAlives, mOut.ToArray(), null);
                    }
                }
            }

            public static void handleGetKeepAlives(byte[] data, RemoteEndpoint endpoint)
            {
                using (MemoryStream m = new MemoryStream(data))
                {
                    using (BinaryReader reader = new BinaryReader(m))
                    {
                        int ka_count = (int)reader.ReadIxiVarUInt();

                        int max_ka_per_chunk = CoreConfig.maximumKeepAlivesPerChunk;

                        for (int i = 0; i < ka_count;)
                        {
                            using (MemoryStream mOut = new MemoryStream(max_ka_per_chunk * 570))
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
                                    writer.WriteIxiVarInt(next_ka_count);

                                    for (int j = 0; j < next_ka_count && i < ka_count; j++)
                                    {
                                        i++;

                                        long in_rollback_pos = reader.BaseStream.Position;
                                        long out_rollback_len = mOut.Length;

                                        if(m.Position == m.Length)
                                        {
                                            break;
                                        }

                                        int address_len = (int)reader.ReadIxiVarUInt();
                                        byte[] address = reader.ReadBytes(address_len);

                                        int device_len = (int)reader.ReadIxiVarUInt();
                                        byte[] device = reader.ReadBytes(device_len);

                                        Presence p = PresenceList.getPresenceByAddress(address);
                                        if (p == null)
                                        {
                                            Logging.info("I don't have presence: " + Base58Check.Base58CheckEncoding.EncodePlain(address));
                                            continue;
                                        }

                                        PresenceAddress pa = p.addresses.Find(x => x.device.SequenceEqual(device));
                                        if (pa == null)
                                        {
                                            Logging.info("I don't have presence address: " + Base58Check.Base58CheckEncoding.EncodePlain(address));
                                            continue;
                                        }

                                        byte[] ka_bytes = pa.getKeepAliveBytes(address);
                                        byte[] ka_len = IxiVarInt.GetIxiVarIntBytes(ka_bytes.Length);
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

            public static void handleKeepAlivesChunk(byte[] data, RemoteEndpoint endpoint)
            {
                using (MemoryStream m = new MemoryStream(data))
                {
                    using (BinaryReader reader = new BinaryReader(m))
                    {
                        int ka_count = (int)reader.ReadIxiVarUInt();

                        int max_ka_per_chunk = CoreConfig.maximumKeepAlivesPerChunk;
                        if(ka_count > max_ka_per_chunk)
                        {
                            ka_count = max_ka_per_chunk;
                        }

                        for (int i = 0; i < ka_count; i++)
                        {
                            if (m.Position == m.Length)
                            {
                                break;
                            }

                            int ka_len = (int)reader.ReadIxiVarUInt();
                            byte[] ka_bytes = reader.ReadBytes(ka_len);
                            byte[] hash = Crypto.sha512sqTrunc(ka_bytes);

                            Node.inventoryCache.setProcessedFlag(InventoryItemTypes.keepAlive, hash, true);

                            byte[] address;
                            long last_seen;
                            byte[] device_id;
                            bool updated = PresenceList.receiveKeepAlive(ka_bytes, out address, out last_seen, out device_id, endpoint);

                            // If a presence entry was updated, broadcast this message again
                            if (updated)
                            {
                                CoreProtocolMessage.addToInventory(new char[] { 'M', 'H', 'W' }, new InventoryItemKeepAlive(hash, last_seen, address, device_id), endpoint, ProtocolMessageCode.keepAlivePresence, ka_bytes, address);

                                // Send this keepalive message to all subscribed clients
                                CoreProtocolMessage.broadcastEventDataMessage(NetworkEvents.Type.keepAlive, address, ProtocolMessageCode.keepAlivePresence, ka_bytes, address, endpoint);
                            }
                        }
                    }
                }
            }

            public static void handleGetPresence(byte[] data, RemoteEndpoint endpoint)
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

            public static void handleGetPresence2(byte[] data, RemoteEndpoint endpoint)
            {
                using (MemoryStream m = new MemoryStream(data))
                {
                    using (BinaryReader reader = new BinaryReader(m))
                    {
                        int walletLen = (int)reader.ReadIxiVarUInt();
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

            public static void handleKeepAlivePresence(byte[] data, RemoteEndpoint endpoint)
            {
                byte[] address = null;
                long last_seen = 0;
                byte[] device_id = null;

                byte[] hash = Crypto.sha512sqTrunc(data);

                Node.inventoryCache.setProcessedFlag(InventoryItemTypes.keepAlive, hash, true);

                bool updated = PresenceList.receiveKeepAlive(data, out address, out last_seen, out device_id, endpoint);

                // If a presence entry was updated, broadcast this message again
                if (updated)
                {
                    CoreProtocolMessage.addToInventory(new char[] { 'M', 'H', 'W' }, new InventoryItemKeepAlive(hash, last_seen, address, device_id), endpoint, ProtocolMessageCode.keepAlivePresence, data, address);

                    // Send this keepalive message to all subscribed clients
                    CoreProtocolMessage.broadcastEventDataMessage(NetworkEvents.Type.keepAlive, address, ProtocolMessageCode.keepAlivePresence, data, address, endpoint);
                }
            }

            public static void handleGetRandomPresences(byte[] data, RemoteEndpoint endpoint)
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

            public static void handleUpdatePresence(byte[] data, RemoteEndpoint endpoint)
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
        }
    }
}