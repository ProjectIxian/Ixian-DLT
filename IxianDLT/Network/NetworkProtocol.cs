using DLT.Meta;
using IXICore;
using IXICore.Inventory;
using IXICore.Meta;
using IXICore.Network;
using IXICore.Utils;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace DLT
{
    namespace Network
    {
        public class ProtocolMessage
        {
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
                            handleHello(data, endpoint);
                            break;

                        case ProtocolMessageCode.helloData:
                            handleHelloData(data, endpoint);
                            break;

                        case ProtocolMessageCode.getBlock:
                            BlockProtocolMessages.handleGetBlock(data, endpoint);
                            break;

                        case ProtocolMessageCode.getBalance:
                            WalletStateProtocolMessages.handleGetBalance(data, endpoint);
                            break;

                        case ProtocolMessageCode.getTransaction:
                            TransactionProtocolMessages.handleGetTransaction(data, endpoint);
                            break;

                        case ProtocolMessageCode.newTransaction:
                        case ProtocolMessageCode.transactionData:
                            TransactionProtocolMessages.handleTransactionData(data, endpoint);
                            break;

                        case ProtocolMessageCode.bye:
                            CoreProtocolMessage.processBye(data, endpoint);
                            break;

                        case ProtocolMessageCode.newBlock:
                        case ProtocolMessageCode.blockData:
                            BlockProtocolMessages.handleBlockData(data, endpoint);
                            break;

                        case ProtocolMessageCode.syncWalletState:
                            WalletStateProtocolMessages.handleSyncWalletState(data, endpoint);
                            break;

                        case ProtocolMessageCode.walletState:
                            WalletStateProtocolMessages.handleWalletState(data, endpoint);
                            break;

                        case ProtocolMessageCode.getWalletStateChunk:
                            WalletStateProtocolMessages.handleGetWalletStateChunk(data, endpoint);
                            break;

                        case ProtocolMessageCode.walletStateChunk:
                            WalletStateProtocolMessages.handleWalletStateChunk(data, endpoint);
                            break;

                        case ProtocolMessageCode.updatePresence:
                            PresenceProtocolMessages.handleUpdatePresence(data, endpoint);
                            break;

                        case ProtocolMessageCode.keepAlivePresence:
                            PresenceProtocolMessages.handleKeepAlivePresence(data, endpoint);
                            break;

                        case ProtocolMessageCode.getPresence:
                            PresenceProtocolMessages.handleGetPresences(data, endpoint);
                            break;

                        case ProtocolMessageCode.getKeepAlives:
                            PresenceProtocolMessages.handleGetKeepAlives(data, endpoint);
                            break;

                        case ProtocolMessageCode.keepAlivesChunk:
                            PresenceProtocolMessages.handleKeepAlivesChunk(data, endpoint);
                            break;

                        // return 10 random presences of the selected type
                        case ProtocolMessageCode.getRandomPresences:
                            PresenceProtocolMessages.handleGetRandomPresences(data, endpoint);
                            break;

                        case ProtocolMessageCode.getBlockTransactions:
                            {
                                using (MemoryStream m = new MemoryStream(data))
                                {
                                    using (BinaryReader reader = new BinaryReader(m))
                                    {
                                        ulong blockNum = reader.ReadUInt64();
                                        bool requestAllTransactions = reader.ReadBoolean();

                                        TransactionProtocolMessages.handleGetBlockTransactions(blockNum, requestAllTransactions, endpoint);
                                    }
                                }
                            }
                            break;

                        case ProtocolMessageCode.getUnappliedTransactions:
                            TransactionProtocolMessages.handleGetUnappliedTransactions(data, endpoint);
                            break;

                        case ProtocolMessageCode.blockTransactionsChunk:
                            BlockProtocolMessages.handleBlockTransactionsChunk(data, endpoint);
                            break;

                        case ProtocolMessageCode.attachEvent:
                            NetworkEvents.handleAttachEventMessage(data, endpoint);
                            break;

                        case ProtocolMessageCode.detachEvent:
                            NetworkEvents.handleDetachEventMessage(data, endpoint);
                            break;

                        case ProtocolMessageCode.blockSignature:
                            SignatureProtocolMessages.handleBlockSignature(data, endpoint);
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

                                        SignatureProtocolMessages.handleGetBlockSignatures(block_num, checksum, endpoint);
                                    }
                                }
                            }
                            break;

                        case ProtocolMessageCode.blockSignatures:
                            SignatureProtocolMessages.handleSigfreezedBlockSignatures(data, endpoint);
                            break;

                        case ProtocolMessageCode.getNextSuperBlock:
                            BlockProtocolMessages.handleGetNextSuperBlock(data, endpoint);
                            break;

                        case ProtocolMessageCode.getBlockHeaders:
                            BlockProtocolMessages.handleGetBlockHeaders(data, endpoint);
                            break;

                        case ProtocolMessageCode.getPIT:
                            BlockProtocolMessages.handleGetPIT(data, endpoint);
                            break;

                        case ProtocolMessageCode.inventory:
                            handleInventory(data, endpoint);
                            break;

                        case ProtocolMessageCode.getSignatures:
                            SignatureProtocolMessages.handleGetSignatures(data, endpoint);
                            break;

                        case ProtocolMessageCode.signaturesChunk:
                            SignatureProtocolMessages.handleSignaturesChunk(data, endpoint);
                            break;

                        case ProtocolMessageCode.getTransactions:
                            TransactionProtocolMessages.handleGetTransactions(data, endpoint);
                            break;

                        case ProtocolMessageCode.transactionsChunk:
                            TransactionProtocolMessages.handleTransactionsChunk(data, endpoint);
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

            public static void handleHello(byte[] data, RemoteEndpoint endpoint)
            {
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
            }

            public static void handleHelloData(byte[] data, RemoteEndpoint endpoint)
            {
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
            }

            static void handleInventory(byte[] data, RemoteEndpoint endpoint)
            {
                using (MemoryStream m = new MemoryStream(data))
                {
                    using (BinaryReader reader = new BinaryReader(m))
                    {
                        ulong item_count = reader.ReadIxiVarUInt();
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
                            ulong len = reader.ReadIxiVarUInt();
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
                        TransactionProtocolMessages.broadcastGetTransactions(tx_list, endpoint);
                        PresenceProtocolMessages.broadcastGetKeepAlives(ka_list, endpoint);
                        foreach(var sig_list in sig_lists)
                        {
                            SignatureProtocolMessages.broadcastGetSignatures(sig_list.Key, sig_list.Value, endpoint);
                        }
                        if (request_next_block)
                        {
                            byte include_tx = 2;
                            if (Node.isMasterNode())
                            {
                                include_tx = 0;
                            }
                            BlockProtocolMessages.broadcastGetBlock(last_block_height + 1, null, endpoint, include_tx, true);
                        }
                    }
                }
            }
        }
    }
}