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

                        case ProtocolMessageCode.getTransaction3:
                            TransactionProtocolMessages.handleGetTransaction3(data, endpoint);
                            break;

                        case ProtocolMessageCode.transactionData:
                            TransactionProtocolMessages.handleTransactionData(data, endpoint);
                            break;

                        case ProtocolMessageCode.bye:
                            CoreProtocolMessage.processBye(data, endpoint);
                            break;

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
                            PresenceProtocolMessages.handleGetPresence(data, endpoint);
                            break;

                        case ProtocolMessageCode.getPresence2:
                            PresenceProtocolMessages.handleGetPresence2(data, endpoint);
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

                        case ProtocolMessageCode.getNextSuperBlock:
                            BlockProtocolMessages.handleGetNextSuperBlock(data, endpoint);
                            break;

                        case ProtocolMessageCode.getBlockHeaders:
                            BlockProtocolMessages.handleGetBlockHeaders(data, endpoint);
                            break;

                        case ProtocolMessageCode.getPIT:
                            BlockProtocolMessages.handleGetPIT(data, endpoint);
                            break;

                        case ProtocolMessageCode.inventory2:
                            handleInventory2(data, endpoint);
                            break;

                        case ProtocolMessageCode.getSignatures:
                            SignatureProtocolMessages.handleGetSignatures(data, endpoint);
                            break;

                        case ProtocolMessageCode.getSignatures2:
                            SignatureProtocolMessages.handleGetSignatures2(data, endpoint);
                            break;

                        case ProtocolMessageCode.signaturesChunk:
                            SignatureProtocolMessages.handleSignaturesChunk(data, endpoint);
                            break;

                        case ProtocolMessageCode.signaturesChunk2:
                            SignatureProtocolMessages.handleSignaturesChunk2(data, endpoint);
                            break;

                        case ProtocolMessageCode.getTransactions2:
                            TransactionProtocolMessages.handleGetTransactions2(data, endpoint);
                            break;

                        case ProtocolMessageCode.transactionsChunk:
                            TransactionProtocolMessages.handleTransactionsChunk(data, endpoint);
                            break;

                        case ProtocolMessageCode.transactionsChunk2:
                            TransactionProtocolMessages.handleTransactionsChunk2(data, endpoint);
                            break;

                        case ProtocolMessageCode.getBlockHeaders2:
                            BlockProtocolMessages.handleGetBlockHeaders2(data, endpoint);
                            break;

                        case ProtocolMessageCode.getPIT2:
                            BlockProtocolMessages.handleGetPIT2(data, endpoint);
                            break;

                        case ProtocolMessageCode.getBlock2:
                            BlockProtocolMessages.handleGetBlock2(data, endpoint);
                            break;

                        case ProtocolMessageCode.getBlock3:
                            BlockProtocolMessages.handleGetBlock3(data, endpoint);
                            break;

                        case ProtocolMessageCode.getBalance2:
                            WalletStateProtocolMessages.handleGetBalance2(data, endpoint);
                            break;

                        case ProtocolMessageCode.getBlockSignatures2:
                            {
                                using (MemoryStream m = new MemoryStream(data))
                                {
                                    using (BinaryReader reader = new BinaryReader(m))
                                    {
                                        ulong block_num = reader.ReadIxiVarUInt();

                                        int checksum_len = (int)reader.ReadIxiVarUInt();
                                        byte[] checksum = reader.ReadBytes(checksum_len);

                                        SignatureProtocolMessages.handleGetBlockSignatures2(block_num, checksum, endpoint);
                                    }
                                }
                            }
                            break;

                        case ProtocolMessageCode.blockSignature2:
                            SignatureProtocolMessages.handleBlockSignature2(data, endpoint);
                            break;

                        case ProtocolMessageCode.getSignerPow:
                            PresenceProtocolMessages.handleGetSignerPow(data, endpoint);
                            break;

                        case ProtocolMessageCode.signerPow:
                            PresenceProtocolMessages.handleSignerPow(data, endpoint);
                            break;

                        default:
                            break;
                    }

                }
                catch (Exception e)
                {
                    Logging.error("Error parsing network message. Details: {0}", e.ToString());
                }
            }

            public static void handleHello(byte[] data, RemoteEndpoint endpoint)
            {
                using (MemoryStream m = new MemoryStream(data))
                {
                    using (BinaryReader reader = new BinaryReader(m))
                    {
                        CoreProtocolMessage.processHelloMessageV6(endpoint, reader);
                    }
                }
            }

            public static void handleHelloData(byte[] data, RemoteEndpoint endpoint)
            {
                using (MemoryStream m = new MemoryStream(data))
                {
                    using (BinaryReader reader = new BinaryReader(m))
                    {
                        if(!CoreProtocolMessage.processHelloMessageV6(endpoint, reader))
                        {
                            return;
                        }
                        char node_type = endpoint.presenceAddress.type;
                        if (node_type != 'M' && node_type != 'H')
                        {
                            CoreProtocolMessage.sendBye(endpoint, ProtocolByeCode.expectingMaster, string.Format("Expecting master node."), "", true);
                            return;
                        }

                        ulong last_block_num = reader.ReadIxiVarUInt();

                        int bcLen = (int)reader.ReadIxiVarUInt();
                        byte[] block_checksum = reader.ReadBytes(bcLen);

                        endpoint.blockHeight = last_block_num;

                        if (Node.checkCurrentBlockDeprecation(last_block_num) == false)
                        {
                            CoreProtocolMessage.sendBye(endpoint, ProtocolByeCode.deprecated, string.Format("This node deprecated or will deprecate on block {0}, your block height is {1}, disconnecting.", Config.nodeDeprecationBlock, last_block_num), last_block_num.ToString(), true);
                            return;
                        }

                        int block_version = (int)reader.ReadIxiVarUInt();

                        Node.blockProcessor.highestNetworkBlockNum = Node.blockProcessor.determineHighestNetworkBlockNum();

                        ulong highest_block_height = IxianHandler.getHighestKnownNetworkBlockHeight();
                        if (last_block_num + 15 < highest_block_height)
                        {
                            CoreProtocolMessage.sendBye(endpoint, ProtocolByeCode.tooFarBehind, string.Format("Your node is too far behind, your block height is {0}, highest network block height is {1}.", last_block_num, highest_block_height), highest_block_height.ToString(), true);
                            return;
                        }

                        // Process the hello data
                        Node.blockSync.onHelloDataReceived(last_block_num, block_checksum, block_version, null, 0, 0, true);
                        endpoint.helloReceived = true;
                        NetworkClientManager.recalculateLocalTimeDifference();
                    }
                }
            }

            static void handleInventory2(byte[] data, RemoteEndpoint endpoint)
            {
                using (MemoryStream m = new MemoryStream(data))
                {
                    using (BinaryReader reader = new BinaryReader(m))
                    {
                        ulong item_count = reader.ReadIxiVarUInt();
                        if (item_count > (ulong)CoreConfig.maxInventoryItems)
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
                            if (item.type == InventoryItemTypes.transaction)
                            {
                                PendingTransactions.increaseReceivedCount(item.hash, endpoint.presence.wallet);
                            }
                            PendingInventoryItem pii = Node.inventoryCache.add(item, endpoint);

                            // First update endpoint blockheights
                            switch (item.type)
                            {
                                case InventoryItemTypes.blockSignature:
                                    var iis = (InventoryItemSignature)item;
                                    if (iis.blockNum > endpoint.blockHeight)
                                    {
                                        endpoint.blockHeight = iis.blockNum;
                                    }
                                    break;

                                case InventoryItemTypes.block:
                                    var iib = ((InventoryItemBlock)item);
                                    if (iib.blockNum > endpoint.blockHeight)
                                    {
                                        endpoint.blockHeight = iib.blockNum;
                                    }
                                    break;
                            }

                            if (!pii.processed && pii.lastRequested == 0)
                            {
                                // first time we're seeing this inventory item
                                switch (item.type)
                                {
                                    case InventoryItemTypes.signerPow:
                                        CoreProtocolMessage.broadcastGetSignerPow(((InventoryItemSignerPow)pii.item).address, endpoint);
                                        pii.lastRequested = Clock.getTimestamp();
                                        break;

                                    case InventoryItemTypes.keepAlive:
                                        ka_list.Add((InventoryItemKeepAlive)item);
                                        pii.lastRequested = Clock.getTimestamp();
                                        break;

                                    case InventoryItemTypes.transaction:
                                        tx_list.Add(item.hash);
                                        pii.lastRequested = Clock.getTimestamp();
                                        break;

                                    case InventoryItemTypes.blockSignature:
                                        var iis = (InventoryItemSignature)item;
                                        if(iis.blockNum < last_block_height - 5)
                                        {
                                            continue;
                                        }
                                        if (iis.blockNum > last_block_height)
                                        {
                                            request_next_block = true;
                                            continue;
                                        }
                                        if (!sig_lists.ContainsKey(iis.blockNum))
                                        {
                                            sig_lists.Add(iis.blockNum, new List<InventoryItemSignature>());
                                        }
                                        sig_lists[iis.blockNum].Add(iis);
                                        pii.lastRequested = Clock.getTimestamp();
                                        break;

                                    case InventoryItemTypes.block:
                                        var iib = ((InventoryItemBlock)item);
                                        if (iib.blockNum <= last_block_height)
                                        {
                                            Node.inventoryCache.processInventoryItem(pii);
                                        }
                                        else
                                        {
                                            pii.lastRequested = Clock.getTimestamp();
                                            request_next_block = true;
                                        }
                                        break;

                                    default:
                                        Node.inventoryCache.processInventoryItem(pii);
                                        break;
                                }
                            }
                        }
                        PresenceProtocolMessages.broadcastGetKeepAlives(ka_list, endpoint);
                        if (Node.blockSync.synchronizing)
                        {
                            return;
                        }
                        TransactionProtocolMessages.broadcastGetTransactions(tx_list, 0, endpoint);
                        if (request_next_block)
                        {
                            byte include_tx = 2;
                            if (Node.isMasterNode())
                            {
                                include_tx = 0;
                            }
                            BlockProtocolMessages.broadcastGetBlock(last_block_height + 1, null, endpoint, include_tx, true);
                            Node.blockProcessor.highestNetworkBlockNum = Node.blockProcessor.determineHighestNetworkBlockNum();
                        }
                        foreach (var sig_list in sig_lists)
                        {
                            SignatureProtocolMessages.broadcastGetSignatures(sig_list.Key, sig_list.Value, endpoint);
                        }
                    }
                }
            }
        }
    }
}