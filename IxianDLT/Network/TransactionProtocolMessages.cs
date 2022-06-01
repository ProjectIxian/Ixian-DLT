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
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;

namespace DLT
{
    namespace Network
    {
        public class TransactionProtocolMessages
        {
            // Handle the getBlockTransactions message
            // This is called from NetworkProtocol
            public static void handleGetBlockTransactions(ulong blockNum, bool requestAllTransactions, RemoteEndpoint endpoint)
            {
                //Logging.info(String.Format("Received request for transactions in block {0}.", blockNum));

                // Get the requested block and corresponding transactions
                bool applied_block = true;
                Block b = null;
                List<byte[]> txIdArr = null;

                bool haveLock = false;
                if (blockNum == IxianHandler.getLastBlockHeight() + 1)
                {
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
                            applied_block = false;
                            b = tmp;
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

                if(b == null)
                {
                    b = Node.blockChain.getBlock(blockNum, Config.storeFullHistory);
                }

                if(b == null)
                {
                    Logging.warn("Unable to find block #{0} in the chain when getting block transactions!", blockNum);
                    return;
                }

                txIdArr = new List<byte[]>(b.transactions);

                if (txIdArr == null)
                    return;

                int tx_count = txIdArr.Count();

                if (tx_count == 0)
                    return;

                // Go through each chunk
                for (int i = 0; i < tx_count;)
                {
                    using (MemoryStream mOut = new MemoryStream(4096))
                    {
                        int txs_in_chunk = 0;
                        using (BinaryWriter writer = new BinaryWriter(mOut))
                        {
                            // Generate a chunk of transactions
                            for (int j = 0; j < CoreConfig.maximumTransactionsPerChunk && i < tx_count; j++)
                            {
                                if (!requestAllTransactions)
                                {
                                    if (txIdArr[i][0] == 0) // stk
                                    {
                                        i++;
                                        continue;
                                    }
                                }
                                Transaction tx;
                                if(applied_block)
                                {
                                    tx = TransactionPool.getAppliedTransaction(txIdArr[i], blockNum, true);
                                }else
                                {
                                    tx = TransactionPool.getUnappliedTransaction(txIdArr[i]);
                                    if (tx == null)
                                    {
                                        tx = TransactionPool.getAppliedTransaction(txIdArr[i], blockNum, true);
                                        if (tx != null)
                                        {
                                            applied_block = true;
                                        }
                                    }
                                }
                                i++;
                                if (tx != null)
                                {
                                    byte[] txBytes = tx.getBytes();

                                    long rollback_len = mOut.Length;
                                    writer.Write(txBytes.Length);
                                    writer.Write(txBytes);
                                    if (mOut.Length > CoreConfig.maxMessageSize)
                                    {
                                        mOut.SetLength(rollback_len);
                                        i--;
                                        break;
                                    }
                                    txs_in_chunk++;
                                }
                            }

#if TRACE_MEMSTREAM_SIZES
                            Logging.info(String.Format("NetworkProtocol::handleGetBlockTransactions: {0}", mOut.Length));
#endif
                        }
                        if (txs_in_chunk > 0)
                        {
                            // Send a chunk
                            endpoint.sendData(ProtocolMessageCode.blockTransactionsChunk, mOut.ToArray());
                        }
                    }
                }
            }

            // Handle the getBlockTransactions message
            // This is called from NetworkProtocol
            public static void handleGetBlockTransactions2(ulong blockNum, bool requestAllTransactions, RemoteEndpoint endpoint)
            {
                //Logging.info(String.Format("Received request for transactions in block {0}.", blockNum));

                // Get the requested block and corresponding transactions
                bool applied_block = true;
                Block b = null;
                List<byte[]> txIdArr = null;
                if (blockNum == IxianHandler.getLastBlockHeight() + 1)
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
                            applied_block = false;
                            b = tmp;
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

                if (b == null)
                {
                    b = Node.blockChain.getBlock(blockNum, Config.storeFullHistory);
                }

                if (b == null)
                {
                    Logging.warn("Unable to find block #{0} in the chain when getting block transactions2!", blockNum);
                    return;
                }

                txIdArr = new List<byte[]>(b.transactions);

                if (txIdArr == null)
                    return;

                int tx_count = txIdArr.Count();

                if (tx_count == 0)
                    return;

                // Go through each chunk
                for (int i = 0; i < tx_count;)
                {
                    using (MemoryStream mOut = new MemoryStream(4096))
                    {
                        int txs_in_chunk = 0;
                        using (BinaryWriter writer = new BinaryWriter(mOut))
                        {
                            writer.WriteIxiVarInt(tx_count);
                            // Generate a chunk of transactions
                            for (int j = 0; j < CoreConfig.maximumTransactionsPerChunk && i < tx_count; j++)
                            {
                                if (!requestAllTransactions)
                                {
                                    if (txIdArr[i][0] == 0) // stk
                                    {
                                        i++;
                                        continue;
                                    }
                                }
                                Transaction tx;
                                if(applied_block)
                                {
                                    tx = TransactionPool.getAppliedTransaction(txIdArr[i], blockNum, true);
                                }else
                                {
                                    tx = TransactionPool.getUnappliedTransaction(txIdArr[i]);
                                    if (tx == null)
                                    {
                                        tx = TransactionPool.getAppliedTransaction(txIdArr[i], blockNum, true);
                                        if (tx != null)
                                        {
                                            applied_block = true;
                                        }
                                    }
                                }
                                i++;
                                if (tx != null)
                                {
                                    byte[] txBytes = tx.getBytes();

                                    long rollback_len = mOut.Length;
                                    writer.WriteIxiVarInt(txBytes.Length);
                                    writer.Write(txBytes);
                                    if (mOut.Length > CoreConfig.maxMessageSize)
                                    {
                                        mOut.SetLength(rollback_len);
                                        i--;
                                        break;
                                    }
                                    txs_in_chunk++;
                                }
                            }

#if TRACE_MEMSTREAM_SIZES
                            Logging.info(String.Format("NetworkProtocol::handleGetBlockTransactions: {0}", mOut.Length));
#endif
                        }
                        if (txs_in_chunk > 0)
                        {
                            // Send a chunk
                            endpoint.sendData(ProtocolMessageCode.transactionsChunk, mOut.ToArray(), null);
                        }
                    }
                }
            }

            // Handle the getBlockTransactions message
            // This is called from NetworkProtocol
            public static void handleGetBlockTransactions3(ulong blockNum, bool requestAllTransactions, RemoteEndpoint endpoint)
            {
                //Logging.info(String.Format("Received request for transactions in block {0}.", blockNum));

                // Get the requested block and corresponding transactions
                bool applied_block = true;
                Block b = null;
                List<byte[]> txIdArr = null;
                if (blockNum == IxianHandler.getLastBlockHeight() + 1)
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
                            applied_block = false;
                            b = tmp;
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

                if (b == null)
                {
                    b = Node.blockChain.getBlock(blockNum, Config.storeFullHistory);
                }

                if(b == null)
                {
                    Logging.warn("Unable to find block #{0} in the chain when getting block transactions3!", blockNum);
                    return;
                }

                txIdArr = new List<byte[]>(b.transactions);

                if (txIdArr == null)
                    return;

                int tx_count = txIdArr.Count();

                if (tx_count == 0)
                    return;

                long msg_id = -(long)blockNum;

                // Go through each chunk
                for (int i = 0; i < tx_count;)
                {
                    using (MemoryStream mOut = new MemoryStream(4096))
                    {
                        int txs_in_chunk = 0;
                        using (BinaryWriter writer = new BinaryWriter(mOut))
                        {
                            writer.WriteIxiVarInt(msg_id);
                            writer.WriteIxiVarInt(tx_count);
                            // Generate a chunk of transactions
                            for (int j = 0; j < CoreConfig.maximumTransactionsPerChunk && i < tx_count; j++)
                            {
                                if (!requestAllTransactions)
                                {
                                    if (txIdArr[i][0] == 0) // stk
                                    {
                                        i++;
                                        continue;
                                    }
                                }
                                Transaction tx;
                                if (applied_block)
                                {
                                    tx = TransactionPool.getAppliedTransaction(txIdArr[i], blockNum, true);
                                }
                                else
                                {
                                    tx = TransactionPool.getUnappliedTransaction(txIdArr[i]);
                                    if (tx == null)
                                    {
                                        tx = TransactionPool.getAppliedTransaction(txIdArr[i], blockNum, true);
                                        if (tx != null)
                                        {
                                            applied_block = true;
                                        }
                                    }
                                }
                                i++;
                                if (tx != null)
                                {
                                    // TODO Omega - force v7 structure and send as transactionsChunk3
                                    byte[] txBytes = tx.getBytes(true, false);

                                    long rollback_len = mOut.Length;
                                    writer.WriteIxiVarInt(txBytes.Length);
                                    writer.Write(txBytes);
                                    if (mOut.Length > CoreConfig.maxMessageSize)
                                    {
                                        mOut.SetLength(rollback_len);
                                        i--;
                                        break;
                                    }
                                    txs_in_chunk++;
                                }
                            }

#if TRACE_MEMSTREAM_SIZES
                            Logging.info(String.Format("NetworkProtocol::handleGetBlockTransactions: {0}", mOut.Length));
#endif
                        }
                        if (txs_in_chunk > 0)
                        {
                            // Send a chunk
                            endpoint.sendData(ProtocolMessageCode.transactionsChunk2, mOut.ToArray(), null, 0, MessagePriority.high);
                        }
                    }
                }
            }

            public static void broadcastGetTransactions(List<byte[]> tx_list, long msg_id, RemoteEndpoint endpoint)
            {
                int tx_count = tx_list.Count;
                int max_tx_per_chunk = CoreConfig.maximumTransactionsPerChunk;
                for (int i = 0; i < tx_count;)
                {
                    using (MemoryStream mOut = new MemoryStream(max_tx_per_chunk * 570))
                    {
                        using (BinaryWriter writer = new BinaryWriter(mOut))
                        {
                            int next_tx_count = tx_count - i;
                            if (next_tx_count > max_tx_per_chunk)
                            {
                                next_tx_count = max_tx_per_chunk;
                            }
                            writer.WriteIxiVarInt(msg_id);
                            writer.WriteIxiVarInt(next_tx_count);

                            for (int j = 0; j < next_tx_count && i < tx_count; j++)
                            {
                                long rollback_len = mOut.Length;

                                writer.WriteIxiVarInt(tx_list[i].Length);
                                writer.Write(tx_list[i]);

                                i++;

                                if (mOut.Length > CoreConfig.maxMessageSize)
                                {
                                    mOut.SetLength(rollback_len);
                                    i--;
                                    break;
                                }
                            }
                        }
                        MessagePriority priority = msg_id > 0 ? MessagePriority.high : MessagePriority.auto;
                        if(endpoint == null)
                        {
                            CoreProtocolMessage.broadcastProtocolMessageToSingleRandomNode(new char[] { 'M', 'H' }, ProtocolMessageCode.getTransactions2, mOut.ToArray(), 0, null);
                        }else
                        {
                            endpoint.sendData(ProtocolMessageCode.getTransactions2, mOut.ToArray(), null, msg_id, priority);
                        }
                    }
                }
            }


            public static void handleGetTransactions2(byte[] data, RemoteEndpoint endpoint)
            {
                using (MemoryStream m = new MemoryStream(data))
                {
                    using (BinaryReader reader = new BinaryReader(m))
                    {
                        int msg_id = (int)reader.ReadIxiVarInt();
                        int tx_count = (int)reader.ReadIxiVarUInt();

                        int max_tx_per_chunk = CoreConfig.maximumTransactionsPerChunk;
                        if (tx_count > max_tx_per_chunk)
                        {
                            tx_count = max_tx_per_chunk;
                        }

                        for (int i = 0; i < tx_count;)
                        {
                            using (MemoryStream mOut = new MemoryStream(max_tx_per_chunk * 570))
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
                                    writer.WriteIxiVarInt(msg_id);
                                    writer.WriteIxiVarInt(next_tx_count);

                                    for (int j = 0; j < next_tx_count && i < tx_count;  j++)
                                    {
                                        long in_rollback_pos = reader.BaseStream.Position;
                                        long out_rollback_len = mOut.Length;

                                        i++;

                                        if (m.Position == m.Length)
                                        {
                                            break;
                                        }

                                        int txid_len = (int)reader.ReadIxiVarUInt();
                                        byte[] txid = reader.ReadBytes(txid_len);

                                        Transaction tx = TransactionPool.getUnappliedTransaction(txid);
                                        if (tx == null)
                                        {
                                            tx = TransactionPool.getAppliedTransaction(txid);
                                            if (tx == null)
                                            {
                                                Logging.warn("I do not have txid '{0}.", Transaction.getTxIdString(txid)); // convert to string
                                                continue;
                                            }
                                        }

                                        // TODO Omega - force v7 structure and send as transactionsChunk3
                                        byte[] tx_bytes = tx.getBytes(true, false);
                                        byte[] tx_len = IxiVarInt.GetIxiVarIntBytes(tx_bytes.Length);
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
                                endpoint.sendData(ProtocolMessageCode.transactionsChunk2, mOut.ToArray(), null, 0, MessagePriority.high);
                            }
                        }
                    }
                }
            }
            public static void handleTransactionsChunk(byte[] data, RemoteEndpoint endpoint)
            {
                using (MemoryStream m = new MemoryStream(data))
                {
                    using (BinaryReader reader = new BinaryReader(m))
                    {
                        int tx_count = (int)reader.ReadIxiVarUInt();

                        int max_tx_per_chunk = CoreConfig.maximumTransactionsPerChunk;
                        if (tx_count > max_tx_per_chunk)
                        {
                            tx_count = max_tx_per_chunk;
                        }

                        var sw = new System.Diagnostics.Stopwatch();
                        sw.Start();
                        int processedTxCount = 0;
                        int totalTxCount = 0;
                        for (int i = 0; i < tx_count; i++)
                        {
                            if (m.Position == m.Length)
                            {
                                break;
                            }

                            int tx_len = (int)reader.ReadIxiVarUInt();
                            byte[] tx_bytes = reader.ReadBytes(tx_len);

                            Transaction tx = new Transaction(tx_bytes);

                            totalTxCount++;
                            if (tx.type == (int)Transaction.Type.StakingReward && !Node.blockSync.synchronizing)
                            {
                                continue;
                            }
                            if (TransactionPool.addTransaction(tx, false, endpoint))
                            {
                                processedTxCount++;
                            }
                        }
                        sw.Stop();
                        TimeSpan elapsed = sw.Elapsed;
                        Logging.info("Processed {0}/{1} txs in {2}ms", processedTxCount, totalTxCount, elapsed.TotalMilliseconds);
                    }
                }
            }

            public static void handleTransactionsChunk2(byte[] data, RemoteEndpoint endpoint)
            {
                using (MemoryStream m = new MemoryStream(data))
                {
                    using (BinaryReader reader = new BinaryReader(m))
                    {
                        long msg_id = reader.ReadIxiVarInt();

                        if (msg_id < 0)
                        {
                            ulong blockNum = (ulong)-msg_id;
                            if(!Node.blockProcessor.isBlockWaitingForTransactions(blockNum))
                            {
                                return;
                            }
                        }

                        int tx_count = (int)reader.ReadIxiVarUInt();

                        int max_tx_per_chunk = CoreConfig.maximumTransactionsPerChunk;
                        if (tx_count > max_tx_per_chunk)
                        {
                            tx_count = max_tx_per_chunk;
                        }

                        var sw = new System.Diagnostics.Stopwatch();
                        sw.Start();
                        int processedTxCount = 0;
                        int totalTxCount = 0;
                        for (int i = 0; i < tx_count; i++)
                        {
                            if (m.Position == m.Length)
                            {
                                break;
                            }

                            int tx_len = (int)reader.ReadIxiVarUInt();
                            byte[] tx_bytes = reader.ReadBytes(tx_len);

                            Transaction tx = new Transaction(tx_bytes, false, false);

                            totalTxCount++;
                            if (tx.type == (int)Transaction.Type.StakingReward && !Node.blockSync.synchronizing)
                            {
                                continue;
                            }
                            if (TransactionPool.addTransaction(tx, false, endpoint))
                            {
                                processedTxCount++;
                            }
                        }
                        sw.Stop();
                        TimeSpan elapsed = sw.Elapsed;
                        Logging.info("Processed {0}/{1} txs in {2}ms", processedTxCount, totalTxCount, elapsed.TotalMilliseconds);
                    }
                }
            }

            public static void handleTransactionsChunk3(byte[] data, RemoteEndpoint endpoint)
            {
                using (MemoryStream m = new MemoryStream(data))
                {
                    using (BinaryReader reader = new BinaryReader(m))
                    {
                        long msg_id = reader.ReadIxiVarInt();

                        if (msg_id < 0)
                        {
                            ulong blockNum = (ulong)-msg_id;
                            if (!Node.blockProcessor.isBlockWaitingForTransactions(blockNum))
                            {
                                return;
                            }
                        }

                        int tx_count = (int)reader.ReadIxiVarUInt();

                        int max_tx_per_chunk = CoreConfig.maximumTransactionsPerChunk;
                        if (tx_count > max_tx_per_chunk)
                        {
                            tx_count = max_tx_per_chunk;
                        }

                        var sw = new System.Diagnostics.Stopwatch();
                        sw.Start();
                        int processedTxCount = 0;
                        int totalTxCount = 0;
                        for (int i = 0; i < tx_count; i++)
                        {
                            if (m.Position == m.Length)
                            {
                                break;
                            }

                            int tx_len = (int)reader.ReadIxiVarUInt();
                            byte[] tx_bytes = reader.ReadBytes(tx_len);

                            Transaction tx = new Transaction(tx_bytes, false, true);

                            totalTxCount++;
                            if (tx.type == (int)Transaction.Type.StakingReward && !Node.blockSync.synchronizing)
                            {
                                continue;
                            }
                            if (TransactionPool.addTransaction(tx, false, endpoint))
                            {
                                processedTxCount++;
                            }
                        }
                        sw.Stop();
                        TimeSpan elapsed = sw.Elapsed;
                        Logging.info("Processed {0}/{1} txs in {2}ms", processedTxCount, totalTxCount, elapsed.TotalMilliseconds);
                    }
                }
            }

            // Handle the getUnappliedTransactions message
            // This is called from NetworkProtocol
            public static void handleGetUnappliedTransactions(byte[] data, RemoteEndpoint endpoint)
            {
                Transaction[] txIdArr = TransactionPool.getUnappliedTransactions();
                int tx_count = txIdArr.Count();

                if (tx_count == 0)
                    return;

                // Go through each chunk
                for (int i = 0; i < tx_count;)
                {
                    using (MemoryStream mOut = new MemoryStream())
                    {
                        using (BinaryWriter writer = new BinaryWriter(mOut))
                        {
                            // Generate a chunk of transactions
                            for (int j = 0; j < CoreConfig.maximumTransactionsPerChunk && i < tx_count; j++)
                            {
                                byte[] txBytes = txIdArr[i].getBytes();

                                i++;

                                long rollback_len = mOut.Length;

                                writer.Write(txBytes.Length);
                                writer.Write(txBytes);

                                if (mOut.Length > CoreConfig.maxMessageSize)
                                {
                                    mOut.SetLength(rollback_len);
                                    i--;
                                    break;
                                }
                            }

                            // Send a chunk
#if TRACE_MEMSTREAM_SIZES
                        Logging.info(String.Format("NetworkProtocol::handleGetUnappliedTransactions: {0}", mOut.Length));
#endif
                        }
                        endpoint.sendData(ProtocolMessageCode.blockTransactionsChunk, mOut.ToArray());
                    }
                }
            }

            public static void handleGetTransaction3(byte[] data, RemoteEndpoint endpoint)
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
                        int txid_len = (int)reader.ReadIxiVarUInt();
                        byte[] txid = reader.ReadBytes(txid_len);
                        ulong block_num = reader.ReadIxiVarUInt();

                        Transaction transaction = null;

                        // Check for a transaction corresponding to this id
                        if (block_num == 0 || block_num == Node.blockChain.getLastBlockNum() + 1)
                        {
                            transaction = TransactionPool.getUnappliedTransaction(txid);
                        }
                        if (transaction == null)
                        {
                            transaction = TransactionPool.getAppliedTransaction(txid, block_num, true);
                        }

                        if (transaction == null)
                        {
                            Logging.warn("I do not have txid '{0}.", Transaction.getTxIdString(txid));
                            return;
                        }

                        Logging.info("Sending transaction {0} - {1} - {2}.", transaction.getTxIdString(), Crypto.hashToString(transaction.checksum), transaction.amount);
                        // TODO Omega replace the uncommented line with commented out line after upgrade
                        //endpoint.sendData(ProtocolMessageCode.transactionData2, transaction.getBytes(true, true));
                        endpoint.sendData(ProtocolMessageCode.transactionData, transaction.getBytes(true, false));
                    }
                }
            }

            public static void handleTransactionData(byte[] data, RemoteEndpoint endpoint)
            {
                /*if(TransactionPool.checkSocketTransactionLimits(socket) == true)
                {
                    // Throttled, ignore this transaction
                    return;
                }*/

                Transaction transaction = new Transaction(data);
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
                }
                else
                {
                    no_broadcast = true;
                }

                // Add the transaction to the pool
                TransactionPool.addTransaction(transaction, no_broadcast, endpoint);
            }

            public static void handleTransactionData2(byte[] data, RemoteEndpoint endpoint)
            {
                /*if(TransactionPool.checkSocketTransactionLimits(socket) == true)
                {
                    // Throttled, ignore this transaction
                    return;
                }*/

                Transaction transaction = new Transaction(data, false, true);
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
                }
                else
                {
                    no_broadcast = true;
                }

                // Add the transaction to the pool
                TransactionPool.addTransaction(transaction, no_broadcast, endpoint);
            }
        }
    }
}