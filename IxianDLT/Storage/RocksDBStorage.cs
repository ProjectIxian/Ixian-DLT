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
using IXICore.Utils;
using RocksDbSharp;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace DLT
{
    namespace Storage
    {
        class RocksDBInternal
        {
            // Internal representation
            class _storage_Block
            {
                public ulong blockNum { get; set; }
                public byte[] blockChecksum { get; set; }
                public byte[] lastBlockChecksum { get; set; }
                public ulong lastSuperblockNum { get; set; }
                public byte[] superBlockSegments { get; set; }
                public byte[] lastSuperblockChecksum { get; set; }
                public byte[] walletStateChecksum { get; set; }
                public byte[] sigFreezeChecksum { get; set; }
                public ulong difficulty { get; set; }
                public byte[] powField { get; set; }
                public byte[][] signatures { get; set; }
                public byte[][] transactions { get; set; }
                public long timestamp { get; set; }
                public int version { get; set; }
                //
                public bool compactedSigs { get; set; }
                //
                public byte[] blockProposer { get; set; }
                public _storage_Block() { }
                public _storage_Block(Block from_block)
                {
                    blockNum = from_block.blockNum;
                    blockChecksum = from_block.blockChecksum;
                    lastBlockChecksum = from_block.lastBlockChecksum;
                    lastSuperblockNum = from_block.lastSuperBlockNum;
                    if (lastSuperblockChecksum != null)
                    {
                        //this is a superblock
                        MemoryStream ms = new MemoryStream();
                        using (BinaryWriter bw = new BinaryWriter(ms, Encoding.UTF8, true))
                        {
                            foreach (var entry in from_block.superBlockSegments)
                            {
                                bw.Write(BitConverter.GetBytes(entry.Value.blockNum));
                                bw.Write(BitConverter.GetBytes(entry.Value.blockChecksum.Length));
                                bw.Write(entry.Value.blockChecksum);
                            }
                        }
                        superBlockSegments = ms.ToArray();
                    }
                    lastSuperblockChecksum = from_block.lastSuperBlockChecksum;
                    walletStateChecksum = from_block.walletStateChecksum;
                    sigFreezeChecksum = from_block.signatureFreezeChecksum;
                    difficulty = from_block.difficulty;
                    powField = from_block.powField;

                    List<BlockSignature> source_signatures = null;
                    if(from_block.frozenSignatures != null)
                    {
                        source_signatures = from_block.frozenSignatures;
                    } else
                    {
                        source_signatures = from_block.signatures;
                    }

                    signatures = new byte[source_signatures.Count][];
                    int i = 0;
                    foreach (BlockSignature sig in source_signatures)
                    {
                        signatures[i] = sig.getBytesForBlock();
                        i += 1;
                    }

                    transactions = from_block.transactions.ToArray();
                    timestamp = from_block.timestamp;
                    version = from_block.version;
                    compactedSigs = from_block.compactedSigs;
                    blockProposer = from_block.blockProposer;
                }
                public Block asBlock()
                {
                    Block b = new Block();
                    b.blockNum = blockNum;
                    b.blockChecksum = blockChecksum;
                    b.lastBlockChecksum = lastBlockChecksum;
                    b.lastSuperBlockNum = lastSuperblockNum;
                    b.lastSuperBlockChecksum = lastSuperblockChecksum;
                    if (superBlockSegments != null)
                    {
                        for (int i = 0; i < superBlockSegments.Length;)
                        {
                            ulong seg_block_num = BitConverter.ToUInt64(superBlockSegments, i);
                            i += 8;
                            int seg_bc_len = BitConverter.ToInt32(superBlockSegments, i);
                            i += 4;
                            byte[] seg_bc = new byte[seg_bc_len];
                            Array.Copy(superBlockSegments, i, seg_bc, 0, seg_bc_len);
                            i += seg_bc_len;

                            b.superBlockSegments.Add(seg_block_num, new SuperBlockSegment(seg_block_num, seg_bc));
                        }
                    }
                    b.walletStateChecksum = walletStateChecksum;
                    b.signatureFreezeChecksum = sigFreezeChecksum;
                    b.difficulty = difficulty;
                    b.powField = powField;
                    b.signatures = new List<BlockSignature>();
                    foreach (var sig in signatures)
                    {
                        b.signatures.Add(new BlockSignature(sig, false));
                    }
                    if (transactions != null)
                    {
                        foreach (byte[] txid in transactions)
                        {
                            b.addTransaction(txid);
                        }
                    }
                    b.timestamp = timestamp;
                    b.version = version;
                    b.compactedSigs = compactedSigs;
                    b.blockProposer = blockProposer;
                    // special flag:
                    b.fromLocalStorage = true;
                    return b;
                }
                public _storage_Block(byte[] from_bytes)
                {
                    using (MemoryStream ms = new MemoryStream(from_bytes))
                    {
                        using (BinaryReader br = new BinaryReader(ms))
                        {
                            int count = 0;
                            blockNum = br.ReadUInt64();

                            count = br.ReadInt32();
                            if (count > 0) { blockChecksum = br.ReadBytes(count); } else { blockChecksum = null; }

                            count = br.ReadInt32();
                            if (count > 0) { lastBlockChecksum = br.ReadBytes(count); } else { lastBlockChecksum = null; }

                            lastSuperblockNum = br.ReadUInt64();

                            count = br.ReadInt32();
                            if (count > 0) { lastSuperblockChecksum = br.ReadBytes(count); } else { lastSuperblockChecksum = null; }

                            count = br.ReadInt32();
                            if (count > 0) { walletStateChecksum = br.ReadBytes(count); } else { walletStateChecksum = null; }

                            count = br.ReadInt32();
                            if (count > 0) { sigFreezeChecksum = br.ReadBytes(count); } else { sigFreezeChecksum = null; }

                            difficulty = br.ReadUInt64();

                            count = br.ReadInt32();
                            if (count > 0) { powField = br.ReadBytes(count); } else { powField = null; }

                            count = br.ReadInt32();
                            if (count > 0)
                            {
                                // signature is [sig][address]
                                signatures = new byte[count][];
                                for (int i = 0; i < count; i++)
                                {
                                    int s_len = br.ReadInt32();
                                    if (s_len > 0) { signatures[i] = br.ReadBytes(s_len); } else { signatures[i] = null; }
                                }
                            }
                            else { signatures = null; }

                            count = br.ReadInt32();
                            if (count > 0)
                            {
                                transactions = new byte[count][];
                                for (int i = 0; i < count; i++)
                                {
                                    int txid_len = br.ReadInt32();
                                    transactions[i] = br.ReadBytes(txid_len);
                                }
                            }
                            else { transactions = null; }

                            timestamp = br.ReadInt64();
                            version = br.ReadInt32();

                            count = br.ReadInt32();
                            if(count > 0)
                            {
                                superBlockSegments = br.ReadBytes(count);
                            }

                            compactedSigs = br.ReadBoolean();

                            int bp_len = br.ReadInt32();
                            if(bp_len > 0)
                            {
                                blockProposer = br.ReadBytes(bp_len);
                            }
                        }
                    }
                }
                public byte[] asBytes()
                {
                    using (MemoryStream ms = new MemoryStream())
                    {
                        using (BinaryWriter wr = new BinaryWriter(ms))
                        {
                            wr.Write(blockNum);

                            if (blockChecksum != null)
                            {
                                wr.Write(blockChecksum.Length);
                                wr.Write(blockChecksum);
                            }
                            else
                            {
                                wr.Write(0);
                            }

                            if (lastBlockChecksum != null)
                            {
                                wr.Write(lastBlockChecksum.Length);
                                wr.Write(lastBlockChecksum);
                            }
                            else
                            {
                                wr.Write(0);
                            }

                            wr.Write(lastSuperblockNum);

                            if (lastSuperblockChecksum != null)
                            {
                                wr.Write(lastSuperblockChecksum.Length);
                                wr.Write(lastSuperblockChecksum);
                            }
                            else
                            {
                                wr.Write(0);
                            }

                            if (walletStateChecksum != null)
                            {
                                wr.Write(walletStateChecksum.Length);
                                wr.Write(walletStateChecksum);
                            }
                            else
                            {
                                wr.Write(0);
                            }

                            if (sigFreezeChecksum != null)
                            {
                                wr.Write(sigFreezeChecksum.Length);
                                wr.Write(sigFreezeChecksum);
                            }
                            else
                            {
                                wr.Write(0);
                            }

                            wr.Write(difficulty);

                            if (powField != null)
                            {
                                wr.Write(powField.Length);
                                wr.Write(powField);
                            }
                            else
                            {
                                wr.Write(0);
                            }

                            if (signatures != null)
                            {
                                wr.Write(signatures.Length);
                                foreach (var s in signatures)
                                {
                                    // signature is [sig][address]
                                    wr.Write(s.Length);
                                    wr.Write(s);
                                }
                            }
                            else
                            {
                                wr.Write(0);
                            }

                            if (transactions != null)
                            {
                                wr.Write(transactions.Length);
                                foreach (var txid in transactions)
                                {
                                    wr.Write(txid.Length);
                                    wr.Write(txid);
                                }
                            }
                            else
                            {
                                wr.Write(0);
                            }

                            wr.Write(timestamp);
                            wr.Write(version);
                            // superblock segments
                            if(superBlockSegments != null)
                            {
                                wr.Write(superBlockSegments.Length);
                                wr.Write(superBlockSegments);
                            } else
                            {
                                wr.Write(0);
                            }
                            wr.Write(compactedSigs);

                            if(blockProposer != null)
                            {
                                wr.Write(blockProposer.Length);
                                wr.Write(blockProposer);
                            }
                            else
                            {
                                wr.Write(0);
                            }
                        }
                        return ms.ToArray();
                    }
                }
            }

            class _storage_Transaction
            {
                public byte[] id { get; set; }
                public int type { get; set; }
                public byte[] amount { get; set; }
                public byte[] fee { get; set; }
                public byte[][][] toList { get; set; }
                public byte[][][] fromList { get; set; }
                public byte[] dataChecksum { get; set; }
                public byte[] data { get; set; }
                public ulong blockHeight { get; set; }
                public int nonce { get; set; }
                public long timestamp { get; set; }
                public byte[] checksum { get; set; }
                public byte[] signature { get; set; }
                public byte[] pubKey { get; set; }
                public ulong applied { get; set; }
                public int version { get; set; }

                public _storage_Transaction() { }
                public _storage_Transaction(Transaction from_tx)
                {
                    id = from_tx.id;
                    type = from_tx.type;
                    amount = from_tx.amount.getAmount().ToByteArray();
                    fee = from_tx.fee.getAmount().ToByteArray();
                    toList = new byte[from_tx.toList.Count][][];
                    int i = 0;
                    foreach (var to in from_tx.toList)
                    {
                        toList[i] = new byte[2][];
                        toList[i][0] = to.Key;
                        toList[i][1] = to.Value.getAmount().ToByteArray();
                        i++;
                    }
                    fromList = new byte[from_tx.fromList.Count][][];
                    i = 0;
                    foreach (var from in from_tx.fromList)
                    {
                        fromList[i] = new byte[2][];
                        fromList[i][0] = from.Key;
                        fromList[i][1] = from.Value.getAmount().ToByteArray();
                        i++;
                    }
                    dataChecksum = from_tx.dataChecksum;
                    data = from_tx.data;
                    blockHeight = from_tx.blockHeight;
                    nonce = from_tx.nonce;
                    timestamp = from_tx.timeStamp;
                    checksum = from_tx.checksum;
                    signature = from_tx.signature;
                    pubKey = from_tx.pubKey;
                    applied = from_tx.applied;
                    version = from_tx.version;
                }

                public Transaction asTransaction()
                {
                    Transaction tx = new Transaction(type, dataChecksum, data);
                    tx.id = id;
                    tx.type = type;
                    tx.amount = new IxiNumber(new System.Numerics.BigInteger(amount));
                    tx.fee = new IxiNumber(new System.Numerics.BigInteger(fee));
                    foreach (var to in toList)
                    {
                        tx.toList.AddOrReplace(to[0], new IxiNumber(new System.Numerics.BigInteger(to[1])));
                    }
                    foreach (var from in fromList)
                    {
                        tx.fromList.AddOrReplace(from[0], new IxiNumber(new System.Numerics.BigInteger(from[1])));
                    }
                    tx.blockHeight = blockHeight;
                    tx.nonce = nonce;
                    tx.timeStamp = timestamp;
                    tx.checksum = checksum;
                    tx.signature = signature;
                    tx.pubKey = pubKey;
                    tx.applied = applied;
                    tx.version = version;
                    // special flag
                    tx.fromLocalStorage = true;
                    return tx;
                }

                public _storage_Transaction(byte[] from_bytes)
                {
                    using (MemoryStream ms = new MemoryStream(from_bytes))
                    {
                        using (BinaryReader br = new BinaryReader(ms))
                        {
                            int count = 0;
                            int id_len = br.ReadInt32();
                            id = br.ReadBytes(id_len);

                            type = br.ReadInt32();

                            count = br.ReadInt32();
                            if (count > 0) { amount = br.ReadBytes(count); } else { amount = null; }

                            count = br.ReadInt32();
                            if (count > 0) { fee = br.ReadBytes(count); } else { fee = null; }

                            count = br.ReadInt32();
                            if (count > 0)
                            {
                                toList = new byte[count][][];
                                for (int i = 0; i < toList.Length; i++)
                                {
                                    toList[i] = new byte[2][];
                                    int a_len = br.ReadInt32();
                                    if (a_len > 0) { toList[i][0] = br.ReadBytes(a_len); } else { toList[i][0] = null; }
                                    int b_len = br.ReadInt32();
                                    if (b_len > 0) { toList[i][1] = br.ReadBytes(b_len); } else { toList[i][1] = null; }
                                }
                            }
                            else { toList = null; }

                            count = br.ReadInt32();
                            if (count > 0)
                            {
                                fromList = new byte[count][][];
                                for (int i = 0; i < fromList.Length; i++)
                                {
                                    fromList[i] = new byte[2][];
                                    int a_len = br.ReadInt32();
                                    if (a_len > 0) { fromList[i][0] = br.ReadBytes(a_len); } else { fromList[i][0] = null; }
                                    int b_len = br.ReadInt32();
                                    if (b_len > 0) { fromList[i][1] = br.ReadBytes(b_len); } else { fromList[i][1] = null; }
                                }
                            }
                            else { fromList = null; }

                            count = br.ReadInt32();
                            if (count > 0) { dataChecksum = br.ReadBytes(count); } else { dataChecksum = null; }

                            count = br.ReadInt32();
                            if (count > 0) { data = br.ReadBytes(count); } else { data = null; }

                            blockHeight = br.ReadUInt64();
                            nonce = br.ReadInt32();
                            timestamp = br.ReadInt64();

                            count = br.ReadInt32();
                            if (count > 0) { checksum = br.ReadBytes(count); } else { checksum = null; }

                            count = br.ReadInt32();
                            if (count > 0) { signature = br.ReadBytes(count); } else { signature = null; }

                            count = br.ReadInt32();
                            if (count > 0) { pubKey = br.ReadBytes(count); } else { pubKey = null; }

                            applied = br.ReadUInt64();
                            version = br.ReadInt32();
                        }
                    }
                }

                public byte[] asBytes()
                {
                    using (MemoryStream ms = new MemoryStream())
                    {
                        using (BinaryWriter wr = new BinaryWriter(ms))
                        {
                            wr.Write(id.Length);
                            wr.Write(id);
                            wr.Write(type);

                            if (amount != null)
                            {
                                wr.Write(amount.Length);
                                wr.Write(amount);
                            }
                            else
                            {
                                wr.Write(0);
                            }

                            if (fee != null)
                            {
                                wr.Write(fee.Length);
                                wr.Write(fee);
                            }
                            else
                            {
                                wr.Write(0);
                            }

                            if (toList != null)
                            {
                                wr.Write(toList.Length);
                                for (int i = 0; i < toList.Length; i++)
                                {
                                    wr.Write(toList[i][0].Length);
                                    wr.Write(toList[i][0]);
                                    wr.Write(toList[i][1].Length);
                                    wr.Write(toList[i][1]);
                                }
                            }
                            else
                            {
                                wr.Write(0);
                            }


                            if (fromList != null)
                            {
                                wr.Write(fromList.Length);
                                for (int i = 0; i < fromList.Length; i++)
                                {
                                    wr.Write(fromList[i][0].Length);
                                    wr.Write(fromList[i][0]);
                                    wr.Write(fromList[i][1].Length);
                                    wr.Write(fromList[i][1]);
                                }
                            }
                            else
                            {
                                wr.Write(0);
                            }

                            if (dataChecksum != null)
                            {
                                wr.Write(dataChecksum.Length);
                                wr.Write(dataChecksum);
                            }
                            else
                            {
                                wr.Write(0);
                            }

                            if (data != null)
                            {
                                wr.Write(data.Length);
                                wr.Write(data);
                            }
                            else
                            {
                                wr.Write(0);
                            }

                            wr.Write(blockHeight);
                            wr.Write(nonce);
                            wr.Write(timestamp);

                            if (checksum != null)
                            {
                                wr.Write(checksum.Length);
                                wr.Write(checksum);
                            }
                            else
                            {
                                wr.Write(0);
                            }

                            if (signature != null)
                            {
                                wr.Write(signature.Length);
                                wr.Write(signature);
                            }
                            else
                            {
                                wr.Write(0);
                            }

                            if (pubKey != null)
                            {
                                wr.Write(pubKey.Length);
                                wr.Write(pubKey);
                            }
                            else
                            {
                                wr.Write(0);
                            }

                            wr.Write(applied);
                            wr.Write(version);
                        }
                        return ms.ToArray();
                    }
                }
            }

            public class _applied_tx_idx_entry
            {
                public ulong tx_original_bh;
                public byte[] tx_id;

                public _applied_tx_idx_entry(ulong orig_bh, byte[] txid)
                {
                    tx_original_bh = orig_bh;
                    tx_id = txid;
                }

                public _applied_tx_idx_entry(byte[] from_bytes)
                {
                    using (MemoryStream ms = new MemoryStream(from_bytes))
                    {
                        using (BinaryReader br = new BinaryReader(ms))
                        {
                            tx_original_bh = br.ReadUInt64();
                            int txid_len = br.ReadInt32();
                            tx_id = br.ReadBytes(txid_len);
                        }
                    }
                }

                public byte[] asBytes()
                {
                    using (MemoryStream ms = new MemoryStream())
                    {
                        using (BinaryWriter bw = new BinaryWriter(ms))
                        {
                            bw.Write(tx_original_bh);
                            bw.Write(tx_id);
                        }
                        return ms.ToArray();
                    }
                }
            }

            class _storage_Index
            {
                private Dictionary<byte[], List<byte[]>> indexMap = new Dictionary<byte[], List<byte[]>>();
                public ColumnFamilyHandle rocksIndexHandle;

                public _storage_Index(string cf_name, RocksDb db)
                {
                    rocksIndexHandle = db.GetColumnFamily(cf_name);
                    loadDBIndex(db);
                }

                public void addIndexEntry(byte[] key, byte[] e)
                {
                    if (indexMap.ContainsKey(key))
                    {
                        if (!indexMap[key].Exists(x => x.SequenceEqual(e)))
                        {
                            indexMap[key].Add(e);
                            indexMap[key][0][0] = (byte)1;
                        }
                    }
                    else
                    {
                        indexMap.Add(key, new List<byte[]>());
                        indexMap[key].Add(new byte[] { 1 }); // diry marker
                    }
                }

                public void delIndexEntry(byte[] key, byte[] e)
                {
                    if (indexMap.ContainsKey(key))
                    {
                        var i = indexMap[key].FindIndex(x => x.SequenceEqual(e));
                        if (i > -1)
                        {
                            indexMap[key].RemoveAt(i);
                            indexMap[key][0][0] = (byte)1;
                        }
                    }
                }

                public IEnumerable<byte[]> getEntriesForKey(byte[] key)
                {
                    if (indexMap.ContainsKey(key))
                    {
                        return indexMap[key].Skip(1);
                    }
                    return Enumerable.Empty<byte[]>();
                }

                public IEnumerable<byte[]> getAllKeys()
                {
                    return indexMap.Keys;
                }

                public void updateDBIndex(RocksDb db)
                {
                    List<byte[]> to_del = new List<byte[]>();
                    foreach (var kv in indexMap)
                    {
                        if (kv.Value[0][0] == 1)
                        {
                            if (kv.Value.Count == 1)
                            {
                                db.Remove(kv.Key, rocksIndexHandle);
                                to_del.Add(kv.Key);
                            }
                            else
                            {
                                db.Put(kv.Key, asBytes(kv.Key));
                                kv.Value[0][0] = (byte)0;
                            }
                        }
                    }
                    foreach (var d in to_del)
                    {
                        indexMap.Remove(d);
                    }
                }

                public void loadDBIndex(RocksDb db)
                {
                    indexMap = new Dictionary<byte[], List<byte[]>>();
                    var iter = db.NewIterator(rocksIndexHandle);
                    iter.SeekToFirst();
                    while (iter.Valid())
                    {
                        fromBytes(iter.Key(), iter.Value());
                        iter.Next();
                    }
                    iter.Dispose();
                }

                private byte[] asBytes(byte[] key)
                {
                    using (MemoryStream ms = new MemoryStream())
                    {
                        using (BinaryWriter bw = new BinaryWriter(ms))
                        {
                            bw.Write(indexMap[key].Count);
                            foreach (var e in indexMap[key])
                            {
                                if (e != null)
                                {
                                    bw.Write(e.Length);
                                    bw.Write(e);
                                }
                                else
                                {
                                    bw.Write(0);
                                }
                            }
                        }
                        return ms.ToArray();
                    }
                }

                private void fromBytes(byte[] key, byte[] bytes)
                {
                    using (MemoryStream ms = new MemoryStream(bytes))
                    {
                        using (BinaryReader br = new BinaryReader(ms))
                        {
                            int count = br.ReadInt32();
                            if (count > 0)
                            {
                                if (!indexMap.ContainsKey(key))
                                {
                                    indexMap.Add(key, new List<byte[]>());
                                }
                                else
                                {
                                    indexMap[key] = new List<byte[]>();
                                }
                                indexMap[key].Add(new byte[] { 0 });
                                for (int i = 0; i < count; i++)
                                {
                                    int len = br.ReadInt32();
                                    if (len > 0)
                                    {
                                        indexMap[key].Add(br.ReadBytes(len));
                                    }
                                }
                            }
                        }
                    }
                }
            }

            public string dbPath { get; private set; }
            private DbOptions rocksOptions;
            private RocksDb database = null;
            // global column families
            private ColumnFamilyHandle rocksCFBlocks;
            private ColumnFamilyHandle rocksCFTransactions;
            private ColumnFamilyHandle rocksCFMeta;
            // index column families
            // block
            private _storage_Index idxBlocksChecksum;
            private _storage_Index idxBlocksLastSBChecksum;
            // transaction
            private _storage_Index idxTXType;
            private _storage_Index idxTXFrom;
            private _storage_Index idxTXTo;
            private _storage_Index idxTXBlockHeight;
            private _storage_Index idxTXTimestamp;
            private _storage_Index idxTXApplied;
            private readonly object rockLock = new object();

            public ulong minBlockNumber { get; private set; }
            public ulong maxBlockNumber { get; private set; }
            public int dbVersion { get; private set; }
            public bool isOpen
            {
                get
                {
                    return database != null;
                }
            }
            public DateTime lastUsedTime { get; private set; }
            public DateTime lastMaintenance { get; private set; }
            public readonly double maintenanceInterval = 120.0;
            // Caches (shared with other rocksDb
            private Cache blockCache = null;
            private Cache compressedBlockCache = null;
            private ulong writeBufferSize = 0;


            public RocksDBInternal(string db_path, Cache block_cache = null, Cache compressed_block_cache = null, ulong write_buffer_size = 0)
            {
                dbPath = db_path;
                minBlockNumber = 0;
                maxBlockNumber = 0;
                dbVersion = 0;
                blockCache = block_cache;
                compressedBlockCache = compressed_block_cache;
                writeBufferSize = write_buffer_size;
            }

            public void openDatabase()
            {
                if (database != null)
                {
                    throw new Exception(String.Format("Rocks Database '{0}' is already open.", dbPath));
                }
                lock (rockLock)
                {
                    rocksOptions = new DbOptions();
                    rocksOptions.SetCreateIfMissing(true);
                    rocksOptions.SetCreateMissingColumnFamilies(true);
                    rocksOptions.SetTargetFileSizeMultiplier(1);
                    rocksOptions.SetLogFileTimeToRoll(604800000000); // about 7 days
                    rocksOptions.SetRecycleLogFileNum(30);
                    if (writeBufferSize > 0)
                    {
                        rocksOptions.SetDbWriteBufferSize(writeBufferSize);
                    }
                    BlockBasedTableOptions bbto = new BlockBasedTableOptions();
                    if(blockCache != null)
                    {
                        bbto.SetBlockCache(blockCache.Handle);
                    }
                    if(compressedBlockCache != null)
                    {
                        bbto.SetBlockCacheCompressed(compressedBlockCache.Handle);
                    }
                    ColumnFamilyOptions cfo = new ColumnFamilyOptions();
                    cfo.SetBlockBasedTableFactory(bbto);
                    cfo.SetCompression(Compression.Snappy);
                    var columnFamilies = new ColumnFamilies(cfo);
                    // default column families
                    columnFamilies.Add("blocks", cfo);
                    columnFamilies.Add("transactions", cfo);
                    columnFamilies.Add("meta", cfo);
                    // index column families
                    columnFamilies.Add("index_block_checksum", cfo);
                    columnFamilies.Add("index_block_last_sb_checksum", cfo);
                    columnFamilies.Add("index_tx_type", cfo);
                    columnFamilies.Add("index_tx_from", cfo);
                    columnFamilies.Add("index_tx_to", cfo);
                    columnFamilies.Add("index_tx_block_height", cfo);
                    columnFamilies.Add("index_tx_timestamp", cfo);
                    columnFamilies.Add("index_tx_applied", cfo);
                    //
                    database = RocksDb.Open(rocksOptions, dbPath, columnFamilies);
                    // initialize column family handles
                    rocksCFBlocks = database.GetColumnFamily("blocks");
                    rocksCFTransactions = database.GetColumnFamily("transactions");
                    rocksCFMeta = database.GetColumnFamily("meta");
                    // initialize indexes - this also loads them in memory
                    idxBlocksChecksum = new _storage_Index("index_block_checksum", database);
                    idxBlocksLastSBChecksum = new _storage_Index("index_block_last_sb_checksum", database);
                    idxTXType = new _storage_Index("index_tx_type", database);
                    idxTXFrom = new _storage_Index("index_tx_from", database);
                    idxTXTo = new _storage_Index("index_tx_to", database);
                    idxTXBlockHeight = new _storage_Index("index_tx_block_height", database);
                    idxTXTimestamp = new _storage_Index("index_tx_timestamp", database);
                    idxTXApplied = new _storage_Index("index_tx_applied", database);

                    // read initial meta values
                    string version_str = database.Get("db_version", rocksCFMeta);
                    if (version_str == null || version_str == "")
                    {
                        dbVersion = 1;
                        database.Put("db_version", dbVersion.ToString(), rocksCFMeta);
                    }
                    else
                    {
                        dbVersion = int.Parse(version_str);
                    }
                    string min_block_str = database.Get("min_block", rocksCFMeta);
                    if (min_block_str == null || min_block_str == "")
                    {
                        minBlockNumber = 0;
                        database.Put("min_block", minBlockNumber.ToString(), rocksCFMeta);
                    }
                    else
                    {
                        minBlockNumber = ulong.Parse(min_block_str);
                    }
                    string max_block_str = database.Get("max_block", rocksCFMeta);
                    if (max_block_str == null || max_block_str == "")
                    {
                        maxBlockNumber = 0;
                        database.Put("max_block", maxBlockNumber.ToString(), rocksCFMeta);
                    }
                    else
                    {
                        maxBlockNumber = ulong.Parse(max_block_str);
                    }
                    Logging.info("RocksDB: Opened Database {0}: Blocks {1} - {2}, version {3}", dbPath, minBlockNumber, maxBlockNumber, dbVersion);
                    Logging.info("RocksDB: Stats: {0}", database.GetProperty("rocksdb.stats"));
                    lastUsedTime = DateTime.Now;
                    lastMaintenance = DateTime.Now;
                }
            }

            public void logStats()
            {
                if (database != null)
                {
                    if (blockCache != null)
                    {
                        Logging.info("RocksDB: Common Cache Bytes Used: {0}", blockCache.GetUsage());
                    }
                    if (compressedBlockCache != null)
                    {
                        Logging.info("RocksDB: Common Compressed Cache Bytes Used: {0}", compressedBlockCache.GetUsage());
                    }
                    Logging.info("RocksDB: Stats [rocksdb.block-cache-usage] '{0}': {1}", dbPath, database.GetProperty("rocksdb.block-cache-usage"));
                    Logging.info("RocksDB: Stats for '{0}': {1}", dbPath, database.GetProperty("rocksdb.dbstats"));

                }
            }

            public void maintenance()
            {
                if(database != null)
                {
                    if((DateTime.Now - lastMaintenance).TotalSeconds > maintenanceInterval)
                    {
                        Logging.info("RocksDB: Performing regular maintenance (compaction) on database '{0}'.", dbPath);
                        try
                        {
                            lock (rockLock)
                            {
                                var i = database.NewIterator(rocksCFBlocks);
                                i = i.SeekToFirst();
                                var first_key = i.Key();
                                i = i.SeekToLast();
                                var last_key = i.Key();
                                database.CompactRange(first_key, last_key, rocksCFBlocks);
                                i.Dispose();
                                //
                                i = database.NewIterator(rocksCFTransactions);
                                i = i.SeekToFirst();
                                first_key = i.Key();
                                i = i.SeekToLast();
                                last_key = i.Key();
                                database.CompactRange(first_key, last_key, rocksCFTransactions);
                                i.Dispose();
                                //
                                i = database.NewIterator(idxBlocksChecksum.rocksIndexHandle);
                                i = i.SeekToFirst();
                                first_key = i.Key();
                                i = i.SeekToLast();
                                last_key = i.Key();
                                database.CompactRange(first_key, last_key, idxBlocksChecksum.rocksIndexHandle);
                                i.Dispose();
                                //
                                i = database.NewIterator(idxBlocksLastSBChecksum.rocksIndexHandle);
                                i = i.SeekToFirst();
                                first_key = i.Key();
                                i = i.SeekToLast();
                                last_key = i.Key();
                                database.CompactRange(first_key, last_key, idxBlocksLastSBChecksum.rocksIndexHandle);
                                i.Dispose();
                                //
                                i = database.NewIterator(idxTXType.rocksIndexHandle);
                                i = i.SeekToFirst();
                                first_key = i.Key();
                                i = i.SeekToLast();
                                last_key = i.Key();
                                database.CompactRange(first_key, last_key, idxTXType.rocksIndexHandle);
                                i.Dispose();
                                //
                                i = database.NewIterator(idxTXFrom.rocksIndexHandle);
                                i = i.SeekToFirst();
                                first_key = i.Key();
                                i = i.SeekToLast();
                                last_key = i.Key();
                                database.CompactRange(first_key, last_key, idxTXFrom.rocksIndexHandle);
                                i.Dispose();
                                //
                                i = database.NewIterator(idxTXTo.rocksIndexHandle);
                                i = i.SeekToFirst();
                                first_key = i.Key();
                                i = i.SeekToLast();
                                last_key = i.Key();
                                database.CompactRange(first_key, last_key, idxTXTo.rocksIndexHandle);
                                i.Dispose();
                                //
                                i = database.NewIterator(idxTXBlockHeight.rocksIndexHandle);
                                i = i.SeekToFirst();
                                first_key = i.Key();
                                i = i.SeekToLast();
                                last_key = i.Key();
                                database.CompactRange(first_key, last_key, idxTXBlockHeight.rocksIndexHandle);
                                i.Dispose();
                                //
                                i = database.NewIterator(idxTXTimestamp.rocksIndexHandle);
                                i = i.SeekToFirst();
                                first_key = i.Key();
                                i = i.SeekToLast();
                                last_key = i.Key();
                                database.CompactRange(first_key, last_key, idxTXTimestamp.rocksIndexHandle);
                                i.Dispose();
                                //
                                i = database.NewIterator(idxTXApplied.rocksIndexHandle);
                                i = i.SeekToFirst();
                                first_key = i.Key();
                                i = i.SeekToLast();
                                last_key = i.Key();
                                database.CompactRange(first_key, last_key, idxTXApplied.rocksIndexHandle);
                                i.Dispose();
                            }
                        }
                        catch(Exception e)
                        {
                            Logging.warn("RocksDB: Error while performing regular maintenance on '{0}': {1}", dbPath, e.Message);
                        }
                        lastMaintenance = DateTime.Now;
                    }
                }
            }

            public void closeDatabase()
            {
                lock (rockLock)
                {
                    if (database == null)
                    {
                        return;
                    }
                    // free all blocks column families
                    rocksCFBlocks = null;
                    rocksCFMeta = null;
                    rocksCFTransactions = null;
                    // free all indexes
                    idxBlocksChecksum = null;
                    idxBlocksLastSBChecksum = null;
                    idxTXType = null;
                    idxTXFrom = null;
                    idxTXTo = null;
                    idxTXBlockHeight = null;
                    idxTXTimestamp = null;
                    idxTXApplied = null;
                    //
                    rocksOptions = null;
                    database.Dispose();
                    database = null;
                }
            }

            private void updateBlockIndexes(_storage_Block sb)
            {
                byte[] block_num_bytes = BitConverter.GetBytes(sb.blockNum);
                idxBlocksChecksum.addIndexEntry(sb.blockChecksum, block_num_bytes);
                idxBlocksChecksum.updateDBIndex(database);
                if (sb.lastSuperblockChecksum != null)
                {
                    idxBlocksLastSBChecksum.addIndexEntry(sb.lastSuperblockChecksum, block_num_bytes);
                    idxBlocksLastSBChecksum.updateDBIndex(database);
                }
                lastUsedTime = DateTime.Now;
            }

            private void updateTXIndexes(_storage_Transaction st)
            {
                byte[] tx_id_bytes = st.id;

                idxTXType.addIndexEntry(BitConverter.GetBytes(st.type), tx_id_bytes);
                idxTXType.updateDBIndex(database);

                foreach (var from in st.fromList)
                {
                    idxTXFrom.addIndexEntry(from[0], tx_id_bytes);
                }
                idxTXFrom.updateDBIndex(database);

                foreach (var to in st.toList)
                {
                    idxTXTo.addIndexEntry(to[0], tx_id_bytes);
                }
                idxTXTo.updateDBIndex(database);

                idxTXBlockHeight.addIndexEntry(BitConverter.GetBytes(st.blockHeight), tx_id_bytes);
                idxTXBlockHeight.updateDBIndex(database);

                idxTXTimestamp.addIndexEntry(BitConverter.GetBytes(st.timestamp), tx_id_bytes);
                idxTXTimestamp.updateDBIndex(database);

                lastUsedTime = DateTime.Now;
            }

            private void updateMinMax(ulong blocknum)
            {
                if (minBlockNumber == 0 || blocknum < minBlockNumber)
                {
                    minBlockNumber = blocknum;
                    database.Put("min_block", minBlockNumber.ToString(), rocksCFMeta);
                }
                if (maxBlockNumber == 0 || blocknum > maxBlockNumber)
                {
                    maxBlockNumber = blocknum;
                    database.Put("max_block", maxBlockNumber.ToString(), rocksCFMeta);
                }

                lastUsedTime = DateTime.Now;
            }

            public bool insertBlock(Block block)
            {
                lock (rockLock)
                {
                    if (database == null)
                    {
                        return false;
                    }
                    var sb = new _storage_Block(block);
                    database.Put(BitConverter.GetBytes(sb.blockNum), sb.asBytes(), rocksCFBlocks);
                    updateBlockIndexes(sb);
                    updateMinMax(sb.blockNum);
                }
                lastUsedTime = DateTime.Now;
                return true;
            }

            public bool insertTransaction(Transaction transaction)
            {
                lock (rockLock)
                {
                    if (database == null)
                    {
                        return false;
                    }
                    var st = new _storage_Transaction(transaction);
                    database.Put(st.id, st.asBytes(), rocksCFTransactions);
                    updateTXIndexes(st);
                }
                lastUsedTime = DateTime.Now;
                return true;
            }

            public bool insertTXApplied(Transaction transaction)
            {
                lock(rockLock)
                {
                    if (database == null)
                    {
                        return false;
                    }
                    idxTXApplied.addIndexEntry(BitConverter.GetBytes(transaction.applied), new _applied_tx_idx_entry(transaction.blockHeight, transaction.id).asBytes());
                    idxTXApplied.updateDBIndex(database);
                    return true;
                }
            }

            private Block getBlockInternal(byte[] block_num_bytes)
            {
                byte[] block_bytes = database.Get(block_num_bytes, rocksCFBlocks);
                lastUsedTime = DateTime.Now;
                if (block_bytes != null)
                {
                    var sb = new _storage_Block(block_bytes);
                    return sb.asBlock();
                }
                return null;
            }

            public Block getBlock(ulong blocknum)
            {
                lock (rockLock)
                {
                    if (database == null)
                    {
                        return null;
                    }
                    if (blocknum < minBlockNumber || blocknum > maxBlockNumber)
                    {
                        return null;
                    }
                    return getBlockInternal(BitConverter.GetBytes(blocknum));
                }
            }

            public Block getBlockByHash(byte[] checksum)
            {
                lock (rockLock)
                {
                    if (database == null)
                    {
                        return null;
                    }
                    lastUsedTime = DateTime.Now;
                    var e = idxBlocksChecksum.getEntriesForKey(checksum);
                    if (e.Any())
                    {
                        return getBlockInternal(e.First());
                    }
                    return null;
                }
            }

            public Block getBlockByLastSBHash(byte[] checksum)
            {
                lock (rockLock)
                {
                    if (database == null)
                    {
                        return null;
                    }
                    lastUsedTime = DateTime.Now;
                    var e = idxBlocksLastSBChecksum.getEntriesForKey(checksum);
                    if (e.Any())
                    {
                        return getBlockInternal(e.First());
                    }
                    return null;
                }
            }

            public IEnumerable<Block> getBlocksByRange(ulong from, ulong to)
            {
                lock (rockLock)
                {
                    var blocks = new List<Block>();
                    lastUsedTime = DateTime.Now;
                    Iterator iter = database.NewIterator(rocksCFBlocks);
                    iter.SeekToFirst();
                    while (iter.Valid())
                    {
                        ulong block_num = BitConverter.ToUInt64(iter.Key(), 0);
                        if (block_num >= from && block_num <= to)
                        {
                            blocks.Add(new _storage_Block(iter.Value()).asBlock());
                        }
                    }
                    iter.Dispose();
                    return blocks;
                }
            }

            private Transaction getTransactionInternal(byte[] txid_bytes)
            {
                lock (rockLock)
                {
                    lastUsedTime = DateTime.Now;
                    var tx_bytes = database.Get(txid_bytes, rocksCFTransactions);
                    if (tx_bytes != null)
                    {
                        return new _storage_Transaction(tx_bytes).asTransaction();
                    }
                    return null;
                }
            }

            public Transaction getTransaction(byte[] txid)
            {
                lock (rockLock)
                {
                    if (database == null)
                    {
                        return null;
                    }
                    return getTransactionInternal(txid);
                }
            }

            public IEnumerable<Transaction> getTransactionsByType(Transaction.Type type)
            {
                lock (rockLock)
                {
                    List<Transaction> txs = new List<Transaction>();
                    if (database == null)
                    {
                        return null;
                    }
                    lastUsedTime = DateTime.Now;
                    foreach (var i in idxTXType.getEntriesForKey(BitConverter.GetBytes((int)type)))
                    {
                        txs.Add(getTransactionInternal(i));
                    }
                    return txs;
                }
            }

            public IEnumerable<Transaction> getTransactionsFromAddress(byte[] from_addr)
            {
                lock (rockLock)
                {
                    List<Transaction> txs = new List<Transaction>();
                    if (database == null)
                    {
                        return null;
                    }
                    lastUsedTime = DateTime.Now;
                    foreach (var i in idxTXFrom.getEntriesForKey(from_addr))
                    {
                        txs.Add(getTransactionInternal(i));
                    }
                    return txs;
                }
            }

            public IEnumerable<Transaction> getTransactionsToAddress(byte[] to_addr)
            {
                lock (rockLock)
                {
                    List<Transaction> txs = new List<Transaction>();
                    if (database == null)
                    {
                        return null;
                    }
                    lastUsedTime = DateTime.Now;
                    foreach (var i in idxTXFrom.getEntriesForKey(to_addr))
                    {
                        txs.Add(getTransactionInternal(i));
                    }
                    return txs;
                }
            }

            // TODO implement tx_type
            public IEnumerable<Transaction> getTransactionsInBlock(ulong block_num, int tx_type = -1)
            {
                lock (rockLock)
                {
                    List<Transaction> txs = new List<Transaction>();
                    if (database == null)
                    {
                        return null;
                    }
                    lastUsedTime = DateTime.Now;
                    foreach (var i in idxTXFrom.getEntriesForKey(BitConverter.GetBytes(block_num)))
                    {
                        txs.Add(getTransactionInternal(i));
                    }
                    return txs;
                }
            }

            public IEnumerable<Transaction> getTransactionsByTime(long time_from, long time_to)
            {
                lock (rockLock)
                {
                    List<Transaction> txs = new List<Transaction>();
                    if (database == null)
                    {
                        return null;
                    }
                    lastUsedTime = DateTime.Now;
                    foreach (var ts_bytes in idxTXTimestamp.getAllKeys())
                    {
                        long timestamp = BitConverter.ToInt64(ts_bytes, 0);
                        if (timestamp >= time_from && timestamp <= time_to)
                        {
                            foreach (var i in idxTXTimestamp.getEntriesForKey(ts_bytes))
                            {
                                txs.Add(getTransactionInternal(i));
                            }
                        }
                    }
                    return txs;
                }
            }

            public IEnumerable<_applied_tx_idx_entry> getTransactionsApplied(ulong block_from, ulong block_to)
            {
                lock (rockLock)
                {
                    List<_applied_tx_idx_entry> txs = new List<_applied_tx_idx_entry>();
                    if (database == null)
                    {
                        return null;
                    }
                    lastUsedTime = DateTime.Now;
                    foreach (var bh_bytes in idxTXApplied.getAllKeys())
                    {
                        ulong blockheight = BitConverter.ToUInt64(bh_bytes, 0);
                        if (blockheight >= block_from && blockheight <= block_to)
                        {
                            foreach (var i in idxTXApplied.getEntriesForKey(bh_bytes))
                            {
                                txs.Add(new _applied_tx_idx_entry(i));
                            }
                        }
                    }
                    return txs;
                }
            }

            public bool removeBlock(ulong blockNum, bool removeTransactions)
            {
                lock(rockLock)
                {
                    Block b = getBlock(blockNum);
                    if(b != null)
                    {
                        var block_num_bytes = BitConverter.GetBytes(blockNum);
                        database.Remove(block_num_bytes, rocksCFBlocks);
                        // remove it from indexes
                        idxBlocksChecksum.delIndexEntry(b.blockChecksum, block_num_bytes);
                        idxBlocksLastSBChecksum.delIndexEntry(b.lastSuperBlockChecksum, block_num_bytes);
                        //
                        if(removeTransactions)
                        {
                            foreach(var tx_id_bytes in idxTXBlockHeight.getEntriesForKey(block_num_bytes))
                            {
                                removeTransactionInternal(tx_id_bytes);
                            }
                        }
                        return true;
                    }
                    return false;
                }
            }

            private bool removeTransactionInternal(byte[] tx_id_bytes)
            {
                lock(rockLock)
                {
                    Transaction tx = getTransactionInternal(tx_id_bytes);
                    if(tx != null)
                    {
                        database.Remove(tx_id_bytes, rocksCFTransactions);
                        // remove it from indexes
                        idxTXApplied.delIndexEntry(BitConverter.GetBytes(tx.applied), tx_id_bytes);
                        idxTXBlockHeight.delIndexEntry(BitConverter.GetBytes(tx.blockHeight), tx_id_bytes);
                        foreach(var f in tx.fromList.Keys)
                        {
                            idxTXFrom.delIndexEntry(f, tx_id_bytes);
                        }
                        foreach(var t in tx.toList.Keys)
                        {
                            idxTXTo.delIndexEntry(t, tx_id_bytes);
                        }
                        idxTXTimestamp.delIndexEntry(BitConverter.GetBytes(tx.timeStamp), tx_id_bytes);
                        idxTXType.delIndexEntry(BitConverter.GetBytes(tx.type), tx_id_bytes);
                        return true;
                    }
                    return false;
                }
            }

            public bool removeTransaction(byte[] txid)
            {
                lock(rockLock)
                {
                    var tx_id_bytes = txid;
                    return removeTransactionInternal(tx_id_bytes);
                }
            }
        }

        public class RocksDBStorage : IStorage
        {
            private readonly Dictionary<ulong, RocksDBInternal> openDatabases = new Dictionary<ulong, RocksDBInternal>();
            public uint closeAfterSeconds = 60;
            public uint oldDBCleanupPeriod = 600;
            public ulong maxBlocksPerDB = 10000;

            // Runtime stuff
            private ulong writeBufferSize = 0;
            private Cache commonBlockCache = null;
            private Cache commonCompressedBlockCache = null;
            private Queue<RocksDBInternal> reopenCleanupList = new Queue<RocksDBInternal>();
            private DateTime lastReopenOptimize = DateTime.Now;

            private RocksDBInternal getDatabase(ulong blockNum, bool onlyExisting = false)
            {
                // open or create the db which should contain blockNum
                ulong baseBlockNum = blockNum / maxBlocksPerDB;
                //Logging.info("RocksDB: Getting database for block {0} (Database: {1}).", blockNum, baseBlockNum);
                RocksDBInternal db = null;
                lock (openDatabases)
                {
                    if (openDatabases.ContainsKey(baseBlockNum))
                    {
                        db = openDatabases[baseBlockNum];
                        //Logging.info("RocksDB: Database is already registered. Opened = {0}", db.isOpen);
                    }
                    else
                    {
                        string db_path = pathBase + Path.DirectorySeparatorChar + baseBlockNum.ToString();
                        if (onlyExisting)
                        {
                            if (!Directory.Exists(db_path))
                            {
                                Logging.info("RocksDB: Open of '{0} requested with onlyExisting = true, but it does not exist.", db_path);
                                return null;
                            }
                        }
                        //
                        Logging.info("RocksDB: Opening a database for blocks {0} - {1}.", baseBlockNum * maxBlocksPerDB, (baseBlockNum * maxBlocksPerDB) + maxBlocksPerDB - 1);
                        db = new RocksDBInternal(db_path, commonBlockCache, commonCompressedBlockCache, writeBufferSize);
                        openDatabases.Add(baseBlockNum, db);
                    }
                }
                if (!db.isOpen)
                {
                    Logging.info("RocksDB: Database {0} is not opened - opening.", baseBlockNum);
                    db.openDatabase();
                }
                return db;
            }

            private ulong getPhysicalMemorySizeMB()
            {
                ulong totalMemoryKB = 0;
                try
                {
                    System.Management.ObjectQuery csQuery = new System.Management.ObjectQuery("SELECT * FROM CIM_OperatingSystem");
                    System.Management.ManagementObjectSearcher searcher = new System.Management.ManagementObjectSearcher(csQuery);

                    foreach (var obj in searcher.Get())
                    {
                        totalMemoryKB = Convert.ToUInt64(obj["TotalVisibleMemorySize"]);
                        break;
                    }
                }
                catch (Exception)
                {
                    // ignore
                }
                return totalMemoryKB / 1024;
            }

            private ulong estimateDBWriteBufferSize()
            {
                const ulong MB = 1024 * 1024;
                const ulong GB = 1024 * MB;
                ulong memMB = getPhysicalMemorySizeMB();
                if (memMB < 4096) // 4GB or below or indeterminate
                {
                    return 128 * MB;
                }
                else if (memMB < 8192) // between 4GB and 8GB
                {
                    return 512 * MB;
                }
                else // above 8GB
                {
                    return 1 * GB;
                }
            }

            private ulong estimateDBBlockCacheSize()
            {
                const ulong MB = 1024 * 1024;
                const ulong GB = 1024 * MB;
                ulong memMB = getPhysicalMemorySizeMB();
                if (memMB < 4096) // 4GB or below or indeterminate
                {
                    return 512 * MB;
                }
                else if (memMB < 8192) // between 4GB and 8GB
                { 
                    return 1 * GB;
                }
                else // above 8GB
                {
                    return 2 * GB;
                }
            }

            private long GetTotalFreeSpace(string driveName)
            {
                foreach (DriveInfo drive in DriveInfo.GetDrives())
                {
                    if (drive.IsReady && drive.Name == driveName)
                    {
                        return drive.TotalFreeSpace;
                    }
                }
                return -1;
            }

            protected override bool prepareStorageInternal()
            {
                // Files structured like:
                //  'pathBase\<startOffset>', where <startOffset> is the nominal lowest block number in that database
                //  the actual lowest block in that database may be higher than <startOffset>
                // <startOffset> is aligned to `maxBlocksPerDB` blocks

                // check that the base path exists, or create it
                if(!Directory.Exists(pathBase))
                {
                    try
                    {
                        Directory.CreateDirectory(pathBase);
                    } catch(Exception e)
                    {
                        Logging.error(String.Format("Unable to prepare block database path '{0}': {1}", pathBase, e.Message));
                        return false;
                    }
                }
                // preare cache
                writeBufferSize = estimateDBWriteBufferSize();
                ulong blockCacheSize = estimateDBBlockCacheSize();
                commonBlockCache = Cache.CreateLru(blockCacheSize / 2);
                commonCompressedBlockCache = Cache.CreateLru(blockCacheSize / 2);
                // pre-start DB optimization
                if (Config.optimizeDBStorage)
                {
                    Logging.info("RocksDB: Performing pre-start DB compaction and optimization.");
                    foreach (string db in Directory.GetDirectories(pathBase)) {
                        Logging.info("RocksDB: Optimizing [{0}].", db);
                        RocksDBInternal temp_db = new RocksDBInternal(db);
                        try
                        {
                            temp_db.openDatabase();
                            temp_db.closeDatabase();
                        } catch(Exception e)
                        {
                            Logging.warn("RocksDB: Error while opening database {0}: {1}", db, e.Message);
                        }
                    }
                    Logging.info("RocksDB: Pre-start optimnization complete.");
                }
                return true;
            }

            protected override void cleanupCache()
            {
                lock (openDatabases)
                {
                    Logging.info("RocksDB Registered database list:");
                    foreach (var db in openDatabases.Values)
                    {
                        Logging.info("RocksDB: [{0}]: open: {1}, last used: {2}, last maintenance: {3}",
                            db.dbPath,
                            db.isOpen,
                            db.lastUsedTime,
                            db.lastMaintenance
                            );
                    }
                    List<ulong> toDrop = new List<ulong>();
                    foreach (var db in openDatabases)
                    {
                        db.Value.maintenance();
                        if (db.Value.isOpen && (DateTime.Now - db.Value.lastUsedTime).TotalSeconds >= closeAfterSeconds)
                        {
                            Logging.info("RocksDB: Closing '{0}' due to inactivity.", db.Value.dbPath);
                            db.Value.closeDatabase();
                            toDrop.Add(db.Key);
                            reopenCleanupList.Enqueue(db.Value);
                        }
                    }
                    foreach (ulong dbnum in toDrop)
                    {
                        openDatabases.Remove(dbnum);
                    }

                    if ((DateTime.Now - lastReopenOptimize).TotalSeconds > 60.0)
                    {
                        int num = 0;
                        List<RocksDBInternal> problemDBs = new List<RocksDBInternal>();
                        while (num < 2 && reopenCleanupList.Count > 0)
                        {
                            var db = reopenCleanupList.Dequeue();
                            if(openDatabases.Values.Any(x => x.dbPath == db.dbPath))
                            {
                                Logging.info("RocksDB: Database [{0}] was still in use, skipping until it is closed.", db.dbPath);
                                continue;
                            }
                            Logging.info("RocksDB: Reopening previously closed database [{0}] to allow RocksDB removal of stale log files.", db.dbPath);
                            try
                            {
                                db.openDatabase();
                                db.closeDatabase();
                                Logging.info("RocksDB: Reopen succeeded");
                            } catch(Exception)
                            {
                                // these were attempted too quickly and RocksDB internal still has some pointers open
                                problemDBs.Add(db);
                                Logging.info("RocksDB: Database [{0}] was locked by another process, will try again later.", db.dbPath);
                            }
                            num += 1;
                        }
                        foreach(var db in problemDBs)
                        {
                            reopenCleanupList.Enqueue(db);
                        }
                        lastReopenOptimize = DateTime.Now;
                    }

                    // check disk status and close databases if we're running low
                    try
                    {
                        long diskFreeBytes = GetTotalFreeSpace(Directory.GetDirectoryRoot(Directory.GetCurrentDirectory()));
                        Logging.info("RocksDB: Disk has {0} bytes free.", diskFreeBytes);
                        if (diskFreeBytes < 10L * 1024L * 1024L * 1024L && openDatabases.Where(x => x.Value.isOpen).Count() > 0)
                        {
                            // close the oldest database - this might cause the only currently-open database to be closed, but it will force block compaction and reorg,
                            // which should return some disk space. 
                            // The log will show this as a warning, because it means the disk hosting the block database really isn't large enough.
                            var oldest_db = openDatabases.OrderBy(x => x.Value.lastUsedTime).Where(x => x.Value.isOpen).First();
                            Logging.warn("RocksDB: Disk free space is low, closing/reopening the oldest database to force compaction: {0}", oldest_db.Value.dbPath);
                            oldest_db.Value.closeDatabase();
                            openDatabases.Remove(oldest_db.Key);
                            reopenCleanupList.Enqueue(oldest_db.Value);
                            long diskFreeBytesAfter = GetTotalFreeSpace(Directory.GetDirectoryRoot(Directory.GetCurrentDirectory()));
                            Logging.info("RocksDB: After close, disk has {0} bytes free.", diskFreeBytesAfter);
                        }
                    }
                    catch (Exception e)
                    {
                        Logging.warn("Unable to read disk free size. Automatic database management will not be used: {0}", e.Message);
                    }
                }
            }

            public override void deleteData()
            {
                lock (openDatabases)
                {
                    while (openDatabases.Count > 0)
                    {
                        ulong d = openDatabases.Keys.First();
                        var db = openDatabases[d];
                        db.closeDatabase();
                        string path = db.dbPath;
                        Logging.info(String.Format("RocksDB: Deleting '{0}'", path));
                        openDatabases.Remove(d);
                        try
                        {
                            Directory.Delete(path);
                        }
                        catch (Exception e)
                        {
                            Logging.warn(String.Format("RocksDB: Delete data - failed removing directory '{0}': {1}", path, e));
                        }
                    }
                }
            }

            protected override void shutdown()
            {
                lock(openDatabases)
                {
                    foreach(var db in openDatabases.Values)
                    {
                        Logging.info(String.Format("RocksDB: Shutdown, closing '{0}'", db.dbPath));
                        db.closeDatabase();
                    }
                }
            }

            public override ulong getHighestBlockInStorage()
            {
                // TODO Cache
                // find our absolute highest block db
                ulong latest_db = 0;
                foreach(var d in Directory.EnumerateDirectories(pathBase))
                {
                    string[] dir_parts = d.Split(Path.DirectorySeparatorChar);
                    string final_dir = dir_parts[dir_parts.Length - 1];
                    if(ulong.TryParse(final_dir, out ulong db_base))
                    {
                        if(db_base > latest_db)
                        {
                            latest_db = db_base;
                        }
                    }
                }
                if (latest_db == 0)
                {
                    return 0; // empty db
                }
                lock (openDatabases)
                {
                    for(ulong i = latest_db; i >= 0; i--)
                    {
                        var db = getDatabase(i * maxBlocksPerDB, true);
                        if (db != null && db.maxBlockNumber > 0)
                        {
                            return db.maxBlockNumber;
                        }
                    }
                    return 0;
                }
            }

            public override ulong getLowestBlockInStorage()
            {
                // TODO Cache
                // find our absolute highest block db
                ulong oldest_db = 0;
                foreach (var d in Directory.EnumerateDirectories(pathBase))
                {
                    string final_dir = Path.GetDirectoryName(d);
                    if (ulong.TryParse(final_dir, out ulong db_base))
                    {
                        if (db_base > oldest_db)
                        {
                            oldest_db = db_base;
                        }
                    }
                }
                if (oldest_db == 0)
                {
                    return 0; // empty db
                }
                lock (openDatabases)
                {
                    var db = getDatabase(oldest_db);
                    return db.minBlockNumber;
                }
            }

            protected override bool insertBlockInternal(Block block)
            {
                lock (openDatabases)
                {
                    var db = getDatabase(block.blockNum);
                    return db.insertBlock(block);
                }
            }

            protected override bool insertTransactionInternal(Transaction transaction)
            {
                lock (openDatabases)
                {
                    var db = getDatabase(transaction.blockHeight);
                    return db.insertTransaction(transaction);
                }
            }

            public override Block getBlock(ulong blocknum)
            {
                lock(openDatabases)
                {
                    var db = getDatabase(blocknum);
                    return db.getBlock(blocknum);
                }
            }

            public override Block getBlockByHash(byte[] checksum)
            {
                lock(openDatabases)
                {
                    foreach (var db in openDatabases.Values)
                    {
                        if (!db.isOpen)
                        {
                            db.openDatabase();
                        }
                        Block b = db.getBlockByHash(checksum);
                        if (b != null)
                        {
                            return b;
                        }
                    }
                    //
                    return null;
                }
            }

            public override Block getBlockByLastSBHash(byte[] checksum)
            {
                lock (openDatabases)
                {
                    foreach (var db in openDatabases.Values)
                    {
                        if (!db.isOpen)
                        {
                            db.openDatabase();
                        }
                        Block b = db.getBlockByLastSBHash(checksum);
                        if (b != null)
                        {
                            return b;
                        }
                    }
                    //
                    return null;
                }
            }

            public override IEnumerable<Block> getBlocksByRange(ulong from, ulong to)
            {
                IEnumerable<Block> combined = Enumerable.Empty<Block>();
                if(to < from || (to+from == 0))
                {
                    return combined;
                }
                lock(openDatabases)
                {
                    for(ulong i = from; i <= to; i++)
                    {
                        var db = getDatabase(i);
                        var matching_blocks = db.getBlocksByRange(from, to);
                        combined = Enumerable.Concat(combined, matching_blocks);
                    }
                    return combined;
                }
            }

            public override Transaction getTransaction(byte[] txid, ulong block_num = 0)
            {
                lock(openDatabases)
                {
                    if(block_num != 0)
                    {
                        var db = getDatabase(block_num);
                        return db.getTransaction(txid);
                    } else
                    {
                        // TODO implement this
                        /*ulong db_blocknum = getHighestBlockInStorage();
                        if (block_num > db_blocknum)
                        {
                            return null;
                        }

                        ulong min_bh = 0;
                        int txid_sep_pos = txid.IndexOf("-");
                        if (txid_sep_pos > 0)
                        {
                            min_bh = UInt64.Parse(txid.Substring(0, txid_sep_pos));
                        }*/

                        // TODO this is incorrect
                        foreach (var db in openDatabases.Values)
                        {
                            if(!db.isOpen)
                            {
                                db.openDatabase();
                            }
                            Transaction t = db.getTransaction(txid);
                            if (t != null)
                            {
                                return t;
                            }
                        }
                    }
                    return null;
                }
            }

            public override IEnumerable<Transaction> getTransactionsByType(Transaction.Type type, ulong block_from = 0, ulong block_to = 0)
            {
                lock(openDatabases)
                {
                    IEnumerable<Transaction> combined = Enumerable.Empty<Transaction>();
                    IEnumerable<RocksDBInternal> dbs_to_search = openDatabases.Values.Where(x => true); // all databases
                    if(block_from + block_to > 0)
                    {
                        dbs_to_search = openDatabases.Where(kvp => kvp.Key >= block_from && kvp.Key <= block_to).Select(kvp => kvp.Value);
                    }
                    foreach(var db in dbs_to_search)
                    {
                        if(!db.isOpen)
                        {
                            db.openDatabase();
                        }
                        var matching_txs = db.getTransactionsByType(type);
                        combined = Enumerable.Concat(combined, matching_txs);
                    }
                    return combined;
                }
            }

            public override IEnumerable<Transaction> getTransactionsFromAddress(byte[] from_addr, ulong block_from = 0, ulong block_to = 0)
            {
                lock (openDatabases)
                {
                    IEnumerable<Transaction> combined = Enumerable.Empty<Transaction>();
                    IEnumerable<RocksDBInternal> dbs_to_search = openDatabases.Values.Where(x => true); // all databases
                    if (block_from + block_to > 0)
                    {
                        dbs_to_search = openDatabases.Where(kvp => kvp.Key >= block_from && kvp.Key <= block_to).Select(kvp => kvp.Value);
                    }
                    foreach (var db in dbs_to_search)
                    {
                        if (!db.isOpen)
                        {
                            db.openDatabase();
                        }
                        var matching_txs = db.getTransactionsFromAddress(from_addr);
                        combined = Enumerable.Concat(combined, matching_txs);
                    }
                    return combined;
                }
            }

            public override IEnumerable<Transaction> getTransactionsToAddress(byte[] to_addr, ulong block_from = 0, ulong block_to = 0)
            {
                lock (openDatabases)
                {
                    IEnumerable<Transaction> combined = Enumerable.Empty<Transaction>();
                    IEnumerable<RocksDBInternal> dbs_to_search = openDatabases.Values.Where(x => true); // all databases
                    if (block_from + block_to > 0)
                    {
                        dbs_to_search = openDatabases.Where(kvp => kvp.Key >= block_from && kvp.Key <= block_to).Select(kvp => kvp.Value);
                    }
                    foreach (var db in dbs_to_search)
                    {
                        if (!db.isOpen)
                        {
                            db.openDatabase();
                        }
                        var matching_txs = db.getTransactionsToAddress(to_addr);
                        combined = Enumerable.Concat(combined, matching_txs);
                    }
                    return combined;
                }
            }

            public override IEnumerable<Transaction> getTransactionsInBlock(ulong block_num, int tx_type = -1)
            {
                lock(openDatabases)
                {
                    var db = getDatabase(block_num);
                    return db.getTransactionsInBlock(block_num, tx_type);
                }
            }

            public override IEnumerable<Transaction> getTransactionsByTime(long time_from, long time_to)
            {
                IEnumerable<Transaction> combined = Enumerable.Empty<Transaction>();
                if(time_to < time_from || (time_to == 0 && time_from == 0))
                {
                    return combined;
                }
                lock(openDatabases)
                {
                    foreach(var db in openDatabases.Values)
                    {
                        if(!db.isOpen)
                        {
                            db.openDatabase();
                        }
                        var matching_txs = db.getTransactionsByTime(time_from, time_to);
                        combined = Enumerable.Concat(combined, matching_txs);
                    }
                    return combined;
                }
            }

            public override IEnumerable<Transaction> getTransactionsApplied(ulong block_from, ulong block_to)
            {
                List<Transaction> combined = new List<Transaction>();
                if(block_to < block_from || (block_from+block_to == 0))
                {
                    return combined;
                }
                lock(openDatabases)
                {
                    for(ulong i = block_from; i <= block_to; i++)
                    {
                        var db = getDatabase(i);
                        foreach (var appidx in db.getTransactionsApplied(block_from, block_to))
                        {
                            var t = getTransaction(appidx.tx_id, appidx.tx_original_bh);
                            if (t != null)
                            {
                                combined.Add(t);
                            }
                        }
                    }
                    return combined;
                }
            }

            public override bool removeBlock(ulong block_num, bool remove_transactions)
            {
                lock(openDatabases)
                {
                    var db = getDatabase(block_num);
                    return db.removeBlock(block_num, remove_transactions);
                }
            }

            public override bool removeTransaction(byte[] txid, ulong block_num = 0)
            {
                lock(openDatabases)
                {
                    if(block_num > 0)
                    {
                        var db = getDatabase(block_num);
                        return db.removeTransaction(txid);
                    } else
                    {
                        foreach(var db in openDatabases.Values)
                        {
                            if(!db.isOpen)
                            {
                                db.openDatabase();
                            }
                            if (db.removeTransaction(txid))
                            {
                                return true;
                            }
                        }
                        return false;
                    }
                }
            }
        }
    }
}
