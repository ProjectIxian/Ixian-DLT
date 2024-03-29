﻿// Copyright (C) 2017-2020 Ixian OU
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

namespace DLT
{
    namespace Storage
    {
        class RocksDBInternal
        {
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
                public ColumnFamilyHandle rocksIndexHandle;
                private RocksDb db;
                public _storage_Index(string cf_name, RocksDb db)
                {
                    this.db = db;
                    rocksIndexHandle = db.GetColumnFamily(cf_name);
                }

                public void addIndexEntry(byte[] key, byte[] e)
                {
                    byte[] keyWithSuffix = new byte[key.Length + e.Length];
                    Array.Copy(key, keyWithSuffix, key.Length);
                    Array.Copy(e, 0, keyWithSuffix, key.Length, e.Length);

                    db.Put(keyWithSuffix, e, rocksIndexHandle);
                }

                public void delIndexEntry(byte[] key, byte[] e)
                {
                    byte[] keyWithSuffix = new byte[key.Length + e.Length];
                    Array.Copy(key, keyWithSuffix, key.Length);
                    Array.Copy(e, 0, keyWithSuffix, key.Length, e.Length);

                    db.Remove(keyWithSuffix, rocksIndexHandle);
                }

                public IEnumerable<byte[]> getEntriesForKey(byte[] key)
                {
                    List<byte[]> entries = new();
                    var iter = db.NewIterator(rocksIndexHandle);
                    for (iter.Seek(key); iter.Valid() && iter.Key().Take(key.Length).SequenceEqual(key); iter.Next())
                    {
                        entries.Add(iter.Value());
                    }
                    iter.Dispose();
                    return entries;
                }

                public IEnumerable<byte[]> getAllKeys()
                {
                    List<byte[]> entries = new();
                    var iter = db.NewIterator(rocksIndexHandle);
                    iter.SeekToFirst();
                    while (iter.Valid())
                    {
                        entries.Add(iter.Key());
                        iter.Next();
                    }
                    iter.Dispose();
                    return entries;
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
            private _storage_Index idxTXAppliedType;
            private _storage_Index idxTXFrom;
            private _storage_Index idxTXTo;
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
                    if (blockCache != null)
                    {
                        bbto.SetBlockCache(blockCache.Handle);
                    }
                    if (compressedBlockCache != null)
                    {
                        bbto.SetBlockCacheCompressed(compressedBlockCache.Handle);
                    }
                    //bbto.SetFilterPolicy(BloomFilterPolicy.Create(256, false));
                    //bbto.SetWholeKeyFiltering(true);
                    ColumnFamilyOptions cfo = new ColumnFamilyOptions();
                    cfo.SetBlockBasedTableFactory(bbto);
                    cfo.SetCompression(Compression.Snappy);
                    //cfo.SetPrefixExtractor(SliceTransform.CreateFixedPrefix(64));
                    //cfo.SetMemtableHugePageSize(2 * 1024 * 1024);
                    var columnFamilies = new ColumnFamilies(cfo);
                    // default column families
                    columnFamilies.Add("blocks", cfo);
                    columnFamilies.Add("transactions", cfo);
                    columnFamilies.Add("meta", cfo);
                    // index column families
                    columnFamilies.Add("index_block_checksum", cfo);
                    columnFamilies.Add("index_block_last_sb_checksum", cfo);
                    columnFamilies.Add("index_tx_applied_type", cfo);
                    columnFamilies.Add("index_tx_from", cfo);
                    columnFamilies.Add("index_tx_to", cfo);
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
                    idxTXAppliedType = new _storage_Index("index_tx_applied_type", database);
                    idxTXFrom = new _storage_Index("index_tx_from", database);
                    idxTXTo = new _storage_Index("index_tx_to", database);
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
                if (database != null)
                {
                    if ((DateTime.Now - lastMaintenance).TotalSeconds > maintenanceInterval)
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
                                i = database.NewIterator(idxTXAppliedType.rocksIndexHandle);
                                i = i.SeekToFirst();
                                first_key = i.Key();
                                i = i.SeekToLast();
                                last_key = i.Key();
                                database.CompactRange(first_key, last_key, idxTXAppliedType.rocksIndexHandle);
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
                                i = database.NewIterator(idxTXApplied.rocksIndexHandle);
                                i = i.SeekToFirst();
                                first_key = i.Key();
                                i = i.SeekToLast();
                                last_key = i.Key();
                                database.CompactRange(first_key, last_key, idxTXApplied.rocksIndexHandle);
                                i.Dispose();
                            }
                        }
                        catch (Exception e)
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
                    idxTXAppliedType = null;
                    idxTXFrom = null;
                    idxTXTo = null;
                    idxTXApplied = null;
                    //
                    rocksOptions = null;
                    database.Dispose();
                    database = null;
                }
            }

            private void updateBlockIndexes(Block sb)
            {
                byte[] block_num_bytes = BitConverter.GetBytes(sb.blockNum);
                idxBlocksChecksum.addIndexEntry(sb.blockChecksum, block_num_bytes);
                if (sb.lastSuperBlockChecksum != null)
                {
                    idxBlocksLastSBChecksum.addIndexEntry(sb.lastSuperBlockChecksum, block_num_bytes);
                }
                lastUsedTime = DateTime.Now;
            }

            private byte[] appliedAndTypeToBytes(ulong bh, int type)
            {
                byte[] bhBytes = BitConverter.GetBytes(bh);
                byte[] typeBytes = BitConverter.GetBytes(type);
                byte[] bhTypeBytes = new byte[bhBytes.Length + typeBytes.Length];
                Array.Copy(bhBytes, bhTypeBytes, bhBytes.Length);
                Array.Copy(typeBytes, 0, bhTypeBytes, bhBytes.Length, typeBytes.Length);
                return bhTypeBytes;
            }

            private void updateTXIndexes(Transaction st)
            {
                byte[] tx_id_bytes = st.id;

                foreach (var from in st.fromList)
                {
                    idxTXFrom.addIndexEntry(new Address(st.pubKey.addressNoChecksum, from.Key).addressNoChecksum, tx_id_bytes);
                }

                foreach (var to in st.toList)
                {
                    idxTXTo.addIndexEntry(to.Key.addressNoChecksum, tx_id_bytes);
                }

                idxTXApplied.addIndexEntry(BitConverter.GetBytes(st.applied), tx_id_bytes);

                idxTXAppliedType.addIndexEntry(appliedAndTypeToBytes(st.applied, st.type), tx_id_bytes);

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
                    database.Put(BitConverter.GetBytes(block.blockNum), block.getBytes(true, true, true), rocksCFBlocks);
                    updateBlockIndexes(block);
                    updateMinMax(block.blockNum);
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
                    database.Put(transaction.id, transaction.getBytes(true, true), rocksCFTransactions);
                    updateTXIndexes(transaction);
                }
                lastUsedTime = DateTime.Now;
                return true;
            }

            private Block getBlockInternal(byte[] block_num_bytes)
            {
                byte[] block_bytes = database.Get(block_num_bytes, rocksCFBlocks);
                lastUsedTime = DateTime.Now;
                if (block_bytes != null)
                {
                    Block b = new Block(block_bytes, true);
                    b.fromLocalStorage = true;
                    return b;
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
                            Block b = new Block(iter.Value(), true);
                            b.fromLocalStorage = true;
                            blocks.Add(b);
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
                        Transaction t = new Transaction(tx_bytes, true, true);
                        t.fromLocalStorage = true;
                        return t;
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
                    IEnumerable<byte[]> entries;
                    if (tx_type == -1)
                    {
                        entries = idxTXApplied.getEntriesForKey(BitConverter.GetBytes(block_num));
                    }
                    else
                    {
                        entries = idxTXAppliedType.getEntriesForKey(appliedAndTypeToBytes(block_num, tx_type));
                    }

                    foreach (var txid in entries)
                    {
                        var tx = getTransactionInternal(txid);
                        txs.Add(tx);
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
                lock (rockLock)
                {
                    Block b = getBlock(blockNum);
                    if (b != null)
                    {
                        var block_num_bytes = BitConverter.GetBytes(blockNum);
                        database.Remove(block_num_bytes, rocksCFBlocks);
                        // remove it from indexes
                        idxBlocksChecksum.delIndexEntry(b.blockChecksum, block_num_bytes);
                        idxBlocksLastSBChecksum.delIndexEntry(b.lastSuperBlockChecksum, block_num_bytes);
                        //
                        if (removeTransactions)
                        {
                            foreach (var tx_id_bytes in idxTXApplied.getEntriesForKey(block_num_bytes))
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
                lock (rockLock)
                {
                    Transaction tx = getTransactionInternal(tx_id_bytes);
                    if (tx != null)
                    {
                        database.Remove(tx_id_bytes, rocksCFTransactions);
                        // remove it from indexes
                        idxTXApplied.delIndexEntry(BitConverter.GetBytes(tx.applied), tx_id_bytes);
                        foreach (var f in tx.fromList.Keys)
                        {
                            idxTXFrom.delIndexEntry(f, tx_id_bytes);
                        }
                        foreach (var t in tx.toList.Keys)
                        {
                            idxTXTo.delIndexEntry(t.addressNoChecksum, tx_id_bytes);
                        }
                        idxTXAppliedType.delIndexEntry(appliedAndTypeToBytes(tx.applied, tx.type), tx_id_bytes);
                        return true;
                    }
                    return false;
                }
            }

            public bool removeTransaction(byte[] txid)
            {
                lock (rockLock)
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

            // Runtime stuff
            private ulong writeBufferSize = 0;
            private Cache commonBlockCache = null;
            private Cache commonCompressedBlockCache = null;
            private Queue<RocksDBInternal> reopenCleanupList = new Queue<RocksDBInternal>();
            private DateTime lastReopenOptimize = DateTime.Now;

            private RocksDBInternal getDatabase(ulong blockNum, bool onlyExisting = false)
            {
                // Open or create the db which should contain blockNum
                ulong baseBlockNum = blockNum / Config.maxBlocksPerDatabase;
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
                        string db_path = Path.Combine(pathBase, "0000" , baseBlockNum.ToString());
                        if (onlyExisting)
                        {
                            if (!Directory.Exists(db_path))
                            {
                                Logging.info("RocksDB: Open of '{0} requested with onlyExisting = true, but it does not exist.", db_path);
                                return null;
                            }
                        }

                        Logging.info("RocksDB: Opening a database for blocks {0} - {1}.", baseBlockNum * Config.maxBlocksPerDatabase, (baseBlockNum * Config.maxBlocksPerDatabase) + Config.maxBlocksPerDatabase - 1);
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

            private ulong estimateDBWriteBufferSize()
            {
                const long MB = 1024 * 1024;
                const long GB = 1024 * MB;
                long memMB = Platform.getAvailableRAM() / MB;
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
                const long MB = 1024 * 1024;
                const long GB = 1024 * MB;
                long memMB = Platform.getAvailableRAM() / MB;
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


            protected override bool prepareStorageInternal()
            {
                // Files structured like:
                //  'pathBase\<startOffset>', where <startOffset> is the nominal lowest block number in that database
                //  the actual lowest block in that database may be higher than <startOffset>
                // <startOffset> is aligned to `maxBlocksPerDB` blocks

                // check that the base path exists, or create it
                if (!Directory.Exists(pathBase))
                {
                    try
                    {
                        Directory.CreateDirectory(pathBase);
                        Directory.CreateDirectory(Path.Combine(pathBase, "0000"));
                    }
                    catch (Exception e)
                    {
                        Logging.error(String.Format("Unable to prepare block database path '{0}': {1}", pathBase, e.Message));
                        return false;
                    }
                }
                // Prepare cache
                writeBufferSize = estimateDBWriteBufferSize();
                ulong blockCacheSize = estimateDBBlockCacheSize();
                commonBlockCache = Cache.CreateLru(blockCacheSize / 2);
                commonCompressedBlockCache = Cache.CreateLru(blockCacheSize / 2);
                // DB optimization
                if (Config.optimizeDBStorage)
                {
                    Logging.info("RocksDB: Performing pre-start DB compaction and optimization.");
                    foreach (string db in Directory.GetDirectories(Path.Combine(pathBase, "0000")))
                    {
                        Logging.info("RocksDB: Optimizing [{0}].", db);
                        RocksDBInternal temp_db = new RocksDBInternal(db);
                        try
                        {
                            temp_db.openDatabase();
                            temp_db.closeDatabase();
                        }
                        catch (Exception e)
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
                            if (openDatabases.Values.Any(x => x.dbPath == db.dbPath))
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
                            }
                            catch (Exception)
                            {
                                // these were attempted too quickly and RocksDB internal still has some pointers open
                                problemDBs.Add(db);
                                Logging.info("RocksDB: Database [{0}] was locked by another process, will try again later.", db.dbPath);
                            }
                            num += 1;
                        }
                        foreach (var db in problemDBs)
                        {
                            reopenCleanupList.Enqueue(db);
                        }
                        lastReopenOptimize = DateTime.Now;
                    }

                    // check disk status and close databases if we're running low
                    try
                    {
                        long diskFreeBytes = Platform.getAvailableDiskSpace();
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
                            Logging.info("RocksDB: After close, disk has {0} bytes free.", Platform.getAvailableDiskSpace());
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
                Directory.Delete(Config.dataFolderPath + Path.DirectorySeparatorChar + "blocks", true);
            }

            protected override void shutdown()
            {
                lock (openDatabases)
                {
                    foreach (var db in openDatabases.Values)
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
                foreach (var d in Directory.EnumerateDirectories(Path.Combine(pathBase, "0000")))
                {
                    string[] dir_parts = d.Split(Path.DirectorySeparatorChar);
                    string final_dir = dir_parts[dir_parts.Length - 1];
                    if (ulong.TryParse(final_dir, out ulong db_base))
                    {
                        if (db_base > latest_db)
                        {
                            latest_db = db_base;
                        }
                    }
                }
                lock (openDatabases)
                {
                    for (ulong i = latest_db; i >= 0; i--)
                    {
                        var db = getDatabase(i * Config.maxBlocksPerDatabase, true);
                        if (db != null && db.maxBlockNumber > 0)
                        {
                            return db.maxBlockNumber;
                        }else if(i == 0)
                        {
                            return 0;
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
                foreach (var d in Directory.EnumerateDirectories(Path.Combine(pathBase, "0000")))
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
                    var db = getDatabase(transaction.applied);
                    return db.insertTransaction(transaction);
                }
            }

            public override Block getBlock(ulong blocknum)
            {
                lock (openDatabases)
                {
                    var db = getDatabase(blocknum);
                    return db.getBlock(blocknum);
                }
            }

            public override Block getBlockByHash(byte[] checksum)
            {
                lock (openDatabases)
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
                if (to < from || (to + from == 0))
                {
                    return combined;
                }
                lock (openDatabases)
                {
                    for (ulong i = from; i <= to; i++)
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
                lock (openDatabases)
                {
                    if (block_num != 0)
                    {
                        var db = getDatabase(block_num);
                        return db.getTransaction(txid);
                    }
                    else
                    {
                        bool found = false;
                        ulong db_blocknum = IxiVarInt.GetIxiVarUInt(txid, 1).num;

                        if (db_blocknum == 0)
                        {
                            Logging.error("Invalid txid {0} - generated at block height 0.", Transaction.getTxIdString(txid));
                            return null;
                        }

                        ulong highest_blocknum = getHighestBlockInStorage();
                        if (db_blocknum > highest_blocknum)
                        {
                            Logging.error("Tried to get transaction generated on block {0} but the highest block in storage is {1}", db_blocknum, highest_blocknum);
                            return null;
                        }

                        if (highest_blocknum > db_blocknum + ConsensusConfig.getRedactedWindowSize(Block.maxVersion))
                        {
                            highest_blocknum = db_blocknum + ConsensusConfig.getRedactedWindowSize(Block.maxVersion);
                        }

                        while (!found)
                        {
                            var db = getDatabase(db_blocknum);
                            if (db == null)
                            {
                                Logging.error("Cannot access database for block {0}", db_blocknum);
                                return null;
                            }

                            Transaction tx = db.getTransaction(txid);
                            if (tx != null)
                            {
                                return tx;
                            }
                            else
                            {
                                if (db_blocknum + Config.maxBlocksPerDatabase <= highest_blocknum)
                                {
                                    db_blocknum += Config.maxBlocksPerDatabase;
                                }
                                else
                                {
                                    // Transaction not found in any database
                                    return null;
                                }
                            }
                        }

                    }
                    return null;
                }
            }

            public override IEnumerable<Transaction> getTransactionsByType(Transaction.Type type, ulong block_from = 0, ulong block_to = 0)
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
                        for (ulong i = db.minBlockNumber; i < db.maxBlockNumber; i++)
                        {
                            var matching_txs = db.getTransactionsInBlock(i, (int)type);
                            combined = Enumerable.Concat(combined, matching_txs);
                        }
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
                lock (openDatabases)
                {
                    var db = getDatabase(block_num);
                    return db.getTransactionsInBlock(block_num, tx_type);
                }
            }

            public override IEnumerable<Transaction> getTransactionsApplied(ulong block_from, ulong block_to)
            {
                List<Transaction> combined = new List<Transaction>();
                if (block_to < block_from || (block_from + block_to == 0))
                {
                    return combined;
                }
                lock (openDatabases)
                {
                    for (ulong i = block_from; i <= block_to; i++)
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
                lock (openDatabases)
                {
                    var db = getDatabase(block_num);
                    return db.removeBlock(block_num, remove_transactions);
                }
            }

            public override bool removeTransaction(byte[] txid, ulong block_num = 0)
            {
                lock (openDatabases)
                {
                    if (block_num > 0)
                    {
                        var db = getDatabase(block_num);
                        return db.removeTransaction(txid);
                    }
                    else
                    {
                        foreach (var db in openDatabases.Values)
                        {
                            if (!db.isOpen)
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

            public override (byte[] blockChecksum, string totalSignerDifficulty) getBlockTotalSignerDifficulty(ulong blocknum)
            {
                throw new NotImplementedException();
            }
        }
    }
}
