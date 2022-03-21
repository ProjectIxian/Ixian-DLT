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
using SQLite;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Numerics;

namespace DLT
{
    namespace Storage
    {
        public class SQLiteStorage : IStorage
        {
            // Sql connections
            private SQLiteConnection sqlConnection = null;
            private readonly object storageLock = new object(); // This should always be placed when performing direct sql operations

            private SQLiteConnection superBlocksSqlConnection = null;
            private readonly object superBlockStorageLock = new object(); // This should always be placed when performing direct sql operations

            private Dictionary<string, object[]> connectionCache = new Dictionary<string, object[]>();

            private int CONNECTION_CACHE_LIMIT = 50;

            // Storage cache
            private ulong cached_lastBlockNum = 0;
            private ulong current_seek = 1;

            public bool upgrading = false;
            public ulong upgradeProgress = 0;
            public ulong upgradeMaxBlockNum = 0;

            public class _storage_Block
            {
                public long blockNum { get; set; }
                public byte[] blockChecksum { get; set; }
                public byte[] lastBlockChecksum { get; set; }
                public byte[] walletStateChecksum { get; set; }
                public byte[] sigFreezeChecksum { get; set; }
                public long difficulty { get; set; }
                public byte[] powField { get; set; }
                public string signatures { get; set; }
                public string transactions { get; set; }
                public long timestamp { get; set; }
                public int version { get; set; }
                public byte[] lastSuperBlockChecksum { get; set; }
                public long lastSuperBlockNum { get; set; }
                public byte[] superBlockSegments { get; set; }
                public bool compactedSigs { get; set; }
                public byte[] blockProposer { get; set; }
                public long signerDifficulty { get; set; }
            }

            public class _storage_Transaction
            {
                public string id { get; set; }
                public int type { get; set; }
                public string amount { get; set; }
                public string fee { get; set; }
                public string toList { get; set; }
                public string fromList { get; set; }
                public byte[] from { get; set; }
                public byte[] dataChecksum { get; set; }
                public byte[] data { get; set; }
                public long blockHeight { get; set; }
                public int nonce { get; set; }
                public long timestamp { get; set; }
                public byte[] checksum { get; set; }
                public byte[] signature { get; set; }
                public byte[] pubKey { get; set; }
                public long applied { get; set; }
                public int version { get; set; }
            }

            // Creates the storage file if not found
            protected override bool prepareStorageInternal()
            {
                string db_path = pathBase + Path.DirectorySeparatorChar + "superblocks.dat";

                // Bind the connection
                superBlocksSqlConnection = getSQLiteConnection(db_path, false);


                // pre-start DB optimization
                if (Config.optimizeDBStorage)
                {
                    Logging.info("SQLite: Performing pre-start DB compaction and optimization.");
                    foreach (string db in Directory.EnumerateFiles(Path.Combine(pathBase, "0000"), "*.dat"))
                    {
                        Logging.info("SQLite: Optimizing [{0}].", db);
                        var con = getSQLiteConnection(db, false);
                        executeSQL(con, "VACUUM;");
                        con.Close();
                        con.Dispose();
                    }
                    Logging.info("SQLite: Pre-start optimnization complete.");
                }


                // Get latest block number to initialize the cache as well
                ulong last_block = getHighestBlockInStorage();
                Logging.info(string.Format("Last storage block number is: #{0}", last_block));

                return true;
            }

            protected override void cleanupCache()
            {
                lock (connectionCache)
                {
                    long curTime = Clock.getTimestamp();
                    Dictionary<string, object[]> tmpConnectionCache = new Dictionary<string, object[]>(connectionCache);
                    bool entryExpired = false;
                    foreach (var entry in tmpConnectionCache)
                    {
                        if (curTime - (long)entry.Value[1] > 60)
                        {
                            if (entry.Value[0] == sqlConnection)
                            {
                                // never close the currently used sqlConnection
                                continue;
                            }
                            SQLiteConnection connection = (SQLiteConnection)entry.Value[0];
                            connection.Close();
                            connection.Dispose();
                            connectionCache.Remove(entry.Key);
                            entryExpired = true;
                        }
                    }

                    if(entryExpired)
                    {
                        // Fix for occasional locked database error
                        GC.Collect();
                        GC.WaitForPendingFinalizers();
                        // End of fix
                    }

                    var tmpCache = connectionCache.OrderBy(x => x.Value[1]);
                    for (int i = 0; i < tmpCache.Count() && connectionCache.Count > CONNECTION_CACHE_LIMIT; i++)
                    {
                        if (tmpCache.ElementAt(i).Value[0] == sqlConnection)
                        {
                            // never close the currently used sqlConnection
                            continue;
                        }
                        connectionCache.Remove(tmpCache.ElementAt(i).Key);
                    }
                }
            }

            private SQLiteConnection getSQLiteConnection(string path, bool cache = false)
            {
                lock (connectionCache)
                {
                    if (connectionCache.ContainsKey(path))
                    {
                        if (cache)
                        {
                            connectionCache[path][1] = Clock.getTimestamp();
                            cleanupCache();
                        }
                        return (SQLiteConnection)connectionCache[path][0];
                    }

                    SQLiteConnection connection = new SQLiteConnection(path);
                    try
                    {
                        connection.ExecuteScalar<string>("PRAGMA journal_mode=WAL;");
                        //connection.ExecuteScalar<string>("PRAGMA locking_mode=EXCLUSIVE;");

                        // check if database exists
                        var tableInfo = connection.GetTableInfo("transactions");

                        if (!tableInfo.Any())
                        {
                            // The database needs to be prepared first
                            // Create the blocks table
                            try
                            {
                                string sql = "CREATE TABLE `blocks` (`blockNum`	INTEGER NOT NULL, `blockChecksum` BLOB, `lastBlockChecksum` BLOB, `walletStateChecksum`	BLOB, `sigFreezeChecksum` BLOB, `difficulty` INTEGER, `powField` BLOB, `transactions` TEXT, `signatures` TEXT, `timestamp` INTEGER, `version` INTEGER, `lastSuperBlockChecksum` BLOB, `lastSuperBlockNum` INTEGER, `superBlockSegments` BLOB, `compactedSigs` INTEGER, `blockProposer` BLOB, `signerDifficulty` INTEGER, PRIMARY KEY(`blockNum`));";
                                executeSQL(connection, sql);

                                sql = "CREATE TABLE `transactions` (`id` TEXT, `type` INTEGER, `amount` TEXT, `fee` TEXT, `toList` TEXT, `fromList` TEXT, `dataChecksum` BLOB, `data` BLOB, `blockHeight` INTEGER, `nonce` INTEGER, `timestamp` INTEGER, `checksum` BLOB, `signature` BLOB, `pubKey` BLOB, `applied` INTEGER, `version` INTEGER, PRIMARY KEY(`id`));";
                                executeSQL(connection, sql);
                                sql = "CREATE INDEX `type` ON `transactions` (`type`);";
                                executeSQL(connection, sql);
                                sql = "CREATE INDEX `toList` ON `transactions` (`toList`);";
                                executeSQL(connection, sql);
                                sql = "CREATE INDEX `fromList` ON `transactions` (`fromList`);";
                                executeSQL(connection, sql);
                                sql = "CREATE INDEX `applied` ON `transactions` (`applied`);";
                                executeSQL(connection, sql);
                            }catch(Exception)
                            {
                                connection.Close();
                                connection.Dispose();
                                connection = null;

                                // Fix for occasional locked database error
                                GC.Collect();
                                GC.WaitForPendingFinalizers();
                                // End of fix

                                File.Delete(path);

                                throw;
                            }
                        }
                        else if (!tableInfo.Exists(x => x.Name == "fromList"))
                        {
                            string sql = "ALTER TABLE `transactions` ADD COLUMN `fromList` TEXT;";
                            executeSQL(connection, sql);
                            sql = "CREATE INDEX `fromList` ON `transactions` (`fromList`);";
                            executeSQL(connection, sql);
                        }
                        else if (!tableInfo.Exists(x => x.Name == "dataChecksum"))
                        {
                            string sql = "ALTER TABLE `transactions` ADD COLUMN `dataChecksum` BLOB;";
                            executeSQL(connection, sql);
                        }

                        tableInfo = connection.GetTableInfo("blocks");
                        if (!tableInfo.Exists(x => x.Name == "compactedSigs"))
                        {
                            string sql = "ALTER TABLE `blocks` ADD COLUMN `compactedSigs` INTEGER;";
                            executeSQL(connection, sql);

                            sql = "ALTER TABLE `blocks` ADD COLUMN `lastSuperBlockChecksum` BLOB;";
                            executeSQL(connection, sql);

                            sql = "ALTER TABLE `blocks` ADD COLUMN `lastSuperBlockNum` INTEGER;";
                            executeSQL(connection, sql);

                            sql = "ALTER TABLE `blocks` ADD COLUMN `superBlockSegments` BLOB;";
                            executeSQL(connection, sql);
                        }

                        if (!tableInfo.Exists(x => x.Name == "blockProposer"))
                        {
                            string sql = "ALTER TABLE `blocks` ADD COLUMN `blockProposer` BLOB;";
                            executeSQL(connection, sql);
                        }

                        if (!tableInfo.Exists(x => x.Name == "signerDifficulty"))
                        {
                            string sql = "ALTER TABLE `blocks` ADD COLUMN `signerDifficulty` INTEGER;";
                            executeSQL(connection, sql);
                        }

                        if (cache)
                        {
                            connectionCache.Add(path, new object[2] { connection, Clock.getTimestamp() });
                        }
                        return connection;
                    }
                    catch (Exception e)
                    {
                        Logging.error("Error opening SQLiteConnection file {0}, exception: {1}", path, e);
                        if (connection != null)
                        {
                            connection.Close();
                            connection.Dispose();

                            // Fix for occasional locked database error
                            GC.Collect();
                            GC.WaitForPendingFinalizers();
                            // End of fix
                        }
                        throw;
                    }
                }
            }

            // Returns true if connection to matching blocknum range database is established
            private bool seekDatabase(ulong blocknum = 0, bool cache = false)
            {
                if(!running)
                {
                    return false;
                }
                lock (storageLock)
                {
                    ulong db_blocknum = ((ulong)(blocknum / Config.maxBlocksPerDatabase)) * Config.maxBlocksPerDatabase;

                    // Check if the current seek location matches this block range
                    if (current_seek == db_blocknum)
                    {
                        return true;
                    }

                    // Update the current seek number
                    current_seek = db_blocknum;

                    string db_path = pathBase + Path.DirectorySeparatorChar + "0000" + Path.DirectorySeparatorChar + db_blocknum + ".dat";

                    // Bind the connection
                    sqlConnection = getSQLiteConnection(db_path, cache);
                }
                return true;
            }

            // Go through all database files until we discover the latest consecutive one
            // Doing it this way prevents skipping over inexistent databases
            // returns 1 on failure
            private ulong seekLatestDatabase()
            {
                ulong db_blocknum = 0;
                bool found = false;

                while (!found)
                {
                    string db_path = pathBase + Path.DirectorySeparatorChar + "0000" + Path.DirectorySeparatorChar + db_blocknum + ".dat";
                    if (File.Exists(db_path))
                    {
                        db_blocknum += Config.maxBlocksPerDatabase;
                    }
                    else
                    {
                        if (db_blocknum > 0)
                        {
                            db_blocknum -= Config.maxBlocksPerDatabase;
                        }
                        found = true;
                    }
                }

                if (seekDatabase(db_blocknum, true))
                {
                    // Seek the found database
                    return db_blocknum;
                }
                return 1;
            }

            public override ulong getHighestBlockInStorage()
            {
                if (cached_lastBlockNum == 0)
                {
                    lock (storageLock)
                    {
                        ulong db_block_num = seekLatestDatabase();

                        _storage_Block[] _storage_block = null;
                        if (db_block_num != 1)
                        {
                            string sql = string.Format("SELECT * FROM `blocks` ORDER BY `blockNum` DESC LIMIT 1");
                            _storage_block = sqlConnection.Query<_storage_Block>(sql).ToArray();
                        }

                        if (_storage_block == null)
                        {
                            return db_block_num;
                        }

                        if (_storage_block.Length < 1)
                        {
                            return db_block_num;
                        }

                        _storage_Block blk = _storage_block[0];
                        cached_lastBlockNum = (ulong)blk.blockNum;
                    }
                }
                return cached_lastBlockNum;
            }

            protected override bool insertBlockInternal(Block block)
            {
                string transactions = "";
                foreach (byte[] tx in block.transactions)
                {
                    transactions += "||" + Transaction.txIdV8ToLegacy(tx);
                }

                List<BlockSignature> tmp_sigs = null;
                if(block.frozenSignatures != null)
                {
                    tmp_sigs = block.frozenSignatures;
                }else
                {
                    tmp_sigs = block.signatures;
                }

                string signatures = "";
                foreach (BlockSignature sig in tmp_sigs)
                {
                    string str_sig = "0";
                    if(sig.signature != null)
                    {
                        str_sig = Convert.ToBase64String(sig.signature);
                    }
                    string str_powSolution = "";
                    if(sig.powSolution != null)
                    {
                        str_powSolution = Crypto.hashToString(sig.powSolution.getBytes());
                    }
                    signatures += "||" + str_sig + ":" + Convert.ToBase64String(sig.signerAddress.getInputBytes()) + ":" + str_powSolution;
                }

                if (!Node.blockProcessor.verifySigFreezedBlock(block))
                {
                    return false;
                }

                bool result = false;
 
                // prepare superBlockSegments
                List<byte> super_block_segments = new List<byte>();
                if (block.lastSuperBlockChecksum != null)
                {
                    // this is a superblock

                    foreach(var entry in block.superBlockSegments)
                    {
                        super_block_segments.AddRange(BitConverter.GetBytes(entry.Value.blockNum));
                        super_block_segments.AddRange(BitConverter.GetBytes(entry.Value.blockChecksum.Length));
                        super_block_segments.AddRange(entry.Value.blockChecksum);
                    }

                    lock (superBlockStorageLock)
                    {
                        string sql = "INSERT OR REPLACE INTO `blocks`(`blockNum`,`blockChecksum`,`lastBlockChecksum`,`walletStateChecksum`,`sigFreezeChecksum`, `difficulty`, `powField`, `transactions`,`signatures`,`timestamp`,`version`,`lastSuperBlockChecksum`,`lastSuperBlockNum`,`superBlockSegments`,`compactedSigs`,`blockProposer`,`signerDifficulty`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";
                        result = executeSQL(superBlocksSqlConnection, sql, (long)block.blockNum, block.blockChecksum, block.lastBlockChecksum, block.walletStateChecksum, block.signatureFreezeChecksum, (long)block.difficulty, block.powField, transactions, signatures, block.timestamp, block.version, block.lastSuperBlockChecksum, (long)block.lastSuperBlockNum, super_block_segments.ToArray(), block.compactedSigs, block.blockProposer,(long)block.signerBits);
                    }
                }

                lock (storageLock)
                {
                    seekDatabase(block.blockNum, true);

                    string sql = "INSERT OR REPLACE INTO `blocks`(`blockNum`,`blockChecksum`,`lastBlockChecksum`,`walletStateChecksum`,`sigFreezeChecksum`, `difficulty`, `powField`, `transactions`,`signatures`,`timestamp`,`version`,`lastSuperBlockChecksum`,`lastSuperBlockNum`,`superBlockSegments`,`compactedSigs`,`blockProposer`,`signerDifficulty`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";
                    result = executeSQL(sql, (long)block.blockNum, block.blockChecksum, block.lastBlockChecksum, block.walletStateChecksum, block.signatureFreezeChecksum, (long)block.difficulty, block.powField, transactions, signatures, block.timestamp, block.version, block.lastSuperBlockChecksum, (long)block.lastSuperBlockNum, super_block_segments.ToArray(), block.compactedSigs, block.blockProposer, (long)block.signerBits);
                }

                if (result)
                {
                    // Update the cached last block number if necessary
                    if (getHighestBlockInStorage() < block.blockNum)
                    {
                        cached_lastBlockNum = block.blockNum;
                    }
                }

                return result;
            }

            protected override bool insertTransactionInternal(Transaction transaction)
            {
                string toList = "";
                foreach (var to in transaction.toList)
                {
                    toList += "||" + Base58Check.Base58CheckEncoding.EncodePlain(to.Key.addressNoChecksum)  + ":" + Convert.ToBase64String(to.Value.getAmount().ToByteArray());
                }

                string fromList = "";
                foreach (var from in transaction.fromList)
                {
                    fromList += "||" + Base58Check.Base58CheckEncoding.EncodePlain(from.Key) + ":" + Convert.ToBase64String(from.Value.getAmount().ToByteArray());
                }

                byte[] tx_data_shuffled = shuffleStorageBytes(transaction.data);

                bool result = false;
                lock (storageLock)
                {
                    // Transaction was not found in any existing database, seek to the proper database
                    seekDatabase(transaction.applied, true);

                    string sql = "INSERT OR REPLACE INTO `transactions`(`id`,`type`,`amount`,`fee`,`toList`,`fromList`,`dataChecksum`,`data`,`blockHeight`, `nonce`, `timestamp`,`checksum`,`signature`, `pubKey`, `applied`, `version`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";
                    result = executeSQL(sql, Transaction.txIdV8ToLegacy(transaction.id), transaction.type, transaction.amount.ToString(), transaction.fee.ToString(), toList, fromList, transaction.dataChecksum, tx_data_shuffled, (long)transaction.blockHeight, transaction.nonce, transaction.timeStamp, transaction.checksum, transaction.signature, transaction.pubKey, (long)transaction.applied, transaction.version);
                }

                return result;
            }

            private Block getSuperBlock(ulong blocknum)
            {
                if (blocknum < 1)
                {
                    return null;
                }

                string sql = "select * from blocks where `blocknum` = ? LIMIT 1";
                List<_storage_Block> _storage_block = null;

                lock (superBlockStorageLock)
                {
                    try
                    {
                        _storage_block = superBlocksSqlConnection.Query<_storage_Block>(sql, (long)blocknum);
                    }
                    catch (Exception e)
                    {
                        Logging.error(String.Format("Exception has been thrown while executing SQL Query {0}. Exception message: {1}", sql, e.Message));
                        return null;
                    }
                }

                if (_storage_block == null)
                {
                    return null;
                }

                if (_storage_block.Count < 1)
                {
                    return null;
                }

                return getBlockFromStorageBlock(_storage_block[0]);
            }

            private Block getSuperBlock(byte[] checksum)
            {
                if (checksum == null)
                {
                    return null;
                }

                string sql = "select * from blocks where `blockChecksum` = ? LIMIT 1";
                List<_storage_Block> _storage_block = null;

                lock (superBlockStorageLock)
                {
                    try
                    {
                        _storage_block = superBlocksSqlConnection.Query<_storage_Block>(sql, checksum);
                    }
                    catch (Exception e)
                    {
                        Logging.error(String.Format("Exception has been thrown while executing SQL Query {0}. Exception message: {1}", sql, e.Message));
                        return null;
                    }
                }

                if (_storage_block == null)
                {
                    return null;
                }

                if (_storage_block.Count < 1)
                {
                    return null;
                }

                return getBlockFromStorageBlock(_storage_block[0]);
            }

            private Block getNextSuperBlock(ulong blocknum)
            {
                if (blocknum < 1)
                {
                    return null;
                }

                string sql = "select * from blocks where `lastSuperBlockNum` = ? LIMIT 1";
                List<_storage_Block> _storage_block = null;

                lock (superBlockStorageLock)
                {
                    try
                    {
                        _storage_block = superBlocksSqlConnection.Query<_storage_Block>(sql, (long)blocknum);
                    }
                    catch (Exception e)
                    {
                        Logging.error(String.Format("Exception has been thrown while executing SQL Query {0}. Exception message: {1}", sql, e.Message));
                        return null;
                    }
                }

                if (_storage_block == null)
                {
                    return null;
                }

                if (_storage_block.Count < 1)
                {
                    return null;
                }

                return getBlockFromStorageBlock(_storage_block[0]);
            }

            private Block getNextSuperBlock(byte[] checksum)
            {
                if (checksum == null)
                {
                    return null;
                }

                string sql = "select * from blocks where `lastSuperBlockChecksum` = ? LIMIT 1";
                List<_storage_Block> _storage_block = null;

                lock (superBlockStorageLock)
                {
                    try
                    {
                        _storage_block = superBlocksSqlConnection.Query<_storage_Block>(sql, checksum);
                    }
                    catch (Exception e)
                    {
                        Logging.error(String.Format("Exception has been thrown while executing SQL Query {0}. Exception message: {1}", sql, e.Message));
                        return null;
                    }
                }

                if (_storage_block == null)
                {
                    return null;
                }

                if (_storage_block.Count < 1)
                {
                    return null;
                }

                return getBlockFromStorageBlock(_storage_block[0]);
            }

            private static Block getBlockFromStorageBlock(_storage_Block storage_block)
            {
                var sw = new System.Diagnostics.Stopwatch();
                sw.Start();

                _storage_Block blk = storage_block;

                Block block = new Block
                {
                    blockNum = (ulong)blk.blockNum,
                    blockChecksum = blk.blockChecksum,
                    lastBlockChecksum = blk.lastBlockChecksum,
                    walletStateChecksum = blk.walletStateChecksum,
                    signatureFreezeChecksum = blk.sigFreezeChecksum,
                    difficulty = (ulong)blk.difficulty,
                    powField = blk.powField,
                    transactions = new HashSet<byte[]>(new ByteArrayComparer()),
                    signatures = new List<BlockSignature>(),
                    timestamp = blk.timestamp,
                    version = blk.version,
                    lastSuperBlockChecksum = blk.lastSuperBlockChecksum,
                    lastSuperBlockNum = (ulong)blk.lastSuperBlockNum,
                    compactedSigs = blk.compactedSigs,
                    blockProposer = blk.blockProposer,
                    signerBits = (uint)blk.signerDifficulty
                };

                try
                {
                    
                    // Add signatures
                    List<BlockSignature> frozen_signatures = new List<BlockSignature>();
                    string[] split_str = blk.signatures.Split(new string[] { "||" }, StringSplitOptions.None);
                    int sigcounter = 0;

                    // Temporary cache of signature addresses for this block
                    List<byte[]> cached_sig_addresses = new List<byte[]>();

                    foreach (string s1 in split_str)
                    {
                        sigcounter++;
                        if (sigcounter == 1)
                        {
                            continue;
                        }
                        
                        string[] split_sig = s1.Split(new string[] { ":" }, StringSplitOptions.None);
                        if (split_sig.Length < 2)
                        {
                            continue;
                        }
                        
                        BlockSignature newSig = new BlockSignature();
                        if (split_sig[0] != "0")
                        {
                            newSig.signature = Convert.FromBase64String(split_sig[0]);
                        }
                        newSig.signerAddress = new Address(Convert.FromBase64String(split_sig[1]), null, false);
                        if(split_sig.Length >= 3 && split_sig[2] != "")
                        {
                            newSig.powSolution = new SignerPowSolution(Crypto.stringToHash(split_sig[2]), newSig.signerAddress.addressNoChecksum);
                        }
                         
                        // Go through all block signatures and check if the resulting address matches
                        byte[] sig_address = newSig.signerAddress.addressNoChecksum;

                        bool found = false;
                        foreach (byte[] bsig in cached_sig_addresses)
                        {
                             if (bsig.SequenceEqual(sig_address))
                             {
                                 found = true;
                                 break;
                             }
                        }

                        // Signature is not already in the block, add it
                        if(!found)
                        {
                            block.signatures.Add(newSig);
                            frozen_signatures.Add(newSig);
                            cached_sig_addresses.Add(sig_address);
                        }
                    }
                    cached_sig_addresses.Clear();

                    block.setFrozenSignatures(frozen_signatures);
                    //Logging.trace("Signature parse: {0}ms", sw.Elapsed.TotalMilliseconds);

                    // Add transaction
                    string[] split_str2 = blk.transactions.Split(new string[] { "||" }, StringSplitOptions.None);
                    int txcounter = 0;
                    foreach (string s1 in split_str2)
                    {
                        txcounter++;
                        if (txcounter == 1)
                        {
                            continue;
                        }

                        block.addTransaction(Transaction.txIdLegacyToV8(s1));
                    }
                    //Logging.trace("Tx parse: {0}ms", sw.Elapsed.TotalMilliseconds);
                    
                    if (blk.superBlockSegments != null)
                    {
                        for (int i = 0; i < blk.superBlockSegments.Length;)
                        {
                            ulong seg_block_num = BitConverter.ToUInt64(blk.superBlockSegments, i);
                            i += 8;
                            int seg_bc_len = BitConverter.ToInt32(blk.superBlockSegments, i);
                            i += 4;
                            byte[] seg_bc = new byte[seg_bc_len];
                            Array.Copy(blk.superBlockSegments, i, seg_bc, 0, seg_bc_len);
                            i += seg_bc_len;

                            block.superBlockSegments.Add(seg_block_num, new SuperBlockSegment(seg_block_num, seg_bc));
                        }
                    }
                    block.fromLocalStorage = true;
                }
                catch (Exception e)
                {
                    Logging.error("Error reading block #{0} from storage: {1}", blk.blockNum, e);
                }

                sw.Stop();
                Logging.trace("|- Local block #{0} fetch took: {1}ms", blk.blockNum, sw.Elapsed.TotalMilliseconds);

                return block;
            }

            public override Block getBlock(ulong blocknum)
            {
                var sw = new System.Diagnostics.Stopwatch();
                sw.Start();
                List<_storage_Block> _storage_block = null;
                if (blocknum < 1)
                {
                    return null;
                }

                string sql = "select * from blocks where `blocknum` = ? LIMIT 1"; // AND `blocknum` < (SELECT MAX(`blocknum`) - 5 from blocks)

                lock (storageLock)
                {
                    if(blocknum > getHighestBlockInStorage())
                    {
                        Logging.error("Tried to get block {0} but the highest block in storage is {1}", blocknum, getHighestBlockInStorage());
                        return null;
                    }
                    seekDatabase(blocknum, true);

                    try
                    {
                        _storage_block = sqlConnection.Query<_storage_Block>(sql, (long)blocknum);
                    }
                    catch (Exception e)
                    {
                        Logging.error(String.Format("Exception has been thrown while executing SQL Query {0}. Exception message: {1}", sql, e.Message));
                        return null;
                    }
                }

                if (_storage_block == null)
                {
                    return null;
                }

                if (_storage_block.Count < 1)
                {
                    return null;
                }

                sw.Stop();
                Logging.trace("|- Local block #{0} read from storage took: {1}ms", blocknum, sw.Elapsed.TotalMilliseconds);

                return getBlockFromStorageBlock(_storage_block[0]);
            }

            public override Block getBlockByHash(byte[] hash)
            {
                var sw = new System.Diagnostics.Stopwatch();
                sw.Start();

                _storage_Block[] _storage_block = null;
                if (hash == null)
                {
                    return null;
                }

                string sql = "select * from blocks where `blockChecksum` = ? LIMIT 1";

                // Go through each database until the block is found
                // TODO: optimize this for better performance
                lock (storageLock)
                {
                    bool found = false;

                    try
                    {
                        _storage_block = sqlConnection.Query<_storage_Block>(sql, hash).ToArray();
                    }
                    catch (Exception e)
                    {
                        Logging.error(String.Format("Exception has been thrown while executing SQL Query {0}. Exception message: {1}", sql, e.Message));
                        found = false;
                    }

                    if (_storage_block != null)
                    {
                        if (_storage_block.Length > 0)
                        {
                            found = true;
                        }
                    }

                    ulong db_blocknum = getHighestBlockInStorage();
                    while (!found)
                    {
                        // Block not found yet, seek to another database
                        seekDatabase(db_blocknum, true);
                        try
                        {
                            _storage_block = sqlConnection.Query<_storage_Block>(sql, hash).ToArray();

                        }
                        catch (Exception)
                        {
                            if (db_blocknum > Config.maxBlocksPerDatabase)
                            {
                                db_blocknum -= Config.maxBlocksPerDatabase;
                            }
                            else
                            {
                                // Block not found
                                return null;
                            }
                        }

                        if (_storage_block == null || _storage_block.Length < 1)
                        {
                            if (db_blocknum > Config.maxBlocksPerDatabase)
                            {
                                db_blocknum -= Config.maxBlocksPerDatabase;
                            }
                            else
                            {
                                // Block not found in any database
                                return null;
                            }
                            continue;
                        }

                        found = true;
                    }
                }

                if (_storage_block == null)
                {
                    return null;
                }

                if (_storage_block.Length < 1)
                {
                    return null;
                }

                sw.Stop();
                Logging.trace("|- Local block #{0} read from storage took: {1}ms", _storage_block[0].blockNum, sw.Elapsed.TotalMilliseconds);

                return getBlockFromStorageBlock(_storage_block[0]);
            }

            public override Block getBlockByLastSBHash(byte[] checksum)
            {
                if (checksum == null)
                {
                    return null;
                }

                string sql = "select * from blocks where `lastSuperBlockChecksum` = ? LIMIT 1";
                List<_storage_Block> _storage_block = null;

                lock (superBlockStorageLock)
                {
                    try
                    {
                        _storage_block = superBlocksSqlConnection.Query<_storage_Block>(sql, checksum);
                    }
                    catch (Exception e)
                    {
                        Logging.error(String.Format("Exception has been thrown while executing SQL Query {0}. Exception message: {1}", sql, e.Message));
                        return null;
                    }
                }

                if (_storage_block == null)
                    return null;

                if (_storage_block.Count < 1)
                    return null;

                return getBlockFromStorageBlock(_storage_block[0]);
            }


            // Retrieve a transaction from the sql database
            public override Transaction getTransaction(byte[] txid, ulong block_num)
            {
                Transaction transaction = null;
                List<_storage_Transaction> _storage_tx = null;

                string sql = "select * from transactions where `id` = ? LIMIT 1";

                // Go through each database until the transaction is found
                // TODO: optimize this for better performance
                ulong highest_blocknum = getHighestBlockInStorage();
                lock (storageLock)
                {
                    bool found = false;
                    try
                    {
                        if(block_num > 0)
                        {
                            if (block_num > highest_blocknum)
                            {
                                Logging.error("Tried to get transaction requested for block {0} but the highest block in storage is {1}", block_num, highest_blocknum);
                                return null;
                            }
                            seekDatabase(block_num, true);
                        }
                        _storage_tx = sqlConnection.Query<_storage_Transaction>(sql, Transaction.txIdV8ToLegacy(txid));

                    }
                    catch (Exception e)
                    {
                        Logging.error("Exception has been thrown while executing SQL Query {0}. Exception message: {1}", sql, e.Message);
                        found = false;
                    }

                    if (_storage_tx != null)
                    {
                        if (_storage_tx.Count > 0)
                        {
                            found = true;
                        }
                    }

                    if(!found && block_num > 0)
                    {
                        return transaction;
                    }

                    if(!found)
                    {
                        // extract blockheight from txid
                        ulong db_blocknum = IxiVarInt.GetIxiVarUInt(txid, 1).num;

                        if (db_blocknum == 0)
                        {
                            Logging.error("Invalid txid {0} - generated at block height 0.", Transaction.txIdV8ToLegacy(txid));
                            return null;
                        }

                        db_blocknum = ((ulong)(db_blocknum / Config.maxBlocksPerDatabase)) * Config.maxBlocksPerDatabase;

                        if (db_blocknum > highest_blocknum)
                        {
                            Logging.error("Tried to get transaction generated on block {0} but the highest block in storage is {1}", db_blocknum, highest_blocknum);
                            return null;
                        }

                        if(highest_blocknum > db_blocknum + ConsensusConfig.getRedactedWindowSize(2))
                        {
                            highest_blocknum = db_blocknum + ConsensusConfig.getRedactedWindowSize(2);
                        }

                        while (!found)
                        {
                            // Transaction not found yet, seek to another database
                            seekDatabase(db_blocknum, true);
                            try
                            {
                                _storage_tx = sqlConnection.Query<_storage_Transaction>(sql, Transaction.txIdV8ToLegacy(txid));

                            }
                            catch (Exception)
                            {
                                if (db_blocknum + Config.maxBlocksPerDatabase <= highest_blocknum)
                                {
                                    db_blocknum += Config.maxBlocksPerDatabase;
                                }
                                else
                                {
                                    // Transaction not found
                                    return transaction;
                                }
                            }

                            if (_storage_tx == null || _storage_tx.Count < 1)
                            {
                                if (db_blocknum + Config.maxBlocksPerDatabase <= highest_blocknum)
                                {
                                    db_blocknum += Config.maxBlocksPerDatabase;
                                }
                                else
                                {
                                    // Transaction not found in any database
                                    return transaction;
                                }
                                continue;
                            }

                            found = true;
                        }
                    }
                }


                if (_storage_tx.Count < 1)
                {
                    return transaction;
                }

                _storage_Transaction tx = _storage_tx[0];

                transaction = new Transaction(tx.type, tx.dataChecksum, unshuffleStorageBytes(tx.data))
                {
                    id = Transaction.txIdLegacyToV8(tx.id),
                    amount = new IxiNumber(tx.amount),
                    fee = new IxiNumber(tx.fee),
                    blockHeight = (ulong)tx.blockHeight,
                    nonce = tx.nonce,
                    timeStamp = tx.timestamp,
                    checksum = tx.checksum,
                    signature = tx.signature,
                    version = tx.version,
                    pubKey = tx.pubKey,
                    applied = (ulong)tx.applied
                };

                try
                {
                    // Add toList
                    string[] split_str = tx.toList.Split(new string[] { "||" }, StringSplitOptions.None);
                    int sigcounter = 0;
                    foreach (string s1 in split_str)
                    {
                        sigcounter++;
                        if (sigcounter == 1)
                        {
                            continue;
                        }

                        string[] split_to = s1.Split(new string[] { ":" }, StringSplitOptions.None);
                        if (split_to.Length < 2)
                        {
                            continue;
                        }
                        byte[] address = Base58Check.Base58CheckEncoding.DecodePlain(split_to[0]);
                        IxiNumber amount = new IxiNumber(new BigInteger(Convert.FromBase64String(split_to[1])));
                        transaction.toList.AddOrReplace(new Address(address, null, false), amount);
                    }

                    if (tx.from != null)
                    {
                        if (tx.pubKey == null)
                        {
                            transaction.pubKey = tx.from;
                        }
                        transaction.fromList.Add(new byte[1] { 0 }, transaction.amount + transaction.fee);
                    }
                    else
                    {
                        // Add fromList
                        split_str = tx.fromList.Split(new string[] { "||" }, StringSplitOptions.None);
                        sigcounter = 0;
                        foreach (string s1 in split_str)
                        {
                            sigcounter++;
                            if (sigcounter == 1)
                            {
                                continue;
                            }

                            string[] split_from = s1.Split(new string[] { ":" }, StringSplitOptions.None);
                            if (split_from.Length < 2)
                            {
                                continue;
                            }
                            byte[] address = Base58Check.Base58CheckEncoding.DecodePlain(split_from[0]);
                            IxiNumber amount = new IxiNumber(new BigInteger(Convert.FromBase64String(split_from[1])));
                            transaction.fromList.AddOrReplace(address, amount);
                        }
                    }

                    transaction.fromLocalStorage = true;
                }catch(Exception e)
                {
                    Logging.error("Error reading transaction {0} from storage: {1}", tx.id, e);
                }

                return transaction;
            }

            // Removes a block from the storage database
            public override bool removeBlock(ulong blockNum, bool remove_transactions)
            {
                // Only remove on non-history nodes
                if (Config.storeFullHistory == true)
                {
                    return false;
                }

                lock (storageLock)
                {
                    if (blockNum > getHighestBlockInStorage())
                    {
                        Logging.error("Tried to remove block {0} but the highest block in storage is {1}", blockNum, getHighestBlockInStorage());
                        return false;
                    }
                    seekDatabase(blockNum, true);
                    Block b = getBlock(blockNum);
                    if (b == null)
                    {
                        return true;
                    }

                    // First go through all transactions and remove them from storage
                    foreach (byte[] txid in b.transactions)
                    {
                        if (removeTransaction(txid) == false)
                        {
                            return false;
                        }
                    }

                    // Now remove the block itself from storage
                    string sql = "DELETE FROM blocks where `blockNum` = ? LIMIT 1";
                    return executeSQL(sql, blockNum);
                }
            }

            // Removes a transaction from the storage database
            // Warning: make sure this is called on the corresponding database (seeked to the blocknum of this transaction)
            public override bool removeTransaction(byte[] txid, ulong blockNum = 0)
            {
                lock (storageLock)
                {
                    string sql = "DELETE FROM transactions where `id` = ? LIMIT 1";
                    return executeSQL(sql, Transaction.txIdV8ToLegacy(txid));
                }
            }

            // Escape and execute an sql command
            private bool executeSQL(string sql, params object[] sqlParameters)
            {
                return executeSQL(sqlConnection, sql, sqlParameters);
            }

            // Escape and execute an sql command
            private bool executeSQL(SQLiteConnection connection, string sql, params object[] sqlParameters)
            {
                try
                {
                    if (connection.Execute(sql, sqlParameters) > 0)
                    {
                        return true;
                    }
                }
                catch (SQLiteException e)
                {
                    if (e.Result == SQLite3.Result.Corrupt)
                    {
                        lock (storageLock)
                        {
                            lock (connectionCache)
                            {
                                string fileName = Path.GetFileNameWithoutExtension(connection.DatabasePath);
                                string fullFilePath = connection.DatabasePath;
                                if (fileName == "superblocks")
                                {
                                    if (superBlocksSqlConnection != null)
                                    {
                                        superBlocksSqlConnection.Close();
                                        superBlocksSqlConnection.Dispose();
                                        superBlocksSqlConnection = null;
                                    }
                                }

                                resetConnectionCache();

                                if (File.Exists(fullFilePath + "-shm") || File.Exists(fullFilePath + "-wal"))
                                {
                                    // First try removing the recovery files
                                    try
                                    {
                                        File.Delete(fullFilePath + "-shm");
                                    }
                                    catch (Exception ex)
                                    {
                                        Logging.error("Error deleting file " + fullFilePath + "-shm: " + ex);
                                    }

                                    try
                                    {
                                        File.Delete(fullFilePath + "-wal");
                                    }
                                    catch (Exception ex)
                                    {
                                        Logging.error("Error deleting file " + fullFilePath + "-wal: " + ex);
                                    }
                                    Logging.warn("Deleted recovery files for database " + fullFilePath);
                                }
                                else if (File.Exists(fullFilePath))
                                {
                                    Logging.warn("Repairing database file " + fullFilePath);
                                    Logging.flush();

                                    repairDatabase(fullFilePath);
                                    
                                    Logging.warn("Repaired database file " + fullFilePath);
                                }

                                if (superBlocksSqlConnection == null)
                                {
                                    superBlocksSqlConnection = getSQLiteConnection(fullFilePath, false);
                                }
                            }
                        }
                    }

                    throw;
                }
                return false;
            }

            private bool repairDatabase(string path)
            {
                if(!File.Exists(path))
                {
                    return false;
                }

                string tmpPath = path + ".tmp";
                string destPath = path;

                SQLiteConnection srcCon = null;
                SQLiteConnection destCon = null;
                try
                {
                    if (File.Exists(tmpPath))
                    {
                        File.Delete(tmpPath);
                    }

                    File.Move(destPath, tmpPath);

                    srcCon = getSQLiteConnection(tmpPath, false);
                    destCon = getSQLiteConnection(destPath, false);

                    srcCon.Execute("PRAGMA writable_schema=ON;");

                    for (int i = 0; running; i += 10)
                    {
                        try
                        {
                            var blocks = srcCon.Query<_storage_Block>("SELECT * FROM `blocks` LIMIT " + i + ",10");
                            if (blocks.Count == 0)
                            {
                                break;
                            }
                            foreach (var block in blocks)
                            {
                                string sql = "INSERT OR REPLACE INTO `blocks`(`blockNum`,`blockChecksum`,`lastBlockChecksum`,`walletStateChecksum`,`sigFreezeChecksum`, `difficulty`, `powField`, `transactions`,`signatures`,`timestamp`,`version`,`lastSuperBlockChecksum`,`lastSuperBlockNum`,`superBlockSegments`,`compactedSigs`,`blockProposer`,`signerDifficulty`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";
                                executeSQL(sql, block.blockNum, block.blockChecksum, block.lastBlockChecksum, block.walletStateChecksum, block.sigFreezeChecksum, block.difficulty, block.powField, block.transactions, block.signatures, block.timestamp, block.version, block.lastSuperBlockChecksum, block.lastSuperBlockNum, block.superBlockSegments, block.compactedSigs, block.blockProposer, block.signerDifficulty);
                            }
                        }
                        catch (Exception e)
                        {
                            Logging.warn("Cannot recover blocks from database: " + e);
                            break;
                        }
                    }

                    for (int i = 0; running; i += 10)
                    {
                        try
                        {
                            var txs = srcCon.Query<_storage_Transaction>("SELECT * FROM `transactions` LIMIT " + i + ",10");
                            if (txs.Count == 0)
                            {
                                break;
                            }
                            foreach (var tx in txs)
                            {
                                string sql = "INSERT OR REPLACE INTO `transactions`(`id`,`type`,`amount`,`fee`,`toList`,`fromList`,`dataChecksum`,`data`,`blockHeight`, `nonce`, `timestamp`,`checksum`,`signature`, `pubKey`, `applied`, `version`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";
                                destCon.Execute(sql, tx.id, tx.type, tx.amount, tx.fee, tx.toList, tx.fromList, tx.dataChecksum, tx.data, tx.blockHeight, tx.nonce, tx.timestamp, tx.checksum, tx.signature, tx.pubKey, tx.applied, tx.version);
                            }
                        }
                        catch (Exception e)
                        {
                            Logging.warn("Cannot recover transactions from database: " + e);
                            break;
                        }
                    }

                    srcCon.Close();
                    srcCon = null;

                    destCon.Close();
                    destCon = null;

                    // Fix for occasional locked database error
                    GC.Collect();
                    GC.WaitForPendingFinalizers();
                    // End of fix

                    File.Delete(tmpPath);
                    return true;
                }
                catch (SQLiteException e)
                {
                    Logging.error("Error repairing file " + path + ": " + e);
                    if (srcCon != null)
                    {
                        srcCon.Close();
                    }
                    if (destCon != null)
                    {
                        destCon.Close();
                    }

                    // Fix for occasional locked database error
                    GC.Collect();
                    GC.WaitForPendingFinalizers();
                    // End of fix

                    if (e.Result == SQLite3.Result.Corrupt)
                    {
                        if (File.Exists(destPath))
                        {
                            File.Delete(destPath);
                        }

                        if (File.Exists(tmpPath))
                        {
                            File.Delete(tmpPath);
                        }
                    }
                }
                catch (Exception e)
                {
                    Logging.error("Error repairing file " + path + ": " + e);
                    if (srcCon != null)
                    {
                        srcCon.Close();
                    }
                    if (destCon != null)
                    {
                        destCon.Close();
                    }

                    // Fix for occasional locked database error
                    GC.Collect();
                    GC.WaitForPendingFinalizers();
                    // End of fix
                }
                return false;
            }

            // Shuffle data storage bytes
            public byte[] shuffleStorageBytes(byte[] bytes)
            {
                if (bytes == null)
                {
                    return null;
                }
                return bytes.Reverse().ToArray();
            }

            // Unshuffle data storage bytes
            public byte[] unshuffleStorageBytes(byte[] bytes)
            {
                if (bytes == null)
                {
                    return null;
                }
                Array.Reverse(bytes, 0, bytes.Length);
                return bytes;//.Reverse().ToArray();
            }
            public override void deleteData()
            {
                string[] fileNames = Directory.GetFiles(Config.dataFolderPath + Path.DirectorySeparatorChar + "blocks" + Path.DirectorySeparatorChar + "0000");
                foreach(string fileName in fileNames)
                {
                    File.Delete(fileName);
                }
                File.Delete(Config.dataFolderPath + Path.DirectorySeparatorChar + "blocks" + Path.DirectorySeparatorChar + "superblocks.dat");
                File.Delete(Config.dataFolderPath + Path.DirectorySeparatorChar + "blocks" + Path.DirectorySeparatorChar + "superblocks.dat-shm");
                File.Delete(Config.dataFolderPath + Path.DirectorySeparatorChar + "blocks" + Path.DirectorySeparatorChar + "superblocks.dat-wal");
            }
            public override ulong getLowestBlockInStorage()
            {
                throw new NotImplementedException();
            }

            public override IEnumerable<Block> getBlocksByRange(ulong from, ulong to)
            {
                throw new NotImplementedException();
            }

            public override IEnumerable<Transaction> getTransactionsByType(Transaction.Type type, ulong block_from = 0, ulong block_to = 0)
            {
                throw new NotImplementedException();
            }

            public override IEnumerable<Transaction> getTransactionsFromAddress(byte[] from_addr, ulong block_from = 0, ulong block_to = 0)
            {
                throw new NotImplementedException();
            }

            public override IEnumerable<Transaction> getTransactionsToAddress(byte[] to_addr, ulong block_from = 0, ulong block_to = 0)
            {
                throw new NotImplementedException();
            }

            public override IEnumerable<Transaction> getTransactionsInBlock(ulong block_num, int tx_type = -1)
            {
                List<Transaction> transactions = new List<Transaction>();

                List<_storage_Transaction> _storage_tx = null;

                string sql = "select * from transactions where `applied` = ?";

                if (tx_type > -1)
                {
                    sql = "select * from transactions where `applied` = ? and `type` = ?";
                }

                ulong highest_blocknum = getHighestBlockInStorage();
                lock (storageLock)
                {
                    bool found = false;
                    try
                    {
                        if (block_num > 0)
                        {
                            if (block_num > highest_blocknum)
                            {
                                Logging.error("Tried to get transaction requested for block {0} but the highest block in storage is {1}", block_num, highest_blocknum);
                                return null;
                            }
                            seekDatabase(block_num, true);
                        }
                        if (tx_type == -1)
                        {
                            _storage_tx = sqlConnection.Query<_storage_Transaction>(sql, (long)block_num).ToList();
                        }
                        else
                        {
                            _storage_tx = sqlConnection.Query<_storage_Transaction>(sql, (long)block_num, tx_type).ToList();
                        }
                    }
                    catch (Exception e)
                    {
                        Logging.error("Exception has been thrown while executing SQL Query {0}. Exception message: {1}", sql, e.Message);
                        found = false;
                    }

                    if (_storage_tx != null)
                    {
                        if (_storage_tx.Count > 0)
                        {
                            found = true;
                        }
                    }

                    if (!found && block_num > 0)
                    {
                        return transactions;
                    }              
                }


                if (_storage_tx.Count < 1)
                {
                    return transactions;
                }

                foreach(_storage_Transaction tx in _storage_tx)
                {
                    Transaction transaction = new Transaction(tx.type, tx.dataChecksum, unshuffleStorageBytes(tx.data))
                    {
                        id = Transaction.txIdLegacyToV8(tx.id),
                        amount = new IxiNumber(tx.amount),
                        fee = new IxiNumber(tx.fee),
                        blockHeight = (ulong)tx.blockHeight,
                        nonce = tx.nonce,
                        timeStamp = tx.timestamp,
                        checksum = tx.checksum,
                        signature = tx.signature,
                        version = tx.version,
                        pubKey = tx.pubKey,
                        applied = (ulong)tx.applied
                    };

 
                    try
                    {
                        // Add toList
                        string[] split_str = tx.toList.Split(new string[] { "||" }, StringSplitOptions.None);
                        int sigcounter = 0;
                        foreach (string s1 in split_str)
                        {
                            sigcounter++;
                            if (sigcounter == 1)
                            {
                                continue;
                            }

                            string[] split_to = s1.Split(new string[] { ":" }, StringSplitOptions.None);
                            if (split_to.Length < 2)
                            {
                                continue;
                            }
                            byte[] address = Base58Check.Base58CheckEncoding.DecodePlain(split_to[0]);
                            IxiNumber amount = new IxiNumber(new BigInteger(Convert.FromBase64String(split_to[1])));
                            transaction.toList.AddOrReplace(new Address(address, null, false), amount);
                        }

                        if (tx.from != null)
                        {
                            if (tx.pubKey == null)
                            {
                                transaction.pubKey = tx.from;
                            }
                            transaction.fromList.Add(new byte[1] { 0 }, transaction.amount + transaction.fee);
                        }
                        else
                        {
                            // Add fromList
                            split_str = tx.fromList.Split(new string[] { "||" }, StringSplitOptions.None);
                            sigcounter = 0;
                            foreach (string s1 in split_str)
                            {
                                sigcounter++;
                                if (sigcounter == 1)
                                {
                                    continue;
                                }

                                string[] split_from = s1.Split(new string[] { ":" }, StringSplitOptions.None);
                                if (split_from.Length < 2)
                                {
                                    continue;
                                }
                                byte[] address = Base58Check.Base58CheckEncoding.DecodePlain(split_from[0]);
                                IxiNumber amount = new IxiNumber(new BigInteger(Convert.FromBase64String(split_from[1])));
                                transaction.fromList.AddOrReplace(address, amount);
                            }
                        }

                        transaction.fromLocalStorage = true;
                    }
                    catch (Exception e)
                    {
                        Logging.error("Error reading transaction {0} from storage: {1}", tx.id, e);
                    }
 
                    transactions.Add(transaction);

                }

                return transactions;
            }

            public override IEnumerable<Transaction> getTransactionsByTime(long time_from, long time_to)
            {
                throw new NotImplementedException();
            }

            public override IEnumerable<Transaction> getTransactionsApplied(ulong block_from, ulong block_to)
            {
                throw new NotImplementedException();
            }

            private void resetConnectionCache()
            {
                lock (connectionCache)
                {
                    current_seek = 1;
                    sqlConnection = null;
                    foreach (var entry in connectionCache)
                    {
                        SQLiteConnection connection = (SQLiteConnection)entry.Value[0];
                        connection.Close();
                        connection.Dispose();
                    }

                    // Fix for occasional locked database error
                    GC.Collect();
                    GC.WaitForPendingFinalizers();
                    // End of fix

                    connectionCache.Clear();
                }
            }

            protected override void shutdown()
            {
                resetConnectionCache();
            }
        }
    }
}