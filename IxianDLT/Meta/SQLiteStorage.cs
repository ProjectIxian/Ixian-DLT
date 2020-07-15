using IXICore;
using IXICore.Meta;
using SQLite;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Numerics;
using System.Threading;

namespace DLT
{
    namespace Meta
    {
        public class SQLiteStorage : IStorage
        {
            // Sql connections
            private SQLiteConnection sqlConnection = null;
            private readonly object storageLock = new object(); // This should always be placed when performing direct sql operations

            private SQLiteConnection superBlocksSqlConnection = null;
            private readonly object superBlockStorageLock = new object(); // This should always be placed when performing direct sql operations

            private Dictionary<string, object[]> connectionCache = new Dictionary<string, object[]>();

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
                var files = Directory.EnumerateFiles(pathBase + Path.DirectorySeparatorChar + "0000", "*.dat-shm");
                foreach (var file in files)
                {
                    File.Delete(file);
                }

                files = Directory.EnumerateFiles(pathBase + Path.DirectorySeparatorChar + "0000", "*.dat-wal");
                foreach (var file in files)
                {
                    File.Delete(file);
                }

                string db_path = pathBase + Path.DirectorySeparatorChar + "superblocks.dat";

                // Bind the connection
                superBlocksSqlConnection = getSQLiteConnection(db_path, false);


                // Get latest block number to initialize the cache as well
                ulong last_block = getHighestBlockInStorage();
                Logging.info(string.Format("Last storage block number is: #{0}", last_block));

                return true;
            }

            protected override void cleanupCache()
            {
                long curTime = Clock.getTimestamp();
                Dictionary<string, object[]> tmpConnectionCache = new Dictionary<string, object[]>(connectionCache);
                foreach (var entry in tmpConnectionCache)
                {
                    if (curTime - (long)entry.Value[1] > 60)
                    {
                        if(entry.Value[0] == sqlConnection)
                        {
                            // never close the currently used sqlConnection
                            continue;
                        }
                        ((SQLiteConnection)entry.Value[0]).Close();
                        connectionCache.Remove(entry.Key);

                        // Fix for occasional locked database error
                        GC.Collect();
                        GC.WaitForPendingFinalizers();
                        // End of fix
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
                                string sql = "CREATE TABLE `blocks` (`blockNum`	INTEGER NOT NULL, `blockChecksum` BLOB, `lastBlockChecksum` BLOB, `walletStateChecksum`	BLOB, `sigFreezeChecksum` BLOB, `difficulty` INTEGER, `powField` BLOB, `transactions` TEXT, `signatures` TEXT, `timestamp` INTEGER, `version` INTEGER, `lastSuperBlockChecksum` BLOB, `lastSuperBlockNum` INTEGER, `superBlockSegments` BLOB, `compactedSigs` INTEGER, PRIMARY KEY(`blockNum`));";
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
                            }catch(Exception e)
                            {
                                connection.Close();
                                connection.Dispose();
                                connection = null;

                                // Fix for occasional locked database error
                                GC.Collect();
                                GC.WaitForPendingFinalizers();
                                // End of fix

                                File.Delete(path);

                                throw e;
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
                Block b = block;
                string transactions = "";
                foreach (string tx in block.transactions)
                {
                    transactions = string.Format("{0}||{1}", transactions, tx);
                }

                List<byte[][]> tmp_sigs = null;
                if(block.frozenSignatures != null)
                {
                    tmp_sigs = block.frozenSignatures;
                }else
                {
                    tmp_sigs = block.signatures;
                }

                string signatures = "";
                foreach (byte[][] sig in tmp_sigs)
                {
                    string str_sig = "0";
                    if(sig[0] != null)
                    {
                        str_sig = Convert.ToBase64String(sig[0]);
                    }
                    signatures = string.Format("{0}||{1}:{2}", signatures, str_sig, Convert.ToBase64String(sig[1]));
                }

                if (!Node.blockProcessor.verifySigFreezedBlock(block))
                {
                    return false;
                }

                bool result = false;
 
                // prepare superBlockSegments
                List<byte> super_block_segments = new List<byte>();
                lock (superBlockStorageLock)
                {
                    if (block.lastSuperBlockChecksum != null)
                    {
                        // this is a superblock

                        foreach(var entry in block.superBlockSegments)
                        {
                            super_block_segments.AddRange(BitConverter.GetBytes(entry.Value.blockNum));
                            super_block_segments.AddRange(BitConverter.GetBytes(entry.Value.blockChecksum.Length));
                            super_block_segments.AddRange(entry.Value.blockChecksum);
                        }

                        if (getSuperBlock(block.blockNum) == null)
                        {
                            string sql = "INSERT INTO `blocks`(`blockNum`,`blockChecksum`,`lastBlockChecksum`,`walletStateChecksum`,`sigFreezeChecksum`, `difficulty`, `powField`, `transactions`,`signatures`,`timestamp`,`version`,`lastSuperBlockChecksum`,`lastSuperBlockNum`,`superBlockSegments`,`compactedSigs`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";
                            result = executeSQL(superBlocksSqlConnection, sql, (long)block.blockNum, block.blockChecksum, block.lastBlockChecksum, block.walletStateChecksum, block.signatureFreezeChecksum, (long)block.difficulty, block.powField, transactions, signatures, block.timestamp, block.version, block.lastSuperBlockChecksum, (long)block.lastSuperBlockNum, super_block_segments.ToArray(), block.compactedSigs);
                        }
                        else
                        {
                            // Likely already have the block stored, update the old entry
                            string sql = "UPDATE `blocks` SET `blockChecksum` = ?, `lastBlockChecksum` = ?, `walletStateChecksum` = ?, `sigFreezeChecksum` = ?, `difficulty` = ?, `powField` = ?, `transactions` = ?, `signatures` = ?, `timestamp` = ?, `version` = ?, `lastSuperBlockChecksum` = ?, `lastSuperBlockNum` = ?, `superBlockSegments` = ?, `compactedSigs` = ? WHERE `blockNum` = ?";
                            //Console.WriteLine("SQL: {0}", sql);
                            result = executeSQL(superBlocksSqlConnection, sql, block.blockChecksum, block.lastBlockChecksum, block.walletStateChecksum, block.signatureFreezeChecksum, (long)block.difficulty, block.powField, transactions, signatures, block.timestamp, block.version, block.lastSuperBlockChecksum, (long)block.lastSuperBlockNum, super_block_segments.ToArray(), block.compactedSigs, (long)block.blockNum);
                        }
                    }
                }

                lock (storageLock)
                {
                    if (getBlock(block.blockNum) == null)
                    {
                        seekDatabase(block.blockNum, true);

                        string sql = "INSERT INTO `blocks`(`blockNum`,`blockChecksum`,`lastBlockChecksum`,`walletStateChecksum`,`sigFreezeChecksum`, `difficulty`, `powField`, `transactions`,`signatures`,`timestamp`,`version`,`lastSuperBlockChecksum`,`lastSuperBlockNum`,`superBlockSegments`,`compactedSigs`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";
                        result = executeSQL(sql, (long)block.blockNum, block.blockChecksum, block.lastBlockChecksum, block.walletStateChecksum, block.signatureFreezeChecksum, (long)block.difficulty, block.powField, transactions, signatures, block.timestamp, block.version, block.lastSuperBlockChecksum, (long)block.lastSuperBlockNum, super_block_segments.ToArray(), block.compactedSigs);
                    }
                    else
                    {
                        seekDatabase(block.blockNum, true);

                        // Likely already have the block stored, update the old entry
                        string sql = "UPDATE `blocks` SET `blockChecksum` = ?, `lastBlockChecksum` = ?, `walletStateChecksum` = ?, `sigFreezeChecksum` = ?, `difficulty` = ?, `powField` = ?, `transactions` = ?, `signatures` = ?, `timestamp` = ?, `version` = ?, `lastSuperBlockChecksum` = ?, `lastSuperBlockNum` = ?, `superBlockSegments` = ?, `compactedSigs` = ? WHERE `blockNum` = ?";
                        //Console.WriteLine("SQL: {0}", sql);
                        result = executeSQL(sql, block.blockChecksum, block.lastBlockChecksum, block.walletStateChecksum, block.signatureFreezeChecksum, (long)block.difficulty, block.powField, transactions, signatures, block.timestamp, block.version, block.lastSuperBlockChecksum, (long)block.lastSuperBlockNum, super_block_segments.ToArray(), block.compactedSigs, (long)block.blockNum);
                    }
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
                    toList = string.Format("{0}||{1}:{2}", toList, Base58Check.Base58CheckEncoding.EncodePlain(to.Key), Convert.ToBase64String(to.Value.getAmount().ToByteArray()));
                }

                string fromList = "";
                foreach (var from in transaction.fromList)
                {
                    fromList = string.Format("{0}||{1}:{2}", fromList, Base58Check.Base58CheckEncoding.EncodePlain(from.Key), Convert.ToBase64String(from.Value.getAmount().ToByteArray()));
                }

                bool result = false;
                lock (storageLock)
                {
                    byte[] tx_data_shuffled = shuffleStorageBytes(transaction.data);

                    // Go through all databases starting from latest and search for the transaction
                    if (getTransaction(transaction.id, transaction.applied) == null)
                    {
                        // Transaction was not found in any existing database, seek to the proper database
                        seekDatabase(transaction.applied, true);

                        string sql = "INSERT INTO `transactions`(`id`,`type`,`amount`,`fee`,`toList`,`fromList`,`dataChecksum`,`data`,`blockHeight`, `nonce`, `timestamp`,`checksum`,`signature`, `pubKey`, `applied`, `version`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";
                        result = executeSQL(sql, transaction.id, transaction.type, transaction.amount.ToString(), transaction.fee.ToString(), toList, fromList, transaction.dataChecksum, tx_data_shuffled, (long)transaction.blockHeight, transaction.nonce, transaction.timeStamp, transaction.checksum, transaction.signature, transaction.pubKey, (long)transaction.applied, transaction.version);
                    }
                    else
                    {
                        // Transaction found. Seeked database was set by getTransaction
                        seekDatabase(transaction.applied, true);

                        // Likely already have the tx stored, update the old entry
                        string sql = "UPDATE `transactions` SET `type` = ?,`amount` = ? ,`fee` = ?, `toList` = ?, `fromList` = ?,`dataChecksum` = ?, `data` = ?, `blockHeight` = ?, `nonce` = ?, `timestamp` = ?,`checksum` = ?,`signature` = ?, `pubKey` = ?, `applied` = ?, `version` = ? WHERE `id` = ?";
                        result = executeSQL(sql, transaction.type, transaction.amount.ToString(), transaction.fee.ToString(), toList, fromList, transaction.dataChecksum, tx_data_shuffled, (long)transaction.blockHeight, transaction.nonce, transaction.timeStamp, transaction.checksum, transaction.signature, transaction.pubKey, (long)transaction.applied, transaction.version, transaction.id);
                    }
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
                    transactions = new List<string>(),
                    signatures = new List<byte[][]>(),
                    timestamp = blk.timestamp,
                    version = blk.version,
                    lastSuperBlockChecksum = blk.lastSuperBlockChecksum,
                    lastSuperBlockNum = (ulong)blk.lastSuperBlockNum,
                    compactedSigs = blk.compactedSigs
                };

                // Add signatures
                string[] split_str = blk.signatures.Split(new string[] { "||" }, StringSplitOptions.None);
                int sigcounter = 0;
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
                    byte[][] newSig = new byte[2][];
                    if (split_sig[0] != "0")
                    {
                        newSig[0] = Convert.FromBase64String(split_sig[0]);
                    }
                    newSig[1] = Convert.FromBase64String(split_sig[1]);
                    if (!block.containsSignature(new Address(newSig[1], null, false)))
                    {
                        block.signatures.Add(newSig);
                    }
                }

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

                    block.addTransaction(s1);
                }

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

                return block;
            }

            public override Block getBlock(ulong blocknum)
            {
                lock (storageLock)
                {
                    if (blocknum < 1)
                    {
                        return null;
                    }

                    string sql = "select * from blocks where `blocknum` = ? LIMIT 1"; // AND `blocknum` < (SELECT MAX(`blocknum`) - 5 from blocks)
                    List<_storage_Block> _storage_block = null;

                    lock (storageLock)
                    {
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

                    //Logging.info(String.Format("Read block #{0} from storage.", block.blockNum));

                    return getBlockFromStorageBlock(_storage_block[0]);
                }
            }

            public override Block getBlockByHash(byte[] hash)
            {
                lock (storageLock)
                {
                    if (hash == null)
                    {
                        return null;
                    }

                    string sql = "select * from blocks where `blockChecksum` = ? LIMIT 1";
                    _storage_Block[] _storage_block = null;

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
                            seekDatabase(db_blocknum);
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

                    Logging.info(String.Format("Read block #{0} from storage.", _storage_block[0].blockNum));

                    return getBlockFromStorageBlock(_storage_block[0]);
                }
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
            public override Transaction getTransaction(string txid, ulong block_num)
            {
                Transaction transaction = null;
                List<_storage_Transaction> _storage_tx = null;

                string sql = "select * from transactions where `id` = ? LIMIT 1";

                // Go through each database until the transaction is found
                // TODO: optimize this for better performance
                lock (storageLock)
                {
                    bool found = false;
                    try
                    {
                        if(block_num > 0)
                        {
                            seekDatabase(block_num, true);
                        }
                        _storage_tx = sqlConnection.Query<_storage_Transaction>(sql, txid);

                    }
                    catch (Exception e)
                    {
                        Logging.error(String.Format("Exception has been thrown while executing SQL Query {0}. Exception message: {1}", sql, e.Message));
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

                    ulong db_blocknum = getHighestBlockInStorage();
                    while (!found)
                    {
                        // Transaction not found yet, seek to another database
                        seekDatabase(db_blocknum);
                        try
                        {
                            _storage_tx = sqlConnection.Query<_storage_Transaction>(sql, txid);

                        }
                        catch (Exception)
                        {
                            if (db_blocknum > Config.maxBlocksPerDatabase)
                            {
                                db_blocknum -= Config.maxBlocksPerDatabase;
                            }
                            else
                            {
                                // Transaction not found
                                return transaction;
                            }
                        }

                        if (_storage_tx == null || _storage_tx.Count < 1)
                        {
                            if (db_blocknum > Config.maxBlocksPerDatabase)
                            {
                                db_blocknum -= Config.maxBlocksPerDatabase;
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

                if (_storage_tx.Count < 1)
                {
                    return transaction;
                }

                _storage_Transaction tx = _storage_tx[0];

                transaction = new Transaction(tx.type, tx.dataChecksum, unshuffleStorageBytes(tx.data))
                {
                    id = tx.id,
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
                    transaction.toList.AddOrReplace(address, amount);
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
                    seekDatabase(blockNum, true);
                    Block b = getBlock(blockNum);
                    if (b == null)
                    {
                        return true;
                    }

                    // First go through all transactions and remove them from storage
                    foreach (string txid in b.transactions)
                    {
                        if (removeTransaction(txid) == false)
                        {
                            return false;
                        }
                    }

                    // Now remove the block itself from storage
                    string sql = "DELETE FROM blocks where `blockNum` = ?";
                    return executeSQL(sql, blockNum);
                }
            }

            // Removes a transaction from the storage database
            // Warning: make sure this is called on the corresponding database (seeked to the blocknum of this transaction)
            public override bool removeTransaction(string txid, ulong blockNum = 0)
            {
                lock (storageLock)
                {
                    string sql = "DELETE FROM transactions where `id` = ?";
                    return executeSQL(sql, txid);
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
                if (connection.Execute(sql, sqlParameters) > 0)
                {
                    return true;
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

                return bytes.Reverse().ToArray();
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

            public override IEnumerable<Transaction> getTransactionsInBlock(ulong block_num)
            {
                throw new NotImplementedException();
            }

            public override IEnumerable<Transaction> getTransactionsByTime(long time_from, long time_to)
            {
                throw new NotImplementedException();
            }

            public override IEnumerable<Transaction> getTransactionsApplied(ulong block_from, ulong block_to)
            {
                throw new NotImplementedException();
            }

            protected override void shutdown()
            {
                // does nothing for this provider
            }
        }
    }
}