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
        public class Storage
        {
            public static string path = Config.dataFolderPath + Path.DirectorySeparatorChar + "blocks";

            // Sql connections
            private static SQLiteConnection sqlConnection = null;
            private static readonly object storageLock = new object(); // This should always be placed when performing direct sql operations

            private static SQLiteConnection superBlocksSqlConnection = null;
            private static readonly object superBlockStorageLock = new object(); // This should always be placed when performing direct sql operations

            private static Dictionary<string, object[]> connectionCache = new Dictionary<string, object[]>();

            // Threading
            private static Thread thread = null;
            private static bool running = false;
            private static ThreadLiveCheck TLC;

            // Storage cache
            private static ulong cached_lastBlockNum = 0;
            private static ulong current_seek = 1;

            public static bool upgrading = false;
            public static ulong upgradeProgress = 0;
            public static ulong upgradeMaxBlockNum = 0;

            private enum QueueStorageCode
            {
                insertTransaction,
                insertBlock,
                updateTxAppliedFlag

            }
            private struct QueueStorageMessage
            {
                public QueueStorageCode code;
                public object data;
            }

            // Maintain a queue of sql statements
            private static List<QueueStorageMessage> queueStatements = new List<QueueStorageMessage>();

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
            public static bool prepareStorage()
            {
                var files = Directory.EnumerateFiles(path + Path.DirectorySeparatorChar + "0000", "*.dat-shm");
                foreach (var file in files)
                {
                    File.Delete(file);
                }

                files = Directory.EnumerateFiles(path + Path.DirectorySeparatorChar + "0000", "*.dat-wal");
                foreach (var file in files)
                {
                    File.Delete(file);
                }

                string db_path = path + Path.DirectorySeparatorChar + "superblocks.dat";

                // Bind the connection
                superBlocksSqlConnection = getSQLiteConnection(db_path, false);


                // Get latest block number to initialize the cache as well
                ulong last_block = getLastBlockNum();
                Logging.info(string.Format("Last storage block number is: #{0}", last_block));

                // Start thread
                TLC = new ThreadLiveCheck();
                running = true;
                thread = new Thread(new ThreadStart(threadLoop));
                thread.Name = "Storage_Thread";
                thread.Start();

                return true;
            }

            // Shutdown storage thread
            public static void stopStorage()
            {
                running = false;
            }

            private static void cleanConnectionCache()
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
                    }
                }
            }

            private static SQLiteConnection getSQLiteConnection(string path, bool cache = false)
            {
                lock (connectionCache)
                {
                    if (connectionCache.ContainsKey(path))
                    {
                        if (cache)
                        {
                            connectionCache[path][1] = Clock.getTimestamp();
                            cleanConnectionCache();
                        }
                        return (SQLiteConnection)connectionCache[path][0];
                    }

                    SQLiteConnection connection = new SQLiteConnection(path);
                    connection.ExecuteScalar<string>("PRAGMA journal_mode=WAL;");
                    //connection.ExecuteScalar<string>("PRAGMA locking_mode=EXCLUSIVE;");
                    if (cache)
                    {
                        connectionCache.Add(path, new object[2] { connection, Clock.getTimestamp() });
                    }

                    // check if database exists
                    var tableInfo = connection.GetTableInfo("transactions");
                    if (!tableInfo.Any())
                    {

                        // The database needs to be prepared first
                        // Create the blocks table
                        string sql = "CREATE TABLE `blocks` (`blockNum`	INTEGER NOT NULL, `blockChecksum` BLOB, `lastBlockChecksum` BLOB, `walletStateChecksum`	BLOB, `sigFreezeChecksum` BLOB, `difficulty` INTEGER, `powField` BLOB, `transactions` TEXT, `signatures` TEXT, `timestamp` INTEGER, `version` INTEGER, `lastSuperBlockChecksum` BLOB, `lastSuperBlockNum` INTEGER, `superBlockSegments` BLOB, `compactedSigs` INTEGER, PRIMARY KEY(`blockNum`));";
                        executeSQL(connection, sql);

                        sql = "CREATE TABLE `transactions` (`id` TEXT, `type` INTEGER, `amount` TEXT, `fee` TEXT, `toList` TEXT, `fromList` TEXT, `data` BLOB, `blockHeight` INTEGER, `nonce` INTEGER, `timestamp` INTEGER, `checksum` BLOB, `signature` BLOB, `pubKey` BLOB, `applied` INTEGER, `version` INTEGER, PRIMARY KEY(`id`));";
                        executeSQL(connection, sql);
                        sql = "CREATE INDEX `type` ON `transactions` (`type`);";
                        executeSQL(connection, sql);
                        sql = "CREATE INDEX `toList` ON `transactions` (`toList`);";
                        executeSQL(connection, sql);
                        sql = "CREATE INDEX `fromList` ON `transactions` (`fromList`);";
                        executeSQL(connection, sql);
                        sql = "CREATE INDEX `applied` ON `transactions` (`applied`);";
                        executeSQL(connection, sql);
                    } else if (!tableInfo.Exists(x => x.Name == "fromList"))
                    {
                        string sql = "ALTER TABLE `transactions` ADD COLUMN `fromList` TEXT;";
                        executeSQL(connection, sql);
                        sql = "CREATE INDEX `fromList` ON `transactions` (`fromList`);";
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

                    return connection;
                }
            }

            // Returns true if connection to matching blocknum range database is established
            public static bool seekDatabase(ulong blocknum = 0, bool cache = false)
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

                    string db_path = path + Path.DirectorySeparatorChar + "0000" + Path.DirectorySeparatorChar + db_blocknum + ".dat";

                    // Bind the connection
                    sqlConnection = getSQLiteConnection(db_path, cache);
                }
                return true;
            }

            // Go through all database files until we discover the latest consecutive one
            // Doing it this way prevents skipping over inexistent databases
            // returns 1 on failure
            public static ulong seekLatestDatabase()
            {
                ulong db_blocknum = 0;
                bool found = false;

                while (!found)
                {
                    string db_path = path + Path.DirectorySeparatorChar + "0000" + Path.DirectorySeparatorChar + db_blocknum + ".dat";
                    if (File.Exists(db_path))
                    {
                        db_blocknum += Config.maxBlocksPerDatabase;
                    }
                    else
                    {
                        if (db_blocknum > 0)
                            db_blocknum -= Config.maxBlocksPerDatabase;

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

            public static ulong getLastBlockNum()
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
                            return db_block_num;

                        if (_storage_block.Length < 1)
                            return db_block_num;

                        _storage_Block blk = _storage_block[0];
                        cached_lastBlockNum = (ulong)blk.blockNum;
                    }
                }
                return cached_lastBlockNum;
            }

            public static bool insertBlockInternal(Block block)
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
                    if (getLastBlockNum() < block.blockNum)
                        cached_lastBlockNum = block.blockNum;
                }

                return result;
            }

            public static bool insertTransactionInternal(Transaction transaction)
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

                        string sql = "INSERT INTO `transactions`(`id`,`type`,`amount`,`fee`,`toList`,`fromList`,`data`,`blockHeight`, `nonce`, `timestamp`,`checksum`,`signature`, `pubKey`, `applied`, `version`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";
                        result = executeSQL(sql, transaction.id, transaction.type, transaction.amount.ToString(), transaction.fee.ToString(), toList, fromList, tx_data_shuffled, (long)transaction.blockHeight, transaction.nonce, transaction.timeStamp, transaction.checksum, transaction.signature, transaction.pubKey, (long)transaction.applied, transaction.version);
                    }
                    else
                    {
                        // Transaction found. Seeked database was set by getTransaction
                        seekDatabase(transaction.applied, true);

                        // Likely already have the tx stored, update the old entry
                        string sql = "UPDATE `transactions` SET `type` = ?,`amount` = ? ,`fee` = ?, `toList` = ?, `fromList` = ?,`data` = ?, `blockHeight` = ?, `nonce` = ?, `timestamp` = ?,`checksum` = ?,`signature` = ?, `pubKey` = ?, `applied` = ?, `version` = ? WHERE `id` = ?";
                        result = executeSQL(sql, transaction.type, transaction.amount.ToString(), transaction.fee.ToString(), toList, fromList, tx_data_shuffled, (long)transaction.blockHeight, transaction.nonce, transaction.timeStamp, transaction.checksum, transaction.signature, transaction.pubKey, (long)transaction.applied, transaction.version, transaction.id);
                    }
                }

                return result;
            }

            public static Block getSuperBlock(ulong blocknum)
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
                    return null;

                if (_storage_block.Count < 1)
                    return null;

                return getBlockFromStorageBlock(_storage_block[0]);
            }

            public static Block getSuperBlock(byte[] checksum)
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
                    return null;

                if (_storage_block.Count < 1)
                    return null;

                return getBlockFromStorageBlock(_storage_block[0]);
            }

            public static Block getNextSuperBlock(ulong blocknum)
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
                    return null;

                if (_storage_block.Count < 1)
                    return null;

                return getBlockFromStorageBlock(_storage_block[0]);
            }

            public static Block getNextSuperBlock(byte[] checksum)
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

            public static Block getBlockFromStorageBlock(_storage_Block storage_block)
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
                        continue;

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
                    if (!block.containsSignature(newSig[1]))
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
                        continue;

                    block.transactions.Add(s1);
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

            // Warning: this assumes it's called with the storageLock active
            public static Block getBlock(ulong blocknum)
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
                    return null;

                if (_storage_block.Count < 1)
                    return null;

                //Logging.info(String.Format("Read block #{0} from storage.", block.blockNum));

                return getBlockFromStorageBlock(_storage_block[0]);
            }

            public static Block getBlockByHash(byte[] hash)
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
                        if (_storage_block.Length > 0)
                            found = true;

                    ulong db_blocknum = getLastBlockNum();
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
                                db_blocknum -= Config.maxBlocksPerDatabase;
                            else
                            {
                                // Block not found
                                return null;
                            }
                        }

                        if (_storage_block == null || _storage_block.Length < 1)
                        {
                            if (db_blocknum > Config.maxBlocksPerDatabase)
                                db_blocknum -= Config.maxBlocksPerDatabase;
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
                    return null;

                if (_storage_block.Length < 1)
                    return null;

                Logging.info(String.Format("Read block #{0} from storage.", _storage_block[0].blockNum));

                return getBlockFromStorageBlock(_storage_block[0]);
            }

            // Retrieve a transaction from the sql database
            public static Transaction getTransaction(string txid, ulong block_num)
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
                        if (_storage_tx.Count > 0)
                            found = true;

                    if(!found && block_num > 0)
                    {
                        return transaction;
                    }

                    ulong db_blocknum = getLastBlockNum();
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
                                db_blocknum -= Config.maxBlocksPerDatabase;
                            else
                            {
                                // Transaction not found
                                return transaction;
                            }
                        }

                        if (_storage_tx == null || _storage_tx.Count < 1)
                        {
                            if (db_blocknum > Config.maxBlocksPerDatabase)
                                db_blocknum -= Config.maxBlocksPerDatabase;
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
                    return transaction;

                _storage_Transaction tx = _storage_tx[0];

                transaction = new Transaction(tx.type);
                transaction.id = tx.id;
                transaction.amount = new IxiNumber(tx.amount);
                transaction.fee = new IxiNumber(tx.fee);
                transaction.data = unshuffleStorageBytes(tx.data);
                transaction.blockHeight = (ulong)tx.blockHeight;
                transaction.nonce = tx.nonce;
                transaction.timeStamp = tx.timestamp;
                transaction.checksum = tx.checksum;
                transaction.signature = tx.signature;
                transaction.version = tx.version;
                transaction.pubKey = tx.pubKey;
                // note - don't ever read .applied field, otherwise there will be issues with sync

                // Add toList
                string[] split_str = tx.toList.Split(new string[] { "||" }, StringSplitOptions.None);
                int sigcounter = 0;
                foreach (string s1 in split_str)
                {
                    sigcounter++;
                    if (sigcounter == 1)
                        continue;

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
                            continue;

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
            // Also removes all transactions linked to this block
            public static bool removeBlock(Block block, bool removePreviousBlocks = false)
            {
                // Only remove on non-history nodes
                if (Config.storeFullHistory == true)
                {
                    return false;
                }

                lock (storageLock)
                {
                    seekDatabase(block.blockNum, true);

                    // First go through all transactions and remove them from storage
                    foreach (string txid in block.transactions)
                    {
                        if (removeTransaction(txid) == false)
                            return false;
                    }

                    // Now remove the block itself from storage
                    string sql = "DELETE FROM blocks where `blockNum` = ?";
                    return executeSQL(sql, block.blockNum);

                }
            }

            // Removes a transaction from the storage database
            // Warning: make sure this is called on the corresponding database (seeked to the blocknum of this transaction)
            public static bool removeTransaction(string txid)
            {
                string sql = "DELETE FROM transactions where `id` = ?";
                return executeSQL(sql, txid);
            }

            // Remove all previous blocks and corresponding transactions outside the redacted window
            // Takes the assigned blockheight and calculates the redacted window automatically
            public static bool redactBlockStorage(ulong blockheight)
            {
                // Only redact on non-history nodes
                if (Config.storeFullHistory == true)
                {
                    return false;
                }

                if (blockheight < ConsensusConfig.redactedWindowSize)
                {
                    // Nothing to redact yet
                    return false;
                }
                lock (storageLock)
                {
                    seekDatabase(blockheight, true);

                    // Calculate the window
                    ulong redactedWindow = blockheight - ConsensusConfig.redactedWindowSize;

                    Logging.info(string.Format("Redacting storage below block #{0}", redactedWindow));

                    string sql = "select * from blocks where `blocknum` < ?";
                    _storage_Block[] _storage_blocks = null;
                    try
                    {
                        _storage_blocks = sqlConnection.Query<_storage_Block>(sql, (long)redactedWindow).ToArray();
                    }
                    catch (Exception e)
                    {
                        Logging.error(String.Format("Exception has been thrown while executing SQL Query {0}. Exception message: {1}", sql, e.Message));
                        return false;
                    }

                    if (_storage_blocks == null)
                        return false;

                    if (_storage_blocks.Length < 1)
                        return false;

                    // Go through each block
                    foreach (_storage_Block blk in _storage_blocks)
                    {
                        // Extract transactions
                        string[] split_str = blk.transactions.Split(new string[] { "||" }, StringSplitOptions.None);
                        int txcounter = 0;
                        foreach (string s1 in split_str)
                        {
                            txcounter++;
                            // Skip placeholder
                            if (txcounter == 1)
                                continue;

                            // Remove this transaction
                            removeTransaction(s1);
                        }

                        // Remove the block as well
                        sql = "DELETE FROM blocks where `blockNum` = ?";
                        executeSQL(sql, blk.blockNum);
                    }
                }
                return true;
            }

            // Escape and execute an sql command
            private static bool executeSQL(string sql, params object[] sqlParameters)
            {
                return executeSQL(sqlConnection, sql, sqlParameters);
            }

            // Escape and execute an sql command
            private static bool executeSQL(SQLiteConnection connection, string sql, params object[] sqlParameters)
            {
                try
                {
                    connection.Execute(sql, sqlParameters);
                }
                catch (Exception e)
                {
                    Logging.error(String.Format("Exception has been thrown while executing SQL Query {0}. Exception message: {1}", sql, e.Message));
                    // TODO TODO TODO TODO this may indicate a corrupt database, usually exception message is simply Corrupt, probably in this case we should delete the file and re-create it
                    return false;
                }
                return true;
            }

            public static bool insertBlock(Block block)
            {
                // Make a copy of the block for the queue storage message processing
                QueueStorageMessage message = new QueueStorageMessage
                {
                    code = QueueStorageCode.insertBlock,
                    data = new Block(block)
                };

                lock (queueStatements)
                {
                    queueStatements.Add(message);
                }
                return true;
            }


            public static void insertTransaction(Transaction transaction)
            {
                // Make a copy of the transaction for the queue storage message processing
                QueueStorageMessage message = new QueueStorageMessage
                {
                    code = QueueStorageCode.insertTransaction,
                    data = new Transaction(transaction)
                };

                lock (queueStatements)
                {

                    queueStatements.Add(message);
                }
            }

            // Storage thread
            private static void threadLoop()
            {
                // Prepare an special message object to use, without locking up the queue messages
                QueueStorageMessage active_message = new QueueStorageMessage();

                bool pending_statements = false;

                while (running || pending_statements == true)
                {
                    pending_statements = false;
                    TLC.Report();
                    try
                    {
                        bool message_found = false;

                        lock (queueStatements)
                        {
                            int statements_count = queueStatements.Count();
                            if (statements_count > 0)
                            {
                                if(statements_count > 1)
                                {
                                    pending_statements = true;
                                }
                                QueueStorageMessage candidate = queueStatements[0];
                                active_message = candidate;
                                queueStatements.Remove(candidate);
                                message_found = true;
                            }
                        }

                        if (message_found)
                        {
                            if (active_message.code == QueueStorageCode.insertTransaction)
                            {
                                insertTransactionInternal((Transaction)active_message.data);
                            }
                            else if (active_message.code == QueueStorageCode.insertBlock)
                            {
                                insertBlockInternal((Block)active_message.data);
                            }
                        }
                        else
                        {
                            // Sleep for 10ms to prevent cpu waste
                            Thread.Sleep(10);
                        }
                    }
                    catch (Exception e)
                    {
                        Logging.error("Exception occured in storage thread loop: " + e);
                    }
                    Thread.Yield();
                }

                superBlocksSqlConnection.Close();
                superBlocksSqlConnection = null;

                sqlConnection = null;
                lock (connectionCache)
                {
                    foreach(var entry in connectionCache)
                    {
                        ((SQLiteConnection)entry.Value[0]).Close();
                    }
                    connectionCache.Clear();
                }

                Logging.info("Storage thread stopped.");
            }

            public static int getQueuedQueryCount()
            {
                lock (queueStatements)
                {
                    return queueStatements.Count;
                }
            }

            // Shuffle data storage bytes
            public static byte[] shuffleStorageBytes(byte[] bytes)
            {
                if (bytes == null)
                    return null;
                return bytes.Reverse().ToArray();
            }

            // Unshuffle data storage bytes
            public static byte[] unshuffleStorageBytes(byte[] bytes)
            {
                if (bytes == null)
                    return null;

                return bytes.Reverse().ToArray();
            }

            public static void deleteCache()
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
        }
    }
}