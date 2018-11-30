using IXICore;
using Newtonsoft.Json;
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
            public static string filename = "blockchain.dat";

            // Sql connections
            private static SQLiteConnection sqlConnection = null;
            private static readonly object storageLock = new object(); // This should always be placed when performing direct sql operations

            // Threading
            private static Thread thread = null;
            private static bool running = false;

            // Storage cache
            private static ulong cached_lastBlockNum = 0;
            private static ulong current_seek = 1;

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
            }

            public class _storage_Transaction
            {
                public string id { get; set; }
                public int type { get; set; }
                public string amount { get; set; }
                public string fee { get; set; }
                public string toList { get; set; }
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
                // Get latest block number to initialize the cache as well
                ulong last_block = getLastBlockNum();
                Logging.info(string.Format("Last storage block number is: #{0}", last_block));

                // Start thread
                running = true;
                thread = new Thread(new ThreadStart(threadLoop));
                thread.Start();

                // Check for an older database and upgrade if found
                checkForOlderDatabase();

                return true;
            }

            // Shutdown storage thread
            public static void stopStorage()
            {
                running = false;
            }

            // Returns true if connection to matching blocknum range database is established
            public static bool seekDatabase(ulong blocknum = 0)
            {
                lock (storageLock)
                {
                    ulong db_blocknum = ((ulong)(blocknum / Config.maxBlocksPerDatabase)) * Config.maxBlocksPerDatabase;

                    // Check if the current seek location matches this block range
                    if (current_seek == db_blocknum)
                    {
                        return true;
                    }

                    string db_path = Config.dataFoldername + Path.DirectorySeparatorChar + "blocks" + Path.DirectorySeparatorChar + filename + "." + db_blocknum;

                    bool prepare_database = false;
                    // Check if the database file does not exist
                    if (File.Exists(db_path) == false)
                    {
                        prepare_database = true;
                    }

                    if (sqlConnection != null)
                    {
                        sqlConnection.Close();
                    }

                    // Bind the connection
                    sqlConnection = new SQLiteConnection(db_path);

                    // Update the current seek number
                    current_seek = db_blocknum;

                    // The database needs to be prepared first
                    if (prepare_database)
                    {
                        // Create the blocks table
                        string sql = "CREATE TABLE `blocks` (`blockNum`	INTEGER NOT NULL, `blockChecksum` BLOB, `lastBlockChecksum` BLOB, `walletStateChecksum`	BLOB, `sigFreezeChecksum` BLOB, `difficulty` INTEGER, `powField` BLOB, `transactions` TEXT, `signatures` TEXT, `timestamp` INTEGER, `version` INTEGER, PRIMARY KEY(`blockNum`));";
                        executeSQL(sql);

                        sql = "CREATE TABLE `transactions` (`id` TEXT, `type` INTEGER, `amount` TEXT, `fee` TEXT, `toList` TEXT, `from` BLOB,  `data` BLOB, `blockHeight` INTEGER, `nonce` INTEGER, `timestamp` INTEGER, `checksum` BLOB, `signature` BLOB, `pubKey` BLOB, `applied` INTEGER, `version` INTEGER, PRIMARY KEY(`id`));";
                        executeSQL(sql);
                        sql = "CREATE INDEX `type` ON `transactions` (`type`);";
                        executeSQL(sql);
                        sql = "CREATE INDEX `from` ON `transactions` (`from`);";
                        executeSQL(sql);
                        sql = "CREATE INDEX `toList` ON `transactions` (`toList`);";
                        executeSQL(sql);
                        sql = "CREATE INDEX `applied` ON `transactions` (`applied`);";
                        executeSQL(sql);
                    }
                }
                return true;
            }

            // Go through all database files until we discover the latest consecutive one
            // Doing it this way prevents skipping over inexistent databases
            public static bool seekLatestDatabase()
            {
                ulong db_blocknum = 0;
                bool found = false;

                while (!found)
                {
                    string db_path = Config.dataFoldername + Path.DirectorySeparatorChar + "blocks" + Path.DirectorySeparatorChar + filename + "." + db_blocknum;
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

                // Seek the found database
                return seekDatabase(db_blocknum);
            }

            public static ulong getLastBlockNum()
            {
                if(cached_lastBlockNum == 0)
                {
                    lock (storageLock)
                    {
                        seekLatestDatabase();

                        string sql = string.Format("SELECT * FROM `blocks` ORDER BY `blockNum` DESC LIMIT 1");
                        var _storage_block = sqlConnection.Query<_storage_Block>(sql).ToArray();

                        if (_storage_block == null)
                            return 0;

                        if (_storage_block.Length < 1)
                            return 0;

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

                string signatures = "";
                foreach (byte[][] sig in block.signatures)
                {
                    signatures = string.Format("{0}||{1}:{2}", signatures, Convert.ToBase64String(sig[0]), Convert.ToBase64String(sig[1]));
                }

                bool result = false;
                lock (storageLock)
                {
                    if (getBlock(block.blockNum) == null)
                    {
                        string sql = "INSERT INTO `blocks`(`blockNum`,`blockChecksum`,`lastBlockChecksum`,`walletStateChecksum`,`sigFreezeChecksum`, `difficulty`, `powField`, `transactions`,`signatures`,`timestamp`,`version`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";
                        result = executeSQL(sql, (long)block.blockNum, block.blockChecksum, block.lastBlockChecksum, block.walletStateChecksum, block.signatureFreezeChecksum, (long)block.difficulty, block.powField, transactions, signatures, block.timestamp, block.version);
                    }
                    else
                    {
                        // Likely already have the block stored, update the old entry
                        string sql = "UPDATE `blocks` SET `blockChecksum` = ?, `lastBlockChecksum` = ?, `walletStateChecksum` = ?, `sigFreezeChecksum` = ?, `difficulty` = ?, `powField` = ?, `transactions` = ?, `signatures` = ?, `timestamp` = ?, `version` = ? WHERE `blockNum` = ?";
                        //Console.WriteLine("SQL: {0}", sql);
                        result = executeSQL(sql, block.blockChecksum, block.lastBlockChecksum, block.walletStateChecksum, block.signatureFreezeChecksum, (long)block.difficulty, block.powField, transactions, signatures, block.timestamp, block.version, (long)block.blockNum);
                    }
                }

                if(result)
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

                bool result = false;
                lock (storageLock)
                {
                    seekDatabase(transaction.applied);

                    byte[] tx_data_shuffled = shuffleStorageBytes(transaction.data);

                    if (getTransaction(transaction.id) == null)
                    {
                        string sql = "INSERT INTO `transactions`(`id`,`type`,`amount`,`fee`,`toList`,`from`,`data`,`blockHeight`, `nonce`, `timestamp`,`checksum`,`signature`, `pubKey`, `applied`, `version`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";
                        result = executeSQL(sql, transaction.id, transaction.type, transaction.amount.ToString(), transaction.fee.ToString(), toList, transaction.from, tx_data_shuffled, (long)transaction.blockHeight, transaction.nonce, transaction.timeStamp, transaction.checksum, transaction.signature, transaction.pubKey, (long)transaction.applied, transaction.version);
                    }
                    else
                    {
                        // Likely already have the tx stored, update the old entry
                        string sql = "UPDATE `transactions` SET `type` = ?,`amount` = ? ,`fee` = ?, `toList` = ?, `from` = ?,`data` = ?, `blockHeight` = ?, `nonce` = ?, `timestamp` = ?,`checksum` = ?,`signature` = ?, `pubKey` = ?, `applied` = ?, `version` = ? WHERE `id` = ?";
                        result = executeSQL(sql, transaction.type, transaction.amount.ToString(), transaction.fee.ToString(), toList, transaction.from, tx_data_shuffled, (long)transaction.blockHeight, transaction.nonce, transaction.timeStamp, transaction.checksum, transaction.signature, transaction.pubKey, (long)transaction.applied, transaction.version, transaction.id);
                    }
                }

                return result;
            }

            // Warning: this assumes it's called with the storageLock active
            public static Block getBlock(ulong blocknum)
            {
                if (blocknum < 0)
                {
                    return null;
                }

                Block block = null;
                string sql = "select * from blocks where `blocknum` = ? LIMIT 1"; // AND `blocknum` < (SELECT MAX(`blocknum`) - 5 from blocks)
                _storage_Block[] _storage_block = null;

                lock (storageLock)
                {
                    seekDatabase(blocknum);
                  
                    try
                    {
                        _storage_block = sqlConnection.Query<_storage_Block>(sql, (long)blocknum).ToArray();
                    }
                    catch (Exception e)
                    {
                        Logging.error(String.Format("Exception has been thrown while executing SQL Query {0}. Exception message: {1}", sql, e.Message));
                        return null;
                    }
                }

                if (_storage_block == null)
                    return block;

                if (_storage_block.Length < 1)
                    return block;

                _storage_Block blk = _storage_block[0];

                block = new Block
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
                    version = blk.version
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
                    byte[][] newSig = new byte[2][];
                    newSig[0] = Convert.FromBase64String(split_sig[0]);
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

                block.fromLocalStorage = true;
                    
                //Logging.info(String.Format("Read block #{0} from storage.", block.blockNum));


                return block;
            }

            public static Block getBlockByHash(byte[] hash)
            {
                if (hash == null)
                {
                    return null;
                }
                Block block = null;
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
                        catch (Exception e)
                        {
                            if (db_blocknum > Config.maxBlocksPerDatabase)
                                db_blocknum -= Config.maxBlocksPerDatabase;
                            else
                            {
                                // Block not found
                                return block;
                            }
                        }

                        if (_storage_block == null || _storage_block.Length < 1)
                        {
                            if (db_blocknum > Config.maxBlocksPerDatabase)
                                db_blocknum -= Config.maxBlocksPerDatabase;
                            else
                            {
                                // Block not found in any database
                                return block;
                            }
                            continue;
                        }

                        found = true;
                    }
                }

                if (_storage_block == null)
                    return block;

                if (_storage_block.Length < 1)
                    return block;

                _storage_Block blk = _storage_block[0];

                block = new Block
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
                    version = blk.version
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
                    byte[][] newSig = new byte[2][];
                    newSig[0] = Convert.FromBase64String(split_sig[0]);
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

                Logging.info(String.Format("Read block #{0} from storage.", block.blockNum));

                return block;
            }

            // Retrieve a transaction from the sql database
            public static Transaction getTransaction(string txid)
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
                        _storage_tx = sqlConnection.Query<_storage_Transaction>(sql, txid);

                    }
                    catch (Exception e)
                    {
                        Logging.error(String.Format("Exception has been thrown while executing SQL Query {0}. Exception message: {1}", sql, e.Message));
                        found = false;
                    }

                    if (_storage_tx != null)
                        if(_storage_tx.Count > 0)
                            found = true;
                    

                    
                    ulong db_blocknum = getLastBlockNum();
                    while (!found)
                    {
                        // Transaction not found yet, seek to another database
                        seekDatabase(db_blocknum);
                        try
                        {
                            _storage_tx = sqlConnection.Query<_storage_Transaction>(sql, txid);
                            
                        }
                        catch (Exception e)
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
                transaction.from = tx.from;
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
                    byte[] address = Base58Check.Base58CheckEncoding.DecodePlain(split_to[0]);
                    IxiNumber amount = new IxiNumber(new BigInteger(Convert.FromBase64String(split_to[1])));
                    transaction.toList.AddOrReplace(address, amount);
                }


                return transaction;
            }


            // Retrieve a bunch of transactions from the sql database
            public static List<Transaction> getTransactions(List<string> txids)
            {
                List<Transaction> transactions = new List<Transaction>();
                return transactions;
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
                    seekDatabase(block.blockNum);

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

                if (blockheight < CoreConfig.redactedWindowSize)
                {
                    // Nothing to redact yet
                    return false;
                }
                lock (storageLock)
                {
                    seekDatabase(blockheight);

                    // Calculate the window
                    ulong redactedWindow = blockheight - CoreConfig.redactedWindowSize;

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
                try
                {
                    sqlConnection.Execute(sql, sqlParameters);
                }
                catch (Exception e)
                {
                    Logging.error(String.Format("Exception has been thrown while executing SQL Query {0}. Exception message: {1}", sql, e.Message));
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

                while (running)
                {
                    try
                    {
                        bool message_found = false;

                        lock (queueStatements)
                        {
                            if (queueStatements.Count() > 0)
                            {
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
                    }catch(Exception e)
                    {
                        Logging.error("Exception occured in storage thread loop: " + e);
                    }
                    Thread.Yield();
                }

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


            // Checks and upgrades an older database if found
            public static bool checkForOlderDatabase()
            {
                if (File.Exists(filename) == false)
                    return false;

                // Bind the connection
                SQLiteConnection sqlConnectionOld = new SQLiteConnection(filename);

                // Check if the database is a very old, incompatible version
                var tableInfo = sqlConnectionOld.GetTableInfo("transactions");
                if (tableInfo.Exists(x => x.Name == "to"))
                {
                    sqlConnectionOld.Close();
                    File.Delete(filename);
                    return false;
                }

                Logging.info("Upgrading old blockchain file...");
                Logging.info("This operation will only happen once.");

                // Get the highest blocknum
                string sql = string.Format("SELECT * FROM `blocks` ORDER BY `blockNum` DESC LIMIT 1");
                var _seek_block = sqlConnectionOld.Query<_storage_Block>(sql).ToArray();
                if (_seek_block == null)
                    return false;
                if (_seek_block.Length < 1)
                    return false;
                _storage_Block sblk = _seek_block[0];

                for (long i = 0; i < sblk.blockNum; i++)
                {
                    if (i % (long)Config.maxBlocksPerDatabase == 0)
                        Logging.info(String.Format("Upgrade progress: {0} / {1}", i, sblk.blockNum));

                    Block block = null;
                    sql = "select * from blocks where `blocknum` = ? LIMIT 1"; // AND `blocknum` < (SELECT MAX(`blocknum`) - 5 from blocks)
                    _storage_Block[] _storage_block = null;
                    try
                    {
                        _storage_block = sqlConnectionOld.Query<_storage_Block>(sql, (long)i).ToArray();
                    }
                    catch (Exception e)
                    {
                        Logging.error(String.Format("Exception has been thrown while executing SQL Query {0}. Exception message: {1}", sql, e.Message));
                        continue;
                    }

                    if (_storage_block == null)
                        continue;

                    if (_storage_block.Length < 1)
                        continue;

                    _storage_Block blk = _storage_block[0];

                    block = new Block
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
                        version = blk.version
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
                        byte[][] newSig = new byte[2][];
                        newSig[0] = Convert.FromBase64String(split_sig[0]);
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

                        // Look and add transactions for this block
                        Transaction transaction = importOldTransaction(s1, sqlConnectionOld);
                        insertTransactionInternal(transaction);
                    }
                    insertBlockInternal(block);
                }

                // Close the database and remove the file
                sqlConnectionOld.Close();
                Logging.info("Upgrading complete, removing old blockchain file...");
                File.Delete(filename);

                return true;
            }

            // Imports an old transaction
            private static Transaction importOldTransaction(string txid, SQLiteConnection sqlConnectionOld)
            {
                Transaction transaction = null;

                string sql = "select * from transactions where `id` = ? LIMIT 1";
                List<_storage_Transaction> _storage_tx = null;
                try
                {
                    _storage_tx = sqlConnectionOld.Query<_storage_Transaction>(sql, txid);
                }
                catch (Exception e)
                {
                    Logging.error(String.Format("Exception has been thrown while executing SQL Query {0}. Exception message: {1}", sql, e.Message));
                    return null;
                }

                if (_storage_tx == null)
                    return transaction;

                if (_storage_tx.Count < 1)
                    return transaction;

                _storage_Transaction tx = _storage_tx[0];

                transaction = new Transaction(tx.type);
                transaction.id = tx.id;
                transaction.amount = new IxiNumber(tx.amount);
                transaction.fee = new IxiNumber(tx.fee);
                transaction.from = tx.from;
                transaction.data = tx.data;
                transaction.blockHeight = (ulong)tx.blockHeight;
                transaction.nonce = tx.nonce;
                transaction.timeStamp = tx.timestamp;
                transaction.checksum = tx.checksum;
                transaction.signature = tx.signature;
                transaction.version = tx.version;
                transaction.pubKey = tx.pubKey;
                transaction.applied = (ulong)tx.applied;

                // Add toList
                string[] split_str = tx.toList.Split(new string[] { "||" }, StringSplitOptions.None);
                int sigcounter = 0;
                foreach (string s1 in split_str)
                {
                    sigcounter++;
                    if (sigcounter == 1)
                        continue;

                    string[] split_to = s1.Split(new string[] { ":" }, StringSplitOptions.None);
                    byte[] address = Base58Check.Base58CheckEncoding.DecodePlain(split_to[0]);
                    IxiNumber amount = new IxiNumber(new BigInteger(Convert.FromBase64String(split_to[1])));
                    transaction.toList.AddOrReplace(address, amount);
                }

                return transaction;
            }
        }
        
    }
}