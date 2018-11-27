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

            private static SQLiteConnection sqlConnection;



            private static Thread thread = null;
            private static bool running = false;



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


            // Creates the storage file if not found
            public static bool prepareStorage()
            {
                bool prepare_database = false;
                // Check if the database file does not exist
                if (File.Exists(filename) == false)
                {
                    prepare_database = true;
                }

                // Bind the connection
                sqlConnection = new SQLiteConnection(filename);

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
                }else
                {
                    // database exists, check if it needs upgrading

                    var tableInfo = sqlConnection.GetTableInfo("transactions");
                    if(tableInfo.Exists(x => x.Name == "to"))
                    {
                        sqlConnection.Close();
                        File.Delete(filename);
                        return prepareStorage();
                    }

                }

                // Start thread
                running = true;
                thread = new Thread(new ThreadStart(threadLoop));
                thread.Start();

                return true;
            }

            // Shutdown storage thread
            public static void stopStorage()
            {
                running = false;
            }


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

            public static ulong getLastBlockNum()
            {
                string sql = string.Format("SELECT * FROM `blocks` ORDER BY `blockNum` DESC LIMIT 1");
                var _storage_block = sqlConnection.Query<_storage_Block>(sql).ToArray();

                if (_storage_block == null)
                    return 0;

                if (_storage_block.Length < 1)
                    return 0;

                _storage_Block blk = _storage_block[0];

                return (ulong)blk.blockNum;
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
                if (getTransaction(transaction.id) == null)
                {
                    string sql = "INSERT INTO `transactions`(`id`,`type`,`amount`,`fee`,`toList`,`from`,`data`,`blockHeight`, `nonce`, `timestamp`,`checksum`,`signature`, `pubKey`, `applied`, `version`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";
                    result = executeSQL(sql, transaction.id, transaction.type, transaction.amount.ToString(), transaction.fee.ToString(), toList, transaction.from, transaction.data, (long)transaction.blockHeight, transaction.nonce, transaction.timeStamp, transaction.checksum, transaction.signature, transaction.pubKey, (long)transaction.applied, transaction.version);
                }
                else
                {
                    // Likely already have the tx stored, update the old entry
                    string sql = "UPDATE `transactions` SET `type` = ?,`amount` = ? ,`fee` = ?, `toList` = ?, `from` = ?,`data` = ?, `blockHeight` = ?, `nonce` = ?, `timestamp` = ?,`checksum` = ?,`signature` = ?, `pubKey` = ?, `applied` = ?, `version` = ? WHERE `id` = ?";
                    result = executeSQL(sql, transaction.type, transaction.amount.ToString(), transaction.fee.ToString(), toList, transaction.from, transaction.data, (long)transaction.blockHeight, transaction.nonce, transaction.timeStamp, transaction.checksum, transaction.signature, transaction.pubKey, (long)transaction.applied, transaction.version, transaction.id);
                }

                return result;
            }

            public static Block getBlock(ulong blocknum)
            {
                if (blocknum < 0)
                {
                    return null;
                }
                Block block = null;
                string sql = "select * from blocks where `blocknum` = ? LIMIT 1"; // AND `blocknum` < (SELECT MAX(`blocknum`) - 5 from blocks)
                _storage_Block[] _storage_block = null;
                try
                {
                    _storage_block = sqlConnection.Query<_storage_Block>(sql, (long)blocknum).ToArray();
                }
                catch (Exception e)
                {
                    Logging.error(String.Format("Exception has been thrown while executing SQL Query {0}. Exception message: {1}", sql, e.Message));
                    return null;
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

            public static Block getBlockByHash(byte[] hash)
            {
                if (hash == null)
                {
                    return null;
                }
                Block block = null;
                string sql = "select * from blocks where `blockChecksum` = ? LIMIT 1";
                _storage_Block[] _storage_block = null;
                try
                {
                    _storage_block = sqlConnection.Query<_storage_Block>(sql, hash).ToArray();
                }
                catch (Exception e)
                {
                    Logging.error(String.Format("Exception has been thrown while executing SQL Query {0}. Exception message: {1}", sql, e.Message));
                    return null;
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

                string sql = "select * from transactions where `id` = ? LIMIT 1";
                _storage_Transaction[] _storage_tx = null;
                try
                {
                    _storage_tx = sqlConnection.Query<_storage_Transaction>(sql, txid).ToArray();
                }
                catch (Exception e)
                {
                    Logging.error(String.Format("Exception has been thrown while executing SQL Query {0}. Exception message: {1}", sql, e.Message));
                    return null;
                }

                if (_storage_tx == null)
                    return transaction;

                if (_storage_tx.Length < 1)
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

            // Removes a transaction from the storage database
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

        }
        /**/
    }
}