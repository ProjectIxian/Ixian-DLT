using Newtonsoft.Json;
using SQLite;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace DLT
{
    namespace Meta
    {   
        public class Storage
        {
            public static string filename = "blockchain.dat";
            public static string presenceFilename = "presence.dat";

            private static SQLiteConnection sqlConnection;

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
                    string sql = "CREATE TABLE `blocks` (`blockNum`	INTEGER NOT NULL, `blockChecksum` TEXT, `lastBlockChecksum` TEXT, `walletStateChecksum`	TEXT, `sigFreezeChecksum` TEXT, `difficulty` INTEGER, `powField` TEXT, `transactions` TEXT, `signatures` TEXT, PRIMARY KEY(`blockNum`));";
                    executeSQL(sql);

                    sql = "CREATE TABLE `transactions` (`id` TEXT, `type` INTEGER, `amount` TEXT, `fee` TEXT, `to` TEXT, `from` TEXT,  `data` TEXT, `nonce` INTEGER, `timestamp` TEXT, `checksum` TEXT, `signature` TEXT, `applied` INTEGER, PRIMARY KEY(`id`));";
                    executeSQL(sql);
                }

                // Check if the presence file exists
                if (File.Exists(presenceFilename))
                {
                    Logging.info("Presence file found. Adding addresses to initial connections.");
                }

                return true;
            }

            // Saves a list of 500 master node addresses to a file
            public static void savePresenceFile()
            {
                // Get a list of master node presences
                List<string> mn_presences = PresenceList.getMasterNodeAddresses(500);
                // Don't write to file if no masternode presences were found in addition to the current node
                if (mn_presences.Count < 2)
                    return;

                using (TextWriter tw = new StreamWriter(presenceFilename))
                {
                    foreach (String addr in mn_presences)
                        tw.WriteLine(addr);
                }
                mn_presences.Clear();
            }

            // Retrieves the master node address file's contents
            public static List<string> readPresenceFile()
            {
                List<string> mn_presences = new List<string>();
                try
                {
                    mn_presences = File.ReadAllLines(presenceFilename).ToList();
                }
                catch(Exception)
                {

                }

                return mn_presences;
            }


            public class _storage_Block
            {
                public long blockNum { get; set; }
                public string blockChecksum { get; set; }
                public string lastBlockChecksum { get; set; }
                public string walletStateChecksum { get; set; }
                public string sigFreezeChecksum { get; set; }
                public long difficulty { get; set; }
                public string powField { get; set; }
                public string signatures { get; set; }
                public string transactions { get; set; }
            }

            public class _storage_Transaction
            {
                public string id { get; set; }
                public int type { get; set; }
                public string amount { get; set; }
                public string fee { get; set; }
                public string to { get; set; }
                public string from { get; set; }
                public string data { get; set; }
                public long nonce { get; set; }
                public string timestamp { get; set; }
                public string checksum { get; set; }
                public string signature { get; set; }
                public long applied { get; set; }
            }

            public static bool readFromStorage()
            {
                Logging.info("Reading blockchain from storage");

                string sql = string.Format("SELECT * FROM blocks ORDER BY blockNum DESC LIMIT 1");
                var _storage_block = sqlConnection.Query<_storage_Block>(sql).ToArray();

                if (_storage_block == null)
                    return false;

                if (_storage_block.Length < 1)
                    return false;

                _storage_Block blk = _storage_block[0];

                Logging.info(string.Format("Storage blockchain goes up to block #{0}", blk.blockNum));

                Node.blockSync.onHelloDataReceived((ulong)blk.blockNum, blk.blockChecksum, blk.walletStateChecksum, 1);

                return true;
            }

            public static bool appendToStorage(byte[] data)
            {
                // Check if history is enabled
                if(Config.storeFullHistory == true)
                {
                    return false;
                }
                return true;
            }

            public static bool insertBlock(Block block)
            {
                // TODO: prevent this from executing when the inserted block is from storage instead of network

                Block b = block;
                string transactions = "";
                foreach(string tx in block.transactions)
                {
                    transactions = string.Format("{0}||{1}", transactions, tx);
                }

                string signatures = "";
                foreach(string sig in block.signatures)
                {
                    signatures = string.Format("{0}||{1}", signatures, sig);
                }
                
                string sql = string.Format(
                    "INSERT INTO `blocks`(`blockNum`,`blockChecksum`,`lastBlockChecksum`,`walletStateChecksum`,`sigFreezeChecksum`, `difficulty`, `powField`, `transactions`,`signatures`) VALUES ({0},\"{1}\",\"{2}\",\"{3}\",\"{4}\",\"{5}\",\"{6}\",\"{7}\",\"{8}\");",
                    block.blockNum, block.blockChecksum, block.lastBlockChecksum, block.walletStateChecksum, block.signatureFreezeChecksum, block.difficulty, block.powField, transactions, signatures);
                bool result = executeSQL(sql);
                if(result == false)
                {
                    // Likely already have the block stored, update the old entry
                    sql = string.Format("UPDATE `blocks` SET `blockChecksum` = \"{0}\", `lastBlockChecksum` = \"{1}\", `walletStateChecksum` = \"{2}\", `sigFreezeChecksum` = \"{3}\", `difficulty` = \"{4}\", `powField` = \"{5}\", `transactions` = \"{6}\", `signatures` = \"{7}\" WHERE `blockNum` =  {8}",
                        block.blockChecksum, block.lastBlockChecksum, block.walletStateChecksum, block.signatureFreezeChecksum, block.difficulty, block.powField, transactions, signatures, block.blockNum);
                    //Console.WriteLine("SQL: {0}", sql);
                    executeSQL(sql);
                }


                return true;
            }

            public static bool insertTransaction(Transaction transaction)
            {
                string sql = string.Format("INSERT INTO `transactions`(`id`,`type`,`amount`,`fee`,`to`,`from`,`data`, `nonce`, `timestamp`,`checksum`,`signature`, `applied`) VALUES (\"{0}\",{1},\"{2}\",\"{3}\",\"{4}\",\"{5}\",\"{6}\", {7}, \"{8}\",\"{9}\", \"{10}\", {11});",
                    transaction.id, transaction.type, transaction.amount.ToString(), transaction.fee.ToString(), transaction.to, transaction.from, transaction.data, transaction.nonce, transaction.timeStamp, transaction.checksum, transaction.signature, transaction.applied);
                bool result = executeSQL(sql);
                if (result == false)
                {
                    // Likely already have the tx stored, update the old entry
                    sql = string.Format("UPDATE `transactions` SET `type` = {1},`amount` = \"{2}\" ,`fee` = \"{3}\",`to` = \"{4}\",`from` = \"{5}\",`data` = \"{6}\", `nonce` = {7}, `timestamp` = \"{8}\",`checksum` = \"{9}\",`signature` = \"{10}\", `applied` = {11} WHERE `id` = \"{0}\"",
    transaction.id, transaction.type, transaction.amount.ToString(), transaction.fee.ToString(), transaction.to, transaction.from, transaction.data, transaction.nonce, transaction.timeStamp, transaction.checksum, transaction.signature, transaction.applied);
                    return executeSQL(sql);
                }

                return result;
            }

            public static Block getBlock(ulong blocknum)
            {
                Block block = null;
                string sql = string.Format("select * from blocks where `blocknum` = {0} LIMIT 1", blocknum); // AND `blocknum` < (SELECT MAX(`blocknum`) - 5 from blocks)
                var _storage_block = sqlConnection.Query<_storage_Block>(sql).ToArray();

                if(_storage_block == null)
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
                    signatures = new List<string>()
                };

                // Add signatures
                string[] split_str = blk.signatures.Split(new string[] { "||" }, StringSplitOptions.None);
                int sigcounter = 0;
                foreach (string s1 in split_str)
                {
                    sigcounter++;
                    if (sigcounter == 1)
                        continue;

                    if (!block.containsSignature(s1))
                    {
                        block.signatures.Add(s1);
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

                Console.WriteLine("Read block #{0} from storage.", block.blockNum);
                return block;
            }

            // Retrieve a transaction from the sql database
            public static Transaction getTransaction(string txid)
            {

                Transaction transaction = null;

                string sql = string.Format("select * from transactions where `id` = \"{0}\"", txid);
                var _storage_tx = sqlConnection.Query<_storage_Transaction>(sql).ToArray();

                if (_storage_tx == null)
                    return transaction;

                if (_storage_tx.Length < 1)
                    return transaction;

                _storage_Transaction tx = _storage_tx[0];

                transaction = new Transaction();
                transaction.id = tx.id;
                transaction.amount = new IxiNumber(tx.amount);
                transaction.fee = new IxiNumber(tx.fee);
                transaction.type = tx.type;
                transaction.from = tx.from;
                transaction.to = tx.to;
                transaction.data = tx.data;
                transaction.nonce = (ulong)tx.nonce;
                transaction.timeStamp = tx.timestamp;
                transaction.checksum = tx.checksum;
                transaction.signature = tx.signature;

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

                // First go through all transactions and remove them from storage
                foreach (string txid in block.transactions)
                {
                    if (removeTransaction(txid) == false)
                        return false;
                }

                // Now remove the block itself from storage
                string sql = string.Format("DELETE FROM blocks where `blockNum` = \"{0}\"", block.blockNum);
                return executeSQL(sql);
            }

            // Removes a transaction from the storage database
            public static bool removeTransaction(string txid)
            {
                string sql = string.Format("DELETE FROM transactions where `id` = \"{0}\"", txid);             
                return executeSQL(sql);
            }

            // Remove all previous blocks and corresponding transactions outside the redacted window
            // Takes the assigned blockheight and calculates the redacted window automatically
            public static bool redactBlockStorage(ulong blockheight)
            {
                // Only redact on non-history nodes
                if(Config.storeFullHistory == true)
                {
                    return false;
                }

                if (blockheight < Node.blockChain.redactedWindow)
                {
                    // Nothing to redact yet
                    return false;
                }

                // Calculate the window
                ulong redactedWindow = blockheight - Node.blockChain.redactedWindow;

                Logging.info(string.Format("Redacting storage below block #{0}", redactedWindow));

                string sql = string.Format("select * from blocks where `blocknum` < {0}", redactedWindow);
                var _storage_blocks = sqlConnection.Query<_storage_Block>(sql).ToArray();

                if (_storage_blocks == null)
                    return false;

                if (_storage_blocks.Length < 1)
                    return false;

                // Go through each block
                foreach(_storage_Block blk in _storage_blocks)
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
                    sql = string.Format("DELETE FROM blocks where `blockNum` = \"{0}\"", blk.blockNum);
                    executeSQL(sql);
                }

                return true;
            }

            // Escape and execute an sql command
            private static bool executeSQL(string sql)
            {
                // TODO: secure any potential injections here
                try
                {
                    sqlConnection.Execute(sql);
                }
                catch (Exception e)
                {
                    Logging.info(String.Format("Exception has been thrown while executing SQL Query {0}. Exception message: {1}", sql, e.Message));
                    return false;
                }
                return true;
            }

            // Execute a prepared SQL query
            private static bool executeSQL(string sql, params object[] sqlParameters)
            {
                SQLiteCommand sqlCommand = sqlConnection.CreateCommand(sql, sqlParameters);
                try
                {
                    sqlCommand.ExecuteNonQuery();
                }
                catch (Exception e)
                {
                    Logging.info(String.Format("Exception has been thrown while executing SQL Query {0}. Exception message: {1}", sql, e.Message));
                    return false;
                }
                return true;
            }

        }
        /**/
    }
}