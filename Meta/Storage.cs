using SQLite;
using System;
using System.Collections.Generic;
using System.IO;

namespace DLT
{
    namespace Meta
    {   
        public class Storage
        {
            public static string filename = "blockchain.dat";

            private static SQLiteConnection sqlConnection;

            // Creates the storage file if not found
            public static bool prepareStorage()
            {
                // Check if history is enabled
                if (Config.noHistory == true)
                {
                    return false;
                }

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
                    string sql = "CREATE TABLE `blocks` (`blockNum`	INTEGER NOT NULL, `blockChecksum` TEXT, `lastBlockChecksum` TEXT, `walletStateChecksum`	TEXT, `transactions` TEXT, `signatures` TEXT, PRIMARY KEY(`blockNum`));";
                    executeSQL(sql);

                    sql = "CREATE TABLE `transactions` (`id` TEXT, `type` INTEGER, `amount` INTEGER, `to` TEXT, `from` TEXT, `timestamp` TEXT, `checksum` TEXT, `signature` TEXT, PRIMARY KEY(`id`));";
                    executeSQL(sql);
                }

                return true;
            }


            public class _storage_Block
            {
                public long blockNum { get; set; }
                public string blockChecksum { get; set; }
                public string lastBlockChecksum { get; set; }
                public string walletStateChecksum { get; set; }
                public string signatures { get; set; }
                public string transactions { get; set; }
            }

            public class _storage_Transaction
            {
                public string id { get; set; }
                public int type { get; set; }
                public string amount { get; set; }
                public string to { get; set; }
                public string from { get; set; }
                public string timestamp { get; set; }
                public string signature { get; set; }
            }

            public static bool readFromStorage()
            {
                Logging.info("Reading blockchain from storage");

                // Setup the genesis balances
                Node.walletState.setWalletBalance("70e27b7f48ef8f6cf691b331879c2fb9a5edfb7239d4ca463764d25e48189f51", new IxiNumber("99999999999999"));
                Node.walletState.setWalletBalance("fca32c0ab94f1051adb16881e41e1fa5024076615bd0e63d14cc00738c89b6d0", new IxiNumber("4000"));
                Node.walletState.setWalletBalance("b27785b95e534eabd9ddb2785ed6b841262eed4d74284e87f0518b635fe12e29", new IxiNumber("4000"));
                Node.walletState.setWalletBalance("1dab9d028759a4fd84503a15cd680ec964ccec17a034347408c34d15195baa76", new IxiNumber("4000"));

                Logging.info(string.Format("Genesis wallet state checksum: {0}", Node.walletState.calculateWalletStateChecksum()));

                var _storage_txlist = sqlConnection.Query<_storage_Transaction>("select * from transactions").ToArray();

                List<Transaction> cached_transactions = new List<Transaction>();

                foreach (_storage_Transaction tx in _storage_txlist)
                {
                    Console.WriteLine("{0} {1}", tx.id, tx.amount);

                    Transaction new_transaction = new Transaction();
                    new_transaction.id = tx.id;
                    new_transaction.amount = new IxiNumber(tx.amount);
                    new_transaction.type = tx.type;
                    new_transaction.from = tx.from;
                    new_transaction.to = tx.to;
                    new_transaction.timeStamp = tx.timestamp;
                    new_transaction.signature = tx.signature;

                    cached_transactions.Add(new_transaction);

                    
                    // Don't remove 'pending' transactions
                    // TODO: add an expire condition to prevent potential spaming of the txpool
                    if (new_transaction.amount == 0)
                    {
                        continue;
                    }

                    // Applies the transaction to the wallet state
                    // TODO: re-validate the transactions here to prevent any potential exploits
                    IxiNumber fromBalance = Node.walletState.getWalletBalance(new_transaction.from);
                    IxiNumber finalFromBalance = fromBalance - new_transaction.amount;

                    IxiNumber toBalance = Node.walletState.getWalletBalance(new_transaction.to);

                    Node.walletState.setWalletBalance(new_transaction.to, toBalance + new_transaction.amount);
                    Node.walletState.setWalletBalance(new_transaction.from, fromBalance - new_transaction.amount);
                    

                }

                // Go through each block now
                var _storage_blocklist = sqlConnection.Query<_storage_Block>("select * from blocks").ToArray();

                foreach (_storage_Block block in _storage_blocklist)
                {
                    Console.WriteLine("{0} {1}", block.blockNum, block.blockChecksum);
                    Block new_block = new Block();
                    new_block.blockNum = (ulong)block.blockNum;
                    new_block.blockChecksum = block.blockChecksum;
                    new_block.lastBlockChecksum = block.lastBlockChecksum;
                    new_block.walletStateChecksum = block.walletStateChecksum;

                    string[] split_str = block.signatures.Split(new string[] { "||" }, StringSplitOptions.None);
                    int sigcounter = 0;
                    foreach (string s1 in split_str)
                    {
                        sigcounter++;
                        if (sigcounter == 1)
                            continue;

                        new_block.signatures.Add(s1);
                    }

                    string[] split_str2 = block.transactions.Split(new string[] { "||" }, StringSplitOptions.None);
                    int txcounter = 0;
                    foreach (string s1 in split_str2)
                    {
                        txcounter++;
                        if (txcounter == 1)
                            continue;

                        new_block.transactions.Add(s1);

                        foreach(Transaction new_transaction in cached_transactions)
                        {
                            if(new_transaction.id.Equals(s1, StringComparison.Ordinal))
                            {
                                if (new_transaction.amount == 0)
                                {
                                    continue;
                                }

                                // Applies the transaction to the wallet state
                                // TODO: re-validate the transactions here to prevent any potential exploits
                                IxiNumber fromBalance = Node.walletState.getWalletBalance(new_transaction.from);
                                IxiNumber finalFromBalance = fromBalance - new_transaction.amount;

                                IxiNumber toBalance = Node.walletState.getWalletBalance(new_transaction.to);

                             //   WalletState.setBalanceForAddress(new_transaction.to, toBalance + new_transaction.amount);
                             //   WalletState.setBalanceForAddress(new_transaction.from, fromBalance - new_transaction.amount);
                            }
                        }
                    }
                    //       Node.blockProcessor.checkIncomingBlock(new_block, null);
                }

                _storage_Block last_block = _storage_blocklist[_storage_blocklist.Length - 1];
                Logging.info(string.Format("Last block {0} - Wallet Checksum {2}", last_block.blockNum, last_block.blockChecksum, last_block.walletStateChecksum));

                Block initial_block = new Block();
                initial_block.blockNum = (ulong)last_block.blockNum;
                initial_block.blockChecksum = last_block.blockChecksum;
                initial_block.lastBlockChecksum = last_block.lastBlockChecksum;
                initial_block.walletStateChecksum = last_block.walletStateChecksum;

                string[] split = last_block.signatures.Split(new string[] { "||" }, StringSplitOptions.None);
                int sigcount = 0;
                foreach (string s1 in split)
                {
                    sigcount++;
                    if (sigcount == 1)
                        continue;

                    initial_block.signatures.Add(s1);
                }

        //        Node.blockProcessor.enterSyncMode((ulong)last_block.blockNum, last_block.blockChecksum, last_block.walletStateChecksum);
        //        Node.blockProcessor.setInitialLocalBlock(initial_block);
        //        Node.blockProcessor.exitSyncMode();

                Console.WriteLine("Current wallet state checksum: {0}", Node.walletState.calculateWalletStateChecksum());

                return true;
            }

            public static bool appendToStorage(byte[] data)
            {
                // Check if history is enabled
                if(Config.noHistory == true)
                {
                    return false;
                }
                return true;
            }

            public static bool insertBlock(Block block)
            {
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
                
                string sql = string.Format("INSERT INTO `blocks`(`blockNum`,`blockChecksum`,`lastBlockChecksum`,`walletStateChecksum`,`transactions`,`signatures`) VALUES ({0},\"{1}\",\"{2}\",\"{3}\",\"{4}\",\"{5}\");",
                    block.blockNum, block.blockChecksum, block.lastBlockChecksum, block.walletStateChecksum, transactions, signatures);
                executeSQL(sql);
                
                return true;
            }

            public static bool insertTransaction(Transaction transaction)
            {
                string sql = string.Format("INSERT INTO `transactions`(`id`,`type`,`amount`,`to`,`from`,`timestamp`,`checksum`,`signature`) VALUES (\"{0}\",{1},\"{2}\",\"{3}\",\"{4}\",\"{5}\",\"{6}\",\"{7}\");",
                    transaction.id, transaction.type, transaction.amount.ToString(), transaction.to, transaction.from, transaction.timeStamp, transaction.checksum, transaction.signature);
                executeSQL(sql);

                return false;
            }


            // Escape and execute an sql command
            private static bool executeSQL(string sql)
            {
                // TODO: secure any potential injections here
                try
                {
                    sqlConnection.Execute(sql);
                }
                catch(Exception)
                {
                    return false;
                }
                return true;
            }

        }
        /**/
    }
}