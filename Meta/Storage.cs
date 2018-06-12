using SQLite;
using System;
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

            public static bool readFromStorage()
            {
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
                string sql = string.Format("INSERT INTO `transactions`(`id`,`type`,`amount`,`to`,`from`,`timestamp`,`checksum`,`signature`) VALUES (\"{0}\",{1},{2},\"{3}\",\"{4}\",\"{5}\",\"{6}\",\"{7}\");",
                    transaction.id, transaction.type, transaction.amount, transaction.to, transaction.from, transaction.timeStamp, transaction.checksum, transaction.signature);
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
    }
}