using SQLite;
using System;
using System.Collections.Generic;
using System.IO;

namespace DLT.Meta
{

    public class Activity
    {
        public long id { get; set; }
        public string address { get; set; }
        public int type { get; set; }
        public string data { get; set; }
        public long timestamp { get; set; }
        public int version { get; set; }
    }

    public class ActivityStorage
    {
        public static string filename = "activity.dat";

        // Sql connections
        private static SQLiteConnection sqlConnection = null;
        private static readonly object storageLock = new object(); // This should always be placed when performing direct sql operations

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
                string sql = "CREATE TABLE `activity` (`id`	INTEGER NOT NULL, `address` TEXT, `type` INTEGER, `data` BLOB, `timestamp` INTEGER, `version` INTEGER, PRIMARY KEY(`id`));";
                executeSQL(sql);

                sql = "CREATE INDEX `type` ON `activity` (`type`);";
                executeSQL(sql);
                sql = "CREATE INDEX `address` ON `activity` (`from`);";
                executeSQL(sql);
            }

            return true;
        }
        
        public static List<Activity> getActivity(string address, int fromIndex, int count, bool descending)
        {
            if (address.Length < 1)
            {
                return null;
            }

            string orderBy = " ORDER BY `timestamp` ASC";
            if (descending)
            {
                orderBy = " ORDER BY `timestamp` DESC";
            }

            string sql = "select * from `activity` where `address` = ? LIMIT " + fromIndex + ", " + count + orderBy;
            List<Activity> activity_list = null;

            lock (storageLock)
            {
                try
                {
                    activity_list = sqlConnection.Query<Activity>(sql, address);
                }
                catch (Exception e)
                {
                    Logging.error(String.Format("Exception has been thrown while executing SQL Query {0}. Exception message: {1}", sql, e.Message));
                    return null;
                }
            }

            return activity_list;
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

        public static void deleteCache()
        {
            string[] fileNames = Directory.GetFiles(Config.dataFoldername + Path.DirectorySeparatorChar + "blocks" + Path.DirectorySeparatorChar + "0000");
            foreach (string fileName in fileNames)
            {
                File.Delete(fileName);
            }
        }
    }
}
