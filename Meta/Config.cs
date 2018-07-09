using Fclp;
using System;

namespace DLT
{
    namespace Meta
    {

        public class Config
        {
            // Providing pre-defined values
            // Can be read from a file later, or read from the command line
            public static int serverPort = 10515;
            public static int apiPort = 8081;
            public static string publicServerIP = "127.0.0.1";

            public static bool noHistory = false; // Flag confirming this is a full history node
            public static bool recoverFromFile = false; // Flag allowing recovery from file

            public static ulong genesisFunds = 0; // If 0, it'll use a hardcoded wallet address

            public static string walletFile = "wallet.dat";

            // Store the device id in a cache for reuse in later instances
            public static string device_id = Guid.NewGuid().ToString();


            // Read-only values
            public static readonly string dltVersion = "0.1.0";
            public static readonly int nodeVersion = 4; // Node protocol version
            public static readonly ulong minimumMasterNodeFunds = 2000; // Limit master nodes to this amount or above
            public static readonly int walletStateChunkSplit = 10000; // 10K wallets per chunk
            public static readonly int networkClientReconnectInterval = 10 * 1000; // Time in milliseconds
            public static readonly int keepAliveInterval = 45; // Number of seconds to wait until next keepalive ping
            public static readonly ulong deprecationBlockOffset = 86400; // 86.4k blocks ~= 30 days
            public static readonly ulong compileTimeBlockNumber = 0;

            private static Config singletonInstance;
            private Config()
            {

            }

            public static Config singleton
            {
                get
                {
                    if (singletonInstance == null)
                    {
                        singletonInstance = new Config();
                    }
                    return singletonInstance;
                }
            }

            public static void readFromCommandLine(string[] args)
            {
                Logging.log(LogSeverity.info, "Reading config...");
                var cmd_parser = new FluentCommandLineParser();

                // Read the parameters from the command line and overwrite values if necessary
                string new_value = "";
                cmd_parser.Setup<string>('v').Callback(value => new_value = value).Required();

                // Toggle between full history node and no history
                cmd_parser.Setup<bool>('n', "no-history").Callback(value => noHistory = value).Required();

                // Check for recovery parameter
                cmd_parser.Setup<bool>('r', "recover").Callback(value => recoverFromFile = value).Required();


                cmd_parser.Setup<int>('p', "port").Callback(value => serverPort = value).Required();
                cmd_parser.Setup<int>('a', "apiport").Callback(value => apiPort = value).Required();

                // Convert the genesis block funds to ulong, as only long is accepted with FCLP
                cmd_parser.Setup<long>('g', "genesis").Callback(value => genesisFunds = (ulong)value).Required();

                cmd_parser.Setup<string>('w', "wallet").Callback(value => walletFile = value).Required();

                cmd_parser.SetupHelp("h", "help").Callback(text => Console.WriteLine("DLT Help"));


                cmd_parser.Parse(args);

                // Log the parameters to notice any changes
                Logging.log(LogSeverity.info, String.Format("Server Port: {0}", serverPort));
                Logging.log(LogSeverity.info, String.Format("API Port: {0}", apiPort));
                Logging.log(LogSeverity.info, String.Format("Wallet File: {0}", walletFile));

            }

        }

    }
}