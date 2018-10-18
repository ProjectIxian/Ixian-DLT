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
            public static int serverPort = 10234;
            public static int testnetServerPort = 11234;
            public static int apiPort = 8081;
            public static int testnetApiPort = 8181;
            public static string publicServerIP = "127.0.0.1";

            public static bool storeFullHistory = true; // Flag confirming this is a full history node
            public static bool recoverFromFile = false; // Flag allowing recovery from file
            public static bool disableMiner = true; // Flag to disable miner

            public static string genesisFunds = "0"; // If 0, it'll use a hardcoded wallet address
            public static string genesis2Address = ""; // For a secondary genesis node

            public static string walletFile = "wallet.dat";

            // Store the device id in a cache for reuse in later instances
            public static string device_id = Guid.NewGuid().ToString();
            public static string externalIp = "";

            // Read-only values
            public static readonly string version = "xdc-0.4.5"; // DLT Node version
            public static bool isTestNet = false; // Testnet designator

            public static readonly int maxLogFileSize = 50 * 1024 * 1024; // 50MB

            public static readonly int protocolVersion = 5; // Ixian protocol version
            public static readonly int walletStateChunkSplit = 10000; // 10K wallets per chunk
            public static readonly int networkClientReconnectInterval = 10 * 1000; // Time in milliseconds
            public static readonly int keepAliveInterval = 45; // Number of seconds to wait until next keepalive ping
            public static readonly int maximumNeighborReconnectCount = 5; // Number of retries before proceeding to a different neighbor node
            public static readonly int simultaneousConnectedNeighbors = 6; // Desired number of simulatenously connected neighbor nodes
            public static readonly int maximumServerClients = 10; // Maximum number of clients this server can accept
            public static readonly ulong deprecationBlockOffset = 86400; // 86.4k blocks ~= 30 days
            public static readonly ulong compileTimeBlockNumber = 0;

            public static readonly double networkConsensusRatio = 0.75;

            public static readonly int defaultRsaKeySize = 4096;

            public static readonly int maxNetworkQueue    = 10000; // Maximum number of received messages in network queue before throttling starts
            public static readonly int maxSendQueue       = 10000; // Maximum number of sent messages in queue per endpoint
            public static readonly int maxMessageSize = 5000000; // Maximum message size in bytes

            public static readonly int pingInterval = 5; // how long to wait for data before sending ping
            public static readonly int pingTimeout = 5; // how long to wait for data after sending ping 

            public static readonly ulong redactedWindowSize = 43200; // approx 15 days. Represents the redacted window size of this node
            public static readonly ulong minimumRedactedWindowSize = 43200; // Represents the minimum redacted window size on any node


            // Transactions and fees
            public static readonly IxiNumber minimumMasterNodeFunds = new IxiNumber("20000"); // Limit master nodes to this amount or above
            public static readonly IxiNumber transactionPrice = 5000; // Per kB
            public static readonly IxiNumber foundationFeePercent = 3; // 3% of transaction fees
            public static readonly string foundationAddress = "018902fd36a0541fa6f95eb8fd15fbee02976979553488098216fc69e686604a56c0"; // Foundation wallet address
            public static readonly IxiNumber relayPriceInitial = new IxiNumber("0.0002"); // Per kB
            public static readonly int nodeNewTransactionsLimit = 3000000; // Limit the number of new transactions per node per block TODO TODO TODO deprecate soon, we have other systems in place for throttling
            public static readonly ulong maximumTransactionsPerBlock = 2000; // Limit the maximum number of transactions in a newly generated block
            public static readonly int maximumTransactionsPerChunk = 500; // Limit the maximum number of transactions per transaction chunk

            // Debugging values
            public static string networkDumpFile = "";

            // internal
            public static bool changePass = false;

            private Config()
            {

            }

            private static string outputHelp()
            {
                DLTNode.Program.noStart = true;

                Console.WriteLine("Starts a new instance of Ixian DLT Node");
                Console.WriteLine("");
                Console.WriteLine("ixiandlt.exe [-h] [-v] [-t] [-s] [-m] [-x] [-r] [-c] [-p port] [-a port] [-i ip] [-g] [-w wallet.dat] [-n seed1.ixian.io:10234] [-d]");
                Console.WriteLine("");
                Console.WriteLine("   -h\t\t Displays this help");
                Console.WriteLine("   -v\t\t Displays version");
                Console.WriteLine("   -t\t\t Starts node in testnet mode");
                Console.WriteLine("   -s\t\t Saves full history");
                Console.WriteLine("   -m\t\t Enables mining");
                Console.WriteLine("   -x\t\t Change password of an existing wallet");
                Console.WriteLine("   -r\t\t Recovers from file (to be used only when recovering the network)");
                Console.WriteLine("   -c\t\t Removes blockchain.dat, peers.dat and ixian.log files before starting");
                Console.WriteLine("   -p\t\t Port to listen on");
                Console.WriteLine("   -a\t\t HTTP/API port to listen on");
                Console.WriteLine("   -i\t\t External IP Address to use");
                Console.WriteLine("   -g\t\t Start node in genesis mode");
                Console.WriteLine("   -w\t\t Specify location of the wallet.dat file");
                Console.WriteLine("   -n\t\t Specify which seed node to use");
                Console.WriteLine("   -d\t\t Enable netdump for debugging purposes");

                return "";
            }

            private static string outputVersion()
            {
                DLTNode.Program.noStart = true;

                // Do nothing since version is the first thing displayed

                return "";
            }

            public static void readFromCommandLine(string[] args)
            {
                //Logging.log(LogSeverity.info, "Reading config...");

                // first pass
                var cmd_parser = new FluentCommandLineParser();

                // testnet
                cmd_parser.Setup<bool>('t', "testnet").Callback(value => isTestNet = true).Required();

                cmd_parser.Parse(args);

                if (isTestNet)
                {
                    serverPort = testnetServerPort;
                    apiPort = testnetApiPort;
                }

                string seedNode = "";


                // second pass
                cmd_parser = new FluentCommandLineParser();

                bool start_clean = false; // Flag to determine if node should delete cache+logs

                // help
                cmd_parser.SetupHelp("h", "help").Callback(text => outputHelp());

                // version
                cmd_parser.Setup<bool>('v', "version").Callback(text => outputVersion());

                // Toggle between full history node and no history
                cmd_parser.Setup<bool>('s', "save-history").Callback(value => storeFullHistory = value).Required();

                // Toggle between mining and no mining mode
                cmd_parser.Setup<bool>('m', "miner").Callback(value => disableMiner = false).Required();

                // Check for password change
                cmd_parser.Setup<bool>('x', "changepass").Callback(value => changePass = value).Required();

                // Check for recovery parameter
                cmd_parser.Setup<bool>('r', "recover").Callback(value => recoverFromFile = value).Required();

                // Check for clean parameter
                cmd_parser.Setup<bool>('c', "clean").Callback(value => start_clean = value).Required();


                cmd_parser.Setup<int>('p', "port").Callback(value => serverPort = value).Required();

                cmd_parser.Setup<int>('a', "apiport").Callback(value => apiPort = value).Required();

                cmd_parser.Setup<string>('i', "ip").Callback(value => externalIp = value).SetDefault("");

                // Convert the genesis block funds to ulong, as only long is accepted with FCLP
                cmd_parser.Setup<string>('g', "genesis").Callback(value => genesisFunds = value).Required();

                cmd_parser.Setup<string>("genesis2").Callback(value => genesis2Address = value).Required();

                cmd_parser.Setup<string>('w', "wallet").Callback(value => walletFile = value).Required();

                cmd_parser.Setup<string>('n', "node").Callback(value => seedNode = value).Required();

                // Debug
                cmd_parser.Setup<string>('d', "netdump").Callback(value => networkDumpFile = value).SetDefault("");

                cmd_parser.Parse(args);

                if (start_clean)
                {
                    Node.cleanCacheAndLogs();
                }

                if (seedNode != "")
                {
                    if (isTestNet)
                    {
                        Network.CoreNetworkUtils.seedTestNetNodes = new string[]
                        {
                            seedNode
                        };
                    }
                    else
                    {
                        Network.CoreNetworkUtils.seedNodes = new string[]
                        {
                            seedNode
                        };
                    }
                }
            }
        }
    }
}