using Fclp;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

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

            public static Dictionary<string, string> apiUsers = new Dictionary<string, string>();

            public static bool storeFullHistory = true; // Flag confirming this is a full history node
            public static bool recoverFromFile = false; // Flag allowing recovery from file
            public static bool disableMiner = false; // Flag to disable miner
            public static bool workerOnly = false; // Flag to disable masternode capability

            public static bool verboseConsoleOutput = false; // Flag for verbose console output

            public static string genesisFunds = "0"; // If 0, it'll use a hardcoded wallet address
            public static string genesis2Address = ""; // For a secondary genesis node
           
            public static uint miningThreads = 1;

            public static string dataFolderPath = "data";
            public static string configFilename = "ixian.cfg";
            public static string walletFile = "ixian.wal";

            public static int maxLogSize = 50;
            public static int maxLogCount = 10;

            public static ulong lastGoodBlock = 0;
            public static bool disableWebStart = false;

            public static bool fullStorageDataVerification = false;

            // Store the device id in a cache for reuse in later instances
            public static string device_id = Guid.NewGuid().ToString();
            public static string externalIp = "";

            // Read-only values
            public static readonly string version = "xdc-0.6.1-dev"; // DLT Node version
            public static bool isTestNet = false; // Testnet designator

            public static readonly ulong maxBlocksPerDatabase = 1000;
            public static readonly ulong deprecationBlockOffset = 86400; // 86.4k blocks ~= 30 days
            public static readonly ulong compileTimeBlockNumber = 129600;

            // Debugging values
            public static string networkDumpFile = "";
            public static int benchmarkKeys = 0;

            public static int forceTimeOffset = int.MaxValue;

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
                Console.WriteLine(" IxianDLT.exe [-h] [-v] [-t] [-s] [-x] [-c] [-p 10234] [-a 8081] [-i ip] [-w ixian.wal] [-n seed1.ixian.io:10234] [--worker] [--threads 1] [--config ixian.cfg] [--maxLogSize 50] [--maxLogCount 10] [--lastGoodBlock 110234] [--disableWebStart] [--disableMiner] [--genesis] [--netdump dumpfile] [--recover] [--forceTimeOffset 0]");
                Console.WriteLine("");
                Console.WriteLine("    -h\t\t\t Displays this help");
                Console.WriteLine("    -v\t\t\t Displays version");
                Console.WriteLine("    -t\t\t\t Starts node in testnet mode");
                Console.WriteLine("    -s\t\t\t Saves full history");
                Console.WriteLine("    -x\t\t\t Change password of an existing wallet");
                Console.WriteLine("    -c\t\t\t Removes blockchain cache, walletstate cache, peers.dat and ixian.log files before starting");
                Console.WriteLine("    -p\t\t\t Port to listen on");
                Console.WriteLine("    -a\t\t\t HTTP/API port to listen on");
                Console.WriteLine("    -i\t\t\t External IP Address to use");
                Console.WriteLine("    -w\t\t\t Specify location of the ixian.wal file");
                Console.WriteLine("    -n\t\t\t Specify which seed node to use");
                Console.WriteLine("    --worker\t\t Enables mining and disables masternode functionality");
                Console.WriteLine("    --threads\t\t Specify number of threads to use for mining (default 1)");
                Console.WriteLine("    --config\t\t Specify config filename (default ixian.cfg)");
                Console.WriteLine("    --maxLogSize\t Specify maximum log file size in MB");
                Console.WriteLine("    --maxLogCount\t Specify maximum number of log files");
                Console.WriteLine("    --lastGoodBlock\t Specify the last block height that should be read from storage");
                Console.WriteLine("    --disableWebStart\t Disable running http://localhost:8081 on startup");
                Console.WriteLine("    --disableMiner\t Disable miner");
                Console.WriteLine("    --fullStorageDataVerification\t Verify blocks and transactions fully even if read from local storage");
                Console.WriteLine("");
                Console.WriteLine("----------- Developer CLI flags -----------");
                Console.WriteLine("    --genesis\t\t Start node in genesis mode");
                Console.WriteLine("    --netdump\t\t Enable netdump for debugging purposes");
                Console.WriteLine("    --benchmarkKeys [key size]\t\t Perform a key-generation benchmark, then exit");
                Console.WriteLine("    --recover\t\t Recovers from file (to be used only by developers when cold-starting the network)");
                Console.WriteLine("    --forceTimeOffset\t Forces network time offset to a certain value");
                Console.WriteLine("");
                Console.WriteLine("----------- Config File Options -----------");
                Console.WriteLine(" Config file options should use parameterName = parameterValue semantics.");
                Console.WriteLine(" Each option should be specified in its own line. Example:");
                Console.WriteLine("    dltPort = 10234");
                Console.WriteLine("    apiPort = 8081");
                Console.WriteLine("");
                Console.WriteLine(" Available options:");
                Console.WriteLine("    dltPort\t\t Port to listen on (same as -p CLI)");
                Console.WriteLine("    testnetDltPort\t Port to listen on in testnet mode (same as -p CLI)");
                Console.WriteLine("    apiPort\t\t HTTP/API port to listen on (same as -a CLI)");
                Console.WriteLine("    testnetApiPort\t HTTP/API port to listen on in testnet mode (same as -a CLI)");
                Console.WriteLine("    addApiUser\t\t Adds user:password that can access the API (can be used multiple times)");
                Console.WriteLine("    externalIp\t\t External IP Address to use (same as -i CLI)");
                Console.WriteLine("    addPeer\t\t Specify which seed node to use (same as -n CLI) (can be used multiple times)");
                Console.WriteLine("    addTestnetPeer\t Specify which seed node to use in testnet mode (same as -n CLI) (can be used multiple times)");
                Console.WriteLine("    maxLogSize\t\t Specify maximum log file size in MB (same as --maxLogSize CLI)");
                Console.WriteLine("    maxLogCount\t\t Specify maximum number of log files (same as --maxLogCount CLI)");

                return "";
            }

            private static string outputVersion()
            {
                DLTNode.Program.noStart = true;

                // Do nothing since version is the first thing displayed

                return "";
            }

            private static void readConfigFile(string filename)
            {
                if (!File.Exists(filename))
                {
                    return;
                }
                Logging.info("Reading config file: " + filename);
                List<string> lines = File.ReadAllLines(filename).ToList();
                foreach(string line in lines)
                {
                    string[] option = line.Split('=');
                    if(option.Length < 2)
                    {
                        continue;
                    }
                    string key = option[0].Trim(new char[] { ' ', '\t', '\r', '\n' });
                    string value = option[1].Trim(new char[] { ' ', '\t', '\r', '\n' });

                    if (key.StartsWith(";"))
                    {
                        continue;
                    }

                    switch (key)
                    {
                        case "dltPort":
                            serverPort = int.Parse(value);
                            break;
                        case "testnetDltPort":
                            testnetServerPort = int.Parse(value);
                            break;
                        case "apiPort":
                            apiPort = int.Parse(value);
                            break;
                        case "testnetApiPort":
                            testnetApiPort = int.Parse(value);
                            break;
                        case "addApiUser":
                            string[] credential = value.Split(':');
                            if (credential.Length == 2)
                            {
                                apiUsers.Add(credential[0], credential[1]);
                            }
                            break;
                        case "externalIp":
                            publicServerIP = value;
                            break;
                        case "addPeer":
                            Network.CoreNetworkUtils.seedNodes.Add(value);
                            break;
                        case "addTestnetPeer":
                            Network.CoreNetworkUtils.seedTestNetNodes.Add(value);
                            break;
                        case "maxLogSize":
                            maxLogSize = int.Parse(value);
                            break;
                        case "maxLogCount":
                            maxLogCount = int.Parse(value);
                            break;
                        default:
                            // unknown key
                            break;
                    }
                }
            }

            public static void readFromCommandLine(string[] args)
            {
                // for now force set forceTimeOffset to 0 (unless overridden by CLI), since the time sync implementation/solution is buggy
                forceTimeOffset = 0;


                //Logging.log(LogSeverity.info, "Reading config...");

                // first pass
                var cmd_parser = new FluentCommandLineParser();


                // config file
                cmd_parser.Setup<string>("config").Callback(value => configFilename = value).Required();

                cmd_parser.Parse(args);

                readConfigFile(configFilename);



                // second pass
                cmd_parser = new FluentCommandLineParser();

                // testnet
                cmd_parser.Setup<bool>('t', "testnet").Callback(value => isTestNet = true).Required();

                cmd_parser.Parse(args);

                if (isTestNet)
                {
                    serverPort = testnetServerPort;
                    apiPort = testnetApiPort;
                    dataFolderPath = "data-testnet";
                    PeerStorage.peersFilename = "testnet-peers.dat";
                }



                string seedNode = "";

                // third pass
                cmd_parser = new FluentCommandLineParser();

                bool start_clean = false; // Flag to determine if node should delete cache+logs

                // help
                cmd_parser.SetupHelp("h", "help").Callback(text => outputHelp());

                // version
                cmd_parser.Setup<bool>('v', "version").Callback(text => outputVersion());

                // Toggle between full history node and no history
                cmd_parser.Setup<bool>('s', "save-history").Callback(value => storeFullHistory = value).Required();

                // Toggle worker-only mode
                cmd_parser.Setup<bool>("worker").Callback(value => workerOnly = true).Required();

                // Check for password change
                cmd_parser.Setup<bool>('x', "changepass").Callback(value => changePass = value).Required();

                // Check for recovery parameter
                cmd_parser.Setup<bool>("recover").Callback(value => recoverFromFile = value).Required();

                // Check for clean parameter
                cmd_parser.Setup<bool>('c', "clean").Callback(value => start_clean = value).Required();


                cmd_parser.Setup<int>('p', "port").Callback(value => serverPort = value).Required();

                cmd_parser.Setup<int>('a', "apiport").Callback(value => apiPort = value).Required();

                cmd_parser.Setup<string>('i', "ip").Callback(value => externalIp = value).SetDefault("");

                cmd_parser.Setup<string>("genesis").Callback(value => genesisFunds = value).Required();

                cmd_parser.Setup<string>("genesis2").Callback(value => genesis2Address = value).Required();

                cmd_parser.Setup<int>("threads").Callback(value => miningThreads = (uint)value).Required();

                cmd_parser.Setup<string>('w', "wallet").Callback(value => walletFile = value).Required();

                cmd_parser.Setup<string>('n', "node").Callback(value => seedNode = value).Required();

                cmd_parser.Setup<int>("maxLogSize").Callback(value => maxLogSize = value).Required();

                cmd_parser.Setup<int>("maxLogCount").Callback(value => maxLogCount = value).Required();

                cmd_parser.Setup<long>("lastGoodBlock").Callback(value => lastGoodBlock = (ulong)value).Required();

                cmd_parser.Setup<bool>("disableWebStart").Callback(value => disableWebStart = true).Required();

                cmd_parser.Setup<string>("dataFolderPath").Callback(value => dataFolderPath = value).Required();

                cmd_parser.Setup<bool>("disableMiner").Callback(value => disableMiner = true).Required();

                cmd_parser.Setup<bool>("fullStorageDataVerification").Callback(value => fullStorageDataVerification = true).Required();
                

                // Debug
                cmd_parser.Setup<string>("netdump").Callback(value => networkDumpFile = value).SetDefault("");

                cmd_parser.Setup<int>("forceTimeOffset").Callback(value => forceTimeOffset = value).Required();

                cmd_parser.Setup<int>("benchmarkKeys").Callback(value => benchmarkKeys = value).SetDefault(0);

                cmd_parser.Parse(args);


                // Validate parameters

                if (start_clean)
                {
                    Node.cleanCacheAndLogs();
                }

                if (miningThreads < 1)
                    miningThreads = 1;

                if (seedNode != "")
                {
                    if (isTestNet)
                    {
                        Network.CoreNetworkUtils.seedTestNetNodes = new List<string>
                        {
                            seedNode
                        };
                    }
                    else
                    {
                        Network.CoreNetworkUtils.seedNodes = new List<string>
                        {
                            seedNode
                        };
                    }
                }
            }
        }
    }
}