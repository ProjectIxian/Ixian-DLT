using Fclp;
using IXICore;
using IXICore.Meta;
using IXICore.Network;
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

            private static int defaultServerPort = 10234;
            private static int defaultTestnetServerPort = 11234;

            public static int apiPort = 8081;
            public static int testnetApiPort = 8181;

            public static Dictionary<string, string> apiUsers = new Dictionary<string, string>();

            public static List<string> apiAllowedIps = new List<string>();
            public static List<string> apiBinds = new List<string>();


            public static bool storeFullHistory = true; // Flag confirming this is a full history node
            public static bool recoverFromFile = false; // Flag allowing recovery from file
            public static bool disableMiner = false; // Flag to disable miner
            public static bool workerOnly = false; // Flag to disable masternode capability

            public static string genesisFunds = "0"; // If 0, it'll use a hardcoded wallet address
            public static string genesis2Address = ""; // For a secondary genesis node
           
            public static uint miningThreads = 1;

            public static string dataFolderPath = "data";
            public static string blockStorageProvider = "SQLite";
            public static string dataFolderBlocks
            {
                get
                {
                    return dataFolderPath + Path.DirectorySeparatorChar + "blocks";
                }
            }
            public static string configFilename = "ixian.cfg";
            public static string walletFile = "ixian.wal";
            public static string genesisFile = "genesis.dat";

            public static int maxLogSize = 50; // MB
            public static int maxLogCount = 10;

            public static ulong lastGoodBlock = 0;
            public static bool disableWebStart = false;

            public static bool fullStorageDataVerification = false;

            public static bool onlyShowAddresses = false;

            public static string externalIp = "";

            // Read-only values
            public static readonly string version = "xdc-0.6.6a"; // DLT Node version
            public static readonly int checkVersionSeconds = 6 * 60 * 60; // 6 hours

            public static readonly ulong maxBlocksPerDatabase = 1000; // number of blocks to store in a single database file

            public static readonly ulong nodeDeprecationBlock = 800000 + (ulong)(new Random()).Next(200); // block height on which this version of Ixian DLT stops working on

            public static readonly ulong saveWalletStateEveryBlock = 1000; // Saves wallet state every 1000 blocks

            // Debugging values
            public static string networkDumpFile = "";
            public static int benchmarkKeys = 0;

            // Development/testing options
            public static bool generateWalletOnly = false;
            public static string dangerCommandlinePasswordCleartextUnsafe = "";

            // internal
            public static bool changePass = false;

            public static int maxBlockVersionToGenerate = 5;

            /// <summary>
            /// Command to execute when a new block is accepted.
            /// </summary>
            public static string blockNotifyCommand = "";


            public static bool noNetworkSync = false;


            private Config()
            {

            }

            private static string outputHelp()
            {
                DLTNode.Program.noStart = true;

                Console.WriteLine("Starts a new instance of Ixian DLT Node");
                Console.WriteLine("");
                Console.WriteLine(" IxianDLT.exe [-h] [-v] [-t] [-s] [-x] [-c] [-p 10234] [-a 8081] [-i ip] [-w ixian.wal] [-n seed1.ixian.io:10234]");
                Console.WriteLine(" [--worker] [--threads 1] [--config ixian.cfg] [--maxLogSize 50] [--maxLogCount 10] [--lastGoodBlock 110234]");
                Console.WriteLine(" [--disableWebStart] [--disableMiner] [--onlyShowAddresses] [--genesis] [--netdump dumpfile] [--benchmarkKeys key_size]");
                Console.WriteLine(" [--recover] [--forceTimeOffset 0] [--verifyStorage] [--generateWallet] [--walletPassword]");
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
                Console.WriteLine("    --onlyShowAddresses\t Shows address list and exits");
                Console.WriteLine("");
                Console.WriteLine("----------- Developer CLI flags -----------");
                Console.WriteLine("    --genesis\t\t Start node in genesis mode (to be used only when setting up your own private network)");
                Console.WriteLine("    --netdump\t\t Enable netdump for debugging purposes");
                Console.WriteLine("    --benchmarkKeys\t Perform a key-generation benchmark, then exit");
                Console.WriteLine("    --recover\t\t Recovers from file (to be used only by developers when cold-starting the network)");
                Console.WriteLine("    --forceTimeOffset\t Forces network time offset to a certain value");
				Console.WriteLine("    --verifyStorage\t Full local storage blocks and transactions verification");
                Console.WriteLine("    --generateWallet\t Generates a wallet file and exits, printing the public address. [TESTNET ONLY!]");
                Console.WriteLine("    --walletPassword\t Specify the password for the wallet. [TESTNET ONLY!]");
                Console.WriteLine("");
                Console.WriteLine("----------- Config File Options -----------");
                Console.WriteLine(" Config file options should use parameterName = parameterValue syntax.");
                Console.WriteLine(" Each option should be specified in its own line. Example:");
                Console.WriteLine("    dltPort = 10234");
                Console.WriteLine("    apiPort = 8081");
                Console.WriteLine("");
                Console.WriteLine(" Available options:");
                Console.WriteLine("    dltPort\t\t Port to listen on (same as -p CLI)");
                Console.WriteLine("    testnetDltPort\t Port to listen on in testnet mode (same as -p CLI)");

                Console.WriteLine("    apiPort\t\t HTTP/API port to listen on (same as -a CLI)");
                Console.WriteLine("    apiAllowIp\t\t Allow API connections from specified source or sources (can be used multiple times)");
                Console.WriteLine("    apiBind\t\t Bind to given address to listen for API connections (can be used multiple times)");
                Console.WriteLine("    testnetApiPort\t HTTP/API port to listen on in testnet mode (same as -a CLI)");
                Console.WriteLine("    addApiUser\t\t Adds user:password that can access the API (can be used multiple times)");

                Console.WriteLine("    externalIp\t\t External IP Address to use (same as -i CLI)");
                Console.WriteLine("    addPeer\t\t Specify which seed node to use (same as -n CLI) (can be used multiple times)");
                Console.WriteLine("    addTestnetPeer\t Specify which seed node to use in testnet mode (same as -n CLI) (can be used multiple times)");
                Console.WriteLine("    maxLogSize\t\t Specify maximum log file size in MB (same as --maxLogSize CLI)");
                Console.WriteLine("    maxLogCount\t\t Specify maximum number of log files (same as --maxLogCount CLI)");
                Console.WriteLine("    disableMiner\t 1 to disable the miner, 0 to enable (same as --disableMiner CLI)");
                Console.WriteLine("    disableWebStart\t 1 to disable running http://localhost:8081 on startup (same as --disableWebStart CLI)");
                Console.WriteLine("    forceTimeOffset\t Forces network time offset to the specified value (same as --forceTimeOffset CLI)");
                Console.WriteLine("    walletNotify\t Execute command when a wallet transaction changes");
                Console.WriteLine("    blockNotify\t\t Execute command when the block changes");

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
                    Logging.info("Processing config parameter '" + key + "' = '" + value + "'");
                    switch (key)
                    {
                        case "dltPort":
                            Config.defaultServerPort = int.Parse(value);
                            break;
                        case "testnetDltPort":
                            Config.defaultTestnetServerPort = int.Parse(value);
                            break;
                        case "apiPort":
                            apiPort = int.Parse(value);
                            break;
                        case "apiAllowIp":
                            apiAllowedIps.Add(value);
                            break;
                        case "apiBind":
                            apiBinds.Add(value);
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
                            externalIp = value;
                            break;
                        case "addPeer":
                            CoreNetworkUtils.seedNodes.Add(new string[2] { value, null });
                            break;
                        case "addTestnetPeer":
                            CoreNetworkUtils.seedTestNetNodes.Add(new string[2] { value, null });
                            break;
                        case "maxLogSize":
                            maxLogSize = int.Parse(value);
                            break;
                        case "maxLogCount":
                            maxLogCount = int.Parse(value);
                            break;
                        case "disableMiner":
                            if (int.Parse(value) != 0)
                            {
                                disableMiner = true;
                            }
                            break;
                        case "disableWebStart":
                            if (int.Parse(value) != 0)
                            {
                                disableWebStart = true;
                            }
                            break;
                        case "forceTimeOffset":
                            CoreConfig.forceTimeOffset = int.Parse(value);
                            break;
                        case "blockStorage":
                            blockStorageProvider = value;
                            break;
                        case "walletNotify":
                            CoreConfig.walletNotifyCommand = value;
                            break;
                        case "blockNotify":
                            Config.blockNotifyCommand = value;
                            break;
                        default:
                            // unknown key
                            Logging.warn("Unknown config parameter was specified '" + key + "'");
                            break;
                    }
                }
            }

            public static void readFromCommandLine(string[] args)
            {
                // first pass
                var cmd_parser = new FluentCommandLineParser();

                // help
                cmd_parser.SetupHelp("h", "help").Callback(text => outputHelp());

                // config file
                cmd_parser.Setup<string>("config").Callback(value => configFilename = value).Required();

                cmd_parser.Parse(args);

                if(DLTNode.Program.noStart)
                {
                    return;
                }

                readConfigFile(configFilename);



                // second pass
                cmd_parser = new FluentCommandLineParser();

                // testnet
                cmd_parser.Setup<bool>('t', "testnet").Callback(value => CoreConfig.isTestNet = true).Required();

                cmd_parser.Parse(args);

                if (CoreConfig.isTestNet)
                {
                    Config.serverPort = defaultTestnetServerPort;
                    apiPort = testnetApiPort;
                    dataFolderPath = "data-testnet";
                    PeerStorage.init("testnet-peers.dat");
                    genesisFile = "testnet-genesis.dat";
                }else
                {
                    Config.serverPort = defaultServerPort;
                }



                string seedNode = "";

                // third pass
                cmd_parser = new FluentCommandLineParser();

                int start_clean = 0; // Flag to determine if node should delete cache+logs

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
                cmd_parser.Setup<bool>('c', "clean").Callback(value => start_clean = 1);
                cmd_parser.Setup<bool>('f', "force").Callback(value => { if (start_clean > 0) { start_clean += 1; } });


                cmd_parser.Setup<int>('p', "port").Callback(value => Config.serverPort = value).Required();

                cmd_parser.Setup<int>('a', "apiport").Callback(value => apiPort = value).Required();

                cmd_parser.Setup<string>('i', "ip").Callback(value => externalIp = value).Required();

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

                cmd_parser.Setup<bool>("verifyStorage").Callback(value => fullStorageDataVerification = true).Required();

                cmd_parser.Setup<bool>("onlyShowAddresses").Callback(value => onlyShowAddresses = true).Required();


                // Debug
                cmd_parser.Setup<string>("netdump").Callback(value => networkDumpFile = value).SetDefault("");

                cmd_parser.Setup<int>("forceTimeOffset").Callback(value => CoreConfig.forceTimeOffset = value).Required();

                cmd_parser.Setup<int>("benchmarkKeys").Callback(value => benchmarkKeys = value).SetDefault(0);

                cmd_parser.Setup<bool>("generateWallet").Callback(value => generateWalletOnly = value).SetDefault(false);

                cmd_parser.Setup<string>("walletPassword").Callback(value => dangerCommandlinePasswordCleartextUnsafe = value).SetDefault("");

                cmd_parser.Setup<bool>("noNetworkSync").Callback(value => noNetworkSync = true).Required();

                cmd_parser.Parse(args);


                Storage.path = dataFolderPath + Path.DirectorySeparatorChar + "blocks";
                WalletStateStorage.path = dataFolderPath + Path.DirectorySeparatorChar + "ws";

                if (start_clean > 0)
                {
                    bool do_clean = false;
                    if (start_clean > 1)
                    {
                        Logging.warn("Ixian DLT node started with the forced clean parameter (-c -c).");
                        do_clean = true;
                    }
                    else
                    {
                        Console.ForegroundColor = ConsoleColor.Magenta;
                        Console.WriteLine("You have started the Ixian DLT node with the '-c' parameter, indicating that you wish to clear all cache and log files.");
                        Console.Write("This will cause the node to re-download any neccessary data, which may take some time. Are you sure? (Y/N)");
                        Console.ResetColor();
                        var k = Console.ReadKey();
                        if (k.Key == ConsoleKey.Y)
                        {
                            do_clean = true;
                        }
                    }
                    if(do_clean)
                    {
                        Node.checkDataFolder();
                        Node.cleanCacheAndLogs();
                    } else
                    {
                        DLTNode.Program.noStart = true;
                        return;
                    }
                }

                if (miningThreads < 1)
                    miningThreads = 1;

                if (seedNode != "")
                {
                    if (CoreConfig.isTestNet)
                    {
                        CoreNetworkUtils.seedTestNetNodes = new List<string[]>
                        {
                            new string[2] { seedNode, null }
                        };
                    }
                    else
                    {
                        CoreNetworkUtils.seedNodes = new List<string[]>
                        {
                            new string[2] { seedNode, null }
                        };
                    }
                }
            }
        }
    }
}