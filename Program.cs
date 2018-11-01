using System;
using System.Collections.Generic;
using System.Linq;
using System.Timers;
using DLT;
using DLT.Meta;
using Fclp;
using System.Text;
using DLT.Network;
using System.Threading;
using Newtonsoft.Json;
using System.Numerics;
using System.Diagnostics;
using System.IO;
using IXICore;

namespace DLTNode
{
    class Program
    {

        static void CheckRequiredFiles()
        {
            string[] critical_dlls =
            {
                "Argon2_C.dll",
                "BouncyCastle.Crypto.dll",
                "FluentCommandLineParser.dll",
                "Newtonsoft.Json.dll",
                "netstandard.dll",
                "Open.Nat.dll",
                "SQLite-net.dll",
                "SQLitePCLRaw.batteries_green.dll",
                "SQLitePCLRaw.batteries_v2.dll",
                "SQLitePCLRaw.core.dll",
                "SQLitePCLRaw.provider.e_sqlite3.dll",
                "System.Console.dll",
                "System.Reflection.TypeExtensions.dll"
            };
            foreach(string critical_dll in critical_dlls)
            {
                if(!File.Exists(critical_dll))
                {
                    Logging.error(String.Format("Missing '{0}' in the program folder. Possibly the IXIAN archive was corrupted or incorrectly installed. Please re-download from http://www.ixian.io!", critical_dll));
                    Logging.info("Press ENTER to exit.");
                    Console.ReadLine();
                    Environment.Exit(-1);
                }
            }
        }
        static void CheckVCRedist()
        {
            object installed_vc_redist = Microsoft.Win32.Registry.GetValue("HKEY_LOCAL_MACHINE\\SOFTWARE\\Microsoft\\VisualStudio\\14.0\\VC\\Runtimes\\x64", "Installed", 0);
            bool success = false;
            if (installed_vc_redist is int && (int)installed_vc_redist > 0)
            {
                Logging.info("Visual C++ 2017 (v141) redistributable is already installed.");
                success = true;
            }
            else
            {
                if (!File.Exists("vc_redist.x64.exe"))
                {
                    Logging.warn("The VC++2017 redistributable file is not found. Please download the v141 version of the Visual C++ 2017 redistributable and install it manually!");
                }
                else
                {
                    Console.ForegroundColor = ConsoleColor.Yellow;
                    Console.WriteLine();
                    Console.WriteLine();
                    Console.WriteLine();
                    Console.WriteLine("NOTICE: In order to run this IXIAN node, Visual Studio 2017 Redistributable (v141) must be installed. This can be done automatically by IXIAN,");
                    Console.Write("or, you can install it manually from this URL:");
                    Console.ForegroundColor = ConsoleColor.Green;
                    Console.WriteLine("https://visualstudio.microsoft.com/downloads/");
                    Console.ForegroundColor = ConsoleColor.Magenta;
                    Console.WriteLine("The installer may open a UAC (User Account Control) prompt. Please verify that the executable is signed by Microsoft Corporation before allowing it to install!");
                    Console.ResetColor();
                    Console.WriteLine();
                    Console.WriteLine("Automatically install Visual C++ 2017 redistributable? (Y/N): ");
                    ConsoleKeyInfo k = Console.ReadKey();
                    Console.WriteLine();
                    Console.WriteLine();
                    if (k.Key == ConsoleKey.Y)
                    {
                        Logging.info("Installing Visual C++ 2017 (v141) redistributable...");
                        ProcessStartInfo installer = new ProcessStartInfo("vc_redist.x64.exe");
                        installer.Arguments = "/install /passive /norestart";
                        installer.LoadUserProfile = false;
                        installer.RedirectStandardError = true;
                        installer.RedirectStandardInput = true;
                        installer.RedirectStandardOutput = true;
                        installer.UseShellExecute = false;
                        Logging.info("Starting installer. Please allow up to one minute for installation...");
                        Process p = Process.Start(installer);
                        while (!p.HasExited)
                        {
                            if (!p.WaitForExit(60000))
                            {
                                Logging.info("The install process seems to be stuck. Terminate? (Y/N): ");
                                k = Console.ReadKey();
                                if (k.Key == ConsoleKey.Y)
                                {
                                    Logging.warn("Terminating installer process...");
                                    p.Kill();
                                    Logging.warn(String.Format("Process output: {0}", p.StandardOutput.ReadToEnd()));
                                    Logging.warn(String.Format("Process error output: {0}", p.StandardError.ReadToEnd()));
                                }
                            }
                        }
                        installed_vc_redist = Microsoft.Win32.Registry.GetValue("HKEY_LOCAL_MACHINE\\SOFTWARE\\Microsoft\\VisualStudio\\14.0\\VC\\Runtimes\\x64", "Installed", 0);
                        if (installed_vc_redist is int && (int)installed_vc_redist > 0)
                        {
                            Logging.info("Visual C++ 2017 (v141) redistributable has installed successfully.");
                            success = true;
                        }
                        else
                        {
                            Logging.info("Visual C++ 2017 has failed to install. Please review the error text (if any) and install manually:");
                            Logging.warn(String.Format("Process exit code: {0}.", p.ExitCode));
                            Logging.warn(String.Format("Process output: {0}", p.StandardOutput.ReadToEnd()));
                            Logging.warn(String.Format("Process error output: {0}", p.StandardError.ReadToEnd()));
                        }
                    }
                }
            }
            if (!success)
            {
                Logging.info("IXIAN requires the Visual Studio 2017 runtime for normal operation. Please ensure it is installed and then restart the program!");
                Logging.info("Press ENTER to exit.");
                Console.ReadLine();
                Environment.Exit(-1);
            }
        }

        private static System.Timers.Timer mainLoopTimer;
        private static APIServer apiServer;

        public static bool noStart = false;

        static void Main(string[] args)
        {
            // Clear the console first
            Console.Clear();

            // Start logging
            Logging.start();

            // For testing only. Run any experiments here as to not affect the infrastructure.
            // Failure of tests will result in termination of the dlt instance.
            /*if(runTests(args) == false)
            {
                return;
            }*/
            
            onStart(args);

            Console.CancelKeyPress += delegate (object sender, ConsoleCancelEventArgs e) {
                e.Cancel = true;
                apiServer.forceShutdown = true;
            };

            if(apiServer != null)
            { 
                while (apiServer.forceShutdown == false)
                {
                    Thread.Sleep(1000);
                }
            }
            onStop();

        }

        static void onStart(string[] args)
        {
            Logging.info(string.Format("IXIAN DLT Node {0}", Config.version));

            // Read configuration from command line
            Config.readFromCommandLine(args);

            if (noStart)
            {
                Thread.Sleep(1000);
                return;
            }

            // Check for critical files in the exe dir
            CheckRequiredFiles();

            // Check for the right vc++ redist for the argon miner
            CheckVCRedist();

            // Log the parameters to notice any changes
            Logging.info(String.Format("Mainnet: {0}", !Config.isTestNet));
            Logging.info(String.Format("Miner: {0}", !Config.disableMiner));
            Logging.info(String.Format("Server Port: {0}", Config.serverPort));
            Logging.info(String.Format("API Port: {0}", Config.apiPort));
            Logging.info(String.Format("Wallet File: {0}", Config.walletFile));

            // Initialize the crypto manager
            CryptoManager.initLib();

            // Start the actual DLT node
            Node.start();

            if (noStart)
            {
                Thread.Sleep(1000);
                return;
            }

            // Start the HTTP JSON API server
            apiServer = new APIServer();

            // Setup a timer to handle routine updates
            mainLoopTimer = new System.Timers.Timer(500);
            mainLoopTimer.Elapsed += new ElapsedEventHandler(onUpdate);
            mainLoopTimer.Start();
            // DEBUG: manual update
            /*while(Node.update())
            {
                Console.WriteLine(" -> PRESS ENTER TO UPDATE (B) for next block<- ");
                ConsoleKeyInfo key = Console.ReadKey();
                if(key.Key == ConsoleKey.B)
                {
                    Node.forceNextBlock = true;
                }
                if(key.Key == ConsoleKey.W)
                {
                    string ws_checksum = Node.walletState.calculateWalletStateChecksum();
                    Console.WriteLine(String.Format("WalletState checksum: ({0} wallets, {1} snapshots) : {2}",
                        Node.walletState.numWallets, Node.walletState.hasSnapshot, ws_checksum));
                }
            }/**/

            Console.WriteLine("-----------\nPress Ctrl-C or use the /shutdown API to stop the DLT process at any time.\n");
        }

        static void onUpdate(object source, ElapsedEventArgs e)
        {
            if (Console.KeyAvailable)
            {
                ConsoleKeyInfo key = Console.ReadKey();
                /*if(key.Key == ConsoleKey.B)
                {
                    Node.forceNextBlock = true;
                }*/
                if (key.Key == ConsoleKey.W)
                {
                    string ws_checksum = Crypto.hashToString(Node.walletState.calculateWalletStateChecksum());
                    Logging.info(String.Format("WalletState checksum: ({0} wallets, {1} snapshots) : {2}",
                        Node.walletState.numWallets, Node.walletState.hasSnapshot, ws_checksum));
                }
                if(key.Key == ConsoleKey.H)
                {
                    ulong[] temp = new ulong[ProtocolMessage.recvByteHist.Length];
                    lock (ProtocolMessage.recvByteHist)
                    {
                        ProtocolMessage.recvByteHist.CopyTo(temp, 0);
                    }
                    Console.ForegroundColor = ConsoleColor.Green;
                    Console.WriteLine("==================RECEIVED BYTES HISTOGRAM:===================");
                    for (int i = 0; i < temp.Length; i++)
                    {
                        Console.WriteLine(String.Format("[{0}]: {1}", i, temp[i]));
                    }
                    Console.WriteLine("==================RECEIVED BYTES HISTOGRAM:===================");
                    Console.ResetColor();
                }

            }
            if (Node.update() == false)
            {
                apiServer.forceShutdown = true;
            }
        }

        static void onStop()
        {
            if (mainLoopTimer != null)
            {
                mainLoopTimer.Stop();
            }

            // Stop the API server
            if (apiServer != null)
            {
                apiServer.stop();
            }

            if (noStart == false)
            {
                // Stop the DLT
                Node.stop();
            }

            // Stop logging
            while(Logging.getRemainingStatementsCount() > 0)
            {
                Thread.Sleep(100);
            }
            Logging.stop();

            if (noStart == false)
            {
                Console.WriteLine("");
                Console.WriteLine("Ixian DLT Node stopped.");
            }
        }


        static bool runTests(string[] args)
        {
            Logging.log(LogSeverity.info, "Running Tests:");

            // Create a crypto lib
            CryptoLib crypto_lib = new CryptoLib(new CryptoLibs.BouncyCastle());
            crypto_lib.generateKeys(CoreConfig.defaultRsaKeySize);

            Logging.log(LogSeverity.info, String.Format("Public Key base64: {0}", crypto_lib.getPublicKey()));
            Logging.log(LogSeverity.info, String.Format("Private Key base64: {0}", crypto_lib.getPrivateKey()));


            /// ECDSA Signature test
            // Generate a new signature
            byte[] signature = crypto_lib.getSignature(Encoding.UTF8.GetBytes("Hello There!"), crypto_lib.getPrivateKey());
            Logging.log(LogSeverity.info, String.Format("Signature: {0}", signature));

            // Verify the signature
            if(crypto_lib.verifySignature(Encoding.UTF8.GetBytes("Hello There!"), crypto_lib.getPublicKey(), signature))
            {
                Logging.log(LogSeverity.info, "SIGNATURE IS VALID");
            }

            // Try a tamper test
            if (crypto_lib.verifySignature(Encoding.UTF8.GetBytes("Hello Tamper!"), crypto_lib.getPublicKey(), signature))
            {
                Logging.log(LogSeverity.info, "SIGNATURE IS VALID AND MATCHES ORIGINAL TEXT");
            }
            else
            {
                Logging.log(LogSeverity.info, "TAMPERED SIGNATURE OR TEXT");
            }

            // Generate a new signature for the same text
            byte[] signature2 = crypto_lib.getSignature(Encoding.UTF8.GetBytes("Hello There!"), crypto_lib.getPrivateKey());
            Logging.log(LogSeverity.info, String.Format("Signature Again: {0}", signature2));

            // Verify the signature again
            if (crypto_lib.verifySignature(Encoding.UTF8.GetBytes("Hello There!"), crypto_lib.getPublicKey(), signature2))
            {
                Logging.log(LogSeverity.info, "SIGNATURE IS VALID");
            }



            Logging.log(LogSeverity.info, "-------------------------");

            // Generate a mnemonic hash from a 64 character string. If the result is always the same, it works correctly.
            Mnemonic mnemonic_addr = new Mnemonic(Wordlist.English, Encoding.ASCII.GetBytes("hahahahahahahahahahahahahahahahahahahahahahahahahahahahahahahaha"));
            Logging.log(LogSeverity.info, String.Format("Mnemonic Hashing Test: {0}", mnemonic_addr));
            Logging.log(LogSeverity.info, "-------------------------");


            // Create an address from the public key
            Address addr = new Address(crypto_lib.getPublicKey());
            Logging.log(LogSeverity.info, String.Format("Address generated from public key above: {0}", addr));
            Logging.log(LogSeverity.info, "-------------------------");


            // Testing sqlite wrapper
            var db = new SQLite.SQLiteConnection("storage.dat");

            // Testing internal data structures
            db.CreateTable<Block>();

            Block new_block = new Block();
            db.Insert(new_block);

            IEnumerable<Block> block_list = db.Query<Block>("select * from Block");

            if (block_list.OfType<Block>().Count() > 0)
            {
                Block first_block = block_list.FirstOrDefault();
                Logging.log(LogSeverity.info, String.Format("Stored genesis block num is: {0}", first_block.blockNum));
            }


            Logging.log(LogSeverity.info, "Tests completed successfully.\n\n");

            return true;
        }
    }
}
