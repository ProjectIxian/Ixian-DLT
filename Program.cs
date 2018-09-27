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

namespace DLTNode
{
    class Program
    {
        private static System.Timers.Timer mainLoopTimer;
        private static APIServer apiServer;

        static void Main(string[] args)
        {
            // Clear the console first
            Console.Clear();

            Logging.info(string.Format("IXIAN DLT Node {0} started", Config.version));
            // For testing only. Run any experiments here as to not affect the infrastructure.
            // Failure of tests will result in termination of the dlt instance.
            /*if(runTests(args) == false)
            {
                return;
            }*/
            
            onStart(args);

            // For testing purposes, wait for the Escape key to be pressed before stopping execution
            // In production this will be changed, as the dlt will run in the background
            /*     while (Console.ReadKey().Key != ConsoleKey.Escape)
                 {

                 }
                 */

            Console.CancelKeyPress += delegate (object sender, ConsoleCancelEventArgs e) {
                e.Cancel = true;
                apiServer.forceShutdown = true;
            };

            while (apiServer.forceShutdown == false)
            {
                Thread.Sleep(1000);
            }

            onStop();

            Logging.log(LogSeverity.info, "DLT Node stopped");
        }

        static void onStart(string[] args)
        {
            // Read configuration from command line
            Config.readFromCommandLine(args);

            // Initialize the crypto manager
            CryptoManager.initLib();

            // Start the actual DLT node
            Node.start();

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
                        Node.walletState.numWallets, Node.walletState.numSnapshots, ws_checksum));
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
                    string ws_checksum = Node.walletState.calculateWalletStateChecksum();
                    Logging.info(String.Format("WalletState checksum: ({0} wallets, {1} snapshots) : {2}",
                        Node.walletState.numWallets, Node.walletState.hasSnapshot, ws_checksum));
                }

            }
            if (Node.update() == false)
            {
                apiServer.forceShutdown = true;
            }
        }

        static void onStop()
        {
            mainLoopTimer.Stop();

            // Stop the API server
            apiServer.stop();

            // Stop the DLT
            Node.stop();

            Console.WriteLine("\n");
        }


        static bool runTests(string[] args)
        {
            Logging.log(LogSeverity.info, "Running Tests:");

            // Create a crypto lib
            CryptoLib crypto_lib = new CryptoLib(new CryptoLibs.BouncyCastle());
            crypto_lib.generateKeys();

            Logging.log(LogSeverity.info, String.Format("Public Key base64: {0}", crypto_lib.getPublicKey()));
            Logging.log(LogSeverity.info, String.Format("Private Key base64: {0}", crypto_lib.getPrivateKey()));


            /// ECDSA Signature test
            // Generate a new signature
            string signature = crypto_lib.getSignature("Hello There!", crypto_lib.getPrivateKey());
            Logging.log(LogSeverity.info, String.Format("Signature: {0}", signature));

            // Verify the signature
            if(crypto_lib.verifySignature("Hello There!", crypto_lib.getPublicKey(), signature))
            {
                Logging.log(LogSeverity.info, "SIGNATURE IS VALID");
            }

            // Try a tamper test
            if (crypto_lib.verifySignature("Hello Tamper!", crypto_lib.getPublicKey(), signature))
            {
                Logging.log(LogSeverity.info, "SIGNATURE IS VALID AND MATCHES ORIGINAL TEXT");
            }
            else
            {
                Logging.log(LogSeverity.info, "TAMPERED SIGNATURE OR TEXT");
            }

            // Generate a new signature for the same text
            string signature2 = crypto_lib.getSignature("Hello There!", crypto_lib.getPrivateKey());
            Logging.log(LogSeverity.info, String.Format("Signature Again: {0}", signature2));

            // Verify the signature again
            if (crypto_lib.verifySignature("Hello There!", crypto_lib.getPublicKey(), signature2))
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
