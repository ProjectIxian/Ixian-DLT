using DLT.Network;
using IXICore.Utils;
using System;
using System.Linq;
using System.Threading;

namespace DLT.Meta
{
    public class StatsConsoleScreen
    {
        private DateTime startTime;

        private Thread thread = null;
        private bool running = false;

        private int consoleWidth = 55;
        private uint drawCycle = 0; // Keep a count of screen draw cycles as a basic method of preventing visual artifacts
        private ThreadLiveCheck TLC;

        public StatsConsoleScreen()
        {          
            Console.Clear();

            Console.CursorVisible = ConsoleHelpers.verboseConsoleOutput;

            // Start thread
            TLC = new ThreadLiveCheck();
            running = true;
            thread = new Thread(new ThreadStart(threadLoop));
            thread.Name = "Stats_Console_Thread";
            thread.Start();

            startTime = DateTime.UtcNow;
        }

        // Shutdown console thread
        public void stop()
        {
            running = false;
        }

        private void threadLoop()
        {
            while (running)
            {
                TLC.Report();
                if (ConsoleHelpers.verboseConsoleOutput == false)
                {
                    // Clear the screen every 10 seconds to prevent any persisting visual artifacts
                    if (drawCycle > 5)
                    {
                        clearScreen();
                        drawCycle = 0;
                    }
                    else
                    {
                        drawScreen();
                        drawCycle++;
                    }
                }

                Thread.Sleep(2000);
            }
        }

        public void clearScreen()
        {
            //Console.BackgroundColor = ConsoleColor.DarkGreen;
            Console.Clear();
            drawScreen();
        }

        public void drawScreen()
        {
            if (Storage.upgrading)
                Console.Clear();

            Console.SetCursorPosition(0, 0);

            string server_version = checkForUpdate();
            bool update_avail = false;
            if(!server_version.StartsWith("("))
            {
                if(server_version != Config.version)
                {
                    update_avail = true;
                } 
            }

            writeLine(" d888888b   db    db   d888888b    .d88b.    d8b   db ");
            writeLine("   `88'     `8b  d8'     `88'     d8'  `8b   888o  88 ");
            writeLine("    88       `8bd8'       88      88oooo88   88V8o 88 ");
            writeLine("    88       .dPYb.       88      88~~~~88   88 V8o88 ");
            writeLine("   .88.     .8P  Y8.     .88.     88    88   88  V888 ");
            writeLine(" Y888888P   YP    YP   Y888888P   YP    YP   VP   V8P ");
            writeLine(" {0}", (Config.version + " BETA ").PadLeft(53));
            writeLine(" {0}", ("http://localhost:" + Config.apiPort + "/"));
            writeLine("──────────────────────────────────────────────────────");
            if(update_avail)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                writeLine(" An update (" + server_version + ") of Ixian DLT is available");
                writeLine(" Please visit https://www.ixian.io");
                Console.ResetColor();
            }
            else
            {
                writeLine(" Thank you for running an Ixian DLT node.");
                writeLine(" For help please visit https://www.ixian.io");
            }
            writeLine("──────────────────────────────────────────────────────");

            if (Storage.upgrading)
            {
                writeLine(" Upgrading database: " + Storage.upgradeProgress + "/" + Storage.upgradeMaxBlockNum);
            }

            if (Node.serverStarted == false)
            {
                return;
            }

            // Node status
            Console.Write(" Status:               ");

            string dltStatus =  "active";
            if (Node.blockSync.synchronizing)
                dltStatus =     "synchronizing";


            int connectionsIn = 0;
            int connectionsOut = NetworkClientManager.getConnectedClients().Count();

            string connectionsInStr = "-";  // Default to no inbound connections accepted
            if (NetworkServer.isRunning())
            {
                // If the server is running, show the number of inbound connections
                connectionsIn = NetworkServer.getConnectedClients().Count();
                if (!NetworkServer.isConnectable() && connectionsOut == 0)
                {
                    connectionsInStr = "Not connectable";
                }
                else
                {
                    connectionsInStr = String.Format("{0}", connectionsIn);
                }
            }

            if (connectionsIn + connectionsOut < 1)
                dltStatus =     "connecting   ";

            if (Node.blockChain.getTimeSinceLastBLock() > 1800) // if no block for over 1800 seconds
            {
                Console.ForegroundColor = ConsoleColor.Red;
                dltStatus = "No fully signed block received for over 30 minutes";
            }

            if (Node.blockProcessor.networkUpgraded)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                dltStatus = "Network has been upgraded, please download a newer version of Ixian DLT";
            }



            writeLine(dltStatus);
            Console.ResetColor();

            writeLine("");
            ulong lastBlockNum = 0;
            string lastBlockChecksum = "";
            int sigCount = 0;
            Block b = Node.blockChain.getLastBlock();
            if(b != null)
            {
                lastBlockNum = b.blockNum;
                sigCount = b.signatures.Count();
                lastBlockChecksum = Crypto.hashToString(b.blockChecksum).Substring(0, 6);
            }

            writeLine(" Last Block:           {0} ({1} sigs) - {2}...", lastBlockNum, sigCount, lastBlockChecksum);

            writeLine(" Connections (I/O):    {0}", connectionsInStr + "/" + connectionsOut);
            writeLine(" Presences:            {0}", PresenceList.getTotalPresences());
            writeLine(" Transaction Pool:     {0}", TransactionPool.getUnappliedTransactions().Count());

            // Mining status
            string mineStatus = "disabled";
            if (!Config.disableMiner)
                mineStatus =    "stopped";
            if (Node.miner.lastHashRate > 0)
                mineStatus =    "active ";
            if (Node.miner.pause)
                mineStatus =    "paused ";

            writeLine("");
            writeLine(" Mining:               {0}", mineStatus);
            writeLine(" Hashrate:             {0}", Node.miner.lastHashRate);
            writeLine(" Search Mode:          {0}", Node.miner.searchMode);
            writeLine(" Solved Blocks:        {0}", Node.miner.getSolvedBlocksCount());
            writeLine("──────────────────────────────────────────────────────");

            TimeSpan elapsed = DateTime.UtcNow - startTime;

            writeLine(" Running for {0} days {1}h {2}m {3}s", elapsed.Days, elapsed.Hours, elapsed.Minutes, elapsed.Seconds);
            writeLine("");
            writeLine(" Press V to toggle stats. Esc key to exit.");

        }

        private void writeLine(string str, params object[] arguments)
        {
            Console.WriteLine(string.Format(str, arguments).PadRight(consoleWidth));
        }

        private string checkForUpdate()
        {
            UpdateVerify.checkVersion();
            if (UpdateVerify.inProgress) return "(checking)";
            if(UpdateVerify.ready)
            {
                if (UpdateVerify.error) return "(error)";
                return UpdateVerify.serverVersion;
            }
            return "(not checked)";
        }
    }
}
