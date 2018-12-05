using DLT.Network;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DLT.Meta
{
    public class StatsConsoleScreen
    {
        private DateTime startTime;

        private Thread thread = null;
        private bool running = false;

        private int consoleWidth = 50;

        public StatsConsoleScreen()
        {          
            Console.Clear();

            Console.CursorVisible = Config.verboseConsoleOutput;

            // Start thread
            running = true;
            thread = new Thread(new ThreadStart(threadLoop));
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
                if(Config.verboseConsoleOutput == false)
                    drawScreen();

                Thread.Sleep(2000);
                Thread.Yield();
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

            writeLine(" d888888b db    db d888888b  .d8b.  d8b   db");
            writeLine("   `88'   `8b  d8'   `88'   d8' `8b 888o  88");
            writeLine("    88     `8bd8'     88    88ooo88 88V8o 88");
            writeLine("    88     .dPYb.     88    88~~~88 88 V8o88");
            writeLine("   .88.   .8P  Y8.   .88.   88   88 88  V888");
            writeLine(" Y888888P YP    YP Y888888P YP   YP VP   V8P");
            writeLine("\n                              {0}", Config.version);
            writeLine("____________________________________________\n");

            writeLine(" Thank you for running an Ixian DLT node.\n For help please visit www.ixian.io");
            writeLine("____________________________________________\n");

            if (Storage.upgrading)
            {
                writeLine("Upgrading database: " + Storage.upgradeProgress + "/" + Storage.upgradeMaxBlockNum);
            }

            if (Node.serverStarted == false)
            {
                return;
            }

            // Node status
            string dltStatus =  "active       ";
            if (Node.blockSync.synchronizing)
                dltStatus =     "synchronizing";

            int connectionsIn = NetworkServer.getConnectedClients().Count();
            int connectionsOut = NetworkClientManager.getConnectedClients().Count();
            if (connectionsIn + connectionsOut < 1)
                dltStatus =     "connecting   ";


            writeLine("\tStatus:\t\t{0}\n", dltStatus);
            ulong lastBlockNum = Node.blockChain.getLastBlockNum();
            int sigCount = 0;
            if(lastBlockNum > 0)
            {
                Block b = Node.blockChain.getBlock(lastBlockNum);
                if(b != null)
                {
                    sigCount = b.signatures.Count();
                }
            }
            writeLine("\tBlock Height:\t\t{0} ({1} sigs)", lastBlockNum, sigCount);
            writeLine("\tConnections (I/O):\t{0}", connectionsIn + "/" + connectionsOut);
            writeLine("\tPresences:\t\t{0}", PresenceList.getTotalPresences());
            writeLine("\tTransaction Pool:\t{0}", TransactionPool.getUnappliedTransactions().Count());

            // Mining status
            string mineStatus = "disabled";
            if (!Config.disableMiner)
                mineStatus =    "stopped";
            if (Node.miner.lastHashRate > 0)
                mineStatus =    "active ";
            if (Node.miner.pause)
                mineStatus =    "paused ";

            writeLine("");
            writeLine("\tMining:\t\t\t{0}", mineStatus);
            writeLine("\tHashrate:\t\t{0}", Node.miner.lastHashRate);
            writeLine("\tSolved Blocks:\t\t{0}", Node.miner.getSolvedBlocksCount());
            writeLine("____________________________________________");

            TimeSpan elapsed = DateTime.UtcNow - startTime;

            writeLine(" Running for {0} days {1}h {2}m {3}s", elapsed.Days, elapsed.Hours, elapsed.Minutes, elapsed.Seconds);
            writeLine("");
            writeLine(" Press V to toggle stats. Ctrl-C to exit.");

        }

        private void writeLine(string str, params object[] arguments)
        {
            Console.WriteLine(string.Format(str, arguments).PadRight(consoleWidth));
        }
    }
}
