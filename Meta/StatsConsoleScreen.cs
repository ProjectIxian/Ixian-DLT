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

        public StatsConsoleScreen()
        {          
            Console.Clear();

            // Start thread
            running = true;
            thread = new Thread(new ThreadStart(threadLoop));
            thread.Start();

            startTime = DateTime.Now;
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

        public void drawScreen()
        {
            // Set the background color
            Console.BackgroundColor = ConsoleColor.DarkGreen;
            Console.Clear();
            Console.SetCursorPosition(0, 0);

            Console.WriteLine(" d888888b db    db d888888b  .d8b.  d8b   db");
            Console.WriteLine("   `88'   `8b  d8'   `88'   d8' `8b 888o  88");
            Console.WriteLine("    88     `8bd8'     88    88ooo88 88V8o 88");
            Console.WriteLine("    88     .dPYb.     88    88~~~88 88 V8o88");
            Console.WriteLine("   .88.   .8P  Y8.   .88.   88   88 88  V888");
            Console.WriteLine(" Y888888P YP    YP Y888888P YP   YP VP   V8P");
            Console.WriteLine("\n                              {0}", Config.version);
            Console.WriteLine("____________________________________________\n");

            Console.WriteLine(" Thank you for running an Ixian DLT node.\n For help please visit www.ixian.io");
            Console.WriteLine("____________________________________________\n\n");

            if(Node.serverStarted == false)
            {
                return;
            }

            // Node status
            string dltStatus = "active";
            if (Node.blockSync.synchronizing)
                dltStatus = "synchronizing";

            int connections = NetworkServer.getConnectedClients().Count() + NetworkClientManager.getConnectedClients().Count();
            if (connections < 1)
                dltStatus = "connecting";

            Console.WriteLine("\tStatus:\t\t{0}\n", dltStatus);
            Console.WriteLine("\tBlock Height:\t\t{0}", Node.blockChain.getLastBlockNum());
            Console.WriteLine("\tConnections:\t\t{0}", connections);
            Console.WriteLine("\tPresences:\t\t{0}", PresenceList.getTotalPresences());
            Console.WriteLine("\tTransaction Pool:\t{0}", TransactionPool.getUnappliedTransactions().Count());

            // Mining status
            string mineStatus = "stopped";
            if (Node.miner.lastHashRate > 0)
                mineStatus = "active";

            Console.WriteLine("");
            Console.WriteLine("\tMining:\t\t\t{0}", mineStatus);
            Console.WriteLine("\tHashrate:\t\t{0}", Node.miner.lastHashRate);
            Console.WriteLine("\tSolved Blocks:\t\t{0}", Node.miner.getSolvedBlocksCount());
            Console.WriteLine("____________________________________________");

            TimeSpan elapsed = DateTime.Now - startTime;

            Console.WriteLine(" Running for {0} days {1}h {2}m {3}s", elapsed.Days, elapsed.Hours, elapsed.Minutes, elapsed.Seconds);
            Console.WriteLine("");
            Console.WriteLine(" Press V to toggle stats. Ctrl-C to exit.");

        }

    }
}
