using DLT.Meta;
using DLT.Network;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Timers;

namespace DLT
{
    class Miner
    {
        private long hashesPerSecond = 0; // Total number of hashes per second
        private ulong blockNum = 0; // Mining block number

        private DateTime lastStatTime; // Last statistics output time
        private bool shouldStop = false; // flag to signal shutdown of threads

        public Miner()
        {
            lastStatTime = DateTime.Now;

        }

        // Starts the mining threads
        public bool start()
        {
            return false; 
            shouldStop = false;
            Thread miner_thread = new Thread(threadLoop);
            miner_thread.Start();

            Console.WriteLine("MINER STARTED");
            return true;
        }

        // Signals all the mining threads to stop
        public bool stop()
        {
            shouldStop = true;
            return true;
        }

        private void threadLoop(object data)
        {
            while(!shouldStop)
            {
                // Wait for blockprocessor network synchronization
                if(Node.blockProcessor.synchronized == false)
                {
                    Thread.Sleep(1000);
                    continue;
                }


                // Output mining stats
                TimeSpan timeSinceLastStat = DateTime.Now - lastStatTime;
                if (timeSinceLastStat.TotalSeconds > 1)
                {
                    printMinerStatus();
                }
            }
        }

        private void searchForBlock()
        {

        }


        // Output the miner status
        private void printMinerStatus()
        {
            Console.WriteLine("Miner: Block #{0} | Hashes per second: {1}", blockNum, hashesPerSecond);
            lastStatTime = DateTime.Now;
            hashesPerSecond = 0;
        }

    }
}
