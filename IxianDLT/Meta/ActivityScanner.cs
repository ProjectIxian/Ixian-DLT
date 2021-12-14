using DLT;
using DLT.Meta;
using IXICore;
using IXICore.Meta;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DLTNode.Meta
{
    class ActivityScanner
    {
        private static bool shouldStop = false; // flag to signal shutdown of threads
        private static bool active = false;
        private static ulong lastBlockNum = 0; 

        // Starts the activity scanner thread
        public static bool start(int startFromBlock = 0)
        {
            // Check if an activity scan is already in progress
            if (isActive())
                return false;

            shouldStop = false;          
            lastBlockNum = (ulong)startFromBlock;

            active = true;
            try
            {
                Thread scanner_thread = new Thread(threadLoop);
                scanner_thread.Name = "Activity_Scanner_Thread";
                scanner_thread.Start();
            }
            catch
            {
                active = false;
                return false;
            }

            return true;
        }

        // Signals the activity scanner thread to stop
        public static bool stop()
        {
            shouldStop = true;
            return true;
        }

        private static void threadLoop(object data)
        {
            try
            {
                while (!shouldStop)
                {
                    // Stop scanning if we reach the last block height in the stored blockchain
                    if (lastBlockNum > IxianHandler.getLastBlockHeight())
                    {
                        active = false;
                        shouldStop = true;
                        return;
                    }

                    // Go through each block in storage
                    IEnumerable<Transaction> txs = Node.storage.getTransactionsInBlock(lastBlockNum);
                    foreach (Transaction tx in txs)
                    {
                        TransactionPool.addTransactionToActivityStorage(tx);
                    }

                    lastBlockNum++;
                }
            }
            catch (Exception e)
            {
                Logging.error("Error in ActivityScanner: " + e);
            }

            active = false;
        }

        // Check if the activity scanner is already running
        public static bool isActive()
        {
            return active;
        }

        // Return activity scanner processed block number
        public static ulong getLastBlockNum()
        {
            return lastBlockNum;
        }

    }
}
