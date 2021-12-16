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
        public static bool start(ulong startFromBlock = 0)
        {
            // Check if an activity scan is already in progress
            if (isActive())
                return false;

            active = true;
            shouldStop = false;          
            lastBlockNum = startFromBlock;

            try
            {
                Thread scanner_thread = new Thread(threadLoop);
                scanner_thread.Name = "Activity_Scanner_Thread";
                scanner_thread.Start();
            }
            catch
            {
                active = false;
                Logging.error("Cannot start ActivityScanner");
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
                if (!ActivityStorage.clearStorage(0))
                {
                    Logging.error("Cannot clear storage before starting ActivityScanner");
                    shouldStop = true;
                    active = false;
                    return;
                }

                ulong scanToBlockNum = IxianHandler.getLastBlockHeight();

                while (!shouldStop && lastBlockNum < scanToBlockNum)
                {
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

            shouldStop = true;
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
