using DLT.Meta;
using DLT.Network;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Timers;
using System.Runtime.InteropServices;

namespace DLT
{
    class Miner
    {
        // Import the argon dll
        [DllImport("Argon2_C.dll", CharSet = CharSet.Ansi, CallingConvention = CallingConvention.Cdecl)]
        static extern int argon2id_hash_raw(UInt32 time_cost, UInt32 mem_cost, UInt32 parallelism,
                                 IntPtr data, UIntPtr data_len,
                                 IntPtr salt, UIntPtr salt_len,
                                 IntPtr output, UIntPtr output_len);


        private long hashesPerSecond = 0; // Total number of hashes per second
        private ulong blockNum = 0; // Mining block number

        private DateTime lastStatTime; // Last statistics output time
        private bool shouldStop = false; // flag to signal shutdown of threads

        Block activeBlock = null;
        bool blockFound = false;

        private static Random random = new Random(); // Used for random nonce

        public Miner()
        {
            lastStatTime = DateTime.Now;

        }

        // Starts the mining threads
        public bool start()
        {
            if(Config.disableMiner)
                return true;

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
            while (!shouldStop)
            {
                // Wait for blockprocessor network synchronization
                if (Node.blockProcessor.operating == false)
                {
                    Thread.Sleep(1000);
                    continue;
                }
                
                // Edge case for seeds
                if (Node.blockChain.getLastBlockNum() < 10)
                {
                    Thread.Sleep(1000);
                    continue;
                }

                if (blockFound == false)
                {
                    searchForBlock();
                }
                else
                {
                    calculatePow();
                }


                // Output mining stats
                TimeSpan timeSinceLastStat = DateTime.Now - lastStatTime;
                if (timeSinceLastStat.TotalSeconds > 1)
                {
                    printMinerStatus();
                }
            }
        }

        // Returns the most recent block without a PoW flag in the redacted blockchain
        private void searchForBlock()
        {
            ulong lastBlockNum = Node.blockChain.getLastBlockNum();
            ulong oldestRedactedBlock = 0;
            if (lastBlockNum > Node.blockChain.redactedWindowSize)
                oldestRedactedBlock = lastBlockNum - Node.blockChain.redactedWindowSize;

            for (ulong i = lastBlockNum; i > oldestRedactedBlock; i--)
            {
                Block block = Node.blockChain.getBlock(i);
                if (block.powField.Length < 1)
                {
                    blockNum = block.blockNum;
                    activeBlock = block;
                    blockFound = true;
                    return;
                }

            }

            // No blocks with empty PoW field found, wait a bit
            Thread.Sleep(1000);
            return;
        }

        // Generate a random nonce with a specified length
        private static string randomNonce(int length)
        {
            const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
            return new string(Enumerable.Repeat(chars, length)
              .Select(s => s[random.Next(s.Length)]).ToArray());
        }

        private void calculatePow()
        {
            // PoW = Argon2id( BlockChecksum + SolverAddress, Nonce)
            string block_checksum = activeBlock.blockChecksum;
            string solver_address = Node.walletStorage.address;
            string p1 = string.Format("{0}{1}", block_checksum, solver_address);
            string nonce = randomNonce(128);
            string hash = findHash(p1, nonce);
            if(hash.Length < 1)
            {
                Logging.warn("Stopping miner due to invalid hash.");
                stop();
                return;
            }

            hashesPerSecond++;

            // We have a valid hash, update the corresponding block
            if (Miner.validateHash(hash) == true)
            {
                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine("SOLUTION FOUND FOR BLOCK #{0}: {1}", activeBlock.blockNum, hash);
                Console.ResetColor();

                // Broadcast the nonce to the network
                sendSolution(nonce);

                activeBlock.powField = hash;
                blockFound = false;
            }
        }

        // Check if a hash is valid based on the current difficulty
        public static bool validateHash(string hash)
        {
            bool valid = false;
            int minDif = 2;
            int numZeros = 0;

            foreach (char c in hash)
            {
                if (c == '0')
                {
                    numZeros++;
                    if (numZeros >= minDif)
                    {
                        valid = true;
                        break;
                    }
                }
                else
                {
                    valid = false;
                    return false;
                }
            }
            return valid;
        }

        // Verify nonce
        public static bool verifyNonce(string nonce, ulong block_num, string solver_address)
        {
            Block block = Node.blockChain.getBlock(block_num);
            if (block == null)
                return false;

            // TODO checksum the solver_address just in case it's not valid
            // also protect against spamming with invalid nonce/block_num

            string p1 = string.Format("{0}{1}", block.blockChecksum, solver_address);
            string hash = Miner.findHash(p1, nonce);

            if (Miner.validateHash(hash) == true)
            {
                // Hash is valid
                return true;
            }

            return false;
        }

        // Broadcasts the solution to the network
        public void sendSolution(string nonce)
        {
            Transaction tx = new Transaction();
            tx.type = (int)Transaction.Type.PoWSolution;
            tx.from = Node.walletStorage.getWalletAddress();
            tx.to = "IXIAN";
            tx.amount = "0";

            string data = string.Format("{0}||{1}||{2}", Node.walletStorage.publicKey, activeBlock.blockNum, nonce);
            Console.WriteLine("Data: {0}", data);
            tx.data = data;

            tx.timeStamp = Clock.getTimestamp(DateTime.Now);
            tx.checksum = Transaction.calculateChecksum(tx);
            tx.signature = Transaction.getSignature(tx.checksum);

            // Broadcast this transaction to the network
            ProtocolMessage.broadcastProtocolMessage(ProtocolMessageCode.newTransaction, tx.getBytes());
        }

        private static string findHash(string p1, string p2)
        {
            string ret = "";
            try
            {
                byte[] hash = new byte[32];
                byte[] sdata = ASCIIEncoding.ASCII.GetBytes(p1);
                byte[] salt = ASCIIEncoding.ASCII.GetBytes(p2);
                IntPtr data_ptr = Marshal.AllocHGlobal(sdata.Length);
                IntPtr salt_ptr = Marshal.AllocHGlobal(sdata.Length);
                Marshal.Copy(sdata, 0, data_ptr, sdata.Length);
                Marshal.Copy(salt, 0, salt_ptr, salt.Length);
                UIntPtr data_len = (UIntPtr)sdata.Length;
                UIntPtr salt_len = (UIntPtr)salt.Length;
                IntPtr result_ptr = Marshal.AllocHGlobal(32);
                DateTime start = DateTime.Now;
                int result = argon2id_hash_raw((UInt32)1, (UInt32)1024, (UInt32)4, data_ptr, data_len, salt_ptr, salt_len, result_ptr, (UIntPtr)32);
                DateTime end = DateTime.Now;
                //    Console.WriteLine(String.Format("Argon took: {0} ms.", (end - start).TotalMilliseconds));
                Marshal.Copy(result_ptr, hash, 0, 32);
                ret = BitConverter.ToString(hash).Replace("-", string.Empty);
                Marshal.FreeHGlobal(data_ptr);
                Marshal.FreeHGlobal(result_ptr);
            }
            catch(Exception e)
            {
                Logging.warn(string.Format("Error during mining: {0}", e.Message));
            }
            return ret;
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
