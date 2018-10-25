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
using System.IO;
using IXICore;

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

        public long lastHashRate = 0; // Last reported hash rate
        public ulong currentBlockNum = 0; // Mining block number
        public ulong currentBlockDifficulty = 0; // Current block difficulty
        public ulong lastSolvedBlockNum = 0; // Last solved block number
        private DateTime lastSolvedTime = DateTime.MinValue; // Last locally solved block time

        private long hashesPerSecond = 0; // Total number of hashes per second
        private DateTime lastStatTime; // Last statistics output time
        private bool shouldStop = false; // flag to signal shutdown of threads

        Block activeBlock = null;
        bool blockFound = false;

        private static Random random = new Random(); // Used for random nonce

        public static int currentDificulty { get; private set; } // 14 to 256
        private static byte[] hashStartDifficulty = { 0xff, 0xfc }; // minimum = 14

        private static List<ulong> solvedBlocks = new List<ulong>(); // Maintain a list of solved blocks to prevent duplicate work

        public Miner()
        {
            lastStatTime = DateTime.Now;

        }

        // Starts the mining threads
        public bool start()
        {
            if(Config.disableMiner)
                return false;

            shouldStop = false;
            Thread miner_thread = new Thread(threadLoop);
            miner_thread.Start();

            return true;
        }

        // Signals all the mining threads to stop
        public bool stop()
        {
            shouldStop = true;
            return true;
        }

        // difficulty is number of consecutive starting bits which must be 0 in the calculated hash
        public static void setDifficulty(int difficulty)
        {
            if(difficulty< 14)
            {
                difficulty = 14;
            }
            if(difficulty > 256)
            {
                difficulty = 256;
            }
            currentDificulty = difficulty;
            List<byte> diff_temp = new List<byte>();
            while(difficulty >= 8)
            {
                diff_temp.Add(0xff);
                difficulty -= 8;
            }
            if(difficulty > 0)
            {
                byte lastbyte = (byte)(0xff << (8 - difficulty));
                diff_temp.Add(lastbyte);
            }
            hashStartDifficulty = diff_temp.ToArray();
        }

        private void threadLoop(object data)
        {
            // Set initial difficulty
            setDifficulty(14);

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
            if (lastBlockNum > CoreConfig.redactedWindowSize)
                oldestRedactedBlock = lastBlockNum - CoreConfig.redactedWindowSize;

            for (ulong i = lastBlockNum; i > oldestRedactedBlock; i--)
            {
                Block block = Node.blockChain.getBlock(i);
                if (block.powField == null)
                {
                    ulong solved = 0;
                    lock(solvedBlocks)
                    {
                        solved = solvedBlocks.Find(x => x == block.blockNum);
                    }

                    // Check if this block is in the solved list
                    if (solved > 0)
                    {
                        // Do nothing at this point
                    }
                    else
                    {
                        // Block is not solved, select it
                        currentBlockNum = block.blockNum;
                        currentBlockDifficulty = block.difficulty;
                        activeBlock = block;
                        setDifficulty((int)block.difficulty);
                        blockFound = true;
                        return;
                    }
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
            byte[] block_checksum = activeBlock.blockChecksum;
            byte[] solver_address = Node.walletStorage.address;
            Byte[] p1 = new Byte[block_checksum.Length + solver_address.Length];
            System.Buffer.BlockCopy(block_checksum, 0, p1, 0, block_checksum.Length);
            System.Buffer.BlockCopy(solver_address, 0, p1, block_checksum.Length, solver_address.Length);


            string nonce = randomNonce(128);
            string hash = findHash(p1, nonce);
            if(hash.Length < 1)
            {
                Logging.error("Stopping miner due to invalid hash.");
                stop();
                return;
            }

            hashesPerSecond++;

            // We have a valid hash, update the corresponding block
            if (Miner.validateHash(hash) == true)
            {
                Logging.info(String.Format("SOLUTION FOUND FOR BLOCK #{0}: {1}", activeBlock.blockNum, hash));

                // Broadcast the nonce to the network
                sendSolution(nonce);

                // Add this block number to the list of solved blocks
                lock (solvedBlocks)
                {
                    solvedBlocks.Add(activeBlock.blockNum);
                }

                lastSolvedBlockNum = activeBlock.blockNum;
                lastSolvedTime = DateTime.Now;

                // Reset the block found flag so we can search for another block
                blockFound = false;
            }
        }

        // Check if a hash is valid based on the current difficulty
        public static bool validateHash(string hash, ulong difficulty = 0)
        {
            int c_difficulty = currentDificulty;
            // Set the difficulty for verification purposes
            if (difficulty > 0)
            {
                setDifficulty((int)difficulty);
            }

            if (hash.Length < hashStartDifficulty.Length)
            {
                // Reset the difficulty
                if (difficulty > 0)
                {
                    setDifficulty(c_difficulty);
                }
                return false;
            }

            for(int i=0;i<hashStartDifficulty.Length;i++)
            {
                byte hash_byte = byte.Parse(hash.Substring(2*i, 2), System.Globalization.NumberStyles.HexNumber);
                if ((hash_byte & hashStartDifficulty[i]) != 0)
                {
                    // Reset the difficulty
                    if (difficulty > 0)
                    {
                        setDifficulty(c_difficulty);
                    }

                    return false;
                }
            }

            // Reset the difficulty
            if(difficulty > 0)
            {
                setDifficulty(c_difficulty);
            }

            return true;
        }

        // Verify nonce
        public static bool verifyNonce(string nonce, ulong block_num, byte[] solver_address, ulong difficulty)
        {
            Block block = Node.blockChain.getBlock(block_num);
            if (block == null)
                return false;

            // TODO checksum the solver_address just in case it's not valid
            // also protect against spamming with invalid nonce/block_num
            Byte[] p1 = new Byte[block.blockChecksum.Length + solver_address.Length];
            System.Buffer.BlockCopy(block.blockChecksum, 0, p1, 0, block.blockChecksum.Length);
            System.Buffer.BlockCopy(solver_address, 0, p1, block.blockChecksum.Length, solver_address.Length);
            string hash = Miner.findHash(p1, nonce);

            if (Miner.validateHash(hash, difficulty) == true)
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
            tx.to = CoreConfig.ixianInfiniMineAddress;
            tx.amount = "0";
            tx.fee = "0";
            tx.blockHeight = Node.blockChain.getLastBlockNum();

            byte[] pubkey = Node.walletStorage.publicKey;
            // Check if this wallet's public key is already in the WalletState
            Wallet mywallet = Node.walletState.getWallet(tx.from, true);
            if (mywallet.publicKey != null && mywallet.publicKey.SequenceEqual(pubkey))
            {
                // Walletstate public key matches, we don't need to send the public key in the transaction
                pubkey = null;
            }

            using (MemoryStream mw = new MemoryStream())
            {
                using (BinaryWriter writerw = new BinaryWriter(mw))
                {
                    writerw.Write(activeBlock.blockNum);
                    writerw.Write(nonce);
                    tx.data = mw.ToArray();
                }
            }

            tx.pubKey = pubkey;

            tx.timeStamp = Core.getCurrentTimestamp();
            tx.id = tx.generateID();
            tx.checksum = Transaction.calculateChecksum(tx);
            tx.signature = Transaction.getSignature(tx.checksum);

            // Broadcast this transaction to the network
            ProtocolMessage.broadcastProtocolMessage(ProtocolMessageCode.newTransaction, tx.getBytes());
        }

        private static string findHash(byte[] p1, string p2)
        {
            string ret = "";
            try
            {
                byte[] hash = new byte[32];
                byte[] sdata = p1;
                byte[] salt = ASCIIEncoding.ASCII.GetBytes(p2);
                IntPtr data_ptr = Marshal.AllocHGlobal(sdata.Length);
                IntPtr salt_ptr = Marshal.AllocHGlobal(salt.Length);
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
                Logging.error(string.Format("Error during mining: {0}", e.Message));
            }
            return ret;
        }

        // Output the miner status
        private void printMinerStatus()
        {
            // Console.WriteLine("Miner: Block #{0} | Hashes per second: {1}", currentBlockNum, hashesPerSecond);
            lastStatTime = DateTime.Now;
            lastHashRate = hashesPerSecond;
            hashesPerSecond = 0;
        }

        // Returns the number of locally solved blocks
        public int getSolvedBlocksCount()
        {
            lock(solvedBlocks)
            {
                return solvedBlocks.Count();
            }
        }

        // Returns the number of empty and full blocks, based on PoW field
        public List<int> getBlocksCount()
        {
            int empty_blocks = 0;
            int full_blocks = 0;

            ulong lastBlockNum = Node.blockChain.getLastBlockNum();
            ulong oldestRedactedBlock = 0;
            if (lastBlockNum > CoreConfig.redactedWindowSize)
                oldestRedactedBlock = lastBlockNum - CoreConfig.redactedWindowSize;

            for (ulong i = lastBlockNum; i > oldestRedactedBlock; i--)
            {
                Block block = Node.blockChain.getBlock(i);
                if (block.powField == null)
                {
                    empty_blocks++;
                }
                else
                {
                    full_blocks++;
                }
            }
            List<int> result = new List<int>();
            result.Add(empty_blocks);
            result.Add(full_blocks);
            return result;
        }

        // Returns the relative time since the last block was solved
        public string getLastSolvedBlockRelativeTime()
        {
            if (lastSolvedTime == DateTime.MinValue)
                return "Never";

            return Clock.getRelativeTime(lastSolvedTime);
        }



        // Calculates the reward amount for a certain block
        public static IxiNumber calculateRewardForBlock(ulong blockNum)
        {
            ulong pow_reward = 0;

            if (blockNum < 1051200) // first year
            {
                pow_reward = (blockNum * 9) + 9; // +0.009 IXI
            }else if (blockNum < 2102400) // second year
            {
                pow_reward = (1051200 * 9);
            }else if (blockNum < 3153600) // third year
            {
                pow_reward = (1051200 * 9) + ((blockNum - 2102400) * 9) + 9; // +0.009 IXI
            }
            else if (blockNum < 4204800) // fourth year
            {
                pow_reward = (2102400 * 9) + ((blockNum - 3153600) * 2) + 2; // +0.0020 IXI
            }
            else if (blockNum < 5256001) // fifth year
            {
                pow_reward = (2102400 * 9) + (1051200 * 2) + ((blockNum - 4204800) * 9) + 9; // +0.009 IXI
            }
            else // after fifth year if mining is still operational
            {
                pow_reward = ((3153600 * 9) + (1051200 * 2))/2;
            }

            pow_reward = (pow_reward/2 + 10000) * 100000; // Divide by 2 (assuming 50% block coverage) + add inital 10 IXI block reward + add the full amount of 0s to cover IxiNumber decimals
            return new IxiNumber(pow_reward); // Generate the corresponding IxiNumber, including decimals
        }
    }
}
