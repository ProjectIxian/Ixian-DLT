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
using System.Numerics;

namespace DLT
{
    class Miner
    {
        // Import the libargon2 shared library
        [DllImport("libargon2", CharSet = CharSet.Ansi, CallingConvention = CallingConvention.Cdecl)]
        static extern int argon2id_hash_raw(UInt32 time_cost, UInt32 mem_cost, UInt32 parallelism,
                                 IntPtr data, UIntPtr data_len,
                                 IntPtr salt, UIntPtr salt_len,
                                 IntPtr output, UIntPtr output_len);

        public long lastHashRate = 0; // Last reported hash rate
        public ulong currentBlockNum = 0; // Mining block number
        public int currentBlockVersion = 0;
        public ulong currentBlockDifficulty = 0; // Current block difficulty
        public byte[] currentHashCeil { get; private set; }
        public ulong lastSolvedBlockNum = 0; // Last solved block number
        private DateTime lastSolvedTime = DateTime.MinValue; // Last locally solved block time

        private long hashesPerSecond = 0; // Total number of hashes per second
        private DateTime lastStatTime; // Last statistics output time
        private bool shouldStop = false; // flag to signal shutdown of threads

        private static int currentDificulty_v0 = 14;
        private static byte[] hashStartDifficulty_v0 = { 0xff, 0xfc };


        Block activeBlock = null;
        bool blockFound = false;

        private static Random random = new Random(); // Used for random nonce

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

        public static byte[] getHashCeilFromDifficulty(ulong difficulty)
        {
            /*
             * difficulty is an 8-byte number from 0 to 2^64-1, which represents how hard it is to find a hash for a certain block
             * the dificulty is converted into a 'ceiling value', which specifies the maximum value a hash can have to be considered valid under that difficulty
             * to do this, follow the attached algorithm:
             *  1. calculate a bit-inverse value of the difficulty
             *  2. create a comparison byte array with the ceiling value of length 10 bytes
             *  3. set the first two bytes to zero
             *  4. insert the inverse difficulty as the next 8 bytes (mind the byte order!)
             *  5. the remaining 22 bytes are assumed to be 'FF'
             */
            byte[] hash_ceil = new byte[10];
            hash_ceil[0] = 0x00;
            hash_ceil[1] = 0x00;
            for(int i=0;i<8;i++)
            {
                int shift = 8 * (7 - i);
                ulong mask = ((ulong)0xff) << shift;
                byte cb = (byte)((difficulty & mask) >> shift);
                hash_ceil[i + 2] = (byte)~cb;
            }
            return hash_ceil;
        }

        public static BigInteger getTargetHashcountPerBlock(ulong difficulty)
        {
            // For difficulty calculations see accompanying TXT document in the IxianDLT folder.
            // I am sorry for this - Zagar
            // What it does:
            // internally (in Miner.cs), we use little-endian byte arrays to represent hashes and solution ceilings, because it is slightly more efficient memory-allocation-wise.
            // in this place, we are using BigInteger's division function, so we don't have to write our own.
            // BigInteger uses a big-endian byte-array, so we have to reverse our ceiling, which looks like this:
            // little endian: 0000 XXXX XXXX XXXX XXXX FFFF FFFF FFFF FFFF FFFF FFFF FFFF FFFF FFFF FFFF FFFF ; X represents where we set the difficulty
            // big endian: FFFF FFFF FFFF FFFF FFFF FFFF FFFF FFFF FFFF FFFF FFFF YYYY YYYY YYYY YYYY 0000 ; Y represents the difficulty, but the bytes are reversed
            // 9 -(i-22) transforms the index in the big-endian byte array into an index in our 'hash_ceil'. Please also note that due to effciency we only return the
            // "interesting part" of the hash_ceil (first 10 bytes) and assume the others to be FF when doing comparisons internally. The first part of the 'if' in the loop
            // fills in those bytes as well, because BigInteger needs them to reconstruct the number.
            byte[] hash_ceil = Miner.getHashCeilFromDifficulty(difficulty);
            byte[] full_ceil = new byte[32];
            // BigInteger requires bytes in big-endian order
            for (int i = 0; i < 32; i++)
            {
                if (i < 22)
                {
                    full_ceil[i] = 0xff;
                }
                else
                {
                    full_ceil[i] = hash_ceil[9 - (i - 22)];
                }
            }

            BigInteger ceil = new BigInteger(full_ceil);
            // the value below is the easiest way to get maximum hash value into a BigInteger (2^256 -1). Ixian shifts the integer 8 places to the right to get 8 decimal places.
            BigInteger max = new IxiNumber("1157920892373161954235709850086879078532699846656405640394575840079131.29639935").getAmount();
            return max / ceil;
        }

        public static ulong calculateTargetDifficulty(BigInteger current_hashes_per_block)
        {
            // Sorry :-)
            // Target difficulty is calculated as such:
            // We input the number of hashes that have been generated to solve a block (Network hash rate * 60 - we want that solving a block should take 60 seconds, if the entire network hash power was focused on one block, thus achieving
            // an approximate 50% solve rate).
            // We are using BigInteger for its division function, so that we don't have to write out own.
            // Dividing the max hash number with the hashrate will give us an appropriate ceiling, which would result in approximately one solution per "current_hashes_per_block" hash attempts.
            // This target ceiling contains our target difficulty, in the format:
            // big endian: FFFF FFFF FFFF FFFF FFFF FFFF FFFF FFFF FFFF FFFF FFFF YYYY YYYY YYYY YYYY 0000; Y represents the difficulty, but the bytes are reversed
            // the bytes being reversed is actually okay, because we are using BitConverter.ToUInt64, which takes a big-endian byte array to return a ulong number.
            BigInteger max = new IxiNumber("1157920892373161954235709850086879078532699846656405640394575840079131.29639935").getAmount();
            BigInteger target_ceil = max / current_hashes_per_block;
            byte[] temp = target_ceil.ToByteArray();
            // we get the bytes in the reverse order, so the padding should go at the end
            byte[] target_ceil_bytes = new byte[32];
            Array.Copy(temp, target_ceil_bytes, temp.Length);
            for (int i = temp.Length; i < 32; i++)
            {
                target_ceil_bytes[i] = 0;
            }
            //
            byte[] difficulty = new byte[8];
            Array.Copy(target_ceil_bytes, 22, difficulty, 0, 8);
            for(int i = 0; i < 8; i++)
            {
                difficulty[i] = (byte)~difficulty[i];
            }
            return BitConverter.ToUInt64(difficulty, 0);
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
                    if (currentBlockVersion == 0)
                    {
                        calculatePow_v0();
                    }
                    else
                    {
                        calculatePow_v1(currentHashCeil);
                    }
                }

                // Output mining stats
                TimeSpan timeSinceLastStat = DateTime.Now - lastStatTime;
                if (timeSinceLastStat.TotalSeconds > 1)
                {
                    printMinerStatus();
                    Block tmpBlock = Node.blockChain.getBlock(currentBlockNum);
                    if (tmpBlock != null && tmpBlock.powField != null)
                    {
                        blockFound = false;
                        continue;
                    }
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
                if(block == null)
                {
                    continue;
                }
                if (block.powField == null)
                {
                    if(block.version == 0 && block.difficulty > 64)
                    {
                        continue;
                    }
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
                        currentDificulty_v0 = (int)block.difficulty;
                        currentBlockVersion = block.version;
                        currentHashCeil = getHashCeilFromDifficulty(currentBlockDifficulty);

                        activeBlock = block;
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

        private void calculatePow_v0()
        {
            // PoW = Argon2id( BlockChecksum + SolverAddress, Nonce)
            byte[] block_checksum = activeBlock.blockChecksum;
            byte[] solver_address = Node.walletStorage.address;
            Byte[] p1 = new Byte[block_checksum.Length + solver_address.Length];
            System.Buffer.BlockCopy(block_checksum, 0, p1, 0, block_checksum.Length);
            System.Buffer.BlockCopy(solver_address, 0, p1, block_checksum.Length, solver_address.Length);


            string nonce = randomNonce(128);
            string hash = findHash_v0(p1, nonce);
            if (hash.Length < 1)
            {
                Logging.error("Stopping miner due to invalid hash.");
                stop();
                return;
            }

            hashesPerSecond++;

            // We have a valid hash, update the corresponding block
            if (Miner.validateHash_v0(hash) == true)
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


        private void calculatePow_v1(byte[] hash_ceil)
        {
            // PoW = Argon2id( BlockChecksum + SolverAddress, Nonce)
            byte[] block_checksum = activeBlock.blockChecksum;
            byte[] solver_address = Node.walletStorage.address;
            Byte[] p1 = new Byte[block_checksum.Length + solver_address.Length];
            System.Buffer.BlockCopy(block_checksum, 0, p1, 0, block_checksum.Length);
            System.Buffer.BlockCopy(solver_address, 0, p1, block_checksum.Length, solver_address.Length);


            string nonce = randomNonce(128);
            byte[] hash = findHash_v1(p1, nonce);
            if(hash.Length < 1)
            {
                Logging.error("Stopping miner due to invalid hash.");
                stop();
                return;
            }

            hashesPerSecond++;

            // We have a valid hash, update the corresponding block
            if (Miner.validateHashInternal(hash, hash_ceil) == true)
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

        // difficulty is number of consecutive starting bits which must be 0 in the calculated hash
        public static void setDifficulty_v0(int difficulty)
        {
            if (difficulty < 14)
            {
                difficulty = 14;
            }
            if (difficulty > 256)
            {
                difficulty = 256;
            }
            currentDificulty_v0 = difficulty;
            List<byte> diff_temp = new List<byte>();
            while (difficulty >= 8)
            {
                diff_temp.Add(0xff);
                difficulty -= 8;
            }
            if (difficulty > 0)
            {
                byte lastbyte = (byte)(0xff << (8 - difficulty));
                diff_temp.Add(lastbyte);
            }
            hashStartDifficulty_v0 = diff_temp.ToArray();
        }

        // Check if a hash is valid based on the current difficulty
        public static bool validateHash_v0(string hash, ulong difficulty = 0)
        {
            int c_difficulty = currentDificulty_v0;
            // Set the difficulty for verification purposes
            if (difficulty > 0)
            {
                setDifficulty_v0((int)difficulty);
            }

            if (hash.Length < hashStartDifficulty_v0.Length)
            {
                // Reset the difficulty
                if (difficulty > 0)
                {
                    setDifficulty_v0(c_difficulty);
                }
                return false;
            }

            for (int i = 0; i < hashStartDifficulty_v0.Length; i++)
            {
                byte hash_byte = byte.Parse(hash.Substring(2 * i, 2), System.Globalization.NumberStyles.HexNumber);
                if ((hash_byte & hashStartDifficulty_v0[i]) != 0)
                {
                    // Reset the difficulty
                    if (difficulty > 0)
                    {
                        setDifficulty_v0(c_difficulty);
                    }

                    return false;
                }
            }

            // Reset the difficulty
            if (difficulty > 0)
            {
                setDifficulty_v0(c_difficulty);
            }

            return true;
        }

        private static bool validateHashInternal(byte[] hash, byte[] hash_ceil)
        {
            for (int i = 0; i < hash.Length; i++)
            {
                byte cb = i < hash_ceil.Length ? hash_ceil[i] : (byte)0xff;
                if (hash_ceil[i] > hash[i]) return true;
                if (hash_ceil[i] < hash[i]) return false;
            }
            // if we reach this point, the hash is exactly equal to the ceiling we consider this a 'passing hash'
            return true;
        }

        // Check if a hash is valid based on the current difficulty
        public static bool validateHash_v1(byte[] hash, ulong difficulty)
        {
            return validateHashInternal(hash, getHashCeilFromDifficulty(difficulty));
        }

        // Verify nonce
        public static bool verifyNonce_v0(string nonce, ulong block_num, byte[] solver_address, ulong difficulty)
        {
            Block block = Node.blockChain.getBlock(block_num);
            if (block == null)
                return false;

            // TODO checksum the solver_address just in case it's not valid
            // also protect against spamming with invalid nonce/block_num
            Byte[] p1 = new Byte[block.blockChecksum.Length + solver_address.Length];
            System.Buffer.BlockCopy(block.blockChecksum, 0, p1, 0, block.blockChecksum.Length);
            System.Buffer.BlockCopy(solver_address, 0, p1, block.blockChecksum.Length, solver_address.Length);
            string hash = Miner.findHash_v0(p1, nonce);

            if (Miner.validateHash_v0(hash, difficulty) == true)
            {
                // Hash is valid
                return true;
            }

            return false;
        }

        // Verify nonce
        public static bool verifyNonce_v1(string nonce, ulong block_num, byte[] solver_address, ulong difficulty)
        {
            Block block = Node.blockChain.getBlock(block_num);
            if (block == null)
                return false;

            // TODO checksum the solver_address just in case it's not valid
            // also protect against spamming with invalid nonce/block_num
            Byte[] p1 = new Byte[block.blockChecksum.Length + solver_address.Length];
            System.Buffer.BlockCopy(block.blockChecksum, 0, p1, 0, block.blockChecksum.Length);
            System.Buffer.BlockCopy(solver_address, 0, p1, block.blockChecksum.Length, solver_address.Length);
            byte[] hash = Miner.findHash_v1(p1, nonce);

            if (Miner.validateHash_v1(hash, difficulty) == true)
            {
                // Hash is valid
                return true;
            }

            return false;
        }

        // Broadcasts the solution to the network
        public void sendSolution(string nonce)
        {
            byte[] pubkey = Node.walletStorage.publicKey;
            // Check if this wallet's public key is already in the WalletState
            Wallet mywallet = Node.walletState.getWallet(Node.walletStorage.getWalletAddress(), true);
            if (mywallet.publicKey != null && mywallet.publicKey.SequenceEqual(pubkey))
            {
                // Walletstate public key matches, we don't need to send the public key in the transaction
                pubkey = null;
            }

            byte[] data = null;

            using (MemoryStream mw = new MemoryStream())
            {
                using (BinaryWriter writerw = new BinaryWriter(mw))
                {
                    writerw.Write(activeBlock.blockNum);
                    writerw.Write(nonce);
                    data = mw.ToArray();
                }
            }

            Transaction tx = new Transaction((int)Transaction.Type.PoWSolution, new IxiNumber(0), new IxiNumber(0), CoreConfig.ixianInfiniMineAddress, Node.walletStorage.getWalletAddress(), data, pubkey, Node.blockChain.getLastBlockNum());

            // Broadcast this transaction to the network
            ProtocolMessage.broadcastProtocolMessage(ProtocolMessageCode.newTransaction, tx.getBytes());
        }

        private static string findHash_v0(byte[] p1, string p2)
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
            catch (Exception e)
            {
                Logging.error(string.Format("Error during mining: {0}", e.Message));
            }
            return ret;
        }

        private static byte[] findHash_v1(byte[] p1, string p2)
        {
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
                int result = argon2id_hash_raw((UInt32)1, (UInt32)1024, (UInt32)2, data_ptr, data_len, salt_ptr, salt_len, result_ptr, (UIntPtr)32);
                DateTime end = DateTime.Now;
                //    Console.WriteLine(String.Format("Argon took: {0} ms.", (end - start).TotalMilliseconds));
                Marshal.Copy(result_ptr, hash, 0, 32);
                Marshal.FreeHGlobal(data_ptr);
                Marshal.FreeHGlobal(result_ptr);
                return hash;
            }
            catch(Exception e)
            {
                Logging.error(string.Format("Error during mining: {0}", e.Message));
                return null;
            }
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
            return new IxiNumber(new BigInteger(pow_reward)); // Generate the corresponding IxiNumber, including decimals
        }
    }
}
