using DLT.Meta;
using DLTNode;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DLT
{
    class WalletState
    {
        public static List<Wallet> wallets = new List<Wallet> { }; // The entire wallet list

        private static WalletState singletonInstance = new WalletState();

        static WalletState() { }

        public static WalletState singleton
        {
            get
            {
                return singletonInstance;
            }
        }

        // The initial wallet state contains the distribution of all tokens in the specified wallet(s)
        public static void generateWalletState()
        {
            if (Config.genesisFunds > 0)
            {
                // Add genesis funds to this node's address
                Wallet wallet = new Wallet(
                    Node.walletStorage.address,
                    Config.genesisFunds
                );
                wallets.Add(wallet);

                Logging.info(String.Format("Node started with Genesis Mode and distributed {0} tokens to {1}", 
                    Config.genesisFunds, Node.walletStorage.address));
            }
            else
            {
                // The walletstate will be fetched from the network
            }

        }

        // Downloads a complete copy of the wallet state
        public static bool downloadCompleteWalletState()
        {

            return true;
        }

        // Calculate the checksum of the entire wallet state
        public static string calculateChecksum()
        {
            string checksum = Crypto.sha256("IXIAN-DLT");
            lock (wallets)
            {
                foreach (var wallet in wallets)
                {
                    string wallet_checksum = wallet.calculateChecksum();
                    checksum = Crypto.sha256(checksum + wallet_checksum);
                }
            }
            return checksum;
        }

        public static byte[] getBytes()
        {
            using (MemoryStream m = new MemoryStream())
            {
                using (BinaryWriter writer = new BinaryWriter(m))
                {

                    lock (wallets)
                    {
                        // Write the number of wallets
                        int num_wallets = wallets.Count();
                        writer.Write(num_wallets);

                        // Write the checksum to validate the state
                        string checksum = calculateChecksum();
                        writer.Write(checksum);

                        // Write each wallet
                        foreach (Wallet wallet in wallets)
                        {
                            byte[] wallet_data = wallet.getBytes();
                            int wallet_data_size = wallet_data.Length;
                            writer.Write(wallet_data_size);
                            writer.Write(wallet_data);
                        }
                    }
                }
                return m.ToArray();
            }
        }

        // Retrieves a chunk of wallet states
        public static byte[] getChunkBytes(long startOffset, long walletCount, ulong blockNumber)
        {
            // First protect against any size issues
            if (startOffset > getTotalWallets())
            {
                Logging.warn("Attempted to retrieve inexistent wallet chunks");
                return null;
            }

            // We should never exceed the total amount of wallets
            if (startOffset + walletCount > getTotalWallets())
            {
                walletCount = getTotalWallets() - startOffset;
            }

            using (MemoryStream m = new MemoryStream())
            {
                using (BinaryWriter writer = new BinaryWriter(m))
                {
                    // Write the start offset
                    writer.Write(startOffset);

                    // Write the number of wallets
                    writer.Write(walletCount);

                    // Write the block number corresponding to this state
                    writer.Write(blockNumber);

                    lock (wallets)
                    {
                        // Write each corresponding wallet.
                        // TODO: handle larger than maxint values
                        for (int i = (int)startOffset; i < (int)startOffset + (int)walletCount; i++)
                        {
                            Wallet wallet = wallets[i];
                            byte[] wallet_data = wallet.getBytes();
                            int wallet_data_size = wallet_data.Length;
                            writer.Write(wallet_data_size);
                            writer.Write(wallet_data);
                        }
                    }
                }
                return m.ToArray();
            }
        }

        // Process an incoming walletstate chunk
        public static void processChunk(byte[] bytes)
        {
            if(Node.blockProcessor.inSyncMode == false)
            {
                // Node is not in synchronization mode, discard.
                Logging.warn("Received walletstate chunk while not in synchronization mode.");
                return;
            }

            using (MemoryStream m = new MemoryStream(bytes))
            {
                using (BinaryReader reader = new BinaryReader(m))
                {
                    try
                    {
                        // Read the number of wallets
                        long startOffset = reader.ReadInt64();
                        long walletCount = reader.ReadInt64();
                        ulong blockNumber = reader.ReadUInt64();

                        // Ignore invalid wallet state chunks
                        if(startOffset < 0 || walletCount < 0)
                        {
                            Logging.warn("Skipped tainted walletstate chunk.");
                            return;
                        }

                        lock (wallets)
                        {
                            // Go through each wallet and add it to the temporary wallet list
                            for (int i = 0; i < walletCount; i++)
                            {
                                int wallet_data_size = reader.ReadInt32();
                                if (wallet_data_size < 1)
                                    continue;
                                byte[] wallet_bytes = reader.ReadBytes(wallet_data_size);
                                Wallet new_wallet = new Wallet(wallet_bytes);

                                foreach(Wallet twallet in wallets)
                                {
                                    if(twallet.id.Equals(new_wallet.id, StringComparison.Ordinal))
                                    {
                                        // Wallet is already present in the walletstate, don't add it again
                                        Logging.info(string.Format("Received duplicate wallet for id {0} and balance {1}", 
                                            new_wallet.id, new_wallet.balance));
                                        continue;
                                    }
                                }

                                // Insert the wallet at the specified index
                                int wallet_index = (int)startOffset + i;
                                wallets.Insert(wallet_index, new_wallet);

                                // Reverse any transactions during synchronization
                                ulong finalBalance = new_wallet.balance;
                                new_wallet.balance = TransactionPool.getInitialBalanceForWallet(new_wallet.id, finalBalance);

                                Console.WriteLine("SYNC Wallet: {0}", new_wallet.id);
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        Logging.error(string.Format("Error processing walletstate chunk: {0}", e.ToString()));
                    }
                }
            }

            // Check if the new walletstate is ready
            Node.blockProcessor.checkWalletState();
        }


        public static bool syncFromBytes(byte[] bytes)
        {
            // Todo: import into a temporary wallet list, validate the checksum and only then update the wallet state

            // Clear the wallets
            clear();

            using (MemoryStream m = new MemoryStream(bytes))
            {
                using (BinaryReader reader = new BinaryReader(m))
                {
                    // Read the number of wallets
                    int num_wallets = reader.ReadInt32();
                    string checksum = reader.ReadString();

                    lock (wallets)
                    {
                        for (int i = 0; i < num_wallets; i++)
                        {
                            int wallet_data_size = reader.ReadInt32();
                            if (wallet_data_size < 1)
                                continue;
                            byte[] wallet_bytes = reader.ReadBytes(wallet_data_size);
                            Wallet new_wallet = new Wallet(wallet_bytes);
                            wallets.Add(new_wallet);

                            //Logging.log(LogSeverity.info, String.Format("SYNC WALLET address created: {0}", new_wallet.id));
                            Console.WriteLine("SYNC Wallet: {0}", new_wallet.id);

                        }
                    }

                }
            }

            return true;
        }

        // Get the balance of a specific address according to the last generated block's state
        public static ulong getBalanceForAddress(string address)
        {
            if (address == null)
                return 0;

            lock (wallets)
            {
                // Now check the entire wallet list
                foreach (var wallet in wallets)
                {
                    if (address.Equals(wallet.id))
                    {
                        return wallet.balance;
                    }
                }
            }

            return 0;
        }

        // Calculate the latest wallet balance, including transactions from the txpool
        public static ulong getDeltaBalanceForAddress(string address)
        {
            if (address == null)
                return 0;

            lock (wallets)
            {
                // TODO: optimize this for low-powered devices+
                foreach (var wallet in wallets)
                {
                    if (address.Equals(wallet.id))
                    {
                        ulong valid_balance = wallet.balance;
                        lock (TransactionPool.transactions)
                        {
                            foreach (Transaction transaction in TransactionPool.transactions)
                            {
                                if (transaction.to.Equals(address))
                                {
                                    valid_balance += transaction.amount;
                                }
                                else
                                if (transaction.from.Equals(address))
                                {
                                    valid_balance -= transaction.amount;
                                }
                            }
                        }

                        return valid_balance;
                    }
                }
            }

            return 0;
        }

        // Sets a wallet balance for a specific address
        public static void setBalanceForAddress(string address, ulong balance)
        {
            if (address == null)
                return;

            lock (wallets)
            {
                foreach (var wallet in wallets)
                {
                    if (address.Equals(wallet.id))
                    {
                        wallet.balance = balance;
                        return;
                    }
                }

                // Address not found, create a new entry
                Wallet new_wallet = new Wallet(address, balance);
                wallets.Add(new_wallet);
                Logging.log(LogSeverity.info, String.Format("New wallet created: {0}", address));
            }
        }

        // Returns the total number of wallets in the current state
        public static long getTotalWallets()
        {
            long total = 0;
            lock(wallets)
            {
                total = wallets.LongCount();
            }
            return total;
        }

        // Clears all the wallets
        public static void clear()
        {
            lock (wallets)
            {
                wallets.Clear();
            }
        }



    }
}
