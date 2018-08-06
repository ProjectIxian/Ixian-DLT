using DLT.Meta;
using DLT.Network;
using DLTNode;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DLT
{
    class TransactionPool
    {
        static readonly List<Transaction> transactions = new List<Transaction> { };
        public static int activeTransactions
        {
            get
            {
                lock(transactions)
                {
                    return transactions.Count;
                }
            }
        }

        public static int numTransactions { get => activeTransactions; }

        static TransactionPool()
        {
        }

        private TransactionPool()
        {
        }

        // Adds a non-verified transaction to the memory pool
        // Returns true if the transaction is added to the pool, false otherwise
        public static bool addTransaction(Transaction transaction)
        {
            // Calculate the transaction checksum and compare it
            string checksum = Transaction.calculateChecksum(transaction);
            if(checksum.Equals(transaction.checksum) == false)
            {
                Logging.warn(String.Format("Adding transaction {{ {0} }}, but checksum doesn't match!", transaction.id));
                return false;
            }

            // Prevent transaction spamming
            // Commented due to implementation of 'pending' transactions for S2 as per whitepaper
            /*if(transaction.amount == 0)
            {
                return false;
            }*/

            // Prevent sending to the sender's address
            if(transaction.from.Equals(transaction.to,StringComparison.Ordinal))
            {
                Logging.warn(string.Format("Invalid TO address for transaction id: {0}", transaction.id));
                return false;
            }

            // Run through existing transactions in the pool and verify for double-spending / invalid states
            // Note that we lock the transaction for the entire duration of the checks, which might pose performance issues
            // Todo: find a better way to handle this without running into threading bugs
            lock (transactions)
            {
                foreach (Transaction tx in transactions)
                {
                    if (tx.equals(transaction) == true)
                        return false;
                }


                // Verify the transaction against the wallet state
                // If the balance after the transaction is negative, do not add it.
                IxiNumber fromBalance = WalletState.getBalanceForAddress(transaction.from);
                IxiNumber finalFromBalance = fromBalance - transaction.amount;

                if (finalFromBalance < (long)0)
                {
                    // Prevent overspending
                    Logging.warn(String.Format("Attempted to overspend with transaction {{ {0} }}.", transaction.id));
                    return false;
                }

                // Finally, verify the signature
                if (transaction.verifySignature() == false)
                {
                    // Transaction signature is invalid
                    Logging.warn(string.Format("Invalid signature for transaction id: {0}", transaction.id));
                    return false;
                }
                Logging.info(String.Format("Accepted transaction {{ {0} }}, amount: {1}", transaction.id, transaction.amount));
                transactions.Add(transaction);
            }

            // Broadcast this transaction to the network
            ProtocolMessage.broadcastProtocolMessage(ProtocolMessageCode.newTransaction, transaction.getBytes());

            return true;
        }

        public static Transaction getTransaction(string txid)
        {
            lock(transactions)
            {
                Logging.info(String.Format("Looking for transaction {{ {0} }}. Pool has {1}.", txid, transactions.Count));
                return transactions.Find(x => x.id == txid);
            }
        }

        public static Transaction[] getAllTransactions()
        {
            lock(transactions)
            {
                return transactions.ToArray();
            }
        }

        // This updates a pre-existing transaction
        // Returns true if the transaction has been updated, false otherwise
        public static bool updateTransaction(Transaction transaction)
        {
            // Calculate the transaction checksum and compare it
            string checksum = Transaction.calculateChecksum(transaction);
            if (checksum.Equals(transaction.checksum) == false)
            {
                return false;
            }

            // Prevent sending to the sender's address
            if (transaction.from.Equals(transaction.to, StringComparison.Ordinal))
            {
                Logging.warn(string.Format("Invalid TO address for updated transaction id: {0}", transaction.id));
                return false;
            }

            // Verify the signature
            if (transaction.verifySignature() == false)
            {
                // Transaction signature is invalid
                Logging.warn(string.Format("Invalid signature for updated transaction id: {0}", transaction.id));
                return false;
            }

            // Run through existing transactions in the pool and verify for double-spending / invalid states
            // Note that we lock the transaction for the entire duration of the checks, which might pose performance issues
            // Todo: find a better way to handle this without running into threading bugs
            lock (transactions)
            {
                foreach (Transaction tx in transactions)
                {
                    if (tx.id.Equals(transaction.id, StringComparison.Ordinal) == true)
                    {
                        tx.amount = transaction.amount;
                        tx.data = transaction.data;
                        tx.checksum = transaction.checksum;

                        // Broadcast this transaction update to the network
                        ProtocolMessage.broadcastProtocolMessage(ProtocolMessageCode.updateTransaction, transaction.getBytes());

                        return true;
                    }
                }
            }

            return false;
        }


        // This applies all the transactions from a block to the actual walletstate.
        // It removes the corresponding transactions as well from the pool.
        public static void applyTransactionsFromBlock(Block block)
        {
            if (block == null)
                return;

            lock (transactions)
            {
                List<Transaction> cached_pool;
                // Console.WriteLine("||| Transactions before applyTransactionsFromBlock: {0}", transactions.Count());
                cached_pool = new List<Transaction>(transactions);

                // Check the transactions against what we have in the transaction pool
                foreach (string txid in block.transactions)
                {
                    foreach (Transaction transaction in cached_pool)
                    {
                        if (txid.Equals(transaction.id))
                        {
                            // Don't remove 'pending' transactions
                            // TODO: add an expire condition to prevent potential spaming of the txpool
                            if(transaction.amount == 0)
                            {
                                continue;
                            }

                            // Applies the transaction to the wallet state
                            // TODO: re-validate the transactions here to prevent any potential exploits
                            IxiNumber fromBalance = WalletState.getBalanceForAddress(transaction.from);
                            IxiNumber finalFromBalance = fromBalance - transaction.amount;

                            IxiNumber toBalance = WalletState.getBalanceForAddress(transaction.to);

                            WalletState.setBalanceForAddress(transaction.to, toBalance + transaction.amount);
                            WalletState.setBalanceForAddress(transaction.from, fromBalance - transaction.amount);

                            // Add the transaction to storage
                            TransactionStorage.addTransaction(transaction);

                            // Remove from
                            cached_pool.Remove(transaction);
                            break;
                        }
                    }
                }

                transactions.Clear();
                transactions.AddRange(cached_pool);
                // Console.WriteLine("||| Transactions after applyTransactionsFromBlock: {0}", transactions.Count());
            }
        }


        // Get the current snapshot of transactions in the pool
        public static byte[] getBytes()
        {
            lock (transactions)
            {
                using (MemoryStream m = new MemoryStream())
                {
                    using (BinaryWriter writer = new BinaryWriter(m))
                    {
                        // Write the number of transactions
                        int num_transactions = transactions.Count();
                        writer.Write(num_transactions);

                        // Write each transactions
                        foreach (Transaction transaction in transactions)
                        {
                            byte[] transaction_data = transaction.getBytes();
                            int transaction_data_size = transaction_data.Length;
                            writer.Write(transaction_data_size);
                            writer.Write(transaction_data);
                        }
                    }
                    return m.ToArray();
                }
            }
        }

        public static bool syncFromBytes(byte[] bytes)
        {
            using (MemoryStream m = new MemoryStream(bytes))
            {
                using (BinaryReader reader = new BinaryReader(m))
                {
                    // Read the number of transactions
                    int num_transactions = reader.ReadInt32();

                    Console.WriteLine("Number of transactions in sync txpool: {0}", num_transactions);
                    if (num_transactions < 0)
                        return false;

                    try
                    {
                        for (int i = 0; i < num_transactions; i++)
                        {
                            int transaction_data_size = reader.ReadInt32();
                            if (transaction_data_size < 1)
                                continue;
                            byte[] transaction_bytes = reader.ReadBytes(transaction_data_size);
                            Transaction new_transaction = new Transaction(transaction_bytes);
                            lock (transactions)
                            {
                                transactions.Add(new_transaction);
                            }

                            Console.WriteLine("SYNC Transaction: {0}", new_transaction.id);

                        }
                    }
                    catch(Exception e)
                    {
                        Logging.error(string.Format("Error reading transaction pool: {0}", e.ToString()));
                        return false;
                    }

                }
            }

            return true;
        }


        // Clears all the transactions in the pool
        public static void clear()
        {
            lock(transactions)
            {
                transactions.Clear();
            }
        }

        // Returns the initial balance of a wallet by reversing all the transactions in the memory pool
        public static IxiNumber getInitialBalanceForWallet(string address, IxiNumber finalBalance)
        {
            List<Transaction> cached_pool;
            IxiNumber initialBalance = finalBalance;

            lock (transactions)
            {
                cached_pool = new List<Transaction>(transactions);

                // Go through each transaction and reverse it for the specific address
                foreach (Transaction transaction in cached_pool)
                {
                    if (address.Equals(transaction.from))
                    {
                        initialBalance += transaction.amount;
                    }
                    else if (address.Equals(transaction.to))
                    {
                        initialBalance -= transaction.amount;
                    }
                }
            }
            return initialBalance;
        }

        public static bool hasTransaction(string txid)
        {
            lock(transactions)
            {
                return transactions.Exists(x => x.id == txid);
            }
        }


    }
}
