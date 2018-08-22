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
        // Estimate transaction pool memory size for ~3000 blocks @ ~3k tx per block
        // Current TX: ~200 B.
        //  3k transactions: ~600 KB
        //  3k blocks: 1.8 GB (Acceptable?)
        // Shrunken TX: 97B (See Transaction.cs)
        //  3k transactions: ~300 KB
        //  3k blocks: 850 MB
        //

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
            if(Node.blockChain.getLastBlockNum() < 10)
            {
                Logging.warn(String.Format("Ignoring transaction before block 10."));
                return false;
            }
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

                // Verify if the transaction contains the minimum fee
                if(transaction.amount < Config.transactionPrice)
                {
                    // Prevent transactions that can't pay the minimum fee
                    Logging.warn(String.Format("Transaction amount does not cover fee for {{ {0} }}.", transaction.id));
                    return false;
                }

                // Verify the transaction against the wallet state
                // If the balance after the transaction is negative, do not add it.
                IxiNumber fromBalance = Node.walletState.getWalletBalance(transaction.from);
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

                // Also add the transaction to storage
                TransactionStorage.addTransaction(transaction);
            }

            // Broadcast this transaction to the network
            ProtocolMessage.broadcastProtocolMessage(ProtocolMessageCode.newTransaction, transaction.getBytes());

            return true;
        }

        public static Transaction getTransaction(string txid)
        {
            lock(transactions)
            {
                //Logging.info(String.Format("Looking for transaction {{ {0} }}. Pool has {1}.", txid, transactions.Count));
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

        public static Transaction[] getUnappliedTransactions()
        {
            lock(transactions)
            {
                return transactions.Where(x => x.applied == false).ToArray();
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

                        // Also update the transaction to storage
                        TransactionStorage.updateTransaction(transaction);

                        return true;
                    }
                }
            }

            return false;
        }


        // This applies all the transactions from a block to the actual walletstate.
        // It removes the corresponding transactions as well from the pool.
        public static bool applyTransactionsFromBlock(Block block)
        {
            if (block == null)
            {
                return true;
            }

            lock (transactions)
            {
                List<Transaction> tx_to_apply = new List<Transaction>();
                foreach(string txid in block.transactions)
                {
                    Transaction tx = getTransaction(txid);
                    if(tx == null)
                    {
                        Logging.error(String.Format("Attempted to apply transactions from block #{0} ({1}), but transaction {{ {2} }} was missing.", 
                            block.blockNum, block.blockChecksum, txid));
                        return false;
                    }

                    if (tx.amount == 0)
                    {
                        continue;
                    }
                    //Logging.info(String.Format("{{ {0} }}->Applied: {1}.", txid, tx.applied));
                    if(tx.applied == true)
                    {
                        return false;
                    }

                    // Calculate the transaction amount without fee
                    IxiNumber txAmountWithoutFee = tx.amount - Config.transactionPrice;

                    if (txAmountWithoutFee < (long) 0)
                    {
                        Logging.error(String.Format("Transaction {{ {0} }} cannot pay minimum fee", txid));
                        continue;
                    }

                    IxiNumber source_balance_before = Node.walletState.getWalletBalance(tx.from);
                    IxiNumber dest_balance_before = Node.walletState.getWalletBalance(tx.to);

                    // Withdraw the full amount, including fee
                    IxiNumber source_balance_after = source_balance_before - tx.amount;
                    if(source_balance_after < (long)0)
                    {
                        Logging.warn(String.Format("Transaction {{ {0} }} in block #{1} ({2}) would take wallet {3} below zero.",
                            txid, block.blockNum, block.lastBlockChecksum, tx.from));
                        return false;
                    }

                    // Deposit the amount without fee, as the fee is distributed by the network a few blocks later
                    IxiNumber dest_balance_after = dest_balance_before + txAmountWithoutFee;

                    // Update the walletstate
                    Node.walletState.setWalletBalance(tx.from, source_balance_after);
                    Node.walletState.setWalletBalance(tx.to, dest_balance_after);
                    tx.applied = true;
                }
            }
            return true;
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
            // TODO: After redaction, this is no longer viable (will have to change logic to on longer depend on this function)
            IxiNumber initialBalance = finalBalance;
            lock (transactions)
            {
                // Go through each transaction and reverse it for the specific address
                foreach (Transaction transaction in transactions)
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
