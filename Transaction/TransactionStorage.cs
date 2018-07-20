using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DLT
{
    class TransactionStorage
    {
        public static List<Transaction> transactions = new List<Transaction> { };

        public TransactionStorage()
        {
            // Todo: read transactions from local file storage

        }

        // Retrieve a transaction from local storage
        // Todo: if transaction not found in local storage, send a network-wide request
        public static Transaction getTransaction(string txid)
        {
            lock (transactions)
            {
                foreach (Transaction tx in transactions)
                {
                    if (txid.Equals(tx.id, StringComparison.Ordinal))
                        return tx;
                }
            }
            return null;
        }

        // Add a transaction to local storage
        public static bool addTransaction(Transaction transaction)
        {
            lock (transactions)
            {
                // Store the transaction in memory
                transactions.Add(transaction);

                // Storage the transaction in the database
                //Meta.Storage.insertTransaction(transaction);
            }
            return true;
        }

    }
}
