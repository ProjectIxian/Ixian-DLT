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
        private readonly object stateLock = new object();
        private readonly Dictionary<string, Wallet> walletState = new Dictionary<string, Wallet>(); // The entire wallet list
        private string cachedChecksum = "";
        private List<Dictionary<string, Wallet>> wsDeltas = new List<Dictionary<string, Wallet>>();
        private readonly List<string> cachedDeltaChecksums = new List<string>();

        /* Size:
         * 10_000 wallets: ~510 KB
         * 100_000 wallets: ~5 MB
         * 10_000_000 wallets: ~510 MB (312 MB)
         * 
         * Keys only:
         * 10_000_000 addresses: 350 MB (176 MB)
         * 
         */

        public int numSnapshots { get => wsDeltas.Count; }
        public int numWallets { get => walletState.Count; }

        public WalletState()
        {
        }

        public WalletState(IEnumerable<Wallet> genesisState)
        {
            Logging.info(String.Format("Generating genesis WalletState with {0} wallets.", genesisState.Count()));
            foreach(Wallet w in genesisState)
            {
                Logging.info(String.Format("-> Genesis wallet ( {0} ) : {1}.", w.id, w.balance));
                walletState.Add(w.id, w);
            }
        }

        public void clear()
        {
            Logging.info("Clearing wallet state!!");
            lock(stateLock)
            {
                walletState.Clear();
                cachedChecksum = "";
                wsDeltas.Clear();
                cachedDeltaChecksums.Clear();

            }
        }

        public void revert()
        {
            lock (stateLock)
            {
                Logging.info(String.Format("Reverting {0} WalletState snapshots.", wsDeltas.Count));
                wsDeltas.Clear();
                cachedDeltaChecksums.Clear();
            }
        }

        public void commit()
        {
            lock(stateLock)
            {
                while (wsDeltas.Count > 0)
                {
                    Logging.info(String.Format("Committting WalletState snapshot ({0} remain). Wallets in snapshot: {1}", 
                        wsDeltas.Count, wsDeltas[0].Count));
                    foreach (var wallet in wsDeltas[0])
                    {
                        walletState.AddOrReplace(wallet.Key, wallet.Value);
                    }
                    wsDeltas.RemoveAt(0);
                }
                cachedDeltaChecksums.Clear();
                cachedChecksum = "";
            }
        }

        public IxiNumber getWalletBalance(string id, int snapshot = 0)
        {
            return getWallet(id, snapshot).balance;
        }

        private int translateSnapshotNum(int snapshot)
        {
            if (Math.Abs(snapshot) > numSnapshots)
            {
                return numSnapshots;
            }
            if (snapshot < 0)
            {
                return (numSnapshots + snapshot + 1);
            }
            return snapshot;
        }

        public Wallet getWallet(string id, int snapshot = 0)
        {
            lock (stateLock)
            {
                snapshot = translateSnapshotNum(snapshot);
                Wallet candidateWallet = new Wallet(id, (ulong)0);
                if (walletState.ContainsKey(id))
                {
                    candidateWallet = walletState[id];
                }
                for (int i = snapshot-1; i >= 0; i--)
                {
                    if (wsDeltas[i].ContainsKey(id))
                    {
                        candidateWallet = wsDeltas[i][id];
                        break;
                    }
                }
                return candidateWallet;
            }
        }

        public void setWalletBalance(string id, IxiNumber balance, int snapshot = 0)
        {
            lock(stateLock)
            {
                snapshot = translateSnapshotNum(snapshot);
                getWallet(id, snapshot).balance = balance;
                if(snapshot == 0)
                {
                    cachedChecksum = "";
                } else
                {
                    cachedDeltaChecksums[snapshot] = "";
                }
            }
        }

        public bool applyTransactions(Block b, bool createSnapshot = false)
        {
            lock(stateLock)
            {
                Logging.info(String.Format("Applying transactions from block #{0} ({1}). Snapshot: {2}",
                    b.blockNum, b.blockChecksum.Substring(4), createSnapshot));
                if(createSnapshot)
                {
                    wsDeltas.Add(new Dictionary<string, Wallet>());
                    cachedDeltaChecksums.Add("");
                    Logging.info(String.Format("WalletState snapshot {0} created.", wsDeltas.Count));
                }
                int targetSnapshot = numSnapshots;
                foreach(string txid in b.transactions)
                {
                    Transaction tx = TransactionPool.getTransaction(txid);
                    if(tx == null)
                    {
                        Logging.warn(String.Format("Unable to apply transaction {{ {0} }} from block #{1} ({2}), because it is not in the pool.", 
                            txid, b.blockNum, b.blockChecksum.Substring(0, 4)));
                        return false;
                    }
                    applyTransactionInternal(tx, targetSnapshot);
                }
                return true;
            }
        }

        private void applyTransactionInternal(Transaction tx, int targetSnapshot)
        {
            if(tx.amount == (long)0)
            {
                return;
            }
            int sourceSnapshot = targetSnapshot;
            Wallet sourceWallet = getWallet(tx.from, sourceSnapshot);
            Wallet destWallet = getWallet(tx.to, sourceSnapshot);
            if (tx.amount > sourceWallet.balance)
            {
                throw new Exception(String.Format("Attempted to withdraw more than wallet contains: wallet ( {0} ), balance: {1}, tx amount: {2}",
                    tx.from, sourceWallet.balance, tx.amount));
            }
            // TODO: TXFee
            sourceWallet.balance -= tx.amount;
            destWallet.balance += tx.amount;

            if (targetSnapshot > 0)
            {
                wsDeltas[targetSnapshot].AddOrReplace(tx.from, sourceWallet);
                wsDeltas[targetSnapshot].AddOrReplace(tx.to, destWallet);
                cachedDeltaChecksums[targetSnapshot] = "";
            }
            else
            {
                walletState.AddOrReplace(tx.from, sourceWallet);
                walletState.AddOrReplace(tx.to, destWallet);
                cachedChecksum = "";
            }
        }

        public string calculateWalletStateChecksum(int snapshot = 0)
        {
            lock (stateLock)
            {
                snapshot = translateSnapshotNum(snapshot);
                if(snapshot == 0 && cachedChecksum != "")
                {
                    return cachedChecksum;
                } else if(cachedDeltaChecksums[snapshot-1] != "")
                {
                    return cachedDeltaChecksums[snapshot - 1];
                }
                // TODO: This could get unwieldy above ~100M wallet addresses. We have to implement sharding by then.
                SortedSet<string> eligible_addresses = new SortedSet<string>(walletState.Keys);
                for (int i = 0; i < snapshot - 1; i++)
                {
                    foreach (string addr in wsDeltas[i].Keys)
                    {
                        eligible_addresses.Add(addr);
                    }
                }
                // TODO: This is probably not the optimal way to do this. Maybe we could do it by blocks to reduce calls to sha256
                // Note: addresses are fixed size
                string checksum = Crypto.sha256("IXIAN-DLT");
                foreach(string addr in eligible_addresses)
                {
                    string wallet_checksum = getWallet(addr, snapshot).calculateChecksum();
                    checksum = Crypto.sha256(checksum + wallet_checksum);
                }
                if(snapshot == 0)
                {
                    cachedChecksum = checksum;
                } else
                {
                    cachedDeltaChecksums[snapshot - 1] = checksum;
                }
                return checksum;
            }
        }

        // only returns 10 wallets from base state (no snapshotting)
        public Wallet[] debugGetWallets()
        {
            lock (stateLock)
            {
                return walletState.Take(10).Select(x => x.Value).ToArray();
            }
        }
    }
}
