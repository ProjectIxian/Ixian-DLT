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
                if(snapshot == 0)
                {
                    walletState.AddOrReplace(id, new Wallet(id, balance));
                    cachedChecksum = "";
                } else
                {
                    wsDeltas[snapshot].AddOrReplace(id, new Wallet(id, balance));
                    cachedDeltaChecksums[snapshot] = "";
                }
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
                } else if(snapshot > 0 && cachedDeltaChecksums[snapshot-1] != "")
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

        public WsChunk[] getWalletStateChunks(int chunk_size)
        {
            lock(stateLock)
            {
                ulong block_num = Node.blockChain.getLastBlockNum();
                int num_chunks = walletState.Count / chunk_size + 1;
                Logging.info(String.Format("Preparing {0} chunks of walletState. Total wallets: {1}", num_chunks, walletState.Count));
                WsChunk[] chunks = new WsChunk[num_chunks];
                for(int i=0;i<num_chunks;i++)
                {
                    chunks[i] = new WsChunk
                    {
                        blockNum = block_num,
                        chunkNum = i,
                        wallets = walletState.Skip(i * chunk_size).Take(chunk_size).Select(x => x.Value).ToArray()
                    };
                }
                Logging.info(String.Format("Prepared {0} WalletState chunks with {1} total wallets.",
                    num_chunks,
                    chunks.Sum(x => x.wallets.Count())));
                return chunks;
            }
        }

        public void setWalletChunk(Wallet[] wallets)
        {
            lock(stateLock)
            {
                if(numSnapshots>0)
                {
                    Logging.error("Attempted to apply a WalletState chunk, but snapshots exist!");
                    return;
                }
                foreach(Wallet w in wallets)
                {
                    walletState.AddOrReplace(w.id, w);
                }
            }
        }

        // Calculates the entire IXI supply based on the latest wallet state
        public IxiNumber calculateTotalSupply()
        {
            IxiNumber total = new IxiNumber();
            try
            {
                foreach (var item in walletState)
                {
                    Wallet wal = (Wallet)item.Value;
                    total = total + wal.balance;
                }
            }
            catch(Exception e)
            {
                Logging.error(string.Format("Exception calculating total supply: {0}", e.Message));
            }

            return total;
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
