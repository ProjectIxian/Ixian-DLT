using IXICore.Meta;
using IXICore.Network;
using IXICore.Utils;
using IXICore;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace DLT
{
    public class WsChunk
    {
        public ulong blockNum;
        public int chunkNum;
        public Wallet[] wallets;
    }

    public class WalletState
    {
        private readonly object stateLock = new object();
        public int version = 0;
        private readonly Dictionary<byte[], Wallet> walletState = new Dictionary<byte[], Wallet>(new ByteArrayComparer()); // The entire wallet list
        private byte[] cachedChecksum = null;
        private int cachedBlockVersion = 0;

        private List<WSJTransaction> wsjTransactions = new List<WSJTransaction>();
        private ulong txIDNumber;
        public bool inTransaction
        {
            get
            {
                return transactionDepth > 0;
            }
        }
        public int transactionDepth { get; private set; }



        private IxiNumber cachedTotalSupply = new IxiNumber(0);
        public int numWallets { get => walletState.Count; }

        public static string Addr2String(byte[] addr)
        {
            return Base58Check.Base58CheckEncoding.EncodePlain(addr);
        }

        public WalletState()
        {
            txIDNumber = 1;
            transactionDepth = 0;
        }

        public WalletState(IEnumerable<Wallet> genesisState)
        {
            Logging.info(String.Format("Generating genesis WalletState with {0} wallets.", genesisState.Count()));
            foreach(Wallet w in genesisState)
            {
                Logging.info(String.Format("-> Genesis wallet ( {0} ) : {1}.", Addr2String(w.id), w.balance));
                walletState.Add(w.id, w);
            }
        }

        public void clear()
        {
            Logging.info("Clearing wallet state!!");
            lock (stateLock)
            {
                walletState.Clear();
                cachedChecksum = null;
                cachedTotalSupply = new IxiNumber(0);
                wsjTransactions.Clear();
                txIDNumber = 1;
                transactionDepth = 0;
            }
        }

        // WSJ Stuff
        public ulong beginTransaction()
        {
            lock(stateLock)
            {
                txIDNumber += 1;
                var tx = new WSJTransaction(txIDNumber);
                transactionDepth += 1;
                wsjTransactions.Add(tx);
                return tx.wsjTxNumber;
            }
        }

        public void commitTransaction(ulong transaction_id)
        {
            lock(stateLock)
            {
                walkbackTransaction(transaction_id, true);
            }
        }

        public void revertTransaction(ulong transaction_id)
        {
            lock(stateLock)
            {
                walkbackTransaction(transaction_id, false);
            }
        }

        private void walkbackTransaction(ulong txid, bool commit)
        {
            String action = commit ? "commit" : "revert";
            if (transactionDepth <= 0)
            {
                Logging.warn(String.Format("Attempted to {0} WSJ transaction id {1}, but none were started.", action, txid));
                return;
            }
            if (!wsjTransactions.Exists(x => x.wsjTxNumber == txid))
            {
                Logging.warn(String.Format("Attempted to {0} WSJ transaction id {1}, but it does not exist.", action, txid));
                return;
            }
            while (wsjTransactions.Count > 0)
            {
                var tx = wsjTransactions.Last();
                if (!tx.apply())
                {
                    Logging.error(String.Format("WSJ transaction {0} for {1} produced an error!", action, tx.wsjTxNumber));
                }
                else
                {
                    Logging.info(String.Format("WSJ transaction {0} {1} successfuly.", tx.wsjTxNumber, action));
                }
                transactionDepth -= 1;
                wsjTransactions.RemoveAt(wsjTransactions.Count - 1);
                if (transactionDepth == 0)
                {
                    Logging.warn(String.Format("Attempted to {0} WSJ transaction {1} which was already previously done.", action, txid));
                    break;
                }
                if (tx.wsjTxNumber == txid)
                {
                    Logging.info(String.Format("WSJ transaction {0} -> {1} successful.", txid, action));
                    break;
                }
            }
        }

        public IxiNumber getWalletBalance(byte[] id)
        {
            return getWallet(id).balance;
        }

        public Wallet getWallet(byte[] id)
        {
            lock (stateLock)
            {
                Wallet candidateWallet = new Wallet(id, (ulong)0);
                if (walletState.ContainsKey(id))
                {
                    // copy
                    candidateWallet = new Wallet(walletState[id]);
                }
                return candidateWallet;
            }
        }

        #region Wallet Manipulation Methods - public use
        // Sets the wallet balance for a specified wallet
        public void setWalletBalance(byte[] id, IxiNumber balance)
        {
            lock (stateLock)
            {
                if(inTransaction)
                {
                    Wallet wallet = getWallet(id);

                    IxiNumber old_balance = wallet.balance;
                    IxiNumber delta = balance - old_balance;

                    var change = new WSJE_Balance(wallet.id, delta);
                    wsjTransactions.Last().addChange(change);
                } else
                {
                    Logging.warn(String.Format("Set wallet {0} to balance {1} -> creating implicit transaction.", Addr2String(id), balance.ToString()));
                    ulong txid = beginTransaction();
                    setWalletBalance(id, balance);
                    commitTransaction(txid);
                }
            }
        }

        // Sets the wallet public key for a specified wallet
        public void setWalletPublicKey(byte[] id, byte[] public_key)
        {
            lock (stateLock)
            {
                if (getWallet(id).publicKey != null)
                {
                    Logging.warn(String.Format("Wallet {0} attempted to set public key, but it is already set.", Addr2String(id)));
                    return;
                }
                if (inTransaction)
                {
                    var change = new WSJE_Pubkey(id, public_key);
                    wsjTransactions.Last().addChange(change);
                }
                else
                {
                    Logging.warn(String.Format("Set wallet {0} public key -> creating implicit transaction.", Addr2String(id)));
                    ulong txid = beginTransaction();
                    setWalletPublicKey(id, public_key);
                    commitTransaction(txid);
                }
            }
        }

        public void addWalletAllowedSigner(byte[] id, byte[] signer)
        {
            lock(stateLock)
            {
                if(getWallet(id).isValidSigner(signer))
                {
                    Logging.warn(String.Format("Wallet {0} attempted to add signer {1}, but it is already in the allowed signer list.", Addr2String(id), Addr2String(signer)));
                    return;
                }
                if(inTransaction)
                {
                    var change = new WSJE_AllowedSigner(id, true, signer);
                    wsjTransactions.Last().addChange(change);
                } else
                {
                    Logging.warn(String.Format("Wallet {0} add allowed signer {1} -> creating implicit transaction.", Addr2String(id), Addr2String(signer)));
                    ulong txid = beginTransaction();
                    addWalletAllowedSigner(id, signer);
                    commitTransaction(txid);
                }
            }
        }

        public void delWalletAllowedSigner(byte[] id, byte[] signer)
        {
            lock (stateLock)
            {
                if (!getWallet(id).isValidSigner(signer))
                {
                    Logging.warn(String.Format("Wallet {0} attempted to delete signer {1}, but it is not in the allowed signer list.", Addr2String(id), Addr2String(signer)));
                    return;
                }
                if (inTransaction)
                {
                    var change = new WSJE_AllowedSigner(id, false, signer);
                    wsjTransactions.Last().addChange(change);
                }
                else
                {
                    Logging.warn(String.Format("Wallet {0} delete allowed signer {1} -> creating implicit transaction.", Addr2String(id), Addr2String(signer)));
                    ulong txid = beginTransaction();
                    delWalletAllowedSigner(id, signer);
                    commitTransaction(txid);
                }
            }
        }

        public void setWalletRequiredSignatures(byte[] id, byte req_sigs)
        {
            Wallet w = getWallet(id);
            if(w.requiredSigs == req_sigs)
            {
                Logging.warn(String.Format("Wallet {0} attempted to set required signatures to {1}, but it is already at {1}.", Addr2String(id), req_sigs));
                return;
            }
            if (inTransaction)
            {
                int delta = req_sigs - w.requiredSigs;
                var change = new WSJE_Signers(id, delta);
                wsjTransactions.Last().addChange(change);
            }
            else
            {
                Logging.warn(String.Format("Wallet {0} set required signers to {1} -> creating implicit transaction.", Addr2String(id), req_sigs));
                ulong txid = beginTransaction();
                setWalletRequiredSignatures(id, req_sigs);
                commitTransaction(txid);
            }
        }

        public void setWalletUserData(byte[] id, byte[] user_data)
        {
            if (inTransaction)
            {
                Wallet w = getWallet(id);
                var change = new WSJE_Data(id, user_data, w.data);
                wsjTransactions.Last().addChange(change);
            }
            else
            {
                Logging.warn(String.Format("Wallet {0} set user data -> creating implicit transaction.", Addr2String(id)));
                ulong txid = beginTransaction();
                setWalletUserData(id, user_data);
                commitTransaction(txid);
            }
        }
        #endregion

        #region Internal (WSJ) Wallet manipulation methods
        // this is called only by WSJ
        public bool setWalletBalanceInternal(byte[] id, IxiNumber delta)
        {
            lock (stateLock)
            {
                Wallet w = getWallet(id);
                IxiNumber new_balance = w.balance + delta;
                if (new_balance < 0)
                {
                    Logging.error(String.Format("WSJE_Balance application would result in a negative value! Wallet: {0}, balance: {1}, delta: {2}", Addr2String(id), w.balance, delta));
                    return false;
                }
                w.balance = new_balance;

                // Send balance update notification to the network
                using (MemoryStream mw = new MemoryStream())
                {
                    using (BinaryWriter writerw = new BinaryWriter(mw))
                    {
                        // Send the address
                        writerw.Write(id.Length);
                        writerw.Write(id);
                        // Send the balance
                        writerw.Write(w.balance.ToString());
                        // Send the block height for this balance
                        writerw.Write(IxianHandler.getLastBlockHeight());
#if TRACE_MEMSTREAM_SIZES
                        Logging.info(String.Format("WalletState::setWalletBalance: {0}", mw.Length));
#endif

                        // Send balance message to all connected clients
                        CoreProtocolMessage.broadcastEventDataMessage(NetworkEvents.Type.balance, id, ProtocolMessageCode.balance, mw.ToArray(), id, null);
                    }
                }

                if (cachedBlockVersion >= 5 && w.balance.getAmount() == 0 && w.type == WalletType.Normal)
                {
                    walletState.Remove(id);
                }
                else
                {
                    walletState.AddOrReplace(id, w);
                }
                cachedChecksum = null;
                cachedTotalSupply = new IxiNumber(0);
                return true;
            }
        }

        public bool setWalletPublicKeyInternal(byte[] id, byte[] public_key)
        {
            lock(stateLock)
            {
                Wallet w = getWallet(id);
                if(w.publicKey != null && public_key != null)
                {
                    Logging.error(String.Format("WSJE_PublicKey attempted to set public key on wallet {0} which already has a public key.", Addr2String(id)));
                } else if(w.publicKey == null && public_key == null)
                {
                    Logging.error(String.Format("WSJE_PublicKey attempted to clear public key on wallet {0} which doesn't have a public key.", Addr2String(id)));
                    return false;
                }
                w.publicKey = public_key;

                walletState.AddOrReplace(id, w);
                cachedChecksum = null;
                return true;
            }
        }

        public bool addWalletAllowedSignerInternal(byte[] id, byte[] signer)
        {
            lock(stateLock)
            {
                Wallet w = getWallet(id);
                if (w.isValidSigner(signer))
                {
                    Logging.error(String.Format("WSJE_AllowedSigner cannot add duplicate signer! Wallet: {0}, signer: {1}", Addr2String(id), signer));
                    return false;
                }
                w.addValidSigner(signer);
                w.type = WalletType.Multisig;
                walletState.AddOrReplace(id, w);
                cachedChecksum = null;
                return true;
            }
        }

        public bool delWalletAllowedSignerInternal(byte[] id, byte[] signer)
        {
            lock(stateLock)
            {
                Wallet w = getWallet(id);
                if (!w.isValidSigner(signer))
                {
                    Logging.error(String.Format("WSJE_AllowedSigner cannot remove nonexistant signer! Wallet: {0}, signer: {1}", Addr2String(id), signer));
                    return false;
                }
                if(w.countAllowedSigners <= w.requiredSigs)
                {
                    Logging.error(String.Format("WSJE_AllowedSigner removing signer would make the wallet inoperable. Wallet: {0}, Allowed signers(before): {1}, Required Signatures: {2}",
                        Addr2String(id), w.countAllowedSigners, w.requiredSigs));
                    return false;
                }
                w.delValidSigner(signer);
                if (w.countAllowedSigners == 0)
                {
                    w.type = WalletType.Normal;
                }
                walletState.AddOrReplace(id, w);
                cachedChecksum = null;
                return true;
            }
        }

        public bool setWalletRequiredSignaturesInternal(byte[] id, int delta)
        {
            lock(stateLock)
            {
                Wallet w = getWallet(id);
                if(w.type != WalletType.Multisig)
                {
                    Logging.error(String.Format("WSJE_Signers attempted apply on a non-multisig wallet {0}.", Addr2String(id)));
                }
                int new_sigs = w.requiredSigs + delta;
                if (new_sigs > w.countAllowedSigners + 1)
                {
                    Logging.error(String.Format("WSJE_Signers application would result in an invalid wallet! Wallet: {0}, validSigners: {1}, reqSigs: {2}, delta: {3}", 
                        Addr2String(id), 
                        w.countAllowedSigners, 
                        w.requiredSigs, 
                        delta));
                    return false;
                }
                w.requiredSigs = (byte)new_sigs;
                walletState.AddOrReplace(id, w);
                cachedChecksum = null;
                return true;
            }
        }
        public bool setWalletUserDataInternal(byte[] id, byte[] new_data, byte[] old_data)
        {
            lock(stateLock)
            {
                Wallet w = getWallet(id);
                if (!w.data.SequenceEqual(old_data))
                {
                    Logging.error(String.Format("WSJE_Data unable to apply - old data does not match! Wallet: {0}", Addr2String(id)));
                    return false;
                }
                w.data = new_data;
                walletState.AddOrReplace(id, w);
                cachedChecksum = null;
                return true;
            }
        }
        #endregion

        public void setCachedBlockVersion(int block_version)
        {
            // edge case for first block of block_version 3
            if (block_version == 3 && IxianHandler.getLastBlockVersion() == 2)
            {
                block_version = 2;
            }

            if (cachedBlockVersion != block_version)
            {
                cachedChecksum = null;
                cachedBlockVersion = block_version;
            }
        }

        public byte[] calculateWalletStateChecksum()
        {
            lock (stateLock)
            {
                if (cachedChecksum != null)
                {
                    return cachedChecksum;
                }

                // TODO: This could get unwieldy above ~100M wallet addresses. We have to implement sharding by then.
                SortedSet<byte[]> eligible_addresses = null;
                eligible_addresses = new SortedSet<byte[]>(walletState.Keys, new ByteArrayComparer());

                byte[] checksum = null;
                if (cachedBlockVersion <= 2)
                {
                    checksum = Crypto.sha512quTrunc(Encoding.UTF8.GetBytes("IXIAN-DLT" + version));
                }else
                {
                    checksum = Crypto.sha512sqTrunc(Encoding.UTF8.GetBytes("IXIAN-DLT" + version), 0, 0, 64);
                }

                // TODO: This is probably not the optimal way to do this. Maybe we could do it by blocks to reduce calls to sha256
                // Note: addresses are not fixed size
                foreach (byte[] addr in eligible_addresses)
                {
                    byte[] wallet_checksum = getWallet(addr).calculateChecksum(cachedBlockVersion);
                    if (cachedBlockVersion <= 2)
                    {
                        checksum = Crypto.sha512quTrunc(Encoding.UTF8.GetBytes(Crypto.hashToString(checksum) + Crypto.hashToString(wallet_checksum)));
                    }else
                    {
                        List<byte> tmp_hash = checksum.ToList();
                        tmp_hash.AddRange(wallet_checksum);
                        checksum = Crypto.sha512sqTrunc(tmp_hash.ToArray(), 0, 0, 64);
                    }
                }

                cachedChecksum = checksum;
                return checksum;
            }
        }

        public WsChunk[] getWalletStateChunks(int chunk_size, ulong block_num)
        {
            lock(stateLock)
            {
                if(chunk_size == 0)
                {
                    chunk_size = walletState.Count;
                }
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
            lock (stateLock)
            {
                foreach (Wallet w in wallets)
                {
                    if (w != null)
                    {
                        walletState.AddOrReplace(w.id, w);
                    }
                }
                cachedChecksum = null;
                cachedTotalSupply = new IxiNumber(0);
            }
        }

        // Calculates the entire IXI supply based on the latest wallet state
        public IxiNumber calculateTotalSupply()
        {
            IxiNumber total = new IxiNumber();
            lock (stateLock)
            {
                if (cachedTotalSupply != (long)0)
                {
                    return cachedTotalSupply;
                }
                try
                {
                    foreach (var item in walletState)
                    {
                        Wallet wal = (Wallet)item.Value;
                        total = total + wal.balance;
                    }
                    cachedTotalSupply = total;
                }
                catch (Exception e)
                {
                    Logging.error(string.Format("Exception calculating total supply: {0}", e.Message));
                }
            }
            return total;
        }

        // only returns 50 wallets from base state (no snapshotting)
        public Wallet[] debugGetWallets()
        {
            lock (stateLock)
            {
                return walletState.Take(50).Select(x => x.Value).ToArray();
            }
        }

    }
}
