// Copyright (C) 2017-2020 Ixian OU
// This file is part of Ixian DLT - www.github.com/ProjectIxian/Ixian-DLT
//
// Ixian DLT is free software: you can redistribute it and/or modify
// it under the terms of the MIT License as published
// by the Open Source Initiative.
//
// Ixian DLT is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// MIT License for more details.

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
        private readonly Dictionary<byte[], Wallet> walletState = new Dictionary<byte[], Wallet>(new ByteArrayComparer()); // The entire wallet list
        private byte[] cachedChecksum = null;
        private int cachedBlockVersion = 0;

        private WSJTransaction wsjTransaction = null;
        private List<WSJTransaction> processedWsjTransactions = new List<WSJTransaction>(); // keep last 6 WSJ states for block reorg purposes
        public bool inTransaction
        {
            get; private set;
        }

        private IxiNumber cachedTotalSupply = new IxiNumber(0);
        public int numWallets { get => walletState.Count; }

        public WalletState()
        {
        }

        public WalletState(IEnumerable<Wallet> genesisState)
        {
            Logging.info("Generating genesis WalletState with {0} wallets.", genesisState.Count());
            foreach(Wallet w in genesisState)
            {
                Logging.info("-> Genesis wallet ( {0} ) : {1}.", w.id.ToString(), w.balance);
                walletState.Add(w.id.addressNoChecksum, w);
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
                wsjTransaction = null;
                inTransaction = false;
                processedWsjTransactions.Clear();
            }
        }

        // WSJ Stuff
        public bool beginTransaction(ulong block_num, bool in_transaction = true)
        {
            lock(stateLock)
            {
                if(wsjTransaction != null)
                {
                    // Transaction is already open
                    return false;
                }
                var tx = new WSJTransaction(block_num);
                wsjTransaction = tx;
                inTransaction = in_transaction;
                return true;
            }
        }

        public void commitTransaction(ulong transaction_id)
        {
            if(transaction_id == 0)
            {
                return;
            }
            // data has already been changed in the WalletState directly
            lock (stateLock)
            {
                processedWsjTransactions.Add(wsjTransaction);
                if(processedWsjTransactions.Count > 10)
                {
                    processedWsjTransactions.RemoveAt(0);
                }
                wsjTransaction = null;
                inTransaction = false;
            }
        }

        public bool canRevertTransaction(ulong transaction_id)
        {
            if (transaction_id == 0)
            {
                return false;
            }
            lock (stateLock)
            {
                if (wsjTransaction != null && wsjTransaction.wsjTxNumber == transaction_id)
                {
                    return true;
                }
                else
                {
                    WSJTransaction wsjt = processedWsjTransactions.Find(x => x.wsjTxNumber == transaction_id);
                    if (wsjt == null)
                    {
                        return false;
                    }
                    return true;
                }
            }
        }

        public bool revertTransaction(ulong transaction_id)
        {
            if(transaction_id == 0)
            {
                return false;
            }
            lock(stateLock)
            {
                bool result = false;
                if(wsjTransaction != null && wsjTransaction.wsjTxNumber == transaction_id)
                {
                    WSJTransaction wsjt = wsjTransaction;
                    result = wsjt.revert();
                    wsjTransaction = null;
                    inTransaction = false;
                }
                else
                {
                    WSJTransaction wsjt = processedWsjTransactions.Find(x => x.wsjTxNumber == transaction_id);
                    if(wsjt == null)
                    {
                        return false;
                    }
                    result = wsjt.revert();
                    processedWsjTransactions.Remove(wsjt);
                }
                cachedTotalSupply = new IxiNumber(0);
                return result;
            }
        }

        private IEnumerable<Wallet> getAlteredWalletsSinceWSJTX(ulong transaction_id, int block_version)
        {
            if (wsjTransaction != null && wsjTransaction.wsjTxNumber == transaction_id)
            {
                WSJTransaction wsjt = wsjTransaction;
                return wsjt.getAffectedWallets(block_version);
            }
            else
            {
                WSJTransaction wsjt = processedWsjTransactions.Find(x => x.wsjTxNumber == transaction_id);
                if (wsjt == null)
                {
                    return null;
                }
                return wsjt.getAffectedWallets(block_version);
            }
        }

        public IxiNumber getWalletBalance(Address id)
        {
            return getWallet(id).balance;
        }

        public Wallet getWallet(Address id, bool return_null_on_missing = false)
        {
            lock (stateLock)
            {
                Wallet candidateWallet = new Wallet(id, (ulong)0);
                if (walletState.ContainsKey(id.addressNoChecksum))
                {
                    // copy
                    candidateWallet = new Wallet(walletState[id.addressNoChecksum]);
                }else if(return_null_on_missing)
                {
                    return null;
                }
                
                return candidateWallet;
            }
        }

        #region Wallet Manipulation Methods - public use
        // Sets the wallet balance for a specified wallet
        public void setWalletBalance(Address id, IxiNumber new_balance)
        {
            lock (stateLock)
            {
                Wallet wallet = getWallet(id, true);
                if(wallet == null)
                {
                    wallet = createWallet(id);
                }
                var change = new WSJE_Balance(wallet.id, wallet.balance, new_balance);
                change.apply();
                if (wsjTransaction != null)
                {
                    wsjTransaction.addChange(change);
                }
            }
        }

        // Sets the wallet public key for a specified wallet
        public void setWalletPublicKey(Address id, byte[] public_key)
        {
            lock (stateLock)
            {
                Wallet wallet = getWallet(id, true);
                if (wallet == null)
                {
                    wallet = createWallet(id);
                }
                if (wallet.publicKey != null)
                {
                    Logging.warn("Wallet {0} attempted to set public key, but it is already set.", id.ToString());
                    return;
                }
                if (public_key == null)
                {
                    // this would be a non-op (current pubkey = null, new pubkey = null)
                    return;
                }
                var change = new WSJE_Pubkey(id, public_key);
                change.apply();
                if (wsjTransaction != null)
                {
                    wsjTransaction.addChange(change);
                }
            }
        }

        public void addWalletAllowedSigner(Address id, Address signer)
        {
            lock (stateLock)
            {
                Wallet w = getWallet(id);
                if (w.isValidSigner(signer))
                {
                    Logging.warn("Wallet {0} attempted to add signer {1}, but it is already in the allowed signer list.", id.ToString(), signer.ToString());
                    return;
                }
                if(w.countAllowedSigners > 250)
                {
                    Logging.warn("Wallet {0} attempted to add signer {1}, but it already has maximum allowed signers.", id.ToString(), signer.ToString());
                    return;
                }
                var change = new WSJE_AllowedSigner(id, true, signer);
                change.apply();
                if (wsjTransaction != null)
                {
                    wsjTransaction.addChange(change);
                }
            }
        }

        public void delWalletAllowedSigner(Address id, Address signer, bool adjust_req_signers)
        {
            lock (stateLock)
            {
                if (!getWallet(id).isValidSigner(signer))
                {
                    Logging.warn("Wallet {0} attempted to delete signer {1}, but it is not in the allowed signer list.", id.ToString(), signer.ToString());
                    return;
                }
                var change = new WSJE_AllowedSigner(id, false, signer, adjust_req_signers);
                change.apply();
                if (wsjTransaction != null)
                {
                    wsjTransaction.addChange(change);
                }
            }
        }

        public void setWalletRequiredSignatures(Address id, byte req_sigs)
        {
            Wallet w = getWallet(id);
            if (w.requiredSigs == req_sigs)
            {
                Logging.warn("Wallet {0} attempted to set required signatures to {1}, but it is already at {1}.", id.ToString(), req_sigs);
                return;
            }
            var change = new WSJE_Signers(id, w.requiredSigs, req_sigs);
            change.apply();
            if (wsjTransaction != null)
            {
                wsjTransaction.addChange(change);
            }
        }

        public void setWalletUserData(Address id, byte[] new_data)
        {
            Wallet w = getWallet(id);
            var change = new WSJE_Data(id, w.data, new_data);
            change.apply();
            if (wsjTransaction != null)
            {
                wsjTransaction.addChange(change);
            }
        }

        public Wallet createWallet(Address id)
        {
            Wallet w = getWallet(id, true);
            if(w != null)
            {
                Logging.warn("Wallet {0} is already created, can't create.", id.ToString());
                return null;
            }
            var change = new WSJE_Create(id);
            change.apply();
            if (wsjTransaction != null)
            {
                wsjTransaction.addChange(change);
            }
            return new Wallet(id, 0);
        }

        public void removeWallet(Address id)
        {
            Wallet w = getWallet(id, true);
            if (w == null)
            {
                Logging.warn("Wallet {0} doesn't exist, can't remove.", id.ToString());
                return;
            }
            var change = new WSJE_Destroy(id, w);
            change.apply();
            if (wsjTransaction != null)
            {
                wsjTransaction.addChange(change);
            }
        }
        #endregion

        #region Internal (WSJ) Wallet manipulation methods
        // this is called only by WSJ
        public bool setWalletBalanceInternal(Address id, IxiNumber balance, bool is_reverting = false)
        {
            lock (stateLock)
            {
                Wallet w = getWallet(id);
                if (balance < 0)
                {
                    Logging.error("WSJE_Balance application would result in a negative value! Wallet: {0}, current balance: {1}, new balance: {2}", id.ToString(), w.balance, balance);
                    return false;
                }
                w.balance = balance;

                // Send balance update notification to the network
                // TODO: WSJ: This should be in a different place
                if (!is_reverting && !inTransaction)
                {
                    using (MemoryStream mw = new MemoryStream())
                    {
                        using (BinaryWriter writerw = new BinaryWriter(mw))
                        {
                            // Send the address
                            writerw.Write(id.addressNoChecksum.Length);
                            writerw.Write(id.addressNoChecksum);
                            // Send the balance
                            writerw.Write(balance.ToString());

                            Block tmp_block = IxianHandler.getLastBlock();
                            if (tmp_block != null)
                            {
                                // Send the block height for this balance
                                writerw.Write(tmp_block.blockNum);
                                // Send the block checksum for this balance
                                writerw.Write(tmp_block.blockChecksum.Length);
                                writerw.Write(tmp_block.blockChecksum);
                            }
                            else // genesis edge case
                            {
                                // Send the block height for this balance
                                writerw.Write((ulong)1);
                                // Send the block checksum for this balance
                                writerw.Write(0); // TODO TODO fill out genesis checksum
                            }
#if TRACE_MEMSTREAM_SIZES
                            Logging.info(String.Format("WalletState::setWalletBalance: {0}", mw.Length));
#endif

                            // Send balance message to all subscribed clients
                            CoreProtocolMessage.broadcastEventDataMessage(NetworkEvents.Type.balance, id.addressNoChecksum, ProtocolMessageCode.balance, mw.ToArray(), id.addressNoChecksum, null);
                        }
                    }
                }

                if (w.isEmptyWallet() && !is_reverting
                    && ((!inTransaction && cachedBlockVersion >= BlockVer.v5) || cachedBlockVersion >= BlockVer.v8))
                {
                    Logging.info("Normal Wallet {0} reaches balance zero and is removed. (Not in WSJ transaction.)", id.ToString());
                    removeWallet(id);
                }else
                {
                    walletState.AddOrReplace(id.addressNoChecksum, w);
                }
                cachedChecksum = null;
                cachedTotalSupply = new IxiNumber(0);
                return true;
            }
        }

        public bool setWalletPublicKeyInternal(Address id, byte[] public_key, bool is_reverting = false)
        {
            lock (stateLock)
            {
                Wallet w = getWallet(id);
                if(w.isEmptyWallet() && public_key == null)
                {
                    // rare edge case: this is a wallet which was recently updated to balance 0 and thus removed from walletstate
                    // we reach this point because processing transactions updates wallet public keys first and then sets their balance,
                    // and reverting the WSJ causes the wallet to be deleted when its balance is reset to 0, then it tries to remove public key on it
                    // Note: getWallet() will return an empty wallet if the id does not exist in its dictionary
                    if ((!inTransaction && !is_reverting && cachedBlockVersion >= BlockVer.v5) || cachedBlockVersion >= BlockVer.v8)
                    {
                        Logging.info("Normal Wallet {0} reaches balance zero and is removed. (Not in WSJ transaction.)", id.ToString());
                        removeWallet(id);
                    }
                    cachedChecksum = null;
                    return true;
                }
                if(w.publicKey != null && public_key != null)
                {
                    Logging.error("WSJE_PublicKey attempted to set public key on wallet {0} which already has a public key.", id.ToString());
                } else if(w.publicKey == null && public_key == null)
                {
                    Logging.error("WSJE_PublicKey attempted to clear public key on wallet {0} which doesn't have a public key.", id.ToString());
                    return false;
                }
                if((public_key != null && public_key.Length < 50) || DLT.Meta.Config.fullBlockLogging)
                {
                    Logging.info("WSJE_PublicKey: Setting public key ({0}) for wallet {1}. (Transaction: {2})", public_key != null?public_key.Length+"B":"null", id.ToString(), inTransaction);
                }
                w.publicKey = public_key;

                walletState.AddOrReplace(id.addressNoChecksum, w);
                cachedChecksum = null;
                return true;
            }
        }

        public bool addWalletAllowedSignerInternal(Address id, Address signer, bool adjust_req_signers)
        {
            lock(stateLock)
            {
                Wallet w = getWallet(id);
                if (w.isValidSigner(signer))
                {
                    Logging.error("WSJE_AllowedSigner cannot add duplicate signer! Wallet: {0}, signer: {1}", id.ToString(), signer);
                    return false;
                }
                w.addValidSigner(signer);
                if(adjust_req_signers)
                {
                    Logging.info("WSJE_AllowedSigner: adjusting required signatures {0} -> {1} as part of reverting a delete signer operation on wallet {2}",
                        w.requiredSigs, w.requiredSigs + 1, id.ToString());
                    w.requiredSigs += 1;
                }
                w.type = WalletType.Multisig;
                walletState.AddOrReplace(id.addressNoChecksum, w);
                cachedChecksum = null;
                return true;
            }
        }

        public bool delWalletAllowedSignerInternal(Address id, Address signer, bool adjust_req_signers, bool is_reverting = false)
        {
            lock(stateLock)
            {
                Wallet w = getWallet(id);
                if (!w.isValidSigner(signer))
                {
                    Logging.error("WSJE_AllowedSigner cannot remove nonexistant signer! Wallet: {0}, signer: {1}", id.ToString(), signer);
                    return false;
                }
                if (adjust_req_signers)
                {
                    Logging.info("WSJE_AllowedSigner: adjusting required signatures {0} -> {1} as part of a delete signer operation on wallet {2}",
                        w.requiredSigs, w.requiredSigs - 1, id.ToString());
                    w.requiredSigs -= 1;
                }
                else if(w.countAllowedSigners < w.requiredSigs) // at this point the sig adjustment has alredy been applied
                {
                    Logging.error("WSJE_AllowedSigner removing signer would make the wallet inoperable. Wallet: {0}, Allowed signers(before): {1}, Required Signatures: {2}",
                        id.ToString(), w.countAllowedSigners, w.requiredSigs);
                    return false;
                }
                w.delValidSigner(signer);
                if (w.countAllowedSigners == 0)
                {
                    w.type = WalletType.Normal;
                    w.allowedSigners = null;
                }

                if (w.isEmptyWallet() && !is_reverting
                    && ((!inTransaction && cachedBlockVersion >= BlockVer.v5) || cachedBlockVersion >= BlockVer.v8))
                {
                    Logging.info("MS->Normal Wallet {0} reaches balance zero and is removed. (Not in WSJ transaction.)", id.ToString());
                    removeWallet(id);
                }else
                {
                    walletState.AddOrReplace(id.addressNoChecksum, w);
                }
                cachedChecksum = null;
                return true;
            }
        }

        public bool setWalletRequiredSignaturesInternal(Address id, int new_sigs)
        {
            lock(stateLock)
            {
                Wallet w = getWallet(id);
                if(w.type != WalletType.Multisig)
                {
                    Logging.error("WSJE_Signers attempted apply on a non-multisig wallet {0}.", id.ToString());
                }
                if (new_sigs > w.countAllowedSigners + 1)
                {
                    Logging.error("WSJE_Signers application would result in an invalid wallet! Wallet: {0}, validSigners: {1}, reqSigs: {2}, new sigs: {3}", 
                        id.ToString(), 
                        w.countAllowedSigners, 
                        w.requiredSigs, 
                        new_sigs);
                    return false;
                }
                w.requiredSigs = (byte)new_sigs;
                walletState.AddOrReplace(id.addressNoChecksum, w);
                cachedChecksum = null;
                return true;
            }
        }
        public bool setWalletUserDataInternal(Address id, byte[] new_data, byte[] old_data)
        {
            lock(stateLock)
            {
                Wallet w = getWallet(id);
                if (!w.data.SequenceEqual(old_data))
                {
                    Logging.error("WSJE_Data unable to apply - old data does not match! Wallet: {0}", id.ToString());
                    return false;
                }
                w.data = new_data;
                walletState.AddOrReplace(id.addressNoChecksum, w);
                cachedChecksum = null;
                return true;
            }
        }
        public bool removeWalletInternal(Address id)
        {
            lock (stateLock)
            {
                Wallet w = getWallet(id, true);
                if (w != null)
                {
                    walletState.Remove(id.addressNoChecksum);
                }
                cachedChecksum = null;
                cachedTotalSupply = 0;
                return true;
            }
        }
        public bool setWalletInternal(Address id, Wallet w)
        {
            lock (stateLock)
            {
                walletState.AddOrReplace(id.addressNoChecksum, w);
                cachedChecksum = null;
                cachedTotalSupply = 0;
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
                if (cachedBlockVersion <= BlockVer.v2)
                {
                    checksum = Crypto.sha512quTrunc(Encoding.UTF8.GetBytes("IXIAN-DLT0"));
                }else if(cachedBlockVersion < BlockVer.v10)
                {
                    checksum = Crypto.sha512sqTrunc(Encoding.UTF8.GetBytes("IXIAN-DLT0"), 0, 0, 64);
                }else
                {
                    checksum = CryptoManager.lib.sha3_512sq(ConsensusConfig.ixianChecksumLock);
                }

                // TODO: This is probably not the optimal way to do this. Maybe we could do it by blocks to reduce calls to sha256
                // Note: addresses are not fixed size
                foreach (byte[] addr in eligible_addresses)
                {
                    byte[] wallet_checksum = getWallet(new Address(addr)).calculateChecksum(cachedBlockVersion);
                    if (cachedBlockVersion <= BlockVer.v2)
                    {
                        checksum = Crypto.sha512quTrunc(Encoding.UTF8.GetBytes(Crypto.hashToString(checksum) + Crypto.hashToString(wallet_checksum)));
                    }else if (cachedBlockVersion < BlockVer.v10)
                    {
                        List<byte> tmp_hash = checksum.ToList();
                        tmp_hash.AddRange(wallet_checksum);
                        checksum = Crypto.sha512sqTrunc(tmp_hash.ToArray(), 0, 0, 64);
                    }else
                    {
                        byte[] tmp_hash = new byte[checksum.Length + wallet_checksum.Length];
                        Array.Copy(checksum, tmp_hash, checksum.Length);
                        Array.Copy(wallet_checksum, 0, tmp_hash, checksum.Length, wallet_checksum.Length);
                        checksum = CryptoManager.lib.sha3_512sq(tmp_hash.ToArray());
                    }
                }

                cachedChecksum = checksum;
                return checksum;
            }
        }

        public byte[] calculateWalletStateDeltaChecksum(ulong transaction_id, int block_version, bool block_debug = false)
        {
            lock (stateLock)
            {
                using (MemoryStream m = new MemoryStream())
                {
                    using (BinaryWriter w = new BinaryWriter(m))
                    {
                        if(block_version >= BlockVer.v10)
                        {
                            // TODO TODO Omega - checksum lock probably isn't necessary
                            w.Write(ConsensusConfig.ixianChecksumLock);
                        }
                        else
                        {
                            w.Write(Encoding.UTF8.GetBytes("IXIAN-DLT0"));
                        }

                        // TODO TODO Omega - this can be optimized by calculating checksums through WSJ; make sure it can be processed in parallel

                        // TODO: WSJ: Kludge until Blockversion upgrade, so we can replace WS Deltas with WSJ
                        var altered_wallets = getAlteredWalletsSinceWSJTX(transaction_id, block_version);
                        if(altered_wallets == null)
                        {
                            Logging.error("Attempted to calculate WS Delta checksum since WSJ transaction {0}, but no such transaction is open.", transaction_id);
                            return null;
                        }
                        int i = 0;
                        foreach (var altered_wallet in altered_wallets)
                        {
                            if (block_debug)
                            {
                                Logging.info("Delta Checksum: Wallet [{6}] {{ {0} }}: Balance: {7}, Type: {1}, Signers: {2}, Req: {3}, Pubkey: {4} bytes, Data: {5} bytes",
                                    altered_wallet.id.ToString(),
                                    altered_wallet.type,
                                    altered_wallet.countAllowedSigners,
                                    altered_wallet.requiredSigs,
                                    altered_wallet.publicKey != null ? altered_wallet.publicKey.Length : -1,
                                    altered_wallet.data != null ? altered_wallet.data.Length : -1,
                                    i,
                                    altered_wallet.balance.ToString());
                                i += 1;
                            }
                            altered_wallet.writeBytes(w);
                        }
#if TRACE_MEMSTREAM_SIZES
                        Logging.info(String.Format("WalletState::calculateWalletStateDeltaChecksum: {0}", m.Length));
#endif
                    }
                    if(block_version >= BlockVer.v10)
                    {
                        return CryptoManager.lib.sha3_512sq(m.ToArray());
                    }
                    else
                    {
                        return Crypto.sha512sqTrunc(m.ToArray(), 0, 0, 64);
                    }
                }
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
                Logging.info("Preparing {0} chunks of walletState. Total wallets: {1}", num_chunks, walletState.Count);
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
                Logging.info("Prepared {0} WalletState chunks with {1} total wallets.",
                    num_chunks,
                    chunks.Sum(x => x.wallets.Count()));
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
                        walletState.AddOrReplace(w.id.addressNoChecksum, w);
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
                    Logging.error("Exception calculating total supply: {0}", e.Message);
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
