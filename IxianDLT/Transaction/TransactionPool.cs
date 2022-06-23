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

using DLT.Meta;
using IXICore;
using IXICore.Inventory;
using IXICore.Meta;
using IXICore.Network;
using IXICore.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using static IXICore.Transaction;

namespace DLT
{
    class TransactionPool
    {
        private class BlockSolution
        {
            public Address address = null;
            public byte[] nonce = null;
            public Transaction tx = null;
        }

        static object stateLock = new object();
        static readonly Dictionary<byte[], Transaction> appliedTransactions = new Dictionary<byte[], Transaction>(new ByteArrayComparer());
        static readonly Dictionary<byte[], Transaction> unappliedTransactions = new Dictionary<byte[], Transaction>(new ByteArrayComparer());

        static readonly List<Address> premineAddresses = new List<Address>()
        {
            new Address(Base58Check.Base58CheckEncoding.DecodePlain("13fiCRZHPqcCFvQvuggKEjDvFsVLmwoavaBw1ng5PdSKvCUGp")),
            new Address(Base58Check.Base58CheckEncoding.DecodePlain("16LUmwUnU9M4Wn92nrvCStj83LDCRwvAaSio6Xtb3yvqqqCCz"))
        };

        static TransactionPool()
        {
        }

        private TransactionPool()
        {
        }

        public static bool verifyMultisigTransaction(Transaction transaction, RemoteEndpoint endpoint)
        {
            // multisig verification
            if (transaction.type == (int)Transaction.Type.MultisigTX || transaction.type == (int)Transaction.Type.ChangeMultisigWallet || transaction.type == (int)Transaction.Type.MultisigAddTxSignature)
            {
                return false;
                // multiple "from" addresses are not supported
                if (transaction.fromList.Count != 1)
                {
                    Logging.warn("Multisig transaction {{ {0} }} has multiple 'from' addresses, which is not allowed!", transaction.getTxIdString());
                    return false;
                }
                object multisig_type = transaction.GetMultisigData();
                if (multisig_type == null)
                {
                    Logging.warn("Multisig transaction {{ {0} }} has invalid multisig data attached!", transaction.getTxIdString());
                    return false;
                }

                // we can use fromList.First because:
                //  a: verifyTransaction() checks that there is at least one fromAddress (if there isn't, totalAmount == 0 and transaction is failed before it gets here)
                //  b: at the start of this function, fromList is checked for fromList.Count > 1 and failed if so
                Address from_address = new Address(transaction.pubKey.addressNoChecksum, transaction.fromList.First().Key);

                byte[] orig_txid;
                byte[] signer_pub_key = null;
                byte[] signer_nonce = null;
                if (multisig_type is Transaction.MultisigTxData)
                {
                    var multisig_obj = (Transaction.MultisigTxData)multisig_type;
                    // regular multisig transaction
                    if (multisig_obj.origTXId != null)
                    {
                        Logging.info("Multisig transaction {{ {0} }} adds signature for origin multisig transaction {{ {1} }}.", transaction.getTxIdString(), Transaction.getTxIdString(multisig_obj.origTXId));
                    }
                    else
                    {
                        Logging.info("Multisig transaction {{ {0} }} is an origin multisig transaction.", transaction.getTxIdString());
                    }
                    orig_txid = multisig_obj.origTXId;
                    signer_pub_key = multisig_obj.signerPubKey;
                    signer_nonce = multisig_obj.signerNonce;
                    if (transaction.type == (int)Transaction.Type.MultisigAddTxSignature)
                    {
                        if(orig_txid == null || orig_txid.Length > 100 || orig_txid.Length < 10)
                        {
                            Logging.warn("Orig txid {0} is invalid.", Transaction.getTxIdString(orig_txid));
                            return false;
                        }

                        Transaction tmp_tx = getUnappliedTransaction(orig_txid);
                        if (tmp_tx == null)
                        {
                            if(!hasAppliedTransaction(orig_txid))
                            {
                                Logging.warn("Orig txid {0} doesn't exist, requesting from network.", Transaction.getTxIdString(orig_txid));
                                CoreProtocolMessage.broadcastGetTransaction(orig_txid, 0, endpoint);
                                CoreProtocolMessage.broadcastGetTransaction(transaction.id, 0, endpoint);
                                return false;
                            }else
                            {
                                Logging.error("Orig txid {0} has already been applied.", Transaction.getTxIdString(orig_txid));
                                return false;
                            }
                        }else if (tmp_tx.type != (int)Transaction.Type.ChangeMultisigWallet && (tmp_tx.type != (int)Transaction.Type.MultisigTX))
                        {
                            Logging.warn("Orig txid {0} is not a multisig transaction.", Transaction.getTxIdString(orig_txid));
                            return false;
                        }
                    }
                }
                if (multisig_type is Transaction.MultisigAddrAdd)
                {
                    var multisig_obj = (Transaction.MultisigAddrAdd)multisig_type;

                    Wallet tmp_w = Node.walletState.getWallet(from_address);
                    if (tmp_w.isValidSigner(multisig_obj.addrToAdd))
                    {
                        Logging.warn("Pubkey {0} is already in allowed multisig list for wallet {1}.", multisig_obj.addrToAdd.ToString(), tmp_w.id.ToString());
                        return false;
                    }

                    Logging.info("Multisig change(add) transaction adds allowed signer {0} to address {1}.", multisig_obj.addrToAdd.ToString(), Base58Check.Base58CheckEncoding.EncodePlain(transaction.pubKey.getInputBytes()));

                    signer_pub_key = multisig_obj.signerPubKey;
                    signer_nonce = multisig_obj.signerNonce;
                }
                if (multisig_type is Transaction.MultisigAddrDel)
                {
                    var multisig_obj = (Transaction.MultisigAddrDel)multisig_type;

                    Logging.info("Multisig change(del) transaction removes allowed signer {0} from wallet {1}.", multisig_obj.addrToDel.ToString(), Base58Check.Base58CheckEncoding.EncodePlain(transaction.pubKey.getInputBytes()));

                    signer_pub_key = multisig_obj.signerPubKey;
                    signer_nonce = multisig_obj.signerNonce;
                }
                if (multisig_type is Transaction.MultisigChSig)
                {
                    var multisig_obj = (Transaction.MultisigChSig)multisig_type;

                    Logging.info("Multisig change(sig) transaction changes required signatures for wallet {0} to {1}.", Base58Check.Base58CheckEncoding.EncodePlain(transaction.pubKey.getInputBytes()), multisig_obj.reqSigs);

                    signer_pub_key = multisig_obj.signerPubKey;
                    signer_nonce = multisig_obj.signerNonce;
                }
                if(signer_pub_key == null)
                {
                    Logging.warn("Multisig transaction {{ {0} }}, has a null signer pubkey!", transaction.getTxIdString());
                }
                Address tx_signer_address = new Address(signer_pub_key, signer_nonce);

                Wallet w = Node.walletState.getWallet(from_address);
                if (!w.isValidSigner(tx_signer_address))
                {
                    Logging.warn("Multisig transaction {{ {0} }} does not have a valid signature for wallet {1}.", transaction.getTxIdString(), w.id.ToString());
                    return false;
                }
                // multisig tx can only be performed on multisig wallets
                if(w.type != WalletType.Multisig)
                {
                    // only exception is the "add signer multisig"
                    if(!(multisig_type is Transaction.MultisigAddrAdd))
                    {
                        Logging.warn("Multisig transaction {{ {0} }} attempts to operate on a non-multisig wallet {1}.", transaction.getTxIdString(), w.id.ToString());
                        return false;
                    }
                }
            }
            return true;
        }

        public static bool verifyPremineTransaction(Transaction transaction)
        {
            ulong block_height = IxianHandler.getLastBlockHeight();
            if(block_height > 5256000)
            {
                return true;
            }

            byte[] tx_address = transaction.pubKey.addressNoChecksum;
            foreach (Address premine_address in premineAddresses)
            {
                if (tx_address.SequenceEqual(premine_address.addressNoChecksum))
                {
                    IxiNumber cur_balance = IxianHandler.getWalletBalance(premine_address);
                    if (block_height < 1051200)
                    {
                        if(cur_balance - transaction.amount - transaction.fee < new IxiNumber("900000000"))
                        {
                            return false;
                        }
                    }else if(block_height < 2102400)
                    {
                        if (cur_balance - transaction.amount - transaction.fee < new IxiNumber("800000000"))
                        {
                            return false;
                        }
                    }
                    else if (block_height < 3153600)
                    {
                        if (cur_balance - transaction.amount - transaction.fee < new IxiNumber("600000000"))
                        {
                            return false;
                        }
                    }
                    else if (block_height < 4204800)
                    {
                        if (cur_balance - transaction.amount - transaction.fee < new IxiNumber("400000000"))
                        {
                            return false;
                        }
                    }
                    else
                    {
                        if (cur_balance - transaction.amount - transaction.fee < new IxiNumber("200000000"))
                        {
                            return false;
                        }
                    }
                }
            }
            return true;
        }

        public static bool verifyTransaction(Transaction transaction, RemoteEndpoint endpoint, bool full_check = true)
        {
            ulong blocknum = Node.blockChain.getLastBlockNum();
            if (blocknum < 1)
            {
                if (transaction.type == (int)Transaction.Type.Genesis)
                {
                    // Adding GENESIS transaction
                    Logging.info("Received GENESIS transaction.");
                    return true;
                }
            }
            else if (blocknum < 10)
            {
                Logging.warn("Ignoring transaction before block 10.");
                return false;
            }
            else if (transaction.type == (int)Transaction.Type.Genesis)
            {
                Logging.warn("Genesis transaction on block #{0} skipped. TXid: {1}.", blocknum, transaction.getTxIdString());
                return false;
            }

            if(transaction.version > Transaction.maxVersion)
            {
                Logging.error("Received transaction {0} with a version higher than this node can handle, discarding the transaction.", transaction.getTxIdString());
                return false;
            }

            // reject any transaction with block height 0
            if (transaction.blockHeight == 0)
            {
                Logging.warn("Transaction without block height specified on block #{0} skipped. TXid: {1}.", blocknum, transaction.getTxIdString());
                return false;
            }

            int block_version = Node.blockChain.getLastBlockVersion();
            if (block_version < BlockVer.v2)
            {
                if (transaction.version > 1)
                {
                    Logging.warn("Transaction version {0} is incorrect, expecting v0 or v1. TXid: {1}.", transaction.version, transaction.getTxIdString());
                    return false;
                }
            }
            else if (block_version == BlockVer.v2)
            {
                if (transaction.version < 1 || transaction.version > 2)
                {
                    Logging.warn("Transaction version {0} is incorrect, expecting v1 or v2. TXid: {1}.", transaction.version, transaction.getTxIdString());
                    return false;
                }
            }
            else if (block_version <= BlockVer.v5)
            {
                if (transaction.version < 2 || transaction.version > 3)
                {
                    Logging.warn("Transaction version {0} is incorrect, expecting v2 or v3. TXid: {1}.", transaction.version, transaction.getTxIdString());
                    return false;
                }
            }
            else if (block_version == BlockVer.v6)
            {
                if (transaction.version < 3 || transaction.version > 4)
                {
                    Logging.warn("Transaction version {0} is incorrect, expecting v3 or v4. TXid: {1}.", transaction.version, transaction.getTxIdString());
                    return false;
                }
            }
            else if (block_version == BlockVer.v7)
            {
                if (transaction.version < 4 || transaction.version > 5)
                {
                    Logging.warn("Transaction version {0} is incorrect, expecting v4 or v5. TXid: {1}.", transaction.version, transaction.getTxIdString());
                    return false;
                }
            }
            else if (block_version == BlockVer.v8 || block_version == BlockVer.v9)
            {
                if (transaction.version < 5 || transaction.version > 6)
                {
                    Logging.warn("Transaction version {0} is incorrect, expecting v5 or v6. TXid: {1}.", transaction.version, transaction.getTxIdString());
                    return false;
                }
            }
            else if (block_version == BlockVer.v10)
            {
                if (transaction.version < 6 || transaction.version > 7)
                {
                    Logging.warn("Transaction version {0} is incorrect, expecting v6 or v7. TXid: {1}.", transaction.version, transaction.getTxIdString());
                    return false;
                }
            }
            else
            {
                if (transaction.version < 7)
                {
                    Logging.warn("Transaction version {0} is incorrect, expecting v6 or higher. TXid: {1}.", transaction.version, transaction.getTxIdString());
                    return false;
                }
            }

            if (transaction.type == (int)Transaction.Type.StakingReward)
            {
                if (transaction.version != Transaction.getExpectedVersion(IxianHandler.getLastBlockVersion()))
                {
                    Logging.warn("Invalid transaction version {0}, expecting {1}. TXid: {2}.", transaction.version, Transaction.getExpectedVersion(IxianHandler.getLastBlockVersion()), transaction.getTxIdString());
                    return false;
                }
            }

            // Check the block height
            ulong minBh = 0;
            if (blocknum > ConsensusConfig.getRedactedWindowSize())
            {
                minBh = blocknum - ConsensusConfig.getRedactedWindowSize();
            }
            ulong highest_block_num = IxianHandler.getLastBlockHeight() + 10;
            if(Node.blockProcessor.highestNetworkBlockNum > highest_block_num)
            {
                highest_block_num = Node.blockProcessor.highestNetworkBlockNum + 10;
            }
            if (minBh > transaction.blockHeight || (transaction.blockHeight > blocknum + 10 && transaction.blockHeight > highest_block_num))
            {
                Logging.warn("Incorrect block height for transaction {0}. Tx block height is {1}, expecting at least {2} and at most {3}", transaction.getTxIdString(), transaction.blockHeight, minBh, highest_block_num);
                return false;
            }

            // Prevent transaction spamming
            // Note: transactions that change multisig wallet parameters may have amount zero, since it will be ignored anyway
            if(transaction.type != (int)Transaction.Type.PoWSolution)
            if (transaction.amount + transaction.fee == (long)0 && transaction.type != (int)Transaction.Type.ChangeMultisigWallet && transaction.type != (int)Transaction.Type.MultisigAddTxSignature)
            {
                    Logging.warn("Transaction amount was zero for txid {0}.", transaction.getTxIdString());
                    return false;
            }

            if (transaction.amount < 0)
            {
                Logging.warn("Transaction amount was negative for txid {0}.", transaction.getTxIdString());
                return false;
            }

            if (transaction.fee < 0)
            {
                Logging.warn("Transaction fee was negative for txid {0}.", transaction.getTxIdString());
                return false;
            }

            if (endpoint != null && endpoint.presenceAddress != null && (endpoint.presenceAddress.type == 'M' || endpoint.presenceAddress.type == 'H'))
            {
                PendingTransactions.increaseReceivedCount(transaction.id, endpoint.presence.wallet);
            }

            if (!verifyMultisigTransaction(transaction, endpoint))
            {
                return false;
            }

            IxiNumber totalAmount = new IxiNumber(0);
            foreach (var entry in transaction.fromList)
            {
                if (entry.Key.Length == 1 && entry.Key.First() != 0)
                {
                    Logging.warn("Input nonce is 1 byte long but has an incorrect value for tx {{ {0} }}.", transaction.getTxIdString());
                    return false;
                }
                else if (entry.Key.Length != 1 && entry.Key.Length != 16)
                {
                    Logging.warn("Input nonce is not 1 or 16 bytes long for tx {{ {0} }}.", transaction.getTxIdString());
                    return false;
                }

                if (entry.Value < 0)
                {
                    Logging.warn("Transaction amount was invalid for txid {0}.", transaction.getTxIdString());
                    return false;
                }

                totalAmount += entry.Value;
                if (transaction.type != (int)Transaction.Type.PoWSolution
                    && transaction.type != (int)Transaction.Type.StakingReward
                    && transaction.type != (int)Transaction.Type.Genesis)
                {
                    if (Node.blockSync.synchronizing == false)
                    {
                        Address tmp_from_address = new Address(transaction.pubKey.addressNoChecksum, entry.Key);


                        if (transaction.type != (int)Transaction.Type.ChangeMultisigWallet
                            && transaction.type != (int)Transaction.Type.MultisigAddTxSignature)
                        {
                            if (transaction.toList.ContainsKey(tmp_from_address))
                            {
                                // Prevent sending to the same address
                                Logging.warn("To and from addresses are the same in transaction {{ {0} }}.", transaction.getTxIdString());
                                return false;
                            }
                        }

                        Wallet tmp_wallet = Node.walletState.getWallet(tmp_from_address);

                        
                        if(transaction.type == (int)Transaction.Type.MultisigTX
                            || transaction.type == (int)Transaction.Type.ChangeMultisigWallet
                            || transaction.type == (int)Transaction.Type.MultisigAddTxSignature)
                        {
                            if (tmp_wallet.type != WalletType.Multisig)
                            {
                                object ms_data = transaction.GetMultisigData();
                                if (ms_data is Transaction.MultisigAddrAdd)
                                {
                                    // wallet type can be normal or multisig
                                }
                                else
                                {
                                    Logging.warn("Attempted to use normal address with multisig transaction {{ {0} }}.", transaction.getTxIdString());
                                    return false;
                                }
                            }
                        }else
                        {
                            if (tmp_wallet.type != WalletType.Normal)
                            {
                                Logging.warn("Attempted to use a non-normal address with normal transaction {{ {0} }}.", transaction.getTxIdString());
                                return false;
                            }
                        }

                        // Verify the transaction against the wallet state
                        IxiNumber fromBalance = tmp_wallet.balance;

                        if (fromBalance < entry.Value)
                        {
                            // Prevent overspending
                            Logging.warn("Attempted to overspend with transaction {{ {0} }}.", transaction.getTxIdString());
                            return false;
                        }
                    }
                }
            }

            if (totalAmount != transaction.amount + transaction.fee)
            {
                Logging.warn("Total amount {0} specified by the transaction {1} inputs is different than the actual total amount {2}.", (transaction.amount + transaction.fee).ToString(), transaction.getTxIdString(), totalAmount.ToString());
                return false;
            }

            totalAmount = new IxiNumber(0);
            foreach (var entry in transaction.toList)
            {
                if (!Address.validateAddress(entry.Key.addressNoChecksum))
                {
                    Logging.warn("Adding transaction {{ {0} }}, but recipient address is invalid!", transaction.getTxIdString());
                    return false;
                }
                if (entry.Value.amount < 0)
                {
                    Logging.warn("Transaction recipient amount was invalid for txid {0}.", transaction.getTxIdString());
                    return false;
                }
                if (IxianHandler.getLastBlockVersion() >= BlockVer.v10)
                {
                    if (transaction.type != (int)Transaction.Type.PoWSolution
                        && transaction.type != (int)Transaction.Type.StakingReward)
                    {
                        if (entry.Value.amount < ConsensusConfig.transactionDustLimit)
                        {
                            Logging.warn("Transaction recipient amount was lower than dust limit for txid {0}.", transaction.getTxIdString());
                            return false;
                        }
                    }
                }
                totalAmount += entry.Value.amount;
            }

            if(totalAmount != transaction.amount)
            {
                Logging.warn("Total amount {0} specified by the transaction {1} outputs is different than the actual total amount {2}.", transaction.amount.ToString(), transaction.getTxIdString(), totalAmount.ToString());
                return false;
            }

            // Special case for PoWSolution transactions
            if (transaction.type == (int)Transaction.Type.PoWSolution)
            {
                ulong tmp = 0;
                byte[] tmp2 = null;
                if (!Node.blockSync.synchronizing && !verifyPoWTransaction(transaction, out tmp, out tmp2, -1, !transaction.fromLocalStorage))
                {
                    return false;
                }
                // TODO: pre-validate the transaction in such a way it doesn't affect performance
            }
            // Special case for Staking Reward transaction
            else if (transaction.type == (int)Transaction.Type.StakingReward)
            {

            }
            // Special case for Genesis transaction
            else if (transaction.type == (int)Transaction.Type.Genesis)
            {
                // Ignore if it's not in the genesis block
                if (blocknum > 1)
                {
                    Logging.warn("Genesis transaction on block #{0} ignored. TXid: {1}.", blocknum, transaction.getTxIdString());
                    return false;
                }
            }
            else
            {

                IxiNumber expectedFee = transaction.calculateMinimumFee(ConsensusConfig.transactionPrice);
                if (transaction.version == 0)
                {
                    expectedFee = ConsensusConfig.transactionPrice;
                }
                // Verify if the transaction contains the minimum fee
                if (transaction.fee < expectedFee)
                {
                    // Prevent transactions that can't pay the minimum fee
                    Logging.warn("Transaction fee does not cover minimum fee for {{ {0} }}, specified tx fee: {1}, min. expected fee: {2}, tx length: {3}.", transaction.getTxIdString(), transaction.fee, expectedFee, transaction.getBytes(false, true).Length);
                    return false;
                }
            }
            /*var sw = new System.Diagnostics.Stopwatch();
            sw.Start();*/

            // Extract the public key if found. Used for transaction verification.
            byte[] pubkey = null;
            byte[] signer_nonce = null; // used for multi sig

            if (transaction.type == (int)Transaction.Type.Genesis ||
                transaction.type == (int)Transaction.Type.StakingReward)
            {
                return true;
            }
            else if (transaction.type == (int)Transaction.Type.MultisigTX
                || transaction.type == (int)Transaction.Type.ChangeMultisigWallet
                || transaction.type == (int)Transaction.Type.MultisigAddTxSignature)
            {
                object ms_data = transaction.GetMultisigData();
                if (ms_data is Transaction.MultisigAddrAdd)
                {
                    pubkey = ((MultisigAddrAdd)ms_data).signerPubKey;
                    signer_nonce = ((MultisigAddrAdd)ms_data).signerNonce;
                }
                else if (ms_data is Transaction.MultisigAddrDel)
                {
                    pubkey = ((MultisigAddrDel)ms_data).signerPubKey;
                    signer_nonce = ((MultisigAddrDel)ms_data).signerNonce;
                }
                else if (ms_data is Transaction.MultisigChSig)
                {
                    pubkey = ((MultisigChSig)ms_data).signerPubKey;
                    signer_nonce = ((MultisigChSig)ms_data).signerNonce;
                }
                else if (ms_data is Transaction.MultisigTxData)
                {
                    pubkey = ((MultisigTxData)ms_data).signerPubKey;
                    signer_nonce = ((MultisigTxData)ms_data).signerNonce;
                }
            }
            else
            {
                pubkey = Node.walletState.getWallet(transaction.pubKey).publicKey;
                // Generate an address from the public key and compare it with the sender
                if (pubkey == null)
                {
                    // There is no supplied public key, extract it from the data section
                    pubkey = transaction.pubKey.pubKey;
                }
            }

            if (pubkey == null || pubkey.Length < 32 || pubkey.Length > 2500)
            {
                Logging.warn("Invalid pubkey for transaction id: {0}", transaction.getTxIdString());
                return false;
            }

            if (signer_nonce != null && signer_nonce.Length > 16)
            {
                Logging.warn("Invalid nonce for transaction id: {0}", transaction.getTxIdString());
                return false;
            }
            
            // Finally, verify the signature
            if (full_check && transaction.verifySignature(pubkey, signer_nonce) == false)
            {
                // Transaction signature is invalid
                Logging.warn("Invalid signature for transaction id: {0}", transaction.getTxIdString());
                return false;
            }

            if(!verifyPremineTransaction(transaction))
            {
                Logging.warn("Cannot spend so much premine yet, txid: {0}", transaction.getTxIdString());
                return false;
            }
            /*sw.Stop();
            TimeSpan elapsed = sw.Elapsed;
            Logging.info(string.Format("VerifySignature duration: {0}ms", elapsed.TotalMilliseconds));*/

            return true;
        }

        public static bool setReadyToApplyFlag(Transaction tx, ulong block_num)
        {
            if(tx.applied != 0)
            {
                Logging.error("An error occurred while setting readyToApply flag to tx {0}, tx was already applied.", tx.getTxIdString());
                return false;
            }
            if (block_num == 0)
            {
                Logging.error("An error occurred while setting readyToApply to tx {0}, block_num was 0.", tx.getTxIdString());
                return false;
            }
            tx.readyToApply = block_num;
            return true;
        }

        public static bool setAppliedFlag(byte[] txid, ulong blockNum, bool add_to_storage = true)
        {
            lock (stateLock)
            {
                if (unappliedTransactions.ContainsKey(txid))
                {
                    Transaction t = unappliedTransactions[txid];
                    unappliedTransactions.Remove(txid);

                    t.applied = blockNum;
                    if (t.applied == 0)
                    {
                        Logging.error("An error occurred while adding tx {0} to storage, applied flag was 0.", t.getTxIdString());
                        return false;
                    }

                    appliedTransactions.Add(txid, t);

                    if (!t.fromLocalStorage)
                    {
                        if (IxianHandler.isMyAddress(t.pubKey) || IxianHandler.extractMyAddressesFromAddressList(t.toList) != null)
                        {
                            ActivityStorage.updateStatus(t.id, ActivityStatus.Final, t.applied);
                        }
                    }

                    PendingTransactions.remove(t.id);

                    if (add_to_storage)
                    {
                        bool insertTx = true;
                        if (Node.blockSync.synchronizing && Config.recoverFromFile)
                        {
                            insertTx = false;
                        }
                        if (insertTx)
                        {
                            Node.storage.insertTransaction(t);
                        }
                    }

                    // Send transaction events to all subscribed clients
                    // TODO: optimize this further to decrease cpu time in the current thread
                    broadcastAddTransactionEvent(t);

                    return true;
                }
            }
            return false;
        }

        public static List<byte[]> getRelatedMultisigTransactions(byte[] txid, Block block, bool remove_failed_transactions = true)
        {
            lock(stateLock)
            {
                List<byte[]> related_transaction_ids = new List<byte[]>();

                List<Transaction> failed_transactions = new List<Transaction>();
                List<byte[]> signer_addresses = new List<byte[]>();
                HashSet<byte[]> tmp_transactions = unappliedTransactions.Keys.ToHashSet(new ByteArrayComparer());
                if(block != null)
                {
                    tmp_transactions = block.transactions;
                }
                Transaction orig_tx = getUnappliedTransaction(txid);
                if(orig_tx != null)
                {
                    object orig_ms_data = orig_tx.GetMultisigData();
                    byte[] signer_pub_key = null;
                    byte[] signer_nonce = null;
                    if (orig_ms_data is Transaction.MultisigAddrAdd)
                    {
                        var multisig_obj = (Transaction.MultisigAddrAdd)orig_ms_data;
                        signer_pub_key = multisig_obj.signerPubKey;
                        signer_nonce = multisig_obj.signerNonce;
                    }
                    else if (orig_ms_data is Transaction.MultisigAddrDel)
                    {
                        var multisig_obj = (Transaction.MultisigAddrDel)orig_ms_data;
                        signer_pub_key = multisig_obj.signerPubKey;
                        signer_nonce = multisig_obj.signerNonce;
                    }
                    else if (orig_ms_data is Transaction.MultisigChSig)
                    {
                        var multisig_obj = (Transaction.MultisigChSig)orig_ms_data;
                        signer_pub_key = multisig_obj.signerPubKey;
                        signer_nonce = multisig_obj.signerNonce;
                    }
                    else if (orig_ms_data is Transaction.MultisigTxData)
                    {
                        var multisig_obj = (Transaction.MultisigTxData)orig_ms_data;
                        signer_pub_key = multisig_obj.signerPubKey;
                        signer_nonce = multisig_obj.signerNonce;
                    }
                    else
                    {
                        Logging.warn("Multisig transaction {{ {0} }} has invalid data!", Transaction.getTxIdString(orig_tx.id));
                        // unknown MS transaction, discard
                        failed_transactions.Add(orig_tx);
                        orig_tx = null;
                    }
                    if (orig_tx != null)
                    {
                        byte[] signer_address = ((new Address(signer_pub_key, signer_nonce)).addressNoChecksum);
                        signer_addresses.Add(signer_address);
                    } else
                    {
                        return related_transaction_ids;
                    }
                }

                foreach (var tx_key in tmp_transactions)
                {
                    if (!unappliedTransactions.ContainsKey(tx_key))
                    {
                        continue;
                    }
                    var tx = unappliedTransactions[tx_key];
                    if (tx == null)
                    {
                        continue;
                    }
                    if(tx.type == (int)Transaction.Type.MultisigAddTxSignature)
                    {
                        object multisig_type = tx.GetMultisigData();
                        byte[] orig_txid = null;
                        byte[] signer_pub_key = null;
                        byte[] signer_nonce = null;
                        if (multisig_type is Transaction.MultisigTxData)
                        {
                            var multisig_obj = (Transaction.MultisigTxData)multisig_type;
                            orig_txid = multisig_obj.origTXId;
                            signer_pub_key = multisig_obj.signerPubKey;
                            signer_nonce = multisig_obj.signerNonce;
                        }

                        if (orig_txid == null)
                        {
                            Logging.warn("Multisig transaction {{ {0} }} has a null orig txid!", tx.getTxIdString());
                            failed_transactions.Add(tx);
                            continue;
                        }

                        if (signer_pub_key == null)
                        {
                            Logging.warn("Multisig transaction {{ {0} }} signer_pub_key is null - orig multisig transaction: {1}!", tx.getTxIdString(), Transaction.getTxIdString(orig_txid));
                            failed_transactions.Add(tx);
                            continue;
                        }

                        if (orig_txid.SequenceEqual(txid))
                        {
                            if (orig_tx == null)
                            {
                                Logging.warn("Multisig transaction {{ {0} }} signs a missing orig multisig transaction {1}!", tx.getTxIdString(), Transaction.getTxIdString(orig_txid));
                                failed_transactions.Add(tx);
                                continue;
                            }

                            Address signer_address = new Address(signer_pub_key, signer_nonce);
                            if (signer_addresses.Contains(signer_address.addressNoChecksum, new ByteArrayComparer()))
                            {
                                Logging.warn("Multisig transaction {{ {0} }} signs an already signed transaction {1} by this address {2}!",
                                    Transaction.getTxIdString(txid), Transaction.getTxIdString(orig_txid), signer_address.ToString());
                                failed_transactions.Add(tx);
                                continue;
                            }

                            Wallet orig = Node.walletState.getWallet(new Address(tx.pubKey.addressNoChecksum, tx.fromList.First().Key));
                            if (!orig.isValidSigner(signer_address))
                            {
                                Logging.warn("Tried to use Multisig transaction {{ {0} }} without being an actual owner {1}!",
                                    Transaction.getTxIdString(txid), signer_address.ToString());
                                failed_transactions.Add(tx);
                                continue;
                            }

                            signer_addresses.Add(signer_address.addressNoChecksum);
                            related_transaction_ids.Add(tx_key);
                        }
                    }
                }

                if (remove_failed_transactions)
                {
                    // Remove all failed transactions from the TxPool and block
                    foreach (Transaction tx in failed_transactions)
                    {
                        Logging.warn("Removing failed ms transaction #{0} from pool.", tx.getTxIdString());
                        // Remove from TxPool
                        if (tx.applied == 0)
                        {
                            lock (stateLock)
                            {
                                unappliedTransactions.Remove(tx.id);
                            }
                        }
                        else
                        {
                            Logging.error("Error, attempting to remove failed ms transaction #{0} from pool, that was already applied.", tx.getTxIdString());
                        }
                    }
                }

                return related_transaction_ids;
            }
        }

        public static int getNumRelatedMultisigTransactions(byte[] txid, Block block)
        {
            return getRelatedMultisigTransactions(txid, block, false).Count();
        }

        public static void addTransactionToActivityStorage(Transaction transaction)
        {
            Activity activity = null;
            int type = -1;
            IxiNumber value = transaction.amount;
            Dictionary<byte[], List<byte[]>> wallet_list = null;
            Address wallet = null;
            Address primary_address = transaction.pubKey;
            if (IxianHandler.isMyAddress(primary_address))
            {
                wallet = primary_address;
                type = (int)ActivityType.TransactionSent;
                if (transaction.type == (int)Transaction.Type.PoWSolution)
                {
                    type = (int)ActivityType.MiningReward;
                    value = ConsensusConfig.calculateMiningRewardForBlock(transaction.powSolution.blockNum);
                }
            }else
            {
                wallet_list = IxianHandler.extractMyAddressesFromAddressList(transaction.toList);
                if (wallet_list != null)
                {
                    type = (int)ActivityType.TransactionReceived;
                    if (transaction.type == (int)Transaction.Type.StakingReward)
                    {
                        type = (int)ActivityType.StakingReward;
                    }
                }
            }
            if (type != -1)
            {
                int status = (int)ActivityStatus.Pending;
                if (transaction.applied > 0)
                {
                    status = (int)ActivityStatus.Final;
                }
                if(wallet_list != null)
                {
                    foreach (var extractedWallet in wallet_list)
                    {
                        foreach(var address in extractedWallet.Value)
                        {
                            activity = new Activity(extractedWallet.Key, new Address(address).ToString(), primary_address.ToString(), transaction.toList, type, transaction.id, transaction.toList[new Address(address)].amount.ToString(), transaction.timeStamp, status, transaction.applied, transaction.getTxIdString());
                            ActivityStorage.insertActivity(activity);
                        }
                    }
                }
                else if(wallet != null)
                {
                    activity = new Activity(IxianHandler.getWalletStorageBySecondaryAddress(primary_address).getSeedHash(), wallet.ToString(), primary_address.ToString(), transaction.toList, type, transaction.id, value.ToString(), transaction.timeStamp, status, transaction.applied, transaction.getTxIdString());
                    ActivityStorage.insertActivity(activity);
                }
            }
        }

        public static void addTxId(byte[] txid)
        {
            if (!Node.blockSync.synchronizing)
            {
                return;
            }

            lock (stateLock)
            {
                appliedTransactions.Add(txid, null);
                if (unappliedTransactions.ContainsKey(txid))
                {
                    unappliedTransactions.Remove(txid);
                }
            }
        }

        // Adds a non-applied transaction to the memory pool
        // Returns true if the transaction is added to the pool, false otherwise
        public static bool addTransaction(Transaction transaction, bool no_broadcast = false, RemoteEndpoint endpoint = null, bool verifyTx = true, bool force_broadcast = false)
        {
            if (verifyTx)
            {
                Node.inventoryCache.setProcessedFlag(InventoryItemTypes.transaction, transaction.id, true);
                if (!verifyTransaction(transaction, endpoint))
                {
                    return false;
                }
            }

            lock (stateLock)
            {
                // Search for duplicates
                if ((transaction.blockHeight <= Node.blockChain.getLastBlockNum() + 1 && appliedTransactions.ContainsKey(transaction.id))
                    || unappliedTransactions.ContainsKey(transaction.id))
                {
                    // Logging.warn("Duplicate transaction {0}: already exists in the Transaction Pool.", transaction.getTxIdString());
                    return false;
                }
                unappliedTransactions.Add(transaction.id, transaction);
                if (!transaction.fromLocalStorage)
                {
                    addTransactionToActivityStorage(transaction);
                }
            }

            //   Logging.info(String.Format("Transaction {{ {0} }} has been added.", transaction.id, transaction.amount));
            if (ConsoleHelpers.verboseConsoleOutput)
                Console.Write("$");

            if (Node.blockSync.synchronizing == true)
            {
                return true;
            }

            // Broadcast this transaction to the network
            if (no_broadcast == false)
            {
                if(force_broadcast)
                {
                    CoreProtocolMessage.broadcastProtocolMessage(new char[] { 'M', 'H' }, ProtocolMessageCode.transactionData, transaction.getBytes(), null, endpoint);
                }else
                {
                    CoreProtocolMessage.addToInventory(new char[] { 'M', 'H' }, new InventoryItem(InventoryItemTypes.transaction, transaction.id), endpoint);
                }
            }

            // Send transaction events to all subscribed clients
            // TODO: optimize this further to decrease cpu time in the current thread
            broadcastAddTransactionEvent(transaction);


            return true;
        }

        // Send transaction events to all subscribed clients
        public static void broadcastAddTransactionEvent(Transaction transaction)
        {
            if(Node.blockSync.synchronizing)
            {
                return;
            }

            // Send transaction FROM event
            byte[] from_addr = transaction.pubKey.addressWithChecksum;
            CoreProtocolMessage.broadcastEventDataMessage(NetworkEvents.Type.transactionFrom, from_addr, ProtocolMessageCode.transactionData, transaction.getBytes(true), Encoding.UTF8.GetBytes(transaction.getTxIdString()));

            // Send transaction TO event
            foreach (var entry in transaction.toList)
            {
                byte[] addr = new byte[entry.Key.addressWithChecksum.Length];
                Array.Copy(entry.Key.addressWithChecksum, addr, addr.Length);
                CoreProtocolMessage.broadcastEventDataMessage(NetworkEvents.Type.transactionTo, addr, ProtocolMessageCode.transactionData, transaction.getBytes(true), Encoding.UTF8.GetBytes(transaction.getTxIdString()));
            }
        }

        public static bool hasAppliedTransaction(byte[] txid)
        {
            lock(stateLock)
            {
                return appliedTransactions.ContainsKey(txid);
            }
        }

        // Attempts to retrieve a transaction from memory or from storage
        // Returns null if no transaction is found
        public static Transaction getAppliedTransaction(byte[] txid, ulong block_num = 0, bool search_in_storage = false)
        {
            Transaction transaction = null;

            bool compacted_transaction = false;

            lock (stateLock)
            {
                //Logging.info(String.Format("Looking for transaction {{ {0} }}. Pool has {1}.", txid, transactions.Count));
                compacted_transaction = appliedTransactions.ContainsKey(txid);
                transaction = compacted_transaction ? appliedTransactions[txid] : null;
            }

            if (transaction != null)
                return transaction;

            if (search_in_storage || compacted_transaction)
            {
                // No transaction found in memory, look into storage

                /*var sw = new System.Diagnostics.Stopwatch();
                sw.Start();*/
                transaction = Node.storage.getTransaction(txid, block_num);
                /*sw.Stop();
                TimeSpan elapsed = sw.Elapsed;
                Logging.info(string.Format("StopWatch duration: {0}ms", elapsed.TotalMilliseconds));*/
            }

            return transaction;
        }

        // Attempts to retrieve a transaction from memory or from storage
        // Returns null if no transaction is found
        public static Transaction getUnappliedTransaction(byte[] txid)
        {
            lock(stateLock)
            {
                if(unappliedTransactions.ContainsKey(txid))
                {
                    return unappliedTransactions[txid];
                }
            }
            return null;
        }

        // Removes all transactions from TransactionPool linked to a block.
        public static bool redactTransactionsForBlock(Block block)
        {
            if (block == null)
                return false;

            lock (stateLock)
            {
                foreach (byte[] txid in block.transactions)
                {
                    appliedTransactions.Remove(txid);
                }
            }
            return true;
        }

        // Re-adds all transactions from storage to TransactionPool linked to a block.
        public static bool unredactTransactionsForBlock(Block block)
        {
            if (block == null)
                return false;

            lock (stateLock)
            {
                foreach (byte[] txid in block.transactions)
                {
                    Transaction tx = getAppliedTransaction(txid, block.blockNum, true);
                    if(tx == null)
                    {
                        Logging.error("Error occurred while fetching transaction {0} for block #{1} from storage.", Transaction.getTxIdString(txid), block.blockNum);
                        return false;
                    }
                    appliedTransactions.Add(txid, tx);
                }
            }
            return true;
        }

        public static Transaction[] getAppliedTransactions(int from_index, int count)
        {
            lock (stateLock)
            {
                List<Transaction> full_tx_list = new List<Transaction>();
                var tx_list = appliedTransactions.Skip(from_index).Take(count).ToArray();
                foreach(var entry in tx_list)
                {
                    Transaction tx = entry.Value;
                    if(tx == null)
                    {
                        tx = getAppliedTransaction(entry.Key);
                    }
                    full_tx_list.Add(tx);
                }
                return full_tx_list.ToArray();
            }
        }

        public static Transaction[] getUnappliedTransactions()
        {
            lock (stateLock)
            {
                return unappliedTransactions.Values.ToArray();
            }
        }

        public static Transaction[] getUnappliedTransactions(int from_index, int count)
        {
            lock (stateLock)
            {
                return unappliedTransactions.Values.Skip(from_index).Take(count).ToArray();
            }
        }

        // This updates a pre-existing transaction
        // Returns true if the transaction has been updated, false otherwise
        // TODO TODO TODO we'll run into problems with this because of the new txid, needs to be done differently, commenting this function out for now
        /*
        public static bool updateTransaction(Transaction transaction)
        {
            Logging.info(String.Format("Received transaction {0} - {1} - {2}.", transaction.id, transaction.checksum, transaction.amount));

            if (!verifyTransaction(transaction))
            {
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
                        if (tx.applied == 0)
                        {
                            tx.amount = transaction.amount;
                            tx.data = transaction.data;
                            tx.nonce = transaction.nonce
                            tx.checksum = transaction.checksum;

                            // Broadcast this transaction update to the network
                            //ProtocolMessage.broadcastProtocolMessage(ProtocolMessageCode.updateTransaction, transaction.getBytes());

                            // Also update the transaction to storage
                            Meta.Storage.insertTransaction(transaction);

                            Logging.info(String.Format("Updated transaction {0} - {1} - {2}.", transaction.id, transaction.checksum, transaction.amount));

                            return true;
                        }
                        else
                        {
                            Logging.info(String.Format("Transaction was already applied, not updating {0} - {1} - {2}.", transaction.id, transaction.checksum, transaction.amount));
                        }
                    }
                }

            }

            return false;
        }*/

        // Verify if a PoW transaction is valid
        public static bool verifyPoWTransaction(Transaction tx, out ulong blocknum, out byte[] nonce_bytes, int block_version = -1, bool verify_pow = true)
        {
            blocknum = 0;
            nonce_bytes = null;

            if (tx.type != (int)Transaction.Type.PoWSolution)
                return false;

            if (Node.blockChain.getLastBlockNum() >= ConsensusConfig.miningExpirationBlockHeight)
            {
                Logging.warn("Received a PoW transaction {0}. Mining has stopped after block #{1} but current block height is #{2}.", tx.getTxIdString(), ConsensusConfig.miningExpirationBlockHeight, Node.blockChain.getLastBlockNum());
                return false;
            }

            blocknum = tx.powSolution.blockNum;
            nonce_bytes = tx.powSolution.nonce;

            if(blocknum > IxianHandler.getLastBlockHeight())
            {
                return false;
            }

            // ignore PoW solutions for first 100 blocks in the redacted window
            if(block_version >= 3 && blocknum + ConsensusConfig.getRedactedWindowSize(block_version) - 100 < IxianHandler.getLastBlockHeight())
            {
                return false;
            }

            try
            {
                Block block = Node.blockChain.getBlock(blocknum, false, false);

                if(block == null)
                {
                    return false;
                }

                // Check if the block has an empty PoW field
                if (block.powField != null)
                {
                    Logging.warn("PoW already applied");
                    return false;
                }

                if (verify_pow == false || tx.powVerified)
                {
                    return true;
                }

                Address primary_address = tx.pubKey;

                if (block.version == BlockVer.v0)
                {
                    // Verify the nonce
                    if ((tx.fromLocalStorage && !Config.fullStorageDataVerification) || Miner.verifyNonce_v0(UTF8Encoding.UTF8.GetString(nonce_bytes), blocknum, primary_address.addressWithChecksum, block.difficulty))
                    {
                        tx.powVerified = true;
                        return true;
                    }
                }
                else if (block.version == BlockVer.v1)
                {
                    // Verify the nonce
                    if ((tx.fromLocalStorage && !Config.fullStorageDataVerification) || Miner.verifyNonce_v1(UTF8Encoding.UTF8.GetString(nonce_bytes), blocknum, primary_address.addressWithChecksum, block.difficulty))
                    {
                        tx.powVerified = true;
                        return true;
                    }
                }
                else if (block.version <= BlockVer.v4)
                {
                    // Verify the nonce
                    if ((tx.fromLocalStorage && !Config.fullStorageDataVerification) || Miner.verifyNonce_v2(nonce_bytes, blocknum, primary_address.addressWithChecksum, block.difficulty))
                    {
                        tx.powVerified = true;
                        return true;
                    }
                }
                else if(block.version <= BlockVer.v9)
                {
                    // Verify the nonce
                    if ((tx.fromLocalStorage && !Config.fullStorageDataVerification) || Miner.verifyNonce_v3(nonce_bytes, blocknum, primary_address.addressWithChecksum, block.difficulty))
                    {
                        tx.powVerified = true;
                        return true;
                    }
                }
                else // >= 10
                {
                    // Verify the nonce
                    if ((tx.fromLocalStorage && !Config.fullStorageDataVerification) || Miner.verifyNonce_v3(nonce_bytes, blocknum, primary_address.addressNoChecksum, block.difficulty))
                    {
                        tx.powVerified = true;
                        return true;
                    }
                }
            }
            catch (Exception e)
            {
                Logging.warn("Error verifying PoW Transaction: {0}. Message: {1}", tx.getTxIdString(), e.Message);
            }

            return false;
        }

        public static bool setAppliedFlagToTransactionsFromBlock(Block b)
        {
            if (b == null)
            {
                return true;
            }
            lock (stateLock)
            {
                Dictionary<ulong, List<BlockSolution>> blockSolutionsDictionary = new Dictionary<ulong, List<BlockSolution>>();
                foreach (byte[] txid in b.transactions)
                {
                    Transaction tx = getUnappliedTransaction(txid);
                    if (tx == null)
                    {
                        Logging.error("Attempted to set applied to transaction from block #{0} ({1}), but transaction {{ {2} }} was missing.",
                            b.blockNum, Crypto.hashToString(b.blockChecksum), Transaction.getTxIdString(txid));
                        return false;
                    }
                    applyPowTransaction(tx, b, blockSolutionsDictionary, null);
                    setAppliedFlag(txid, b.blockNum, !tx.fromLocalStorage);
                }
                // set PoW fields
                for (int i = 0; i < blockSolutionsDictionary.Count; i++)
                {
                    ulong blockNum = blockSolutionsDictionary.Keys.ElementAt(i);

                    Block block = Node.blockChain.getBlock(blockNum, false, true);
                    // Check if the block is valid
                    if (block == null)
                        continue;

                    if (block.powField == null)
                    {
                        Node.blockChain.increaseSolvedBlocksCount();
                        // Set the powField as a checksum of all miners for this block
                        block.powField = BitConverter.GetBytes(b.blockNum);
                        Node.blockChain.updateBlock(block);
                    }
                }
            }
            return true;
        }

        public static bool applyStakingTransactionsFromBlock(Block block, List<Transaction> failed_staking_transactions)
        {
            ulong stakingRewardBlockNum;
            if (block.version >= BlockVer.v10)
            {
                if (block.blockNum <= ConsensusConfig.rewardMaturity + 1)
                {
                    return true;
                }
                stakingRewardBlockNum = block.blockNum -ConsensusConfig.rewardMaturity;
                if (Node.blockChain.getBlock(stakingRewardBlockNum - 1).version < BlockVer.v10)
                {
                    return true;
                }
            }
            else
            {
                stakingRewardBlockNum = block.blockNum - 6;
            }
            // TODO: move this to a seperate function. Left here for now for dev purposes
            // Apply any staking transactions in the pool at this moment
            List<Transaction> staking_txs = null;
            if (Node.walletState.inTransaction)
            {
                if (Config.fullBlockLogging) { Logging.info("Applying block #{0} -> applyStakingTransactionsFromBlock - generating staking transactions (transaction = {1})", block.blockNum, Node.walletState.inTransaction); }
                staking_txs = Node.blockProcessor.generateStakingTransactions(stakingRewardBlockNum, block.version, block.timestamp);
                if (Config.fullBlockLogging) { Logging.info("Applying block #{0} -> applyStakingTransactionsFromBlock - generated {1} staking transactions", block.blockNum, staking_txs.Count); }
            }
            else
            {
                lock (stateLock)
                {
                    if (Config.fullBlockLogging) { Logging.info("Applying block #{0} -> applyStakingTransactionsFromBlock - selecting staking transactions from pool (transaction = {1})", block.blockNum, Node.walletState.inTransaction); }
                    staking_txs = unappliedTransactions.Select(e => e.Value).Where(x => x != null && x.type == (int)Transaction.Type.StakingReward).ToList();
                    if (Config.fullBlockLogging) { Logging.info("Applying block #{0} -> applyStakingTransactionsFromBlock - selected {1} staking transactions", block.blockNum, staking_txs.Count); }
                }
            }

            HashSet<byte[]> stakingTxIds = block.transactions.Where(x => x[0] == 0).ToHashSet(new ByteArrayComparer());
            if (Config.fullBlockLogging) { Logging.info("Applying block #{0} -> applyStakingTransactionsFromBlock - block has {1} staking transaction ids", block.blockNum, stakingTxIds.Count); }

            foreach (Transaction tx in staking_txs)
            {
                if (!stakingTxIds.Remove(tx.id))
                {
                    Logging.error("Invalid staking txid in transaction pool {0}, removing from pool.", tx.getTxIdString());
                    lock(stateLock)
                    {
                        unappliedTransactions.Remove(tx.id);
                    }
                    continue;
                }

                if (tx.readyToApply > 0 || tx.applied > 0)
                    continue;

                ulong txbnum;
                if (block.version >= BlockVer.v10)
                {
                    txbnum = tx.toList.First().Value.data.GetIxiVarUInt(0).num;
                }
                else
                {
                    txbnum = BitConverter.ToUInt64(tx.toList.First().Value.data, 0);
                }

                if (txbnum != stakingRewardBlockNum)
                {
                    return false;
                }

                // Special case for Staking Reward transaction
                // Do not apply them if we are synchronizing
                // TODO: note that this can backfire when recovering completely from a file
                if (Node.blockSync.synchronizing && Config.recoverFromFile == false && Config.storeFullHistory == false && Config.fullStorageDataVerification == false)
                    continue;

                if (Config.fullBlockLogging) { Logging.info("Applying block #{0} -> applyStakingTransactionsFromBlock - applying staking transaction {1}", block.blockNum, tx.getTxIdString()); }
                if (applyStakingTransaction(tx, block, failed_staking_transactions))
                {
                    //Console.WriteLine("!!! APPLIED STAKE {0}", tx.id);
                    continue;
                }else
                {
                    return false;
                }
            }

            if(stakingTxIds.Count > 0)
            {
                return false;
            }

            return true;
        }

        // This applies all the transactions from a block to the actual walletstate.
        // It removes the failed transactions as well from the pool and block.
        public static bool applyTransactionsFromBlock(Block block, bool generating_new = false)
        {
            if (block == null)
            {
                return false;
            }

            try
            {
                // Maintain a dictionary of block solutions and the corresponding miners for solved blocks
                Dictionary<ulong, List<BlockSolution>> blockSolutionsDictionary = new Dictionary<ulong, List<BlockSolution>>();

                // Maintain a list of failed transactions to remove them from the TxPool in one go
                List<Transaction> failed_transactions = new List<Transaction>();

                List<Transaction> failed_staking_transactions = new List<Transaction>();

                if (Config.fullBlockLogging) { Logging.info("Applying block #{0} -> applyTransactionsFromBlock (transaction = {1})", block.blockNum, Node.walletState.inTransaction); }

                if (!applyStakingTransactionsFromBlock(block, failed_staking_transactions))
                {
                    if (Config.fullBlockLogging) { Logging.info("Applying block #{0} -> applyStakingTransactionsFromBlock failed!", block.blockNum); }
                    return false;
                }

                // Remove all failed transactions from the TxPool
                foreach (Transaction tx in failed_staking_transactions)
                {
                    Logging.warn("Removing failed staking transaction #{0} from pool.", tx.getTxIdString());
                    if (tx.applied == 0)
                    {
                        lock (stateLock)
                        {
                            // Remove from TxPool
                            unappliedTransactions.Remove(tx.id);
                        }
                    }
                    else
                    {
                        Logging.error("Error, attempting to remove failed transaction #{0} from pool, that was already applied.", tx.getTxIdString());
                    }
                    //block.transactions.Remove(tx.id);
                }
                if (failed_staking_transactions.Count > 0)
                {
                    failed_staking_transactions.Clear();
                    Logging.error(string.Format("Block #{0} has failed staking transactions, rejecting the block.", block.blockNum));
                    return false;
                }

                if (Config.fullBlockLogging) { Logging.info("Applying block #{0} -> applying other transactions ({1} transactions)!", block.blockNum, block.transactions.Count); }
                foreach (byte[] txid in block.transactions)
                {
                    // Skip staking txids
                    if (txid[0] == 0) // stk
                    {
                        if (Node.blockSync.synchronizing && !Config.recoverFromFile && !Config.storeFullHistory)
                        {
                            if (getUnappliedTransaction(txid) == null)
                            {
                                Logging.info(string.Format("Missing staking transaction during sync: {0}", txid));
                            }
                        }
                        continue;
                    }

                    Transaction tx = getUnappliedTransaction(txid);
                    if (Config.fullBlockLogging) { Logging.info("Applying block #{0} -> transaction {1}: Type: {2}, Amount: {3}", block.blockNum, tx.getTxIdString(), tx.type, tx.amount.getAmount()); }
                    if (tx == null)
                    {
                        Logging.error("Attempted to apply transactions from block #{0} ({1}), but transaction {{ {2} }} was missing.",
                            block.blockNum, Crypto.hashToString(block.blockChecksum), Transaction.getTxIdString(txid));
                        return false;
                    }

                    if (tx.type == (int)Transaction.Type.StakingReward)
                    {
                        continue;
                    }

                    // Special case for Genesis transactions
                    if (applyGenesisTransaction(tx, block, failed_transactions))
                    {
                        continue;
                    }


                    if(block.version >= BlockVer.v3)
                    {
                        if (block.version < BlockVer.v10
                            || tx.type != (int)Transaction.Type.PoWSolution)
                        {
                            if (Config.fullBlockLogging) { Logging.info("Checking if transaction has a public key set - {0} B", tx.pubKey == null ? 0 : tx.pubKey.getInputBytes().Length); }
                            Address tmp_address = tx.pubKey;
                            // Update the walletstate public key
                            byte[] pubkey = Node.walletState.getWallet(tmp_address).publicKey;
                            // Generate an address from the public key and compare it with the sender
                            if (pubkey == null)
                            {
                                if (Config.fullBlockLogging)
                                {
                                    Logging.info("Applying block #{0} -> transaction {1}: Originator Wallet ({{ {2} }}) does not have pubkey yet, setting.", block.blockNum, tx.getTxIdString(),
                                        tmp_address.ToString());
                                }
                                // There is no supplied public key, extract it from transaction
                                pubkey = tx.pubKey.pubKey;
                                if (pubkey != null)
                                {
                                    // Update the walletstate public key
                                    Node.walletState.setWalletPublicKey(tmp_address, pubkey);
                                }
                            }
                        }
                    }

                    // Special case for PoWSolution transactions
                    if (Config.fullBlockLogging) { Logging.info("Applying block #{0} -> transaction {1}: attempting as PoW", block.blockNum, tx.getTxIdString()); }
                    if (applyPowTransaction(tx, block, blockSolutionsDictionary, failed_transactions))
                    {
                        if (Config.fullBlockLogging) { Logging.info("Applying block #{0} -> transaction {1}: as PoW succeeded", block.blockNum, tx.getTxIdString(), tx.type, tx.amount.getAmount()); }
                        continue;
                    }

                    // Check the transaction amount
                    if (tx.amount == (long)0
                        && tx.type != (int)Transaction.Type.ChangeMultisigWallet
                        && tx.type != (int)Transaction.Type.MultisigAddTxSignature
                        )
                    {
                        failed_transactions.Add(tx);
                        continue;
                    }

                    if (block.version < BlockVer.v3)
                    {
                        Address tmp_address = tx.pubKey;
                        // Update the walletstate public key
                        byte[] pubkey = Node.walletState.getWallet(tmp_address).publicKey;
                        // Generate an address from the public key and compare it with the sender
                        if (pubkey == null)
                        {
                            // There is no supplied public key, extract it from transaction
                            pubkey = tx.pubKey.pubKey;
                            if (pubkey != null)
                            {
                                // Update the walletstate public key
                                Node.walletState.setWalletPublicKey(tmp_address, pubkey);
                            }
                        }
                    }

                    // Special case for Multisig
                    if (tx.type == (int)Transaction.Type.MultisigTX)
                    {
                        List<byte[]> related_tx_ids = applyMultisigTransaction(tx, block, failed_transactions);
                        if(related_tx_ids == null)
                        {
                            Logging.error(string.Format("Block #{0} has failed multisig transactions, rejecting the block.", block.blockNum));
                            if(generating_new)
                            {
                                continue;
                            }
                            return false;
                        }
                        else
                        {
                            applyMultisigRelatedTransactions(related_tx_ids, block, failed_transactions);
                        }
                        continue;
                    }
                    if (tx.type == (int)Transaction.Type.ChangeMultisigWallet)
                    {
                        List<byte[]> related_tx_ids = applyMultisigChangeTransaction(tx, block, failed_transactions);
                        if (related_tx_ids == null)
                        {
                            Logging.error(string.Format("Block #{0} has failed multisig transactions, rejecting the block.", block.blockNum));
                            if (generating_new)
                            {
                                continue;
                            }
                            return false;
                        }
                        else
                        {
                            applyMultisigRelatedTransactions(related_tx_ids, block, failed_transactions);
                        }
                        continue;
                    }else if(tx.type == (int)Transaction.Type.MultisigAddTxSignature)
                    {
                        // continue as this is processed by applyMultisigRelatedTransactions
                        continue;
                    }

                    // If we reached this point, it means this is a normal transaction
                    if (Config.fullBlockLogging) { Logging.info("Applying block #{0} -> transaction {1}: attempting as normal", block.blockNum, tx.getTxIdString()); }
                    applyNormalTransaction(tx, block, failed_transactions);

                }

                // Finally, Check if we have any miners to reward
                if (block.version < BlockVer.v10)
                {
                    if (blockSolutionsDictionary.Count > 0)
                    {
                        if (Config.fullBlockLogging) { Logging.info("Applying block #{0} -> Block solutions: {1} - rewarding miners...", block.blockNum, blockSolutionsDictionary.Count); }
                        rewardMiners(block.blockNum, blockSolutionsDictionary);
                    }
                }else
                {
                    if (Config.fullBlockLogging) { Logging.info("Applying block #{0} -> Block solutions: {1} - rewarding miners...", block.blockNum, blockSolutionsDictionary.Count); }
                    rewardMinersV10(block, blockSolutionsDictionary);
                }

                // Clear the solutions dictionary
                blockSolutionsDictionary.Clear();

                // double-check if there are any unapplied transactions and if so reject the block
                if (!Node.walletState.inTransaction)
                {
                    foreach (byte[] txid in block.transactions)
                    {
                        Transaction tx = getUnappliedTransaction(txid);
                        if (tx == null || tx.readyToApply != block.blockNum)
                        {
                            Logging.error(string.Format("Block #{0} has unapplied transactions, rejecting the block.", block.blockNum));
                            return false;
                        }
                        setAppliedFlag(tx.id, block.blockNum);
                    }
                }

                // Remove all failed transactions from the TxPool and block
                foreach (Transaction tx in failed_transactions)
                {
                    Logging.warn(String.Format("Removing failed transaction #{0} from pool.", tx.getTxIdString()));
                    // Remove from TxPool
                    if (tx.applied == 0)
                    {
                        lock (stateLock)
                        {
                            unappliedTransactions.Remove(tx.id);
                        }
                    }
                    else
                    {
                        Logging.error("Error, attempting to remove failed transaction #{0} from pool, that was already applied.", tx.getTxIdString());
                    }
                }
                if (failed_transactions.Count > 0)
                {
                    failed_transactions.Clear();
                    Logging.error(string.Format("Block #{0} has failed transactions, rejecting the block.", block.blockNum));
                    return false;
                }
            }
            catch (Exception e)
            {
                Logging.error(string.Format("Error applying transactions from block #{0}. Message: {1}", block.blockNum, e));
                return false;
            }
            
            return true;
        }

        // Checks if a transaction is a pow transaction and applies it.
        // Returns true if it's a PoW transaction, otherwise false
        // be careful when changing/updating ws_snapshot related things in this function as the parameter relies on sync as well
        private static bool applyPowTransaction(Transaction tx, Block block, IDictionary<ulong, List<BlockSolution>> blockSolutionsDictionary, List<Transaction> failedTransactions)
        {
            if (tx.type != (int)Transaction.Type.PoWSolution)
            {
                return false;
            }

            ulong minBh = 0;
            if (block.blockNum > ConsensusConfig.getRedactedWindowSize(block.version))
            {
                minBh = block.blockNum - ConsensusConfig.getRedactedWindowSize(block.version);
            }
            // Check the block height
            if (minBh > tx.blockHeight || tx.blockHeight > block.blockNum)
            {
                Logging.warn("Incorrect block height for transaction {0}. Tx block height is {1}, expecting at least {2} and at most {3}", tx.getTxIdString(), tx.blockHeight, minBh, block.blockNum + 5);
                failedTransactions.Add(tx);
                return false;
            }

            // Update the block's applied field
            if (!Node.walletState.inTransaction)
            {
                //setAppliedFlag(tx.id, block.blockNum);
                setReadyToApplyFlag(tx, block.blockNum);
            }

            bool verify_pow = true;
            if(tx.fromLocalStorage && !Config.fullStorageDataVerification)
            {
                verify_pow = false;
            }

            // Verify if the solution is correct
            if (verifyPoWTransaction(tx, out ulong powBlockNum, out byte[] nonce, block.version, verify_pow) == true)
            {
                // Check if we already have a key matching the block number
                if (blockSolutionsDictionary.ContainsKey(powBlockNum) == false)
                {
                    blockSolutionsDictionary[powBlockNum] = new List<BlockSolution>();
                }
                Address primary_address = tx.pubKey;
                if (block.version < BlockVer.v2)
                {
                    blockSolutionsDictionary[powBlockNum].Add(new BlockSolution { address = primary_address, nonce = nonce, tx = tx });
                }
                else
                {
                    if (!blockSolutionsDictionary[powBlockNum].Exists(x => x.address.addressNoChecksum.SequenceEqual(primary_address.addressNoChecksum) && x.nonce.SequenceEqual(nonce)))
                    {
                        // Add the miner to the block number dictionary reward list
                        blockSolutionsDictionary[powBlockNum].Add(new BlockSolution { address = primary_address, nonce = nonce, tx = tx });
                    }
                    else
                    {
                        if (failedTransactions != null)
                        {
                            Logging.warn("Duplicate PoW transaction {0}.", tx.getTxIdString());
                            failedTransactions.Add(tx);
                        }
                    }
                }
            }else
            {
                if (failedTransactions != null)
                {
                    Logging.warn("Invalid PoW transaction {0}.", tx.getTxIdString());
                    failedTransactions.Add(tx);
                }
            }

            return true;
        }

        // Checks if a transaction is a genesis transaction and applies it.
        // Returns true if it's a PoW transaction, otherwise false
        public static bool applyGenesisTransaction(Transaction tx, Block block, List<Transaction> failed_transactions)
        {
            if (tx.type != (int)Transaction.Type.Genesis)
            {
                return false;
            }

            // Check for the genesis block first
            if (block.blockNum > 1)
            {
                // Add it to the failed transactions list
                Logging.error("Genesis transaction {0} detected after block #1. Ignored.", tx.getTxIdString());
                failed_transactions.Add(tx);
                return true;
            }

            // Apply the amount
            foreach (var entry in tx.toList)
            {
                Node.walletState.setWalletBalance(entry.Key, entry.Value.amount);
            }

            if (!Node.walletState.inTransaction)
            {
                //setAppliedFlag(tx.id, block.blockNum);
                setReadyToApplyFlag(tx, block.blockNum);
            }

            return true;
        }

        // Checks if a transaction is a staking transaction and applies it.
        // Returns true if it's a Staking transaction, otherwise false
        public static bool applyStakingTransaction(Transaction tx, Block block, List<Transaction> failed_transactions)
        {
            if (tx.type != (int)Transaction.Type.StakingReward)
            {
                return false;
            }
            // Check if the transaction is in the sigfreeze
            // TODO: refactor this and make it more efficient
            ulong blocknum;

            if (block.version >= BlockVer.v10)
            {
                blocknum = tx.toList.First().Value.data.GetIxiVarUInt(0).num;
            }
            else
            {
                blocknum = BitConverter.ToUInt64(tx.toList.First().Value.data, 0);
            }

            // Verify the staking transaction is accurate
            Block targetBlock = Node.blockChain.getBlock(blocknum);
            if (targetBlock == null)
            {
                failed_transactions.Add(tx);
                return true;
            }
            if (block.version >= BlockVer.v10 && targetBlock.getFrozenSignatureCount() != tx.toList.Count)
            {
                failed_transactions.Add(tx);
                return true;
            }
            List<Address> blockStakers = new List<Address>();
            foreach (var toEntry in tx.toList)
            {
                // Check if the staker's transaction has already been processed
                bool valid = true;
                if(blockStakers.Exists(x => x.addressNoChecksum.SequenceEqual(toEntry.Key.addressNoChecksum)))
                {
                    valid = false;
                }

                // If there's another staking transaction for the staker in this block, ignore
                if (valid == false)
                {
                    Logging.error("There's a duplicate staker transaction {0}.", tx.getTxIdString());
                    failed_transactions.Add(tx);
                    return true;
                }

                Wallet staking_wallet = Node.walletState.getWallet(toEntry.Key);
                IxiNumber staking_balance_before = staking_wallet.balance;

                IxiNumber tx_amount = toEntry.Value.amount;

                if (tx_amount < new IxiNumber(new System.Numerics.BigInteger(1)))
                {
                    Logging.error("Staking transaction {0} does not have a positive amount.", tx.getTxIdString());
                    failed_transactions.Add(tx);
                    return true;
                }

                valid = false;
                var signatureWallets = targetBlock.getSignaturesWalletAddressesWithDifficulty();
                foreach (var wallet_addr_diff in signatureWallets)
                {
                    Address wallet_addr = wallet_addr_diff.address;
                    if (toEntry.Key.addressNoChecksum.SequenceEqual(wallet_addr.addressNoChecksum))
                    {
                        valid = true;
                        break;
                    }
                }
                if (valid == false)
                {
                    Logging.error("Staking transaction {0} does not have a corresponding block signature.", tx.getTxIdString());
                    failed_transactions.Add(tx);
                    return true;
                }

                // Deposit the amount
                IxiNumber staking_balance_after = staking_balance_before + tx_amount;

                Node.walletState.setWalletBalance(toEntry.Key, staking_balance_after);

                if (block.version < BlockVer.v10)
                {
                    if (staking_wallet.publicKey == null)
                    {
                        var sig = targetBlock.getSignature(toEntry.Key);
                        if (sig == null || sig.recipientPubKeyOrAddress.pubKey == null)
                        {
                            Logging.error("Signer wallet's pubKey entry is null, expecting a non-null entry for address: " + sig.ToString());
                            if (sig != null)
                            {
                                try
                                {
                                    targetBlock.signatures.Remove(sig);
                                    if (targetBlock.frozenSignatures != null)
                                    {
                                        targetBlock.frozenSignatures.Remove(sig);
                                    }
                                }
                                catch (Exception e)
                                {
                                    Logging.warn("Exception occurred in applyStakingTransaction() " + e);
                                }
                            }
                            failed_transactions.Add(tx);
                            return true;
                        }
                        // Set the WS public key
                        Node.walletState.setWalletPublicKey(toEntry.Key, sig.recipientPubKeyOrAddress.pubKey);
                    }
                }

                blockStakers.Add(toEntry.Key);
            }

            if (!Node.walletState.inTransaction)
            {
                //setAppliedFlag(tx.id, block.blockNum);
                setReadyToApplyFlag(tx, block.blockNum);
            }

            return true;
        }

        public static List<byte[]> applyMultisigTransaction(Transaction tx, Block block, List<Transaction> failed_transactions)
        {
            if (tx.type == (int)Transaction.Type.MultisigTX)
            {
                ulong minBh = 0;
                if (block.blockNum > ConsensusConfig.getRedactedWindowSize(block.version))
                {
                    minBh = block.blockNum - ConsensusConfig.getRedactedWindowSize(block.version);
                }
                // Check the block height
                if (minBh > tx.blockHeight || tx.blockHeight > block.blockNum)
                {
                    Logging.warn("Incorrect block height for transaction {0}. Tx block height is {1}, expecting at least {2} and at most {3}", tx.getTxIdString(), tx.blockHeight, minBh, block.blockNum + 5);
                    failed_transactions.Add(tx);
                    return null;
                }

                if (tx.fromList.Count > 1)
                {
                    Logging.error("Multisig transaction {{ {0} }} has more than one 'from' address!", tx.getTxIdString());
                    failed_transactions.Add(tx);
                    return null;
                }
                object multisig_type = tx.GetMultisigData();
                Address from_address = new Address(tx.pubKey.addressNoChecksum, tx.fromList.First().Key);
                Wallet orig = Node.walletState.getWallet(from_address);
                if (orig is null)
                {
                    Logging.error("Multisig transaction {{ {0} }} names a non-existent wallet {1}.", tx.getTxIdString(), from_address.ToString());
                    failed_transactions.Add(tx);
                    return null;
                }
                if (orig.type != WalletType.Multisig)
                {
                    Logging.error("Attempted to apply a multisig TX where the originating wallet is not a multisig wallet! Wallet: {0}, Transaction: {{ {1} }}.",
                        from_address.ToString(), tx.getTxIdString());
                    failed_transactions.Add(tx);
                    return null;
                }

                List<byte[]> related_tx_ids = new List<byte[]>();
                // we only attempt to execute the original transaction, if there are enough signatures
                if (multisig_type is Transaction.MultisigTxData)
                {
                    var multisig_obj = (Transaction.MultisigTxData)multisig_type;
                    if (multisig_obj.origTXId == null)
                    {
                        related_tx_ids = getRelatedMultisigTransactions(tx.id, block);
                        // +1, because the search will not find the current transaction, only the ones related to it
                        int num_multisig_txs = related_tx_ids.Count + 1;
                        if (num_multisig_txs < orig.requiredSigs)
                        {
                            Logging.error("Multisig transaction {{ {0} }} doesn't have enough signatures!", tx.getTxIdString());
                            failed_transactions.Add(tx);
                            return null;
                        }

                        Address signer_address = new Address(multisig_obj.signerPubKey, multisig_obj.signerNonce);
                        if (!orig.isValidSigner(signer_address))
                        {
                            Logging.warn("Tried to use Multisig transaction {{ {0} }} without being an actual owner {1}!",
                                tx.getTxIdString(), signer_address.ToString());
                            failed_transactions.Add(tx);
                            return null;
                        }
                    }
                    else if (multisig_obj.origTXId != null)
                    {
                        // this is a related transaction, which we ignore, because all processing is done on the origin transaction
                        return null;
                    }
                    else
                    {
                        Logging.error("Multisig transaction {{ {0} }} has invalid multisig data!", tx.getTxIdString());
                        failed_transactions.Add(tx);
                        return null;
                    }
                }
                else
                {
                    Logging.error("Multisig transaction {{ {0} }} has invalid multisig data!", tx.getTxIdString());
                    failed_transactions.Add(tx);
                    return null;
                }
                // it processes as normal
                if(applyNormalTransaction(tx, block, failed_transactions))
                {
                    return related_tx_ids;
                }
            }
            return null;
        }

        public static bool applyMultisigRelatedTransactions(List<byte[]> related_tx_ids, Block block, List<Transaction> failed_transactions)
        {
            foreach (byte[] txid in related_tx_ids)
            {
                Transaction tx = getUnappliedTransaction(txid);
                if (tx.type == (int)Transaction.Type.MultisigAddTxSignature)
                {
                    ulong minBh = 0;
                    if (block.blockNum > ConsensusConfig.getRedactedWindowSize(block.version))
                    {
                        minBh = block.blockNum - ConsensusConfig.getRedactedWindowSize(block.version);
                    }
                    // Check the block height
                    if (minBh > tx.blockHeight || tx.blockHeight > block.blockNum)
                    {
                        Logging.warn("Incorrect block height for multisig transaction {0}. Tx block height is {1}, expecting at least {2} and at most {3}", tx.getTxIdString(), tx.blockHeight, minBh, block.blockNum + 5);
                        failed_transactions.Add(tx);
                        return false;
                    }

                    if (tx.fromList.Count > 1)
                    {
                        Logging.error("Multisig transaction {{ {0} }} has more than one 'from' address!", tx.getTxIdString());
                        failed_transactions.Add(tx);
                        return false;
                    }

                    foreach (var entry in tx.fromList)
                    {
                        Address tmp_address = new Address(tx.pubKey.addressNoChecksum, entry.Key);

                        Wallet source_wallet = Node.walletState.getWallet(tmp_address);
                        IxiNumber source_balance_before = source_wallet.balance;
                        if (source_balance_before < entry.Value)
                        {
                            Logging.warn("Multisig transaction {{ {0} }} in block #{1} ({2}) would take wallet {3} below zero.",
                                tx.getTxIdString(), block.blockNum, Crypto.hashToString(block.lastBlockChecksum), tmp_address.ToString());
                            failed_transactions.Add(tx);
                            return false;
                        }
                        // Withdraw the full amount, including fee
                        IxiNumber source_balance_after = source_balance_before - entry.Value;

                        Node.walletState.setWalletBalance(tmp_address, source_balance_after);
                    }

                    if (!Node.walletState.inTransaction)
                    {
                        //setAppliedFlag(tx.id, block.blockNum);
                        setReadyToApplyFlag(tx, block.blockNum);
                    }
                }
                else
                {
                    Logging.warn("Error processing multisig transaction {{ {0} }} in block #{1}, expecting type MultisigAddTxSignature, received {2}.",
                        tx.getTxIdString(), block.blockNum, tx.type);
                    failed_transactions.Add(tx);
                    return false;
                }
            }
            return true;
        }

        public static List<byte[]> applyMultisigChangeTransaction(Transaction tx, Block block, List<Transaction> failed_transactions)
        {
            if (tx.type == (int)Transaction.Type.ChangeMultisigWallet)
            {
                ulong minBh = 0;
                if (block.blockNum > ConsensusConfig.getRedactedWindowSize(block.version))
                {
                    minBh = block.blockNum - ConsensusConfig.getRedactedWindowSize(block.version);
                }
                // Check the block height
                if (minBh > tx.blockHeight || tx.blockHeight > block.blockNum)
                {
                    Logging.warn("Incorrect block height for transaction {0}. Tx block height is {1}, expecting at least {2} and at most {3}", tx.getTxIdString(), tx.blockHeight, minBh, block.blockNum + 5);
                    failed_transactions.Add(tx);
                    return null;
                }

                object multisig_type = tx.GetMultisigData();
                Address target_wallet_address = new Address(tx.pubKey.addressNoChecksum, tx.fromList.First().Key);
                Wallet orig = Node.walletState.getWallet(target_wallet_address);
                if (orig is null)
                {
                    Logging.error("Multisig change transaction {{ {0} }} names a non-existent wallet {1}.", tx.getTxIdString(), target_wallet_address.ToString());
                    failed_transactions.Add(tx);
                    return null;
                }
                List<byte[]> related_tx_ids = getRelatedMultisigTransactions(tx.id, block);
                // +1 because this current transaction will not be found by the search
                int num_valid_sigs = related_tx_ids.Count + 1;
                if (num_valid_sigs < orig.requiredSigs)
                {
                    Logging.error("Transaction {{ {0} }} has {1} valid signatures out of required {2}.", tx.getTxIdString(), num_valid_sigs, orig.requiredSigs);
                    failed_transactions.Add(tx);
                    return null;
                }

                if (multisig_type is Transaction.MultisigAddrAdd)
                {
                    var multisig_obj = (Transaction.MultisigAddrAdd)multisig_type;

                    Address signer_address = new Address(multisig_obj.signerPubKey, multisig_obj.signerNonce);
                    if (!orig.isValidSigner(signer_address))
                    {
                        Logging.warn("Tried to use Multisig transaction {{ {0} }} without being an actual owner {1}!",
                            tx.getTxIdString(), signer_address.ToString());
                        failed_transactions.Add(tx);
                        return null;
                    }

                    if (orig.isValidSigner(multisig_obj.addrToAdd))
                    {
                        Logging.warn("Pubkey {0} is already in allowed multisig list for wallet {1}.", multisig_obj.addrToAdd.ToString(), orig.id.ToString());
                        failed_transactions.Add(tx);
                        return null;
                    }

                    if (orig.countAllowedSigners > 250)
                    {
                        Logging.warn("MS Wallet {0} attempted to add signer {1}, but it already has maximum allowed signers.", multisig_obj.addrToAdd.ToString(), orig.id.ToString());
                        failed_transactions.Add(tx);
                        return null;
                    }

                    Logging.info("Adding multisig address {0} to wallet {1}.", multisig_obj.addrToAdd.ToString(), orig.id.ToString());
                    Node.walletState.addWalletAllowedSigner(orig.id, multisig_obj.addrToAdd);
                }
                else if (multisig_type is Transaction.MultisigAddrDel)
                {
                    if (orig.type != WalletType.Multisig)
                    {
                        Logging.error("Attempted to execute a multisig change transaction {{ {0} }} on a non-multisig wallet {1}!",
                            tx.getTxIdString(), orig.id.ToString());
                        failed_transactions.Add(tx);
                        return null;
                    }
                    var multisig_obj = (Transaction.MultisigAddrDel)multisig_type;

                    Address signer_address = new Address(multisig_obj.signerPubKey, multisig_obj.signerNonce);
                    if (!orig.isValidSigner(signer_address))
                    {
                        Logging.warn("Tried to use Multisig transaction {{ {0} }} without being an actual owner {1}!",
                            tx.getTxIdString(), signer_address.ToString());
                        failed_transactions.Add(tx);
                        return null;
                    }

                    if (multisig_obj.addrToDel.addressNoChecksum.SequenceEqual(orig.id.addressNoChecksum))
                    {
                        Logging.error("Attempted to remove wallet owner ({0}) from the multisig wallet!", multisig_obj.addrToDel.ToString());
                        failed_transactions.Add(tx);
                        return null;
                    }

                    bool adjust_req_sigs = false;
                    if (orig.requiredSigs > orig.countAllowedSigners)
                    {
                        Logging.info("Removing a signer would make using the wallet impossible. Adjusting required signatures: {0} -> {1}.",
                            orig.requiredSigs, orig.allowedSigners.Count);
                        adjust_req_sigs = true;
                    }
                    Logging.info("Removing multisig address {0} from wallet {1}.", multisig_obj.addrToDel.ToString(), orig.id.ToString());
                    Node.walletState.delWalletAllowedSigner(orig.id, multisig_obj.addrToDel, adjust_req_sigs);
                }
                else if (multisig_type is Transaction.MultisigChSig)
                {
                    var multisig_obj = (Transaction.MultisigChSig)multisig_type;
                    if (orig.type != WalletType.Multisig)
                    {
                        Logging.error("Attempted to execute a multisig change transaction {{ {0} }} on a non-multisig wallet {1}!",
                            tx.getTxIdString(), orig.id.ToString());
                        failed_transactions.Add(tx);
                        return null;
                    }

                    Address signer_address = new Address(multisig_obj.signerPubKey, multisig_obj.signerNonce);
                    if (!orig.isValidSigner(signer_address))
                    {
                        Logging.warn("Tried to use Multisig transaction {{ {0} }} without being an actual owner {1}!",
                            tx.getTxIdString(), signer_address.ToString());
                        failed_transactions.Add(tx);
                        return null;
                    }

                    // +1 because "allowedSigners" will contain addresses distinct from the wallet owner, but wallet owner is also one of the permitted signers
                    if (multisig_obj.reqSigs > orig.allowedSigners.Count + 1)
                    {
                        Logging.error("Attempted to set required sigs for a multisig wallet to a larger value than the number of allowed pubkeys! Pubkeys = {0}, reqSigs = {1}.",
                            orig.allowedSigners.Count, multisig_obj.reqSigs);
                        failed_transactions.Add(tx);
                        return null;
                    }
                    Logging.info("Changing multisig wallet {0} required sigs {1} -> {2}.", orig.id.ToString(), orig.requiredSigs, multisig_obj.reqSigs);
                    Node.walletState.setWalletRequiredSignatures(orig.id, multisig_obj.reqSigs);
                }

                foreach (var entry in tx.fromList)
                {
                    Address tmp_address = new Address(tx.pubKey.addressNoChecksum, entry.Key);

                    Wallet source_wallet = Node.walletState.getWallet(tmp_address);
                    IxiNumber source_balance_before = source_wallet.balance;
                    // Withdraw the full amount, including fee
                    IxiNumber source_balance_after = source_balance_before - entry.Value;
                    if (source_balance_after < (long)0)
                    {
                        Logging.warn("Transaction {{ {0} }} in block #{1} ({2}) would take wallet {3} below zero.",
                            tx.getTxIdString(), block.blockNum, Crypto.hashToString(block.lastBlockChecksum), tmp_address.ToString());
                        failed_transactions.Add(tx);
                        return null;
                    }

                    Node.walletState.setWalletBalance(tmp_address, source_balance_after);
                }

                if (!Node.walletState.inTransaction)
                {
                    //setAppliedFlag(tx.id, block.blockNum);
                    setReadyToApplyFlag(tx, block.blockNum);
                }
                return related_tx_ids;
            }
            return null;
        }


        // Applies a normal transaction
        public static bool applyNormalTransaction(Transaction tx, Block block, List<Transaction> failed_transactions)
        {
            // TODO: WSJ is withdrawing and depositing same addresses allowed in a single transaction?
            ulong minBh = 0;
            if (block.blockNum > ConsensusConfig.getRedactedWindowSize(block.version))
            {
                minBh = block.blockNum - ConsensusConfig.getRedactedWindowSize(block.version);
            }
            // Check the block height
            if (minBh > tx.blockHeight || tx.blockHeight > block.blockNum)
            {
                Logging.warn("Incorrect block height for transaction {0}. Tx block height is {1}, expecting at least {2} and at most {3}", tx.getTxIdString(), tx.blockHeight, minBh, block.blockNum + 5);
                failed_transactions.Add(tx);
                return false;
            }

            if(!verifyPremineTransaction(tx))
            {
                Logging.warn("Tried to spent too much premine, too early - transaction {0}.", tx.getTxIdString());
                failed_transactions.Add(tx);
                return false;
            }

            // Check if the fee covers the current network minimum fee
            // TODO: adjust this dynamically

            IxiNumber expectedFee = tx.calculateMinimumFee(ConsensusConfig.transactionPrice);
            if (tx.version == 0)
            {
                expectedFee = ConsensusConfig.transactionPrice;
            }
            if (tx.fee < expectedFee)
            {
                Logging.error("Transaction {{ {0} }} cannot pay minimum fee", tx.getTxIdString());
                failed_transactions.Add(tx);
                return false;
            }

            IxiNumber total_amount = 0;
            foreach (var entry in tx.fromList)
            {
                Address tmp_address = new Address(tx.pubKey.addressNoChecksum, entry.Key);

                Wallet source_wallet = Node.walletState.getWallet(tmp_address);
                IxiNumber source_balance_before = source_wallet.balance;
                // Withdraw the full amount, including fee
                IxiNumber source_balance_after = source_balance_before - entry.Value;
                if (source_balance_after < (long)0)
                {
                    Logging.warn("Transaction {{ {0} }} in block #{1} ({2}) would take wallet {3} below zero.",
                        tx.getTxIdString(), block.blockNum, Crypto.hashToString(block.lastBlockChecksum), tmp_address.ToString());
                    failed_transactions.Add(tx);
                    return false;
                }

                Node.walletState.setWalletBalance(tmp_address, source_balance_after);
                total_amount += entry.Value;
            }

            if (tx.amount + tx.fee != total_amount)
            {
                Logging.error("Transaction {{ {0} }}'s input values are different than the total amount + fee", tx.getTxIdString());
                failed_transactions.Add(tx);
                return false;
            }

            total_amount = 0;
            foreach (var entry in tx.toList)
            {
                Wallet dest_wallet = Node.walletState.getWallet(entry.Key);
                IxiNumber dest_balance_before = dest_wallet.balance;


                // Deposit the amount without fee, as the fee is distributed by the network a few blocks later
                IxiNumber dest_balance_after = dest_balance_before + entry.Value.amount;


                // Update the walletstate
                if (Config.fullBlockLogging)
                {
                    Logging.info("Normal transaction {{ {3} }} updates wallet {0} balance: {1} -> {2}.",
                        entry.Key.ToString(),
                        dest_balance_before.ToString(),
                        dest_balance_after.ToString(),
                        tx.getTxIdString());
                }
                Node.walletState.setWalletBalance(entry.Key, dest_balance_after);
                total_amount += entry.Value.amount;
            }

            if (tx.amount != total_amount)
            {
                Logging.error("Transaction {{ {0} }}'s output values are different than the total amount", tx.getTxIdString());
                failed_transactions.Add(tx);
                return false;
            }

            if (!Node.walletState.inTransaction)
            {
                //setAppliedFlag(tx.id, block.blockNum);
                setReadyToApplyFlag(tx, block.blockNum);
            }

            return true;
        }

        // Go through a dictionary of block numbers and respective miners and reward them
        private static void rewardMiners(ulong sent_block_num, IDictionary<ulong, List<BlockSolution>> blockSolutionsDictionary)
        {
            for (int i = 0; i < blockSolutionsDictionary.Count; i++)
            {
                ulong blockNum = blockSolutionsDictionary.Keys.ElementAt(i);

                Block block = Node.blockChain.getBlock(blockNum, true, true);
                // Check if the block is valid
                if (block == null)
                {
                    Logging.error("PoW target block {0} not found", blockNum);
                    continue;
                }

                var miners_to_reward = blockSolutionsDictionary[blockNum];

                IxiNumber miners_count = new IxiNumber(miners_to_reward.Count);

                IxiNumber pow_reward = ConsensusConfig.calculateMiningRewardForBlock(blockNum);
                IxiNumber powRewardPart = pow_reward / miners_count;

                //Logging.info(String.Format("Rewarding {0} IXI to block #{1} miners", powRewardPart.ToString(), blockNum));
                foreach (var entry in miners_to_reward)
                {
                    // Update the wallet state
                    Wallet miner_wallet = Node.walletState.getWallet(entry.address);
                    IxiNumber miner_balance_before = miner_wallet.balance;
                    IxiNumber miner_balance_after = miner_balance_before + powRewardPart;
                    Node.walletState.setWalletBalance(miner_wallet.id, miner_balance_after);

                    if (miner_wallet.id.addressNoChecksum.SequenceEqual(IxianHandler.getWalletStorage().getPrimaryAddress().addressNoChecksum))
                    {
                        ActivityStorage.updateValue(entry.tx.id, powRewardPart);
                    }
                }

                // Ignore if we're in a bigger transaction, which is not yet complete
                if (!Node.walletState.inTransaction)
                {
                    if (block.powField == null)
                    {
                        Node.blockChain.increaseSolvedBlocksCount();
                        // Set the powField as a checksum of all miners for this block
                        block.powField = BitConverter.GetBytes(sent_block_num);
                        Node.blockChain.updateBlock(block);
                    }
                }

            }
        }

        private static void rewardMinersV10(Block block, IDictionary<ulong, List<BlockSolution>> blockSolutionsDictionary)
        {
            ulong blockNum = block.blockNum;
            if (blockSolutionsDictionary.Count > 0)
            {
                setPoWAppliedFlagToBlocks(blockNum, blockSolutionsDictionary);
            }

            if(blockNum <= ConsensusConfig.rewardMaturity + 1)
            {
                return;
            }

            if (Node.blockChain.getBlock(blockNum - ConsensusConfig.rewardMaturity - 1).version < BlockVer.v10)
            {
                return;
            }

            Block targetBlock = Node.blockChain.getBlock(blockNum - ConsensusConfig.rewardMaturity);
            // Check if the block is valid
            if (targetBlock == null)
            {
                Logging.error("Mining reward target block {0} not found", blockNum);
                return;
            }

            // TODO could use caching
            Dictionary<ulong, List<BlockSolution>> targetBlockSolutionsDictionary = new Dictionary<ulong, List<BlockSolution>>();
            var powTransactions = TransactionPool.getFullBlockTransactions(targetBlock); // TODO TODO optimize this to fetch only PoW transactions - fetch tx by type
            foreach (var powTx in powTransactions)
            {
                if(powTx.type != (int)Transaction.Type.PoWSolution)
                {
                    continue;
                }
                ulong powBlockNum = powTx.powSolution.blockNum;
                byte[] nonce = powTx.powSolution.nonce;

                // Check if we already have a key matching the block number
                if (targetBlockSolutionsDictionary.ContainsKey(powBlockNum) == false)
                {
                    targetBlockSolutionsDictionary[powBlockNum] = new List<BlockSolution>();
                }
                Address primary_address = powTx.pubKey;
                if (block.version < BlockVer.v2)
                {
                    targetBlockSolutionsDictionary[powBlockNum].Add(new BlockSolution { address = primary_address, nonce = nonce, tx = powTx });
                }
                else
                {
                    if (!targetBlockSolutionsDictionary[powBlockNum].Exists(x => x.address.addressNoChecksum.SequenceEqual(primary_address.addressNoChecksum) && x.nonce.SequenceEqual(nonce)))
                    {
                        // Add the miner to the block number dictionary reward list
                        targetBlockSolutionsDictionary[powBlockNum].Add(new BlockSolution { address = primary_address, nonce = nonce, tx = powTx });
                    }
                    else
                    {
                        throw new Exception(String.Format("PoW solution for block {0} with the same signer address is already present", powBlockNum));
                    }
                }
            }

            for (int i = 0; i < targetBlockSolutionsDictionary.Count; i++)
            {
                ulong powBlockNum = targetBlockSolutionsDictionary.Keys.ElementAt(i);

                Block powBlock = Node.blockChain.getBlock(powBlockNum, true, true);
                // Check if the block is valid
                if (powBlock == null)
                {
                    throw new Exception(String.Format("PoW target block {0} not found", powBlockNum));
                }

                var miners_to_reward = targetBlockSolutionsDictionary[powBlockNum];

                IxiNumber miners_count = new IxiNumber(miners_to_reward.Count);

                IxiNumber pow_reward = ConsensusConfig.calculateMiningRewardForBlock(powBlockNum);
                IxiNumber powRewardPart = pow_reward / miners_count;

                //Logging.info(String.Format("Rewarding {0} IXI to block #{1} miners", powRewardPart.ToString(), blockNum));
                foreach (var entry in miners_to_reward)
                {
                    // Update the wallet state
                    Wallet miner_wallet = Node.walletState.getWallet(entry.address);
                    IxiNumber miner_balance_before = miner_wallet.balance;
                    IxiNumber miner_balance_after = miner_balance_before + powRewardPart;
                    Node.walletState.setWalletBalance(miner_wallet.id, miner_balance_after);

                    if (miner_wallet.id.addressNoChecksum.SequenceEqual(IxianHandler.getWalletStorage().getPrimaryAddress().addressNoChecksum))
                    {
                        ActivityStorage.updateValue(entry.tx.id, powRewardPart);
                    }
                }
            }
        }

        private static void setPoWAppliedFlagToBlocks(ulong sent_block_num, IDictionary<ulong, List<BlockSolution>> blockSolutionsDictionary)
        {
            for (int i = 0; i < blockSolutionsDictionary.Count; i++)
            {
                ulong blockNum = blockSolutionsDictionary.Keys.ElementAt(i);

                Block block = Node.blockChain.getBlock(blockNum, true, true);
                // Check if the block is valid
                if (block == null)
                {
                    Logging.error("PoW target block {0} not found", blockNum);
                    continue;
                }

                // Ignore if we're in a bigger transaction, which is not yet complete
                if (!Node.walletState.inTransaction)
                {
                    if (block.powField == null)
                    {
                        Node.blockChain.increaseSolvedBlocksCount();
                        // Set the powField as a checksum of all miners for this block
                        block.powField = BitConverter.GetBytes(sent_block_num);
                        Node.blockChain.updateBlock(block);
                    }
                }

            }
        }

        // Clears all the transactions in the pool
        public static void clear()
        {
            lock(stateLock)
            {
                appliedTransactions.Clear();
                unappliedTransactions.Clear();
            }
        }

        public static void performCleanup()
        {
            if(Node.blockSync.synchronizing)
            {
                return;
            }

            ulong minBlockHeight = 1;
            if (Node.blockChain.getLastBlockNum() > ConsensusConfig.getRedactedWindowSize())
            {
                minBlockHeight = Node.blockChain.getLastBlockNum() - ConsensusConfig.getRedactedWindowSize();
            }

            lock (stateLock)
            {
                var txList = unappliedTransactions.Select(e => e.Value).Where(x => x != null && x.type == (int)Transaction.Type.PoWSolution).ToArray();
                foreach (var entry in txList)
                {
                    try
                    {
                        ulong blocknum = entry.powSolution.blockNum;

                        Block block = null;
                        
                        if (Node.blockChain.getLastBlockNum() < ConsensusConfig.miningExpirationBlockHeight)
                        {
                            block = Node.blockChain.getBlock(blocknum, false, false);
                        }

                        if (block == null || block.powField != null)
                        {
                            if (IxianHandler.isMyAddress(entry.pubKey))
                            {
                                ActivityStorage.updateStatus(entry.id, ActivityStatus.Error, 0);
                            }
                            unappliedTransactions.Remove(entry.id);
                        }
                    }
                    catch (Exception e)
                    {
                        Logging.error("Exception occurred in transactionPool.cleanUp() " + e);
                        // remove invalid/corrupt transaction
                        unappliedTransactions.Remove(entry.id);
                    }
                }

                txList = unappliedTransactions.Values.Where(x => x != null && x.blockHeight < minBlockHeight).ToArray();
                foreach (var entry in txList)
                {
                    unappliedTransactions.Remove(entry.id);
                }
            }
        }


        public static void processPendingTransactions()
        {
            // TODO TODO this has to be refactored and moved to PendingTransactions
            ulong last_block_height = IxianHandler.getLastBlockHeight();
            lock (stateLock) // this lock must be here to prevent deadlocks TODO: improve this at some point
            {
                lock (PendingTransactions.pendingTransactions)
                {
                    long cur_time = Clock.getTimestamp();
                    List<PendingTransaction> tmp_pending_transactions = new List<PendingTransaction>(PendingTransactions.pendingTransactions);
                    int idx = 0;
                    foreach (var entry in tmp_pending_transactions)
                    {
                        Transaction t = entry.transaction;
                        long tx_time = entry.addedTimestamp;

                        if (t.applied != 0)
                        {
                            PendingTransactions.pendingTransactions.RemoveAll(x => x.transaction.id.SequenceEqual(t.id));
                            continue;
                        }

                        // if transaction expired, remove it from pending transactions
                        if (last_block_height > ConsensusConfig.getRedactedWindowSize() && t.blockHeight < last_block_height - ConsensusConfig.getRedactedWindowSize())
                        {
                            ActivityStorage.updateStatus(t.id, ActivityStatus.Error, 0);
                            PendingTransactions.pendingTransactions.RemoveAll(x => x.transaction.id.SequenceEqual(t.id));
                            continue;
                        }

                        // check if PoW and if already solved
                        if (t.type == (int)Transaction.Type.PoWSolution)
                        {
                            ulong pow_block_num = t.powSolution.blockNum;

                            Block tmpBlock = Node.blockChain.getBlock(pow_block_num, false, false);
                            if (tmpBlock == null || tmpBlock.powField != null)
                            {
                                ActivityStorage.updateStatus(t.id, ActivityStatus.Error, 0);
                                PendingTransactions.pendingTransactions.RemoveAll(x => x.transaction.id.SequenceEqual(t.id));
                                continue;
                            }
                        }
                        else
                        {
                            // check if transaction is still valid
                            if (getUnappliedTransaction(t.id) == null && !verifyTransaction(t, null, false))
                            {
                                ActivityStorage.updateStatus(t.id, ActivityStatus.Error, 0);
                                PendingTransactions.pendingTransactions.RemoveAll(x => x.transaction.id.SequenceEqual(t.id));
                                continue;
                            }
                        }

                        if ((int)entry.confirmedNodeList.Count() > 3) // already received 3+ feedback
                        {
                            continue;
                        }

                        if (cur_time - tx_time > 40) // if the transaction is pending for over 40 seconds, resend
                        {
                            CoreProtocolMessage.broadcastProtocolMessage(new char[] { 'M', 'H' }, ProtocolMessageCode.transactionData, t.getBytes(), null);
                            entry.addedTimestamp = cur_time;
                            entry.confirmedNodeList.Clear();
                        }

                        if (entry.confirmedNodeList.Count() > 3) // already received 3+ feedback
                        {
                            continue;
                        }

                        if (cur_time - tx_time > 20) // if the transaction is pending for over 20 seconds, send inquiry
                        {
                            CoreProtocolMessage.broadcastGetTransaction(t.id, 0, null, false);
                        }

                        idx++;
                    }
                }
            }
        }

        public static long getAppliedTransactionCount()
        {
            lock(stateLock)
            {
                return appliedTransactions.LongCount();
            }
        }

        public static long getUnappliedTransactionCount()
        {
            lock (stateLock)
            {
                return unappliedTransactions.LongCount();
            }
        }

        public static void compactTransactionsForBlock(Block b)
        {
            lock (stateLock)
            {
                foreach (var entry in b.transactions)
                {
                    appliedTransactions[entry] = null;
                }
            }
        }

        public static bool removeAppliedTransaction(byte[] txid)
        {
            lock (stateLock)
            {
                return appliedTransactions.Remove(txid);
            }
        }

        public static bool removeUnappliedTransaction(byte[] txid)
        {
            lock (stateLock)
            {
                return unappliedTransactions.Remove(txid);
            }
        }

        // Returns a list of transactions connected to this block 
        public static List<Transaction> getFullBlockTransactions(Block block)
        {
            List<Transaction> tx_list = new List<Transaction>();
            HashSet<byte[]> tx_ids = block.transactions;
            for (int i = 0; i < tx_ids.Count; i++)
            {
                Transaction t = getAppliedTransaction(tx_ids.ElementAt(i), block.blockNum, true);
                if (t == null)
                {
                    Logging.error("nulltx: {0}", Transaction.getTxIdString(tx_ids.ElementAt(i)));
                    continue;
                }
                tx_list.Add(t);
            }
            return tx_list;
        }

        // temporary function that will correctly JSON Serialize IxiNumber
        public static List<Dictionary<string, object>> getFullBlockTransactionsAsArray(Block block)
        {
            List<Dictionary<string, object>> tx_list = new List<Dictionary<string, object>>();
            HashSet<byte[]> tx_ids = block.transactions;
            for (int i = 0; i < tx_ids.Count; i++)
            {
                Transaction t = getAppliedTransaction(tx_ids.ElementAt(i), block.blockNum, true);
                if (t == null)
                {
                    Logging.error("nulltx: {0}", Transaction.getTxIdString(tx_ids.ElementAt(i)));
                    continue;
                }

                tx_list.Add(t.toDictionary());

            }
            return tx_list;
        }

        // Returs total value of transactions connected to this block 
        public static IxiNumber getTotalTransactionsValueInBlock(Block block)
        {
            IxiNumber val = 0;
            HashSet<byte[]> tx_ids = block.transactions;
            for (int i = 0; i < tx_ids.Count; i++)
            {
                Transaction t = getAppliedTransaction(tx_ids.ElementAt(i), block.blockNum, true);
                if (t == null)
                    Logging.error("nulltx: {0}", Transaction.getTxIdString(tx_ids.ElementAt(i)));
                else
                    val.add(t.amount);
            }
            return val;
        }

        public static bool unapplyTransaction(byte[] txid)
        {
            lock(stateLock)
            {
                if(appliedTransactions.ContainsKey(txid))
                {
                    Transaction t = appliedTransactions[txid];
                    appliedTransactions.Remove(txid);

                    t.applied = 0;
                    unappliedTransactions.AddOrReplace(txid, t);
                    return true;
                }
            }
            return false;
        }
    }
}
