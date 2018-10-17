using DLT.Meta;
using DLTNode;
using SQLite;
using System;
using System.Collections.Generic;
using System.IO;

namespace DLT
{
    public class Block
    {
        // TODO: Refactor all of these as readonly get-params
        [PrimaryKey, AutoIncrement]
        public ulong blockNum { get; set; }

        public List<string> transactions = new List<string> { };
        public List<string> signatures = new List<string> { };

        public string blockChecksum = "0";
        public string lastBlockChecksum = "0";
        public string walletStateChecksum = "0";
        public string signatureFreezeChecksum = "0";
        public string timestamp = "";
        public ulong difficulty = 0;

        // Locally calculated
        public string powField = "";

        public static string[] splitter = { "::" };

        // Generate the genesis block
        static Block createGenesisBlock()
        {
            Block genesis = new Block();
 
            genesis.calculateChecksum();
            genesis.applySignature();

            return genesis;
        }


        public Block()
        {
            blockNum = 0;
            transactions = new List<string>();
        }

        public Block(Block block)
        {
            blockNum = block.blockNum;

            // Add transactions and signatures from the old block
            foreach (string txid in block.transactions)
            {
                transactions.Add(txid);
            }

            foreach (string signature in block.signatures)
            {
                if (!containsSignature(signature))
                {
                    signatures.Add(signature);
                }
            }

            blockChecksum = block.blockChecksum;
            lastBlockChecksum = block.lastBlockChecksum;
            walletStateChecksum = block.walletStateChecksum;
            signatureFreezeChecksum = block.signatureFreezeChecksum;
            timestamp = block.timestamp;
            difficulty = block.difficulty;
            powField = block.powField;
        }

        public Block(byte[] bytes)
        {
            try
            {
                using (MemoryStream m = new MemoryStream(bytes))
                {
                    using (BinaryReader reader = new BinaryReader(m))
                    {
                        blockNum = reader.ReadUInt64();

                        // Get the transaction ids
                        int num_transactions = reader.ReadInt32();
                        for (int i = 0; i < num_transactions; i++)
                        {
                            string txid = reader.ReadString();
                            transactions.Add(txid);
                        }

                        // Get the signatures
                        int num_signatures = reader.ReadInt32();
                        for (int i = 0; i < num_signatures; i++)
                        {
                            string signature = reader.ReadString();
                            if (!containsSignature(signature))
                            {
                                signatures.Add(signature);
                            }
                        }

                        blockChecksum = reader.ReadString();
                        lastBlockChecksum = reader.ReadString();
                        walletStateChecksum = reader.ReadString();
                        signatureFreezeChecksum = reader.ReadString();
                        difficulty = reader.ReadUInt64();
                        powField = reader.ReadString();
                        timestamp = reader.ReadString();
                    }
                }
            }
            catch(Exception e)
            {
                Logging.warn(string.Format("Cannot create block from bytes: {0}", e.ToString()));
                throw;
            }
        }

        public byte[] getBytes()
        {
            using (MemoryStream m = new MemoryStream())
            {
                using (BinaryWriter writer = new BinaryWriter(m))
                {
                    writer.Write(blockNum);

                    // Write the number of transactions
                    int num_transactions = transactions.Count;
                    writer.Write(num_transactions);

                    // Write each wallet
                    foreach (string txid in transactions)
                    {
                        writer.Write(txid);
                    }

                    lock (signatures)
                    {
                        // Write the number of signatures
                        int num_signatures = signatures.Count;
                        writer.Write(num_signatures);

                        // Write each signature
                        foreach (string signature in signatures)
                        {
                            writer.Write(signature);
                        }
                    }

                    writer.Write(blockChecksum);
                    writer.Write(lastBlockChecksum);
                    writer.Write(walletStateChecksum);
                    writer.Write(signatureFreezeChecksum);
                    writer.Write(difficulty);
                    writer.Write(powField);
                    writer.Write(timestamp);
                }
                return m.ToArray();
            }
        }

        public bool addTransaction(Transaction transaction)
        {
            // TODO: this assumes the transaction is properly validated as it's already in the Transaction Pool
            // Could add an additional layer of checks here, just as in the TransactionPool - to avoid tampering
            if (transactions.Find(x => x == transaction.id) == null)
            {
                transactions.Add(transaction.id);
            }else
            {
                Logging.warn(String.Format("Tried to add a duplicate transaction {0} to block {1}.", transaction.id, blockNum));
            }

            return true;
        }

        // Returns the checksum of this block, without considering signatures
        public string calculateChecksum()
        {
            System.Text.StringBuilder merged_txids = new System.Text.StringBuilder();
            foreach (string txid in transactions)
            {
                merged_txids.Append(txid);
            }

            string checksum = Crypto.sha256(blockNum + merged_txids.ToString() + lastBlockChecksum + walletStateChecksum + signatureFreezeChecksum + difficulty);
            return checksum;
        }

        // Returns the checksum of all signatures of this block
        public string calculateSignatureChecksum()
        {
            // Sort the signature first
            List<string> sortedSigs = null;
            lock (signatures)
            {
               sortedSigs = new List<string>(signatures);
            }
            sortedSigs.Sort();

            // Merge the sorted signatures
            System.Text.StringBuilder merged_sigs = new System.Text.StringBuilder();
            foreach (string sig in sortedSigs)
            {
                merged_sigs.Append(sig);
            }

            // Generate a checksum from the merged sorted signatures
            string checksum = Crypto.sha256(merged_sigs.ToString());
            return checksum;
        }

        // Applies this node's signature to this block
        public bool applySignature()
        {
            if(Node.isWorkerNode())
            {
                return true;
            }

            // Note: we don't need any further validation, since this block has already passed through BlockProcessor.verifyBlock() at this point.
            string public_key = Node.walletStorage.publicKey;

            // TODO: optimize this in case our signature is already in the block, without locking signatures for too long
            string private_key = Node.walletStorage.privateKey;
            string signature = CryptoManager.lib.getSignature(blockChecksum, private_key);

            lock (signatures)
            {
                foreach (string sig in signatures)
                {
                    string[] parts = sig.Split(splitter, StringSplitOptions.RemoveEmptyEntries);
                    if (parts[1].Equals(public_key, StringComparison.Ordinal))
                    {
                        // we have already signed it
                        return false;
                    }
                }

                string merged_signature = signature + splitter[0] + public_key;
                signatures.Add(merged_signature);               
            }

            Logging.info(String.Format("Signed block #{0}.", blockNum));

            return true;
        }

        public bool containsSignature(String verifiedSig)
        {
            lock (signatures)
            {
                foreach (string sig in signatures)
                {
                    string[] parts = sig.Split(splitter, StringSplitOptions.RemoveEmptyEntries);
                    string[] parts2 = verifiedSig.Split(splitter, StringSplitOptions.RemoveEmptyEntries);
                    if (parts[1] == parts2[1])
                    {
                        return true;
                    }
                }
                return false;
            }
        }

        public bool addSignaturesFrom(Block other)
        {
            // Note: we don't need any further validation, since this block has already passed through BlockProcessor.verifyBlock() at this point.
            lock (signatures)
            {
                int count = 0;
                foreach (String sig in other.signatures)
                {
                    if(!containsSignature(sig))
                    {
                        count++;
                        signatures.Add(sig);
                    }
                }
                if (count > 0)
                {
                    //Logging.info(String.Format("Merged {0} new signatures from incoming block.", count));
                    return true;
                }
            }
            return false;
        }

        public bool verifySignatures()
        {
            lock (signatures)
            {
                foreach (string sig in signatures)
                {
                    string[] parts = sig.Split(splitter, StringSplitOptions.RemoveEmptyEntries);
                    if (parts.Length != 2)
                    {
                        return false;
                    }
                    string signature = parts[0];
                    string signerPubkey = parts[1];
                    if (CryptoManager.lib.verifySignature(blockChecksum, signerPubkey, signature) == false)
                    {
                        return false;
                    }
                }
                return true;
            }
        }

        // Goes through all signatures and verifies if the block is already signed with this node's pubkey
        public bool hasNodeSignature(string public_key = null)
        {
            if (public_key == null)
            {
                public_key = Node.walletStorage.publicKey;
            }
            lock (signatures)
            {
                foreach (string merged_signature in signatures)
                {
                    string[] signature_parts = merged_signature.Split(splitter, StringSplitOptions.None);
                    if (signature_parts.Length < 2)
                        continue;

                    // Check if public key matches
                    if (public_key.Equals(signature_parts[1], StringComparison.Ordinal))
                    {
                        // Check if signature is actually valid
                        if (CryptoManager.lib.verifySignature(blockChecksum, public_key, signature_parts[0]))
                        {
                            return true;
                        }
                        else
                        {
                            // Somebody tampered this block. Show a warning and do not broadcast it further
                            // TODO: Possibly denounce the tampered block's origin node
                            Logging.warn(string.Format("Possible tampering on received block: {0}", blockNum));
                            return false;
                        }
                    }
                }
            }
            return false;
        }

        // Goes through all signatures and generates the corresponding Ixian wallet addresses
        public List<string> getSignaturesWalletAddresses()
        {
            List<string> result = new List<string>();

            lock (signatures)
            {

                foreach (string merged_signature in signatures)
                {
                    string[] signature_parts = merged_signature.Split(splitter, StringSplitOptions.None);
                    if (signature_parts.Length < 2)
                        continue;

                    string signature = signature_parts[0];
                    string public_key = signature_parts[1];

                    // Check if signature is actually valid
                    if (CryptoManager.lib.verifySignature(blockChecksum, public_key, signature) == false)
                    {
                        // Signature is not valid, don't extract the wallet address
                        // TODO: maybe do something else here as well. Perhaps reject the block?
                        continue;
                    }

                    Address address = new Address(public_key);
                    string address_string = address.ToString();
                    // TODO: check if it's it worth it validating the address again here

                    // Add the address to the list
                    result.Add(address_string);
                }
                result.Sort();
            }
            return result;
        }

        // Returns the number of unique signatures
        public int getUniqueSignatureCount()
        {
            int signature_count = 0;

            // TODO: optimize this section to handle a large amount of signatures efficiently
            int sindex1 = 0;

            lock (signatures)
            {

                foreach (string signature in signatures)
                {
                    bool duplicate = false;
                    int sindex2 = 0;
                    foreach (string signature_check in signatures)
                    {
                        if (sindex1 == sindex2)
                            continue;

                        string[] partsSignature_check = signature_check.Split(splitter, StringSplitOptions.RemoveEmptyEntries);
                        string[] partsSignature = signature.Split(splitter, StringSplitOptions.RemoveEmptyEntries);
                        if (partsSignature[1].Equals(partsSignature_check[1], StringComparison.Ordinal))
                        {
                            duplicate = true;
                        }
                        sindex2++;
                    }

                    if (duplicate == false)
                    {
                        signature_count++;
                    }
                    sindex1++;
                }
            }
            return signature_count;
        }

        public void setWalletStateChecksum(string checksum)
        {
            walletStateChecksum = string.Copy(checksum);
        }

        // Returs a list of transactions connected to this block 
        public List<Transaction> getFullTransactions()
        {
            List<Transaction> txList = new List<Transaction>();
            for (int i = 0; i < transactions.Count; i++)
            {
                Transaction t = t = TransactionPool.getTransaction(transactions[i]);
                if (t == null)
                {
                    Logging.error(string.Format("nulltx: {0}", transactions[i]));
                    continue;
                }
                txList.Add(t);
            }
            return txList;
        }

        // temporary function that will correctly JSON Serialize IxiNumber
        public List<Dictionary<string, string>> getFullTransactionsAsArray()
        {
            List<Dictionary<string, string>> txList = new List<Dictionary<string, string>>();
            for (int i = 0; i < transactions.Count; i++)
            {
                Transaction t = TransactionPool.getTransaction(transactions[i]);
                if (t == null)
                {
                    Logging.error(string.Format("nulltx: {0}", transactions[i]));
                    continue;
                }

                Dictionary<string, string> tDic = new Dictionary<string, string>();
                tDic.Add("id", t.id);
                tDic.Add("blockHeight", t.blockHeight.ToString());
                tDic.Add("nonce", t.nonce.ToString());
                tDic.Add("signature", t.signature);
                tDic.Add("data", t.data);
                tDic.Add("timeStamp", t.timeStamp);
                tDic.Add("type", t.type.ToString());
                tDic.Add("amount", t.amount.ToString());
                tDic.Add("applied", t.applied.ToString());
                tDic.Add("checksum", t.checksum);
                tDic.Add("from", t.from);
                tDic.Add("to", t.to);
                tDic.Add("fee", t.fee.ToString());
                txList.Add(tDic);

            }
            return txList;
        }

        // Returs total value of transactions connected to this block 
        public IxiNumber getTotalTransactionsValue()
        {
            IxiNumber val = 0;
            for(int i = 0; i < transactions.Count; i++)
            {
                Transaction t = TransactionPool.getTransaction(transactions[i]);
                if (t == null)
                    Logging.error(string.Format("nulltx: {0}", transactions[i]));
                else
                    val.add(t.amount);
            }
            return val;
        }

        public void logBlockDetails()
        {
            string last_block_chksum = lastBlockChecksum;
            if(last_block_chksum.Length == 0)
            {
                last_block_chksum = "G E N E S I S  B L O C K";
            }
            Logging.info(String.Format("\t\t|- Block Number:\t\t {0}", blockNum));
            Logging.info(String.Format("\t\t|- Signatures:\t\t\t {0} ({1} req)", signatures.Count, Node.blockChain.getRequiredConsensus()));
            Logging.info(String.Format("\t\t|- Block Checksum:\t\t {0}", blockChecksum));
            Logging.info(String.Format("\t\t|- Last Block Checksum: \t {0}", last_block_chksum));
            Logging.info(String.Format("\t\t|- WalletState Checksum:\t {0}", walletStateChecksum));
            Logging.info(String.Format("\t\t|- Sig Freeze Checksum: \t {0}", signatureFreezeChecksum));
            Logging.info(String.Format("\t\t|- Difficulty:\t\t\t {0}", difficulty));
            Logging.info(String.Format("\t\t|- Transaction Count:\t {0}", transactions.Count));
        }

        public bool isGenesis { get { return this.blockNum == 0 && this.lastBlockChecksum == null; } }

    }    
}