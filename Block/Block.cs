using DLT.Meta;
using DLTNode;
using SQLite;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Linq;
using IXICore.Utils;

namespace DLT
{
    public class Block
    {
        // TODO: Refactor all of these as readonly get-params
        [PrimaryKey, AutoIncrement]
        public ulong blockNum { get; set; }

        public List<string> transactions = new List<string> { };
        public List<byte[][]> signatures = new List<byte[][]> { };

        public byte[] blockChecksum = null;
        public byte[] lastBlockChecksum = null;
        public byte[] walletStateChecksum = null;
        public byte[] signatureFreezeChecksum = null;
        public string timestamp = "";
        public ulong difficulty = 0;

        // Locally calculated
        public byte[] powField = null;

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

            foreach (byte[][] signature in block.signatures)
            {
                if (!containsSignature(signature[0]))
                {
                    byte[][] newSig = new byte[2][];
                    newSig[0] = new byte[signature[0].Length];
                    Array.Copy(signature[0], newSig[0], newSig[0].Length);
                    newSig[1] = new byte[signature[1].Length];
                    Array.Copy(signature[1], newSig[1], newSig[1].Length);
                    signatures.Add(newSig);
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
                            int sigLen = reader.ReadInt32();
                            byte[] sig = reader.ReadBytes(sigLen);
                            int sigAddresLen = reader.ReadInt32();
                            byte[] sigAddress = reader.ReadBytes(sigAddresLen);
                            if (!containsSignature(sigAddress))
                            {
                                byte[][] newSig = new byte[2][];
                                newSig[0] = sig;
                                newSig[1] = sigAddress;
                                signatures.Add(newSig);
                            }
                        }
                        int dataLen = reader.ReadInt32();
                        blockChecksum = reader.ReadBytes(dataLen);

                        dataLen = reader.ReadInt32();
                        if (dataLen > 0)
                        {
                            lastBlockChecksum = reader.ReadBytes(dataLen);
                        }

                        dataLen = reader.ReadInt32();
                        if (dataLen > 0)
                        {
                            walletStateChecksum = reader.ReadBytes(dataLen);
                        }

                        dataLen = reader.ReadInt32();
                        if (dataLen > 0)
                        {
                            signatureFreezeChecksum = reader.ReadBytes(dataLen);
                        }

                        difficulty = reader.ReadUInt64();

                        dataLen = reader.ReadInt32();
                        if (dataLen > 0)
                        {
                            powField = reader.ReadBytes(dataLen);
                        }

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
                        foreach (byte[][] signature in signatures)
                        {
                            writer.Write(signature[0].Length);
                            writer.Write(signature[0]);
                            writer.Write(signature[1].Length);
                            writer.Write(signature[1]);
                        }
                    }

                    writer.Write(blockChecksum.Length);
                    writer.Write(blockChecksum);
                    if (lastBlockChecksum != null)
                    {
                        writer.Write(lastBlockChecksum.Length);
                        writer.Write(lastBlockChecksum);
                    }else
                    {
                        writer.Write((int)0);
                    }
                    if (walletStateChecksum != null)
                    {
                        writer.Write(walletStateChecksum.Length);
                        writer.Write(walletStateChecksum);
                    }
                    else
                    {
                        writer.Write((int)0);
                    }
                    if (signatureFreezeChecksum != null)
                    {
                        writer.Write(signatureFreezeChecksum.Length);
                        writer.Write(signatureFreezeChecksum);
                    }
                    else
                    {
                        writer.Write((int)0);
                    }
                    writer.Write(difficulty);
                    if (powField != null)
                    {
                        writer.Write(powField.Length);
                        writer.Write(powField);
                    }
                    else
                    {
                        writer.Write((int)0);
                    }
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
        public byte[] calculateChecksum()
        {
            StringBuilder merged_txids = new StringBuilder();
            foreach (string txid in transactions)
            {
                merged_txids.Append(txid);
            }

            List<byte> rawData = new List<byte>();
            rawData.AddRange(Config.ixianChecksumLock);
            rawData.AddRange(BitConverter.GetBytes(blockNum));
            rawData.AddRange(Encoding.UTF8.GetBytes(merged_txids.ToString()));
            if (lastBlockChecksum != null)
            {
                rawData.AddRange(lastBlockChecksum);
            }
            if (walletStateChecksum != null)
            {
                rawData.AddRange(walletStateChecksum);
            }
            if (signatureFreezeChecksum != null)
            {
                rawData.AddRange(signatureFreezeChecksum);
            }
            rawData.AddRange(BitConverter.GetBytes(difficulty));
            return Crypto.sha256(rawData.ToArray());
        }

        // Returns the checksum of all signatures of this block
        public byte[] calculateSignatureChecksum()
        {
            // Sort the signature first
            List<byte[][]> sortedSigs = null;
            lock (signatures)
            {
               sortedSigs = new List<byte[][]>(signatures);
            }
            sortedSigs.OrderBy(x => x[1], new ByteArrayComparer());

            // Merge the sorted signatures
            List<byte> merged_sigs = new List<byte>();
            foreach (byte[][] sig in sortedSigs)
            {
                merged_sigs.AddRange(sig[0]);
            }

            // Generate a checksum from the merged sorted signatures
            byte[] checksum = Crypto.sha256(merged_sigs.ToArray());
            return checksum;
        }

        // Applies this node's signature to this block
        public bool applySignature()
        {
            if (Node.isWorkerNode())
            {
                return true;
            }

            // Note: we don't need any further validation, since this block has already passed through BlockProcessor.verifyBlock() at this point.
            byte[] address = Node.walletStorage.getWalletAddress();

            Wallet signerWallet = Node.walletState.getWallet(address);

            // Use public key when applying signature to legacy block
            if (signerWallet.publicKey == null)
            {
                address = Node.walletStorage.publicKey;
            }

            // TODO: optimize this in case our signature is already in the block, without locking signatures for too long
            byte[] private_key = Node.walletStorage.privateKey;
            byte[] signature = CryptoManager.lib.getSignature(blockChecksum, private_key);

            lock (signatures)
            {
                foreach (byte[][] sig in signatures)
                {
                    if (sig[1].SequenceEqual(address))
                    {
                        // we have already signed it
                        return false;
                    }
                }

                byte[][] newSig = new byte[2][];
                newSig[0] = signature;
                newSig[1] = address;
                signatures.Add(newSig);               
            }

            Logging.info(String.Format("Signed block #{0}.", blockNum));

            return true;
        }

        public bool containsSignature(byte[] address)
        {
            lock (signatures)
            {
                foreach (byte[][] sig in signatures)
                {
                    if (address.SequenceEqual(sig[1]))
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
                foreach (byte[][] sig in other.signatures)
                {
                    if(!containsSignature(sig[1]))
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
                foreach (byte[][] sig in signatures)
                {
                    byte[] signature = sig[0];
                    byte[] address = sig[1];

                    byte[] signerPubKey = sig[1];


                    if (signerPubKey.Length < 70)
                    {
                        // Extract the public key from the walletstate
                        Wallet signerWallet = Node.walletState.getWallet(address);
                        if (signerWallet.publicKey != null)
                        {
                            signerPubKey = signerWallet.publicKey;
                        }
                        else
                        {
                            // No public key in wallet state                        
                        }
                    }

                    // Failed to find signer publickey in walletstate
                    if (signerPubKey.Length < 1)
                        return false;                                                 

                    if (CryptoManager.lib.verifySignature(blockChecksum, signerPubKey, signature) == false)
                    {
                        return false;
                    }


                }
                return true;
            }
        }

        // Updates the walletstate public keys. Called from BlockProcessor applyAcceptedBlock()
        public bool updateWalletStatePublicKeys(bool ws_snapshot = false)
        {
            Block targetBlock = Node.blockChain.getBlock(blockNum - 6, false);
            if(targetBlock == null)
            {
                return false;
            }
            List<byte[][]> sigs = targetBlock.signatures;
            foreach (byte[][] sig in sigs)
            {
                byte[] signature = sig[0];
                byte[] signerPubkey = sig[1];
                bool should_store_pkey = false;

                // Extract the public key from the walletstate
                byte[] signer_address = signerPubkey;
                if (signerPubkey.Length < 70)
                {
                    Wallet signerWallet = Node.walletState.getWallet(signer_address);
                    if (signerWallet.publicKey != null)
                        signerPubkey = signerWallet.publicKey;
                    else
                    {
                        // No public key in wallet state
                        should_store_pkey = true;
                    }

                    // Failed to find signer publickey in walletstate
                    if (signerPubkey == null)
                        return false;
                }else
                {
                    should_store_pkey = true;
                }
                // TODO: check if we should verify the signature again at this point

                // Check if we should store this public key
                // TODO: check if this should be moved to getSignaturesWalletAddresses
                if (should_store_pkey)
                {
                    // Generate an address
                    Address p_address = new Address(signerPubkey);
                    // Set the WS public key
                    Node.walletState.setWalletPublicKey(p_address.address, signerPubkey, ws_snapshot);
                }
            }

            return true;
        }

        // Goes through all signatures and verifies if the block is already signed with this node's pubkey
        public bool hasNodeSignature(byte[] public_key = null)
        {
            if (public_key == null)
            {
                public_key = Node.walletStorage.publicKey;
            }
            lock (signatures)
            {
                foreach (byte[][] merged_signature in signatures)
                {
                    Wallet signerWallet = Node.walletState.getWallet(Node.walletStorage.getWalletAddress());

                    bool condition = false;

                    // Check if we have an address instead of a public key
                    if (merged_signature[1].Length < 70)
                    {
                        // Compare wallet address
                        condition = Node.walletStorage.address.SequenceEqual(merged_signature[1]);
                    }
                    else
                    {
                        // Legacy, compare public key
                        condition = public_key.SequenceEqual(merged_signature[1]);
                    }

                    // Check if it matches
                    if (condition)
                    {
                        // Check if signature is actually valid
                        if (CryptoManager.lib.verifySignature(blockChecksum, public_key, merged_signature[0]))
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
        public List<byte[]> getSignaturesWalletAddresses()
        {
            List<byte[]> result = new List<byte[]>();

            lock (signatures)
            {

                foreach (byte[][] merged_signature in signatures)
                {
                    byte[] signature = merged_signature[0];
                    byte[] public_key = merged_signature[1];

                    bool found_public_key = false;

                    // Check if we have an address instead of a public key
                    if (public_key.Length < 70)
                    {
                        // Extract the public key from the walletstate
                        byte[] signer_address = public_key;
                        Wallet signerWallet = Node.walletState.getWallet(signer_address);
                        if (signerWallet.publicKey != null)
                        {
                            found_public_key = true;
                            public_key = signerWallet.publicKey;
                        }
                        // Failed to find signer publickey in walletstate
                        if (public_key.Length < 1)
                            continue;
                    }

                    // Check if signature is actually valid
                    if (CryptoManager.lib.verifySignature(blockChecksum, public_key, signature) == false)
                    {
                        // Signature is not valid, don't extract the wallet address
                        // TODO: maybe do something else here as well. Perhaps reject the block?
                        continue;
                    }

                    byte[] addressBytes = public_key;

                    if (found_public_key == false)
                    {
                        Address address = new Address(public_key);
                        addressBytes = address.address;
                        // TODO: check if it's it worth it validating the address again here
                    }

                    // Add the address to the list
                    result.Add(addressBytes);
                }
                result.OrderBy(x => x, new ByteArrayComparer());
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

                foreach (byte[][] signature in signatures)
                {
                    bool duplicate = false;
                    int sindex2 = 0;
                    foreach (byte[][] signature_check in signatures)
                    {
                        if (sindex1 == sindex2)
                            continue;

                        if (signature[1].SequenceEqual(signature_check[1]))
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

        public void setWalletStateChecksum(byte[] checksum)
        {
            walletStateChecksum = new byte[checksum.Length];
            Array.Copy(checksum, walletStateChecksum, walletStateChecksum.Length);
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
                tDic.Add("signature", Crypto.hashToString(t.signature) + ":" + Crypto.hashToString(t.pubKey));
                tDic.Add("data", t.data.ToString());
                tDic.Add("timeStamp", t.timeStamp.ToString());
                tDic.Add("type", t.type.ToString());
                tDic.Add("amount", t.amount.ToString());
                tDic.Add("applied", t.applied.ToString());
                tDic.Add("checksum", Crypto.hashToString(t.checksum));
                tDic.Add("from", Crypto.hashToString(t.from));
                tDic.Add("to", Crypto.hashToString(t.to));
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
            string last_block_chksum = "";
            if (lastBlockChecksum != null)
            {
               last_block_chksum = Crypto.hashToString(lastBlockChecksum);
            }
            if(last_block_chksum.Length == 0)
            {
                last_block_chksum = "G E N E S I S  B L O C K";
            }
            Logging.info(String.Format("\t\t|- Block Number:\t\t {0}", blockNum));
            Logging.info(String.Format("\t\t|- Signatures:\t\t\t {0} ({1} req)", signatures.Count, Node.blockChain.getRequiredConsensus()));
            Logging.info(String.Format("\t\t|- Block Checksum:\t\t {0}", Crypto.hashToString(blockChecksum)));
            Logging.info(String.Format("\t\t|- Last Block Checksum: \t {0}", last_block_chksum));
            Logging.info(String.Format("\t\t|- WalletState Checksum:\t {0}", Crypto.hashToString(walletStateChecksum)));
            Logging.info(String.Format("\t\t|- Sig Freeze Checksum: \t {0}", Crypto.hashToString(signatureFreezeChecksum)));
            Logging.info(String.Format("\t\t|- Difficulty:\t\t\t {0}", difficulty));
            Logging.info(String.Format("\t\t|- Transaction Count:\t {0}", transactions.Count));
        }

        public bool isGenesis { get { return this.blockNum == 0 && this.lastBlockChecksum == null; } }

    }    
}