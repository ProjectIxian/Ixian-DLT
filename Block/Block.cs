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

        public string blockChecksum;
        public string lastBlockChecksum;
        public string walletStateChecksum;


        private static string[] splitter = { "::" };

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
                signatures.Add(signature);
            }

            blockChecksum = block.blockChecksum;
            lastBlockChecksum = block.lastBlockChecksum;
            walletStateChecksum = block.walletStateChecksum;
        }

        public Block(byte[] bytes)
        {
            using (MemoryStream m = new MemoryStream(bytes))
            {
                using (BinaryReader reader = new BinaryReader(m))
                {
                    blockNum = reader.ReadUInt64();

                    // Get the transaction ids
                    int num_transactions = reader.ReadInt32();
                    for(int i = 0; i < num_transactions; i++)
                    {
                        string txid = reader.ReadString();
                        transactions.Add(txid);
                    }

                    // Get the signatures
                    int num_signatures = reader.ReadInt32();
                    for (int i = 0; i < num_signatures; i++)
                    {
                        string signature = reader.ReadString();
                        signatures.Add(signature);
                    }

                    blockChecksum = reader.ReadString();
                    lastBlockChecksum = reader.ReadString();
                    walletStateChecksum = reader.ReadString();
                }
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

                    // Write the number of signatures
                    int num_signatures = signatures.Count;
                    writer.Write(num_signatures);

                    // Write each signature
                    foreach (string signature in signatures)
                    {
                        writer.Write(signature);
                    }

                    writer.Write(blockChecksum);
                    writer.Write(lastBlockChecksum);
                    writer.Write(walletStateChecksum);
                }
                return m.ToArray();
            }
        }

        public bool addTransaction(Transaction transaction)
        {
            // TODO: this assumes the transaction is properly validated as it's already in the Transaction Pool
            // Could add an additional layer of checks here, just as in the TransactionPool - to avoid tampering

            transactions.Add(transaction.id);

            return true;
        }

        public string calculateChecksum()
        {
            System.Text.StringBuilder merged_txids = new System.Text.StringBuilder();
            foreach (string txid in transactions)
            {
                merged_txids.Append(txid);
            }

            string checksum = Crypto.sha256(blockNum + merged_txids.ToString() + lastBlockChecksum + walletStateChecksum);

            return checksum;
        }

        public bool applySignature()
        {
            // Note: we don't need any further validation, since this block has already passed through BlockProcessor.verifyBlock() at this point.
            string public_key = Node.walletStorage.publicKey;
            lock (signatures)
            {
                foreach (string sig in signatures)
                {
                    string[] parts = sig.Split(splitter, StringSplitOptions.RemoveEmptyEntries);
                    if(parts[1] == public_key)
                    {
                        // we have already signed it
                        return false;
                    }
                }
                string private_key = Node.walletStorage.privateKey;
                string signature = CryptoManager.lib.getSignature(blockChecksum, private_key);

                string merged_signature = signature + splitter[0] + public_key;

                signatures.Add(merged_signature);
            }

            return true;
        }

        public void addSignaturesFrom(Block other)
        {
            // Note: we don't need any further validation, since this block has already passed through BlockProcessor.verifyBlock() at this point.
            lock (signatures)
            {
                foreach (String sig in other.signatures)
                {
                    if(signatures.Contains(sig) == false)
                    {
                        signatures.Add(sig);
                    }
                }
            }
        }

        public bool verifySignatures()
        {
            foreach(string sig in signatures)
            {
                string[] parts = sig.Split(splitter, StringSplitOptions.RemoveEmptyEntries);
                if(parts.Length != 2)
                {
                    return false;
                }
                string signature = parts[0];
                string signerPubkey = parts[1];
                if(CryptoManager.lib.verifySignature(blockChecksum, signerPubkey, signature) == false)
                {
                    return false;
                }
            }
            return true;
        }

        // Goes through all signatures and verifies if the block is already signed with this node's pubkey
        public bool hasNodeSignature()
        {
            string public_key = Node.walletStorage.publicKey;
            string private_key = Node.walletStorage.privateKey;

            string signature = CryptoManager.lib.getSignature(blockChecksum, private_key);

            foreach (string merged_signature in signatures)
            {
                string[] signature_parts = merged_signature.Split(splitter,StringSplitOptions.None);
                if (signature_parts.Length < 2)
                    continue;

                // Check if public key matches
                if (public_key.Equals(signature_parts[1], StringComparison.Ordinal))
                {
                    // Check if signature is actually valid
                    if(CryptoManager.lib.verifySignature(blockChecksum, public_key, signature))
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

            return false;
        }

        // Returns the number of unique signatures
        public int getUniqueSignatureCount()
        {
            int signature_count = 0;

            // TODO: optimize this section to handle a large amount of signatures efficiently
            int sindex1 = 0;
            foreach (string signature in signatures)
            {
                bool duplicate = false;
                int sindex2 = 0;
                foreach (string signature_check in signatures)
                {
                    if (sindex1 == sindex2)
                        continue;

                    if(signature.Equals(signature_check, StringComparison.Ordinal))
                    {
                        duplicate = true;
                    }
                    sindex2++;
                }

                if(duplicate == false)
                {
                    signature_count++;
                }
                sindex1++;
            }

            return signature_count;
        }

        public void setWalletStateChecksum(string checksum)
        {
            walletStateChecksum = string.Copy(checksum);
        }

        public void logBlockDetails()
        {
            string last_block_chksum = lastBlockChecksum;
            if(last_block_chksum.Length == 0)
            {
                last_block_chksum = "G E N E S I S  B L O C K";
            }
            Console.WriteLine("\t\t|- Block Number:\t\t {0}", blockNum);
            Console.WriteLine("\t\t|- Signatures:\t\t\t {0}", signatures.Count);
            Console.WriteLine("\t\t|- Block Checksum:\t\t {0}", blockChecksum);
            Console.WriteLine("\t\t|- Last Block Checksum: \t {0}", last_block_chksum);
            Console.WriteLine("\t\t|- WalletState Checksum:\t {0}", walletStateChecksum);
        }

        public bool isGenesis { get { return this.blockNum == 0 && this.lastBlockChecksum == null; } }

    }    
}