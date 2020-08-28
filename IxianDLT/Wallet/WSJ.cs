using DLT.Meta;
using IXICore;
using IXICore.Meta;
using IXICore.Utils;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace DLT
{
    public enum WSJEntryType : int
    {
        Balance = 1,
        MS_AllowedSigner = 2,
        MS_RequiredSignatures = 3,
        Pubkey = 4,
        Data = 5
    }

    public abstract class WSJEntry
    {
        public byte[] targetWallet { get; protected set; }

        public virtual byte[] checksum()
        {
            return Crypto.sha512sqTrunc(getBytes());
        }

        public virtual byte[] getBytes()
        {
            using (MemoryStream m = new MemoryStream(64))
            {
                using (BinaryWriter w = new BinaryWriter(m))
                {
                    writeBytes(w);
                    return m.ToArray();
                }
            }
        }
        public abstract void writeBytes(BinaryWriter w);
        public abstract bool apply();
        public abstract bool revert();
    }

    public class WSJE_Balance : WSJEntry
    {
        private IxiNumber old_value;
        private IxiNumber new_value;

        public WSJE_Balance(byte[] address, IxiNumber old_balance, IxiNumber new_balance)
        {
            targetWallet = address;
            old_value = old_balance;
            new_value = new_balance;
        }

        public WSJE_Balance(BinaryReader r)
        {
            int type = r.ReadInt32();
            if (type != (int)WSJEntryType.Balance)
            {
                throw new Exception(String.Format("Incorrect WSJ Entry type: {0}. Expected: {1}.", type, (int)WSJEntryType.Balance));
            }
            int target_wallet_len = r.ReadInt32();
            if (target_wallet_len > 0)
            {
                targetWallet = r.ReadBytes(target_wallet_len);
            }
            string old_str = r.ReadString();
            string new_str = r.ReadString();
            old_value = new IxiNumber(old_str);
            new_value = new IxiNumber(new_str);
        }

        public override void writeBytes(BinaryWriter w)
        {
            w.Write((int)WSJEntryType.Balance);
            if (targetWallet != null)
            {
                w.Write(targetWallet.Length);
                w.Write(targetWallet);
            }
            else
            {
                w.Write((int)0);
            }
            w.Write(old_value.ToString());
            w.Write(new_value.ToString());
        }

        public override bool apply()
        {
            if (targetWallet == null)
            {
                Logging.error("WSJE_Balance entry is missing target wallet!");
                return false;
            }
            return Node.walletState.setWalletBalanceInternal(targetWallet, new_value);
        }

        public override bool revert()
        {
            if (targetWallet == null)
            {
                Logging.error("WSJE_Balance entry is missing target wallet!");
                return false;
            }
            return Node.walletState.setWalletBalanceInternal(targetWallet, old_value, true);
        }
    }

    public class WSJE_AllowedSigner : WSJEntry
    {
        private byte[] signer;
        private bool adding;
        private bool adjustSigners;

        public WSJE_AllowedSigner(byte[] address, bool adding_signer, byte[] signer_address, bool adjust_signers = false)
        {
            targetWallet = address;
            adding = adding_signer;
            signer = signer_address;
            if (!adding)
            {
                // required signatures are adjusted only when removing signer addresses
                adjustSigners= adjust_signers;
            }
            else
            {
                adjustSigners = false;
            }
        }

        public WSJE_AllowedSigner(BinaryReader r)
        {
            int type = r.ReadInt32();
            if (type != (int)WSJEntryType.MS_AllowedSigner)
            {
                throw new Exception(String.Format("Incorrect WSJ Entry type: {0}. Expected: {1}.", type, (int)WSJEntryType.MS_AllowedSigner));
            }
            int target_wallet_len = r.ReadInt32();
            if (target_wallet_len > 0)
            {
                targetWallet = r.ReadBytes(target_wallet_len);
            }
            int target_signer_len = r.ReadInt32();
            if (target_signer_len > 0)
            {
                signer = r.ReadBytes(target_signer_len);
            }
            adding = r.ReadBoolean();
            if (!adding)
            {
                // required signatures are adjusted only when removing signer addresses
                adjustSigners = r.ReadBoolean();
            } else
            {
                adjustSigners = false;
            }
        }

        public override void writeBytes(BinaryWriter w)
        {
            w.Write((int)WSJEntryType.MS_AllowedSigner);
            if (targetWallet != null)
            {
                w.Write(targetWallet.Length);
                w.Write(targetWallet);
            }
            else
            {
                w.Write((int)0);
            }
            if (signer != null)
            {
                w.Write(signer.Length);
                w.Write(signer);
            }
            else
            {
                w.Write((int)0);
            }
            w.Write(adding);
            if (!adding)
            {
                // required signatures are adjusted only when removing signer addresses
                w.Write(adjustSigners);
            }
        }

        public override bool apply()
        {
            if (targetWallet == null || signer == null)
            {
                Logging.error("WSJE_AllowedSigner entry is missing target wallet or signer!");
                return false;
            }
            if(adding)
            {
                return Node.walletState.addWalletAllowedSignerInternal(targetWallet, signer, adjustSigners);
            } else
            {
                return Node.walletState.delWalletAllowedSignerInternal(targetWallet, signer, adjustSigners);
            }
        }

        public override bool revert()
        {
            if (targetWallet == null || signer == null)
            {
                Logging.error("WSJE_AllowedSigner entry is missing target wallet or signer!");
                return false;
            }
            if (adding)
            {
                return Node.walletState.delWalletAllowedSignerInternal(targetWallet, signer, adjustSigners, true);
            }
            else
            {
                return Node.walletState.addWalletAllowedSignerInternal(targetWallet, signer, adjustSigners);
            }
        }
    }

    public class WSJE_Signers : WSJEntry
    {
        private byte old_sigs;
        private byte new_sigs;

        public WSJE_Signers(byte[] address, byte old_req_sigs, byte new_req_sigs)
        {
            targetWallet = address;
            old_sigs = old_req_sigs;
            new_sigs = new_req_sigs;
        }

        public WSJE_Signers(BinaryReader r)
        {
            int type = r.ReadInt32();
            if (type != (int)WSJEntryType.MS_RequiredSignatures)
            {
                throw new Exception(String.Format("Incorrect WSJ Entry type: {0}. Expected: {1}.", type, (int)WSJEntryType.MS_RequiredSignatures));
            }
            int target_wallet_len = r.ReadInt32();
            if (target_wallet_len > 0)
            {
                targetWallet = r.ReadBytes(target_wallet_len);
            }
            old_sigs = r.ReadByte();
            new_sigs = r.ReadByte();
        }

        public override void writeBytes(BinaryWriter w)
        {
            w.Write((int)WSJEntryType.MS_RequiredSignatures);
            if (targetWallet != null)
            {
                w.Write(targetWallet.Length);
                w.Write(targetWallet);
            }
            else
            {
                w.Write((int)0);
            }
            w.Write(old_sigs);
            w.Write(new_sigs);
        }

        public override bool apply()
        {
            if (targetWallet == null)
            {
                Logging.error("WSJE_Signers entry is missing target wallet!");
                return false;
            }
            return Node.walletState.setWalletRequiredSignaturesInternal(targetWallet, new_sigs);
        }

        public override bool revert()
        {
            if (targetWallet == null)
            {
                Logging.error("WSJE_Signers entry is missing target wallet!");
                return false;
            }
            return Node.walletState.setWalletRequiredSignaturesInternal(targetWallet, old_sigs);
        }
    }

    public class WSJE_Pubkey : WSJEntry
    {
        private byte[] pubkey;

        public WSJE_Pubkey(byte[] address, byte[] adding_pubkey)
        {
            targetWallet = address;
            pubkey = adding_pubkey;
        }

        public WSJE_Pubkey(BinaryReader r)
        {
            int type = r.ReadInt32();
            if (type != (int)WSJEntryType.Pubkey)
            {
                throw new Exception(String.Format("Incorrect WSJ Entry type: {0}. Expected: {1}.", type, (int)WSJEntryType.Pubkey));
            }
            int target_wallet_len = r.ReadInt32();
            if (target_wallet_len > 0)
            {
                targetWallet = r.ReadBytes(target_wallet_len);
            }
            int target_pubkey_len = r.ReadInt32();
            if (target_pubkey_len > 0)
            {
                pubkey = r.ReadBytes(target_pubkey_len);
            }
        }

        public override void writeBytes(BinaryWriter w)
        {
            w.Write((int)WSJEntryType.Pubkey);
            if (targetWallet != null)
            {
                w.Write(targetWallet.Length);
                w.Write(targetWallet);
            }
            else
            {
                w.Write((int)0);
            }
            if (pubkey != null)
            {
                w.Write(pubkey.Length);
                w.Write(pubkey);
            }
            else
            {
                w.Write((int)0);
            }
        }

        public override bool apply()
        {
            if (targetWallet == null || pubkey == null)
            {
                Logging.error("WSJE_Pubkey entry is missing target wallet or pubkey!");
                return false;
            }
            return Node.walletState.setWalletPublicKeyInternal(targetWallet, pubkey);
        }

        public override bool revert()
        {
            if (targetWallet == null)
            {
                Logging.error("WSJE_Pubkey entry is missing target wallet!");
                return false;
            }
            return Node.walletState.setWalletPublicKeyInternal(targetWallet, null, true);
        }
    }

    public class WSJE_Data : WSJEntry
    {
        private byte[] new_data;
        private byte[] old_data;

        public WSJE_Data(byte[] address, byte[] old_wallet_data, byte[] new_wallet_data)
        {
            targetWallet = address;
            old_data = old_wallet_data;
            new_data = new_wallet_data;
        }

        public WSJE_Data(BinaryReader r)
        {
            int type = r.ReadInt32();
            if (type != (int)WSJEntryType.Pubkey)
            {
                throw new Exception(String.Format("Incorrect WSJ Entry type: {0}. Expected: {1}.", type, (int)WSJEntryType.Pubkey));
            }
            int target_wallet_len = r.ReadInt32();
            if (target_wallet_len > 0)
            {
                targetWallet = r.ReadBytes(target_wallet_len);
            }
            int new_data_len = r.ReadInt32();
            if (new_data_len > 0)
            {
                new_data = r.ReadBytes(new_data_len);
            }
            int old_data_len = r.ReadInt32();
            if (old_data_len > 0)
            {
                old_data = r.ReadBytes(old_data_len);
            }
        }

        public override void writeBytes(BinaryWriter w)
        {
            w.Write((int)WSJEntryType.Data);
            if (targetWallet != null)
            {
                w.Write(targetWallet.Length);
                w.Write(targetWallet);
            }
            else
            {
                w.Write((int)0);
            }
            if (new_data != null)
            {
                w.Write(new_data.Length);
                w.Write(new_data);
            }
            else
            {
                w.Write((int)0);
            }
            if (old_data != null)
            {
                w.Write(old_data.Length);
                w.Write(old_data);
            }
            else
            {
                w.Write((int)0);
            }
        }

        public override bool apply()
        {
            if (targetWallet == null || (new_data == null && old_data == null))
            {
                Logging.error("WSJE_Data entry is missing target wallet or data!");
                return false;
            }
            return Node.walletState.setWalletUserDataInternal(targetWallet, new_data, old_data);
        }

        public override bool revert()
        {
            if (targetWallet == null || (new_data == null && old_data == null))
            {
                Logging.error("WSJE_Data entry is missing target wallet or data!");
                return false;
            }
            return Node.walletState.setWalletUserDataInternal(targetWallet, old_data, new_data);
        }
    }


    public class WSJTransaction
    {
        private readonly List<WSJEntry> entries = new List<WSJEntry>();

        public ulong wsjTxNumber { get; private set; }

        public WSJTransaction(ulong number)
        {
            wsjTxNumber = number;
        }

        public WSJTransaction(byte[] bytes)
        {
            using (MemoryStream m = new MemoryStream(bytes))
            {
                using (BinaryReader r = new BinaryReader(m))
                {
                    wsjTxNumber = r.ReadUInt64();
                    int count_entries = r.ReadInt32();
                    lock(entries)
                    {
                        for(int i=0;i<count_entries;i++)
                        {
                            int type = r.ReadInt32();
                            r.BaseStream.Seek(-4, SeekOrigin.Current);
                            switch(type)
                            {
                                case (int)WSJEntryType.Balance: entries.Add(new WSJE_Balance(r)); break;
                                case (int)WSJEntryType.MS_AllowedSigner: entries.Add(new WSJE_AllowedSigner(r)); break;
                                case (int)WSJEntryType.MS_RequiredSignatures: entries.Add(new WSJE_Signers(r)); break;
                                case (int)WSJEntryType.Pubkey: entries.Add(new WSJE_Pubkey(r)); break;
                                case (int)WSJEntryType.Data: entries.Add(new WSJE_Data(r)); break;
                                default:
                                    throw new Exception(String.Format("Unknown WSJ Entry Type: {0}.", type));
                            }
                        }
                    }
                }
            }
        }

        public bool apply()
        {
            lock (entries)
            {
                foreach (var e in entries)
                {
                    if (e.apply() == false)
                    {
                        Logging.error(String.Format("Error while applying WSJ transaction."));
                        return false;
                    }
                }
            }
            return true;
        }

        public bool revert()
        {
            lock (entries)
            {
                foreach (var e in entries.AsEnumerable().Reverse())
                {
                    if (e.revert() == false)
                    {
                        Logging.error(String.Format("Error while reverting WSJ transaction."));
                    }
                }
            }
            return true;
        }

        public void addChange(WSJEntry entry)
        {
            entries.Add(entry);
        }

        public IEnumerable<Wallet> getAffectedWallets()
        {
            SortedSet<Wallet> sortedWallets = new SortedSet<Wallet>(new LambdaComparer<Wallet>((a, b) => _ByteArrayComparer.Compare(a.id, b.id) ));
            foreach(var entry in entries)
            {
                sortedWallets.Add(Node.walletState.getWallet(entry.targetWallet));
            }
            return sortedWallets;

        }

        public byte[] getBytes()
        {
            lock (entries)
            {
                // 144 = guid + before checksum + after checksum
                // entries are 64 bytes on average
                using (MemoryStream m = new MemoryStream(144 + 80 * entries.Count))
                {
                    using (BinaryWriter w = new BinaryWriter(m))
                    {
                        w.Write(wsjTxNumber);

                        w.Write(entries.Count);
                        foreach(var e in entries)
                        {
                            e.writeBytes(w);
                        }

                        return m.ToArray();
                    }
                }
            }
        }
    }
}
