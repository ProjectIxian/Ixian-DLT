using DLT.Meta;
using IXICore;
using IXICore.Meta;
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
        private IxiNumber delta;

        public WSJE_Balance(byte[] address, IxiNumber delta_balance)
        {
            targetWallet = address;
            delta = delta_balance;
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
            string delta_str = r.ReadString();
            delta = new IxiNumber(delta_str);
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
            w.Write(delta.ToString());
        }

        public override bool apply()
        {
            if (targetWallet == null || delta == null)
            {
                Logging.error("WSJE_Balance entry is missing target wallet or delta!");
                return false;
            }
            return Node.walletState.setWalletBalanceInternal(targetWallet, delta);
        }

        public override bool revert()
        {
            if (targetWallet == null || delta == null)
            {
                Logging.error("WSJE_Balance entry is missing target wallet or delta!");
                return false;
            }
            return Node.walletState.setWalletBalanceInternal(targetWallet, new IxiNumber(0) - delta);
        }
    }

    public class WSJE_AllowedSigner : WSJEntry
    {
        private byte[] signer;
        private bool adding;

        public WSJE_AllowedSigner(byte[] address, bool adding_signer, byte[] signer_address)
        {
            targetWallet = address;
            adding = adding_signer;
            signer = signer_address;
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
                return Node.walletState.addWalletAllowedSignerInternal(targetWallet, signer);
            } else
            {
                return Node.walletState.delWalletAllowedSignerInternal(targetWallet, signer);
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
                return Node.walletState.delWalletAllowedSignerInternal(targetWallet, signer);
            }
            else
            {
                return Node.walletState.addWalletAllowedSignerInternal(targetWallet, signer);
            }
        }
    }

    public class WSJE_Signers : WSJEntry
    {
        private int delta;

        public WSJE_Signers(byte[] address, int delta_signers)
        {
            targetWallet = address;
            delta = delta_signers;
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
            delta = r.ReadInt32();
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
            w.Write(delta);
        }

        public override bool apply()
        {
            if (targetWallet == null)
            {
                Logging.error("WSJE_Signers entry is missing target wallet!");
                return false;
            }
            return Node.walletState.setWalletRequiredSignaturesInternal(targetWallet, delta);
        }

        public override bool revert()
        {
            if (targetWallet == null)
            {
                Logging.error("WSJE_Signers entry is missing target wallet!");
                return false;
            }
            return Node.walletState.setWalletRequiredSignaturesInternal(targetWallet, -delta);
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
            if (targetWallet == null || pubkey == null)
            {
                Logging.error("WSJE_Pubkey entry is missing target wallet or pubkey!");
                return false;
            }
            return Node.walletState.setWalletPublicKeyInternal(targetWallet, null);
        }
    }

    public class WSJE_Data : WSJEntry
    {
        private byte[] new_data;
        private byte[] old_data;

        public WSJE_Data(byte[] address, byte[] adding_data, byte[] previous_data)
        {
            targetWallet = address;
            old_data = previous_data;
            new_data = adding_data;
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
        public byte[] beforeWSChecksum { get; private set; }
        public byte[] afterWSChecksum { get; private set; }

        public WSJTransaction(ulong number, bool auto_checksum = true)
        {
            if (auto_checksum)
            {
                beforeWSChecksum = Node.walletState.calculateWalletStateChecksum();
            }
            wsjTxNumber = number;
        }

        public WSJTransaction(byte[] bytes)
        {
            using (MemoryStream m = new MemoryStream(bytes))
            {
                using (BinaryReader r = new BinaryReader(m))
                {
                    wsjTxNumber = r.ReadUInt64();
                    int before_checksum_len = r.ReadInt32();
                    if(before_checksum_len > 0)
                    {
                        beforeWSChecksum = r.ReadBytes(before_checksum_len);
                    }
                    int after_checksum_len = r.ReadInt32();
                    if(after_checksum_len > 0)
                    {
                        afterWSChecksum = r.ReadBytes(after_checksum_len);
                    }
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
                        Logging.error(String.Format("Error while applying WSJ transaction {0} -> {1}.", beforeWSChecksum, afterWSChecksum));
                        return false;
                    }
                }
            }
            afterWSChecksum = Node.walletState.calculateWalletStateChecksum();
            return true;
        }

        public bool revert()
        {
            lock (entries)
            {
                foreach (var e in entries)
                {
                    if (e.revert() == false)
                    {
                        Logging.error(String.Format("Error while reverting WSJ transaction {0} <- {1}.", beforeWSChecksum, afterWSChecksum));
                        return false;
                    }
                }
            }
            return true;
        }

        public void addChange(WSJEntry entry)
        {
            entries.Add(entry);
        }

        public byte[] getBytes()
        {
            if (afterWSChecksum == null)
            {
                throw new Exception("WSJ Transaction must be applied at least once before it can be serialized.");
            }
            lock (entries)
            {
                // 144 = guid + before checksum + after checksum
                // entries are 64 bytes on average
                using (MemoryStream m = new MemoryStream(144 + 80 * entries.Count))
                {
                    using (BinaryWriter w = new BinaryWriter(m))
                    {
                        w.Write(wsjTxNumber);

                        w.Write(beforeWSChecksum.Length);
                        w.Write(beforeWSChecksum);

                        w.Write(afterWSChecksum.Length);
                        w.Write(afterWSChecksum);

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
