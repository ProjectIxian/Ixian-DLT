// Copyright (C) 2017-2024 Ixian OU
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

using IXICore;
using IXICore.Journal;
using IXICore.Meta;
using IXICore.RegNames;
using IXICore.Utils;
using System;
using System.Collections.Generic;
using System.IO;

namespace DLT.RegNames
{
    public partial class RegisteredNames : GenericJournal
    {


        public enum RegNameEntryType : int
        {
            Register = 1,
            UpdateRecord = 2,
            Recover = 3,
            Extend = 4,
            IncreaseCapacity = 5,
            Destroy = 6,
            DecreaseRewardPool = 7,
            SetHighestExpirationBlockHeight = 8
        }

        public class RNE_Register : JournalEntry
        {
            private uint capacity;
            private ulong registrationBlockHeight;
            private ulong expirationBlockHeight;
            private Address nextPkHash;
            private Address recoveryHash;
            private IxiNumber regFee;

            private List<RegisteredNameRecord> topLevelNames;

            public RNE_Register(byte[] address, ulong registrationBlockHeight, uint capacity, ulong expirationBlockHeight, Address nextPkHash, Address recoveryHash, IxiNumber regFee, List<RegisteredNameRecord> topLevelNames)
            {
                targetWallet = address;
                this.registrationBlockHeight = registrationBlockHeight;
                this.capacity = capacity;
                this.expirationBlockHeight = expirationBlockHeight;
                this.nextPkHash = nextPkHash;
                this.recoveryHash = recoveryHash;
                this.regFee = regFee;
                this.topLevelNames = topLevelNames;
            }

            public RNE_Register(BinaryReader r)
            {
                throw new NotImplementedException();
            }

            public override void writeBytes(BinaryWriter w)
            {
                throw new NotImplementedException();
            }

            public override bool apply()
            {
                if (targetWallet == null)
                {
                    Logging.error(GetType().Name + " entry is missing target name!");
                    return false;
                }
                RegisteredNameRecord rnr = new RegisteredNameRecord(targetWallet, registrationBlockHeight, capacity, expirationBlockHeight, nextPkHash, recoveryHash);
                return rnInstance.registerNameInternal(rnr, regFee);
            }

            public override bool revert()
            {
                if (targetWallet == null)
                {
                    Logging.error(GetType().Name + " entry is missing target name!");
                    return false;
                }
                bool result = rnInstance.removeNameInternal(targetWallet);
                if (result)
                {
                    rnInstance.decreaseRewardPoolInternal(regFee);
                    foreach (var name in topLevelNames)
                    {
                        rnInstance.storage.updateRegName(name);
                    }
                }
                return result;
            }
        }

        public class RNE_DecreaseRewardPool : JournalEntry
        {
            private IxiNumber amount = 0;

            public RNE_DecreaseRewardPool(IxiNumber amount)
            {
                this.amount = amount;
            }

            public RNE_DecreaseRewardPool(BinaryReader r)
            {
                throw new NotImplementedException();
            }

            public override void writeBytes(BinaryWriter w)
            {
                throw new NotImplementedException();
            }

            public override bool apply()
            {
                rnInstance.decreaseRewardPoolInternal(amount);
                return true;
            }

            public override bool revert()
            {
                rnInstance.increaseRewardPoolInternal(amount);
                return true;
            }
        }


        public class RNE_SetHighestExpirationBlockHeight : JournalEntry
        {
            private ulong oldHighestExpirationBlockHeight = 0;
            private ulong highestExpirationBlockHeight = 0;

            public RNE_SetHighestExpirationBlockHeight(ulong highestExpirationBlockHeight, ulong oldHighestExpirationBlockHeight)
            {
                this.highestExpirationBlockHeight = highestExpirationBlockHeight;
                this.oldHighestExpirationBlockHeight = oldHighestExpirationBlockHeight;
            }

            public RNE_SetHighestExpirationBlockHeight(BinaryReader r)
            {
                throw new NotImplementedException();
            }

            public override void writeBytes(BinaryWriter w)
            {
                throw new NotImplementedException();
            }

            public override bool apply()
            {
                rnInstance.storage.setHighestExpirationBlockHeight(highestExpirationBlockHeight);
                return true;
            }

            public override bool revert()
            {
                rnInstance.storage.setHighestExpirationBlockHeight(oldHighestExpirationBlockHeight);
                return true;
            }
        }

        public class RNE_Destroy : JournalEntry
        {
            private RegisteredNameRecord nameRecord;

            public RNE_Destroy(byte[] address, RegisteredNameRecord oldNameRecord)
            {
                targetWallet = address;
                nameRecord = oldNameRecord;
            }

            public RNE_Destroy(BinaryReader r)
            {
                throw new NotImplementedException();
            }

            public override void writeBytes(BinaryWriter w)
            {
                throw new NotImplementedException();
            }

            public override bool apply()
            {
                if (targetWallet == null)
                {
                    Logging.error(GetType().Name + " entry is missing target name!");
                    return false;
                }
                return rnInstance.removeNameInternal(targetWallet);
            }

            public override bool revert()
            {
                if (targetWallet == null || nameRecord == null)
                {
                    Logging.error(GetType().Name + " entry is missing target name!");
                    return false;
                }
                return rnInstance.setNameInternal(nameRecord);
            }
        }

        public class RNE_Extend : JournalEntry
        {
            private uint extensionTimeInBlocks = 0;
            private IxiNumber fee;

            private List<RegisteredNameRecord> topLevelNames;

            public ulong expirationBlockHeight { get; private set; } = 0;

            public RNE_Extend(byte[] address, uint extensionTimeInBlocks, IxiNumber fee, List<RegisteredNameRecord> topLevelNames)
            {
                targetWallet = address;
                this.extensionTimeInBlocks = extensionTimeInBlocks;
                this.fee = fee;

                this.topLevelNames = topLevelNames;
            }

            public RNE_Extend(BinaryReader r)
            {
                throw new NotImplementedException();
            }

            public override void writeBytes(BinaryWriter w)
            {
                throw new NotImplementedException();
            }

            public override bool apply()
            {
                if (targetWallet == null)
                {
                    Logging.error(GetType().Name + " entry is missing target name!");
                    return false;
                }
                expirationBlockHeight = rnInstance.extendNameInternal(targetWallet, extensionTimeInBlocks, fee);
                if (expirationBlockHeight <= 0)
                {
                    return false;
                }
                return true;
            }

            public override bool revert()
            {
                if (targetWallet == null)
                {
                    Logging.error(GetType().Name + " entry is missing target name!");
                    return false;
                }
                var result = rnInstance.extendNameInternal(targetWallet, -extensionTimeInBlocks, 0);
                if (result <= 0)
                {
                    return false;
                }
                foreach (var name in topLevelNames)
                {
                    rnInstance.storage.updateRegName(name);
                }
                rnInstance.decreaseRewardPoolInternal(fee);
                return true;
            }
        }

        public class RNE_UpdateCapacity : JournalEntry
        {
            private uint oldCapacity = 0;
            private uint newCapacity = 0;

            private Address oldPkHash;
            private Address newPkHash;

            private byte[] oldSigPk;
            private byte[] newSigPk;

            private byte[] oldSig;
            private byte[] newSig;

            private IxiNumber fee;

            public RNE_UpdateCapacity(
                byte[] address,
                uint oldCapacity,
                Address oldPkHash,
                byte[] oldSigPk,
                byte[] oldSig,
                uint newCapacity,
                Address newPkHash,
                byte[] newSigPk,
                byte[] newSig,
                IxiNumber fee
                )
            {
                targetWallet = address;
                this.oldCapacity = oldCapacity;
                this.newCapacity = newCapacity;
                this.oldPkHash = oldPkHash;
                this.newPkHash = newPkHash;
                this.oldSigPk = oldSigPk;
                this.newSigPk = newSigPk;
                this.oldSig = oldSig;
                this.newSig = newSig;
                this.fee = fee;
            }

            public RNE_UpdateCapacity(BinaryReader r)
            {
                throw new NotImplementedException();
            }

            public override void writeBytes(BinaryWriter w)
            {
                throw new NotImplementedException();
            }

            public override bool apply()
            {
                if (targetWallet == null)
                {
                    Logging.error(GetType().Name + " entry is missing target name!");
                    return false;
                }
                return rnInstance.updateCapacityInternal(targetWallet, newCapacity, newPkHash, newSigPk, newSig, fee);
            }

            public override bool revert()
            {
                if (targetWallet == null)
                {
                    Logging.error(GetType().Name + " entry is missing target name!");
                    return false;
                }
                return rnInstance.revertCapacityInternal(targetWallet, oldCapacity, oldPkHash, oldSigPk, oldSig, fee);
            }
        }

        public class RNE_Recover : JournalEntry
        {
            private Address oldRecoveryKey;
            private Address newRecoveryKey;
            private Address oldPkHash;
            private Address newPkHash;

            private byte[] oldSigPk;
            private byte[] newSigPk;

            private byte[] oldSig;
            private byte[] newSig;

            public RNE_Recover(
                byte[] address,
                Address oldRecoveryKey,
                Address oldPkHash,
                byte[] oldSigPk,
                byte[] oldSig,
                Address newRecoveryKey,
                Address newPkHash,
                byte[] newSigPk,
                byte[] newSig)
            {
                targetWallet = address;
                this.oldRecoveryKey = oldRecoveryKey;
                this.newRecoveryKey = newRecoveryKey;
                this.oldPkHash = oldPkHash;
                this.newPkHash = newPkHash;
                this.oldSigPk = oldSigPk;
                this.newSigPk = newSigPk;
                this.oldSig = oldSig;
                this.newSig = newSig;
            }

            public RNE_Recover(BinaryReader r)
            {
                throw new NotImplementedException();
            }

            public override void writeBytes(BinaryWriter w)
            {
                throw new NotImplementedException();
            }

            public override bool apply()
            {
                if (targetWallet == null)
                {
                    Logging.error(GetType().Name + " entry is missing target name!");
                    return false;
                }
                return rnInstance.recoverNameInternal(targetWallet, newRecoveryKey, newPkHash, newSigPk, newSig);
            }

            public override bool revert()
            {
                if (targetWallet == null)
                {
                    Logging.error(GetType().Name + " entry is missing target name!");
                    return false;
                }
                return rnInstance.revertRecoverNameInternal(targetWallet, oldRecoveryKey, oldPkHash, oldSigPk, oldSig);
            }
        }

        public class RNE_UpdateRecord : JournalEntry
        {
            List<RegisteredNameDataRecord> oldRecords = null;
            List<RegisteredNameDataRecord> newRecords = null;

            Address oldPkHash = null;
            byte[] oldSigPubKey = null;
            byte[] oldSig = null;

            Address nextPkHash = null;
            byte[] newSigPubKey = null;
            byte[] newSig = null;

            public RNE_UpdateRecord(
                byte[] address,
                List<RegisteredNameDataRecord> oldRecords,
                Address oldPkHash,
                byte[] oldSigPubKey,
                byte[] oldSig,
                List<RegisteredNameDataRecord> newRecords,
                Address nextPkHash,
                byte[] newSigPubKey,
                byte[] newSig)
            {
                targetWallet = address;
                this.oldRecords = oldRecords;
                this.newRecords = newRecords;

                this.oldSig = oldSig;
                this.oldSigPubKey = oldSigPubKey;

                this.oldPkHash = oldPkHash;
                this.nextPkHash = nextPkHash;

                this.newSig = newSig;
                this.newSigPubKey = newSigPubKey;
            }

            public RNE_UpdateRecord(BinaryReader r)
            {
                throw new NotImplementedException();
            }

            public override void writeBytes(BinaryWriter w)
            {
                throw new NotImplementedException();
            }

            public override bool apply()
            {
                if (targetWallet == null)
                {
                    Logging.error(GetType().Name + " entry is missing target name!");
                    return false;
                }
                return rnInstance.updateRecordsInternal(targetWallet, newRecords, nextPkHash, newSigPubKey, newSig);
            }

            public override bool revert()
            {
                if (targetWallet == null)
                {
                    Logging.error(GetType().Name + " entry is missing target name!");
                    return false;
                }
                return rnInstance.setNameRecordsInternal(targetWallet, oldRecords, oldPkHash, oldSigPubKey, oldSig, true);
            }
        }

        public class RNE_ToggleAllowSubname : JournalEntry
        {
            private RegisteredNameRecord nameRecord;

            private bool allowSubnames;
            private IxiNumber subnameFee;
            private Address subnameFeeRecipientAddress;

            private Address newPkHash;
            private byte[] newSigPk;
            private byte[] newSig;

            public RNE_ToggleAllowSubname(
                RegisteredNameRecord nameRecord,
                bool allowSubnames,
                IxiNumber subnameFee,
                Address subnameFeeRecipientAddress,
                Address newPkHash,
                byte[] newSigPk,
                byte[] newSig
                )
            {
                targetWallet = nameRecord.name;
                this.nameRecord = nameRecord;
                this.allowSubnames = allowSubnames;
                this.subnameFee = subnameFee;
                this.subnameFeeRecipientAddress = subnameFeeRecipientAddress;
                this.newPkHash = newPkHash;
                this.newSigPk = newSigPk;
                this.newSig = newSig;
            }

            public RNE_ToggleAllowSubname(BinaryReader r)
            {
                throw new NotImplementedException();
            }

            public override void writeBytes(BinaryWriter w)
            {
                throw new NotImplementedException();
            }

            public override bool apply()
            {
                if (targetWallet == null)
                {
                    Logging.error(GetType().Name + " entry is missing target name!");
                    return false;
                }
                return rnInstance.toggleAllowSubnameInternal(targetWallet, allowSubnames, subnameFee, subnameFeeRecipientAddress, newPkHash, newSigPk, newSig);
            }

            public override bool revert()
            {
                if (targetWallet == null)
                {
                    Logging.error(GetType().Name + " entry is missing target name!");
                    return false;
                }
                return rnInstance.setNameInternal(nameRecord);
            }
        }

        public class RNJTransaction : JournalTransaction
        {
            public RNJTransaction(ulong number) : base(number)
            {
            }

            public RNJTransaction(byte[] bytes)
            {
                throw new NotImplementedException();
            }

            public IEnumerable<RegisteredNameRecord> getAffectedRegNames()
            {
                List<RegisteredNameRecord> regNames = new List<RegisteredNameRecord>();
                Dictionary<byte[], bool> addresses = new Dictionary<byte[], bool>(new ByteArrayComparer());
                foreach (var entry in entries)
                {
                    if (entry.targetWallet == null || addresses.ContainsKey(entry.targetWallet))
                    {
                        continue;
                    }
                    regNames.Add(rnInstance.getName(entry.targetWallet));
                    addresses.Add(entry.targetWallet, true);
                }
                return regNames;
            }
        }
    }
}
