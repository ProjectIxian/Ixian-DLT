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
using System.Linq;
using static IXICore.Transaction;

namespace DLT.RegNames
{
    public partial class RegisteredNames : GenericJournal
    {
        private IRegNameStorage storage;

        private byte[] cachedChecksum = null;
        private int cachedBlockVersion = 0;

        private static RegisteredNames rnInstance = null;

        public RegisteredNames(IRegNameStorage storage)
        {
            this.storage = storage;
            rnInstance = this;
        }

        public new bool beginTransaction(ulong blockNum, bool inTransaction = true)
        {
            lock (stateLock)
            {
                if (currentTransaction != null)
                {
                    // Transaction is already open
                    return false;
                }
                var tx = new RNJTransaction(blockNum);
                currentTransaction = tx;
                this.inTransaction = inTransaction;
                return true;
            }
        }

        public List<RegisteredNameRecord> getTopLevelNames(byte[] id)
        {
            List<RegisteredNameRecord> records = new();
            lock (stateLock)
            {
                List<byte[]> splitIds = IxiNameUtils.splitIxiNameBytes(id);
                byte[] topId = null;
                int level = 0;
                foreach (var subId in splitIds)
                {
                    if (topId == null)
                    {
                        topId = subId;
                    }
                    else
                    {
                        byte[] tmpTopId = new byte[topId.Length + subId.Length];
                        Array.Copy(topId, 0, tmpTopId, 0, topId.Length);
                        Array.Copy(subId, 0, tmpTopId, topId.Length, subId.Length);
                        topId = tmpTopId;
                    }

                    var rnRecord = storage.getRegNameHeader(topId);
                    if (rnRecord == null)
                    {
                        break;
                    }

                    if (!rnRecord.allowSubnames)
                    {
                        break;
                    }

                    if (level == ConsensusConfig.rnMaxSubNameLevels)
                    {
                        break;
                    }

                    records.Add(rnRecord);
                    level++;
                }
            }
            return records;
        }

        public RegisteredNameRecord getName(byte[] id, bool useAbsoluteId = true)
        {
            RegisteredNameRecord previousRnRecord = null;
            lock (stateLock)
            {
                if (useAbsoluteId)
                {
                    return storage.getRegNameHeader(id);
                }

                List<byte[]> splitIds = IxiNameUtils.splitIxiNameBytes(id);
                byte[] topId = null;
                int level = 0;
                foreach (var subId in splitIds)
                {
                    if (topId == null)
                    {
                        topId = subId;
                    } else
                    {
                        byte[] tmpTopId = new byte[topId.Length + subId.Length];
                        Array.Copy(topId, 0, tmpTopId, 0, topId.Length);
                        Array.Copy(subId, 0, tmpTopId, topId.Length, subId.Length);
                        topId = tmpTopId;
                    }

                    var rnRecord = storage.getRegNameHeader(topId);
                    if (rnRecord == null)
                    {
                        return previousRnRecord;
                    }

                    if (!rnRecord.allowSubnames)
                    {
                        return rnRecord;
                    }

                    if (level == ConsensusConfig.rnMaxSubNameLevels)
                    {
                        return rnRecord;
                    }

                    previousRnRecord = rnRecord;
                    level++;
                }
            }
            return previousRnRecord;
        }

        public List<RegisteredNameDataRecord> getNameData(byte[] id)
        {
            lock (stateLock)
            {
                var regName = getName(id, false);
                if (regName == null)
                {
                    return null;
                }

                byte[] dataRecordKey = new byte[] { (byte)'@' };
                if (id.Length - regName.name.Length > 0)
                {
                    dataRecordKey = new byte[id.Length - regName.name.Length];
                    Array.Copy(id, regName.name.Length, dataRecordKey, 0, dataRecordKey.Length);

                }
                return regName.getDataRecords(dataRecordKey).ToList();
            }
        }

        public IxiNumber getRewardPool()
        {
            lock (stateLock)
            {
                return storage.getRewardPool();
            }
        }

        public ulong count()
        {
            lock (stateLock)
            {
                return storage.count();
            }
        }

        public void clear()
        {
            Logging.info("Clearing reg names state!!");
            lock (stateLock)
            {
                storage.clear();
                cachedChecksum = null;
                currentTransaction = null;
                inTransaction = false;
                processedJournalTransactions.Clear();
            }
        }

        #region Reg Name Records Manipulation Methods - internal use
        // this is called only by RegNameEntries
        private bool registerNameInternal(RegisteredNameRecord r, IxiNumber fee)
        {
            lock (stateLock)
            {
                var rnRecord = getName(r.name, false);
                var splitName = IxiNameUtils.splitIxiNameBytes(r.name);
                if (rnRecord != null)
                {
                    if (splitName.Count == 1 || rnRecord.name.SequenceEqual(r.name))
                    {
                        Logging.error("Registered name {0} already exists.", Crypto.hashToString(r.name));
                        return false;
                    }

                    if (splitName.Count > 2)
                    {
                        Logging.error("Rregistered subname {0} is invalid.", Crypto.hashToString(r.name));
                        return false;
                    }

                    if (!rnRecord.allowSubnames)
                    {
                        Logging.error("Registered top name for {0} doesn't allow subnames.", Crypto.hashToString(r.name));
                        return false;
                    }
                } else
                {
                    if (splitName.Count == 2)
                    {
                        Logging.error("Registered top name for {0} doesn't exist.", Crypto.hashToString(r.name));
                        return false;
                    }else if (splitName.Count > 2)
                    {
                        Logging.error("Rregistered subname {0} is invalid.", Crypto.hashToString(r.name));
                        return false;
                    }
                }

                storage.createRegName(r);
                if (rnRecord != null && rnRecord.expirationBlockHeight < r.expirationBlockHeight)
                {
                    rnRecord.setExpirationBlockHeight(r.expirationBlockHeight, r.registrationBlockHeight);
                    storage.updateRegName(rnRecord);
                }
                storage.increaseRewardPool(fee);
                cachedChecksum = null;
                return true;
            }
        }

        private ulong extendNameInternal(byte[] id, long extensionTimeInBlocks, IxiNumber fee, ulong updatedBlockHeight)
        {
            lock (stateLock)
            {
                RegisteredNameRecord rnr = getName(id);
                if (rnr == null)
                {
                    Logging.error("Registered name {0} does not exist.", Crypto.hashToString(id));
                    return 0;
                }
                if (extensionTimeInBlocks > 0)
                {
                    rnr.setExpirationBlockHeight(rnr.expirationBlockHeight + (ulong)extensionTimeInBlocks, updatedBlockHeight);
                } else
                {
                    rnr.setExpirationBlockHeight(rnr.expirationBlockHeight - (ulong)-extensionTimeInBlocks, updatedBlockHeight);
                }

                storage.updateRegName(rnr);


                var topLevelNames = getTopLevelNames(rnr.name);
                if (topLevelNames.Count > 0)
                {
                    var topLevelName = topLevelNames.First();
                    if (topLevelName.expirationBlockHeight < rnr.expirationBlockHeight)
                    {
                        topLevelName.setExpirationBlockHeight(rnr.expirationBlockHeight, updatedBlockHeight);
                        storage.updateRegName(topLevelName);
                    }
                }

                storage.increaseRewardPool(fee);
                cachedChecksum = null;
                return rnr.expirationBlockHeight;
            }
        }

        private bool updateCapacityInternal(byte[] id, uint newCapacity, Address nextPkHash, byte[] sigPubKey, byte[] sig, ulong updatedBlockHeight, IxiNumber fee)
        {
            lock (stateLock)
            {
                if (newCapacity < ConsensusConfig.rnMinCapacity)
                {
                    Logging.error("Invalid capacity change requested for registered name {0}.", Crypto.hashToString(id));
                    return false;
                }

                RegisteredNameRecord rnr = getName(id);
                if (rnr == null)
                {
                    Logging.error("Registered name {0} does not exist.", Crypto.hashToString(id));
                    return false;
                }

                if (rnr.allowSubnames)
                {
                    Logging.error("Registered name {0} cannot have capacity updated because it allows subnames.", Crypto.hashToString(id));
                    return false;
                }

                int totalRecordSize = rnr.getTotalRecordSize();
                if (totalRecordSize > newCapacity * 1024)
                {
                    Logging.error("New name records size is too large to reduce capacity {0} > {1} for {2}. Remove some records first.", totalRecordSize, newCapacity * 1024, Crypto.hashToString(id));
                    return false;
                }

                if (!rnr.nextPkHash.addressNoChecksum.SequenceEqual(new Address(sigPubKey).addressNoChecksum))
                {
                    Logging.error("Invalid signature public key received when updating capacity for name {0}.", Crypto.hashToString(id));
                    return false;
                }

                rnr.setCapacity(newCapacity, rnr.sequence + 1, nextPkHash, sigPubKey, sig, updatedBlockHeight);
                var rnrChecksum = rnr.calculateChecksum(RegNameRecordByteTypes.forSignature);

                if (!CryptoManager.lib.verifySignature(rnrChecksum, sigPubKey, sig))
                {
                    Logging.error("Invalid signature received when updating capacity for name {0}.", Crypto.hashToString(id));
                    return false;
                }

                storage.updateRegName(rnr);
                storage.increaseRewardPool(fee);
                cachedChecksum = null;
                return true;
            }
        }

        private bool revertCapacityInternal(byte[] id, uint newCapacity, Address nextPkHash, byte[] sigPubKey, byte[] sig, ulong updateBlockHeight, IxiNumber fee)
        {
            lock (stateLock)
            {
                RegisteredNameRecord rnr = getName(id);
                if (rnr == null)
                {
                    Logging.error("Registered name {0} does not exist.", Crypto.hashToString(id));
                    return false;
                }

                int totalRecordSize = rnr.getTotalRecordSize();
                if (totalRecordSize > newCapacity * 1024)
                {
                    Logging.error("New name records size is too large to reduce capacity {0} > {1} for {2}. Remove some records first.", totalRecordSize, newCapacity * 1024, Crypto.hashToString(id));
                    return false;
                }

                if ((sigPubKey != null && sig != null) && !rnr.nextPkHash.addressNoChecksum.SequenceEqual(new Address(sigPubKey).addressNoChecksum))
                {
                    Logging.error("Invalid signature public key received when updating capacity for name {0}.", Crypto.hashToString(id));
                    return false;
                }

                if (rnr.sequence < 1)
                {
                    Logging.error("Sequence is invalid while reverting capacity for name {0}.", Crypto.hashToString(id));
                    return false;
                }

                rnr.setCapacity(newCapacity, rnr.sequence - 1, nextPkHash, sigPubKey, sig, updateBlockHeight);
                var rnrChecksum = rnr.calculateChecksum(RegNameRecordByteTypes.forSignature);

                if ((sigPubKey != null && sig != null) && !CryptoManager.lib.verifySignature(rnrChecksum, sigPubKey, sig))
                {
                    Logging.error("Invalid signature received when updating capacity for name {0}.", Crypto.hashToString(id));
                    return false;
                }

                storage.updateRegName(rnr);
                storage.decreaseRewardPool(fee);
                cachedChecksum = null;
                return true;
            }
        }

        private bool updateRecordsInternal(byte[] id, List<RegisteredNameDataRecord> updateRecords, Address nextPkHash, byte[] sigPubKey, byte[] sig, ulong updateBlockHeight)
        {
            lock (stateLock)
            {
                RegisteredNameRecord rnr = getName(id);
                if (rnr == null)
                {
                    Logging.error("Registered name {0} does not exist.", Crypto.hashToString(id));
                    return false;
                }

                List<RegisteredNameDataRecord> curRecords = RegisteredNamesTransactions.mergeDataRecords(id, rnr.getDataRecords(null), updateRecords, rnr.allowSubnames);
                if (curRecords == null)
                {
                    return false;
                }

                int totalRecordSize = rnr.getTotalRecordSize();
                if (totalRecordSize > rnr.capacity * 1024)
                {
                    Logging.error("New name records size is larger than capacity {0} > {1} for {2}. Please increase capacity or remove some records.", totalRecordSize, rnr.capacity * 1024, Crypto.hashToString(id));
                    return false;
                }

                if (!rnr.nextPkHash.addressNoChecksum.SequenceEqual(new Address(sigPubKey).addressNoChecksum))
                {
                    Logging.error("Invalid signature public key received when updating records for name {0}.", Crypto.hashToString(id));
                    return false;
                }

                rnr.setRecords(curRecords, rnr.sequence + 1, nextPkHash, sigPubKey, sig, updateBlockHeight);
                var rnrChecksum = rnr.calculateChecksum(RegNameRecordByteTypes.forSignature);

                if (!CryptoManager.lib.verifySignature(rnrChecksum, sigPubKey, sig))
                {
                    Logging.error("Invalid signature received when updating records for name {0}.", Crypto.hashToString(id));
                    return false;
                }
                storage.updateRegName(rnr);
                cachedChecksum = null;
                return true;
            }
        }

        private bool setNameRecordsInternal(byte[] id, List<RegisteredNameDataRecord> records, Address nextPkHash, byte[] sigPubKey, byte[] sig, ulong updateBlockHeight, bool reverting)
        {
            lock (stateLock)
            {
                RegisteredNameRecord rnr = getName(id);
                if (rnr == null)
                {
                    Logging.error("Registered name {0} does not exist.", Crypto.hashToString(id));
                    return false;
                }

                ulong nextSequence;
                if (reverting)
                {
                    if (rnr.sequence < 1)
                    {
                        Logging.error("Sequence is invalid while reverting capacity for name {0}.", Crypto.hashToString(id));
                        return false;
                    }
                    nextSequence = rnr.sequence - 1;
                } else
                {
                    nextSequence = rnr.sequence + 1;
                }

                rnr.setRecords(records, nextSequence, nextPkHash, sigPubKey, sig, updateBlockHeight);
                var rnrChecksum = rnr.calculateChecksum(RegNameRecordByteTypes.forSignature);

                if ((sigPubKey != null && sig != null) && !CryptoManager.lib.verifySignature(rnrChecksum, sigPubKey, sig))
                {
                    Logging.error("Invalid signature received when updating capacity for name {0}.", Crypto.hashToString(id));
                    return false;
                }
                storage.updateRegName(rnr);
                cachedChecksum = null;
                return true;
            }
        }

        private bool recoverNameInternal(byte[] id, Address newRecoveryHash, Address nextPkHash, byte[] sigPubKey, byte[] sig, ulong updateBlockHeight)
        {
            lock (stateLock)
            {
                RegisteredNameRecord rnr = getName(id);
                if (rnr == null)
                {
                    Logging.error("Registered name {0} does not exist.", Crypto.hashToString(id));
                    return false;
                }

                if (!rnr.recoveryHash.addressNoChecksum.SequenceEqual(new Address(sigPubKey).addressNoChecksum))
                {
                    Logging.error("Invalid signature public key received when recovering name {0}.", Crypto.hashToString(id));
                    return false;
                }

                rnr.setRecoveryHash(newRecoveryHash, rnr.sequence + 1, nextPkHash, sigPubKey, sig, updateBlockHeight);
                byte[] rnrChecksum = rnr.calculateChecksum(RegNameRecordByteTypes.forSignature);

                if (!CryptoManager.lib.verifySignature(rnrChecksum, sigPubKey, sig))
                {
                    Logging.error("Invalid signature received when recovering name {0}.", Crypto.hashToString(id));
                    return false;
                }

                storage.updateRegName(rnr);
                cachedChecksum = null;
                return true;
            }
        }

        private bool revertRecoverNameInternal(byte[] id, Address newRecoveryHash, Address nextPkHash, byte[] sigPubKey, byte[] sig, ulong updateBlockHeight)
        {
            lock (stateLock)
            {
                RegisteredNameRecord rnr = getName(id);
                if (rnr == null)
                {
                    Logging.error("Registered name {0} does not exist.", Crypto.hashToString(id));
                    return false;
                }

                if ((sigPubKey != null && sig != null) && !rnr.recoveryHash.addressNoChecksum.SequenceEqual(new Address(sigPubKey).addressNoChecksum))
                {
                    Logging.error("Invalid signature public key received when recovering name {0}.", Crypto.hashToString(id));
                    return false;
                }

                if (rnr.sequence < 1)
                {
                    Logging.error("Sequence is invalid while reverting recovery for name {0}.", Crypto.hashToString(id));
                    return false;
                }

                rnr.setRecoveryHash(newRecoveryHash, rnr.sequence - 1, nextPkHash, sigPubKey, sig, updateBlockHeight);
                byte[] rnrChecksum = rnr.calculateChecksum(RegNameRecordByteTypes.forSignature);

                if ((sigPubKey != null && sig != null) && !CryptoManager.lib.verifySignature(rnrChecksum, sigPubKey, sig))
                {
                    Logging.error("Invalid signature received when recovering name {0}.", Crypto.hashToString(id));
                    return false;
                }

                storage.updateRegName(rnr);
                cachedChecksum = null;
                return true;
            }
        }

        private bool toggleAllowSubnameInternal(byte[] id, bool allowSubnames, IxiNumber subnameFee, Address subnameFeeRecipientAddress, Address nextPkHash, byte[] sigPubKey, byte[] sig, ulong updateBlockHeight)
        {
            lock (stateLock)
            {
                RegisteredNameRecord rnr = getName(id);
                if (rnr == null)
                {
                    Logging.error("Registered name {0} does not exist.", Crypto.hashToString(id));
                    return false;
                }

                if (!rnr.nextPkHash.addressNoChecksum.SequenceEqual(new Address(sigPubKey).addressNoChecksum))
                {
                    Logging.error("Invalid signature public key received when toggling allow subname {0}.", Crypto.hashToString(id));
                    return false;
                }

                rnr.setAllowSubnames(allowSubnames, subnameFee, subnameFeeRecipientAddress, rnr.sequence + 1, nextPkHash, sigPubKey, sig, updateBlockHeight);
                byte[] rnrChecksum = rnr.calculateChecksum(RegNameRecordByteTypes.forSignature);

                if (!CryptoManager.lib.verifySignature(rnrChecksum, sigPubKey, sig))
                {
                    Logging.error("Invalid signature received when toggling allow subname {0}.", Crypto.hashToString(id));
                    return false;
                }

                storage.updateRegName(rnr);
                cachedChecksum = null;
                return true;
            }
        }

        private bool removeNameInternal(byte[] id)
        {
            lock (stateLock)
            {
                if (!storage.removeRegName(id))
                {
                    Logging.error("Error removing registered name {0}.", Crypto.hashToString(id));
                    return false;
                }
                cachedChecksum = null;
                return true;
            }
        }

        private bool setNameInternal(RegisteredNameRecord r)
        {
            lock (stateLock)
            {
                storage.updateRegName(r, true);
                cachedChecksum = null;
                return true;
            }
        }

        private bool decreaseRewardPoolInternal(IxiNumber amount)
        {
            lock (stateLock)
            {
                storage.decreaseRewardPool(amount);
                cachedChecksum = null;
                return true;
            }
        }

        public bool increaseRewardPoolInternal(IxiNumber amount)
        {
            lock (stateLock)
            {
                storage.increaseRewardPool(amount);
                cachedChecksum = null;
                return true;
            }
        }
        #endregion


        #region Reg Name Records Manipulation Methods - public use
        public ulong getHighestExpirationBlockHeight()
        {
            return storage.getHighestExpirationBlockHeight();
        }

        public bool setHighestExpirationBlockHeight(ulong newBlockHeight, ulong oldBlockHeight)
        {
            var change = new RNE_SetHighestExpirationBlockHeight(newBlockHeight, oldBlockHeight);
            if (!change.apply())
            {
                return false;
            }

            if (currentTransaction != null)
            {
                currentTransaction.addChange(change);
            }
            return true;
        }

        public bool registerName(RegNameRegister rnr, ulong curBlockHeight, IxiNumber regFee, out ulong expirationBlockHeight)
        {
            expirationBlockHeight = rnr.registrationTimeInBlocks + curBlockHeight;
            return registerName(rnr.name, curBlockHeight, rnr.capacity, expirationBlockHeight, rnr.nextPkHash, rnr.recoveryHash, regFee);
        }

        public bool registerName(byte[] id, ulong registrationBlockHeight, uint initialCapacity, ulong expirationBlockHeight, Address nextPkHash, Address recoveryHash, IxiNumber regFee)
        {
            RegisteredNameRecord rnr = getName(id);
            if (rnr != null)
            {
                Logging.warn("Registered name {0} already exists, can't create.", Crypto.hashToString(id));
                return false;
            }

            var topLevelNames = getTopLevelNames(id);
            var change = new RNE_Register(id, registrationBlockHeight, initialCapacity, expirationBlockHeight, nextPkHash, recoveryHash, regFee, topLevelNames);
            if (!change.apply())
            {
                return false;
            }

            if (currentTransaction != null)
            {
                currentTransaction.addChange(change);
            }
            return true;
        }

        public void removeExpiredNames(ulong blockHeight)
        {
            lock (stateLock)
            {
                var names = storage.getExpiredNames(blockHeight);
                foreach (RegisteredNameRecord rnr in names)
                {
                    removeName(rnr.name);
                }
            }
        }

        public bool decreaseRewardPool(IxiNumber amount)
        {
            var change = new RNE_DecreaseRewardPool(amount);
            if (!change.apply())
            {
                return false;
            }

            if (currentTransaction != null)
            {
                currentTransaction.addChange(change);
            }
            return true;
        }

        public bool updateRecords(RegNameUpdateRecord rnu, ulong curBlockHeight)
        {
            return updateRecords(rnu.name, rnu.records, rnu.nextPkHash, new Address(rnu.signaturePk), rnu.signature, curBlockHeight);
        }

        public bool updateRecords(byte[] id, List<RegisteredNameDataRecord> newRecords, Address nextPkHash, Address pubKey, byte[] signature, ulong curBlockHeight)
        {
            RegisteredNameRecord r = getName(id);
            if (r == null)
            {
                Logging.warn("Registered name {0} doesn't exist, can't update records.", Crypto.hashToString(id));
                return false;
            }

            var change = new RNE_UpdateRecord(id, r.getDataRecords(null), r.nextPkHash, r.signaturePk, r.signature, r.updatedBlockHeight, newRecords, nextPkHash, pubKey.pubKey, signature, curBlockHeight);
            if (!change.apply())
            {
                return false;
            }

            if (currentTransaction != null)
            {
                currentTransaction.addChange(change);
            }
            return true;
        }

        public bool extendName(RegNameExtend rne, IxiNumber fee, ulong curBlockHeight, out ulong expirationBlockHeight)
        {
            return extendName(rne.name, rne.extensionTimeInBlocks, fee, curBlockHeight, out expirationBlockHeight);
        }

        public bool extendName(byte[] id, uint extensionTimeInBlocks, IxiNumber fee, ulong curBlockHeight, out ulong expirationBlockHeight)
        {
            expirationBlockHeight = 0;
            RegisteredNameRecord r = getName(id);
            if (r == null)
            {
                Logging.warn("Registered name {0} doesn't exist, can't extend it.", Crypto.hashToString(id));
                return false;
            }

            var topLevelNames = getTopLevelNames(id);
            var change = new RNE_Extend(id, extensionTimeInBlocks, fee, topLevelNames, r.updatedBlockHeight, curBlockHeight);
            if (!change.apply())
            {
                return false;
            }

            expirationBlockHeight = change.expirationBlockHeight;
            if (currentTransaction != null)
            {
                currentTransaction.addChange(change);
            }
            return true;
        }

        public bool updateCapacity(RegNameChangeCapacity rnCap, IxiNumber fee, ulong curBlockHeight)
        {
            return updateCapacity(rnCap.name, rnCap.newCapacity, rnCap.nextPkHash, rnCap.signaturePk, rnCap.signature, fee, curBlockHeight);
        }

        public bool updateCapacity(byte[] id, uint newCapacity, Address nextPkHash, byte[] sigPubKey, byte[] sig, IxiNumber fee, ulong curBlockHeight)
        {
            RegisteredNameRecord r = getName(id);
            if (r == null)
            {
                Logging.warn("Registered name {0} doesn't exist, can't extend it.", Crypto.hashToString(id));
                return false;
            }
            var change = new RNE_UpdateCapacity(id, r.capacity, r.nextPkHash, r.signaturePk, r.signature, r.updatedBlockHeight, newCapacity, nextPkHash, sigPubKey, sig, curBlockHeight, fee);
            if (!change.apply())
            {
                return false;
            }

            if (currentTransaction != null)
            {
                currentTransaction.addChange(change);
            }
            return true;
        }

        public bool toggleAllowSubname(RegNameToggleAllowSubnames rnSub, ulong curBlockHeight)
        {
            return toggleAllowSubname(rnSub.name, rnSub.allowSubnames, rnSub.fee, rnSub.feeRecipientAddress, rnSub.nextPkHash, rnSub.signaturePk, rnSub.signature, curBlockHeight);
        }

        public bool toggleAllowSubname(byte[] id, bool allowSubnames, IxiNumber subNameFee, Address feeRecipientAddress, Address nextPkHash, byte[] sigPubKey, byte[] sig, ulong curBlockHeight)
        {
            RegisteredNameRecord r = getName(id);
            if (r == null)
            {
                Logging.warn("Registered name {0} doesn't exist, can't toggle allow subname.", Crypto.hashToString(id));
                return false;
            }
            var change = new RNE_ToggleAllowSubname(r, allowSubnames, subNameFee, feeRecipientAddress, nextPkHash, sigPubKey, sig, curBlockHeight);
            if (!change.apply())
            {
                return false;
            }

            if (currentTransaction != null)
            {
                currentTransaction.addChange(change);
            }
            return true;
        }

        public bool recoverName(RegNameRecover rnr, ulong curBlockHeight)
        {
            return recoverName(rnr.name, rnr.nextPkHash, rnr.newRecoveryHash, rnr.signaturePk, rnr.signature, curBlockHeight);
        }

        public bool recoverName(byte[] id, Address nextPkHash, Address newRecoveryHash, byte[] sigPubKey, byte[] signature, ulong curBlockHeight)
        {
            RegisteredNameRecord r = getName(id);
            if (r == null)
            {
                Logging.warn("Registered name {0} doesn't exist, can't recover it.", Crypto.hashToString(id));
                return false;
            }
            var change = new RNE_Recover(id, r.recoveryHash, r.nextPkHash, r.signaturePk, r.signature, r.updatedBlockHeight, newRecoveryHash, nextPkHash, sigPubKey, signature, curBlockHeight);
            if (!change.apply())
            {
                return false;
            }

            if (currentTransaction != null)
            {
                currentTransaction.addChange(change);
            }
            return true;
        }

        public bool removeName(byte[] id)
        {
            RegisteredNameRecord r = getName(id);
            if (r == null)
            {
                Logging.warn("Registered name {0} doesn't exist, can't remove.", Crypto.hashToString(id));
                return false;
            }
            var change = new RNE_Destroy(id, r);
            if (!change.apply())
            {
                return false;
            }

            if (currentTransaction != null)
            {
                currentTransaction.addChange(change);
            }
            return true;
        }
        #endregion

        public void setCachedBlockVersion(int blockVersion)
        {
            if (cachedBlockVersion != blockVersion)
            {
                cachedChecksum = null;
                cachedBlockVersion = blockVersion;
            }
        }

        public RegisteredNameRecord[] debugGetRegisteredNames()
        {
            return storage.debugGetRegisteredNames();
        }

        public byte[] calculateRegNameChecksumFromUpdatedDataRecords(byte[] id, List<RegisteredNameDataRecord> dataRecords, ulong sequence, Address nextPkHash)
        {
            var rnr = getName(id);
            if (rnr == null)
            {
                return null;
            }

            return RegisteredNamesTransactions.calculateRegNameChecksumFromUpdatedDataRecords(rnr, id, dataRecords, sequence, nextPkHash);
        }

        public byte[] calculateRegNameChecksumForRecovery(byte[] id, Address recoveryHash, ulong sequence, Address nextPkHash)
        {
            var rnr = getName(id);
            if (rnr == null)
            {
                return null;
            }

            return RegisteredNamesTransactions.calculateRegNameChecksumForRecovery(rnr, id, recoveryHash, sequence, nextPkHash);
        }

        public bool verifyTransaction(Transaction tx, IxiNumber minPricePerUnit = null)
        {
            Logging.trace("Verifying RN Tx: {0}", tx.getTxIdString());

            if (tx.toList.Count > 3)
            {
                Logging.error("RN Transaction {0} has too many outputs.", tx.getTxIdString());
                return false;
            }

            if (!tx.toList.First().Key.SequenceEqual(ConsensusConfig.rnRewardPoolAddress))
            {
                Logging.error("RN Transaction {0}'s first output is to the wrong address.", tx.getTxIdString());
                return false;
            }

            if (tx.toList.Count == 2 && tx.toList.ElementAt(1).Key.SequenceEqual(ConsensusConfig.rnRewardPoolAddress))
            {
                Logging.error("RN Transaction {0}'s second output is to the wrong address, it must be top name's recipient address or your return address if used.", tx.getTxIdString());
                return false;
            }

            if (tx.toList.Count == 3 && tx.toList.ElementAt(2).Key.SequenceEqual(ConsensusConfig.rnRewardPoolAddress))
            {
                Logging.error("RN Transaction {0}'s second output is to the wrong address, it must be your return address if used.", tx.getTxIdString());
                return false;
            }

            ToEntry txOut = tx.toList.First().Value;
            if (txOut.amount < 0)
            {
                Logging.error("RN Transaction {0} tried to use negative amount.", tx.getTxIdString());
                return false;
            }

            byte[] txOutData = txOut.data;
            RegNameInstruction rni = (RegNameInstruction)txOutData[0];
            switch(rni)
            {
                case RegNameInstruction.register:
                    if (!verifyRegisterTransaction(tx, minPricePerUnit))
                    {
                        return false;
                    }
                    break;
                case RegNameInstruction.extend:
                    if (!verifyExtendTransaction(tx, minPricePerUnit))
                    {
                        return false;
                    }
                    break;
                case RegNameInstruction.recover:
                    if (!verifyRecoverTransaction(tx))
                    {
                        return false;
                    }
                    break;
                case RegNameInstruction.changeCapacity:
                    if (!verifyChangeCapacityTransaction(tx, minPricePerUnit))
                    {
                        return false;
                    }
                    break;
                case RegNameInstruction.toggleAllowSubnames:
                    if (!verifyToggleAllowSubnamesTransaction(tx))
                    {
                        return false;
                    }
                    break;
                case RegNameInstruction.updateRecord:
                    if (!verifyUpdateRecordTransaction(tx))
                    {
                        return false;
                    }
                    break;
            }

            return true;
        }

        private bool verifyRegisterTransaction(Transaction tx, IxiNumber minPricePerUnit)
        {
            ToEntry txOut = tx.toList.First().Value;
            byte[] txOutData = txOut.data;
            RegNameRegister rnReg = new RegNameRegister(txOutData);
            var rnRecord = getName(rnReg.name, false);
            var splitName = IxiNameUtils.splitIxiNameBytes(rnReg.name);
            if (rnRecord != null)
            {
                if (splitName.Count == 1 || rnRecord.name.SequenceEqual(rnReg.name))
                {
                    Logging.error("RN Transaction {0} tried to register name {1} but name is already registered.", tx.getTxIdString(), Crypto.hashToString(rnReg.name));
                    return false;
                }

                if (splitName.Count > 2)
                {
                    Logging.error("RN Transaction {0} tried to register a subname {1} but name is invalid.", tx.getTxIdString(), Crypto.hashToString(rnReg.name));
                    return false;
                }

                if (!rnRecord.allowSubnames)
                {
                    Logging.error("RN Transaction {0} tried to register a subname {1} but top name doesn't allow subnames.", tx.getTxIdString(), Crypto.hashToString(rnReg.name));
                    return false;
                }

                if (rnRecord.subnamePrice > 0)
                {
                    IxiNumber minSubnameFee = RegisteredNamesTransactions.calculateExpectedRegistrationFee(rnReg.registrationTimeInBlocks, rnReg.capacity, rnRecord.subnamePrice);
                    if (tx.toList.Count < 2
                        || tx.toList.Values.ElementAt(1).amount < minSubnameFee
                        || !tx.toList.Keys.ElementAt(1).addressNoChecksum.SequenceEqual(rnRecord.subnameFeeRecipient.addressNoChecksum))
                    {
                        Logging.error("RN Transaction {0} tried to register a subname {1} but no fee is paid to recipient.", tx.getTxIdString(), Crypto.hashToString(rnReg.name));
                        return false;
                    }
                }
            } else
            {
                if (splitName.Count == 2)
                {
                    Logging.error("RN Transaction {0} tried to register a subname {1} but top name doesn't exist.", tx.getTxIdString(), Crypto.hashToString(rnReg.name));
                    return false;
                }
                else if (splitName.Count > 2)
                {
                    Logging.error("RN Transaction {0} tried to register a subname {1} but name is invalid.", tx.getTxIdString(), Crypto.hashToString(rnReg.name));
                    return false;
                }
            }

            if (rnReg.capacity < ConsensusConfig.rnMinCapacity)
            {
                Logging.error("RN Transaction {0} tried to register name {1} with invalid capacity.", tx.getTxIdString(), Crypto.hashToString(rnReg.name));
                return false;
            }

            if (rnReg.registrationTimeInBlocks < ConsensusConfig.rnMinRegistrationTimeInBlocks)
            {
                Logging.error("RN Transaction {0} tried to register name {1} with invalid registration time {2} < {3}.", tx.getTxIdString(), Crypto.hashToString(rnReg.name), rnReg.registrationTimeInBlocks, ConsensusConfig.rnMinRegistrationTimeInBlocks);
                return false;
            }

            if (rnReg.registrationTimeInBlocks > ConsensusConfig.rnMaxRegistrationTimeInBlocks)
            {
                Logging.error("RN Transaction {0} tried to register name {1} with invalid registration time {2} > {3}.", tx.getTxIdString(), Crypto.hashToString(rnReg.name), rnReg.registrationTimeInBlocks, ConsensusConfig.rnMaxRegistrationTimeInBlocks);
                return false;
            }

            if ((rnReg.registrationTimeInBlocks % ConsensusConfig.rnMonthInBlocks) != 0)
            {
                Logging.error("RN Transaction {0} tried to register name {1} with registration time that's not a factor of {2}.", tx.getTxIdString(), Crypto.hashToString(rnReg.name), ConsensusConfig.rnMonthInBlocks);
                return false;
            }

            IxiNumber minExpectedFee = RegisteredNamesTransactions.calculateExpectedRegistrationFee(rnReg.registrationTimeInBlocks, rnReg.capacity, minPricePerUnit);
            if (txOut.amount < minExpectedFee)
            {
                Logging.error("RN Transaction {0} tried to register name {1} with too low fee {2} < {3}.", tx.getTxIdString(), Crypto.hashToString(rnReg.name), txOut.amount, minExpectedFee);
                return false;
            }

            return true;
        }

        private bool verifyExtendTransaction(Transaction tx, IxiNumber minPricePerUnit)
        {
            ToEntry txOut = tx.toList.First().Value;
            byte[] txOutData = txOut.data;
            RegNameExtend rnExt = new RegNameExtend(txOutData);
            RegisteredNameRecord rnr = getName(rnExt.name);

            if (rnr == null)
            {
                Logging.error("RN Transaction {0} tried to extend name {1} but name doesn't exist.", tx.getTxIdString(), Crypto.hashToString(rnExt.name));
                return false;
            }

            if (rnExt.extensionTimeInBlocks < ConsensusConfig.rnMinRegistrationTimeInBlocks)
            {
                Logging.error("RN Transaction {0} tried to extend name {1} with invalid registration time {2} < {3}.", tx.getTxIdString(), Crypto.hashToString(rnExt.name), rnExt.extensionTimeInBlocks, ConsensusConfig.rnMinRegistrationTimeInBlocks);
                return false;
            }

            if ((rnr.expirationBlockHeight - IxianHandler.getLastBlockHeight()) + rnExt.extensionTimeInBlocks > ConsensusConfig.rnMaxRegistrationTimeInBlocks)
            {
                Logging.error("RN Transaction {0} tried to extend name {1} with invalid registration time {2} > {3}.", tx.getTxIdString(), Crypto.hashToString(rnExt.name), (rnr.expirationBlockHeight - IxianHandler.getLastBlockHeight()) + rnExt.extensionTimeInBlocks, ConsensusConfig.rnMaxRegistrationTimeInBlocks);
                return false;
            }

            if ((rnExt.extensionTimeInBlocks % ConsensusConfig.rnMonthInBlocks) != 0)
            {
                Logging.error("RN Transaction {0} tried to extend name {1} with registration time that's not a factor of {2}.", tx.getTxIdString(), Crypto.hashToString(rnExt.name), ConsensusConfig.rnMonthInBlocks);
                return false;
            }

            var topLevelNames = getTopLevelNames(rnExt.name);
            if (topLevelNames.Count > 0)
            {
                var topLevelName = topLevelNames.First();
                if (topLevelName.subnamePrice > 0)
                {
                    IxiNumber minSubnameFee = RegisteredNamesTransactions.calculateExpectedRegistrationFee(rnExt.extensionTimeInBlocks, rnr.capacity, topLevelName.subnamePrice);
                    if (tx.toList.Count < 2
                        || tx.toList.Values.ElementAt(1).amount < minSubnameFee
                        || !tx.toList.Keys.ElementAt(1).addressNoChecksum.SequenceEqual(topLevelName.subnameFeeRecipient.addressNoChecksum))
                    {
                        Logging.error("RN Transaction {0} tried to extend a subname {1} but no fee is paid to recipient.", tx.getTxIdString(), Crypto.hashToString(rnExt.name));
                        return false;
                    }
                }
            }

            IxiNumber minExpectedFee = RegisteredNamesTransactions.calculateExpectedRegistrationFee(rnExt.extensionTimeInBlocks, rnr.capacity, minPricePerUnit);
            if (txOut.amount < minExpectedFee)
            {
                Logging.error("RN Transaction {0} tried to extend name {1} with too low fee {2} < {3}.", tx.getTxIdString(), Crypto.hashToString(rnExt.name), txOut.amount, minExpectedFee);
                return false;
            }

            return true;
        }

        private bool verifyChangeCapacityTransaction(Transaction tx, IxiNumber minPricePerUnit)
        {
            ToEntry txOut = tx.toList.First().Value;
            byte[] txOutData = txOut.data;
            RegNameChangeCapacity rnCap = new RegNameChangeCapacity(txOutData);
            RegisteredNameRecord rnr = getName(rnCap.name);
            if (rnr == null)
            {
                Logging.error("RN Transaction {0} tried to change capacity for name {1} but name doesn't exist.", tx.getTxIdString(), Crypto.hashToString(rnCap.name));
                return false;
            }

            if (rnCap.sequence != rnr.sequence + 1)
            {
                Logging.error("RN Transaction {0} tried to update name {1} but invalid sequence number was used: {2}, expected {3}.", tx.getTxIdString(), Crypto.hashToString(rnCap.name), rnCap.sequence, (rnr.sequence + 1));
                return false;
            }

            if (rnr.allowSubnames)
            {
                Logging.error("RN Transaction {0} tried to change capacity for name {1} but name allows subnames.", tx.getTxIdString(), Crypto.hashToString(rnCap.name));
                return false;
            }

            if (rnCap.newCapacity < ConsensusConfig.rnMinCapacity)
            {
                Logging.error("RN Transaction {0} tried to change capacity for name {1} with invalid capacity.", tx.getTxIdString(), Crypto.hashToString(rnCap.name));
                return false;
            }

            if (rnr.capacity < rnCap.newCapacity)
            {
                var topLevelNames = getTopLevelNames(rnCap.name);
                if (topLevelNames.Count > 0)
                {
                    var topLevelName = topLevelNames.First();
                    if (topLevelName.subnamePrice > 0)
                    {
                        IxiNumber minSubnameFee = RegisteredNamesTransactions.calculateExpectedRegistrationFee((rnr.expirationBlockHeight - IxianHandler.getLastBlockHeight()), rnCap.newCapacity - rnr.capacity, topLevelName.subnamePrice);
                        if (tx.toList.Count < 2
                            || tx.toList.Values.ElementAt(1).amount < minSubnameFee
                            || !tx.toList.Keys.ElementAt(1).addressNoChecksum.SequenceEqual(topLevelName.subnameFeeRecipient.addressNoChecksum))
                        {
                            Logging.error("RN Transaction {0} tried to increase capacity for subname {1} but no fee is paid to recipient.", tx.getTxIdString(), Crypto.hashToString(rnCap.name));
                            return false;
                        }
                    }
                }

                IxiNumber minExpectedFee = RegisteredNamesTransactions.calculateExpectedRegistrationFee((rnr.expirationBlockHeight - IxianHandler.getLastBlockHeight()), rnCap.newCapacity - rnr.capacity, minPricePerUnit);
                if (txOut.amount < minExpectedFee)
                {
                    Logging.error("RN Transaction {0} tried to change capacity for name {1} with too low fee {2} < {3}.", tx.getTxIdString(), Crypto.hashToString(rnCap.name), txOut.amount, minExpectedFee);
                    return false;
                }
            } else
            {
                int totalRecordSize = rnr.getTotalRecordSize();
                if (totalRecordSize > rnCap.newCapacity * 1024)
                {
                    Logging.error("RN Transaction {0} tried to update capacity for name {1} but name records size is too large to reduce capacity {0} > {1}. Remove some records first.", tx.getTxIdString(), Crypto.hashToString(rnCap.name), totalRecordSize, rnCap.newCapacity * 1024);
                    return false;
                }
            }

            if (!rnr.nextPkHash.addressNoChecksum.SequenceEqual(new Address(rnCap.signaturePk).addressNoChecksum))
            {
                Logging.error("RN Transaction {0} tried to update capacity for name {1} but used an invalid signature public key.", tx.getTxIdString(), Crypto.hashToString(rnCap.name));
                return false;
            }

            rnr.setCapacity(rnCap.newCapacity, rnCap.sequence, rnCap.nextPkHash, rnCap.signaturePk, rnCap.signature, 0);
            var rnrChecksum = rnr.calculateChecksum(RegNameRecordByteTypes.forSignature);

            if (!CryptoManager.lib.verifySignature(rnrChecksum, rnCap.signaturePk, rnCap.signature))
            {
                Logging.error("RN Transaction {0} tried to update capacity for name {1} but used an invalid signature.", tx.getTxIdString(), Crypto.hashToString(rnCap.name));
                return false;
            }

            return true;
        }

        private bool verifyToggleAllowSubnamesTransaction(Transaction tx)
        {
            ToEntry txOut = tx.toList.First().Value;
            byte[] txOutData = txOut.data;
            RegNameToggleAllowSubnames rnSub = new RegNameToggleAllowSubnames(txOutData);
            RegisteredNameRecord rnr = getName(rnSub.name);
            if (rnr == null)
            {
                Logging.error("RN Transaction {0} tried to toggle subnames for name {1} but name doesn't exist.", tx.getTxIdString(), Crypto.hashToString(rnSub.name));
                return false;
            }

            if (rnSub.sequence != rnr.sequence + 1)
            {
                Logging.error("RN Transaction {0} tried to update name {1} but invalid sequence number was used: {2}, expected {3}.", tx.getTxIdString(), Crypto.hashToString(rnSub.name), rnSub.sequence, (rnr.sequence + 1));
                return false;
            }

            if (rnr.allowSubnames && !rnSub.allowSubnames)
            {
                Logging.error("RN Transaction {0} tried to toggle subnames for name {1} but name can't be reverted to allow subnames yet.", tx.getTxIdString(), Crypto.hashToString(rnSub.name));
                return false;
            }

            if (!rnr.nextPkHash.addressNoChecksum.SequenceEqual(new Address(rnSub.signaturePk).addressNoChecksum))
            {
                Logging.error("RN Transaction {0} tried to toggle subnames for name {1} but used an invalid signature public key.", tx.getTxIdString(), Crypto.hashToString(rnSub.name));
                return false;
            }

            rnr.setAllowSubnames(rnSub.allowSubnames, rnSub.fee, rnSub.feeRecipientAddress, rnSub.sequence, rnSub.nextPkHash, rnSub.signaturePk, rnSub.signature, 0);
            var rnrChecksum = rnr.calculateChecksum(RegNameRecordByteTypes.forSignature);

            if (!CryptoManager.lib.verifySignature(rnrChecksum, rnSub.signaturePk, rnSub.signature))
            {
                Logging.error("RN Transaction {0} tried to toggle subnames for name {1} but used an invalid signature.", tx.getTxIdString(), Crypto.hashToString(rnSub.name));
                return false;
            }

            return true;
        }

        private bool verifyUpdateRecordTransaction(Transaction tx)
        {
            ToEntry txOut = tx.toList.First().Value;
            byte[] txOutData = txOut.data;
            RegNameUpdateRecord rnUpd = new RegNameUpdateRecord(txOutData);
            RegisteredNameRecord rnr = getName(rnUpd.name);
            if (rnr == null)
            {
                Logging.error("RN Transaction {0} tried to update name {1} but name doesn't exist.", tx.getTxIdString(), Crypto.hashToString(rnUpd.name));
                return false;
            }

            if (rnUpd.sequence != rnr.sequence + 1)
            {
                Logging.error("RN Transaction {0} tried to update name {1} but invalid sequence number was used: {2}, expected {3}.", tx.getTxIdString(), Crypto.hashToString(rnUpd.name), rnUpd.sequence, (rnr.sequence + 1));
                return false;
            }

            if (txOut.amount != 0)
            {
                Logging.error("RN Transaction {0} update record amount is higher than zero.", tx.getTxIdString());
                return false;
            }

            return true;
        }

        private bool verifyRecoverTransaction(Transaction tx)
        {
            ToEntry txOut = tx.toList.First().Value;
            byte[] txOutData = txOut.data;
            RegNameRecover rnRec = new RegNameRecover(txOutData);
            RegisteredNameRecord rnr = getName(rnRec.name);
            if (rnr == null)
            {
                Logging.error("RN Transaction {0} tried to recover name {1} but name doesn't exist.", tx.getTxIdString(), Crypto.hashToString(rnRec.name));
                return false;
            }

            if (rnRec.sequence != rnr.sequence + 1)
            {
                Logging.error("RN Transaction {0} tried to update name {1} but invalid sequence number was used: {2}, expected {3}.", tx.getTxIdString(), Crypto.hashToString(rnRec.name), rnRec.sequence, (rnr.sequence + 1));
                return false;
            }

            if (txOut.amount != 0)
            {
                Logging.error("RN Transaction {0} tried to recover name {1} but amount is higher than zero.", tx.getTxIdString(), Crypto.hashToString(rnRec.name));
                return false;
            }

            if (!rnr.recoveryHash.addressNoChecksum.SequenceEqual(new Address(rnRec.signaturePk).addressNoChecksum))
            {
                Logging.error("RN Transaction {0} tried to recover name {1} but used an invalid signature public key.", tx.getTxIdString(), Crypto.hashToString(rnRec.name));
                return false;
            }

            rnr.setRecoveryHash(rnRec.newRecoveryHash, rnRec.sequence, rnRec.nextPkHash, rnRec.signaturePk, rnRec.signature, 0);
            var rnrChecksum = rnr.calculateChecksum(RegNameRecordByteTypes.forSignature);

            if (!CryptoManager.lib.verifySignature(rnrChecksum, rnRec.signaturePk, rnRec.signature))
            {
                Logging.error("RN Transaction {0} tried to recover name {1} but used an invalid signature.", tx.getTxIdString(), Crypto.hashToString(rnRec.name));
                return false;
            }

            return true;
        }


        private IEnumerable<RegisteredNameRecord> getAlteredRegNamesSinceRNJTX(ulong transactionId)
        {
            if (currentTransaction != null && currentTransaction.journalTxNumber == transactionId)
            {
                RNJTransaction rnjt = (RNJTransaction)currentTransaction;
                return rnjt.getAffectedRegNames();
            }
            else
            {
                RNJTransaction rnjt = (RNJTransaction)processedJournalTransactions.Find(x => x.journalTxNumber == transactionId);
                if (rnjt == null)
                {
                    return null;
                }
                return rnjt.getAffectedRegNames();
            }
        }


        public byte[] calculateRegNameStateDeltaChecksum(ulong transactionId)
        {
            lock (stateLock)
            {
                var alteredNames = getAlteredRegNamesSinceRNJTX(transactionId);
                if (alteredNames == null)
                {
                    Logging.error("Attempted to calculate RN Delta checksum since RNJ transaction {0}, but no such transaction is open.", transactionId);
                    return null;
                }

                List<byte[]> hashes = new();
                foreach (var name in alteredNames)
                {
                    hashes.Add(name.calculateChecksum(RegNameRecordByteTypes.forMerkle));
                }
                var merkleRoot = IxiUtils.calculateMerkleRoot(hashes);
                if (merkleRoot == null)
                {
                    merkleRoot = new byte[64];
                }
                return merkleRoot;

            }
        }

        public byte[] calculateRegNameStateChecksum(ulong blockNum)
        {
            if (blockNum % ConsensusConfig.superblockInterval == 0)
            {
                // Superblock, full state checksum
                return storage.calculateRegNameStateChecksum();
            }

            return calculateRegNameStateDeltaChecksum(blockNum);
        }

        public (bool isApplySuccess, RegNameInstruction instruction) applyTransaction(Transaction tx, ulong curBlockHeight, out ulong expirationBlockHeight)
        {
            Logging.trace("Applying RN Tx: {0}", tx.getTxIdString());

            expirationBlockHeight = 0;
            ToEntry txOut = tx.toList.First().Value;
            byte[] txOutData = txOut.data;
            RegNameInstruction rni = (RegNameInstruction)txOutData[0];
            bool isApplySuccess = false;
            switch (rni)
            {
                case RegNameInstruction.register:
                    RegNameRegister rnReg  = new RegNameRegister(txOutData);
                    isApplySuccess = registerName(rnReg, curBlockHeight, txOut.amount, out expirationBlockHeight);
                    break;
                case RegNameInstruction.extend:
                    RegNameExtend rnExt = new RegNameExtend(txOutData);
                    isApplySuccess = extendName(rnExt, txOut.amount, curBlockHeight, out expirationBlockHeight);
                    break;
                case RegNameInstruction.recover:
                    RegNameRecover rnRec = new RegNameRecover(txOutData);
                    isApplySuccess = recoverName(rnRec, curBlockHeight);
                    break;
                case RegNameInstruction.changeCapacity:
                    RegNameChangeCapacity rnCap = new RegNameChangeCapacity(txOutData);
                    isApplySuccess = updateCapacity(rnCap, txOut.amount, curBlockHeight);
                    break;
                case RegNameInstruction.toggleAllowSubnames:
                    RegNameToggleAllowSubnames rnSub = new RegNameToggleAllowSubnames(txOutData);
                    isApplySuccess = toggleAllowSubname(rnSub, curBlockHeight);
                    break;
                case RegNameInstruction.updateRecord:
                    RegNameUpdateRecord rnUpd;
                    try
                    {
                        rnUpd = new RegNameUpdateRecord(txOutData);
                    }
                    catch (Exception e)
                    {
                        Logging.error("Exception occured while parsing RegName transaciton data: {0}", e);
                        return (false, rni);
                    }
                    isApplySuccess = updateRecords(rnUpd, curBlockHeight);
                    break;
            }
            return (isApplySuccess, rni);
        }
    }
}
