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

using DLT.Meta;
using DLT.RegNames;
using IXICore;
using IXICore.Meta;
using IXICore.RegNames;
using IXICore.Utils;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using static IXICore.Transaction;

namespace UnitTests
{
    [TestClass]
    public class TestNames
    {
        RegisteredNames regNames;
        RegNamesMemoryStorage regNamesMemoryStorage;
        static WalletStorage wallet1;
        static WalletStorage wallet2;
        static WalletStorage wallet3;
        static WalletStorage wallet4;

        [TestInitialize]
        public void Init()
        {
            regNamesMemoryStorage = new RegNamesMemoryStorage("test", Config.saveWalletStateEveryBlock);
            Directory.CreateDirectory("test");
            Directory.CreateDirectory(Path.Combine("test", "0000"));
            regNames = new RegisteredNames(regNamesMemoryStorage);

            IxianHandler.init(new DummyIxianNode(), NetworkType.test, null);
        }

        [TestCleanup]
        public void Cleanup()
        {
        }

        [ClassInitialize]
        public static void ClassInitialize(TestContext tc)
        {
            wallet1 = new WalletStorage("test1.wal");
            wallet1.generateWallet("test");

            wallet2 = new WalletStorage("test2.wal");
            wallet2.generateWallet("test");

            wallet3 = new WalletStorage("test3.wal");
            wallet3.generateWallet("test");

            wallet4 = new WalletStorage("test4.wal");
            wallet4.generateWallet("test");
        }

        [ClassCleanup]
        public static void ClassCleanup()
        {
            wallet1.deleteWallet();
            wallet2.deleteWallet();
            wallet3.deleteWallet();
            wallet4.deleteWallet();
        }

        [TestMethod]
        public void RegisterName_Simple()
        {
            string name = "test";
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = ConsensusConfig.rnMinCapacity;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * (ulong)capacity * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
            Address nextPkHash = wallet1.getPrimaryAddress();
            Address recoveryHash = wallet2.getPrimaryAddress();

            regNames.beginTransaction(1);

            registerName(name, registrationTimeInBlocks, capacity, regFee, nextPkHash, recoveryHash);

            Assert.IsTrue(regNames.revertTransaction(1));

            Assert.AreEqual((ulong)0, regNames.count());
            Assert.AreEqual(0, regNames.getRewardPool());
        }

        [TestMethod]
        public void RegisterName_AlreadyRegistered()
        {
            string name = "test";
            byte[] nameBytes = IxiNameUtils.encodeAndHashIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = ConsensusConfig.rnMinCapacity;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * (ulong)capacity * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
            Address nextPkHash = wallet1.getPrimaryAddress();
            Address recoveryHash = wallet2.getPrimaryAddress();


            Assert.IsTrue(registerName(name, registrationTimeInBlocks, capacity, regFee, nextPkHash, recoveryHash));
            var origRnRecord = regNames.getName(nameBytes);

            regNames.beginTransaction(1);

            ulong curBlockHeight = 1;
            ulong expirationBlockHeight;

            var toEntry = RegisteredNamesTransactions.createRegisterToEntry(nameBytes, registrationTimeInBlocks, capacity, nextPkHash, recoveryHash, regFee);
            Transaction tx = createDummyTransaction(toEntry);
            Assert.IsFalse(regNames.verifyTransaction(tx));
            Assert.IsFalse(regNames.applyTransaction(tx, curBlockHeight, out expirationBlockHeight).isApplySuccess);

            var rnRecord = regNames.getName(nameBytes);
            Assert.IsTrue(nameBytes.SequenceEqual(rnRecord.name), "name != rnRecord.name");
            Assert.AreEqual(registrationTimeInBlocks + curBlockHeight, rnRecord.expirationBlockHeight);
            Assert.AreEqual(capacity, rnRecord.capacity);
            Assert.IsTrue(nextPkHash.SequenceEqual(rnRecord.nextPkHash), "nextPkHash != rnRecord.nextPkHash");
            Assert.IsTrue(recoveryHash.SequenceEqual(rnRecord.recoveryHash), "recoveryHash != rnRecord.recoveryHash");

            Assert.AreEqual(regFee, regNames.getRewardPool());
            Assert.AreEqual(registrationTimeInBlocks + curBlockHeight, expirationBlockHeight);

            Assert.IsTrue(regNames.revertTransaction(1));

            rnRecord = regNames.getName(nameBytes);
            assertRecords(origRnRecord, rnRecord);
            Assert.AreEqual((ulong)1, regNames.count());
            Assert.AreEqual(regFee, regNames.getRewardPool());
        }

        [TestMethod]
        public void RegisterName_FeeTooLow()
        {
            string name = "test";
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = ConsensusConfig.rnMinCapacity;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks) / 2;
            Address nextPkHash = wallet1.getPrimaryAddress();
            Address recoveryHash = wallet2.getPrimaryAddress();

            regNames.beginTransaction(1);

            byte[] nameBytes = IxiNameUtils.encodeAndHashIxiName(name);

            var toEntry = RegisteredNamesTransactions.createRegisterToEntry(nameBytes, registrationTimeInBlocks, capacity, nextPkHash, recoveryHash, regFee);
            Transaction tx = createDummyTransaction(toEntry);
            Assert.IsFalse(regNames.verifyTransaction(tx));

            var rnRecord = regNames.getName(nameBytes);

            Assert.IsTrue(rnRecord == null);
            Assert.AreEqual(0, regNames.getRewardPool());

            Assert.IsTrue(regNames.revertTransaction(1));

            Assert.AreEqual((ulong)0, regNames.count());
            Assert.AreEqual(0, regNames.getRewardPool());
        }

        [TestMethod]
        public void RegisterName_RegistrationTimeTooLow()
        {
            string name = "test";
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks / 2;
            uint capacity = ConsensusConfig.rnMinCapacity;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * (ulong)capacity * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
            Address nextPkHash = wallet1.getPrimaryAddress();
            Address recoveryHash = wallet2.getPrimaryAddress();

            regNames.beginTransaction(1);

            byte[] nameBytes = IxiNameUtils.encodeAndHashIxiName(name);

            var toEntry = RegisteredNamesTransactions.createRegisterToEntry(nameBytes, registrationTimeInBlocks, capacity, nextPkHash, recoveryHash, regFee);
            Transaction tx = createDummyTransaction(toEntry);
            Assert.IsFalse(regNames.verifyTransaction(tx));

            var rnRecord = regNames.getName(nameBytes);

            Assert.IsTrue(rnRecord == null);
            Assert.AreEqual(0, regNames.getRewardPool());

            Assert.IsTrue(regNames.revertTransaction(1));

            Assert.AreEqual((ulong)0, regNames.count());
            Assert.AreEqual(0, regNames.getRewardPool());
        }

        [TestMethod]
        public void RegisterName_RegistrationTimeTooHigh()
        {
            string name = "test";
            uint registrationTimeInBlocks = ConsensusConfig.rnMaxRegistrationTimeInBlocks + 1;
            uint capacity = ConsensusConfig.rnMinCapacity;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * (ulong)capacity * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
            Address nextPkHash = wallet1.getPrimaryAddress();
            Address recoveryHash = wallet2.getPrimaryAddress();

            regNames.beginTransaction(1);

            byte[] nameBytes = IxiNameUtils.encodeAndHashIxiName(name);

            var toEntry = RegisteredNamesTransactions.createRegisterToEntry(nameBytes, registrationTimeInBlocks, capacity, nextPkHash, recoveryHash, regFee);
            Transaction tx = createDummyTransaction(toEntry);
            Assert.IsFalse(regNames.verifyTransaction(tx));

            var rnRecord = regNames.getName(nameBytes);

            Assert.IsTrue(rnRecord == null);
            Assert.AreEqual(0, regNames.getRewardPool());

            Assert.IsTrue(regNames.revertTransaction(1));

            Assert.AreEqual((ulong)0, regNames.count());
            Assert.AreEqual(0, regNames.getRewardPool());
        }

        private bool registerName(string name, uint registrationTimeInBlocks, uint capacity, IxiNumber regFee, Address nextPkHash, Address recoveryHash, IxiNumber expectedRewardPool = null)
        {
            byte[] nameBytes = IxiNameUtils.encodeAndHashIxiName(name);
            return registerName(nameBytes, registrationTimeInBlocks, capacity, regFee, nextPkHash, recoveryHash, expectedRewardPool);
        }

        private bool registerName(byte[] nameBytes, uint registrationTimeInBlocks, uint capacity, IxiNumber regFee, Address nextPkHash, Address recoveryHash, IxiNumber expectedRewardPool = null)
        {
            if (expectedRewardPool == null)
            {
                expectedRewardPool = regFee;
            }

            ulong curBlockHeight = 1;
            ulong expirationBlockHeight;

            var toEntry = RegisteredNamesTransactions.createRegisterToEntry(nameBytes, registrationTimeInBlocks, capacity, nextPkHash, recoveryHash, regFee);
            Transaction tx = createDummyTransaction(toEntry);
            Assert.IsTrue(regNames.verifyTransaction(tx));
            var status = regNames.applyTransaction(tx, curBlockHeight, out expirationBlockHeight);

            var rnRecord = regNames.getName(nameBytes);
            Assert.IsTrue(nameBytes.SequenceEqual(rnRecord.name), "name != rnRecord.name");
            Assert.AreEqual(registrationTimeInBlocks + curBlockHeight, rnRecord.expirationBlockHeight);
            Assert.AreEqual(capacity, rnRecord.capacity);
            Assert.IsTrue(nextPkHash.SequenceEqual(rnRecord.nextPkHash), "nextPkHash != rnRecord.nextPkHash");
            Assert.IsTrue(recoveryHash.SequenceEqual(rnRecord.recoveryHash), "recoveryHash != rnRecord.recoveryHash");

            Assert.AreEqual(expectedRewardPool, regNames.getRewardPool());
            Assert.AreEqual(registrationTimeInBlocks + curBlockHeight, expirationBlockHeight);

            Assert.AreEqual(rnRecord.updatedBlockHeight, curBlockHeight);

            return status.isApplySuccess;
        }

        [TestMethod]
        public void ExtendName()
        {
            string name = "test";
            byte[] nameBytes = IxiNameUtils.encodeAndHashIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = ConsensusConfig.rnMinCapacity;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * (ulong)capacity * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
            Address nextPkHash = wallet1.getPrimaryAddress();
            Address recoveryHash = wallet2.getPrimaryAddress();

            registerName(name, registrationTimeInBlocks, capacity, regFee, nextPkHash, recoveryHash);

            var rnRecord = regNames.getName(nameBytes);
            var origRecord = new RegisteredNameRecord(rnRecord);

            regNames.beginTransaction(1);

            uint extensionTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            ulong expirationBlockHeight;

            ulong lastExpirationTime = rnRecord.expirationBlockHeight;

            var toEntry = RegisteredNamesTransactions.createExtendToEntry(nameBytes, registrationTimeInBlocks, regFee);
            Transaction tx = createDummyTransaction(toEntry);
            Assert.IsTrue(regNames.verifyTransaction(tx));
            Assert.IsTrue(regNames.applyTransaction(tx, 1, out expirationBlockHeight).isApplySuccess);

            rnRecord = regNames.getName(nameBytes);
            Assert.IsTrue(nameBytes.SequenceEqual(rnRecord.name), "rne.name != rnRecord.name");
            Assert.AreEqual(extensionTimeInBlocks + lastExpirationTime, rnRecord.expirationBlockHeight);

            Assert.AreEqual(regFee * 2, regNames.getRewardPool());
            Assert.AreEqual(extensionTimeInBlocks + lastExpirationTime, expirationBlockHeight);

            Assert.IsTrue(regNames.revertTransaction(1));

            rnRecord = regNames.getName(nameBytes);
            Assert.AreEqual((ulong)1, regNames.count());
            Assert.IsTrue(nameBytes.SequenceEqual(rnRecord.name), "rne.name != rnRecord.name");
            Assert.AreEqual(lastExpirationTime, rnRecord.expirationBlockHeight);

            Assert.AreEqual(regFee, regNames.getRewardPool());

            assertRecords(origRecord, rnRecord);
        }

        [TestMethod]
        public void ExtendName_FeeTooLow()
        {
            string name = "test";
            byte[] nameBytes = IxiNameUtils.encodeAndHashIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = ConsensusConfig.rnMinCapacity;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * (ulong)capacity * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
            Address nextPkHash = wallet1.getPrimaryAddress();
            Address recoveryHash = wallet2.getPrimaryAddress();

            registerName(name, registrationTimeInBlocks, capacity, regFee, nextPkHash, recoveryHash);

            var rnRecord = regNames.getName(nameBytes);
            var origRecord = new RegisteredNameRecord(rnRecord);

            regNames.beginTransaction(1);

            uint extensionTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            ulong expirationBlockHeight;

            ulong lastExpirationTime = rnRecord.expirationBlockHeight;

            var toEntry = RegisteredNamesTransactions.createExtendToEntry(nameBytes, registrationTimeInBlocks, regFee / 2);
            Transaction tx = createDummyTransaction(toEntry);
            Assert.IsFalse(regNames.verifyTransaction(tx));
            Assert.IsTrue(regNames.applyTransaction(tx, 1, out expirationBlockHeight).isApplySuccess);

            rnRecord = regNames.getName(nameBytes);
            Assert.IsTrue(nameBytes.SequenceEqual(rnRecord.name), "rne.name != rnRecord.name");
            Assert.AreEqual(extensionTimeInBlocks + lastExpirationTime, rnRecord.expirationBlockHeight);

            Assert.AreEqual(regFee + (regFee / 2), regNames.getRewardPool());
            Assert.AreEqual(extensionTimeInBlocks + lastExpirationTime, expirationBlockHeight);

            Assert.IsTrue(regNames.revertTransaction(1));

            rnRecord = regNames.getName(nameBytes);
            Assert.AreEqual((ulong)1, regNames.count());
            Assert.IsTrue(nameBytes.SequenceEqual(rnRecord.name), "rne.name != rnRecord.name");
            Assert.AreEqual(lastExpirationTime, rnRecord.expirationBlockHeight);

            Assert.AreEqual(regFee, regNames.getRewardPool());

            assertRecords(origRecord, rnRecord);
        }

        [TestMethod]
        public void ExtendName_NameDoesntExist()
        {
            string name = "test";
            byte[] nameBytes = IxiNameUtils.encodeAndHashIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = ConsensusConfig.rnMinCapacity;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * (ulong)capacity * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
            Address nextPkHash = wallet1.getPrimaryAddress();
            Address recoveryHash = wallet2.getPrimaryAddress();

            registerName(name, registrationTimeInBlocks, capacity, regFee, nextPkHash, recoveryHash);

            var rnRecord = regNames.getName(nameBytes);
            var origRecord = new RegisteredNameRecord(rnRecord);

            regNames.beginTransaction(1);

            ulong expirationBlockHeight;

            ulong lastExpirationTime = rnRecord.expirationBlockHeight;

            var nonExistentNameBytes = IxiNameUtils.encodeAndHashIxiName("nonExistentName");
            var toEntry = RegisteredNamesTransactions.createExtendToEntry(nonExistentNameBytes, registrationTimeInBlocks, regFee);
            Transaction tx = createDummyTransaction(toEntry);
            Assert.IsFalse(regNames.verifyTransaction(tx));
            Assert.IsFalse(regNames.applyTransaction(tx, 1, out expirationBlockHeight).isApplySuccess);

            rnRecord = regNames.getName(nonExistentNameBytes);
            Assert.IsNull(rnRecord);

            Assert.AreEqual(regFee, regNames.getRewardPool());

            Assert.IsTrue(regNames.revertTransaction(1));

            rnRecord = regNames.getName(nameBytes);
            Assert.AreEqual((ulong)1, regNames.count());
            Assert.IsTrue(nameBytes.SequenceEqual(rnRecord.name), "rne.name != rnRecord.name");
            Assert.AreEqual(lastExpirationTime, rnRecord.expirationBlockHeight);

            Assert.AreEqual(regFee, regNames.getRewardPool());

            assertRecords(origRecord, rnRecord);
        }

        public void assertRecords(RegisteredNameRecord rec1, RegisteredNameRecord rec2)
        {
            Assert.AreEqual(rec1.version, rec2.version);
            Assert.IsTrue(rec1.name.SequenceEqual(rec2.name));
            Assert.AreEqual(rec1.expirationBlockHeight, rec2.expirationBlockHeight);
            Assert.IsTrue(rec1.subnameFeeRecipient == rec2.subnameFeeRecipient || rec1.subnameFeeRecipient.SequenceEqual(rec2.subnameFeeRecipient));
            Assert.AreEqual(rec1.subnamePrice, rec2.subnamePrice);
            Assert.AreEqual(rec1.allowSubnames, rec2.allowSubnames);
            Assert.AreEqual(rec1.registrationBlockHeight, rec2.registrationBlockHeight);
            Assert.AreEqual(rec1.capacity, rec2.capacity);
            Assert.AreEqual(rec1.dataRecords.LongCount(), rec2.dataRecords.LongCount());
            Assert.IsTrue(rec1.dataMerkleRoot == rec2.dataMerkleRoot || rec1.dataMerkleRoot.SequenceEqual(rec2.dataMerkleRoot));
            Assert.AreEqual(rec1.sequence, rec2.sequence);
            Assert.IsTrue(rec1.nextPkHash.SequenceEqual(rec2.nextPkHash));
            Assert.IsTrue(rec1.recoveryHash.SequenceEqual(rec2.recoveryHash));
            Assert.IsTrue(rec1.signature == rec2.signature || rec1.signature.SequenceEqual(rec2.signature));
            Assert.IsTrue(rec1.signaturePk == rec2.signaturePk || rec1.signaturePk.SequenceEqual(rec2.signaturePk));
            Assert.AreEqual(rec1.updatedBlockHeight, rec2.updatedBlockHeight);
            Assert.IsTrue(rec1.calculateChecksum().SequenceEqual(rec2.calculateChecksum()));
        }

        [TestMethod]
        public void UpdateRecord()
        {
            string name = "test";
            byte[] nameBytes = IxiNameUtils.encodeAndHashIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = ConsensusConfig.rnMinCapacity;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * (ulong)capacity * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
            Address nextPkHash = wallet1.getPrimaryAddress();
            Address recoveryHash = wallet2.getPrimaryAddress();

            registerName(name, registrationTimeInBlocks, capacity, regFee, nextPkHash, recoveryHash);

            var rnRecord = regNames.getName(nameBytes);
            var origRecord = new RegisteredNameRecord(rnRecord);

            regNames.beginTransaction(1);

            nextPkHash = wallet3.getPrimaryAddress();
            Address pkSig = new Address(wallet1.getPrimaryPublicKey());

            byte[] recordName = UTF8Encoding.UTF8.GetBytes("record1");
            byte[] data = RandomUtils.GetBytes(100);
            List<RegisteredNameDataRecord> records = new()
            {
                new RegisteredNameDataRecord(recordName, 1, data)
            };

            var newChecksum = regNames.calculateRegNameChecksumFromUpdatedDataRecords(nameBytes, records, 1, nextPkHash);

            byte[] sig = CryptoManager.lib.getSignature(newChecksum, wallet1.getPrimaryPrivateKey());

            ulong expirationBlockHeight;

            var toEntry = RegisteredNamesTransactions.createUpdateRecordToEntry(nameBytes, records, 1, nextPkHash, pkSig, sig);
            Transaction tx = createDummyTransaction(toEntry);
            Assert.IsTrue(regNames.verifyTransaction(tx));
            Assert.IsTrue(regNames.applyTransaction(tx, 1, out expirationBlockHeight).isApplySuccess);

            rnRecord = regNames.getName(nameBytes);
            Assert.IsTrue(nameBytes.SequenceEqual(rnRecord.name), "rnur.name != rnRecord.name");
            Assert.AreEqual(1, rnRecord.dataRecords.Count);

            Assert.IsTrue(newChecksum.SequenceEqual(rnRecord.calculateChecksum()), "newChecksum != rnRecord.checksum");
            Assert.IsTrue(nextPkHash.SequenceEqual(rnRecord.nextPkHash), "nextPkHash != rnRecord.nextPkHash");
            Assert.IsTrue(pkSig.pubKey.SequenceEqual(rnRecord.signaturePk), "pkSig != rnRecord.signaturePk");
            Assert.IsTrue(sig.SequenceEqual(rnRecord.signature), "sig != rnRecord.signature");

            Assert.IsTrue(regNames.revertTransaction(1));

            rnRecord = regNames.getName(nameBytes);
            Assert.AreEqual((ulong)1, regNames.count());
            Assert.AreEqual(0, rnRecord.dataRecords.Count);

            assertRecords(origRecord, rnRecord);
        }



        [TestMethod]
        public void UpdateRecord_InvalidSequence()
        {
            string name = "test";
            byte[] nameBytes = IxiNameUtils.encodeAndHashIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = ConsensusConfig.rnMinCapacity;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * (ulong)capacity * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
            Address nextPkHash = wallet1.getPrimaryAddress();
            Address recoveryHash = wallet2.getPrimaryAddress();

            registerName(name, registrationTimeInBlocks, capacity, regFee, nextPkHash, recoveryHash);

            var rnRecord = regNames.getName(nameBytes);
            var origRecord = new RegisteredNameRecord(rnRecord);

            regNames.beginTransaction(1);

            nextPkHash = wallet3.getPrimaryAddress();
            Address pkSig = new Address(wallet1.getPrimaryPublicKey());

            byte[] recordName = UTF8Encoding.UTF8.GetBytes("record1");
            byte[] data = RandomUtils.GetBytes(100);
            List<RegisteredNameDataRecord> records = new()
            {
                new RegisteredNameDataRecord(recordName, 1, data)
            };

            var newChecksum = regNames.calculateRegNameChecksumFromUpdatedDataRecords(nameBytes, records, 100, nextPkHash);

            byte[] sig = CryptoManager.lib.getSignature(newChecksum, wallet1.getPrimaryPrivateKey());

            ulong expirationBlockHeight;

            var toEntry = RegisteredNamesTransactions.createUpdateRecordToEntry(nameBytes, records, 100, nextPkHash, pkSig, sig);
            Transaction tx = createDummyTransaction(toEntry);
            Assert.IsFalse(regNames.verifyTransaction(tx));
            Assert.IsFalse(regNames.applyTransaction(tx, 1, out expirationBlockHeight).isApplySuccess);

            rnRecord = regNames.getName(nameBytes);
            Assert.AreEqual((ulong)1, regNames.count());
            Assert.AreEqual(0, rnRecord.dataRecords.Count);

            assertRecords(origRecord, rnRecord);
        }
        [TestMethod]
        public void UpdateRecord_DuplicateRecord1()
        {
            string name = "test";
            byte[] nameBytes = IxiNameUtils.encodeAndHashIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = ConsensusConfig.rnMinCapacity;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * (ulong)capacity * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
            Address nextPkHash1 = wallet1.getPrimaryAddress();
            Address recoveryHash = wallet2.getPrimaryAddress();

            registerName(name, registrationTimeInBlocks, capacity, regFee, nextPkHash1, recoveryHash);

            var rnRecord = regNames.getName(nameBytes);
            var origRecord = new RegisteredNameRecord(rnRecord);

            regNames.beginTransaction(1);

            var nextPkHash2 = wallet3.getPrimaryAddress();
            Address pkSig = new Address(wallet1.getPrimaryPublicKey());

            byte[] recordName = UTF8Encoding.UTF8.GetBytes("record1");
            byte[] data = RandomUtils.GetBytes(100);
            List<RegisteredNameDataRecord> records = new()
            {
                new RegisteredNameDataRecord(recordName, 1, data),
                new RegisteredNameDataRecord(recordName, 1, data)
            };

            var newChecksum = regNames.calculateRegNameChecksumFromUpdatedDataRecords(nameBytes, records, 1, nextPkHash2);

            byte[] sig = CryptoManager.lib.getSignature(newChecksum, wallet1.getPrimaryPrivateKey());

            ulong expirationBlockHeight;

            var toEntry = RegisteredNamesTransactions.createUpdateRecordToEntry(nameBytes, records, 1, nextPkHash2, pkSig, sig);
            Transaction tx = createDummyTransaction(toEntry);
            Assert.IsTrue(regNames.verifyTransaction(tx));
            Assert.IsFalse(regNames.applyTransaction(tx, 1, out expirationBlockHeight).isApplySuccess);

            rnRecord = regNames.getName(nameBytes);
            Assert.IsTrue(nameBytes.SequenceEqual(rnRecord.name), "rnur.name != rnRecord.name");
            Assert.AreEqual(0, rnRecord.dataRecords.Count);

            Assert.IsTrue(nextPkHash1.SequenceEqual(rnRecord.nextPkHash), "nextPkHash1 != rnRecord.nextPkHash");

            Assert.IsTrue(regNames.revertTransaction(1));

            rnRecord = regNames.getName(nameBytes);
            Assert.AreEqual((ulong)1, regNames.count());
            Assert.AreEqual(0, rnRecord.dataRecords.Count);

            assertRecords(origRecord, rnRecord);
        }

        [TestMethod]
        public void UpdateRecord_DuplicateRecord2()
        {
            string name = "test";
            byte[] nameBytes = IxiNameUtils.encodeAndHashIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = ConsensusConfig.rnMinCapacity;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * (ulong)capacity * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
            Address nextPkHash = wallet1.getPrimaryAddress();
            Address recoveryHash = wallet2.getPrimaryAddress();

            registerName(name, registrationTimeInBlocks, capacity, regFee, nextPkHash, recoveryHash);

            var rnRecord = regNames.getName(nameBytes);
            var origRecord = new RegisteredNameRecord(rnRecord);

            regNames.beginTransaction(1);

            var nextPkHash1 = wallet3.getPrimaryAddress();
            Address pkSig1 = new Address(wallet1.getPrimaryPublicKey());

            byte[] recordName1 = UTF8Encoding.UTF8.GetBytes("record1");
            byte[] recordName2 = UTF8Encoding.UTF8.GetBytes("record2");
            byte[] data = RandomUtils.GetBytes(100);
            List<RegisteredNameDataRecord> records = new()
            {
                new RegisteredNameDataRecord(recordName1, 1, data),
                new RegisteredNameDataRecord(recordName2, 1, data)
            };

            var newChecksum1 = regNames.calculateRegNameChecksumFromUpdatedDataRecords(nameBytes, records, 1, nextPkHash1);

            byte[] sig1 = CryptoManager.lib.getSignature(newChecksum1, wallet1.getPrimaryPrivateKey());

            ulong expirationBlockHeight;

            var toEntry = RegisteredNamesTransactions.createUpdateRecordToEntry(nameBytes, records, 1, nextPkHash1, pkSig1, sig1);
            Transaction tx = createDummyTransaction(toEntry);
            Assert.IsTrue(regNames.verifyTransaction(tx));
            Assert.IsTrue(regNames.applyTransaction(tx, 1, out expirationBlockHeight).isApplySuccess);

            rnRecord = regNames.getName(nameBytes);

            regNames.beginTransaction(1);

            var nextPkHash2 = wallet1.getPrimaryAddress();
            var pkSig2 = new Address(wallet3.getPrimaryPublicKey());

            records = new()
            {
                new RegisteredNameDataRecord(recordName1, 1, data)
            };

            var newChecksum2 = regNames.calculateRegNameChecksumFromUpdatedDataRecords(nameBytes, records, 2, nextPkHash2);

            var sig2 = CryptoManager.lib.getSignature(newChecksum2, wallet3.getPrimaryPrivateKey());

            toEntry = RegisteredNamesTransactions.createUpdateRecordToEntry(nameBytes, records, 2, nextPkHash2, pkSig2, sig2);
            tx = createDummyTransaction(toEntry);
            Assert.IsTrue(regNames.verifyTransaction(tx));
            Assert.IsFalse(regNames.applyTransaction(tx, 2, out expirationBlockHeight).isApplySuccess);


            rnRecord = regNames.getName(nameBytes);
            Assert.IsTrue(nameBytes.SequenceEqual(rnRecord.name), "rnur.name != rnRecord.name");
            Assert.AreEqual(2, rnRecord.dataRecords.Count);

            Assert.IsTrue(newChecksum1.SequenceEqual(rnRecord.calculateChecksum()), "newChecksum1 != rnRecord.checksum");
            Assert.IsTrue(nextPkHash1.SequenceEqual(rnRecord.nextPkHash), "nextPkHash1 != rnRecord.nextPkHash");
            Assert.IsTrue(pkSig1.pubKey.SequenceEqual(rnRecord.signaturePk), "pkSig1 != rnRecord.signaturePk");
            Assert.IsTrue(sig1.SequenceEqual(rnRecord.signature), "sig1 != rnRecord.signature");

            Assert.IsTrue(regNames.revertTransaction(1));

            rnRecord = regNames.getName(nameBytes);
            Assert.AreEqual((ulong)1, regNames.count());
            Assert.AreEqual(0, rnRecord.dataRecords.Count);

            assertRecords(origRecord, rnRecord);
        }

        [TestMethod]
        public void UpdateRecord_ReplaceRecord()
        {
            string name = "test";
            byte[] nameBytes = IxiNameUtils.encodeAndHashIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = ConsensusConfig.rnMinCapacity;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * (ulong)capacity * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
            Address nextPkHash = wallet1.getPrimaryAddress();
            Address recoveryHash = wallet2.getPrimaryAddress();

            registerName(name, registrationTimeInBlocks, capacity, regFee, nextPkHash, recoveryHash);

            var rnRecord = regNames.getName(nameBytes);
            var origRecord = new RegisteredNameRecord(rnRecord);

            regNames.beginTransaction(1);

            nextPkHash = wallet3.getPrimaryAddress();
            Address pkSig = new Address(wallet1.getPrimaryPublicKey());

            byte[] recordName1 = UTF8Encoding.UTF8.GetBytes("record1");
            byte[] recordName2 = UTF8Encoding.UTF8.GetBytes("record2");
            byte[] recordName3 = UTF8Encoding.UTF8.GetBytes("record3");
            byte[] data = RandomUtils.GetBytes(100);
            List<RegisteredNameDataRecord> records = new()
            {
                new RegisteredNameDataRecord(recordName1, 1, data),
                new RegisteredNameDataRecord(recordName2, 1, data)
            };

            var newChecksum = regNames.calculateRegNameChecksumFromUpdatedDataRecords(nameBytes, records, 1, nextPkHash);

            byte[] sig = CryptoManager.lib.getSignature(newChecksum, wallet1.getPrimaryPrivateKey());

            ulong expirationBlockHeight;

            var toEntry = RegisteredNamesTransactions.createUpdateRecordToEntry(nameBytes, records, 1, nextPkHash, pkSig, sig);
            Transaction tx = createDummyTransaction(toEntry);
            Assert.IsTrue(regNames.verifyTransaction(tx));
            Assert.IsTrue(regNames.applyTransaction(tx, 1, out expirationBlockHeight).isApplySuccess);

            rnRecord = regNames.getName(nameBytes);

            regNames.beginTransaction(1);

            nextPkHash = wallet1.getPrimaryAddress();
            pkSig = new Address(wallet3.getPrimaryPublicKey());

            records.First().recalculateChecksum();
            var recordChecksumToReplace = records.First().checksum;
            records = new()
            {
                new RegisteredNameDataRecord(recordName3, 1, data, recordChecksumToReplace)
            };

            newChecksum = regNames.calculateRegNameChecksumFromUpdatedDataRecords(nameBytes, records, 2, nextPkHash);

            sig = CryptoManager.lib.getSignature(newChecksum, wallet3.getPrimaryPrivateKey());

            toEntry = RegisteredNamesTransactions.createUpdateRecordToEntry(nameBytes, records, 2, nextPkHash, pkSig, sig);
            tx = createDummyTransaction(toEntry);
            Assert.IsTrue(regNames.verifyTransaction(tx));
            Assert.IsTrue(regNames.applyTransaction(tx, 2, out expirationBlockHeight).isApplySuccess);


            rnRecord = regNames.getName(nameBytes);
            Assert.IsTrue(nameBytes.SequenceEqual(rnRecord.name), "rnur.name != rnRecord.name");
            Assert.AreEqual(2, rnRecord.dataRecords.Count);

            Assert.IsTrue(newChecksum.SequenceEqual(rnRecord.calculateChecksum()), "newChecksum != rnRecord.checksum");
            Assert.IsTrue(nextPkHash.SequenceEqual(rnRecord.nextPkHash), "nextPkHash != rnRecord.nextPkHash");
            Assert.IsTrue(pkSig.pubKey.SequenceEqual(rnRecord.signaturePk), "pkSig != rnRecord.signaturePk");
            Assert.IsTrue(sig.SequenceEqual(rnRecord.signature), "sig != rnRecord.signature");

            Assert.IsTrue(regNames.revertTransaction(1));

            rnRecord = regNames.getName(nameBytes);
            Assert.AreEqual((ulong)1, regNames.count());
            Assert.AreEqual(0, rnRecord.dataRecords.Count);

            assertRecords(origRecord, rnRecord);
        }

        [TestMethod]
        public void UpdateRecord_DeleteRecord()
        {
            string name = "test";
            byte[] nameBytes = IxiNameUtils.encodeAndHashIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = ConsensusConfig.rnMinCapacity;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * (ulong)capacity * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
            Address nextPkHash = wallet1.getPrimaryAddress();
            Address recoveryHash = wallet2.getPrimaryAddress();

            registerName(name, registrationTimeInBlocks, capacity, regFee, nextPkHash, recoveryHash);

            var rnRecord = regNames.getName(nameBytes);
            var origRecord = new RegisteredNameRecord(rnRecord);

            regNames.beginTransaction(1);

            nextPkHash = wallet3.getPrimaryAddress();
            Address pkSig = new Address(wallet1.getPrimaryPublicKey());

            byte[] recordName1 = UTF8Encoding.UTF8.GetBytes("record1");
            byte[] recordName2 = UTF8Encoding.UTF8.GetBytes("record2");
            byte[] data = RandomUtils.GetBytes(100);
            List<RegisteredNameDataRecord> records = new()
            {
                new RegisteredNameDataRecord(recordName1, 1, data),
                new RegisteredNameDataRecord(recordName2, 1, data)
            };

            var newChecksum = regNames.calculateRegNameChecksumFromUpdatedDataRecords(nameBytes, records, 1, nextPkHash);

            byte[] sig = CryptoManager.lib.getSignature(newChecksum, wallet1.getPrimaryPrivateKey());

            ulong expirationBlockHeight;

            var toEntry = RegisteredNamesTransactions.createUpdateRecordToEntry(nameBytes, records, 1, nextPkHash, pkSig, sig);
            Transaction tx = createDummyTransaction(toEntry);
            Assert.IsTrue(regNames.verifyTransaction(tx));
            Assert.IsTrue(regNames.applyTransaction(tx, 1, out expirationBlockHeight).isApplySuccess);

            rnRecord = regNames.getName(nameBytes);

            regNames.beginTransaction(1);

            nextPkHash = wallet1.getPrimaryAddress();
            pkSig = new Address(wallet3.getPrimaryPublicKey());

            records.First().recalculateChecksum();
            var recordChecksumToRemove = records.First().checksum;
            records = new()
            {
                new RegisteredNameDataRecord(recordName1, 1, null, recordChecksumToRemove)
            };

            newChecksum = regNames.calculateRegNameChecksumFromUpdatedDataRecords(nameBytes, records, 2, nextPkHash);

            sig = CryptoManager.lib.getSignature(newChecksum, wallet3.getPrimaryPrivateKey());

            toEntry = RegisteredNamesTransactions.createUpdateRecordToEntry(nameBytes, records, 2, nextPkHash, pkSig, sig);
            tx = createDummyTransaction(toEntry);
            Assert.IsTrue(regNames.verifyTransaction(tx));
            Assert.IsTrue(regNames.applyTransaction(tx, 1, out expirationBlockHeight).isApplySuccess);


            rnRecord = regNames.getName(nameBytes);
            Assert.IsTrue(nameBytes.SequenceEqual(rnRecord.name), "rnur.name != rnRecord.name");
            Assert.AreEqual(1, rnRecord.dataRecords.Count);

            Assert.IsTrue(newChecksum.SequenceEqual(rnRecord.calculateChecksum()), "newChecksum != rnRecord.checksum");
            Assert.IsTrue(nextPkHash.SequenceEqual(rnRecord.nextPkHash), "nextPkHash != rnRecord.nextPkHash");
            Assert.IsTrue(pkSig.pubKey.SequenceEqual(rnRecord.signaturePk), "pkSig != rnRecord.signaturePk");
            Assert.IsTrue(sig.SequenceEqual(rnRecord.signature), "sig != rnRecord.signature");

            Assert.IsTrue(regNames.revertTransaction(1));

            rnRecord = regNames.getName(nameBytes);
            Assert.AreEqual((ulong)1, regNames.count());
            Assert.AreEqual(0, rnRecord.dataRecords.Count);

            assertRecords(origRecord, rnRecord);
        }

        [TestMethod]
        public void UpdateRecord_TooLarge()
        {
            string name = "test";
            byte[] nameBytes = IxiNameUtils.encodeAndHashIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = ConsensusConfig.rnMinCapacity;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * (ulong)capacity * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
            Address nextPkHash = wallet1.getPrimaryAddress();
            Address recoveryHash = wallet2.getPrimaryAddress();

            registerName(name, registrationTimeInBlocks, capacity, regFee, nextPkHash, recoveryHash);

            var rnRecord = regNames.getName(nameBytes);
            var origRecord = new RegisteredNameRecord(rnRecord);

            regNames.beginTransaction(1);

            nextPkHash = wallet3.getPrimaryAddress();
            Address pkSig = new Address(wallet1.getPrimaryPublicKey());

            byte[] recordName = UTF8Encoding.UTF8.GetBytes("record1");
            byte[] data = RandomUtils.GetBytes(20000);
            List<RegisteredNameDataRecord> records = new()
            {
                new RegisteredNameDataRecord(recordName, 1, data)
            };

            var newChecksum = regNames.calculateRegNameChecksumFromUpdatedDataRecords(nameBytes, records, 1, nextPkHash);

            byte[] sig = CryptoManager.lib.getSignature(newChecksum, wallet1.getPrimaryPrivateKey());

            ulong expirationBlockHeight;

            var toEntry = RegisteredNamesTransactions.createUpdateRecordToEntry(nameBytes, records, 1, nextPkHash, pkSig, sig);
            Transaction tx = createDummyTransaction(toEntry);
            Assert.IsTrue(regNames.verifyTransaction(tx));
            Assert.IsFalse(regNames.applyTransaction(tx, 1, out expirationBlockHeight).isApplySuccess);

            rnRecord = regNames.getName(nameBytes);
            Assert.IsTrue(nameBytes.SequenceEqual(rnRecord.name), "rnur.name != rnRecord.name");
            Assert.AreEqual(0, rnRecord.dataRecords.Count);

            assertRecords(origRecord, rnRecord);

            Assert.IsTrue(regNames.revertTransaction(1));

            rnRecord = regNames.getName(nameBytes);
            Assert.AreEqual((ulong)1, regNames.count());
            Assert.AreEqual(0, rnRecord.dataRecords.Count);

            assertRecords(origRecord, rnRecord);
        }

        [TestMethod]
        public void UpdateRecord_IncorrectKey()
        {
            string name = "test";
            byte[] nameBytes = IxiNameUtils.encodeAndHashIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = ConsensusConfig.rnMinCapacity;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * (ulong)capacity * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
            Address nextPkHash = wallet1.getPrimaryAddress();
            Address recoveryHash = wallet2.getPrimaryAddress();

            registerName(name, registrationTimeInBlocks, capacity, regFee, nextPkHash, recoveryHash);

            var rnRecord = regNames.getName(nameBytes);
            var origRecord = new RegisteredNameRecord(rnRecord);

            regNames.beginTransaction(1);

            Address pkSig = new Address(wallet2.getPrimaryPublicKey());

            byte[] recordName = UTF8Encoding.UTF8.GetBytes("record1");
            byte[] data = RandomUtils.GetBytes(100);
            List<RegisteredNameDataRecord> records = new()
            {
                new RegisteredNameDataRecord(recordName, 1, data)
            };

            var newChecksum = regNames.calculateRegNameChecksumFromUpdatedDataRecords(nameBytes, records, 1, nextPkHash);

            byte[] sig = CryptoManager.lib.getSignature(newChecksum, wallet2.getPrimaryPrivateKey());

            ulong expirationBlockHeight;

            var toEntry = RegisteredNamesTransactions.createUpdateRecordToEntry(nameBytes, records, 1, nextPkHash, pkSig, sig);
            Transaction tx = createDummyTransaction(toEntry);
            Assert.IsTrue(regNames.verifyTransaction(tx));
            Assert.IsFalse(regNames.applyTransaction(tx, 1, out expirationBlockHeight).isApplySuccess);

            rnRecord = regNames.getName(nameBytes);
            Assert.IsTrue(nameBytes.SequenceEqual(rnRecord.name), "rnur.name != rnRecord.name");
            Assert.AreEqual(0, rnRecord.dataRecords.Count);

            assertRecords(origRecord, rnRecord);

            Assert.IsTrue(regNames.revertTransaction(1));

            rnRecord = regNames.getName(nameBytes);
            Assert.AreEqual((ulong)1, regNames.count());
            Assert.AreEqual(0, rnRecord.dataRecords.Count);

            assertRecords(origRecord, rnRecord);
        }

        [TestMethod]
        public void UpdateRecord_IncorrectSig()
        {
            string name = "test";
            byte[] nameBytes = IxiNameUtils.encodeAndHashIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = ConsensusConfig.rnMinCapacity;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * (ulong)capacity * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
            Address nextPkHash = wallet1.getPrimaryAddress();
            Address recoveryHash = wallet2.getPrimaryAddress();

            registerName(name, registrationTimeInBlocks, capacity, regFee, nextPkHash, recoveryHash);

            var rnRecord = regNames.getName(nameBytes);
            var origRecord = new RegisteredNameRecord(rnRecord);

            regNames.beginTransaction(1);

            Address pkSig = new Address(wallet1.getPrimaryPublicKey());

            byte[] recordName = UTF8Encoding.UTF8.GetBytes("record1");
            byte[] data = RandomUtils.GetBytes(100);
            List<RegisteredNameDataRecord> records = new()
            {
                new RegisteredNameDataRecord(recordName, 1, data)
            };

            var newChecksum = regNames.calculateRegNameChecksumFromUpdatedDataRecords(nameBytes, records, 1, nextPkHash);

            byte[] sig = CryptoManager.lib.getSignature(newChecksum, wallet2.getPrimaryPrivateKey());

            ulong expirationBlockHeight;

            var toEntry = RegisteredNamesTransactions.createUpdateRecordToEntry(nameBytes, records, 1, nextPkHash, pkSig, sig);
            Transaction tx = createDummyTransaction(toEntry);
            Assert.IsTrue(regNames.verifyTransaction(tx));
            Assert.IsFalse(regNames.applyTransaction(tx, 1, out expirationBlockHeight).isApplySuccess);

            rnRecord = regNames.getName(nameBytes);
            Assert.IsTrue(nameBytes.SequenceEqual(rnRecord.name), "rnur.name != rnRecord.name");
            Assert.AreEqual(0, rnRecord.dataRecords.Count);

            assertRecords(origRecord, rnRecord);

            Assert.IsTrue(regNames.revertTransaction(1));

            rnRecord = regNames.getName(nameBytes);
            Assert.AreEqual((ulong)1, regNames.count());
            Assert.AreEqual(0, rnRecord.dataRecords.Count);

            assertRecords(origRecord, rnRecord);
        }

        [TestMethod]
        public void IncreaseCapacity()
        {
            string name = "test";
            byte[] nameBytes = IxiNameUtils.encodeAndHashIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = ConsensusConfig.rnMinCapacity;
            uint newCapacity = ConsensusConfig.rnMinCapacity * 2;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * (ulong)capacity * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
            Address nextPkHash = wallet1.getPrimaryAddress();
            Address recoveryHash = wallet2.getPrimaryAddress();

            registerName(name, registrationTimeInBlocks, capacity, regFee, nextPkHash, recoveryHash);

            var rnRecord = regNames.getName(nameBytes);
            var origRecord = new RegisteredNameRecord(rnRecord);

            regNames.beginTransaction(1);

            nextPkHash = wallet3.getPrimaryAddress();
            Address pkSig = new Address(wallet1.getPrimaryPublicKey());

            rnRecord.setCapacity(newCapacity, 1, nextPkHash, pkSig.pubKey, null, 0);
            var newChecksum = rnRecord.calculateChecksum();

            byte[] sig = CryptoManager.lib.getSignature(newChecksum, wallet1.getPrimaryPrivateKey());

            ulong expirationBlockHeight;

            var toEntry = RegisteredNamesTransactions.createChangeCapacityToEntry(nameBytes, newCapacity, 1, nextPkHash, pkSig, sig, regFee);
            Transaction tx = createDummyTransaction(toEntry);
            Assert.IsTrue(regNames.verifyTransaction(tx));
            Assert.IsTrue(regNames.applyTransaction(tx, 234, out expirationBlockHeight).isApplySuccess);

            rnRecord = regNames.getName(nameBytes);
            Assert.IsTrue(nameBytes.SequenceEqual(rnRecord.name), "rnc.name != rnRecord.name");

            Assert.AreEqual(newCapacity, rnRecord.capacity);

            Assert.IsTrue(newChecksum.SequenceEqual(rnRecord.calculateChecksum()), "newChecksum != rnRecord.checksum");
            Assert.IsTrue(nextPkHash.SequenceEqual(rnRecord.nextPkHash), "nextPkHash != rnRecord.nextPkHash");
            Assert.IsTrue(pkSig.pubKey.SequenceEqual(rnRecord.signaturePk), "pkSig != rnRecord.signaturePk");
            Assert.IsTrue(sig.SequenceEqual(rnRecord.signature), "sig != rnRecord.signature");

            Assert.AreEqual(regFee * 2, regNames.getRewardPool());

            Assert.AreEqual(234uL, rnRecord.updatedBlockHeight);

            Assert.IsTrue(regNames.revertTransaction(1));

            rnRecord = regNames.getName(nameBytes);
            Assert.AreEqual((ulong)1, regNames.count());
            Assert.AreEqual(0, rnRecord.dataRecords.Count);

            Assert.AreEqual(regFee, regNames.getRewardPool());

            assertRecords(origRecord, rnRecord);
        }

        [TestMethod]
        public void IncreaseCapacity_IncorrectKey()
        {
            string name = "test";
            byte[] nameBytes = IxiNameUtils.encodeAndHashIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = ConsensusConfig.rnMinCapacity;
            uint newCapacity = ConsensusConfig.rnMinCapacity * 2;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * (ulong)capacity * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
            Address nextPkHash = wallet1.getPrimaryAddress();
            Address recoveryHash = wallet2.getPrimaryAddress();

            registerName(name, registrationTimeInBlocks, capacity, regFee, nextPkHash, recoveryHash);

            var rnRecord = regNames.getName(nameBytes);
            var origRecord = new RegisteredNameRecord(rnRecord);

            regNames.beginTransaction(1);

            nextPkHash = wallet3.getPrimaryAddress();
            Address pkSig = new Address(wallet2.getPrimaryPublicKey());

            rnRecord.setCapacity(newCapacity, 1, nextPkHash, pkSig.pubKey, null, 0);
            var newChecksum = rnRecord.calculateChecksum();

            byte[] sig = CryptoManager.lib.getSignature(newChecksum, wallet2.getPrimaryPrivateKey());

            ulong expirationBlockHeight;

            var toEntry = RegisteredNamesTransactions.createChangeCapacityToEntry(nameBytes, newCapacity, 1, nextPkHash, pkSig, sig, regFee);
            Transaction tx = createDummyTransaction(toEntry);
            Assert.IsFalse(regNames.verifyTransaction(tx));
            Assert.IsFalse(regNames.applyTransaction(tx, 235, out expirationBlockHeight).isApplySuccess);

            rnRecord = regNames.getName(nameBytes);
            Assert.AreEqual(regFee, regNames.getRewardPool());

            assertRecords(origRecord, rnRecord);

            Assert.AreEqual(1uL, rnRecord.updatedBlockHeight);

            Assert.IsTrue(regNames.revertTransaction(1));

            rnRecord = regNames.getName(nameBytes);
            Assert.AreEqual((ulong)1, regNames.count());
            Assert.AreEqual(0, rnRecord.dataRecords.Count);

            Assert.AreEqual(regFee, regNames.getRewardPool());

            assertRecords(origRecord, rnRecord);
        }

        [TestMethod]
        public void IncreaseCapacity_IncorrectSig()
        {
            string name = "test";
            byte[] nameBytes = IxiNameUtils.encodeAndHashIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = ConsensusConfig.rnMinCapacity;
            uint newCapacity = ConsensusConfig.rnMinCapacity * 2;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * (ulong)capacity * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
            Address nextPkHash = wallet1.getPrimaryAddress();
            Address recoveryHash = wallet2.getPrimaryAddress();

            registerName(name, registrationTimeInBlocks, capacity, regFee, nextPkHash, recoveryHash);

            var rnRecord = regNames.getName(nameBytes);
            var origRecord = new RegisteredNameRecord(rnRecord);

            regNames.beginTransaction(1);

            nextPkHash = wallet3.getPrimaryAddress();
            Address pkSig = new Address(wallet1.getPrimaryPublicKey());

            rnRecord.setCapacity(newCapacity, 1, nextPkHash, pkSig.pubKey, null, 0);
            var newChecksum = rnRecord.calculateChecksum();

            byte[] sig = CryptoManager.lib.getSignature(newChecksum, wallet2.getPrimaryPrivateKey());

            ulong expirationBlockHeight;

            var toEntry = RegisteredNamesTransactions.createChangeCapacityToEntry(nameBytes, newCapacity, 1, nextPkHash, pkSig, sig, regFee);
            Transaction tx = createDummyTransaction(toEntry);
            Assert.IsFalse(regNames.verifyTransaction(tx));
            Assert.IsFalse(regNames.applyTransaction(tx, 1, out expirationBlockHeight).isApplySuccess);

            rnRecord = regNames.getName(nameBytes);
            Assert.AreEqual(regFee, regNames.getRewardPool());

            assertRecords(origRecord, rnRecord);

            Assert.AreEqual(1uL, rnRecord.updatedBlockHeight);

            Assert.IsTrue(regNames.revertTransaction(1));

            rnRecord = regNames.getName(nameBytes);
            Assert.AreEqual((ulong)1, regNames.count());
            Assert.AreEqual(0, rnRecord.dataRecords.Count);

            Assert.AreEqual(regFee, regNames.getRewardPool());

            assertRecords(origRecord, rnRecord);
        }

        [TestMethod]
        public void IncreaseCapacity_FeeTooLow()
        {
            string name = "test";
            byte[] nameBytes = IxiNameUtils.encodeAndHashIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = ConsensusConfig.rnMinCapacity;
            uint newCapacity = ConsensusConfig.rnMinCapacity * 2;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * (ulong)capacity * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
            Address nextPkHash = wallet1.getPrimaryAddress();
            Address recoveryHash = wallet2.getPrimaryAddress();

            registerName(name, registrationTimeInBlocks, capacity, regFee, nextPkHash, recoveryHash);

            var rnRecord = regNames.getName(nameBytes);
            var origRecord = new RegisteredNameRecord(rnRecord);

            regNames.beginTransaction(1);

            nextPkHash = wallet3.getPrimaryAddress();
            Address pkSig = new Address(wallet1.getPrimaryPublicKey());

            rnRecord.setCapacity(newCapacity, 1, nextPkHash, pkSig.pubKey, null, 0);
            var newChecksum = rnRecord.calculateChecksum();

            byte[] sig = CryptoManager.lib.getSignature(newChecksum, wallet1.getPrimaryPrivateKey());

            ulong expirationBlockHeight;

            var toEntry = RegisteredNamesTransactions.createChangeCapacityToEntry(nameBytes, newCapacity, 1, nextPkHash, pkSig, sig, regFee / 2);
            Transaction tx = createDummyTransaction(toEntry);
            Assert.IsFalse(regNames.verifyTransaction(tx));
            Assert.IsTrue(regNames.applyTransaction(tx, 235, out expirationBlockHeight).isApplySuccess);

            rnRecord = regNames.getName(nameBytes);
            Assert.IsTrue(nameBytes.SequenceEqual(rnRecord.name), "rnc.name != rnRecord.name");

            Assert.AreEqual(newCapacity, rnRecord.capacity);

            Assert.IsTrue(newChecksum.SequenceEqual(rnRecord.calculateChecksum()), "newChecksum != rnRecord.checksum");
            Assert.IsTrue(nextPkHash.SequenceEqual(rnRecord.nextPkHash), "nextPkHash != rnRecord.nextPkHash");
            Assert.IsTrue(pkSig.pubKey.SequenceEqual(rnRecord.signaturePk), "pkSig != rnRecord.signaturePk");
            Assert.IsTrue(sig.SequenceEqual(rnRecord.signature), "sig != rnRecord.signature");

            Assert.AreEqual(regFee + (regFee / 2), regNames.getRewardPool());

            Assert.AreEqual(235uL, rnRecord.updatedBlockHeight);

            Assert.IsTrue(regNames.revertTransaction(1));

            rnRecord = regNames.getName(nameBytes);
            Assert.AreEqual((ulong)1, regNames.count());
            Assert.AreEqual(0, rnRecord.dataRecords.Count);

            Assert.AreEqual(regFee, regNames.getRewardPool());

            assertRecords(origRecord, rnRecord);
        }

        [TestMethod]
        public void DecreaseCapacity()
        {
            string name = "test";
            byte[] nameBytes = IxiNameUtils.encodeAndHashIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = ConsensusConfig.rnMinCapacity * 2;
            uint newCapacity = ConsensusConfig.rnMinCapacity;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * (ulong)capacity * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks) * 2;
            Address nextPkHash = wallet1.getPrimaryAddress();
            Address recoveryHash = wallet2.getPrimaryAddress();

            registerName(name, registrationTimeInBlocks, capacity, regFee, nextPkHash, recoveryHash);

            var rnRecord = regNames.getName(nameBytes);
            var origRecord = new RegisteredNameRecord(rnRecord);

            regNames.beginTransaction(1);

            nextPkHash = wallet3.getPrimaryAddress();
            Address pkSig = new Address(wallet1.getPrimaryPublicKey());

            rnRecord.setCapacity(newCapacity, 1, nextPkHash, pkSig.pubKey, null, 0);
            var newChecksum = rnRecord.calculateChecksum();

            byte[] sig = CryptoManager.lib.getSignature(newChecksum, wallet1.getPrimaryPrivateKey());

            ulong expirationBlockHeight;

            var toEntry = RegisteredNamesTransactions.createChangeCapacityToEntry(nameBytes, newCapacity, 1, nextPkHash, pkSig, sig, 0);
            Transaction tx = createDummyTransaction(toEntry);
            Assert.IsTrue(regNames.verifyTransaction(tx));
            Assert.IsTrue(regNames.applyTransaction(tx, 234, out expirationBlockHeight).isApplySuccess);

            rnRecord = regNames.getName(nameBytes);
            Assert.IsTrue(nameBytes.SequenceEqual(rnRecord.name), "rnc.name != rnRecord.name");

            Assert.AreEqual(newCapacity, rnRecord.capacity);

            Assert.IsTrue(newChecksum.SequenceEqual(rnRecord.calculateChecksum()), "newChecksum != rnRecord.checksum");
            Assert.IsTrue(nextPkHash.SequenceEqual(rnRecord.nextPkHash), "nextPkHash != rnRecord.nextPkHash");
            Assert.IsTrue(pkSig.pubKey.SequenceEqual(rnRecord.signaturePk), "pkSig != rnRecord.signaturePk");
            Assert.IsTrue(sig.SequenceEqual(rnRecord.signature), "sig != rnRecord.signature");

            Assert.AreEqual(regFee, regNames.getRewardPool());

            Assert.AreEqual(234uL, rnRecord.updatedBlockHeight);

            Assert.IsTrue(regNames.revertTransaction(1));

            rnRecord = regNames.getName(nameBytes);
            Assert.AreEqual((ulong)1, regNames.count());
            Assert.AreEqual(0, rnRecord.dataRecords.Count);

            Assert.AreEqual(regFee, regNames.getRewardPool());

            assertRecords(origRecord, rnRecord);
        }

        [TestMethod]
        public void DecreaseCapacity_RecordsTooLarge()
        {
            string name = "test";
            byte[] nameBytes = IxiNameUtils.encodeAndHashIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = ConsensusConfig.rnMinCapacity * 2;
            uint newCapacity = ConsensusConfig.rnMinCapacity;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * (ulong)capacity * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks) * 2;
            Address nextPkHash = wallet1.getPrimaryAddress();
            Address recoveryHash = wallet2.getPrimaryAddress();

            Assert.IsTrue(registerName(name, registrationTimeInBlocks, capacity, regFee, nextPkHash, recoveryHash));

            nextPkHash = wallet3.getPrimaryAddress();
            Address pkSig = new Address(wallet1.getPrimaryPublicKey());

            // Set Records
            byte[] recordName = UTF8Encoding.UTF8.GetBytes("record1");
            byte[] data = RandomUtils.GetBytes(1015);
            List<RegisteredNameDataRecord> records = new()
            {
                new RegisteredNameDataRecord(recordName, 1, data)
            };

            var newChecksum = regNames.calculateRegNameChecksumFromUpdatedDataRecords(nameBytes, records, 1, nextPkHash);

            byte[] sig = CryptoManager.lib.getSignature(newChecksum, wallet1.getPrimaryPrivateKey());

            var rnur = new RegNameUpdateRecord(nameBytes, records, 1, nextPkHash, pkSig, sig);
            Assert.IsTrue(regNames.updateRecords(rnur, 234));

            var rnRecord = regNames.getName(nameBytes);
            var origRecord = new RegisteredNameRecord(rnRecord);

            regNames.beginTransaction(1);

            // Set Capacity
            rnRecord.setCapacity(newCapacity, 2, wallet1.getPrimaryAddress(), new Address(wallet3.getPrimaryPublicKey()).pubKey, sig, 235);
            var newCapacityChecksum = rnRecord.calculateChecksum();

            var capacitySig = CryptoManager.lib.getSignature(newCapacityChecksum, wallet3.getPrimaryPrivateKey());

            ulong expirationBlockHeight;

            var toEntry = RegisteredNamesTransactions.createChangeCapacityToEntry(nameBytes, newCapacity, 2, wallet1.getPrimaryAddress(), pkSig, sig, 0);
            Transaction tx = createDummyTransaction(toEntry);
            Assert.IsFalse(regNames.verifyTransaction(tx));
            Assert.IsFalse(regNames.applyTransaction(tx, 1, out expirationBlockHeight).isApplySuccess);

            rnRecord = regNames.getName(nameBytes);
            Assert.IsTrue(nameBytes.SequenceEqual(rnRecord.name), "rnc.name != rnRecord.name");

            Assert.AreEqual(capacity, rnRecord.capacity);

            Assert.IsTrue(newChecksum.SequenceEqual(rnRecord.calculateChecksum()), "newChecksum != rnRecord.checksum");
            Assert.IsTrue(nextPkHash.SequenceEqual(rnRecord.nextPkHash), "nextPkHash != rnRecord.nextPkHash");
            Assert.IsTrue(pkSig.pubKey.SequenceEqual(rnRecord.signaturePk), "pkSig != rnRecord.signaturePk");
            Assert.IsTrue(sig.SequenceEqual(rnRecord.signature), "sig != rnRecord.signature");

            Assert.AreEqual(regFee, regNames.getRewardPool());

            Assert.AreEqual(234uL, rnRecord.updatedBlockHeight);

            Assert.IsTrue(regNames.revertTransaction(1));

            rnRecord = regNames.getName(nameBytes);
            Assert.AreEqual((ulong)1, regNames.count());
            Assert.AreEqual(1, rnRecord.dataRecords.Count);

            Assert.AreEqual(regFee, regNames.getRewardPool());

            assertRecords(origRecord, rnRecord);
        }


        [TestMethod]
        public void RecoverName()
        {
            string name = "test";
            byte[] nameBytes = IxiNameUtils.encodeAndHashIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = ConsensusConfig.rnMinCapacity;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * (ulong)capacity * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
            Address nextPkHash = wallet1.getPrimaryAddress();
            Address recoveryHash = wallet2.getPrimaryAddress();

            registerName(name, registrationTimeInBlocks, capacity, regFee, nextPkHash, recoveryHash);

            var rnRecord = regNames.getName(nameBytes);
            var origRecord = new RegisteredNameRecord(rnRecord);

            regNames.beginTransaction(1);

            nextPkHash = wallet3.getPrimaryAddress();
            Address pkSig = new Address(wallet2.getPrimaryPublicKey());
            Address newRecoveryHash = wallet4.getPrimaryAddress();

            rnRecord.setRecoveryHash(newRecoveryHash, 1, nextPkHash, pkSig.pubKey, null, 0);
            var newChecksum = rnRecord.calculateChecksum();

            byte[] sig = CryptoManager.lib.getSignature(newChecksum, wallet2.getPrimaryPrivateKey());

            ulong expirationBlockHeight;

            var toEntry = RegisteredNamesTransactions.createRecoverToEntry(nameBytes, 1, nextPkHash, newRecoveryHash, pkSig, sig);
            Transaction tx = createDummyTransaction(toEntry);
            Assert.IsTrue(regNames.verifyTransaction(tx));
            Assert.IsTrue(regNames.applyTransaction(tx, 234, out expirationBlockHeight).isApplySuccess);

            rnRecord = regNames.getName(nameBytes);
            Assert.IsTrue(nameBytes.SequenceEqual(rnRecord.name), "rnRec.name != rnRecord.name");

            Assert.IsTrue(newRecoveryHash.SequenceEqual(rnRecord.recoveryHash), "newRecoveryHash != rnRecord.recoveryHash");

            Assert.IsTrue(newChecksum.SequenceEqual(rnRecord.calculateChecksum()), "newChecksum != rnRecord.checksum");
            Assert.IsTrue(nextPkHash.SequenceEqual(rnRecord.nextPkHash), "nextPkHash != rnRecord.nextPkHash");
            Assert.IsTrue(pkSig.pubKey.SequenceEqual(rnRecord.signaturePk), "pkSig != rnRecord.signaturePk");
            Assert.IsTrue(sig.SequenceEqual(rnRecord.signature), "sig != rnRecord.signature");

            Assert.AreEqual(234uL, rnRecord.updatedBlockHeight);

            Assert.IsTrue(regNames.revertTransaction(1));

            rnRecord = regNames.getName(nameBytes);
            Assert.AreEqual((ulong)1, regNames.count());

            assertRecords(origRecord, rnRecord);
        }

        [TestMethod]
        public void RecoverName_IncorrectKey()
        {
            string name = "test";
            byte[] nameBytes = IxiNameUtils.encodeAndHashIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = ConsensusConfig.rnMinCapacity;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * (ulong)capacity * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
            Address nextPkHash = wallet1.getPrimaryAddress();
            Address recoveryHash = wallet2.getPrimaryAddress();

            registerName(name, registrationTimeInBlocks, capacity, regFee, nextPkHash, recoveryHash);

            var rnRecord = regNames.getName(nameBytes);
            var origRecord = new RegisteredNameRecord(rnRecord);

            regNames.beginTransaction(1);

            nextPkHash = wallet3.getPrimaryAddress();
            Address pkSig = new Address(wallet1.getPrimaryPublicKey());
            Address newRecoveryHash = wallet4.getPrimaryAddress();

            rnRecord.setRecoveryHash(newRecoveryHash, 1, nextPkHash, pkSig.pubKey, null, 0);
            var newChecksum = rnRecord.calculateChecksum();

            byte[] sig = CryptoManager.lib.getSignature(newChecksum, wallet2.getPrimaryPrivateKey());

            ulong expirationBlockHeight;

            var toEntry = RegisteredNamesTransactions.createRecoverToEntry(nameBytes, 1, nextPkHash, newRecoveryHash, pkSig, sig);
            Transaction tx = createDummyTransaction(toEntry);
            Assert.IsFalse(regNames.verifyTransaction(tx));
            Assert.IsFalse(regNames.applyTransaction(tx, 1, out expirationBlockHeight).isApplySuccess);

            rnRecord = regNames.getName(nameBytes);

            assertRecords(origRecord, rnRecord);

            Assert.IsTrue(regNames.revertTransaction(1));

            rnRecord = regNames.getName(nameBytes);
            Assert.AreEqual((ulong)1, regNames.count());

            assertRecords(origRecord, rnRecord);
        }

        [TestMethod]
        public void RecoverName_IncorrectSig()
        {
            string name = "test";
            byte[] nameBytes = IxiNameUtils.encodeAndHashIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = ConsensusConfig.rnMinCapacity;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * (ulong)capacity * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
            Address nextPkHash = wallet1.getPrimaryAddress();
            Address recoveryHash = wallet2.getPrimaryAddress();

            registerName(name, registrationTimeInBlocks, capacity, regFee, nextPkHash, recoveryHash);

            var rnRecord = regNames.getName(nameBytes);
            var origRecord = new RegisteredNameRecord(rnRecord);

            regNames.beginTransaction(1);

            nextPkHash = wallet3.getPrimaryAddress();
            Address pkSig = new Address(wallet2.getPrimaryPublicKey());
            Address newRecoveryHash = wallet4.getPrimaryAddress();

            rnRecord.setRecoveryHash(newRecoveryHash, 1, nextPkHash, pkSig.pubKey, null, 0);
            var newChecksum = rnRecord.calculateChecksum();

            byte[] sig = CryptoManager.lib.getSignature(newChecksum, wallet1.getPrimaryPrivateKey());

            ulong expirationBlockHeight;

            var toEntry = RegisteredNamesTransactions.createRecoverToEntry(nameBytes, 1, nextPkHash, newRecoveryHash, pkSig, sig);
            Transaction tx = createDummyTransaction(toEntry);
            Assert.IsFalse(regNames.verifyTransaction(tx));
            Assert.IsFalse(regNames.applyTransaction(tx, 1, out expirationBlockHeight).isApplySuccess);

            rnRecord = regNames.getName(nameBytes);

            assertRecords(origRecord, rnRecord);

            Assert.IsTrue(regNames.revertTransaction(1));

            rnRecord = regNames.getName(nameBytes);
            Assert.AreEqual((ulong)1, regNames.count());

            assertRecords(origRecord, rnRecord);
        }

        private Transaction createDummyTransaction(ToEntry toEntry)
        {
            Transaction t = new Transaction((int)Transaction.Type.RegName);
            t.toList = new Dictionary<Address, ToEntry>(new AddressComparer())
            {
                { ConsensusConfig.rnRewardPoolAddress, toEntry }
            };
            t.id = UTF8Encoding.UTF8.GetBytes("test");
            return t;
        }

        [TestMethod]
        public void Subname_Top_Enable()
        {
            string name = "test";
            byte[] nameBytes = IxiNameUtils.encodeAndHashIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = ConsensusConfig.rnMinCapacity;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * (ulong)capacity * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
            Address nextPkHash = wallet1.getPrimaryAddress();
            Address recoveryHash = wallet2.getPrimaryAddress();
            IxiNumber subnameFee = 100000;
            Address subnameFeeRecipient = wallet4.getPrimaryAddress();

            registerName(name, registrationTimeInBlocks, capacity, regFee, nextPkHash, recoveryHash);

            var rnRecord = regNames.getName(nameBytes);
            var origRecord = new RegisteredNameRecord(rnRecord);

            regNames.beginTransaction(1);

            nextPkHash = wallet3.getPrimaryAddress();
            allowSubnames(nameBytes, true, subnameFee, subnameFeeRecipient, 1, nextPkHash, wallet1);

            Assert.IsTrue(regNames.revertTransaction(1));

            rnRecord = regNames.getName(nameBytes);
            Assert.AreEqual((ulong)1, regNames.count());
            Assert.AreEqual(0, rnRecord.dataRecords.Count);

            assertRecords(origRecord, rnRecord);
        }

        [TestMethod]
        public void Subname_Top_AddRecord()
        {
            string name = "test";
            byte[] nameBytes = IxiNameUtils.encodeAndHashIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = ConsensusConfig.rnMinCapacity;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * (ulong)capacity * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
            Address nextPkHash = wallet1.getPrimaryAddress();
            Address recoveryHash = wallet2.getPrimaryAddress();
            IxiNumber subnameFee = 100000;
            Address subnameFeeRecipient = wallet4.getPrimaryAddress();

            registerName(name, registrationTimeInBlocks, capacity, regFee, nextPkHash, recoveryHash);

            nextPkHash = wallet3.getPrimaryAddress();
            allowSubnames(nameBytes, true, subnameFee, subnameFeeRecipient, 1, nextPkHash, wallet1);

            var rnRecord = regNames.getName(nameBytes);
            var origRecord = new RegisteredNameRecord(rnRecord);

            regNames.beginTransaction(1);

            nextPkHash = wallet4.getPrimaryAddress();

            byte[] recordName = UTF8Encoding.UTF8.GetBytes("@");
            byte[] data = RandomUtils.GetBytes(100);
            List<RegisteredNameDataRecord> records = new()
            {
                new RegisteredNameDataRecord(recordName, 1, data)
            };

            var newChecksum = regNames.calculateRegNameChecksumFromUpdatedDataRecords(nameBytes, records, 2, nextPkHash);

            Address pkSig = new Address(wallet3.getPrimaryPublicKey());
            byte[] sig = CryptoManager.lib.getSignature(newChecksum, wallet3.getPrimaryPrivateKey());

            ulong expirationBlockHeight;

            var toEntry = RegisteredNamesTransactions.createUpdateRecordToEntry(nameBytes, records, 2, nextPkHash, pkSig, sig);
            Transaction tx = createDummyTransaction(toEntry);
            Assert.IsTrue(regNames.verifyTransaction(tx));
            Assert.IsTrue(regNames.applyTransaction(tx, 1, out expirationBlockHeight).isApplySuccess);

            rnRecord = regNames.getName(nameBytes);
            Assert.IsTrue(nameBytes.SequenceEqual(rnRecord.name), "rnur.name != rnRecord.name");
            Assert.AreEqual(1, rnRecord.dataRecords.Count);

            Assert.IsTrue(newChecksum.SequenceEqual(rnRecord.calculateChecksum()), "newChecksum != rnRecord.checksum");
            Assert.IsTrue(nextPkHash.SequenceEqual(rnRecord.nextPkHash), "nextPkHash != rnRecord.nextPkHash");
            Assert.IsTrue(pkSig.pubKey.SequenceEqual(rnRecord.signaturePk), "pkSig != rnRecord.signaturePk");
            Assert.IsTrue(sig.SequenceEqual(rnRecord.signature), "sig != rnRecord.signature");

            Assert.IsTrue(regNames.revertTransaction(1));

            rnRecord = regNames.getName(nameBytes);
            Assert.AreEqual((ulong)1, regNames.count());
            Assert.AreEqual(0, rnRecord.dataRecords.Count);

            assertRecords(origRecord, rnRecord);
        }

        [TestMethod]
        public void Subname_Top_AddRecord_InvalidKey()
        {
            string name = "test";
            byte[] nameBytes = IxiNameUtils.encodeAndHashIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = ConsensusConfig.rnMinCapacity;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * (ulong)capacity * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
            Address nextPkHash = wallet1.getPrimaryAddress();
            Address recoveryHash = wallet2.getPrimaryAddress();
            IxiNumber subnameFee = 100000;
            Address subnameFeeRecipient = wallet4.getPrimaryAddress();

            registerName(name, registrationTimeInBlocks, capacity, regFee, nextPkHash, recoveryHash);

            nextPkHash = wallet3.getPrimaryAddress();
            allowSubnames(nameBytes, true, subnameFee, subnameFeeRecipient, 1, nextPkHash, wallet1);

            var rnRecord = regNames.getName(nameBytes);
            var origRecord = new RegisteredNameRecord(rnRecord);

            regNames.beginTransaction(1);

            nextPkHash = wallet4.getPrimaryAddress();

            byte[] recordName = UTF8Encoding.UTF8.GetBytes("test");
            byte[] data = RandomUtils.GetBytes(100);
            List<RegisteredNameDataRecord> records = new()
            {
                new RegisteredNameDataRecord(recordName, 1, data)
            };

            var newChecksum = regNames.calculateRegNameChecksumFromUpdatedDataRecords(nameBytes, records, 2, nextPkHash);

            Address pkSig = new Address(wallet3.getPrimaryPublicKey());
            byte[] sig = CryptoManager.lib.getSignature(newChecksum, wallet3.getPrimaryPrivateKey());

            ulong expirationBlockHeight;

            var toEntry = RegisteredNamesTransactions.createUpdateRecordToEntry(nameBytes, records, 2, nextPkHash, pkSig, sig);
            Transaction tx = createDummyTransaction(toEntry);
            Assert.IsTrue(regNames.verifyTransaction(tx));
            Assert.IsFalse(regNames.applyTransaction(tx, 1, out expirationBlockHeight).isApplySuccess);

            rnRecord = regNames.getName(nameBytes);
            Assert.IsTrue(nameBytes.SequenceEqual(rnRecord.name), "rnur.name != rnRecord.name");
            Assert.AreEqual(0, rnRecord.dataRecords.Count);

            assertRecords(origRecord, rnRecord);

            Assert.IsTrue(regNames.revertTransaction(1));

            rnRecord = regNames.getName(nameBytes);
            Assert.AreEqual((ulong)1, regNames.count());
            Assert.AreEqual(0, rnRecord.dataRecords.Count);

            assertRecords(origRecord, rnRecord);
        }

        [TestMethod]
        public void Subname_Enable_Top_IncorrectKey()
        {
            string name = "test";
            byte[] nameBytes = IxiNameUtils.encodeAndHashIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = ConsensusConfig.rnMinCapacity;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * (ulong)capacity * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
            Address nextPkHash = wallet1.getPrimaryAddress();
            Address recoveryHash = wallet2.getPrimaryAddress();
            IxiNumber subnameFee = 100000;
            Address subnameFeeRecipient = wallet4.getPrimaryAddress();

            registerName(name, registrationTimeInBlocks, capacity, regFee, nextPkHash, recoveryHash);

            var rnRecord = regNames.getName(nameBytes);
            var origRecord = new RegisteredNameRecord(rnRecord);

            regNames.beginTransaction(1);

            nextPkHash = wallet3.getPrimaryAddress();
            Address pkSig = new Address(wallet4.getPrimaryPublicKey());

            rnRecord.setAllowSubnames(true, subnameFee, subnameFeeRecipient, 1, nextPkHash, pkSig.pubKey, null, 0);
            var newChecksum = rnRecord.calculateChecksum();

            byte[] sig = CryptoManager.lib.getSignature(newChecksum, wallet1.getPrimaryPrivateKey());

            ulong expirationBlockHeight;

            var toEntry = RegisteredNamesTransactions.createToggleAllowSubnamesToEntry(nameBytes, true, subnameFee, subnameFeeRecipient, 1, nextPkHash, pkSig, sig);
            Transaction tx = createDummyTransaction(toEntry);
            Assert.IsFalse(regNames.verifyTransaction(tx));
            Assert.IsFalse(regNames.applyTransaction(tx, 1, out expirationBlockHeight).isApplySuccess);

            rnRecord = regNames.getName(nameBytes);
            assertRecords(origRecord, rnRecord);

            Assert.IsTrue(regNames.revertTransaction(1));

            rnRecord = regNames.getName(nameBytes);
            Assert.AreEqual((ulong)1, regNames.count());
            Assert.AreEqual(0, rnRecord.dataRecords.Count);

            assertRecords(origRecord, rnRecord);
        }

        [TestMethod]
        public void Subname_Enable_Top_IncorrectSig()
        {
            string name = "test";
            byte[] nameBytes = IxiNameUtils.encodeAndHashIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = ConsensusConfig.rnMinCapacity;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * (ulong)capacity * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
            Address nextPkHash = wallet1.getPrimaryAddress();
            Address recoveryHash = wallet2.getPrimaryAddress();
            IxiNumber subnameFee = 100000;
            Address subnameFeeRecipient = wallet4.getPrimaryAddress();

            registerName(name, registrationTimeInBlocks, capacity, regFee, nextPkHash, recoveryHash);

            var rnRecord = regNames.getName(nameBytes);
            var origRecord = new RegisteredNameRecord(rnRecord);

            regNames.beginTransaction(1);

            nextPkHash = wallet3.getPrimaryAddress();
            Address pkSig = new Address(wallet1.getPrimaryPublicKey());

            rnRecord.setAllowSubnames(true, subnameFee, subnameFeeRecipient, 1, nextPkHash, pkSig.pubKey, null, 0);
            var newChecksum = rnRecord.calculateChecksum();

            byte[] sig = CryptoManager.lib.getSignature(newChecksum, wallet4.getPrimaryPrivateKey());

            ulong expirationBlockHeight;

            var toEntry = RegisteredNamesTransactions.createToggleAllowSubnamesToEntry(nameBytes, true, subnameFee, subnameFeeRecipient, 1, nextPkHash, pkSig, sig);
            Transaction tx = createDummyTransaction(toEntry);
            Assert.IsFalse(regNames.verifyTransaction(tx));
            Assert.IsFalse(regNames.applyTransaction(tx, 1, out expirationBlockHeight).isApplySuccess);

            rnRecord = regNames.getName(nameBytes);
            assertRecords(origRecord, rnRecord);

            Assert.IsTrue(regNames.revertTransaction(1));

            rnRecord = regNames.getName(nameBytes);
            Assert.AreEqual((ulong)1, regNames.count());
            Assert.AreEqual(0, rnRecord.dataRecords.Count);

            assertRecords(origRecord, rnRecord);
        }

        private void allowSubnames(byte[] nameBytes, bool allowSubnames, IxiNumber subnameFee, Address subnameFeeRecipient, ulong sequence, Address nextPkHash, WalletStorage signingWallet)
        {
            var pkSig = signingWallet.getPrimaryPublicKey();
            var rnRecord = regNames.getName(nameBytes);
            rnRecord.setAllowSubnames(allowSubnames, subnameFee, subnameFeeRecipient, sequence, nextPkHash, pkSig, null, 0);
            var newChecksum = rnRecord.calculateChecksum();

            byte[] sig = CryptoManager.lib.getSignature(newChecksum, signingWallet.getPrimaryPrivateKey());

            ulong expirationBlockHeight;

            var toEntry = RegisteredNamesTransactions.createToggleAllowSubnamesToEntry(nameBytes, allowSubnames, subnameFee, subnameFeeRecipient, sequence, nextPkHash, new Address(pkSig), sig);
            Transaction tx = createDummyTransaction(toEntry);
            Assert.IsTrue(regNames.verifyTransaction(tx));
            Assert.IsTrue(regNames.applyTransaction(tx, 234, out expirationBlockHeight).isApplySuccess);

            rnRecord = regNames.getName(nameBytes);
            Assert.IsTrue(nameBytes.SequenceEqual(rnRecord.name), "nameBytes != rnRecord.name");
            Assert.AreEqual(0, rnRecord.dataRecords.Count);
            Assert.AreEqual(true, rnRecord.allowSubnames);
            Assert.AreEqual(subnameFee, rnRecord.subnamePrice);
            Assert.IsTrue(subnameFeeRecipient.SequenceEqual(rnRecord.subnameFeeRecipient), "subnameFeeRecipient != rnRecord.subnameFeeRecipient");

            Assert.IsTrue(newChecksum.SequenceEqual(rnRecord.calculateChecksum()), "newChecksum != rnRecord.checksum");
            Assert.IsTrue(nextPkHash.SequenceEqual(rnRecord.nextPkHash), "nextPkHash != rnRecord.nextPkHash");
            Assert.IsTrue(pkSig.SequenceEqual(rnRecord.signaturePk), "pkSig != rnRecord.signaturePk");
            Assert.IsTrue(sig.SequenceEqual(rnRecord.signature), "sig != rnRecord.signature");

            Assert.AreEqual(234uL, rnRecord.updatedBlockHeight);
        }

        [TestMethod]
        public void Subname_Register()
        {
            string name = "test";
            byte[] nameBytes = IxiNameUtils.encodeAndHashIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = ConsensusConfig.rnMinCapacity;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * (ulong)capacity * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
            Address nextPkHash = wallet1.getPrimaryAddress();
            Address recoveryHash = wallet2.getPrimaryAddress();
            IxiNumber subnamePrice = 100000;
            Address subnameFeeRecipient = wallet4.getPrimaryAddress();

            Assert.IsTrue(registerName(name, registrationTimeInBlocks, capacity, regFee, nextPkHash, recoveryHash));

            nextPkHash = wallet3.getPrimaryAddress();
            allowSubnames(nameBytes, true, subnamePrice, subnameFeeRecipient, 1, nextPkHash, wallet1);

            Address subnameNextPkHash = wallet4.getPrimaryAddress();
            Address subnameRecoveryHash = wallet1.getPrimaryAddress();

            var rnRecord = regNames.getName(nameBytes);
            var origRecord = new RegisteredNameRecord(rnRecord);

            regNames.beginTransaction(1);

            // Register subname
            byte[] subnameBytes = IxiNameUtils.encodeAndHashIxiName("subNameTest." + name);
            IxiNumber subnameFee = subnamePrice * (ulong)capacity * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);

            ulong curBlockHeight = 1;
            ulong expirationBlockHeight;

            var toEntry = RegisteredNamesTransactions.createRegisterToEntry(subnameBytes, registrationTimeInBlocks, capacity, subnameNextPkHash, subnameRecoveryHash, regFee);
            Transaction tx = createDummyTransaction(toEntry);
            tx.toList.Add(subnameFeeRecipient, new ToEntry(Transaction.maxVersion, subnameFee));
            Assert.IsTrue(regNames.verifyTransaction(tx));
            Assert.IsTrue(regNames.applyTransaction(tx, curBlockHeight, out expirationBlockHeight).isApplySuccess);

            rnRecord = regNames.getName(subnameBytes);
            Assert.IsTrue(subnameBytes.SequenceEqual(rnRecord.name), "name != rnRecord.name");
            Assert.AreEqual(registrationTimeInBlocks + curBlockHeight, rnRecord.expirationBlockHeight);
            Assert.AreEqual(capacity, rnRecord.capacity);
            Assert.IsTrue(subnameNextPkHash.SequenceEqual(rnRecord.nextPkHash), "nextPkHash != rnRecord.nextPkHash");
            Assert.IsTrue(subnameRecoveryHash.SequenceEqual(rnRecord.recoveryHash), "recoveryHash != rnRecord.recoveryHash");

            Assert.AreEqual(regFee * 2, regNames.getRewardPool());
            Assert.AreEqual(registrationTimeInBlocks + curBlockHeight, expirationBlockHeight);

            rnRecord = regNames.getName(nameBytes);
            assertRecords(origRecord, rnRecord);

            Assert.IsTrue(regNames.revertTransaction(1));

            rnRecord = regNames.getName(nameBytes);
            Assert.AreEqual((ulong)1, regNames.count());
            Assert.AreEqual(0, rnRecord.dataRecords.Count);

            assertRecords(origRecord, rnRecord);
        }

        [TestMethod]
        public void Subname_Extend()
        {
            string name = "test";
            byte[] nameBytes = IxiNameUtils.encodeAndHashIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = ConsensusConfig.rnMinCapacity;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * (ulong)capacity * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
            Address nextPkHash = wallet1.getPrimaryAddress();
            Address recoveryHash = wallet2.getPrimaryAddress();
            IxiNumber subnamePrice = 100000;
            Address subnameFeeRecipient = wallet4.getPrimaryAddress();

            Assert.IsTrue(registerName(name, registrationTimeInBlocks, capacity, regFee, nextPkHash, recoveryHash));

            nextPkHash = wallet3.getPrimaryAddress();
            allowSubnames(nameBytes, true, subnamePrice, subnameFeeRecipient, 1, nextPkHash, wallet1);

            Address subnameNextPkHash = wallet4.getPrimaryAddress();
            Address subnameRecoveryHash = wallet1.getPrimaryAddress();

            var rnRecord = regNames.getName(nameBytes);
            var origRecord = new RegisteredNameRecord(rnRecord);


            // Register subname
            byte[] subnameBytes = IxiNameUtils.encodeAndHashIxiName("subNameTest." + name);
            IxiNumber subnameFee = subnamePrice * (ulong)capacity * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);

            ulong curBlockHeight = 1;
            ulong expirationBlockHeight;

            var toEntry = RegisteredNamesTransactions.createRegisterToEntry(subnameBytes, registrationTimeInBlocks, capacity, subnameNextPkHash, subnameRecoveryHash, regFee);
            Transaction tx = createDummyTransaction(toEntry);
            tx.toList.Add(subnameFeeRecipient, new ToEntry(Transaction.maxVersion, subnameFee));
            Assert.IsTrue(regNames.verifyTransaction(tx));
            Assert.IsTrue(regNames.applyTransaction(tx, curBlockHeight, out expirationBlockHeight).isApplySuccess);

            rnRecord = regNames.getName(subnameBytes);
            var origSubnameRecord = new RegisteredNameRecord(rnRecord);

            regNames.beginTransaction(1);

            toEntry = RegisteredNamesTransactions.createExtendToEntry(subnameBytes, registrationTimeInBlocks, regFee);
            tx = createDummyTransaction(toEntry);
            tx.toList.Add(subnameFeeRecipient, new ToEntry(Transaction.maxVersion, subnameFee));
            Assert.IsTrue(regNames.verifyTransaction(tx));
            Assert.IsTrue(regNames.applyTransaction(tx, curBlockHeight, out expirationBlockHeight).isApplySuccess);

            rnRecord = regNames.getName(subnameBytes);
            Assert.IsTrue(subnameBytes.SequenceEqual(rnRecord.name), "name != rnRecord.name");
            Assert.AreEqual((registrationTimeInBlocks * 2) + curBlockHeight, rnRecord.expirationBlockHeight);
            Assert.AreEqual(capacity, rnRecord.capacity);
            Assert.IsTrue(subnameNextPkHash.SequenceEqual(rnRecord.nextPkHash), "nextPkHash != rnRecord.nextPkHash");
            Assert.IsTrue(subnameRecoveryHash.SequenceEqual(rnRecord.recoveryHash), "recoveryHash != rnRecord.recoveryHash");

            Assert.AreEqual(regFee * 3, regNames.getRewardPool());
            Assert.AreEqual((registrationTimeInBlocks * 2) + curBlockHeight, expirationBlockHeight);

            rnRecord = regNames.getName(nameBytes);
            Assert.AreEqual(rnRecord.version, rnRecord.version);
            Assert.IsTrue(rnRecord.name.SequenceEqual(rnRecord.name));
            Assert.AreEqual((registrationTimeInBlocks * 2) + curBlockHeight, rnRecord.expirationBlockHeight);
            Assert.IsTrue(rnRecord.subnameFeeRecipient == rnRecord.subnameFeeRecipient || rnRecord.subnameFeeRecipient.SequenceEqual(rnRecord.subnameFeeRecipient));
            Assert.AreEqual(rnRecord.subnamePrice, rnRecord.subnamePrice);
            Assert.AreEqual(rnRecord.allowSubnames, rnRecord.allowSubnames);
            Assert.AreEqual(rnRecord.capacity, rnRecord.capacity);
            Assert.AreEqual(rnRecord.dataRecords.LongCount(), rnRecord.dataRecords.LongCount());
            Assert.IsTrue(rnRecord.dataMerkleRoot == rnRecord.dataMerkleRoot || rnRecord.dataMerkleRoot.SequenceEqual(rnRecord.dataMerkleRoot));
            Assert.IsTrue(rnRecord.nextPkHash.SequenceEqual(rnRecord.nextPkHash));
            Assert.IsTrue(rnRecord.recoveryHash.SequenceEqual(rnRecord.recoveryHash));
            Assert.IsTrue(rnRecord.signature == rnRecord.signature || rnRecord.signature.SequenceEqual(rnRecord.signature));
            Assert.IsTrue(rnRecord.signaturePk == rnRecord.signaturePk || rnRecord.signaturePk.SequenceEqual(rnRecord.signaturePk));
            Assert.AreEqual(rnRecord.updatedBlockHeight, rnRecord.updatedBlockHeight);
            Assert.IsTrue(rnRecord.calculateChecksum().SequenceEqual(rnRecord.calculateChecksum()));

            Assert.IsTrue(regNames.revertTransaction(1));

            Assert.AreEqual((ulong)2, regNames.count());

            rnRecord = regNames.getName(nameBytes);
            Assert.AreEqual(0, rnRecord.dataRecords.Count);
            assertRecords(origRecord, rnRecord);

            rnRecord = regNames.getName(subnameBytes);
            assertRecords(origSubnameRecord, rnRecord);
        }

        [TestMethod]
        public void Subname_Extend_SubnameDoesntExist()
        {
            string name = "test";
            byte[] nameBytes = IxiNameUtils.encodeAndHashIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = ConsensusConfig.rnMinCapacity;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * (ulong)capacity * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
            Address nextPkHash = wallet1.getPrimaryAddress();
            Address recoveryHash = wallet2.getPrimaryAddress();
            IxiNumber subnamePrice = 100000;
            Address subnameFeeRecipient = wallet4.getPrimaryAddress();

            Assert.IsTrue(registerName(name, registrationTimeInBlocks, capacity, regFee, nextPkHash, recoveryHash));

            nextPkHash = wallet3.getPrimaryAddress();
            allowSubnames(nameBytes, true, subnamePrice, subnameFeeRecipient, 1, nextPkHash, wallet1);

            Address subnameNextPkHash = wallet4.getPrimaryAddress();
            Address subnameRecoveryHash = wallet1.getPrimaryAddress();

            var rnRecord = regNames.getName(nameBytes);
            var origRecord = new RegisteredNameRecord(rnRecord);


            // Register subname
            byte[] subnameBytes = IxiNameUtils.encodeAndHashIxiName("subNameTest." + name);
            IxiNumber subnameFee = subnamePrice * (ulong)capacity * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);

            ulong curBlockHeight = 1;
            ulong expirationBlockHeight;

            var toEntry = RegisteredNamesTransactions.createRegisterToEntry(subnameBytes, registrationTimeInBlocks, capacity, subnameNextPkHash, subnameRecoveryHash, regFee);
            Transaction tx = createDummyTransaction(toEntry);
            tx.toList.Add(subnameFeeRecipient, new ToEntry(Transaction.maxVersion, subnameFee));
            Assert.IsTrue(regNames.verifyTransaction(tx));
            Assert.IsTrue(regNames.applyTransaction(tx, curBlockHeight, out expirationBlockHeight).isApplySuccess);

            rnRecord = regNames.getName(subnameBytes);
            var origSubnameRecord = new RegisteredNameRecord(rnRecord);

            regNames.beginTransaction(1);

            var nonExistentNameBytes = IxiNameUtils.encodeAndHashIxiName("nonExistentName." + name);
            toEntry = RegisteredNamesTransactions.createExtendToEntry(nonExistentNameBytes, registrationTimeInBlocks, regFee);
            tx = createDummyTransaction(toEntry);
            tx.toList.Add(subnameFeeRecipient, new ToEntry(Transaction.maxVersion, subnameFee));
            Assert.IsFalse(regNames.verifyTransaction(tx));
            Assert.IsFalse(regNames.applyTransaction(tx, curBlockHeight, out expirationBlockHeight).isApplySuccess);

            rnRecord = regNames.getName(nonExistentNameBytes);
            Assert.IsNull(rnRecord);

            Assert.AreEqual(regFee * 2, regNames.getRewardPool());

            Assert.IsTrue(regNames.revertTransaction(1));

            Assert.AreEqual((ulong)2, regNames.count());

            rnRecord = regNames.getName(nameBytes);
            Assert.AreEqual(0, rnRecord.dataRecords.Count);
            assertRecords(origRecord, rnRecord);

            rnRecord = regNames.getName(subnameBytes);
            assertRecords(origSubnameRecord, rnRecord);
        }

        [TestMethod]
        public void Subname_Extend_NoTopTransactionFee()
        {
            string name = "test";
            byte[] nameBytes = IxiNameUtils.encodeAndHashIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = ConsensusConfig.rnMinCapacity;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * (ulong)capacity * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
            Address nextPkHash = wallet1.getPrimaryAddress();
            Address recoveryHash = wallet2.getPrimaryAddress();
            IxiNumber subnamePrice = 100000;
            Address subnameFeeRecipient = wallet4.getPrimaryAddress();

            Assert.IsTrue(registerName(name, registrationTimeInBlocks, capacity, regFee, nextPkHash, recoveryHash));

            nextPkHash = wallet3.getPrimaryAddress();
            allowSubnames(nameBytes, true, subnamePrice, subnameFeeRecipient, 1, nextPkHash, wallet1);

            Address subnameNextPkHash = wallet4.getPrimaryAddress();
            Address subnameRecoveryHash = wallet1.getPrimaryAddress();

            var rnRecord = regNames.getName(nameBytes);
            var origRecord = new RegisteredNameRecord(rnRecord);


            // Register subname
            byte[] subnameBytes = IxiNameUtils.encodeAndHashIxiName("subNameTest." + name);
            IxiNumber subnameFee = subnamePrice * (ulong)capacity * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);

            ulong curBlockHeight = 1;
            ulong expirationBlockHeight;

            var toEntry = RegisteredNamesTransactions.createRegisterToEntry(subnameBytes, registrationTimeInBlocks, capacity, subnameNextPkHash, subnameRecoveryHash, regFee);
            Transaction tx = createDummyTransaction(toEntry);
            tx.toList.Add(subnameFeeRecipient, new ToEntry(Transaction.maxVersion, subnameFee));
            Assert.IsTrue(regNames.verifyTransaction(tx));
            Assert.IsTrue(regNames.applyTransaction(tx, curBlockHeight, out expirationBlockHeight).isApplySuccess);

            rnRecord = regNames.getName(subnameBytes);
            var origSubnameRecord = new RegisteredNameRecord(rnRecord);

            regNames.beginTransaction(1);

            toEntry = RegisteredNamesTransactions.createExtendToEntry(subnameBytes, registrationTimeInBlocks, regFee);
            tx = createDummyTransaction(toEntry);
            tx.toList.Add(nextPkHash, new ToEntry(Transaction.maxVersion, subnameFee));
            Assert.IsFalse(regNames.verifyTransaction(tx));
            Assert.IsTrue(regNames.applyTransaction(tx, curBlockHeight, out expirationBlockHeight).isApplySuccess);

            rnRecord = regNames.getName(subnameBytes);
            Assert.IsTrue(subnameBytes.SequenceEqual(rnRecord.name), "name != rnRecord.name");
            Assert.AreEqual((registrationTimeInBlocks * 2) + curBlockHeight, rnRecord.expirationBlockHeight);
            Assert.AreEqual(capacity, rnRecord.capacity);
            Assert.IsTrue(subnameNextPkHash.SequenceEqual(rnRecord.nextPkHash), "nextPkHash != rnRecord.nextPkHash");
            Assert.IsTrue(subnameRecoveryHash.SequenceEqual(rnRecord.recoveryHash), "recoveryHash != rnRecord.recoveryHash");

            Assert.AreEqual(regFee * 3, regNames.getRewardPool());
            Assert.AreEqual((registrationTimeInBlocks * 2) + curBlockHeight, expirationBlockHeight);

            rnRecord = regNames.getName(nameBytes);
            Assert.AreEqual(rnRecord.version, rnRecord.version);
            Assert.IsTrue(rnRecord.name.SequenceEqual(rnRecord.name));
            Assert.AreEqual((registrationTimeInBlocks * 2) + curBlockHeight, rnRecord.expirationBlockHeight);
            Assert.IsTrue(rnRecord.subnameFeeRecipient == rnRecord.subnameFeeRecipient || rnRecord.subnameFeeRecipient.SequenceEqual(rnRecord.subnameFeeRecipient));
            Assert.AreEqual(rnRecord.subnamePrice, rnRecord.subnamePrice);
            Assert.AreEqual(rnRecord.allowSubnames, rnRecord.allowSubnames);
            Assert.AreEqual(rnRecord.capacity, rnRecord.capacity);
            Assert.AreEqual(rnRecord.dataRecords.LongCount(), rnRecord.dataRecords.LongCount());
            Assert.IsTrue(rnRecord.dataMerkleRoot == rnRecord.dataMerkleRoot || rnRecord.dataMerkleRoot.SequenceEqual(rnRecord.dataMerkleRoot));
            Assert.IsTrue(rnRecord.nextPkHash.SequenceEqual(rnRecord.nextPkHash));
            Assert.IsTrue(rnRecord.recoveryHash.SequenceEqual(rnRecord.recoveryHash));
            Assert.IsTrue(rnRecord.signature == rnRecord.signature || rnRecord.signature.SequenceEqual(rnRecord.signature));
            Assert.IsTrue(rnRecord.signaturePk == rnRecord.signaturePk || rnRecord.signaturePk.SequenceEqual(rnRecord.signaturePk));
            Assert.AreEqual(rnRecord.updatedBlockHeight, rnRecord.updatedBlockHeight);
            Assert.IsTrue(rnRecord.calculateChecksum().SequenceEqual(rnRecord.calculateChecksum()));

            Assert.IsTrue(regNames.revertTransaction(1));

            Assert.AreEqual((ulong)2, regNames.count());

            rnRecord = regNames.getName(nameBytes);
            Assert.AreEqual(0, rnRecord.dataRecords.Count);
            assertRecords(origRecord, rnRecord);

            rnRecord = regNames.getName(subnameBytes);
            assertRecords(origSubnameRecord, rnRecord);
        }

        [TestMethod]
        public void Subname_Extend_TopTransactionFeeTooLow()
        {
            string name = "test";
            byte[] nameBytes = IxiNameUtils.encodeAndHashIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = ConsensusConfig.rnMinCapacity;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * (ulong)capacity * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
            Address nextPkHash = wallet1.getPrimaryAddress();
            Address recoveryHash = wallet2.getPrimaryAddress();
            IxiNumber subnamePrice = 100000;
            Address subnameFeeRecipient = wallet4.getPrimaryAddress();

            Assert.IsTrue(registerName(name, registrationTimeInBlocks, capacity, regFee, nextPkHash, recoveryHash));

            nextPkHash = wallet3.getPrimaryAddress();
            allowSubnames(nameBytes, true, subnamePrice, subnameFeeRecipient, 1, nextPkHash, wallet1);

            Address subnameNextPkHash = wallet4.getPrimaryAddress();
            Address subnameRecoveryHash = wallet1.getPrimaryAddress();

            var rnRecord = regNames.getName(nameBytes);
            var origRecord = new RegisteredNameRecord(rnRecord);


            // Register subname
            byte[] subnameBytes = IxiNameUtils.encodeAndHashIxiName("subNameTest." + name);
            IxiNumber subnameFee = subnamePrice * (ulong)capacity * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);

            ulong curBlockHeight = 1;
            ulong expirationBlockHeight;

            var toEntry = RegisteredNamesTransactions.createRegisterToEntry(subnameBytes, registrationTimeInBlocks, capacity, subnameNextPkHash, subnameRecoveryHash, regFee);
            Transaction tx = createDummyTransaction(toEntry);
            tx.toList.Add(subnameFeeRecipient, new ToEntry(Transaction.maxVersion, subnameFee));
            Assert.IsTrue(regNames.verifyTransaction(tx));
            Assert.IsTrue(regNames.applyTransaction(tx, curBlockHeight, out expirationBlockHeight).isApplySuccess);

            rnRecord = regNames.getName(subnameBytes);
            var origSubnameRecord = new RegisteredNameRecord(rnRecord);

            regNames.beginTransaction(1);

            toEntry = RegisteredNamesTransactions.createExtendToEntry(subnameBytes, registrationTimeInBlocks, regFee);
            tx = createDummyTransaction(toEntry);
            tx.toList.Add(subnameFeeRecipient, new ToEntry(Transaction.maxVersion, subnameFee / 2));
            Assert.IsFalse(regNames.verifyTransaction(tx));
            Assert.IsTrue(regNames.applyTransaction(tx, curBlockHeight, out expirationBlockHeight).isApplySuccess);

            rnRecord = regNames.getName(subnameBytes);
            Assert.IsTrue(subnameBytes.SequenceEqual(rnRecord.name), "name != rnRecord.name");
            Assert.AreEqual((registrationTimeInBlocks * 2) + curBlockHeight, rnRecord.expirationBlockHeight);
            Assert.AreEqual(capacity, rnRecord.capacity);
            Assert.IsTrue(subnameNextPkHash.SequenceEqual(rnRecord.nextPkHash), "nextPkHash != rnRecord.nextPkHash");
            Assert.IsTrue(subnameRecoveryHash.SequenceEqual(rnRecord.recoveryHash), "recoveryHash != rnRecord.recoveryHash");

            Assert.AreEqual(regFee * 3, regNames.getRewardPool());
            Assert.AreEqual((registrationTimeInBlocks * 2) + curBlockHeight, expirationBlockHeight);

            rnRecord = regNames.getName(nameBytes);
            Assert.AreEqual(rnRecord.version, rnRecord.version);
            Assert.IsTrue(rnRecord.name.SequenceEqual(rnRecord.name));
            Assert.AreEqual((registrationTimeInBlocks * 2) + curBlockHeight, rnRecord.expirationBlockHeight);
            Assert.IsTrue(rnRecord.subnameFeeRecipient == rnRecord.subnameFeeRecipient || rnRecord.subnameFeeRecipient.SequenceEqual(rnRecord.subnameFeeRecipient));
            Assert.AreEqual(rnRecord.subnamePrice, rnRecord.subnamePrice);
            Assert.AreEqual(rnRecord.allowSubnames, rnRecord.allowSubnames);
            Assert.AreEqual(rnRecord.capacity, rnRecord.capacity);
            Assert.AreEqual(rnRecord.dataRecords.LongCount(), rnRecord.dataRecords.LongCount());
            Assert.IsTrue(rnRecord.dataMerkleRoot == rnRecord.dataMerkleRoot || rnRecord.dataMerkleRoot.SequenceEqual(rnRecord.dataMerkleRoot));
            Assert.IsTrue(rnRecord.nextPkHash.SequenceEqual(rnRecord.nextPkHash));
            Assert.IsTrue(rnRecord.recoveryHash.SequenceEqual(rnRecord.recoveryHash));
            Assert.IsTrue(rnRecord.signature == rnRecord.signature || rnRecord.signature.SequenceEqual(rnRecord.signature));
            Assert.IsTrue(rnRecord.signaturePk == rnRecord.signaturePk || rnRecord.signaturePk.SequenceEqual(rnRecord.signaturePk));
            Assert.AreEqual(rnRecord.updatedBlockHeight, rnRecord.updatedBlockHeight);
            Assert.IsTrue(rnRecord.calculateChecksum().SequenceEqual(rnRecord.calculateChecksum()));

            Assert.IsTrue(regNames.revertTransaction(1));

            Assert.AreEqual((ulong)2, regNames.count());

            rnRecord = regNames.getName(nameBytes);
            Assert.AreEqual(0, rnRecord.dataRecords.Count);
            assertRecords(origRecord, rnRecord);

            rnRecord = regNames.getName(subnameBytes);
            assertRecords(origSubnameRecord, rnRecord);
        }

        [TestMethod]
        public void Subname_IncreaseCapacity()
        {
            string name = "test";
            byte[] nameBytes = IxiNameUtils.encodeAndHashIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = ConsensusConfig.rnMinCapacity;
            uint newCapacity = ConsensusConfig.rnMinCapacity * 2;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * (ulong)capacity * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
            Address nextPkHash = wallet1.getPrimaryAddress();
            Address recoveryHash = wallet2.getPrimaryAddress();
            IxiNumber subnamePrice = 100000;
            Address subnameFeeRecipient = wallet4.getPrimaryAddress();

            Assert.IsTrue(registerName(name, registrationTimeInBlocks, capacity, regFee, nextPkHash, recoveryHash));

            nextPkHash = wallet3.getPrimaryAddress();
            allowSubnames(nameBytes, true, subnamePrice, subnameFeeRecipient, 1, nextPkHash, wallet1);

            Address subnameNextPkHash = wallet4.getPrimaryAddress();
            Address subnameRecoveryHash = wallet1.getPrimaryAddress();

            var rnRecord = regNames.getName(nameBytes);
            var origRecord = new RegisteredNameRecord(rnRecord);


            // Register subname
            byte[] subnameBytes = IxiNameUtils.encodeAndHashIxiName("subNameTest." + name);
            IxiNumber subnameFee = subnamePrice * (ulong)capacity * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);

            ulong curBlockHeight = 1;
            ulong expirationBlockHeight;

            var toEntry = RegisteredNamesTransactions.createRegisterToEntry(subnameBytes, registrationTimeInBlocks, capacity, subnameNextPkHash, subnameRecoveryHash, regFee);
            Transaction tx = createDummyTransaction(toEntry);
            tx.toList.Add(subnameFeeRecipient, new ToEntry(Transaction.maxVersion, subnameFee));
            Assert.IsTrue(regNames.verifyTransaction(tx));
            Assert.IsTrue(regNames.applyTransaction(tx, curBlockHeight, out expirationBlockHeight).isApplySuccess);

            rnRecord = regNames.getName(subnameBytes);
            var origSubnameRecord = new RegisteredNameRecord(rnRecord);

            regNames.beginTransaction(1);

            subnameNextPkHash = wallet1.getPrimaryAddress();
            var subnameSigPk = new Address(wallet4.getPrimaryPublicKey());

            rnRecord.setCapacity(newCapacity, 1, subnameNextPkHash, subnameSigPk.pubKey, null, 0);
            var newChecksum = rnRecord.calculateChecksum();

            byte[] subnameSig = CryptoManager.lib.getSignature(newChecksum, wallet4.getPrimaryPrivateKey());

            toEntry = RegisteredNamesTransactions.createChangeCapacityToEntry(subnameBytes, newCapacity, 1, subnameNextPkHash, subnameSigPk, subnameSig, regFee);
            tx = createDummyTransaction(toEntry);
            tx.toList.Add(subnameFeeRecipient, new ToEntry(Transaction.maxVersion, subnameFee));
            Assert.IsTrue(regNames.verifyTransaction(tx));
            Assert.IsTrue(regNames.applyTransaction(tx, curBlockHeight, out expirationBlockHeight).isApplySuccess);

            rnRecord = regNames.getName(subnameBytes);
            Assert.IsTrue(subnameBytes.SequenceEqual(rnRecord.name), "name != rnRecord.name");
            Assert.AreEqual(registrationTimeInBlocks + curBlockHeight, rnRecord.expirationBlockHeight);
            Assert.AreEqual(newCapacity, rnRecord.capacity);
            Assert.IsTrue(subnameNextPkHash.SequenceEqual(rnRecord.nextPkHash), "nextPkHash != rnRecord.nextPkHash");
            Assert.IsTrue(subnameRecoveryHash.SequenceEqual(rnRecord.recoveryHash), "recoveryHash != rnRecord.recoveryHash");

            Assert.AreEqual(regFee * 3, regNames.getRewardPool());

            rnRecord = regNames.getName(nameBytes);
            Assert.AreEqual(rnRecord.version, rnRecord.version);
            Assert.IsTrue(rnRecord.name.SequenceEqual(rnRecord.name));
            Assert.AreEqual(registrationTimeInBlocks + curBlockHeight, rnRecord.expirationBlockHeight);
            Assert.IsTrue(rnRecord.subnameFeeRecipient == rnRecord.subnameFeeRecipient || rnRecord.subnameFeeRecipient.SequenceEqual(rnRecord.subnameFeeRecipient));
            Assert.AreEqual(rnRecord.subnamePrice, rnRecord.subnamePrice);
            Assert.AreEqual(rnRecord.allowSubnames, rnRecord.allowSubnames);
            Assert.AreEqual(rnRecord.capacity, rnRecord.capacity);
            Assert.AreEqual(rnRecord.dataRecords.LongCount(), rnRecord.dataRecords.LongCount());
            Assert.IsTrue(rnRecord.dataMerkleRoot == rnRecord.dataMerkleRoot || rnRecord.dataMerkleRoot.SequenceEqual(rnRecord.dataMerkleRoot));
            Assert.IsTrue(rnRecord.nextPkHash.SequenceEqual(rnRecord.nextPkHash));
            Assert.IsTrue(rnRecord.recoveryHash.SequenceEqual(rnRecord.recoveryHash));
            Assert.IsTrue(rnRecord.signature == rnRecord.signature || rnRecord.signature.SequenceEqual(rnRecord.signature));
            Assert.IsTrue(rnRecord.signaturePk == rnRecord.signaturePk || rnRecord.signaturePk.SequenceEqual(rnRecord.signaturePk));
            Assert.AreEqual(rnRecord.updatedBlockHeight, rnRecord.updatedBlockHeight);
            Assert.IsTrue(rnRecord.calculateChecksum().SequenceEqual(rnRecord.calculateChecksum()));

            Assert.AreEqual(234uL, rnRecord.updatedBlockHeight);

            Assert.IsTrue(regNames.revertTransaction(1));

            Assert.AreEqual((ulong)2, regNames.count());

            rnRecord = regNames.getName(nameBytes);
            Assert.AreEqual(0, rnRecord.dataRecords.Count);
            assertRecords(origRecord, rnRecord);

            rnRecord = regNames.getName(subnameBytes);
            assertRecords(origSubnameRecord, rnRecord);
        }

        [TestMethod]
        public void Subname_IncreaseCapacity_NoTopTransactionFee()
        {
            string name = "test";
            byte[] nameBytes = IxiNameUtils.encodeAndHashIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = ConsensusConfig.rnMinCapacity;
            uint newCapacity = ConsensusConfig.rnMinCapacity * 2;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * (ulong)capacity * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
            Address nextPkHash = wallet1.getPrimaryAddress();
            Address recoveryHash = wallet2.getPrimaryAddress();
            IxiNumber subnamePrice = 100000;
            Address subnameFeeRecipient = wallet4.getPrimaryAddress();

            Assert.IsTrue(registerName(name, registrationTimeInBlocks, capacity, regFee, nextPkHash, recoveryHash));

            nextPkHash = wallet3.getPrimaryAddress();
            allowSubnames(nameBytes, true, subnamePrice, subnameFeeRecipient, 1, nextPkHash, wallet1);

            Address subnameNextPkHash = wallet4.getPrimaryAddress();
            Address subnameRecoveryHash = wallet1.getPrimaryAddress();

            var rnRecord = regNames.getName(nameBytes);
            var origRecord = new RegisteredNameRecord(rnRecord);


            // Register subname
            byte[] subnameBytes = IxiNameUtils.encodeAndHashIxiName("subNameTest." + name);
            IxiNumber subnameFee = subnamePrice * (ulong)capacity * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);

            ulong curBlockHeight = 1;
            ulong expirationBlockHeight;

            var toEntry = RegisteredNamesTransactions.createRegisterToEntry(subnameBytes, registrationTimeInBlocks, capacity, subnameNextPkHash, subnameRecoveryHash, regFee);
            Transaction tx = createDummyTransaction(toEntry);
            tx.toList.Add(subnameFeeRecipient, new ToEntry(Transaction.maxVersion, subnameFee));
            Assert.IsTrue(regNames.verifyTransaction(tx));
            Assert.IsTrue(regNames.applyTransaction(tx, curBlockHeight, out expirationBlockHeight).isApplySuccess);

            rnRecord = regNames.getName(subnameBytes);
            var origSubnameRecord = new RegisteredNameRecord(rnRecord);

            regNames.beginTransaction(1);

            subnameNextPkHash = wallet1.getPrimaryAddress();
            var subnameSigPk = new Address(wallet4.getPrimaryPublicKey());

            rnRecord.setCapacity(newCapacity, 1, subnameNextPkHash, subnameSigPk.pubKey, null, 0);
            var newChecksum = rnRecord.calculateChecksum();

            byte[] subnameSig = CryptoManager.lib.getSignature(newChecksum, wallet4.getPrimaryPrivateKey());

            toEntry = RegisteredNamesTransactions.createChangeCapacityToEntry(subnameBytes, newCapacity, 1, subnameNextPkHash, subnameSigPk, subnameSig, regFee);
            tx = createDummyTransaction(toEntry);
            Assert.IsFalse(regNames.verifyTransaction(tx));
            Assert.IsTrue(regNames.applyTransaction(tx, curBlockHeight, out expirationBlockHeight).isApplySuccess);

            rnRecord = regNames.getName(subnameBytes);
            Assert.IsTrue(subnameBytes.SequenceEqual(rnRecord.name), "name != rnRecord.name");
            Assert.AreEqual(registrationTimeInBlocks + curBlockHeight, rnRecord.expirationBlockHeight);
            Assert.AreEqual(newCapacity, rnRecord.capacity);
            Assert.IsTrue(subnameNextPkHash.SequenceEqual(rnRecord.nextPkHash), "nextPkHash != rnRecord.nextPkHash");
            Assert.IsTrue(subnameRecoveryHash.SequenceEqual(rnRecord.recoveryHash), "recoveryHash != rnRecord.recoveryHash");

            Assert.AreEqual(regFee * 3, regNames.getRewardPool());

            rnRecord = regNames.getName(nameBytes);
            Assert.AreEqual(rnRecord.version, rnRecord.version);
            Assert.IsTrue(rnRecord.name.SequenceEqual(rnRecord.name));
            Assert.AreEqual(registrationTimeInBlocks + curBlockHeight, rnRecord.expirationBlockHeight);
            Assert.IsTrue(rnRecord.subnameFeeRecipient == rnRecord.subnameFeeRecipient || rnRecord.subnameFeeRecipient.SequenceEqual(rnRecord.subnameFeeRecipient));
            Assert.AreEqual(rnRecord.subnamePrice, rnRecord.subnamePrice);
            Assert.AreEqual(rnRecord.allowSubnames, rnRecord.allowSubnames);
            Assert.AreEqual(rnRecord.capacity, rnRecord.capacity);
            Assert.AreEqual(rnRecord.dataRecords.LongCount(), rnRecord.dataRecords.LongCount());
            Assert.IsTrue(rnRecord.dataMerkleRoot == rnRecord.dataMerkleRoot || rnRecord.dataMerkleRoot.SequenceEqual(rnRecord.dataMerkleRoot));
            Assert.IsTrue(rnRecord.nextPkHash.SequenceEqual(rnRecord.nextPkHash));
            Assert.IsTrue(rnRecord.recoveryHash.SequenceEqual(rnRecord.recoveryHash));
            Assert.IsTrue(rnRecord.signature == rnRecord.signature || rnRecord.signature.SequenceEqual(rnRecord.signature));
            Assert.IsTrue(rnRecord.signaturePk == rnRecord.signaturePk || rnRecord.signaturePk.SequenceEqual(rnRecord.signaturePk));
            Assert.AreEqual(rnRecord.updatedBlockHeight, rnRecord.updatedBlockHeight);
            Assert.IsTrue(rnRecord.calculateChecksum().SequenceEqual(rnRecord.calculateChecksum()));

            Assert.IsTrue(regNames.revertTransaction(1));

            Assert.AreEqual((ulong)2, regNames.count());

            rnRecord = regNames.getName(nameBytes);
            Assert.AreEqual(0, rnRecord.dataRecords.Count);
            assertRecords(origRecord, rnRecord);

            rnRecord = regNames.getName(subnameBytes);
            assertRecords(origSubnameRecord, rnRecord);
        }

        [TestMethod]
        public void Subname_IncreaseCapacity_TopTransactionFeeTooLow()
        {
            string name = "test";
            byte[] nameBytes = IxiNameUtils.encodeAndHashIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = ConsensusConfig.rnMinCapacity;
            uint newCapacity = ConsensusConfig.rnMinCapacity * 2;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * (ulong)capacity * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
            Address nextPkHash = wallet1.getPrimaryAddress();
            Address recoveryHash = wallet2.getPrimaryAddress();
            IxiNumber subnamePrice = 100000;
            Address subnameFeeRecipient = wallet4.getPrimaryAddress();

            Assert.IsTrue(registerName(name, registrationTimeInBlocks, capacity, regFee, nextPkHash, recoveryHash));

            nextPkHash = wallet3.getPrimaryAddress();
            allowSubnames(nameBytes, true, subnamePrice, subnameFeeRecipient, 1, nextPkHash, wallet1);

            Address subnameNextPkHash = wallet4.getPrimaryAddress();
            Address subnameRecoveryHash = wallet1.getPrimaryAddress();

            var rnRecord = regNames.getName(nameBytes);
            var origRecord = new RegisteredNameRecord(rnRecord);


            // Register subname
            byte[] subnameBytes = IxiNameUtils.encodeAndHashIxiName("subNameTest." + name);
            IxiNumber subnameFee = subnamePrice * (ulong)capacity * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);

            ulong curBlockHeight = 1;
            ulong expirationBlockHeight;

            var toEntry = RegisteredNamesTransactions.createRegisterToEntry(subnameBytes, registrationTimeInBlocks, capacity, subnameNextPkHash, subnameRecoveryHash, regFee);
            Transaction tx = createDummyTransaction(toEntry);
            tx.toList.Add(subnameFeeRecipient, new ToEntry(Transaction.maxVersion, subnameFee));
            Assert.IsTrue(regNames.verifyTransaction(tx));
            Assert.IsTrue(regNames.applyTransaction(tx, curBlockHeight, out expirationBlockHeight).isApplySuccess);

            rnRecord = regNames.getName(subnameBytes);
            var origSubnameRecord = new RegisteredNameRecord(rnRecord);

            regNames.beginTransaction(1);

            subnameNextPkHash = wallet1.getPrimaryAddress();
            var subnameSigPk = new Address(wallet4.getPrimaryPublicKey());

            rnRecord.setCapacity(newCapacity, 1, subnameNextPkHash, subnameSigPk.pubKey, null, 0);
            var newChecksum = rnRecord.calculateChecksum();

            byte[] subnameSig = CryptoManager.lib.getSignature(newChecksum, wallet4.getPrimaryPrivateKey());

            toEntry = RegisteredNamesTransactions.createChangeCapacityToEntry(subnameBytes, newCapacity, 1, subnameNextPkHash, subnameSigPk, subnameSig, regFee);
            tx = createDummyTransaction(toEntry);
            tx.toList.Add(subnameFeeRecipient, new ToEntry(Transaction.maxVersion, subnameFee / 2));
            Assert.IsFalse(regNames.verifyTransaction(tx));
            Assert.IsTrue(regNames.applyTransaction(tx, curBlockHeight, out expirationBlockHeight).isApplySuccess);

            rnRecord = regNames.getName(subnameBytes);
            Assert.IsTrue(subnameBytes.SequenceEqual(rnRecord.name), "name != rnRecord.name");
            Assert.AreEqual(registrationTimeInBlocks + curBlockHeight, rnRecord.expirationBlockHeight);
            Assert.AreEqual(newCapacity, rnRecord.capacity);
            Assert.IsTrue(subnameNextPkHash.SequenceEqual(rnRecord.nextPkHash), "nextPkHash != rnRecord.nextPkHash");
            Assert.IsTrue(subnameRecoveryHash.SequenceEqual(rnRecord.recoveryHash), "recoveryHash != rnRecord.recoveryHash");

            Assert.AreEqual(regFee * 3, regNames.getRewardPool());

            rnRecord = regNames.getName(nameBytes);
            Assert.AreEqual(rnRecord.version, rnRecord.version);
            Assert.IsTrue(rnRecord.name.SequenceEqual(rnRecord.name));
            Assert.AreEqual(registrationTimeInBlocks + curBlockHeight, rnRecord.expirationBlockHeight);
            Assert.IsTrue(rnRecord.subnameFeeRecipient == rnRecord.subnameFeeRecipient || rnRecord.subnameFeeRecipient.SequenceEqual(rnRecord.subnameFeeRecipient));
            Assert.AreEqual(rnRecord.subnamePrice, rnRecord.subnamePrice);
            Assert.AreEqual(rnRecord.allowSubnames, rnRecord.allowSubnames);
            Assert.AreEqual(rnRecord.capacity, rnRecord.capacity);
            Assert.AreEqual(rnRecord.dataRecords.LongCount(), rnRecord.dataRecords.LongCount());
            Assert.IsTrue(rnRecord.dataMerkleRoot == rnRecord.dataMerkleRoot || rnRecord.dataMerkleRoot.SequenceEqual(rnRecord.dataMerkleRoot));
            Assert.IsTrue(rnRecord.nextPkHash.SequenceEqual(rnRecord.nextPkHash));
            Assert.IsTrue(rnRecord.recoveryHash.SequenceEqual(rnRecord.recoveryHash));
            Assert.IsTrue(rnRecord.signature == rnRecord.signature || rnRecord.signature.SequenceEqual(rnRecord.signature));
            Assert.IsTrue(rnRecord.signaturePk == rnRecord.signaturePk || rnRecord.signaturePk.SequenceEqual(rnRecord.signaturePk));
            Assert.AreEqual(rnRecord.updatedBlockHeight, rnRecord.updatedBlockHeight);
            Assert.IsTrue(rnRecord.calculateChecksum().SequenceEqual(rnRecord.calculateChecksum()));

            Assert.IsTrue(regNames.revertTransaction(1));

            Assert.AreEqual((ulong)2, regNames.count());

            rnRecord = regNames.getName(nameBytes);
            Assert.AreEqual(0, rnRecord.dataRecords.Count);
            assertRecords(origRecord, rnRecord);

            rnRecord = regNames.getName(subnameBytes);
            assertRecords(origSubnameRecord, rnRecord);
        }

        [TestMethod]
        public void Subname_Register_LongerExpirationTime()
        {
            string name = "test";
            byte[] nameBytes = IxiNameUtils.encodeAndHashIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = ConsensusConfig.rnMinCapacity;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * (ulong)capacity * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
            Address nextPkHash = wallet1.getPrimaryAddress();
            Address recoveryHash = wallet2.getPrimaryAddress();
            IxiNumber subnamePrice = 100000;
            Address subnameFeeRecipient = wallet4.getPrimaryAddress();

            Assert.IsTrue(registerName(name, registrationTimeInBlocks, capacity, regFee, nextPkHash, recoveryHash));

            nextPkHash = wallet3.getPrimaryAddress();
            allowSubnames(nameBytes, true, subnamePrice, subnameFeeRecipient, 1, nextPkHash, wallet1);

            Address subnameNextPkHash = wallet4.getPrimaryAddress();
            Address subnameRecoveryHash = wallet1.getPrimaryAddress();

            var rnRecord = regNames.getName(nameBytes);
            var origRecord = new RegisteredNameRecord(rnRecord);

            regNames.beginTransaction(1);

            // Register subname
            byte[] subnameBytes = IxiNameUtils.encodeAndHashIxiName("subNameTest." + name);
            IxiNumber subnameFee = subnamePrice * (ulong)capacity * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks) * 2;

            ulong curBlockHeight = 1;
            ulong expirationBlockHeight;

            var toEntry = RegisteredNamesTransactions.createRegisterToEntry(subnameBytes, registrationTimeInBlocks * 2, capacity, subnameNextPkHash, subnameRecoveryHash, regFee * 2);
            Transaction tx = createDummyTransaction(toEntry);
            tx.toList.Add(subnameFeeRecipient, new ToEntry(Transaction.maxVersion, subnameFee));
            Assert.IsTrue(regNames.verifyTransaction(tx));
            Assert.IsTrue(regNames.applyTransaction(tx, curBlockHeight, out expirationBlockHeight).isApplySuccess);

            rnRecord = regNames.getName(subnameBytes);
            Assert.IsTrue(subnameBytes.SequenceEqual(rnRecord.name), "name != rnRecord.name");
            Assert.AreEqual((registrationTimeInBlocks * 2) + curBlockHeight, rnRecord.expirationBlockHeight);
            Assert.AreEqual((registrationTimeInBlocks * 2) + curBlockHeight, expirationBlockHeight);
            Assert.AreEqual(capacity, rnRecord.capacity);
            Assert.IsTrue(subnameNextPkHash.SequenceEqual(rnRecord.nextPkHash), "nextPkHash != rnRecord.nextPkHash");
            Assert.IsTrue(subnameRecoveryHash.SequenceEqual(rnRecord.recoveryHash), "recoveryHash != rnRecord.recoveryHash");

            Assert.AreEqual(regFee * 3, regNames.getRewardPool());

            rnRecord = regNames.getName(nameBytes);
            Assert.AreEqual((registrationTimeInBlocks * 2) + curBlockHeight, rnRecord.expirationBlockHeight);

            Assert.IsTrue(regNames.revertTransaction(1));

            rnRecord = regNames.getName(nameBytes);
            Assert.AreEqual((ulong)1, regNames.count());
            Assert.AreEqual(0, rnRecord.dataRecords.Count);

            assertRecords(origRecord, rnRecord);
        }

        [TestMethod]
        public void Subname_Register_NoTopName()
        {
            string name = "test";
            byte[] nameBytes = IxiNameUtils.encodeAndHashIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = ConsensusConfig.rnMinCapacity;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * (ulong)capacity * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
            Address nextPkHash = wallet1.getPrimaryAddress();
            Address recoveryHash = wallet2.getPrimaryAddress();
            IxiNumber subnameFee = 100000;
            Address subnameFeeRecipient = wallet4.getPrimaryAddress();

            Assert.IsTrue(registerName(name, registrationTimeInBlocks, capacity, regFee, nextPkHash, recoveryHash));

            nextPkHash = wallet3.getPrimaryAddress();
            allowSubnames(nameBytes, true, subnameFee, subnameFeeRecipient, 1, nextPkHash, wallet1);

            Address subnameNextPkHash = wallet4.getPrimaryAddress();
            Address subnameRecoveryHash = wallet1.getPrimaryAddress();

            var rnRecord = regNames.getName(nameBytes);
            var origRecord = new RegisteredNameRecord(rnRecord);

            regNames.beginTransaction(1);

            // Register subname
            byte[] subnameBytes = IxiNameUtils.encodeAndHashIxiName("subNameTest." + name + "1");

            ulong curBlockHeight = 1;
            ulong expirationBlockHeight;

            var toEntry = RegisteredNamesTransactions.createRegisterToEntry(subnameBytes, registrationTimeInBlocks, capacity, subnameNextPkHash, subnameRecoveryHash, regFee);
            Transaction tx = createDummyTransaction(toEntry);
            Assert.IsFalse(regNames.verifyTransaction(tx));
            Assert.IsFalse(regNames.applyTransaction(tx, curBlockHeight, out expirationBlockHeight).isApplySuccess);

            rnRecord = regNames.getName(subnameBytes);
            Assert.IsNull(rnRecord);

            Assert.AreEqual(regFee, regNames.getRewardPool());
            Assert.AreEqual(registrationTimeInBlocks + curBlockHeight, expirationBlockHeight);


            Assert.IsTrue(regNames.revertTransaction(1));

            rnRecord = regNames.getName(nameBytes);
            Assert.AreEqual((ulong)1, regNames.count());
            Assert.AreEqual(0, rnRecord.dataRecords.Count);

            assertRecords(origRecord, rnRecord);
        }

        [TestMethod]
        public void Subname_Register_TopNameDoesntAllowSubnames()
        {
            string name = "test";
            byte[] nameBytes = IxiNameUtils.encodeAndHashIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = ConsensusConfig.rnMinCapacity;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * (ulong)capacity * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
            Address nextPkHash = wallet1.getPrimaryAddress();
            Address recoveryHash = wallet2.getPrimaryAddress();
            IxiNumber subnameFee = 100000;
            Address subnameFeeRecipient = wallet4.getPrimaryAddress();

            Assert.IsTrue(registerName(name, registrationTimeInBlocks, capacity, regFee, nextPkHash, recoveryHash));

            Address subnameNextPkHash = wallet4.getPrimaryAddress();
            Address subnameRecoveryHash = wallet1.getPrimaryAddress();

            var rnRecord = regNames.getName(nameBytes);
            var origRecord = new RegisteredNameRecord(rnRecord);

            regNames.beginTransaction(1);

            // Register subname
            byte[] subnameBytes = IxiNameUtils.encodeAndHashIxiName("subNameTest." + name);

            ulong curBlockHeight = 1;
            ulong expirationBlockHeight;

            var toEntry = RegisteredNamesTransactions.createRegisterToEntry(subnameBytes, registrationTimeInBlocks, capacity, subnameNextPkHash, subnameRecoveryHash, regFee);
            Transaction tx = createDummyTransaction(toEntry);
            Assert.IsFalse(regNames.verifyTransaction(tx));
            Assert.IsFalse(regNames.applyTransaction(tx, curBlockHeight, out expirationBlockHeight).isApplySuccess);

            rnRecord = regNames.getName(subnameBytes);
            Assert.IsNull(rnRecord);

            Assert.AreEqual(regFee, regNames.getRewardPool());
            Assert.AreEqual(registrationTimeInBlocks + curBlockHeight, expirationBlockHeight);


            Assert.IsTrue(regNames.revertTransaction(1));

            rnRecord = regNames.getName(nameBytes);
            Assert.AreEqual((ulong)1, regNames.count());
            Assert.AreEqual(0, rnRecord.dataRecords.Count);

            assertRecords(origRecord, rnRecord);
        }

        [TestMethod]
        public void Subname_Register_TransactionHasTooLowTopFee()
        {
            string name = "test";
            byte[] nameBytes = IxiNameUtils.encodeAndHashIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = ConsensusConfig.rnMinCapacity;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * (ulong)capacity * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
            Address nextPkHash = wallet1.getPrimaryAddress();
            Address recoveryHash = wallet2.getPrimaryAddress();
            IxiNumber subnamePrice = 100000;
            Address subnameFeeRecipient = wallet4.getPrimaryAddress();

            Assert.IsTrue(registerName(name, registrationTimeInBlocks, capacity, regFee, nextPkHash, recoveryHash));

            nextPkHash = wallet3.getPrimaryAddress();
            allowSubnames(nameBytes, true, subnamePrice, subnameFeeRecipient, 1, nextPkHash, wallet1);

            Address subnameNextPkHash = wallet4.getPrimaryAddress();
            Address subnameRecoveryHash = wallet1.getPrimaryAddress();

            var rnRecord = regNames.getName(nameBytes);
            var origRecord = new RegisteredNameRecord(rnRecord);

            regNames.beginTransaction(1);

            // Register subname
            byte[] subnameBytes = IxiNameUtils.encodeAndHashIxiName("subNameTest." + name);
            IxiNumber subnameFee = subnamePrice * (ulong)capacity * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);

            ulong curBlockHeight = 1;
            ulong expirationBlockHeight;

            var toEntry = RegisteredNamesTransactions.createRegisterToEntry(subnameBytes, registrationTimeInBlocks, capacity, subnameNextPkHash, subnameRecoveryHash, regFee);
            Transaction tx = createDummyTransaction(toEntry);
            tx.toList.Add(subnameFeeRecipient, new ToEntry(Transaction.maxVersion, subnameFee - 1));
            Assert.IsFalse(regNames.verifyTransaction(tx));
            Assert.IsTrue(regNames.applyTransaction(tx, curBlockHeight, out expirationBlockHeight).isApplySuccess);

            rnRecord = regNames.getName(subnameBytes);
            Assert.IsTrue(subnameBytes.SequenceEqual(rnRecord.name), "name != rnRecord.name");
            Assert.AreEqual(registrationTimeInBlocks + curBlockHeight, rnRecord.expirationBlockHeight);
            Assert.AreEqual(capacity, rnRecord.capacity);
            Assert.IsTrue(subnameNextPkHash.SequenceEqual(rnRecord.nextPkHash), "nextPkHash != rnRecord.nextPkHash");
            Assert.IsTrue(subnameRecoveryHash.SequenceEqual(rnRecord.recoveryHash), "recoveryHash != rnRecord.recoveryHash");

            Assert.AreEqual(regFee * 2, regNames.getRewardPool());
            Assert.AreEqual(registrationTimeInBlocks + curBlockHeight, expirationBlockHeight);


            Assert.IsTrue(regNames.revertTransaction(1));

            rnRecord = regNames.getName(nameBytes);
            Assert.AreEqual((ulong)1, regNames.count());
            Assert.AreEqual(0, rnRecord.dataRecords.Count);

            assertRecords(origRecord, rnRecord);
        }

        [TestMethod]
        public void Subname_Register_TransactionHasNoTopFee()
        {
            string name = "test";
            byte[] nameBytes = IxiNameUtils.encodeAndHashIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = ConsensusConfig.rnMinCapacity;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * (ulong)capacity * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
            Address nextPkHash = wallet1.getPrimaryAddress();
            Address recoveryHash = wallet2.getPrimaryAddress();
            IxiNumber subnameFee = 100000;
            Address subnameFeeRecipient = wallet4.getPrimaryAddress();

            Assert.IsTrue(registerName(name, registrationTimeInBlocks, capacity, regFee, nextPkHash, recoveryHash));

            nextPkHash = wallet3.getPrimaryAddress();
            allowSubnames(nameBytes, true, subnameFee, subnameFeeRecipient, 1, nextPkHash, wallet1);

            Address subnameNextPkHash = wallet4.getPrimaryAddress();
            Address subnameRecoveryHash = wallet1.getPrimaryAddress();

            var rnRecord = regNames.getName(nameBytes);
            var origRecord = new RegisteredNameRecord(rnRecord);

            regNames.beginTransaction(1);

            // Register subname
            byte[] subnameBytes = IxiNameUtils.encodeAndHashIxiName("subNameTest." + name);

            ulong curBlockHeight = 1;
            ulong expirationBlockHeight;

            var toEntry = RegisteredNamesTransactions.createRegisterToEntry(subnameBytes, registrationTimeInBlocks, capacity, subnameNextPkHash, subnameRecoveryHash, regFee);
            Transaction tx = createDummyTransaction(toEntry);
            Assert.IsFalse(regNames.verifyTransaction(tx));
            Assert.IsTrue(regNames.applyTransaction(tx, curBlockHeight, out expirationBlockHeight).isApplySuccess);

            rnRecord = regNames.getName(subnameBytes);
            Assert.IsTrue(subnameBytes.SequenceEqual(rnRecord.name), "name != rnRecord.name");
            Assert.AreEqual(registrationTimeInBlocks + curBlockHeight, rnRecord.expirationBlockHeight);
            Assert.AreEqual(capacity, rnRecord.capacity);
            Assert.IsTrue(subnameNextPkHash.SequenceEqual(rnRecord.nextPkHash), "nextPkHash != rnRecord.nextPkHash");
            Assert.IsTrue(subnameRecoveryHash.SequenceEqual(rnRecord.recoveryHash), "recoveryHash != rnRecord.recoveryHash");

            Assert.AreEqual(regFee * 2, regNames.getRewardPool());
            Assert.AreEqual(registrationTimeInBlocks + curBlockHeight, expirationBlockHeight);


            Assert.IsTrue(regNames.revertTransaction(1));

            rnRecord = regNames.getName(nameBytes);
            Assert.AreEqual((ulong)1, regNames.count());
            Assert.AreEqual(0, rnRecord.dataRecords.Count);

            assertRecords(origRecord, rnRecord);
        }

        [TestMethod]
        public void Subname_Register_AlreadyRegistered()
        {
            string name = "test";
            byte[] nameBytes = IxiNameUtils.encodeAndHashIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = ConsensusConfig.rnMinCapacity;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * (ulong)capacity * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
            Address nextPkHash = wallet1.getPrimaryAddress();
            Address recoveryHash = wallet2.getPrimaryAddress();
            IxiNumber subnamePrice = 100000;
            Address subnameFeeRecipient = wallet4.getPrimaryAddress();

            Assert.IsTrue(registerName(name, registrationTimeInBlocks, capacity, regFee, nextPkHash, recoveryHash));

            nextPkHash = wallet3.getPrimaryAddress();
            allowSubnames(nameBytes, true, subnamePrice, subnameFeeRecipient, 1, nextPkHash, wallet1);

            Address subnameNextPkHash = wallet4.getPrimaryAddress();
            Address subnameRecoveryHash = wallet1.getPrimaryAddress();

            var rnRecord = regNames.getName(nameBytes);
            var origRecord = new RegisteredNameRecord(rnRecord);

            regNames.beginTransaction(1);

            // Register subname
            byte[] subnameBytes = IxiNameUtils.encodeAndHashIxiName("subNameTest." + name);
            IxiNumber subnameFee = subnamePrice * (ulong)capacity * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);

            ulong curBlockHeight = 1;
            ulong expirationBlockHeight;

            var toEntry = RegisteredNamesTransactions.createRegisterToEntry(subnameBytes, registrationTimeInBlocks, capacity, subnameNextPkHash, subnameRecoveryHash, regFee);
            Transaction tx = createDummyTransaction(toEntry);
            tx.toList.Add(subnameFeeRecipient, new ToEntry(Transaction.maxVersion, subnameFee));
            Assert.IsTrue(regNames.verifyTransaction(tx));
            Assert.IsTrue(regNames.applyTransaction(tx, curBlockHeight, out expirationBlockHeight).isApplySuccess);

            rnRecord = regNames.getName(subnameBytes);
            Assert.IsTrue(subnameBytes.SequenceEqual(rnRecord.name), "name != rnRecord.name");
            Assert.AreEqual(registrationTimeInBlocks + curBlockHeight, rnRecord.expirationBlockHeight);
            Assert.AreEqual(capacity, rnRecord.capacity);
            Assert.IsTrue(subnameNextPkHash.SequenceEqual(rnRecord.nextPkHash), "nextPkHash != rnRecord.nextPkHash");
            Assert.IsTrue(subnameRecoveryHash.SequenceEqual(rnRecord.recoveryHash), "recoveryHash != rnRecord.recoveryHash");

            Assert.AreEqual(regFee * 2, regNames.getRewardPool());
            Assert.AreEqual(registrationTimeInBlocks + curBlockHeight, expirationBlockHeight);

            // Register the same subname again
            Address newSubnameNextPkHash = wallet3.getPrimaryAddress();
            Address newSubnameRecoveryHash = wallet2.getPrimaryAddress();

            toEntry = RegisteredNamesTransactions.createRegisterToEntry(subnameBytes, registrationTimeInBlocks, capacity, newSubnameNextPkHash, newSubnameRecoveryHash, regFee);
            tx = createDummyTransaction(toEntry);
            tx.toList.Add(subnameFeeRecipient, new ToEntry(Transaction.maxVersion, subnameFee));
            Assert.IsFalse(regNames.verifyTransaction(tx));
            Assert.IsFalse(regNames.applyTransaction(tx, curBlockHeight, out expirationBlockHeight).isApplySuccess);

            rnRecord = regNames.getName(subnameBytes);
            Assert.IsTrue(subnameBytes.SequenceEqual(rnRecord.name), "name != rnRecord.name");
            Assert.AreEqual(registrationTimeInBlocks + curBlockHeight, rnRecord.expirationBlockHeight);
            Assert.AreEqual(capacity, rnRecord.capacity);
            Assert.IsTrue(subnameNextPkHash.SequenceEqual(rnRecord.nextPkHash), "nextPkHash != rnRecord.nextPkHash");
            Assert.IsTrue(subnameRecoveryHash.SequenceEqual(rnRecord.recoveryHash), "recoveryHash != rnRecord.recoveryHash");

            Assert.AreEqual(regFee * 2, regNames.getRewardPool());
            Assert.AreEqual(registrationTimeInBlocks + curBlockHeight, expirationBlockHeight);


            Assert.IsTrue(regNames.revertTransaction(1));

            rnRecord = regNames.getName(nameBytes);
            Assert.AreEqual((ulong)1, regNames.count());
            Assert.AreEqual(0, rnRecord.dataRecords.Count);

            assertRecords(origRecord, rnRecord);
        }

        [TestMethod]
        public void SaveToDisk_Single()
        {
            byte[] name = IxiNameUtils.encodeAndHashIxiName("test");
            uint capacity = ConsensusConfig.rnMinCapacity + 1234;
            ulong registeredBlockHeight = 412;
            ulong expirationBlockHeight = 5691;
            byte[] nextPkHashBytes = RandomUtils.GetBytes(33);
            nextPkHashBytes[0] = 0;
            Address nextPkHash = new Address(nextPkHashBytes, null, false);
            byte[] recoveryHashBytes = RandomUtils.GetBytes(45);
            recoveryHashBytes[0] = 1;
            Address recoveryHash = new Address(recoveryHashBytes, null, false);
            var rnr = new RegisteredNameRecord(name, registeredBlockHeight, capacity, expirationBlockHeight, nextPkHash, recoveryHash);

            byte[] recordName = RandomUtils.GetBytes(64);
            int ttl = 23400;
            byte[] data = RandomUtils.GetBytes(64);
            byte[] checksum = RandomUtils.GetBytes(64);
            var rndr = new RegisteredNameDataRecord(recordName, ttl, data, checksum);
            rnr.dataRecords.Add(rndr);
            regNamesMemoryStorage.updateRegName(rnr, true);

            regNamesMemoryStorage.saveToDisk(Config.saveWalletStateEveryBlock);
            regNamesMemoryStorage.clear();
            regNamesMemoryStorage.loadFromDisk(Config.saveWalletStateEveryBlock);

            var rnrRestored = regNamesMemoryStorage.getRegNameHeader(name);
            Assert.IsTrue(name.SequenceEqual(rnrRestored.name));
            Assert.AreEqual(capacity, rnrRestored.capacity);
            Assert.AreEqual(registeredBlockHeight, rnrRestored.registrationBlockHeight);
            Assert.AreEqual(expirationBlockHeight, rnrRestored.expirationBlockHeight);
            Assert.IsTrue(nextPkHashBytes.SequenceEqual(rnrRestored.nextPkHash.addressNoChecksum));
            Assert.IsTrue(recoveryHashBytes.SequenceEqual(rnrRestored.recoveryHash.addressNoChecksum));

            var rndrRestored = rnrRestored.dataRecords.First();
            Assert.IsTrue(recordName.SequenceEqual(rndrRestored.name));
            Assert.AreEqual(ttl, rndrRestored.ttl);
            Assert.IsTrue(data.SequenceEqual(rndrRestored.data));
            Assert.IsTrue(checksum.SequenceEqual(rndrRestored.checksum));

        }

        [TestMethod]
        public void SaveToDisk_Multiple()
        {
            byte[] name = IxiNameUtils.encodeAndHashIxiName("test");
            uint capacity = ConsensusConfig.rnMinCapacity;
            ulong registeredBlockHeight = 421;
            ulong expirationBlockHeight = 5691;
            byte[] nextPkHashBytes = RandomUtils.GetBytes(33);
            nextPkHashBytes[0] = 0;
            Address nextPkHash = new Address(nextPkHashBytes, null, false);
            byte[] recoveryHashBytes = RandomUtils.GetBytes(45);
            recoveryHashBytes[0] = 1;
            Address recoveryHash = new Address(recoveryHashBytes, null, false);
            byte[] subnameFeeRecipientBytes = RandomUtils.GetBytes(45);
            subnameFeeRecipientBytes[0] = 1;
            Address subnameFeeRecipient = new Address(subnameFeeRecipientBytes, null, false);
            var rnr = new RegisteredNameRecord(name, registeredBlockHeight, capacity, expirationBlockHeight, nextPkHash, recoveryHash);
            byte[] sigPk = RandomUtils.GetBytes(512);
            byte[] sig = RandomUtils.GetBytes(512);

            rnr.setAllowSubnames(true, 100, subnameFeeRecipient, 55, nextPkHash, sigPk, sig, 234);


            byte[] recordName = RandomUtils.GetBytes(64);
            int ttl = 23400;
            byte[] data = RandomUtils.GetBytes(64);
            byte[] checksum = RandomUtils.GetBytes(64);
            var rndr = new RegisteredNameDataRecord(recordName, ttl, data, checksum);
            rnr.dataRecords.Add(rndr);
            regNamesMemoryStorage.updateRegName(rnr, true);

            byte[] name2 = IxiNameUtils.encodeAndHashIxiName("test2");
            uint capacity2 = 1234;
            ulong registeredBlockHeight2 = 422;
            ulong expirationBlockHeight2 = 5692;
            byte[] nextPkHashBytes2 = RandomUtils.GetBytes(33);
            nextPkHashBytes2[0] = 0;
            Address nextPkHash2 = new Address(nextPkHashBytes2, null, false);
            byte[] recoveryHashBytes2 = RandomUtils.GetBytes(45);
            recoveryHashBytes2[0] = 1;
            Address recoveryHash2 = new Address(recoveryHashBytes2, null, false);
            var rnr2 = new RegisteredNameRecord(name2, registeredBlockHeight2, capacity2, expirationBlockHeight2, nextPkHash2, recoveryHash2);
            rnr2.signaturePk = RandomUtils.GetBytes(512);
            rnr2.signature = RandomUtils.GetBytes(512);


            byte[] recordName2 = RandomUtils.GetBytes(64);
            int ttl2 = 23400;
            byte[] data2 = RandomUtils.GetBytes(64);
            byte[] checksum2 = RandomUtils.GetBytes(64);
            var rndr2 = new RegisteredNameDataRecord(recordName2, ttl2, data2, checksum2);
            rnr2.dataRecords.Add(rndr2);

            byte[] recordName3 = RandomUtils.GetBytes(64);
            int ttl3 = 23400;
            byte[] data3 = RandomUtils.GetBytes(64);
            byte[] checksum3 = RandomUtils.GetBytes(64);
            var rndr3 = new RegisteredNameDataRecord(recordName3, ttl3, data3, checksum3);
            rnr2.dataRecords.Add(rndr3);
            regNamesMemoryStorage.updateRegName(rnr2, true);

            regNamesMemoryStorage.saveToDisk(Config.saveWalletStateEveryBlock);
            regNamesMemoryStorage.clear();
            regNamesMemoryStorage.loadFromDisk(Config.saveWalletStateEveryBlock);

            var rnrRestored = regNamesMemoryStorage.getRegNameHeader(name);
            Assert.IsTrue(name.SequenceEqual(rnrRestored.name));
            Assert.AreEqual(capacity, rnrRestored.capacity);
            Assert.AreEqual(registeredBlockHeight, rnrRestored.registrationBlockHeight);
            Assert.AreEqual(expirationBlockHeight, rnrRestored.expirationBlockHeight);
            Assert.IsTrue(nextPkHashBytes.SequenceEqual(rnrRestored.nextPkHash.addressNoChecksum));
            Assert.IsTrue(recoveryHashBytes.SequenceEqual(rnrRestored.recoveryHash.addressNoChecksum));
            Assert.IsTrue(sigPk.SequenceEqual(rnrRestored.signaturePk));
            Assert.IsTrue(sig.SequenceEqual(rnrRestored.signature));
            Assert.IsTrue(rnrRestored.allowSubnames);
            Assert.IsTrue(subnameFeeRecipientBytes.SequenceEqual(rnrRestored.subnameFeeRecipient.addressNoChecksum));
            Assert.IsTrue(rnrRestored.subnamePrice == 100);
            Assert.IsTrue(rnrRestored.sequence == 55);
            Assert.AreEqual(234uL, rnrRestored.updatedBlockHeight);

            var rndrRestored = rnrRestored.dataRecords.First();
            Assert.IsTrue(recordName.SequenceEqual(rndrRestored.name));
            Assert.AreEqual(ttl, rndrRestored.ttl);
            Assert.IsTrue(data.SequenceEqual(rndrRestored.data));
            Assert.IsTrue(checksum.SequenceEqual(rndrRestored.checksum));

            var rnrRestored2 = regNamesMemoryStorage.getRegNameHeader(name2);
            Assert.IsTrue(name2.SequenceEqual(rnrRestored2.name));
            Assert.AreEqual(capacity2, rnrRestored2.capacity);
            Assert.AreEqual(registeredBlockHeight2, rnrRestored2.registrationBlockHeight);
            Assert.AreEqual(expirationBlockHeight2, rnrRestored2.expirationBlockHeight);
            Assert.IsTrue(nextPkHashBytes2.SequenceEqual(rnrRestored2.nextPkHash.addressNoChecksum));
            Assert.IsTrue(recoveryHashBytes2.SequenceEqual(rnrRestored2.recoveryHash.addressNoChecksum));

            rndrRestored = rnrRestored2.dataRecords.First();
            Assert.IsTrue(recordName2.SequenceEqual(rndrRestored.name));
            Assert.AreEqual(ttl2, rndrRestored.ttl);
            Assert.IsTrue(data2.SequenceEqual(rndrRestored.data));
            Assert.IsTrue(checksum2.SequenceEqual(rndrRestored.checksum));

            rndrRestored = rnrRestored2.dataRecords.ElementAt(1);
            Assert.IsTrue(recordName3.SequenceEqual(rndrRestored.name));
            Assert.AreEqual(ttl3, rndrRestored.ttl);
            Assert.IsTrue(data3.SequenceEqual(rndrRestored.data));
            Assert.IsTrue(checksum3.SequenceEqual(rndrRestored.checksum));
        }

        [TestMethod]
        public void Merkle_Single()
        {
            List<byte[]> hashes = new List<byte[]>()
            {
                RandomUtils.GetBytes(64)
            };
            var root = IxiUtils.calculateMerkleRoot(hashes);
            Assert.IsTrue(hashes.First().SequenceEqual(root));
        }

        [TestMethod]
        public void Merkle_Pair()
        {
            List<byte[]> hashes = new List<byte[]>()
            {
                Crypto.stringToHash("4f65bf2b9e99943e76bf3b5312baa17a6778a360fdb3b2f40877fbdf70938f623bec83372916bff76b789e23cc35f990a9f4378aa4c5da1667cfb82bb4b12be8"),
                Crypto.stringToHash("50ebdf50474dbed0a3bb238c68e79c2277f8f87e08869322faf828c681c62df1da96e5c15164d6d56adf512b7127b020ea30f9cfe8526fedaee74db8cbc6bad3")
            };
            var expectedRoot = "63da7490c8ae09a43bbeaa31045ac179aa31ed77146ba5df3799756f5a49121690be76a26713c8e5f9903dbe1af8a973ffc915797b2bddfd028f0a161cd08c89";

            var root = IxiUtils.calculateMerkleRoot(hashes);
            Assert.IsTrue(Crypto.stringToHash(expectedRoot).SequenceEqual(root), expectedRoot + " != " + Crypto.hashToString(root));
        }


        [TestMethod]
        public void Merkle_Multiple_Even()
        {
            List<byte[]> hashes = new List<byte[]>()
            {
                Crypto.stringToHash("4f65bf2b9e99943e76bf3b5312baa17a6778a360fdb3b2f40877fbdf70938f623bec83372916bff76b789e23cc35f990a9f4378aa4c5da1667cfb82bb4b12be8"),
                Crypto.stringToHash("50ebdf50474dbed0a3bb238c68e79c2277f8f87e08869322faf828c681c62df1da96e5c15164d6d56adf512b7127b020ea30f9cfe8526fedaee74db8cbc6bad3"),
                Crypto.stringToHash("76efb0904ef2d451719132a93dd40d6e1a024e7ca05787acf5debde9ca5eb7677835b36f5e52b71ce9afbbf16d051c1fc21c27b522b0f2b205a93450cfe6d15f"),
                Crypto.stringToHash("8e56d7bdc2afcfc4e24eb95c12bef06286ce8ebf68b2e211b9d1335f3ea61e4ab174a5713821875b949b33835249eb770b5bdf089ead0998d435db3b6ce75eaa"),
                Crypto.stringToHash("452e4ec8b1f5987de84b804f2851f3318c580c4fb3858b15d99359c8a59e2d7fb016d7a668a77021ef12fa53d976bb2bc05e9d3e699ebe3c0d2c02b786593b96"),
                Crypto.stringToHash("6307a14d4e24a6b6446c68193554678a4cc30672b2ec5fa323f10f945cda7493e249025a7d8aa585c77b960ddda64aec2230c0aa18cf7f3a56aae6d8e4b4a386")
            };
            var expectedRoot = "acd27e41797139fc5247f8cb1f59fbb9f39a84561eb4f740400e991201ce3b5381438a1049c1cf75577fef5832fdc9884389cbb5ccacf6eb90d7fcf7e2f9f0cc";

            var root = IxiUtils.calculateMerkleRoot(hashes);
            Assert.IsTrue(Crypto.stringToHash(expectedRoot).SequenceEqual(root), expectedRoot + " != " + Crypto.hashToString(root));
        }

        [TestMethod]
        public void Merkle_Multiple_Odd()
        {
            List<byte[]> hashes = new List<byte[]>()
            {
                Crypto.stringToHash("4f65bf2b9e99943e76bf3b5312baa17a6778a360fdb3b2f40877fbdf70938f623bec83372916bff76b789e23cc35f990a9f4378aa4c5da1667cfb82bb4b12be8"),
                Crypto.stringToHash("50ebdf50474dbed0a3bb238c68e79c2277f8f87e08869322faf828c681c62df1da96e5c15164d6d56adf512b7127b020ea30f9cfe8526fedaee74db8cbc6bad3"),
                Crypto.stringToHash("76efb0904ef2d451719132a93dd40d6e1a024e7ca05787acf5debde9ca5eb7677835b36f5e52b71ce9afbbf16d051c1fc21c27b522b0f2b205a93450cfe6d15f"),
                Crypto.stringToHash("8e56d7bdc2afcfc4e24eb95c12bef06286ce8ebf68b2e211b9d1335f3ea61e4ab174a5713821875b949b33835249eb770b5bdf089ead0998d435db3b6ce75eaa"),
                Crypto.stringToHash("452e4ec8b1f5987de84b804f2851f3318c580c4fb3858b15d99359c8a59e2d7fb016d7a668a77021ef12fa53d976bb2bc05e9d3e699ebe3c0d2c02b786593b96"),
                Crypto.stringToHash("6307a14d4e24a6b6446c68193554678a4cc30672b2ec5fa323f10f945cda7493e249025a7d8aa585c77b960ddda64aec2230c0aa18cf7f3a56aae6d8e4b4a386"),
                Crypto.stringToHash("68c621ac1e3972b83a376b281f646bc2303c4c15fb84c7fc687f98c856dc2e110fbccf12370cfe21f0921c58faa0ddd05f842f3aa4b1f1c118e88b039effe270")
            };
            var expectedRoot = "a77d2610378fa125dd8c463f87a4e222e35d1b9d280d30cb0ee8036a34a6b56b624176289dda66d22b57d036ab452d770f77a26e1cba1070df726c78105a6b14";

            var root = IxiUtils.calculateMerkleRoot(hashes);
            Assert.IsTrue(Crypto.stringToHash(expectedRoot).SequenceEqual(root), expectedRoot + " != " + Crypto.hashToString(root));
        }
    }
}