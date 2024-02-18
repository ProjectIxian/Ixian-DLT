using DLT.Meta;
using DLT.RegNames;
using IXICore;
using IXICore.Meta;
using IXICore.RegNames;
using IXICore.Utils;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Unicode;
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
            uint capacity = 1;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
            Address nextPkHash = wallet1.getPrimaryAddress();
            Address recoveryHash = wallet2.getPrimaryAddress();

            regNames.beginTransaction(1);

            registerName(name, registrationTimeInBlocks, capacity, regFee, nextPkHash, recoveryHash);

            regNames.revertTransaction(1);

            Assert.AreEqual((ulong)0, regNames.count());
            Assert.AreEqual(0, regNames.getRewardPool());
        }

        [TestMethod]
        public void RegisterName_AlreadyRegistered()
        {
            string name = "test";
            byte[] nameBytes = IxiNameUtils.encodeIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = 1;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
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

            regNames.revertTransaction(1);

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
            uint capacity = 1;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks) / 2;
            Address nextPkHash = wallet1.getPrimaryAddress();
            Address recoveryHash = wallet2.getPrimaryAddress();

            regNames.beginTransaction(1);

            byte[] nameBytes = IxiNameUtils.encodeIxiName(name);

            var toEntry = RegisteredNamesTransactions.createRegisterToEntry(nameBytes, registrationTimeInBlocks, capacity, nextPkHash, recoveryHash, regFee);
            Transaction tx = createDummyTransaction(toEntry);
            Assert.IsFalse(regNames.verifyTransaction(tx));

            var rnRecord = regNames.getName(nameBytes);

            Assert.IsTrue(rnRecord == null);
            Assert.AreEqual(0, regNames.getRewardPool());

            regNames.revertTransaction(1);

            Assert.AreEqual((ulong)0, regNames.count());
            Assert.AreEqual(0, regNames.getRewardPool());
        }

        [TestMethod]
        public void RegisterName_RegistrationTimeTooLow()
        {
            string name = "test";
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks / 2;
            uint capacity = 1;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
            Address nextPkHash = wallet1.getPrimaryAddress();
            Address recoveryHash = wallet2.getPrimaryAddress();

            regNames.beginTransaction(1);

            byte[] nameBytes = IxiNameUtils.encodeIxiName(name);

            var toEntry = RegisteredNamesTransactions.createRegisterToEntry(nameBytes, registrationTimeInBlocks, capacity, nextPkHash, recoveryHash, regFee);
            Transaction tx = createDummyTransaction(toEntry);
            Assert.IsFalse(regNames.verifyTransaction(tx));

            var rnRecord = regNames.getName(nameBytes);

            Assert.IsTrue(rnRecord == null);
            Assert.AreEqual(0, regNames.getRewardPool());

            regNames.revertTransaction(1);

            Assert.AreEqual((ulong)0, regNames.count());
            Assert.AreEqual(0, regNames.getRewardPool());
        }

        [TestMethod]
        public void RegisterName_RegistrationTimeTooHigh()
        {
            string name = "test";
            uint registrationTimeInBlocks = ConsensusConfig.rnMaxRegistrationTimeInBlocks + 1;
            uint capacity = 1;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
            Address nextPkHash = wallet1.getPrimaryAddress();
            Address recoveryHash = wallet2.getPrimaryAddress();

            regNames.beginTransaction(1);

            byte[] nameBytes = IxiNameUtils.encodeIxiName(name);

            var toEntry = RegisteredNamesTransactions.createRegisterToEntry(nameBytes, registrationTimeInBlocks, capacity, nextPkHash, recoveryHash, regFee);
            Transaction tx = createDummyTransaction(toEntry);
            Assert.IsFalse(regNames.verifyTransaction(tx));

            var rnRecord = regNames.getName(nameBytes);

            Assert.IsTrue(rnRecord == null);
            Assert.AreEqual(0, regNames.getRewardPool());

            regNames.revertTransaction(1);

            Assert.AreEqual((ulong)0, regNames.count());
            Assert.AreEqual(0, regNames.getRewardPool());
        }

        private bool registerName(string name, uint registrationTimeInBlocks, uint capacity, IxiNumber regFee, Address nextPkHash, Address recoveryHash, IxiNumber expectedRewardPool = null)
        {
            byte[] nameBytes = IxiNameUtils.encodeIxiName(name);
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

            return status.isApplySuccess;
        }

        [TestMethod]
        public void ExtendName()
        {
            string name = "test";
            byte[] nameBytes = IxiNameUtils.encodeIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = 1;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
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

            regNames.revertTransaction(1);

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
            byte[] nameBytes = IxiNameUtils.encodeIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = 1;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
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

            Assert.AreEqual(750, regNames.getRewardPool());
            Assert.AreEqual(extensionTimeInBlocks + lastExpirationTime, expirationBlockHeight);

            regNames.revertTransaction(1);

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
            byte[] nameBytes = IxiNameUtils.encodeIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = 1;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
            Address nextPkHash = wallet1.getPrimaryAddress();
            Address recoveryHash = wallet2.getPrimaryAddress();

            registerName(name, registrationTimeInBlocks, capacity, regFee, nextPkHash, recoveryHash);

            var rnRecord = regNames.getName(nameBytes);
            var origRecord = new RegisteredNameRecord(rnRecord);

            regNames.beginTransaction(1);

            ulong expirationBlockHeight;

            ulong lastExpirationTime = rnRecord.expirationBlockHeight;

            var nonExistentNameBytes = IxiNameUtils.encodeIxiName("nonExistentName");
            var toEntry = RegisteredNamesTransactions.createExtendToEntry(nonExistentNameBytes, registrationTimeInBlocks, regFee);
            Transaction tx = createDummyTransaction(toEntry);
            Assert.IsFalse(regNames.verifyTransaction(tx));
            Assert.IsFalse(regNames.applyTransaction(tx, 1, out expirationBlockHeight).isApplySuccess);

            rnRecord = regNames.getName(nonExistentNameBytes);
            Assert.IsNull(rnRecord);

            Assert.AreEqual(500, regNames.getRewardPool());

            regNames.revertTransaction(1);

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
            Assert.AreEqual(rec1.capacity, rec2.capacity);
            Assert.AreEqual(rec1.dataRecords.LongCount(), rec2.dataRecords.LongCount());
            Assert.IsTrue(rec1.dataMerkleRoot == rec2.dataMerkleRoot || rec1.dataMerkleRoot.SequenceEqual(rec2.dataMerkleRoot));
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
            byte[] nameBytes = IxiNameUtils.encodeIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = 1;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
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

            var newChecksum = regNames.calculateRegNameChecksumFromUpdatedDataRecords(nameBytes, records, nextPkHash);

            byte[] sig = CryptoManager.lib.getSignature(newChecksum, wallet1.getPrimaryPrivateKey());

            ulong expirationBlockHeight;

            var toEntry = RegisteredNamesTransactions.createUpdateRecordToEntry(nameBytes, records, nextPkHash, pkSig, sig);
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

            regNames.revertTransaction(1);

            rnRecord = regNames.getName(nameBytes);
            Assert.AreEqual((ulong)1, regNames.count());
            Assert.AreEqual(0, rnRecord.dataRecords.Count);

            assertRecords(origRecord, rnRecord);
        }

        [TestMethod]
        public void UpdateRecord_DuplicateRecord()
        {
            string name = "test";
            byte[] nameBytes = IxiNameUtils.encodeIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = 1;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
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
                new RegisteredNameDataRecord(recordName, 1, data),
                new RegisteredNameDataRecord(recordName, 1, data)
            };

            var newChecksum = regNames.calculateRegNameChecksumFromUpdatedDataRecords(nameBytes, records, nextPkHash);

            byte[] sig = CryptoManager.lib.getSignature(newChecksum, wallet1.getPrimaryPrivateKey());

            ulong expirationBlockHeight;

            var toEntry = RegisteredNamesTransactions.createUpdateRecordToEntry(nameBytes, records, nextPkHash, pkSig, sig);
            Transaction tx = createDummyTransaction(toEntry);
            Assert.IsTrue(regNames.verifyTransaction(tx));
            Assert.IsTrue(regNames.applyTransaction(tx, 1, out expirationBlockHeight).isApplySuccess);

            rnRecord = regNames.getName(nameBytes);

            regNames.beginTransaction(1);

            nextPkHash = wallet1.getPrimaryAddress();
            pkSig = new Address(wallet3.getPrimaryPublicKey());

            records = new()
            {
                new RegisteredNameDataRecord(recordName, 1, data)
            };

            newChecksum = regNames.calculateRegNameChecksumFromUpdatedDataRecords(nameBytes, records, nextPkHash);

            sig = CryptoManager.lib.getSignature(newChecksum, wallet3.getPrimaryPrivateKey());

            toEntry = RegisteredNamesTransactions.createUpdateRecordToEntry(nameBytes, records, nextPkHash, pkSig, sig);
            tx = createDummyTransaction(toEntry);
            Assert.IsTrue(regNames.verifyTransaction(tx));
            Assert.IsTrue(regNames.applyTransaction(tx, 1, out expirationBlockHeight).isApplySuccess);


            rnRecord = regNames.getName(nameBytes);
            Assert.IsTrue(nameBytes.SequenceEqual(rnRecord.name), "rnur.name != rnRecord.name");
            Assert.AreEqual(3, rnRecord.dataRecords.Count);

            Assert.IsTrue(newChecksum.SequenceEqual(rnRecord.calculateChecksum()), "newChecksum != rnRecord.checksum");
            Assert.IsTrue(nextPkHash.SequenceEqual(rnRecord.nextPkHash), "nextPkHash != rnRecord.nextPkHash");
            Assert.IsTrue(pkSig.pubKey.SequenceEqual(rnRecord.signaturePk), "pkSig != rnRecord.signaturePk");
            Assert.IsTrue(sig.SequenceEqual(rnRecord.signature), "sig != rnRecord.signature");

            regNames.revertTransaction(1);

            rnRecord = regNames.getName(nameBytes);
            Assert.AreEqual((ulong)1, regNames.count());
            Assert.AreEqual(0, rnRecord.dataRecords.Count);

            assertRecords(origRecord, rnRecord);
        }

        [TestMethod]
        public void UpdateRecord_ReplaceRecord()
        {
            string name = "test";
            byte[] nameBytes = IxiNameUtils.encodeIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = 1;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
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

            var newChecksum = regNames.calculateRegNameChecksumFromUpdatedDataRecords(nameBytes, records, nextPkHash);

            byte[] sig = CryptoManager.lib.getSignature(newChecksum, wallet1.getPrimaryPrivateKey());

            ulong expirationBlockHeight;

            var toEntry = RegisteredNamesTransactions.createUpdateRecordToEntry(nameBytes, records, nextPkHash, pkSig, sig);
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

            newChecksum = regNames.calculateRegNameChecksumFromUpdatedDataRecords(nameBytes, records, nextPkHash);

            sig = CryptoManager.lib.getSignature(newChecksum, wallet3.getPrimaryPrivateKey());

            toEntry = RegisteredNamesTransactions.createUpdateRecordToEntry(nameBytes, records, nextPkHash, pkSig, sig);
            tx = createDummyTransaction(toEntry);
            Assert.IsTrue(regNames.verifyTransaction(tx));
            Assert.IsTrue(regNames.applyTransaction(tx, 1, out expirationBlockHeight).isApplySuccess);


            rnRecord = regNames.getName(nameBytes);
            Assert.IsTrue(nameBytes.SequenceEqual(rnRecord.name), "rnur.name != rnRecord.name");
            Assert.AreEqual(2, rnRecord.dataRecords.Count);

            Assert.IsTrue(newChecksum.SequenceEqual(rnRecord.calculateChecksum()), "newChecksum != rnRecord.checksum");
            Assert.IsTrue(nextPkHash.SequenceEqual(rnRecord.nextPkHash), "nextPkHash != rnRecord.nextPkHash");
            Assert.IsTrue(pkSig.pubKey.SequenceEqual(rnRecord.signaturePk), "pkSig != rnRecord.signaturePk");
            Assert.IsTrue(sig.SequenceEqual(rnRecord.signature), "sig != rnRecord.signature");

            regNames.revertTransaction(1);

            rnRecord = regNames.getName(nameBytes);
            Assert.AreEqual((ulong)1, regNames.count());
            Assert.AreEqual(0, rnRecord.dataRecords.Count);

            assertRecords(origRecord, rnRecord);
        }

        [TestMethod]
        public void UpdateRecord_DeleteRecord()
        {
            string name = "test";
            byte[] nameBytes = IxiNameUtils.encodeIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = 1;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
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
                new RegisteredNameDataRecord(recordName, 1, data),
                new RegisteredNameDataRecord(recordName, 1, data)
            };

            var newChecksum = regNames.calculateRegNameChecksumFromUpdatedDataRecords(nameBytes, records, nextPkHash);

            byte[] sig = CryptoManager.lib.getSignature(newChecksum, wallet1.getPrimaryPrivateKey());

            ulong expirationBlockHeight;

            var toEntry = RegisteredNamesTransactions.createUpdateRecordToEntry(nameBytes, records, nextPkHash, pkSig, sig);
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
                new RegisteredNameDataRecord(recordName, 1, null, recordChecksumToRemove)
            };

            newChecksum = regNames.calculateRegNameChecksumFromUpdatedDataRecords(nameBytes, records, nextPkHash);

            sig = CryptoManager.lib.getSignature(newChecksum, wallet3.getPrimaryPrivateKey());

            toEntry = RegisteredNamesTransactions.createUpdateRecordToEntry(nameBytes, records, nextPkHash, pkSig, sig);
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

            regNames.revertTransaction(1);

            rnRecord = regNames.getName(nameBytes);
            Assert.AreEqual((ulong)1, regNames.count());
            Assert.AreEqual(0, rnRecord.dataRecords.Count);

            assertRecords(origRecord, rnRecord);
        }

        [TestMethod]
        public void UpdateRecord_TooLarge()
        {
            string name = "test";
            byte[] nameBytes = IxiNameUtils.encodeIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = 1;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
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

            var newChecksum = regNames.calculateRegNameChecksumFromUpdatedDataRecords(nameBytes, records, nextPkHash);

            byte[] sig = CryptoManager.lib.getSignature(newChecksum, wallet1.getPrimaryPrivateKey());

            ulong expirationBlockHeight;

            var toEntry = RegisteredNamesTransactions.createUpdateRecordToEntry(nameBytes, records, nextPkHash, pkSig, sig);
            Transaction tx = createDummyTransaction(toEntry);
            Assert.IsTrue(regNames.verifyTransaction(tx));
            Assert.IsFalse(regNames.applyTransaction(tx, 1, out expirationBlockHeight).isApplySuccess);

            rnRecord = regNames.getName(nameBytes);
            Assert.IsTrue(nameBytes.SequenceEqual(rnRecord.name), "rnur.name != rnRecord.name");
            Assert.AreEqual(0, rnRecord.dataRecords.Count);

            assertRecords(origRecord, rnRecord);

            regNames.revertTransaction(1);

            rnRecord = regNames.getName(nameBytes);
            Assert.AreEqual((ulong)1, regNames.count());
            Assert.AreEqual(0, rnRecord.dataRecords.Count);

            assertRecords(origRecord, rnRecord);
        }

        [TestMethod]
        public void UpdateRecord_IncorrectKey()
        {
            string name = "test";
            byte[] nameBytes = IxiNameUtils.encodeIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = 1;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
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

            var newChecksum = regNames.calculateRegNameChecksumFromUpdatedDataRecords(nameBytes, records, nextPkHash);

            byte[] sig = CryptoManager.lib.getSignature(newChecksum, wallet2.getPrimaryPrivateKey());

            ulong expirationBlockHeight;

            var toEntry = RegisteredNamesTransactions.createUpdateRecordToEntry(nameBytes, records, nextPkHash, pkSig, sig);
            Transaction tx = createDummyTransaction(toEntry);
            Assert.IsTrue(regNames.verifyTransaction(tx));
            Assert.IsFalse(regNames.applyTransaction(tx, 1, out expirationBlockHeight).isApplySuccess);

            rnRecord = regNames.getName(nameBytes);
            Assert.IsTrue(nameBytes.SequenceEqual(rnRecord.name), "rnur.name != rnRecord.name");
            Assert.AreEqual(0, rnRecord.dataRecords.Count);

            assertRecords(origRecord, rnRecord);

            regNames.revertTransaction(1);

            rnRecord = regNames.getName(nameBytes);
            Assert.AreEqual((ulong)1, regNames.count());
            Assert.AreEqual(0, rnRecord.dataRecords.Count);

            assertRecords(origRecord, rnRecord);
        }

        [TestMethod]
        public void UpdateRecord_IncorrectSig()
        {
            string name = "test";
            byte[] nameBytes = IxiNameUtils.encodeIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = 1;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
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

            var newChecksum = regNames.calculateRegNameChecksumFromUpdatedDataRecords(nameBytes, records, nextPkHash);

            byte[] sig = CryptoManager.lib.getSignature(newChecksum, wallet2.getPrimaryPrivateKey());

            ulong expirationBlockHeight;

            var toEntry = RegisteredNamesTransactions.createUpdateRecordToEntry(nameBytes, records, nextPkHash, pkSig, sig);
            Transaction tx = createDummyTransaction(toEntry);
            Assert.IsTrue(regNames.verifyTransaction(tx));
            Assert.IsFalse(regNames.applyTransaction(tx, 1, out expirationBlockHeight).isApplySuccess);

            rnRecord = regNames.getName(nameBytes);
            Assert.IsTrue(nameBytes.SequenceEqual(rnRecord.name), "rnur.name != rnRecord.name");
            Assert.AreEqual(0, rnRecord.dataRecords.Count);

            assertRecords(origRecord, rnRecord);

            regNames.revertTransaction(1);

            rnRecord = regNames.getName(nameBytes);
            Assert.AreEqual((ulong)1, regNames.count());
            Assert.AreEqual(0, rnRecord.dataRecords.Count);

            assertRecords(origRecord, rnRecord);
        }

        [TestMethod]
        public void IncreaseCapacity()
        {
            string name = "test";
            byte[] nameBytes = IxiNameUtils.encodeIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = 1;
            uint newCapacity = 2;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
            Address nextPkHash = wallet1.getPrimaryAddress();
            Address recoveryHash = wallet2.getPrimaryAddress();

            registerName(name, registrationTimeInBlocks, capacity, regFee, nextPkHash, recoveryHash);

            var rnRecord = regNames.getName(nameBytes);
            var origRecord = new RegisteredNameRecord(rnRecord);

            regNames.beginTransaction(1);

            nextPkHash = wallet3.getPrimaryAddress();
            Address pkSig = new Address(wallet1.getPrimaryPublicKey());

            rnRecord.setCapacity(newCapacity, nextPkHash, pkSig.pubKey, null);
            var newChecksum = rnRecord.calculateChecksum();

            byte[] sig = CryptoManager.lib.getSignature(newChecksum, wallet1.getPrimaryPrivateKey());

            ulong expirationBlockHeight;

            var toEntry = RegisteredNamesTransactions.createChangeCapacityToEntry(nameBytes, newCapacity, nextPkHash, pkSig, sig, regFee);
            Transaction tx = createDummyTransaction(toEntry);
            Assert.IsTrue(regNames.verifyTransaction(tx));
            Assert.IsTrue(regNames.applyTransaction(tx, 1, out expirationBlockHeight).isApplySuccess);

            rnRecord = regNames.getName(nameBytes);
            Assert.IsTrue(nameBytes.SequenceEqual(rnRecord.name), "rnc.name != rnRecord.name");

            Assert.AreEqual(newCapacity, rnRecord.capacity);

            Assert.IsTrue(newChecksum.SequenceEqual(rnRecord.calculateChecksum()), "newChecksum != rnRecord.checksum");
            Assert.IsTrue(nextPkHash.SequenceEqual(rnRecord.nextPkHash), "nextPkHash != rnRecord.nextPkHash");
            Assert.IsTrue(pkSig.pubKey.SequenceEqual(rnRecord.signaturePk), "pkSig != rnRecord.signaturePk");
            Assert.IsTrue(sig.SequenceEqual(rnRecord.signature), "sig != rnRecord.signature");

            Assert.AreEqual(regFee * 2, regNames.getRewardPool());

            regNames.revertTransaction(1);

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
            byte[] nameBytes = IxiNameUtils.encodeIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = 1;
            uint newCapacity = 2;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
            Address nextPkHash = wallet1.getPrimaryAddress();
            Address recoveryHash = wallet2.getPrimaryAddress();

            registerName(name, registrationTimeInBlocks, capacity, regFee, nextPkHash, recoveryHash);

            var rnRecord = regNames.getName(nameBytes);
            var origRecord = new RegisteredNameRecord(rnRecord);

            regNames.beginTransaction(1);

            nextPkHash = wallet3.getPrimaryAddress();
            Address pkSig = new Address(wallet2.getPrimaryPublicKey());

            rnRecord.setCapacity(newCapacity, nextPkHash, pkSig.pubKey, null);
            var newChecksum = rnRecord.calculateChecksum();

            byte[] sig = CryptoManager.lib.getSignature(newChecksum, wallet2.getPrimaryPrivateKey());

            ulong expirationBlockHeight;

            var toEntry = RegisteredNamesTransactions.createChangeCapacityToEntry(nameBytes, newCapacity, nextPkHash, pkSig, sig, regFee);
            Transaction tx = createDummyTransaction(toEntry);
            Assert.IsFalse(regNames.verifyTransaction(tx));
            Assert.IsFalse(regNames.applyTransaction(tx, 1, out expirationBlockHeight).isApplySuccess);

            rnRecord = regNames.getName(nameBytes);
            Assert.AreEqual(regFee, regNames.getRewardPool());

            assertRecords(origRecord, rnRecord);

            regNames.revertTransaction(1);

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
            byte[] nameBytes = IxiNameUtils.encodeIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = 1;
            uint newCapacity = 2;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
            Address nextPkHash = wallet1.getPrimaryAddress();
            Address recoveryHash = wallet2.getPrimaryAddress();

            registerName(name, registrationTimeInBlocks, capacity, regFee, nextPkHash, recoveryHash);

            var rnRecord = regNames.getName(nameBytes);
            var origRecord = new RegisteredNameRecord(rnRecord);

            regNames.beginTransaction(1);

            nextPkHash = wallet3.getPrimaryAddress();
            Address pkSig = new Address(wallet1.getPrimaryPublicKey());

            rnRecord.setCapacity(newCapacity, nextPkHash, pkSig.pubKey, null);
            var newChecksum = rnRecord.calculateChecksum();

            byte[] sig = CryptoManager.lib.getSignature(newChecksum, wallet2.getPrimaryPrivateKey());

            ulong expirationBlockHeight;

            var toEntry = RegisteredNamesTransactions.createChangeCapacityToEntry(nameBytes, newCapacity, nextPkHash, pkSig, sig, regFee);
            Transaction tx = createDummyTransaction(toEntry);
            Assert.IsFalse(regNames.verifyTransaction(tx));
            Assert.IsFalse(regNames.applyTransaction(tx, 1, out expirationBlockHeight).isApplySuccess);

            rnRecord = regNames.getName(nameBytes);
            Assert.AreEqual(regFee, regNames.getRewardPool());

            assertRecords(origRecord, rnRecord);

            regNames.revertTransaction(1);

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
            byte[] nameBytes = IxiNameUtils.encodeIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = 1;
            uint newCapacity = 2;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
            Address nextPkHash = wallet1.getPrimaryAddress();
            Address recoveryHash = wallet2.getPrimaryAddress();

            registerName(name, registrationTimeInBlocks, capacity, regFee, nextPkHash, recoveryHash);

            var rnRecord = regNames.getName(nameBytes);
            var origRecord = new RegisteredNameRecord(rnRecord);

            regNames.beginTransaction(1);

            nextPkHash = wallet3.getPrimaryAddress();
            Address pkSig = new Address(wallet1.getPrimaryPublicKey());

            rnRecord.setCapacity(newCapacity, nextPkHash, pkSig.pubKey, null);
            var newChecksum = rnRecord.calculateChecksum();

            byte[] sig = CryptoManager.lib.getSignature(newChecksum, wallet1.getPrimaryPrivateKey());

            ulong expirationBlockHeight;

            var toEntry = RegisteredNamesTransactions.createChangeCapacityToEntry(nameBytes, newCapacity, nextPkHash, pkSig, sig, regFee / 2);
            Transaction tx = createDummyTransaction(toEntry);
            Assert.IsFalse(regNames.verifyTransaction(tx));
            Assert.IsTrue(regNames.applyTransaction(tx, 1, out expirationBlockHeight).isApplySuccess);

            rnRecord = regNames.getName(nameBytes);
            Assert.IsTrue(nameBytes.SequenceEqual(rnRecord.name), "rnc.name != rnRecord.name");

            Assert.AreEqual(newCapacity, rnRecord.capacity);

            Assert.IsTrue(newChecksum.SequenceEqual(rnRecord.calculateChecksum()), "newChecksum != rnRecord.checksum");
            Assert.IsTrue(nextPkHash.SequenceEqual(rnRecord.nextPkHash), "nextPkHash != rnRecord.nextPkHash");
            Assert.IsTrue(pkSig.pubKey.SequenceEqual(rnRecord.signaturePk), "pkSig != rnRecord.signaturePk");
            Assert.IsTrue(sig.SequenceEqual(rnRecord.signature), "sig != rnRecord.signature");

            Assert.AreEqual(750, regNames.getRewardPool());

            regNames.revertTransaction(1);

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
            byte[] nameBytes = IxiNameUtils.encodeIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = 2;
            uint newCapacity = 1;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks) * 2;
            Address nextPkHash = wallet1.getPrimaryAddress();
            Address recoveryHash = wallet2.getPrimaryAddress();

            registerName(name, registrationTimeInBlocks, capacity, regFee, nextPkHash, recoveryHash);

            var rnRecord = regNames.getName(nameBytes);
            var origRecord = new RegisteredNameRecord(rnRecord);

            regNames.beginTransaction(1);

            nextPkHash = wallet3.getPrimaryAddress();
            Address pkSig = new Address(wallet1.getPrimaryPublicKey());

            rnRecord.setCapacity(newCapacity, nextPkHash, pkSig.pubKey, null);
            var newChecksum = rnRecord.calculateChecksum();

            byte[] sig = CryptoManager.lib.getSignature(newChecksum, wallet1.getPrimaryPrivateKey());

            ulong expirationBlockHeight;

            var toEntry = RegisteredNamesTransactions.createChangeCapacityToEntry(nameBytes, newCapacity, nextPkHash, pkSig, sig, 0);
            Transaction tx = createDummyTransaction(toEntry);
            Assert.IsTrue(regNames.verifyTransaction(tx));
            Assert.IsTrue(regNames.applyTransaction(tx, 1, out expirationBlockHeight).isApplySuccess);

            rnRecord = regNames.getName(nameBytes);
            Assert.IsTrue(nameBytes.SequenceEqual(rnRecord.name), "rnc.name != rnRecord.name");

            Assert.AreEqual(newCapacity, rnRecord.capacity);

            Assert.IsTrue(newChecksum.SequenceEqual(rnRecord.calculateChecksum()), "newChecksum != rnRecord.checksum");
            Assert.IsTrue(nextPkHash.SequenceEqual(rnRecord.nextPkHash), "nextPkHash != rnRecord.nextPkHash");
            Assert.IsTrue(pkSig.pubKey.SequenceEqual(rnRecord.signaturePk), "pkSig != rnRecord.signaturePk");
            Assert.IsTrue(sig.SequenceEqual(rnRecord.signature), "sig != rnRecord.signature");

            Assert.AreEqual(regFee, regNames.getRewardPool());

            regNames.revertTransaction(1);

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
            byte[] nameBytes = IxiNameUtils.encodeIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = 2;
            uint newCapacity = 1;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks) * 2;
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

            var newChecksum = regNames.calculateRegNameChecksumFromUpdatedDataRecords(nameBytes, records, nextPkHash);

            byte[] sig = CryptoManager.lib.getSignature(newChecksum, wallet1.getPrimaryPrivateKey());

            var rnur = new RegNameUpdateRecord(nameBytes, records, nextPkHash, pkSig, sig);
            Assert.IsTrue(regNames.updateRecords(rnur));

            var rnRecord = regNames.getName(nameBytes);
            var origRecord = new RegisteredNameRecord(rnRecord);

            regNames.beginTransaction(1);

            // Set Capacity
            rnRecord.setCapacity(newCapacity, wallet1.getPrimaryAddress(), new Address(wallet3.getPrimaryPublicKey()).pubKey, sig);
            var newCapacityChecksum = rnRecord.calculateChecksum();

            var capacitySig = CryptoManager.lib.getSignature(newCapacityChecksum, wallet3.getPrimaryPrivateKey());

            ulong expirationBlockHeight;

            var toEntry = RegisteredNamesTransactions.createChangeCapacityToEntry(nameBytes, newCapacity, wallet1.getPrimaryAddress(), pkSig, sig, 0);
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

            regNames.revertTransaction(1);

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
            byte[] nameBytes = IxiNameUtils.encodeIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = 1;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
            Address nextPkHash = wallet1.getPrimaryAddress();
            Address recoveryHash = wallet2.getPrimaryAddress();

            registerName(name, registrationTimeInBlocks, capacity, regFee, nextPkHash, recoveryHash);

            var rnRecord = regNames.getName(nameBytes);
            var origRecord = new RegisteredNameRecord(rnRecord);

            regNames.beginTransaction(1);

            nextPkHash = wallet3.getPrimaryAddress();
            Address pkSig = new Address(wallet2.getPrimaryPublicKey());
            Address newRecoveryHash = wallet4.getPrimaryAddress();

            rnRecord.setRecoveryHash(newRecoveryHash, nextPkHash, pkSig.pubKey, null);
            var newChecksum = rnRecord.calculateChecksum();

            byte[] sig = CryptoManager.lib.getSignature(newChecksum, wallet2.getPrimaryPrivateKey());

            ulong expirationBlockHeight;

            var toEntry = RegisteredNamesTransactions.createRecoverToEntry(nameBytes, nextPkHash, newRecoveryHash, pkSig, sig);
            Transaction tx = createDummyTransaction(toEntry);
            Assert.IsTrue(regNames.verifyTransaction(tx));
            Assert.IsTrue(regNames.applyTransaction(tx, 1, out expirationBlockHeight).isApplySuccess);

            rnRecord = regNames.getName(nameBytes);
            Assert.IsTrue(nameBytes.SequenceEqual(rnRecord.name), "rnRec.name != rnRecord.name");

            Assert.IsTrue(newRecoveryHash.SequenceEqual(rnRecord.recoveryHash), "newRecoveryHash != rnRecord.recoveryHash");

            Assert.IsTrue(newChecksum.SequenceEqual(rnRecord.calculateChecksum()), "newChecksum != rnRecord.checksum");
            Assert.IsTrue(nextPkHash.SequenceEqual(rnRecord.nextPkHash), "nextPkHash != rnRecord.nextPkHash");
            Assert.IsTrue(pkSig.pubKey.SequenceEqual(rnRecord.signaturePk), "pkSig != rnRecord.signaturePk");
            Assert.IsTrue(sig.SequenceEqual(rnRecord.signature), "sig != rnRecord.signature");

            regNames.revertTransaction(1);

            rnRecord = regNames.getName(nameBytes);
            Assert.AreEqual((ulong)1, regNames.count());

            assertRecords(origRecord, rnRecord);
        }

        [TestMethod]
        public void RecoverName_IncorrectKey()
        {
            string name = "test";
            byte[] nameBytes = IxiNameUtils.encodeIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = 1;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
            Address nextPkHash = wallet1.getPrimaryAddress();
            Address recoveryHash = wallet2.getPrimaryAddress();

            registerName(name, registrationTimeInBlocks, capacity, regFee, nextPkHash, recoveryHash);

            var rnRecord = regNames.getName(nameBytes);
            var origRecord = new RegisteredNameRecord(rnRecord);

            regNames.beginTransaction(1);

            nextPkHash = wallet3.getPrimaryAddress();
            Address pkSig = new Address(wallet1.getPrimaryPublicKey());
            Address newRecoveryHash = wallet4.getPrimaryAddress();

            rnRecord.setRecoveryHash(newRecoveryHash, nextPkHash, pkSig.pubKey, null);
            var newChecksum = rnRecord.calculateChecksum();

            byte[] sig = CryptoManager.lib.getSignature(newChecksum, wallet2.getPrimaryPrivateKey());

            ulong expirationBlockHeight;

            var toEntry = RegisteredNamesTransactions.createRecoverToEntry(nameBytes, nextPkHash, newRecoveryHash, pkSig, sig);
            Transaction tx = createDummyTransaction(toEntry);
            Assert.IsFalse(regNames.verifyTransaction(tx));
            Assert.IsFalse(regNames.applyTransaction(tx, 1, out expirationBlockHeight).isApplySuccess);

            rnRecord = regNames.getName(nameBytes);

            assertRecords(origRecord, rnRecord);

            regNames.revertTransaction(1);

            rnRecord = regNames.getName(nameBytes);
            Assert.AreEqual((ulong)1, regNames.count());

            assertRecords(origRecord, rnRecord);
        }

        [TestMethod]
        public void RecoverName_IncorrectSig()
        {
            string name = "test";
            byte[] nameBytes = IxiNameUtils.encodeIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = 1;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
            Address nextPkHash = wallet1.getPrimaryAddress();
            Address recoveryHash = wallet2.getPrimaryAddress();

            registerName(name, registrationTimeInBlocks, capacity, regFee, nextPkHash, recoveryHash);

            var rnRecord = regNames.getName(nameBytes);
            var origRecord = new RegisteredNameRecord(rnRecord);

            regNames.beginTransaction(1);

            nextPkHash = wallet3.getPrimaryAddress();
            Address pkSig = new Address(wallet2.getPrimaryPublicKey());
            Address newRecoveryHash = wallet4.getPrimaryAddress();

            rnRecord.setRecoveryHash(newRecoveryHash, nextPkHash, pkSig.pubKey, null);
            var newChecksum = rnRecord.calculateChecksum();

            byte[] sig = CryptoManager.lib.getSignature(newChecksum, wallet1.getPrimaryPrivateKey());

            ulong expirationBlockHeight;

            var toEntry = RegisteredNamesTransactions.createRecoverToEntry(nameBytes, nextPkHash, newRecoveryHash, pkSig, sig);
            Transaction tx = createDummyTransaction(toEntry);
            Assert.IsFalse(regNames.verifyTransaction(tx));
            Assert.IsFalse(regNames.applyTransaction(tx, 1, out expirationBlockHeight).isApplySuccess);

            rnRecord = regNames.getName(nameBytes);

            assertRecords(origRecord, rnRecord);

            regNames.revertTransaction(1);

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
            byte[] nameBytes = IxiNameUtils.encodeIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = 1;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
            Address nextPkHash = wallet1.getPrimaryAddress();
            Address recoveryHash = wallet2.getPrimaryAddress();
            IxiNumber subnameFee = 100000;
            Address subnameFeeRecipient = wallet4.getPrimaryAddress();

            registerName(name, registrationTimeInBlocks, capacity, regFee, nextPkHash, recoveryHash);

            var rnRecord = regNames.getName(nameBytes);
            var origRecord = new RegisteredNameRecord(rnRecord);

            regNames.beginTransaction(1);

            nextPkHash = wallet3.getPrimaryAddress();
            allowSubnames(nameBytes, true, subnameFee, subnameFeeRecipient, nextPkHash, wallet1);

            regNames.revertTransaction(1);

            rnRecord = regNames.getName(nameBytes);
            Assert.AreEqual((ulong)1, regNames.count());
            Assert.AreEqual(0, rnRecord.dataRecords.Count);

            assertRecords(origRecord, rnRecord);
        }

        [TestMethod]
        public void Subname_Top_AddRecord()
        {
            string name = "test";
            byte[] nameBytes = IxiNameUtils.encodeIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = 1;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
            Address nextPkHash = wallet1.getPrimaryAddress();
            Address recoveryHash = wallet2.getPrimaryAddress();
            IxiNumber subnameFee = 100000;
            Address subnameFeeRecipient = wallet4.getPrimaryAddress();

            registerName(name, registrationTimeInBlocks, capacity, regFee, nextPkHash, recoveryHash);

            nextPkHash = wallet3.getPrimaryAddress();
            allowSubnames(nameBytes, true, subnameFee, subnameFeeRecipient, nextPkHash, wallet1);

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

            var newChecksum = regNames.calculateRegNameChecksumFromUpdatedDataRecords(nameBytes, records, nextPkHash);

            Address pkSig = new Address(wallet3.getPrimaryPublicKey());
            byte[] sig = CryptoManager.lib.getSignature(newChecksum, wallet3.getPrimaryPrivateKey());

            ulong expirationBlockHeight;

            var toEntry = RegisteredNamesTransactions.createUpdateRecordToEntry(nameBytes, records, nextPkHash, pkSig, sig);
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

            regNames.revertTransaction(1);

            rnRecord = regNames.getName(nameBytes);
            Assert.AreEqual((ulong)1, regNames.count());
            Assert.AreEqual(0, rnRecord.dataRecords.Count);

            assertRecords(origRecord, rnRecord);
        }

        [TestMethod]
        public void Subname_Top_AddRecord_InvalidKey()
        {
            string name = "test";
            byte[] nameBytes = IxiNameUtils.encodeIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = 1;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
            Address nextPkHash = wallet1.getPrimaryAddress();
            Address recoveryHash = wallet2.getPrimaryAddress();
            IxiNumber subnameFee = 100000;
            Address subnameFeeRecipient = wallet4.getPrimaryAddress();

            registerName(name, registrationTimeInBlocks, capacity, regFee, nextPkHash, recoveryHash);

            nextPkHash = wallet3.getPrimaryAddress();
            allowSubnames(nameBytes, true, subnameFee, subnameFeeRecipient, nextPkHash, wallet1);

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

            var newChecksum = regNames.calculateRegNameChecksumFromUpdatedDataRecords(nameBytes, records, nextPkHash);

            Address pkSig = new Address(wallet3.getPrimaryPublicKey());
            byte[] sig = CryptoManager.lib.getSignature(newChecksum, wallet3.getPrimaryPrivateKey());

            ulong expirationBlockHeight;

            var toEntry = RegisteredNamesTransactions.createUpdateRecordToEntry(nameBytes, records, nextPkHash, pkSig, sig);
            Transaction tx = createDummyTransaction(toEntry);
            Assert.IsTrue(regNames.verifyTransaction(tx));
            Assert.IsFalse(regNames.applyTransaction(tx, 1, out expirationBlockHeight).isApplySuccess);

            rnRecord = regNames.getName(nameBytes);
            Assert.IsTrue(nameBytes.SequenceEqual(rnRecord.name), "rnur.name != rnRecord.name");
            Assert.AreEqual(0, rnRecord.dataRecords.Count);

            assertRecords(origRecord, rnRecord);

            regNames.revertTransaction(1);

            rnRecord = regNames.getName(nameBytes);
            Assert.AreEqual((ulong)1, regNames.count());
            Assert.AreEqual(0, rnRecord.dataRecords.Count);

            assertRecords(origRecord, rnRecord);
        }

        [TestMethod]
        public void Subname_Enable_Top_IncorrectKey()
        {
            string name = "test";
            byte[] nameBytes = IxiNameUtils.encodeIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = 1;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
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

            rnRecord.setAllowSubnames(true, subnameFee, subnameFeeRecipient, nextPkHash, pkSig.pubKey, null);
            var newChecksum = rnRecord.calculateChecksum();

            byte[] sig = CryptoManager.lib.getSignature(newChecksum, wallet1.getPrimaryPrivateKey());

            ulong expirationBlockHeight;

            var toEntry = RegisteredNamesTransactions.createToggleAllowSubnamesToEntry(nameBytes, true, subnameFee, subnameFeeRecipient, nextPkHash, pkSig, sig);
            Transaction tx = createDummyTransaction(toEntry);
            Assert.IsFalse(regNames.verifyTransaction(tx));
            Assert.IsFalse(regNames.applyTransaction(tx, 1, out expirationBlockHeight).isApplySuccess);

            rnRecord = regNames.getName(nameBytes);
            assertRecords(origRecord, rnRecord);

            regNames.revertTransaction(1);

            rnRecord = regNames.getName(nameBytes);
            Assert.AreEqual((ulong)1, regNames.count());
            Assert.AreEqual(0, rnRecord.dataRecords.Count);

            assertRecords(origRecord, rnRecord);
        }

        [TestMethod]
        public void Subname_Enable_Top_IncorrectSig()
        {
            string name = "test";
            byte[] nameBytes = IxiNameUtils.encodeIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = 1;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
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

            rnRecord.setAllowSubnames(true, subnameFee, subnameFeeRecipient, nextPkHash, pkSig.pubKey, null);
            var newChecksum = rnRecord.calculateChecksum();

            byte[] sig = CryptoManager.lib.getSignature(newChecksum, wallet4.getPrimaryPrivateKey());

            ulong expirationBlockHeight;

            var toEntry = RegisteredNamesTransactions.createToggleAllowSubnamesToEntry(nameBytes, true, subnameFee, subnameFeeRecipient, nextPkHash, pkSig, sig);
            Transaction tx = createDummyTransaction(toEntry);
            Assert.IsFalse(regNames.verifyTransaction(tx));
            Assert.IsFalse(regNames.applyTransaction(tx, 1, out expirationBlockHeight).isApplySuccess);

            rnRecord = regNames.getName(nameBytes);
            assertRecords(origRecord, rnRecord);

            regNames.revertTransaction(1);

            rnRecord = regNames.getName(nameBytes);
            Assert.AreEqual((ulong)1, regNames.count());
            Assert.AreEqual(0, rnRecord.dataRecords.Count);

            assertRecords(origRecord, rnRecord);
        }

        private void allowSubnames(byte[] nameBytes, bool allowSubnames, IxiNumber subnameFee, Address subnameFeeRecipient, Address nextPkHash, WalletStorage signingWallet)
        {
            var pkSig = signingWallet.getPrimaryPublicKey();
            var rnRecord = regNames.getName(nameBytes);
            rnRecord.setAllowSubnames(allowSubnames, subnameFee, subnameFeeRecipient, nextPkHash, pkSig, null);
            var newChecksum = rnRecord.calculateChecksum();

            byte[] sig = CryptoManager.lib.getSignature(newChecksum, signingWallet.getPrimaryPrivateKey());

            ulong expirationBlockHeight;

            var toEntry = RegisteredNamesTransactions.createToggleAllowSubnamesToEntry(nameBytes, allowSubnames, subnameFee, subnameFeeRecipient, nextPkHash, new Address(pkSig), sig);
            Transaction tx = createDummyTransaction(toEntry);
            Assert.IsTrue(regNames.verifyTransaction(tx));
            Assert.IsTrue(regNames.applyTransaction(tx, 1, out expirationBlockHeight).isApplySuccess);

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
        }

        [TestMethod]
        public void Subname_Register()
        {
            string name = "test";
            byte[] nameBytes = IxiNameUtils.encodeIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = 1;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
            Address nextPkHash = wallet1.getPrimaryAddress();
            Address recoveryHash = wallet2.getPrimaryAddress();
            IxiNumber subnamePrice = 100000;
            Address subnameFeeRecipient = wallet4.getPrimaryAddress();

            Assert.IsTrue(registerName(name, registrationTimeInBlocks, capacity, regFee, nextPkHash, recoveryHash));

            nextPkHash = wallet3.getPrimaryAddress();
            allowSubnames(nameBytes, true, subnamePrice, subnameFeeRecipient, nextPkHash, wallet1);

            Address subnameNextPkHash = wallet4.getPrimaryAddress();
            Address subnameRecoveryHash = wallet1.getPrimaryAddress();

            var rnRecord = regNames.getName(nameBytes);
            var origRecord = new RegisteredNameRecord(rnRecord);

            regNames.beginTransaction(1);

            // Register subname
            byte[] subnameBytes = IxiNameUtils.encodeIxiName("subNameTest." + name);
            IxiNumber subnameFee = subnamePrice * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);

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

            regNames.revertTransaction(1);

            rnRecord = regNames.getName(nameBytes);
            Assert.AreEqual((ulong)1, regNames.count());
            Assert.AreEqual(0, rnRecord.dataRecords.Count);

            assertRecords(origRecord, rnRecord);
        }

        [TestMethod]
        public void Subname_Extend()
        {
            string name = "test";
            byte[] nameBytes = IxiNameUtils.encodeIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = 1;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
            Address nextPkHash = wallet1.getPrimaryAddress();
            Address recoveryHash = wallet2.getPrimaryAddress();
            IxiNumber subnamePrice = 100000;
            Address subnameFeeRecipient = wallet4.getPrimaryAddress();

            Assert.IsTrue(registerName(name, registrationTimeInBlocks, capacity, regFee, nextPkHash, recoveryHash));

            nextPkHash = wallet3.getPrimaryAddress();
            allowSubnames(nameBytes, true, subnamePrice, subnameFeeRecipient, nextPkHash, wallet1);

            Address subnameNextPkHash = wallet4.getPrimaryAddress();
            Address subnameRecoveryHash = wallet1.getPrimaryAddress();

            var rnRecord = regNames.getName(nameBytes);
            var origRecord = new RegisteredNameRecord(rnRecord);


            // Register subname
            byte[] subnameBytes = IxiNameUtils.encodeIxiName("subNameTest." + name);
            IxiNumber subnameFee = subnamePrice * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);

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

            regNames.revertTransaction(1);

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
            byte[] nameBytes = IxiNameUtils.encodeIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = 1;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
            Address nextPkHash = wallet1.getPrimaryAddress();
            Address recoveryHash = wallet2.getPrimaryAddress();
            IxiNumber subnamePrice = 100000;
            Address subnameFeeRecipient = wallet4.getPrimaryAddress();

            Assert.IsTrue(registerName(name, registrationTimeInBlocks, capacity, regFee, nextPkHash, recoveryHash));

            nextPkHash = wallet3.getPrimaryAddress();
            allowSubnames(nameBytes, true, subnamePrice, subnameFeeRecipient, nextPkHash, wallet1);

            Address subnameNextPkHash = wallet4.getPrimaryAddress();
            Address subnameRecoveryHash = wallet1.getPrimaryAddress();

            var rnRecord = regNames.getName(nameBytes);
            var origRecord = new RegisteredNameRecord(rnRecord);


            // Register subname
            byte[] subnameBytes = IxiNameUtils.encodeIxiName("subNameTest." + name);
            IxiNumber subnameFee = subnamePrice * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);

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

            var nonExistentNameBytes = IxiNameUtils.encodeIxiName("nonExistentName." + name);
            toEntry = RegisteredNamesTransactions.createExtendToEntry(nonExistentNameBytes, registrationTimeInBlocks, regFee);
            tx = createDummyTransaction(toEntry);
            tx.toList.Add(subnameFeeRecipient, new ToEntry(Transaction.maxVersion, subnameFee));
            Assert.IsFalse(regNames.verifyTransaction(tx));
            Assert.IsFalse(regNames.applyTransaction(tx, curBlockHeight, out expirationBlockHeight).isApplySuccess);

            rnRecord = regNames.getName(nonExistentNameBytes);
            Assert.IsNull(rnRecord);

            Assert.AreEqual(regFee * 2, regNames.getRewardPool());

            regNames.revertTransaction(1);

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
            byte[] nameBytes = IxiNameUtils.encodeIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = 1;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
            Address nextPkHash = wallet1.getPrimaryAddress();
            Address recoveryHash = wallet2.getPrimaryAddress();
            IxiNumber subnamePrice = 100000;
            Address subnameFeeRecipient = wallet4.getPrimaryAddress();

            Assert.IsTrue(registerName(name, registrationTimeInBlocks, capacity, regFee, nextPkHash, recoveryHash));

            nextPkHash = wallet3.getPrimaryAddress();
            allowSubnames(nameBytes, true, subnamePrice, subnameFeeRecipient, nextPkHash, wallet1);

            Address subnameNextPkHash = wallet4.getPrimaryAddress();
            Address subnameRecoveryHash = wallet1.getPrimaryAddress();

            var rnRecord = regNames.getName(nameBytes);
            var origRecord = new RegisteredNameRecord(rnRecord);


            // Register subname
            byte[] subnameBytes = IxiNameUtils.encodeIxiName("subNameTest." + name);
            IxiNumber subnameFee = subnamePrice * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);

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

            regNames.revertTransaction(1);

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
            byte[] nameBytes = IxiNameUtils.encodeIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = 1;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
            Address nextPkHash = wallet1.getPrimaryAddress();
            Address recoveryHash = wallet2.getPrimaryAddress();
            IxiNumber subnamePrice = 100000;
            Address subnameFeeRecipient = wallet4.getPrimaryAddress();

            Assert.IsTrue(registerName(name, registrationTimeInBlocks, capacity, regFee, nextPkHash, recoveryHash));

            nextPkHash = wallet3.getPrimaryAddress();
            allowSubnames(nameBytes, true, subnamePrice, subnameFeeRecipient, nextPkHash, wallet1);

            Address subnameNextPkHash = wallet4.getPrimaryAddress();
            Address subnameRecoveryHash = wallet1.getPrimaryAddress();

            var rnRecord = regNames.getName(nameBytes);
            var origRecord = new RegisteredNameRecord(rnRecord);


            // Register subname
            byte[] subnameBytes = IxiNameUtils.encodeIxiName("subNameTest." + name);
            IxiNumber subnameFee = subnamePrice * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);

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

            regNames.revertTransaction(1);

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
            byte[] nameBytes = IxiNameUtils.encodeIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = 1;
            uint newCapacity = 2;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
            Address nextPkHash = wallet1.getPrimaryAddress();
            Address recoveryHash = wallet2.getPrimaryAddress();
            IxiNumber subnamePrice = 100000;
            Address subnameFeeRecipient = wallet4.getPrimaryAddress();

            Assert.IsTrue(registerName(name, registrationTimeInBlocks, capacity, regFee, nextPkHash, recoveryHash));

            nextPkHash = wallet3.getPrimaryAddress();
            allowSubnames(nameBytes, true, subnamePrice, subnameFeeRecipient, nextPkHash, wallet1);

            Address subnameNextPkHash = wallet4.getPrimaryAddress();
            Address subnameRecoveryHash = wallet1.getPrimaryAddress();

            var rnRecord = regNames.getName(nameBytes);
            var origRecord = new RegisteredNameRecord(rnRecord);


            // Register subname
            byte[] subnameBytes = IxiNameUtils.encodeIxiName("subNameTest." + name);
            IxiNumber subnameFee = subnamePrice * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);

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

            rnRecord.setCapacity(newCapacity, subnameNextPkHash, subnameSigPk.pubKey, null);
            var newChecksum = rnRecord.calculateChecksum();

            byte[] subnameSig = CryptoManager.lib.getSignature(newChecksum, wallet4.getPrimaryPrivateKey());

            toEntry = RegisteredNamesTransactions.createChangeCapacityToEntry(subnameBytes, newCapacity, subnameNextPkHash, subnameSigPk, subnameSig, regFee);
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

            regNames.revertTransaction(1);

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
            byte[] nameBytes = IxiNameUtils.encodeIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = 1;
            uint newCapacity = 2;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
            Address nextPkHash = wallet1.getPrimaryAddress();
            Address recoveryHash = wallet2.getPrimaryAddress();
            IxiNumber subnamePrice = 100000;
            Address subnameFeeRecipient = wallet4.getPrimaryAddress();

            Assert.IsTrue(registerName(name, registrationTimeInBlocks, capacity, regFee, nextPkHash, recoveryHash));

            nextPkHash = wallet3.getPrimaryAddress();
            allowSubnames(nameBytes, true, subnamePrice, subnameFeeRecipient, nextPkHash, wallet1);

            Address subnameNextPkHash = wallet4.getPrimaryAddress();
            Address subnameRecoveryHash = wallet1.getPrimaryAddress();

            var rnRecord = regNames.getName(nameBytes);
            var origRecord = new RegisteredNameRecord(rnRecord);


            // Register subname
            byte[] subnameBytes = IxiNameUtils.encodeIxiName("subNameTest." + name);
            IxiNumber subnameFee = subnamePrice * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);

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

            rnRecord.setCapacity(newCapacity, subnameNextPkHash, subnameSigPk.pubKey, null);
            var newChecksum = rnRecord.calculateChecksum();

            byte[] subnameSig = CryptoManager.lib.getSignature(newChecksum, wallet4.getPrimaryPrivateKey());

            toEntry = RegisteredNamesTransactions.createChangeCapacityToEntry(subnameBytes, newCapacity, subnameNextPkHash, subnameSigPk, subnameSig, regFee);
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

            regNames.revertTransaction(1);

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
            byte[] nameBytes = IxiNameUtils.encodeIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = 1;
            uint newCapacity = 2;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
            Address nextPkHash = wallet1.getPrimaryAddress();
            Address recoveryHash = wallet2.getPrimaryAddress();
            IxiNumber subnamePrice = 100000;
            Address subnameFeeRecipient = wallet4.getPrimaryAddress();

            Assert.IsTrue(registerName(name, registrationTimeInBlocks, capacity, regFee, nextPkHash, recoveryHash));

            nextPkHash = wallet3.getPrimaryAddress();
            allowSubnames(nameBytes, true, subnamePrice, subnameFeeRecipient, nextPkHash, wallet1);

            Address subnameNextPkHash = wallet4.getPrimaryAddress();
            Address subnameRecoveryHash = wallet1.getPrimaryAddress();

            var rnRecord = regNames.getName(nameBytes);
            var origRecord = new RegisteredNameRecord(rnRecord);


            // Register subname
            byte[] subnameBytes = IxiNameUtils.encodeIxiName("subNameTest." + name);
            IxiNumber subnameFee = subnamePrice * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);

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

            rnRecord.setCapacity(newCapacity, subnameNextPkHash, subnameSigPk.pubKey, null);
            var newChecksum = rnRecord.calculateChecksum();

            byte[] subnameSig = CryptoManager.lib.getSignature(newChecksum, wallet4.getPrimaryPrivateKey());

            toEntry = RegisteredNamesTransactions.createChangeCapacityToEntry(subnameBytes, newCapacity, subnameNextPkHash, subnameSigPk, subnameSig, regFee);
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

            regNames.revertTransaction(1);

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
            byte[] nameBytes = IxiNameUtils.encodeIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = 1;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
            Address nextPkHash = wallet1.getPrimaryAddress();
            Address recoveryHash = wallet2.getPrimaryAddress();
            IxiNumber subnamePrice = 100000;
            Address subnameFeeRecipient = wallet4.getPrimaryAddress();

            Assert.IsTrue(registerName(name, registrationTimeInBlocks, capacity, regFee, nextPkHash, recoveryHash));

            nextPkHash = wallet3.getPrimaryAddress();
            allowSubnames(nameBytes, true, subnamePrice, subnameFeeRecipient, nextPkHash, wallet1);

            Address subnameNextPkHash = wallet4.getPrimaryAddress();
            Address subnameRecoveryHash = wallet1.getPrimaryAddress();

            var rnRecord = regNames.getName(nameBytes);
            var origRecord = new RegisteredNameRecord(rnRecord);

            regNames.beginTransaction(1);

            // Register subname
            byte[] subnameBytes = IxiNameUtils.encodeIxiName("subNameTest." + name);
            IxiNumber subnameFee = subnamePrice * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks) * 2;

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

            regNames.revertTransaction(1);

            rnRecord = regNames.getName(nameBytes);
            Assert.AreEqual((ulong)1, regNames.count());
            Assert.AreEqual(0, rnRecord.dataRecords.Count);

            assertRecords(origRecord, rnRecord);
        }

        [TestMethod]
        public void Subname_Register_NoTopName()
        {
            string name = "test";
            byte[] nameBytes = IxiNameUtils.encodeIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = 1;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
            Address nextPkHash = wallet1.getPrimaryAddress();
            Address recoveryHash = wallet2.getPrimaryAddress();
            IxiNumber subnameFee = 100000;
            Address subnameFeeRecipient = wallet4.getPrimaryAddress();

            Assert.IsTrue(registerName(name, registrationTimeInBlocks, capacity, regFee, nextPkHash, recoveryHash));

            nextPkHash = wallet3.getPrimaryAddress();
            allowSubnames(nameBytes, true, subnameFee, subnameFeeRecipient, nextPkHash, wallet1);

            Address subnameNextPkHash = wallet4.getPrimaryAddress();
            Address subnameRecoveryHash = wallet1.getPrimaryAddress();

            var rnRecord = regNames.getName(nameBytes);
            var origRecord = new RegisteredNameRecord(rnRecord);

            regNames.beginTransaction(1);

            // Register subname
            byte[] subnameBytes = IxiNameUtils.encodeIxiName("subNameTest." + name + "1");

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


            regNames.revertTransaction(1);

            rnRecord = regNames.getName(nameBytes);
            Assert.AreEqual((ulong)1, regNames.count());
            Assert.AreEqual(0, rnRecord.dataRecords.Count);

            assertRecords(origRecord, rnRecord);
        }

        [TestMethod]
        public void Subname_Register_TopNameDoesntAllowSubnames()
        {
            string name = "test";
            byte[] nameBytes = IxiNameUtils.encodeIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = 1;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
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
            byte[] subnameBytes = IxiNameUtils.encodeIxiName("subNameTest." + name);

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


            regNames.revertTransaction(1);

            rnRecord = regNames.getName(nameBytes);
            Assert.AreEqual((ulong)1, regNames.count());
            Assert.AreEqual(0, rnRecord.dataRecords.Count);

            assertRecords(origRecord, rnRecord);
        }

        [TestMethod]
        public void Subname_Register_TransactionHasTooLowTopFee()
        {
            string name = "test";
            byte[] nameBytes = IxiNameUtils.encodeIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = 1;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
            Address nextPkHash = wallet1.getPrimaryAddress();
            Address recoveryHash = wallet2.getPrimaryAddress();
            IxiNumber subnamePrice = 100000;
            Address subnameFeeRecipient = wallet4.getPrimaryAddress();

            Assert.IsTrue(registerName(name, registrationTimeInBlocks, capacity, regFee, nextPkHash, recoveryHash));

            nextPkHash = wallet3.getPrimaryAddress();
            allowSubnames(nameBytes, true, subnamePrice, subnameFeeRecipient, nextPkHash, wallet1);

            Address subnameNextPkHash = wallet4.getPrimaryAddress();
            Address subnameRecoveryHash = wallet1.getPrimaryAddress();

            var rnRecord = regNames.getName(nameBytes);
            var origRecord = new RegisteredNameRecord(rnRecord);

            regNames.beginTransaction(1);

            // Register subname
            byte[] subnameBytes = IxiNameUtils.encodeIxiName("subNameTest." + name);
            IxiNumber subnameFee = subnamePrice * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);

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


            regNames.revertTransaction(1);

            rnRecord = regNames.getName(nameBytes);
            Assert.AreEqual((ulong)1, regNames.count());
            Assert.AreEqual(0, rnRecord.dataRecords.Count);

            assertRecords(origRecord, rnRecord);
        }

        [TestMethod]
        public void Subname_Register_TransactionHasNoTopFee()
        {
            string name = "test";
            byte[] nameBytes = IxiNameUtils.encodeIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = 1;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
            Address nextPkHash = wallet1.getPrimaryAddress();
            Address recoveryHash = wallet2.getPrimaryAddress();
            IxiNumber subnameFee = 100000;
            Address subnameFeeRecipient = wallet4.getPrimaryAddress();

            Assert.IsTrue(registerName(name, registrationTimeInBlocks, capacity, regFee, nextPkHash, recoveryHash));

            nextPkHash = wallet3.getPrimaryAddress();
            allowSubnames(nameBytes, true, subnameFee, subnameFeeRecipient, nextPkHash, wallet1);

            Address subnameNextPkHash = wallet4.getPrimaryAddress();
            Address subnameRecoveryHash = wallet1.getPrimaryAddress();

            var rnRecord = regNames.getName(nameBytes);
            var origRecord = new RegisteredNameRecord(rnRecord);

            regNames.beginTransaction(1);

            // Register subname
            byte[] subnameBytes = IxiNameUtils.encodeIxiName("subNameTest." + name);

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


            regNames.revertTransaction(1);

            rnRecord = regNames.getName(nameBytes);
            Assert.AreEqual((ulong)1, regNames.count());
            Assert.AreEqual(0, rnRecord.dataRecords.Count);

            assertRecords(origRecord, rnRecord);
        }

        [TestMethod]
        public void Subname_Register_AlreadyRegistered()
        {
            string name = "test";
            byte[] nameBytes = IxiNameUtils.encodeIxiName(name);
            uint registrationTimeInBlocks = ConsensusConfig.rnMinRegistrationTimeInBlocks;
            uint capacity = 1;
            IxiNumber regFee = ConsensusConfig.rnMinPricePerUnit * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);
            Address nextPkHash = wallet1.getPrimaryAddress();
            Address recoveryHash = wallet2.getPrimaryAddress();
            IxiNumber subnamePrice = 100000;
            Address subnameFeeRecipient = wallet4.getPrimaryAddress();

            Assert.IsTrue(registerName(name, registrationTimeInBlocks, capacity, regFee, nextPkHash, recoveryHash));

            nextPkHash = wallet3.getPrimaryAddress();
            allowSubnames(nameBytes, true, subnamePrice, subnameFeeRecipient, nextPkHash, wallet1);

            Address subnameNextPkHash = wallet4.getPrimaryAddress();
            Address subnameRecoveryHash = wallet1.getPrimaryAddress();

            var rnRecord = regNames.getName(nameBytes);
            var origRecord = new RegisteredNameRecord(rnRecord);

            regNames.beginTransaction(1);

            // Register subname
            byte[] subnameBytes = IxiNameUtils.encodeIxiName("subNameTest." + name);
            IxiNumber subnameFee = subnamePrice * ((ulong)ConsensusConfig.rnMinRegistrationTimeInBlocks / ConsensusConfig.rnMonthInBlocks);

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


            regNames.revertTransaction(1);

            rnRecord = regNames.getName(nameBytes);
            Assert.AreEqual((ulong)1, regNames.count());
            Assert.AreEqual(0, rnRecord.dataRecords.Count);

            assertRecords(origRecord, rnRecord);
        }
    }
}