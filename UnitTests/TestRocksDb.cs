using Microsoft.VisualStudio.TestTools.UnitTesting;
using DLT.Storage;
using IXICore;
using System.Linq;
using System.Threading;
using System.Collections.Generic;
using System;

namespace UnitTests
{
    [TestClass]
    public class TestRocksDb
    {
        private RocksDBStorage db;

        [TestInitialize]
        public void Init()
        {
            db = new RocksDBStorage();
            db.prepareStorage();
        }

        [TestCleanup]
        public void cleanup()
        {
            db.stopStorage();
            Thread.Sleep(100);
            db.deleteData();
        }

        private Block InsertDummyBlock(ulong blockNum)
        {
            Block block = new Block()
            {
                blockNum = blockNum,
                version = Block.maxVersion,
                walletStateChecksum = new byte[64],
                regNameStateChecksum = new byte[64],
                timestamp = Clock.getNetworkTimestamp()
            };
            block.blockChecksum = block.calculateChecksum();
            db.insertBlock(block);
            return block;
        }

        private Transaction InsertDummyTransaction(ulong applied, ulong blockHeight, int nonce, Transaction.Type type = Transaction.Type.Normal)
        {
            Transaction tx = new Transaction((int)type, Transaction.maxVersion)
            {
                applied = applied,
                blockHeight = blockHeight,
                nonce = nonce
            };
            tx.generateChecksums();
            db.insertTransaction(tx);
            return tx;
        }

        [TestMethod]
        public void GetHighestBlockInStorageEmpty()
        {
            // Make sure it works with no blocks in storage
            db.getHighestBlockInStorage();
        }

        [TestMethod]
        public void GetBlock()
        {

            var block1 = InsertDummyBlock(1);
            var block2 = InsertDummyBlock(2);
            var block3 = InsertDummyBlock(3);
            //Thread.Sleep(1000);

            var retBlock1 = db.getBlock(block1.blockNum);
            Assert.IsNotNull(retBlock1);
            Assert.IsTrue(block1.blockChecksum.SequenceEqual(retBlock1.blockChecksum));

            var retBlock2 = db.getBlock(block2.blockNum);
            Assert.IsNotNull(retBlock2);
            Assert.IsTrue(block2.blockChecksum.SequenceEqual(retBlock2.blockChecksum));
        }

        [TestMethod]
        public void GetBlockByHash()
        {
            var block1 = InsertDummyBlock(1);
            var block2 = InsertDummyBlock(2);
            //Thread.Sleep(2000);

            var retBlock1 = db.getBlockByHash(block1.blockChecksum);
            Assert.IsNotNull(retBlock1);
            Assert.IsTrue(block1.blockChecksum.SequenceEqual(retBlock1.blockChecksum));

            var retBlock2 = db.getBlockByHash(block2.blockChecksum);
            Assert.IsNotNull(retBlock2);
            Assert.IsTrue(block2.blockChecksum.SequenceEqual(retBlock2.blockChecksum));
        }

        [TestMethod]
        public void GetTransaction()
        {
            InsertDummyBlock(1);
            InsertDummyBlock(2);
            InsertDummyBlock(3);

            var tx1 = InsertDummyTransaction(2, 1, 1);
            var tx2 = InsertDummyTransaction(3, 1, 1);

            //Thread.Sleep(2000);

            var retTx1 = db.getTransaction(tx1.id, 2);
            Assert.IsNotNull(retTx1);
            Assert.IsTrue(tx1.id.SequenceEqual(retTx1.id));

            var retTx2 = db.getTransaction(tx2.id, 3);
            Assert.IsNotNull(retTx2);
            Assert.IsTrue(tx2.id.SequenceEqual(retTx2.id));

            db.stopStorage();
            Thread.Sleep(100);
            db = new RocksDBStorage();
            db.prepareStorage();

            retTx1 = db.getTransaction(tx1.id, 0);
            Assert.IsNotNull(retTx1);
            Assert.IsTrue(tx1.id.SequenceEqual(retTx1.id));

            retTx2 = db.getTransaction(tx2.id, 0);
            Assert.IsNotNull(retTx2);
            Assert.IsTrue(tx2.id.SequenceEqual(retTx2.id));
        }

        [TestMethod]
        public void GetTransactionsInBlock()
        {
            InsertDummyBlock(1);
            InsertDummyBlock(2);
            InsertDummyBlock(3);

            var tx1 = InsertDummyTransaction(2, 1, 1, Transaction.Type.PoWSolution);
            var tx2 = InsertDummyTransaction(2, 1, 2, Transaction.Type.Normal);
            var tx3 = InsertDummyTransaction(3, 2, 1);
            //Thread.Sleep(2000);

            var retTxs = db.getTransactionsInBlock(1).ToArray();
            Assert.AreEqual(0, retTxs.Count());

            retTxs = db.getTransactionsInBlock(2).ToArray();
            Assert.AreEqual(2, retTxs.Count());
            Assert.IsTrue(tx1.id.SequenceEqual(retTxs[0].id));
            Assert.IsTrue(tx2.id.SequenceEqual(retTxs[1].id));

            retTxs = db.getTransactionsInBlock(3).ToArray();
            Assert.AreEqual(1, retTxs.Count());
            Assert.IsTrue(tx3.id.SequenceEqual(retTxs[0].id));

            retTxs = db.getTransactionsInBlock(2, (int)Transaction.Type.PoWSolution).ToArray();
            Assert.AreEqual(1, retTxs.Count());
            Assert.IsTrue(tx1.id.SequenceEqual(retTxs[0].id));
        }

        [TestMethod]
        public void GetTransactionsInBlockMixed()
        {
            InsertDummyBlock(1);
            InsertDummyBlock(2);
            InsertDummyBlock(3);

            var tx1 = InsertDummyTransaction(2, 1, 1);
            var tx3 = InsertDummyTransaction(3, 2, 1);
            var tx2 = InsertDummyTransaction(2, 1, 2);
            //Thread.Sleep(2000);

            var retTxs = db.getTransactionsInBlock(1).ToArray();
            Assert.AreEqual(0, retTxs.Count());

            retTxs = db.getTransactionsInBlock(2).ToArray();
            Assert.AreEqual(2, retTxs.Count());
            Assert.IsTrue(tx1.id.SequenceEqual(retTxs[0].id));
            Assert.IsTrue(tx2.id.SequenceEqual(retTxs[1].id));

            retTxs = db.getTransactionsInBlock(3).ToArray();
            Assert.AreEqual(1, retTxs.Count());
            Assert.IsTrue(tx3.id.SequenceEqual(retTxs[0].id));
        }

        [TestMethod]
        public void GetTransactionsMany()
        {
            InsertDummyBlock(1);
            InsertDummyBlock(2);
            InsertDummyBlock(3);

            List<Transaction> txs = new();
            Random rnd = new();
            int block2TxCount = 0;
            int block2Type1Count = 0;
            int block2Type2Count = 0;

            int block3TxCount = 0;
            int block3Type1Count = 0;
            int block3Type2Count = 0;

            for (int i = 0; i < 10000; i++)
            {
                ulong blockNum = (ulong)rnd.Next(2, 4);
                int type = rnd.Next(1, 3);
                txs.Add(InsertDummyTransaction(blockNum, 1, i, (Transaction.Type)type));
                if (blockNum == 2)
                {
                    block2TxCount++;
                    if (type == 1)
                    {
                        block2Type1Count++;
                    }else if(type == 2)
                    {
                        block2Type2Count++;
                    }
                }
                else if (blockNum == 3)
                {
                    block3TxCount++;
                    if (type == 1)
                    {
                        block3Type1Count++;
                    }
                    else if (type == 2)
                    {
                        block3Type2Count++;
                    }
                }
            }

            Console.WriteLine("Block2TxCount: " + block2TxCount);
            Console.WriteLine("block2Type1Count: " + block2Type1Count);
            Console.WriteLine("block2Type2Count: " + block2Type2Count);
            Console.WriteLine("block3TxCount: " + block3TxCount);
            Console.WriteLine("block3Type1Count: " + block3Type1Count);
            Console.WriteLine("block3Type2Count: " + block3Type2Count);

            var retTxs = db.getTransactionsInBlock(1).ToArray();
            Assert.AreEqual(0, retTxs.Count());

            retTxs = db.getTransactionsInBlock(2).ToArray();
            Assert.AreEqual(block2TxCount, retTxs.Count());

            retTxs = db.getTransactionsInBlock(2, 1).ToArray();
            Assert.AreEqual(block2Type1Count, retTxs.Count());

            retTxs = db.getTransactionsInBlock(2, 2).ToArray();
            Assert.AreEqual(block2Type2Count, retTxs.Count());

            retTxs = db.getTransactionsInBlock(3).ToArray();
            Assert.AreEqual(block3TxCount, retTxs.Count());

            retTxs = db.getTransactionsInBlock(3, 1).ToArray();
            Assert.AreEqual(block3Type1Count, retTxs.Count());

            retTxs = db.getTransactionsInBlock(3, 2).ToArray();
            Assert.AreEqual(block3Type2Count, retTxs.Count());

            db.stopStorage();
            Thread.Sleep(100);
            db = new RocksDBStorage();
            db.prepareStorage();

            retTxs = db.getTransactionsInBlock(1).ToArray();
            Assert.AreEqual(0, retTxs.Count());

            retTxs = db.getTransactionsInBlock(2).ToArray();
            Assert.AreEqual(block2TxCount, retTxs.Count());

            retTxs = db.getTransactionsInBlock(2, 1).ToArray();
            Assert.AreEqual(block2Type1Count, retTxs.Count());

            retTxs = db.getTransactionsInBlock(2, 2).ToArray();
            Assert.AreEqual(block2Type2Count, retTxs.Count());

            retTxs = db.getTransactionsInBlock(3).ToArray();
            Assert.AreEqual(block3TxCount, retTxs.Count());

            retTxs = db.getTransactionsInBlock(3, 1).ToArray();
            Assert.AreEqual(block3Type1Count, retTxs.Count());

            retTxs = db.getTransactionsInBlock(3, 2).ToArray();
            Assert.AreEqual(block3Type2Count, retTxs.Count());
        }
    }
}
