using System;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;

using IXICore;
using System.Collections.Generic;
using System.Diagnostics;

namespace UnitTests
{
    [TestClass]
    public class IxianUT_PrefixInclusionTree
    {
        static readonly byte[] emptyPITHash = { 0x82, 0x6d, 0xf0, 0x68, 0x45, 0x7d, 0xf5, 0xdd, 0x19, 0x5b, 0x43, 0x7a, 0xb7, 0xe7, 0x73, 0x9f };
        private Random RNG;


        private string generateRandomTXID()
        {
            byte[] random_txid = new byte[44];
            RNG.NextBytes(random_txid);
            return Base58Check.Base58CheckEncoding.EncodePlain(random_txid);
        }

        private void verifyMinimumTree(int sizeFrom, int sizeTo, byte numLevels = 4)
        {
            PrefixInclusionTree pit = new PrefixInclusionTree(16, numLevels);
            List<string> txids = new List<string>();
            for (int i = 0; i < RNG.Next(sizeTo-sizeFrom) + sizeFrom; i++)
            {
                string tx = generateRandomTXID();
                if (!txids.Contains(tx))
                {
                    txids.Add(tx);
                    pit.add(tx);
                }
            }
            Trace.WriteLine(String.Format("Generated {0} transactions...", txids.Count));
            // pick a random tx
            string rtx = txids[RNG.Next(txids.Count)];
            Stopwatch sw = new Stopwatch();
            sw.Start();
            byte[] minimal_tree = pit.getMinimumTree(rtx);
            sw.Stop();
            Trace.WriteLine(String.Format("Retrieving minimum TX tree took {0} ms and yielded {1} bytes.", sw.ElapsedMilliseconds, minimal_tree.Length));
            sw.Reset();
            Assert.IsNotNull(minimal_tree, "PIT returns null minimal tree!");

            PrefixInclusionTree pit2 = new PrefixInclusionTree();
            sw.Start();
            pit2.reconstructMinimumTree(minimal_tree);
            sw.Stop();
            Trace.WriteLine(String.Format("Reconstructing minimum TX tree took {0} ms.", sw.ElapsedMilliseconds));
            sw.Reset();
            sw.Start();
            Assert.IsTrue(pit2.contains(rtx), "Reconstructed PIT should contain the minimal transaction!");
            Assert.IsTrue(pit2.verifyMinimumTreeHash(), "Minimum PIT tree does not verify successfully!");
            sw.Stop();
            Trace.WriteLine(String.Format("Verifying minimum TX tree took {0} ms.", sw.ElapsedMilliseconds));
        }

        private void hashIsRepeatableInternal(int sizeFrom, int sizeTo)
        {
            PrefixInclusionTree pit = new PrefixInclusionTree();
            List<string> txids = new List<string>();
            for (int i = 0; i < RNG.Next(sizeTo - sizeFrom) + sizeFrom; i++)
            {
                string tx = generateRandomTXID();
                if (!txids.Contains(tx))
                {
                    txids.Add(tx);
                    pit.add(tx);
                }
            }
            byte[] pit_hash = pit.calculateTreeHash();
            Assert.IsFalse(pit.calculateTreeHash().SequenceEqual(emptyPITHash), "PIT hash shouldn't be equal to empty after hashes are added!");
            foreach (string tx in txids)
            {
                Assert.IsTrue(pit.contains(tx), "PIT should contain the added txid!");
                pit.remove(tx);
                Assert.IsFalse(pit.contains(tx), "PIT shouldn't contain hash which was removed!");
            }
            Assert.IsTrue(pit.calculateTreeHash().SequenceEqual(emptyPITHash), "PIT hash should be equal to empty after all txids are removed!");
            foreach (string tx in txids)
            {
                pit.add(tx);
            }
            Assert.IsTrue(pit.calculateTreeHash().SequenceEqual(pit_hash), "PIT hash should be repeatable if the same txids are added!");
        }

        [TestInitialize]
        public void testInitialize()
        {
            RNG = new Random();
        }

        [TestMethod]
        public void trivialSanityTest()
        {
            PrefixInclusionTree pit = new PrefixInclusionTree();
            byte[] hash = pit.calculateTreeHash();
            Assert.IsTrue(hash.SequenceEqual(emptyPITHash), "Empty PIT hash is incorrect!");
            Assert.IsFalse(pit.contains("ABCDEFGH"), "PIT reports that it contains an invalid txid");
        }

        [TestMethod]
        public void addHash()
        {
            PrefixInclusionTree pit = new PrefixInclusionTree();
            // transaction IDs for version > 2 are 44
            string txid_str = generateRandomTXID();
            pit.add(txid_str);
            Assert.IsTrue(pit.contains(txid_str), "PIT does not contain the added txid!");
        }

        [TestMethod]
        public void addMultipleHashes()
        {
            List<string> addedHashes = new List<string>();
            PrefixInclusionTree pit = new PrefixInclusionTree();
            // transaction IDs for version > 2 are 44
            for (int i = 0; i < RNG.Next(20); i++)
            {
                string txid_str = generateRandomTXID();
                addedHashes.Add(txid_str);
                pit.add(txid_str);
            }
            foreach (string txid in addedHashes)
            {
                Assert.IsTrue(pit.contains(txid), "PIT does not contain the added txid!");
            }
        }

        [TestMethod]
        public void addRemoveHash()
        {
            PrefixInclusionTree pit = new PrefixInclusionTree();
            string txid = generateRandomTXID();
            pit.add(txid);
            Assert.IsFalse(pit.calculateTreeHash().SequenceEqual(emptyPITHash), "PIT hash should not be the same as empty!");
            pit.remove(txid);
            Assert.IsTrue(pit.calculateTreeHash().SequenceEqual(emptyPITHash), "PIT hash should be equal to empty if all hashes are removed!");
        }

        [TestMethod]
        public void hashIsRepeatable()
        {
            hashIsRepeatableInternal(50, 100);
        }

        [TestMethod]
        public void hashIsRepeatableLarge()
        {
            hashIsRepeatableInternal(10000, 15000);
        }

        [TestMethod]
        public void largeHashTree()
        {
            PrefixInclusionTree pit = new PrefixInclusionTree();
            List<string> txids = new List<string>();
            Stopwatch sw = new Stopwatch();
            // between 2000 and 3000 hashes
            for (int i = 0; i < RNG.Next(1000) + 2000; i++)
            {
                txids.Add(generateRandomTXID());
            }
            sw.Start();
            foreach (var tx in txids)
            {
                pit.add(tx);
            }
            sw.Stop();
            Trace.WriteLine(String.Format("Large PIT test - adding: {0} hashes = {1} ms", txids.Count, sw.ElapsedMilliseconds));
            sw.Reset();
            sw.Start();
            foreach (var tx in txids)
            {
                Assert.IsTrue(pit.contains(tx), "PIT should contain all the added hashes (large quantity)");
            }
            sw.Stop();
            Trace.WriteLine(String.Format("Large PIT test - verifying: {0} hashes = {1} ms", txids.Count, sw.ElapsedMilliseconds));
            sw.Reset();
            sw.Start();
            byte[] pit_hash = pit.calculateTreeHash();
            sw.Stop();
            Trace.WriteLine(String.Format("Large PIT test - calculating hash: {0} hashes = {1} ms", txids.Count, sw.ElapsedMilliseconds));
            Assert.IsFalse(pit_hash.SequenceEqual(emptyPITHash), "PIT hash should be different from empty (large quantity)");
        }

        [TestMethod]
        public void minimumTreeSimple()
        {
            verifyMinimumTree(5, 5);
        }

        [TestMethod]
        public void minimumTreeLarge()
        {
            verifyMinimumTree(1800, 2200);
        }

        [TestMethod]
        public void minimumTreeLarge2Levels()
        {
            verifyMinimumTree(1800, 2200, 2);
        }

        [TestMethod]
        public void minimumTreeLarge3Levels()
        {
            verifyMinimumTree(1800, 2200, 3);
        }


        [TestMethod]
        public void minimumTreeExtraLarge()
        {
            verifyMinimumTree(10000, 15000);
        }

        [TestMethod]
        public void minimumTreeExtraExtraLarge()
        {
            verifyMinimumTree(100000, 110000);
        }

    }
}
