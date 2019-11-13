using System;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;

using IXICore;
using System.Collections.Generic;

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
    }
}
