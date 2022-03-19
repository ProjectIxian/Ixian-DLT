using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using IXICore;


namespace UnitTests
{
    [TestClass]
    public class TestCrypto
    {
        // SHA-3 implementation tests
        // Reference Values are based on https://csrc.nist.gov/projects/cryptographic-standards-and-guidelines/example-values#aHashing

        string ixian_hex = "697869616e"; // The word "ixian" in hexadecimal representation

        [TestMethod]
        public void TestSha3_256()
        {
            byte[] hash = null;

            byte[] hash_data = Crypto.stringToHash("");
            hash = CryptoManager.lib.sha3_256(hash_data);
            Assert.AreEqual("a7ffc6f8bf1ed76651c14756a061d662f580ff4de43b49fa82d80a4b80f8434a", Crypto.hashToString(hash));

            hash_data = Crypto.stringToHash(ixian_hex);
            hash = CryptoManager.lib.sha3_256(hash_data);
            Assert.AreEqual("b0eca25a3baaed56745dedb4d803c14e290640e951049ddad74d16685d6bb8cb", Crypto.hashToString(hash));
        }

        [TestMethod]
        public void TestSha3_512()
        {
            byte[] hash = null;

            byte[] hash_data = Crypto.stringToHash("");
            hash = CryptoManager.lib.sha3_512(hash_data);
            Assert.AreEqual("a69f73cca23a9ac5c8b567dc185a756e97c982164fe25859e0d1dcc1475c80a615b2123af1f5f94c11e3e9402c3ac558f500199d95b6d3e301758586281dcd26", Crypto.hashToString(hash));

            hash_data = Crypto.stringToHash(ixian_hex);
            hash = CryptoManager.lib.sha3_512(hash_data);
            Assert.AreEqual("e9a5f6666f4dc96469793084ab119db1010c884eeb750f30fc63af760f5ecc40038afc75d036bd11dfc750b5a6624a92c9ae6a06d6cfd6059527d4e784678c3b", Crypto.hashToString(hash));
        }


    }
}
