using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Numerics;
using IXICore;
using System.Linq;

namespace UnitTests
{
    [TestClass]
    public class TestSignerPowSolution
    {
        byte[] minHash = Crypto.stringToHash("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF0000");
        byte[] maxHash = Crypto.stringToHash("0100000000000000000000000000000000000000000000000000000000000000000000000000000000000000");
        BigInteger maxDifficulty = BigInteger.Parse("139984046386112763159840142535527767382602843577165595931249318810236991948760059086304843329475444735");

        [TestMethod]
        public void HashToDifficulty()
        {
            BigInteger difficulty = SignerPowSolution.hashToDifficulty(minHash);
            Assert.AreEqual(1, difficulty);
            Assert.AreNotEqual(0, difficulty);

            difficulty = SignerPowSolution.hashToDifficulty(maxHash);
            Assert.AreEqual(maxDifficulty, difficulty);
            Assert.AreNotEqual(0, difficulty);
        }

        [TestMethod]
        public void DifficultyToHash()
        {
            byte[] hash = SignerPowSolution.difficultyToHash(1);
            Assert.AreEqual(new BigInteger(minHash), new BigInteger(hash));

            hash = SignerPowSolution.difficultyToHash(2);
            Assert.AreNotEqual(new BigInteger(minHash), new BigInteger(hash));
            byte[] expHash = new byte[hash.Length];
            Array.Copy(hash, expHash, expHash.Length);
            Assert.IsTrue(expHash.SequenceEqual(hash), "Expected {0}, got {1}", Crypto.hashToString(expHash), Crypto.hashToString(hash));

            hash = SignerPowSolution.difficultyToHash(maxDifficulty);
            Assert.AreEqual(new BigInteger(maxHash), new BigInteger(hash));
        }

        [TestMethod]
        public void HashToBits()
        {
            uint target = SignerPowSolution.hashToBits(minHash);
            Assert.AreEqual((uint)0x02000000, target);

            target = SignerPowSolution.hashToBits(Crypto.stringToHash("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFA9CBED0000000000"));
            Assert.AreEqual((uint)0x05123456, target);

            target = SignerPowSolution.hashToBits(maxHash);
            Assert.AreEqual((uint)0x29FFFFFE, target);
        }

        [TestMethod]
        public void BitsToHash()
        {
            byte[] hash = SignerPowSolution.bitsToHash(0x02000000);
            Assert.IsTrue(minHash.SequenceEqual(hash), "Expected {0}, got {1}", Crypto.hashToString(minHash), Crypto.hashToString(hash));

            byte[] expHash = Crypto.stringToHash("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFA9CBED0000000000");
            hash = SignerPowSolution.bitsToHash(0x05123456);
            Assert.IsTrue(expHash.SequenceEqual(hash), "Expected {0}, got {1}", Crypto.hashToString(expHash), Crypto.hashToString(hash));

            hash = SignerPowSolution.bitsToHash(0x29FFFFFE);
            Assert.IsTrue(maxHash.SequenceEqual(hash), "Expected {0}, got {1}", Crypto.hashToString(maxHash), Crypto.hashToString(hash));
        }

        [TestMethod]
        [Ignore]
        public void Loop_BitsFull()
        {
            Loop_BitsInternal(SignerPowSolution.minTargetBits, SignerPowSolution.maxTargetBits);
        }

        [TestMethod]
        public void Loop_BitsShort()
        {
            Loop_BitsInternal(SignerPowSolution.minTargetBits, SignerPowSolution.minTargetBits + 0x01FFFFFF);
        }

        public void Loop_BitsInternal(uint minTargetBits, uint maxTargetBits)
        {
            BigInteger prevDiff = -1;
            BigInteger prevHash = 0;
            for (uint i = minTargetBits; i < maxTargetBits; i++)
            {
                byte[] hash = SignerPowSolution.bitsToHash(i);
                uint bits = SignerPowSolution.hashToBits(hash);
                byte[] hash2 = SignerPowSolution.bitsToHash(bits);
                Assert.IsTrue(new BigInteger(hash) == new BigInteger(hash2), "Expected {0}, got {1}", Crypto.hashToString(hash), Crypto.hashToString(hash2));

                BigInteger diff = SignerPowSolution.hashToDifficulty(hash);
                Assert.IsTrue(diff > -1, "Expected positive diff: {0}", diff);

                BigInteger biHash = new BigInteger(hash);
                if ((i & 0x00FFFFFF) != 0)
                {
                    Assert.IsTrue(diff >= prevDiff, "Expected {0} >= {1}", diff, prevDiff);
                    //Assert.IsTrue(diff > prevDiff, "Expected {0} > {1}", diff, prevDiff);
                    if (prevHash != 0)
                    {
                        Assert.IsTrue(biHash < prevHash, "Expected {0} < {1}", biHash, prevHash);
                    }
                }
                prevDiff = diff;
                prevHash = biHash;

                byte[] hash3 = SignerPowSolution.difficultyToHash(diff);
                //Assert.IsTrue(new BigInteger(hash) == new BigInteger(hash3), "Expected {0}, got {1}", Crypto.hashToString(hash), Crypto.hashToString(hash3));
            }
        }

        [TestMethod]
        public void Loop_Difficulty_Plus1()
        {
            uint prevBits = 0;
            BigInteger prevHash = 0;
            for (BigInteger i = 1; i < 0x04FFFFFF; i = i + 1)
            {
                var ret = Loop_DifficultyInternal(i, prevHash, prevBits);
                prevHash = ret.Item1;
                prevBits = ret.Item2;
            }
        }

        [TestMethod]
        public void Loop_Difficulty_Times3Full()
        {
            uint prevBits = 0;
            BigInteger prevHash = 0;
            BigInteger difficulty = 1;
            do
            {
                var ret = Loop_DifficultyInternal(difficulty, prevHash, prevBits);
                prevHash = ret.Item1;
                prevBits = ret.Item2;
                difficulty = (difficulty * 3) + 1;
            } while (difficulty < maxDifficulty);
        }

        private (BigInteger, uint) Loop_DifficultyInternal(BigInteger difficulty, BigInteger prevHash, uint prevBits)
        {
            byte[] hash = SignerPowSolution.difficultyToHash(difficulty);
            BigInteger biHash = new BigInteger(hash);
            if (prevHash != 0)
            {
                Assert.IsTrue(biHash < prevHash, "Expected {0} < {1}", biHash, prevHash);
            }
            prevHash = biHash;

            BigInteger diff2 = SignerPowSolution.hashToDifficulty(hash);
            //Assert.IsTrue(diff2 == difficulty, "Expected {0} == {1}", diff2, difficulty);

            if(hash.Length > 2)
            {
                uint bits = SignerPowSolution.hashToBits(hash);

                Assert.IsTrue(bits >= +prevBits, "Expected {0} >+ {1}", bits, prevBits);
                //Assert.IsTrue(bits > prevBits, "Expected {0} > {1}", bits, prevBits);
                prevBits = bits;
            }

            return (prevHash, prevBits);
        }

        [TestMethod]
        public void ValidateHash()
        {
            Assert.IsTrue(SignerPowSolution.validateHash(minHash, 1));
            Assert.IsFalse(SignerPowSolution.validateHash(minHash, 2));

            Assert.IsTrue(SignerPowSolution.validateHash(maxHash, maxDifficulty));
            Assert.IsFalse(SignerPowSolution.validateHash(maxHash, maxDifficulty + 1));
        }

        [TestMethod]
        public void OutOfBounds()
        {
            // TODO: Perhaps add byte length check (not sure if necessary)
            try
            {
                SignerPowSolution.bitsToHash(0x01FFFFFF);
                Assert.Fail();
            }
            catch (AssertFailedException) { throw; }
            catch (Exception) { }

            try
            {
                SignerPowSolution.bitsToHash(0x30000000);
                Assert.Fail();
            }
            catch (AssertFailedException) { throw; }
            catch (Exception) { }

            try
            {
                SignerPowSolution.difficultyToHash(-1);
                Assert.Fail();
            }
            catch (AssertFailedException) { throw; }
            catch (Exception) { }
        }
    }
}
