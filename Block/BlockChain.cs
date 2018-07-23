using DLT.Meta;
using DLT.Network;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DLT
{
    class BlockChain
    {
        int redactedWindowSize = 3000; // approx 25 hours - this should be a network-calculable parameter at some point
        List<Block> blocks = new List<Block>();

        public int Count { get => blocks.Count; }

        public BlockChain()
        {
        }

        public void onUpdate() {
            lock (blocks)
            {
                // redaction
                int begin_size = blocks.Count();
                while (blocks.Count() > redactedWindowSize)
                {
                    blocks.RemoveAt(0);
                }
                if (begin_size > blocks.Count())
                {
                    Console.WriteLine(String.Format("REDACTED {0} blocks to keep the chain length appropriate.", begin_size - blocks.Count()));
                }
            }
        }

        public bool appendBlock(Block b)
        {
            lock (blocks)
            {
                // special case when we are starting up and have an empty chain
                if (blocks.Count == 0)
                {
                    blocks.Add(b);
                    return true;
                }
                // check for invalid block appending
                if (b.blockNum != blocks[blocks.Count - 1].blockNum + 1)
                {
                    Logging.warn(String.Format("Attempting to add non-sequential block #{0} after block #{1}.",
                        b.blockNum,
                        blocks[blocks.Count - 1].blockNum));
                    return false;
                }
                if(b.lastBlockChecksum != blocks[blocks.Count-1].blockChecksum)
                {
                    Logging.warn(String.Format("Attempting to add a block #{0}with invalid lastBlockChecksum!", b.blockNum));
                    return false;
                }
                blocks.Add(b);
                return true;
            }
        }

        public Block getBlock(ulong blocknum)
        {
            lock(blocks)
            {
                return blocks.Find(x => x.blockNum == blocknum);
            }
        }

        public ulong getLastBlockNum()
        {
            if (blocks.Count == 0) return 0;
            lock (blocks)
            {
                return blocks[blocks.Count - 1].blockNum;
            }
        }

        public string getLastBlockChecksum()
        {
            if (blocks.Count == 0) return "";
            lock(blocks)
            {
                return blocks[blocks.Count - 1].blockChecksum;
            }
        }

    }
}
