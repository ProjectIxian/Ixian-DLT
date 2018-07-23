using DLT.Meta;
using DLT.Network;
using DLTNode;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace DLT
{
    enum BlockVerifyStatus
    {
        Valid,
        Invalid,
        Indeterminate
    }
    class BlockProcessor
    {
        public bool synchronizing { get => inSyncMode; }
        public bool synchronized { get => !inSyncMode; }

        Block localNewBlock; // Block being worked on currently
        Object localBlockLock = new object(); // used because localNewBlock can change while this lock should be held.
        DateTime lastBlockStartTime;

        bool inSyncMode = false;

        List<Block> pendingBlocks = new List<Block>();
        ulong syncTargetBlockNum;
        int consensusSignaturesRequired = 1;
        int blockGenerationInterval = 30; // in seconds

        public BlockProcessor()
        {
            lastBlockStartTime = DateTime.Now;
            localNewBlock = null;
            // we start up in sync mode
            inSyncMode = true;
            syncTargetBlockNum = 0;
        }

        // Returns true if a block was generated
        public bool onUpdate()
        {
            if (inSyncMode && syncTargetBlockNum > 0)
            {
                // attempt to merge pending blocks to the chain
                lock (pendingBlocks)
                {
                    while (pendingBlocks.Count > 0)
                    {
                        ulong nextRequired = Node.blockChain.getLastBlockNum();
                        int idx = pendingBlocks.FindIndex(x => x.blockNum == nextRequired);
                        if (idx > -1)
                        {
                            Block toAppend = pendingBlocks[idx];
                            pendingBlocks.RemoveAt(idx);
                            if (!Node.blockChain.appendBlock(toAppend))
                            {
                                Logging.warn(String.Format("Block #{0} could not be appended to the chain. It is possibly corrupt. Requesting a new copy...", toAppend.blockNum));
                                ProtocolMessage.broadcastGetBlock(toAppend.blockNum);
                                return true;
                            }
                        }
                        else
                        {
                            break;
                        }
                    }
                }
                if(Node.blockChain.getLastBlockNum() == syncTargetBlockNum)
                {
                    inSyncMode = false;
                    lock (pendingBlocks)
                    {
                        pendingBlocks.Clear();
                    }
                    return true;
                }
            } else // !inSyncMode
            {
                // check if it is time to generate a new block
                if((DateTime.Now - lastBlockStartTime).TotalSeconds > blockGenerationInterval)
                {
                    generateNewBlock();
                } else
                {
                    verifyBlockAcceptance();
                }
            }
            
            return true;
        }

        public void onBlockReceived(Block b)
        {
            if(inSyncMode)
            {
                if (b.signatures.Count() < consensusSignaturesRequired)
                {
                    // ignore blocks which haven't been accepted while we're syncing.
                    return;
                }
                else
                {
                    lock (pendingBlocks)
                    {
                        int idx = pendingBlocks.FindIndex(x => x.blockNum == b.blockNum);
                        if (idx > -1)
                        {
                            if (pendingBlocks[idx].signatures.Count() < b.signatures.Count())
                            {
                                pendingBlocks[idx] = b;
                            }
                        }
                        else // idx <= -1
                        {
                            pendingBlocks.Add(b);
                        }
                    }
                }
            } else // !inSyncMode
            {
                lock (localBlockLock)
                {
                    if (localNewBlock == null || b.signatures.Count() > localNewBlock.signatures.Count())
                    {
                        if (localNewBlock == null)
                        {
                            //we use the instant we received this block as the indicator for when to generate the next block
                            lastBlockStartTime = DateTime.Now;
                        }
                        if (verifyBlock(b) != BlockVerifyStatus.Invalid)
                        {
                            localNewBlock = b;
                            b.applySignature();
                        }
                        else // block is invalid
                        {
                            Logging.warn(String.Format("Received invalid block from network: #{0}", localNewBlock.blockNum));
                            localNewBlock = null;
                            // generate a block ASAP (unless we receive another block in the meantime)
                            lastBlockStartTime = DateTime.MinValue;
                        }
                    }
                    ProtocolMessage.broadcastNewBlock(b);
                }
            }
        }

        public BlockVerifyStatus verifyBlock(Block b)
        {
            // Check all transactions in the block against our TXpool, make sure all is legal
            // Note: it is possible we don't have all the required TXs in our TXpool - in this case, request the missing ones and return Intederminate
            // Verify checksums
            // Verify signatures
            // Any problems should be sent to the log, so we have some diagnostic for when things go wrong
            // TODO: blacklisting would happen here - whoever sent us an invalid block is problematic
            //  Note: This will need a change in the Network code to tag incoming blocks with sender info.
            return BlockVerifyStatus.Valid;
        }

        private void verifyBlockAcceptance()
        {
            if (localNewBlock == null) return;
            lock(localBlockLock)
            {
                if (verifyBlock(localNewBlock) == BlockVerifyStatus.Valid)
                {
                    if (localNewBlock.signatures.Count() >= consensusSignaturesRequired)
                    {
                        // accept this block, apply its transactions, recalc consensus, etc
                        applyAcceptedBlock();
                        Node.blockChain.appendBlock(localNewBlock);
                        localNewBlock = null;
                    }
                }
            }
        }

        private void applyAcceptedBlock()
        {
            
        }
        

        public void generateNewBlock()
        {
            lock(localBlockLock) {
                Console.WriteLine("GENERATING NEW BLOCK");
                if(localBlockLock != null)
                {
                    // it must have arrived just before we started creating it!
                    return;
                }
          
                // Create a new block and add all the transactions in the pool
                localNewBlock = new Block();
                lastBlockStartTime = DateTime.Now;
                localNewBlock.blockNum = Node.blockChain.getLastBlockNum() + 1;

                Console.WriteLine("\t\t|- Block Number: {0}", localNewBlock.blockNum);

                ulong total_transactions = 0;
                ulong total_amount = 0;
                lock (TransactionPool.transactions)
                {
                    foreach (var transaction in TransactionPool.transactions)
                    {
                        //Console.WriteLine("\t\t|- tx: {0}, amount: {1}", transaction.id, transaction.amount);
                        localNewBlock.addTransaction(new Transaction(transaction));
                        total_amount += transaction.amount;
                        total_transactions++;
                    }
                }
                Console.WriteLine("\t\t|- Transactions: {0} \t\t Amount: {1}", total_transactions, total_amount);

                // Calculate the block checksums and sign it
                localNewBlock.setWalletStateChecksum(WalletState.calculateChecksum());
                localNewBlock.blockChecksum = localNewBlock.calculateChecksum();
                localNewBlock.lastBlockChecksum = Node.blockChain.getLastBlockChecksum();
                localNewBlock.applySignature();

                Console.WriteLine("\t\t|- Block Checksum:\t\t {0}", localNewBlock.blockChecksum);
                Console.WriteLine("\t\t|- Last Block Checksum: \t {0}", localNewBlock.lastBlockChecksum);
                Console.WriteLine("\t\t|- WalletState Checksum:\t {0}", localNewBlock.walletStateChecksum);

                // Broadcast the new block
                ProtocolMessage.broadcastNewBlock(localNewBlock);
            }
        }


        public bool hasNewBlock()
        {
            return localNewBlock != null;
        }

        public Block getLocalBlock()
        {
            return localNewBlock;
        }
    }
}
