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
        int blockGenerationInterval = 300; // in seconds

        public BlockProcessor()
        {
            lastBlockStartTime = DateTime.Now;
            localNewBlock = null;
            // we start up in sync mode (except for genesis block)
            inSyncMode = Config.genesisFunds == 0;
            syncTargetBlockNum = 0;
        }

        // Returns true if a block was generated
        public bool onUpdate()
        {
            if (inSyncMode)
            {
                if(syncTargetBlockNum == 0)
                {
                    // we haven't connected to any clients yet
                    return true;
                }
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
                    // we keep this locked so that onBlockReceive can't change syncTarget while we're processing
                    if (syncTargetBlockNum > 0 && Node.blockChain.getLastBlockNum() == syncTargetBlockNum)
                    {
                        inSyncMode = false;
                        lock (pendingBlocks)
                        {
                            pendingBlocks.Clear();
                        }
                        return true;
                    }
                }
            } else // !inSyncMode
            {
                // check if it is time to generate a new block
                TimeSpan timeSinceLastBlock = DateTime.Now - lastBlockStartTime;
                Logging.info(String.Format("Last block was at: {0}. That is {1} seconds in the past. {2} seconds to go.",
                    lastBlockStartTime.ToLongTimeString(),
                    timeSinceLastBlock.TotalSeconds,
                    blockGenerationInterval - timeSinceLastBlock.TotalSeconds));
                if(timeSinceLastBlock.TotalSeconds > blockGenerationInterval || Node.forceNextBlock)
                {
                    if(Node.forceNextBlock)
                    {
                        Logging.info("Forcing new block generation");
                        Node.forceNextBlock = false;
                    }
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
                            if(b.blockNum > syncTargetBlockNum)
                            {
                                // we move the goalpost to make sure we end up in the valid state
                                syncTargetBlockNum = b.blockNum;
                            }
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
                if(b.blockNum < Node.blockChain.getLastBlockNum())
                {
                    Node.blockChain.refreshSignatures(b);
                }
            }
        }

        public BlockVerifyStatus verifyBlock(Block b)
        {
            // Check all transactions in the block against our TXpool, make sure all is legal
            // Note: it is possible we don't have all the required TXs in our TXpool - in this case, request the missing ones and return Intederminate
            bool hasAllTransactions = true;
            Dictionary<string, ulong> minusBalances = new Dictionary<string, ulong>();
            foreach (string txid in b.transactions)
            {
                Transaction t = TransactionPool.getTransaction(txid);
                if (t == null)
                {
                    hasAllTransactions = false;
                    continue;
                }
                if (!minusBalances.ContainsKey(t.from))
                {
                    minusBalances.Add(t.from, 0);
                }
                try
                {
                    checked
                    {
                        ulong new_minus_balance = minusBalances[t.from] + t.amount;
                        minusBalances[t.from] = new_minus_balance;
                    }
                }
                catch (OverflowException)
                {
                    // someone is doing something bad with this transaction, so we invalidate the block
                    // TODO: Blacklisting for the transaction originator node
                    Logging.warn(String.Format("Overflow caused by transaction {0}: amount: {1} from: {2}, to: {3}",
                        t.id, t.amount, t.from, t.to));
                    return BlockVerifyStatus.Invalid;
                }
            }
            // overspending:
            foreach(string addr in minusBalances.Keys)
            {
                ulong initial_balance = WalletState.getBalanceForAddress(addr);
                if(initial_balance < minusBalances[addr])
                {
                    Logging.warn(String.Format("Address {0} is attempting to overspend: Balance: {1}, Total Outgoing: {2}.",
                        addr, initial_balance, minusBalances[addr]));
                    return BlockVerifyStatus.Invalid;
                }
            }
            //
            if (!hasAllTransactions)
            {
                Logging.info(String.Format("Block #{0} is missing some transactions, which have been requested from the network.", b.blockNum));
                return BlockVerifyStatus.Indeterminate;
            }
            // Verify checksums
            string checksum = b.calculateChecksum();
            if(b.blockChecksum != checksum)
            {
                Logging.warn(String.Format("Block verification failed for #{0}. Checksum is {1}, but should be {2}.",
                    b.blockNum, b.blockChecksum, checksum));
                return BlockVerifyStatus.Invalid;
            }
            // Verify signatures
            if(!b.verifySignatures())
            {
                Logging.warn(String.Format("Block #{0} failed while verifying signatures. There are invalid signatures on the block.", b.blockNum));
                return BlockVerifyStatus.Invalid;
            }
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
                    // TODO: we will need an edge case here in the event that too many nodes dropped and consensus
                    // can no longer be reached according to this number - I don't have a clean answer yet - MZ
                    if (localNewBlock.signatures.Count() >= consensusSignaturesRequired)
                    {
                        // accept this block, apply its transactions, recalc consensus, etc
                        applyAcceptedBlock();
                        Node.blockChain.appendBlock(localNewBlock);
                        Logging.info(String.Format("Accepted block #{0}: {1}.", localNewBlock.blockNum, localNewBlock.blockChecksum));
                        localNewBlock = null;
                    }
                }
            }
        }

        private void applyAcceptedBlock()
        {
            TransactionPool.applyTransactionsFromBlock(localNewBlock);
            int n1Sigs = Node.blockChain.getBlockSignaturesReverse(1);
            int n2Sigs = Node.blockChain.getBlockSignaturesReverse(2);
            if(n1Sigs != 0 && n2Sigs != 0)
            {
                int targetSigs = (n1Sigs + n2Sigs) / 2;
                // amortization for consensus sigs
                int delta = (targetSigs - consensusSignaturesRequired) / 2;
                consensusSignaturesRequired += delta;

            }

        }
        

        public void generateNewBlock()
        {
            lock(localBlockLock) {
                Console.WriteLine("GENERATING NEW BLOCK");
                if(localNewBlock != null)
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

                Transaction[] poolTransactions = TransactionPool.getAllTransactions();
                foreach (var transaction in poolTransactions)
                {
                    //Console.WriteLine("\t\t|- tx: {0}, amount: {1}", transaction.id, transaction.amount);
                    localNewBlock.addTransaction(transaction);
                    total_amount += transaction.amount;
                    total_transactions++;
                }
                Console.WriteLine("\t\t|- Transactions: {0} \t\t Amount: {1}", total_transactions, total_amount);

                // Calculate the block checksums and sign it
                localNewBlock.setWalletStateChecksum(WalletState.calculateChecksum());
                localNewBlock.lastBlockChecksum = Node.blockChain.getLastBlockChecksum();
                localNewBlock.blockChecksum = localNewBlock.calculateChecksum();
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
