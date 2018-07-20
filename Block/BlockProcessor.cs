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
    class BlockProcessor
    {
        Block localNewBlock; // Block being worked on currently
        DateTime lastBlockGenerationTime;

        bool inSyncMode = false;
        bool isSynchronized = false;

        public bool synchronizing { get => inSyncMode; }
        public bool synchronized { get => isSynchronized; }

        ulong targetBlockHeight = 0;
        string targetBlockChecksum = "";
        string targetWalletStateChecksum = "";

        public BlockProcessor()
        {
            lastBlockGenerationTime = DateTime.Now;
            localNewBlock = null;
        }

        // Returns true if a block was generated
        public bool onUpdate()
        {
            if (Node.checkCurrentBlockDeprecation(localNewBlock.blockNum) == false)
            {
                return false;
            }

            if (DateTime.Now.Second % 10 == 0)
            {
                if (inSyncMode)
                {
                    Console.WriteLine("Synchronization: Block Height #{0} / #{1}", Node.blockChain.currentBlockNum, targetBlockHeight);
                    if (Node.blockChain.currentBlockNum == targetBlockHeight)
                    {
                        checkWalletState();
                    }
                    return true;
                } else
                {
                    Console.WriteLine("\n\n++++ @Block Height #{0} ++++", localNewBlock.blockNum);
                }
            }

            // check if "currently-in-progress" block is ready
            if(localNewBlock != null)
            {
                if(localNewBlock.signatures.Count() >= Node.blockChain.minimumConsensusSignatures)
                {
                    Node.blockChain.insertBlock(localNewBlock);
                    TransactionPool.applyTransactionsFromBlock(localNewBlock);
                    lastBlockGenerationTime = DateTime.Now;
                } else
                {
                    if (DateTime.Now.Second % 10 == 0)
                    {
                        Logging.warn(String.Format("Not enough signatures {0}/{1} to insert new block #{2} into blockchain. Waiting for more...",
                            localNewBlock.signatures.Count(), Node.blockChain.minimumConsensusSignatures, localNewBlock.blockNum));
                    }
                }
            } else
            {
                // check if it is time to generate a new block yet
                TimeSpan timeSinceLastBlock = DateTime.Now - lastBlockGenerationTime;
                if(timeSinceLastBlock.TotalSeconds > 30) // TODO: this value should be controlled by the network. Hardcoded for now.
                {
                    generateNewBlock();
                }
            }
            return true;
        }



        public void generateNewBlock()
        {
            Console.WriteLine("GENERATING NEW BLOCK");
          
            // Create a new block and add all the transactions in the pool
            localNewBlock = new Block();
            lock (localNewBlock)
            {
                localNewBlock.blockNum = Node.blockChain.currentBlockNum + 1;

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
                ProtocolMessage.broadcastProtocolMessage(ProtocolMessageCode.newBlock, localNewBlock.getBytes());
            }
        }

        // Checks an incoming new block
        public bool checkIncomingBlock(Block incomingBlock, Socket socket)
        {
            if(inSyncMode)
            {
                if (incomingBlock.blockNum > targetBlockHeight)
                {
                    Node.blockChain.insertTemporaryBlock(incomingBlock);
                } else
                {
                    if(incomingBlock.blockNum < Node.blockChain.currentBlockNum)
                    {
                        Node.blockChain.insertOldBlock(incomingBlock);
                    }
                }
            }

            /**************************************************************************/

            lock (localNewBlock)
            {
                // We're currently in synchronization mode
                if (inSyncMode == true)
                {
                    Console.WriteLine("Syncmode block insert: {0}", incomingBlock.blockNum);
                    if (incomingBlock.blockNum > targetBlockHeight)
                    {
                        // Add this block to a temporary location
                        Node.blockChain.insertTemporaryBlock(incomingBlock);
                    } else
                    {
                        // this is part of the set we need while synchronizing

                    }

                    return false;
                }

                // Verify the blocknum, check against what we already have
                // Todo: this part is currently for development purposes.
                if (incomingBlock.blockNum < localNewBlock.blockNum)
                {
                    // Todo: validate a previous block in the blockchain (if possible at this point)
                    //Console.WriteLine("Merging older block {0} into blockchain. Processing block is {1}", incomingBlock.blockNum, localNewBlock.blockNum);
                    Node.blockChain.insertOldBlock(incomingBlock);
                    return false;
                }


                // See if the checksum is the same as our local block
                if (incomingBlock.blockChecksum.Equals(localNewBlock.blockChecksum, StringComparison.Ordinal))
                {
                    // Verify if we already signed this block
                    bool already_signed = incomingBlock.hasNodeSignature();
                    if (already_signed)
                    {
                        if (incomingBlock.signatures.Count() > localNewBlock.signatures.Count())
                        {
                            //Console.WriteLine("Block {0} already signed and has more signatures. Re-broadcasting...", incomingBlock.blockNum);

                            ProtocolMessage.broadcastProtocolMessage(ProtocolMessageCode.newBlock, incomingBlock.getBytes(), socket);
                            localNewBlock = incomingBlock;
                            return true;
                        }

                        // Console.WriteLine("Block {0} already signed and has less signatures. Discarding.", incomingBlock.blockNum);
                        // Discard the block
                        //Console.WriteLine("Already signed. Discarding.");
                        return true;
                    }
                    else
                    {
                        // Apply our signature. 
                        // No additional checks needed as the block checksum matches the locally calculated one
                        incomingBlock.applySignature();
                        //Console.WriteLine("Incoming block #{0} = local block. Signing and broadcasting to network...", incomingBlock.blockNum);

                        // Broadcast it
                        ProtocolMessage.broadcastProtocolMessage(ProtocolMessageCode.newBlock, incomingBlock.getBytes());
                        localNewBlock = incomingBlock;
                    }

                    return false;
                }


                // Incoming block is newer
                if (incomingBlock.blockNum > localNewBlock.blockNum)
                {
                    //Console.WriteLine("Incoming block is newer, checking consensus...");
                    // If the incoming block has more than the network-defined lower limit of signatures,
                    // the block is immediately accepted as valid.
                    // In this case we've probably been delayed by external factors.
                    if (incomingBlock.signatures.Count() >= Node.blockChain.minimumConsensusSignatures)
                    {
                        // Apply our signature
                        incomingBlock.applySignature();
                        // Broadcast the block
                        ProtocolMessage.broadcastProtocolMessage(ProtocolMessageCode.newBlock, incomingBlock.getBytes());
                        localNewBlock = incomingBlock;
                        //Console.WriteLine("Accepted newer block");
                        return true;
                    }
                    //Console.WriteLine("Discarding newer block.");
                    // Otherwise discard this newer block.
                    return false;
                }

                Logging.warn(String.Format("block #{0} checksum is not equal to local block #{1}", incomingBlock.blockNum, localNewBlock.blockNum));

                // If the incoming block has more than the network-defined lower limit of signatures,
                // the block is immediately accepted as valid.
                if (incomingBlock.signatures.Count() >= Node.blockChain.minimumConsensusSignatures)
                {
                    //Console.WriteLine("block {0} is consensus approved. Accepting as valid...", incomingBlock.blockNum);
                    bool already_signed = incomingBlock.hasNodeSignature();
                    if (already_signed == false)
                    {
                        //Console.WriteLine("block {0} signed and rebroadcast.");
                        // Apply our signature
                        incomingBlock.applySignature();
                        // Broadcast the block
                        ProtocolMessage.broadcastProtocolMessage(ProtocolMessageCode.newBlock, incomingBlock.getBytes());
                    }
                    localNewBlock = incomingBlock;
                    return true;
                }


                // If the incoming block has fewer transactions than the local block, no action is taken.
                if (incomingBlock.transactions.Count() < localNewBlock.transactions.Count())
                {
                    //Console.WriteLine("block {0} has fewer transactions than the local block. No action is taken.");
                    return false;
                }


                canGenerateNewBlock = false;
                newBlockReady = false;

                // Store a local copy of the transaction pool
                List<Transaction> cached_pool;
                lock (TransactionPool.transactions)
                {
                    cached_pool = new List<Transaction>(TransactionPool.transactions);
                }

                // Check the transactions against what we have in the transaction pool
                foreach (string txid in incomingBlock.transactions)
                {
                    bool tx_found = false;
                    foreach (Transaction transaction in cached_pool)
                    {
                        if (txid.Equals(transaction.id))
                        {
                            tx_found = true;
                        }
                    }

                    // We do not have this transaction in the pool. Request it.
                    if (tx_found == false)
                    {
                        //Console.WriteLine(">>>>> Missing TX: {0}", txid);
                        //ProtocolMessage.broadcastProtocolMessage(ProtocolMessageCode.getTransaction,);
                    }
                }

                //Console.WriteLine("Accepting incoming block {1} as local block {0}", localNewBlock.blockNum, incomingBlock.blockNum);
                // Apply our signature
                incomingBlock.applySignature();
                // Broadcast the block
                ProtocolMessage.broadcastProtocolMessage(ProtocolMessageCode.newBlock, incomingBlock.getBytes());
                localNewBlock = incomingBlock;

                newBlockReady = true;
            }
            return false;
        }

        public bool hasNewBlock()
        {
            return localNewBlock != null;
        }

        public Block getLocalBlock()
        {
            return localNewBlock;
        }

        // Checks the walletstate against the target walletstate
        public void checkWalletState()
        {
            Console.Write("Checking walletstate: ");
            // Check if the current wallet state matches
            if (targetWalletStateChecksum.Equals(WalletState.calculateChecksum(), StringComparison.Ordinal))
            {
                Console.ForegroundColor = ConsoleColor.Green;                
                Console.WriteLine("Wallet state checksums match!");
                Console.ResetColor();

                exitSyncMode();
            }
            else
            {
                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.WriteLine("Wallet state checksum mismatch! Requesting walletstate synchronization.");
                Console.ResetColor();

                ProtocolMessage.broadcastProtocolMessage(ProtocolMessageCode.syncWalletState, new byte[1]);
            }
        }

        public void enterSyncMode(ulong tBlockNum, string tBlockChecksum, string tWalletStateChecksum)
        {
            Console.WriteLine("=====\nEntering blockchain synchronization mode...");

            isSynchronized = false;

            inSyncMode = true;
            targetBlockHeight = tBlockNum;
            targetBlockChecksum = tBlockChecksum;
            targetWalletStateChecksum = tWalletStateChecksum;
            localNewBlock = null;

        }

        public void exitSyncMode()
        {
            Console.WriteLine("Exiting blockchain synchronization mode...\n=====");

            // Merge all temporary blocks
            Node.blockChain.mergeTemporaryBlocks();

            targetBlockHeight = 0;
            targetBlockChecksum = "";
            targetWalletStateChecksum = "";

            inSyncMode = false;
            isSynchronized = true;
        }

    }
}
