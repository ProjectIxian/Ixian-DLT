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
        DateTime lastProcessedTime;
        bool newBlockReady;
        Block localNewBlock; // Locally generated new block


        public bool inSyncMode = false;

        ulong targetBlockHeight = 0;
        string targetBlockChecksum = "";
        string targetWalletStateChecksum = "";

        bool canGenerateNewBlock = true;

        public bool synchronized = false;

        public BlockProcessor()
        {
            lastProcessedTime = DateTime.Now;
            newBlockReady = false;
            localNewBlock = null;
            canGenerateNewBlock = true;
        }

        // Returns true if a block was generated
        public bool onUpdate()
        {
            DateTime now_time = DateTime.Now;
            int seconds = DateTime.Now.Second;

            // Only perform a block update at :00s and :30s of each minute 
            if (seconds == 0 || seconds == 30)
            {
                if (localNewBlock != null)
                {
                    // Deprecation support
                    if (Node.checkCurrentBlockDeprecation(localNewBlock.blockNum) == false)
                        return false;

                    Console.WriteLine("\n\n++++ @Block Height #{0} ++++", localNewBlock.blockNum);
                }
                // Check if we're still in synchronization mode. If so, do not generate a new block.
                if(inSyncMode)
                {
                    Console.WriteLine("Synchronization: Block Height #{0} / #{1}", Node.blockChain.currentBlockNum, targetBlockHeight);

                    if (Node.blockChain.currentBlockNum == targetBlockHeight)
                        checkWalletState();

                    return true;
                }

                // Don't generate new blocks if we haven't synchronized yet and we're not on the genesis node.
                if(synchronized == false && Node.genesisNode == false)
                {
                    Logging.warn(String.Format("Blockchain not synchronized to network. Please connect to a valid node."));
                    return true;
                }


                // If the new block is ready, add it to the blockchain and update the walletstate
                if(newBlockReady == true)
                {
                    if(localNewBlock.signatures.Count() < Node.blockChain.minimumConsensusSignatures && localNewBlock.blockNum > 1)
                    {
                        Logging.warn(String.Format("Not enough signatures {0}/{1} to insert new block #{2} into blockchain. Waiting for more...", 
                            localNewBlock.signatures.Count(), Node.blockChain.minimumConsensusSignatures, localNewBlock.blockNum));

                        // Request the block from the network
                        using (MemoryStream mw = new MemoryStream())
                        {
                            using (BinaryWriter writerw = new BinaryWriter(mw))
                            {
                                writerw.Write(localNewBlock.blockNum);
                                writerw.Write(false);

                                ProtocolMessage.broadcastProtocolMessage(ProtocolMessageCode.getBlock, mw.ToArray());
                                //NetworkClientManager.restartClients();
                            }
                        }

                        return false;
                    }

                    // Insert the block into the blockchain
                    Node.blockChain.insertBlock(localNewBlock);

                    // Update the walletstate
                    TransactionPool.applyTransactionsFromBlock(localNewBlock);

                    canGenerateNewBlock = true;
                    newBlockReady = false;
                }
                else
                {

                }

                // Check if we're ready to generate a new block.
                // Depending on network conditions, block generation might be delayed
                if (canGenerateNewBlock == false)
                    return false;

                //Logging.info(String.Format("TxPool contains {0} transactions", TransactionPool.activeTransactions));

                // Generate a new block
                generateNewBlock();

                return true;

            }

            return false;
        }



        public void generateNewBlock()
        {
            Console.WriteLine("GENERATING NEW BLOCK");

            newBlockReady = false;
            canGenerateNewBlock = false;
          
            // Create a new block and add all the transactions in the pool
            localNewBlock = new Block();
            lock (localNewBlock)
            {
                localNewBlock.blockNum = Node.blockChain.currentBlockNum + 1;

                Console.WriteLine("\t\t|- Block Number: {0}", localNewBlock.blockNum);

                ulong total_transactions = 0;
                IxiNumber total_amount = new IxiNumber();
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

                newBlockReady = true;

                // Broadcast the new block
                ProtocolMessage.broadcastProtocolMessage(ProtocolMessageCode.newBlock, localNewBlock.getBytes());
            }
        }

        // Checks an incoming new block
        public bool checkIncomingBlock(Block incomingBlock, Socket socket)
        {
            //Logging.info(string.Format("Incoming block #{0}...", incomingBlock.blockNum));

            // We have no local block generated yet. Possibly fresh start of node
            if (localNewBlock == null)
            {
                Console.WriteLine("Block Processor: No localblock yet. Starting blockchain.");
                setInitialLocalBlock(incomingBlock);
                return false;
            }

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

        // Used to initialize the partial blockchain and set the local block if not processed yet
        public void setInitialLocalBlock(Block incomingBlock)
        {
            // Check if we've reached our target block
            if (incomingBlock.blockNum == targetBlockHeight)
            {
                // Check if the block checksum matches
                if (incomingBlock.blockChecksum.Equals(targetBlockChecksum, StringComparison.Ordinal))
                {
                    Console.WriteLine("Target block reached, checksums match!");

                    // Check if we have an accurate walletstate (unlikely at this point, but still possible)
                    checkWalletState();

                }
            }

            localNewBlock = new Block(incomingBlock);
            localNewBlock.applySignature();

            newBlockReady = false;
            canGenerateNewBlock = true;

            Node.blockChain.insertBlock(localNewBlock);

            // Broadcast the signed block
            ProtocolMessage.broadcastProtocolMessage(ProtocolMessageCode.newBlock, localNewBlock.getBytes());
        }



        public bool hasNewBlock()
        {
            return newBlockReady;
        }

        public Block getNewBlock()
        {
            if (newBlockReady == false)
                return null;

            newBlockReady = false;
            return localNewBlock;
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

        // Signals that we're entering synchronization mode
        public void enterSyncMode(ulong tBlockNum, string tBlockChecksum, string tWalletStateChecksum)
        {
            Console.WriteLine("=====\nEntering blockchain synchronization mode...");

            synchronized = false;

            inSyncMode = true;
            targetBlockHeight = tBlockNum;
            targetBlockChecksum = tBlockChecksum;
            targetWalletStateChecksum = tWalletStateChecksum;
            localNewBlock = null;
            newBlockReady = false;
            canGenerateNewBlock = false;

        }

        // Signals that we're exiting synchronization mode
        public void exitSyncMode()
        {
            Console.WriteLine("Exiting blockchain synchronization mode...\n=====");

            // Merge all temporary blocks
            Node.blockChain.mergeTemporaryBlocks();


            inSyncMode = false;
            canGenerateNewBlock = true;

            targetBlockHeight = 0;
            targetBlockChecksum = "";
            targetWalletStateChecksum = "";

            synchronized = true;
        }

    }
}
