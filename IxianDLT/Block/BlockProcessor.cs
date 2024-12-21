﻿// Copyright (C) 2017-2024 Ixian OU
// This file is part of Ixian DLT - www.github.com/ProjectIxian/Ixian-DLT
//
// Ixian DLT is free software: you can redistribute it and/or modify
// it under the terms of the MIT License as published
// by the Open Source Initiative.
//
// Ixian DLT is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// MIT License for more details.

using DLT.Meta;
using DLT.Network;
using IXICore;
using IXICore.Inventory;
using IXICore.Meta;
using IXICore.Network;
using IXICore.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Text;
using System.Threading;

namespace DLT
{
    enum BlockVerifyStatus
    {
        Valid,
        Invalid,
        Indeterminate,
        IndeterminateFutureBlock,
        IndeterminatePastBlock,
        AlreadyProcessed,
        PotentiallyForkedBlock,
        IndeterminateVersionUpgradeBlock
    }
    class BlockProcessor
    {
        public bool operating { get; private set; }
        public ulong firstSplitOccurence { get; private set; }

        public Block localNewBlock; // Block being worked on currently
        public readonly object localBlockLock = new object(); // used because localNewBlock can change while this lock should be held.
        public DateTime lastBlockStartTime;
        public DateTime currentBlockStartTime;

        object superBlockLock = new object();
        Dictionary<ulong, SuperBlockSegment> cache_currentSuperBlockSegments = null; // Current superblock being worked on currently
        ulong cache_lastSuperBlockNum = 0;
        byte[] cache_lastSuperBlockChecksum = null;

        long lastUpgradeTry = 0;

        int blockGenerationInterval = ConsensusConfig.blockGenerationInterval;

        long averageBlockGenerationInterval = ConsensusConfig.blockGenerationInterval;

        public bool firstBlockAfterSync;

        private SortedList<ulong, long> fetchingTxForBlocks = new SortedList<ulong, long>();
        private SortedList<ulong, long> fetchingBulkTxForBlocks = new SortedList<ulong, long>();

        private Thread block_thread = null;

        public ulong highestNetworkBlockNum = 0;

        private Dictionary<ulong, Dictionary<byte[], DateTime>> blockBlacklist = new Dictionary<ulong, Dictionary<byte[], DateTime>>();

        Dictionary<ulong, Block> pendingSuperBlocks = new Dictionary<ulong, Block>();

        public bool networkUpgraded = false;

        private ThreadLiveCheck TLC;

        private bool forkedFlag = false; // flag that indicates if the next block is forked

        private int randomInt = 0;

        public BlockProcessor()
        {
            lastBlockStartTime = DateTime.UtcNow;
            localNewBlock = null;
            operating = false;
            firstBlockAfterSync = false;
            randomInt = new Random().Next(999);
        }

        public void resumeOperation()
        {
            Logging.info("BlockProcessor resuming normal operation.");
            operating = true;

            lock (localBlockLock)
            {

                // Abort the thread if it's already created
                if (block_thread != null)
                {
                    block_thread.Interrupt();
                    block_thread.Join();
                }

                TLC = new ThreadLiveCheck();
                // Start the thread
                block_thread = new Thread(onUpdate);
                block_thread.Name = "Block_Processor_Update_Thread";
                block_thread.Start();
            }
        }

        public void stopOperation()
        {
            operating = false;
            Logging.info("BlockProcessor stopped.");
        }

        public void handleForkedFlag()
        {
            if (!Config.disableChainReorg)
            {
                lock(localBlockLock)
                {
                    lastBlockStartTime = DateTime.UtcNow;
                    forkedFlag = false;
                    blacklistBlock(Node.blockChain.getLastBlock());
                    Node.blockChain.revertLastBlock();
                }
                BlockProtocolMessages.broadcastGetBlock(Node.blockChain.getLastBlockNum() + 1, null, null);
            }
        }

        // Check passed time since last block generation and if needed generate a new block
        public void onUpdate()
        {
            try
            {
                lastBlockStartTime = DateTime.UtcNow.AddSeconds(-blockGenerationInterval * 10);

                while (operating)
                {
                    TLC.Report();
                    bool sleep = false;
                    try
                    {

                        lock (localBlockLock)
                        {
                            // check if it is time to generate a new block
                            TimeSpan timeSinceLastBlock = DateTime.UtcNow - lastBlockStartTime;

                            if (timeSinceLastBlock.TotalSeconds < 0)
                            {
                                // edge case, system time apparently changed
                                lastBlockStartTime = DateTime.UtcNow.AddSeconds(-blockGenerationInterval * 10);
                                timeSinceLastBlock = DateTime.UtcNow - lastBlockStartTime;
                                lock (blockBlacklist)
                                {
                                    blockBlacklist.Clear();
                                }
                                // TODO TODO check if there's anything else that we should clear in such scenario - perhaps add a global handler for this edge case
                            }

                            bool generateNextBlock = Node.forceNextBlock;

                            ulong last_block_num = Node.blockChain.getLastBlockNum();
                            int block_version = Node.blockChain.getLastBlockVersion();

                            if (block_version < Config.maxBlockVersionToGenerate
                                && (last_block_num + 1) % ConsensusConfig.superblockInterval == 0)
                            {
                                block_version = Config.maxBlockVersionToGenerate;
                            }

                            if (generateNextBlock)
                            {
                                localNewBlock = null;
                            }
                            else
                            {
                                // First 7 blocks should be generated only by genesis node
                                if (localNewBlock == null)
                                {
                                    if (last_block_num > 7 || Node.genesisNode)
                                    {
                                        if (timeSinceLastBlock.TotalSeconds > (blockGenerationInterval * 4) + randomInt / 100) // no block for 4 block times + random seconds, we don't want all nodes sending at once
                                        {
                                            generateNextBlock = true;
                                        }
                                        else
                                        {
                                            Block last_block = Node.blockChain.getLastBlock();
                                            if (last_block == null || Clock.getNetworkTimestamp() - last_block.timestamp >= blockGenerationInterval)
                                            {
                                                if (last_block_num < 8 || Node.isElectedToGenerateNextBlock())
                                                {
                                                    generateNextBlock = true;
                                                }
                                            }
                                        }
                                    }
                                    else
                                    {
                                        BlockProtocolMessages.broadcastGetBlock(last_block_num + 1);
                                    }
                                }

                                // if the node is stuck on the same block for too long, discard the block
                                if (localNewBlock != null && timeSinceLastBlock.TotalSeconds > (blockGenerationInterval * 20))
                                {
                                    blacklistBlock(localNewBlock);
                                    localNewBlock = null;
                                    lastBlockStartTime = DateTime.UtcNow.AddSeconds(-blockGenerationInterval * 10);
                                    sleep = true;
                                    if (forkedFlag)
                                    {
                                        handleForkedFlag();
                                    }
                                    else
                                    {
                                        generateNextBlock = true;
                                    }
                                }
                            }


                            //Logging.info(String.Format("Waiting for {0} to generate the next block #{1}. offset {2}", Node.blockChain.getLastElectedNodePubKey(getElectedNodeOffset()), Node.blockChain.getLastBlockNum()+1, getElectedNodeOffset()));
                            if (generateNextBlock)
                            {
                                if (lastUpgradeTry > 0 && Clock.getTimestamp() - lastUpgradeTry < blockGenerationInterval * 120)
                                {
                                    block_version = Node.blockChain.getLastBlockVersion();
                                }
                                else
                                {
                                    lastUpgradeTry = 0;
                                }

                                if (Node.forceNextBlock)
                                {
                                    Logging.info("Forcing new block generation");
                                    Node.forceNextBlock = false;
                                }

                                generateNewBlock(block_version);
                            }
                            else
                            {
                                if (localNewBlock != null)
                                {
                                    if (Node.isMasterNode())
                                    {
                                        if ((DateTime.UtcNow - currentBlockStartTime).TotalSeconds > (ConsensusConfig.blockGenerationInterval / 2) && localNewBlock.signatures.Count() < Node.blockChain.getRequiredConsensus())
                                        {
                                            if (last_block_num < 10)
                                            {
                                                BlockProtocolMessages.broadcastNewBlock(localNewBlock);
                                            }
                                            Logging.info("Waiting for local block #{0} to reach consensus {1}/{2}.", localNewBlock.blockNum, localNewBlock.signatures.Count, Node.blockChain.getRequiredConsensus());
                                            sleep = true;
                                        }
                                    }
                                    if (localNewBlock.version > Node.blockChain.getLastBlockVersion())
                                    {
                                        lastUpgradeTry = Clock.getTimestamp();
                                    }
                                }
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        Logging.error("Exception occurred in blockProcessor onUpdate() {0}", e);
                    }
                    // Sleep until next iteration
                    if (sleep)
                    {
                        Thread.Sleep(10000 + randomInt);
                    }
                    else
                    {
                        Thread.Sleep(1000 + randomInt);
                    }
                }
            }
            catch (ThreadInterruptedException)
            {

            }
            catch (Exception e)
            {
                Console.WriteLine("OnUpdate exception: {0}", e);
            }
            return;
        }

        // returns offset depending on time since last block and block generation interval. This function will return -1 if more than 10 block generation intervals have passed
        public int getElectedNodeOffset()
        {
            Block b = Node.blockChain.getLastBlock();
            if(b == null)
            {
                return -1;
            }
            long timeSinceLastBlock = Clock.getNetworkTimestamp() - b.timestamp;
            if(timeSinceLastBlock < 0)
            {
                return 0;
            }
            if(timeSinceLastBlock > blockGenerationInterval * 4) // edge case, if network is stuck for more than 4 block times always return -1 as the node offset.
            {
                return -1;
            }
            return (int)(timeSinceLastBlock / (blockGenerationInterval*2));
        }

        public List<BlockSignature> getSignaturesWithoutPlEntry(Block b)
        {
            List<BlockSignature> sigs = new List<BlockSignature>();

            for (int i = 0; i < b.signatures.Count; i++)
            {
                BlockSignature sig = b.signatures[i];

                Presence p = PresenceList.getPresenceByAddress(sig.recipientPubKeyOrAddress);
                if (p != null)
                {
                    bool masterEntryFound = false;
                    lock (p)
                    {
                        foreach (PresenceAddress pa in p.addresses)
                        {
                            if (pa.type == 'M' || pa.type == 'H')
                            {
                                masterEntryFound = true;
                                break;
                            }
                        }
                    }
                    if (!masterEntryFound)
                    {
                        p = null;
                    }
                }

                //Logging.info(String.Format("Searching for {0}", parts[1]));                 
                if (p == null)
                {
                    sigs.Add(sig);
                    continue;
                }
            }
            return sigs;
        }

        public bool removeSignaturesWithoutPlEntry(Block b)
        {
            List<BlockSignature> sigs = getSignaturesWithoutPlEntry(b);
            for (int i = 0; i < sigs.Count; i++)
            {
                // Don't remove block proposer's signature
                if(b.blockProposer != null && sigs[i].recipientPubKeyOrAddress.addressNoChecksum.SequenceEqual(b.blockProposer))
                {
                    continue;
                }
                b.signatures.Remove(sigs[i]);
            }
            if (sigs.Count > 0)
            {
                return true;
            }
            return false;
        }

        // Checks if the block has been sigFreezed and if all the hashes match, returns false if the block shouldn't be processed further
        public bool handleSigFreezedBlock(Block b, bool verifySigs, RemoteEndpoint endpoint = null)
        {
            Block sigFreezingBlock = Node.blockChain.getBlock(b.blockNum + 5);
            byte[] sigFreezeChecksum = null;
            lock (localBlockLock)
            {
                if (sigFreezingBlock == null && localNewBlock != null && localNewBlock.blockNum == b.blockNum + 5)
                {
                    sigFreezingBlock = localNewBlock;
                }
                if (sigFreezingBlock != null)
                {
                    sigFreezeChecksum = sigFreezingBlock.signatureFreezeChecksum;
                    // this block already has a sigfreeze, don't tamper with the signatures
                    Block targetBlock = Node.blockChain.getBlock(b.blockNum);
                    if(targetBlock == null)
                    {
                        Logging.error("Target block #{0} ({1}) is null, cannot handle sig freeze.", b.blockNum, Crypto.hashToString(b.blockChecksum));
                        return false;
                    }
                    if(b.blockProposer == null && b.version < BlockVer.v10)
                    {
                        b.blockProposer = targetBlock.blockProposer;
                    }
                    if (targetBlock != null && sigFreezeChecksum.SequenceEqual(targetBlock.calculateSignatureChecksum()) && targetBlock.verifyBlockProposer())
                    {
                        // we already have the correct block
                        return false;
                    }
                    if (sigFreezeChecksum.SequenceEqual(b.calculateSignatureChecksum()) && b.verifyBlockProposer())
                    {
                        Logging.warn("Received block #{0} ({1}) which was sigFreezed with correct checksum, force updating signatures locally!", b.blockNum, Crypto.hashToString(b.blockChecksum));
                        targetBlock.addSignaturesFrom(b, verifySigs);
                        if (verifyBlockSignatures(b, endpoint))
                        {
                            // this is likely the correct block, update and broadcast to others
                            if(Node.blockChain.refreshSignatures(b, true, endpoint))
                            {
                                if (sigFreezingBlock == localNewBlock)
                                {
                                    acceptLocalNewBlock();
                                }
                            }
                        }
                        else
                        {
                            // the block is invalid, we should disconnect, most likely a malformed block - somebody removed signatures
                            // TODO TODO TODO TODO it's too agressive to disconnect here, it's better if we disconnect the node as part of node behaviour monitoring

                            Logging.warn("Target block " + b.blockNum + " does not have the required consensus.");
                            //CoreProtocolMessage.sendBye(endpoint, ProtocolByeCode.blockInvalidNoConsensus, "Block #" + b.blockNum + " is invalid", b.blockNum.ToString());
                            localNewBlock = null;
                        }
                        return false;
                    }
                    else
                    {
                        Logging.warn("Received block #{0} ({1}) which was sigFreezed but has an incorrect sigfreeze checksum, re-requesting the block from the network!", b.blockNum, Crypto.hashToString(b.blockChecksum));
                        BlockProtocolMessages.broadcastGetBlock(b.blockNum, endpoint, null);
                        return false;
                    }
                }
            }
            return true;
        }

        public bool verifySigFreezedBlock(Block b)
        {
            Block sigFreezingBlock = Node.blockChain.getBlock(b.blockNum + 5);
            byte[] sigFreezeChecksum = null;
            lock (localBlockLock)
            {
                if (sigFreezingBlock == null && localNewBlock != null && localNewBlock.blockNum == b.blockNum + 5)
                {
                    sigFreezingBlock = localNewBlock;
                }
                if (sigFreezingBlock != null)
                {
                    sigFreezeChecksum = sigFreezingBlock.signatureFreezeChecksum;
                    if (sigFreezeChecksum.SequenceEqual(b.calculateSignatureChecksum()))
                    {
                        return true;
                    }
                    else
                    {
                        return false;
                    }
                }
            }
            return true;
        }

        public Block getPendingSuperBlock(byte[] block_checksum)
        {
            var sb_list = pendingSuperBlocks.Where(x => x.Value.blockChecksum.SequenceEqual(block_checksum));
            if(sb_list.Count() == 0)
            {
                return null;
            }
            return sb_list.First().Value;
        }

        private bool onSuperBlockReceived(Block b, RemoteEndpoint endpoint = null)
        {
            if(b.version < BlockVer.v5) // super blocks were supported with block v5
            {
                return true;
            }

            var local_block_list = pendingSuperBlocks.Where(x => x.Value.blockChecksum.SequenceEqual(b.blockChecksum));
            if (local_block_list.Count() > 0)
            {
                if (IxianHandler.getLastBlockHeight() + 1 == b.blockNum)
                {
                    generateSuperBlockSegments(b, false, endpoint);
                }
                return true;
            }

            if (b.lastSuperBlockChecksum == null)
            {
                // block is not a superblock
                if(b.blockNum % ConsensusConfig.superblockInterval == 0)
                {
                    // block was supposed to be a superblock
                    Logging.warn("Received a normal block {0}, which was supposed to be a super block.", b.blockNum);
                    return false;
                }
                return true;
            }

            // block is a superblock
            if (b.blockNum % ConsensusConfig.superblockInterval != 0)
            {
                // block was not supposed to be a superblock
                Logging.warn("Received a super block {0}, which was supposed to be a normal block.", b.blockNum);
                return false;
            }

            Block last_super_block = Node.blockChain.getSuperBlock(b.lastSuperBlockNum);
            if(last_super_block != null)
            {
                if (!last_super_block.blockChecksum.SequenceEqual(b.lastSuperBlockChecksum))
                {
                    Logging.warn("Received a forked super block {0}.", b.blockNum);
                    return false;
                }else if(last_super_block.lastSuperBlockChecksum == null && last_super_block.blockNum > 1)
                {
                    Logging.warn("Received a forked superblock that points to a last superblock, which isn't a superblock {0}.", b.blockNum);
                    return false;
                }
            }
            else
            {
                // TODO TODO handle this and getLastSuperBlockChecksum()
                /*byte[] last_accepted_super_block_checksum = Node.blockChain.getLastSuperBlockChecksum();
                if (getPendingSuperBlock(last_accepted_super_block_checksum) == null)
                {
                    Logging.info("Received a future super block {0}.", b.blockNum);
                    //ProtocolMessage.broadcastGetNextSuperBlock(Node.blockChain.getLastSuperBlockNum(), last_accepted_super_block_checksum, 0, null, null);
                }*/
                BlockProtocolMessages.broadcastGetBlock(Node.blockChain.getLastBlockNum() + 1, null, null, 0, false);
                return false;
            }

            if (IxianHandler.getLastBlockHeight() + 1 == b.blockNum)
            {
                // next block
                generateSuperBlockSegments(b, false, endpoint);
            }else if(IxianHandler.getLastBlockHeight() + 1 > b.blockNum)
            {
                // already processed block
                Block local_block = Node.blockChain.getBlock(b.blockNum, true, true);
                if(local_block == null)
                {
                    // this should never happen
                    Logging.error("Received a superblock {0} that we're supposed to have but don't.", b.blockNum);
                }

                b.superBlockSegments = local_block.superBlockSegments;
            }else
            {
                // TODO TODO TODO implement for superblock catch-up
                return false;
            }
            return true;
        }

        public void onBlockReceived(Block b, RemoteEndpoint endpoint = null)
        {
            if (operating == false) return;
            //Logging.info(String.Format("Received block #{0} {1} ({2} sigs) from the network.", b.blockNum, Crypto.hashToString(b.blockChecksum), b.getFrozenSignatureCount()));

            if(isBlockBlacklisted(b))
            {
                Logging.info("Received block #{0} {1} ({2} sigs) from the network which has been blacklisted.", b.blockNum, Crypto.hashToString(b.blockChecksum), b.getFrozenSignatureCount());
                return;
            }

            if (!onSuperBlockReceived(b, endpoint))
            {
                return;
            }

            if (b.lastSuperBlockChecksum != null)
            {
                b.blockChecksum = b.calculateChecksum();
            }

            // if historic block, only the sigs should be updated if not older than 5 blocks in history
            if (b.blockNum <= Node.blockChain.getLastBlockNum())
            {
                if (b.blockNum + 5 > Node.blockChain.getLastBlockNum())
                {
                    Logging.info(String.Format("Already processed block #{0}, doing basic verification and collecting only sigs if relevant!", b.blockNum));
                    Block localBlock = Node.blockChain.getBlock(b.blockNum);
                    lock (localBlock)
                    {
                        BlockVerifyStatus block_status = verifyBlockBasic(b, true, endpoint);
                        if (b.blockChecksum.SequenceEqual(localBlock.blockChecksum) && block_status == BlockVerifyStatus.Valid)
                        {
                            if (handleSigFreezedBlock(b, false, endpoint))
                            {
                                if (b.blockNum + 4 > Node.blockChain.getLastBlockNum())
                                {
                                    Block block_to_update = Node.blockChain.getBlock(b.blockNum);
                                    if (!block_to_update.calculateSignatureChecksum().SequenceEqual(b.calculateSignatureChecksum()))
                                    {
                                        removeSignaturesWithoutPlEntry(b);
                                        if (!Node.blockChain.refreshSignatures(b) && b.getFrozenSignatureCount() < block_to_update.getFrozenSignatureCount())
                                        {
                                            BlockProtocolMessages.broadcastNewBlock(block_to_update, null, endpoint); // TODO TODO TODO this can be optimized, to only send new sigs
                                        }
                                    }
                                }
                            }
                        }
                        else if(!b.blockChecksum.SequenceEqual(localBlock.blockChecksum) && block_status == BlockVerifyStatus.Valid)
                        {
                            Logging.info("Block #{0} ({1}) is forked", b.blockNum, Crypto.hashToString(b.blockChecksum));
                            if (Node.isMasterNode())
                            {
                                if (!forkedFlag)
                                {
                                    if (b.getFrozenSignatureCount() >= Node.blockChain.getRequiredConsensus(b.blockNum))
                                    {
                                        forkedFlag = true;
                                    }
                                    BlockProtocolMessages.broadcastNewBlock(localBlock, null, endpoint);
                                }
                            }else
                            {
                                handleForkedFlag();
                            }
                            // TODO: Blacklisting point
                        }
                        else if(block_status == BlockVerifyStatus.Invalid)
                        {
                            Logging.info("Block #{0} ({1}) is invalid", b.blockNum, Crypto.hashToString(b.blockChecksum));
                        }else if(block_status == BlockVerifyStatus.PotentiallyForkedBlock)
                        {
                            Logging.info("Block #{0} ({1}) is forked", b.blockNum, Crypto.hashToString(b.blockChecksum));
                            if(Node.isMasterNode())
                            {
                                if (!forkedFlag)
                                {
                                    if (b.getFrozenSignatureCount() >= Node.blockChain.getRequiredConsensus(b.blockNum))
                                    {
                                        forkedFlag = true;
                                    }
                                    BlockProtocolMessages.broadcastNewBlock(localBlock, null, endpoint);
                                }
                            }
                            else
                            {
                                handleForkedFlag();
                            }
                        }
                    }
                }else // b.blockNum < Node.blockChain.getLastBlockNum() - 5
                {
                    BlockVerifyStatus past_block_status = verifyBlock(b, true, null);
                    if(past_block_status == BlockVerifyStatus.AlreadyProcessed || past_block_status == BlockVerifyStatus.Valid)
                    {
                        // likely the node is missing sigs or has his very own custom block, let's send our block and also send the latest block, since he's obviously falling behind
                        Block block = Node.blockChain.getBlock(b.blockNum);
                        if (!b.Equals(block))
                        {
                            BlockProtocolMessages.broadcastNewBlock(block, null, endpoint);
                            BlockProtocolMessages.broadcastNewBlock(Node.blockChain.getBlock(IxianHandler.getLastBlockHeight()), null, endpoint);
                        }
                    }
                    else if(past_block_status == BlockVerifyStatus.IndeterminatePastBlock)
                    {
                        // the node seems to be way behind, send the current last block
                        BlockProtocolMessages.broadcastNewBlock(Node.blockChain.getBlock(IxianHandler.getLastBlockHeight()), null, endpoint);
                    }else if(past_block_status == BlockVerifyStatus.PotentiallyForkedBlock)
                    {
                        Block localBlock = Node.blockChain.getBlock(b.blockNum);
                        BlockProtocolMessages.broadcastNewBlock(localBlock, null, endpoint);
                        // TODO: Blacklisting point
                    }
                    else
                    {
                        // the block is invalid, we should disconnect the node as it is likely on a forked network
                        Logging.error("Disconnecting node {0} which sent an invalid block #{1}", endpoint.getFullAddress(true), b.blockNum);
                        CoreProtocolMessage.sendBye(endpoint, ProtocolByeCode.blockInvalid, "Block #" + b.blockNum + " is invalid, you are possibly on a non-compatible network", b.blockNum.ToString());
                    }
                }
                return;
            }

            b.powField = null;

            BlockVerifyStatus b_status;

            lock (localBlockLock)
            {
                if (localNewBlock != null && localNewBlock.blockChecksum.SequenceEqual(b.blockChecksum))
                {
                    b_status = verifyBlockBasic(b, true, endpoint);
                }else
                {
                    b_status = verifyBlock(b, false, endpoint);
                }
            }

            if(b_status == BlockVerifyStatus.Invalid)
            {
                // the block is invalid, we should disconnect the node as it is likely on a forked network
                Logging.info("Block #{0} ({1}) is invalid", b.blockNum, Crypto.hashToString(b.blockChecksum));
                return;
            }
            else if (b_status == BlockVerifyStatus.PotentiallyForkedBlock)
            {
                Logging.info("Block #{0} ({1}) is forked", b.blockNum, Crypto.hashToString(b.blockChecksum));
                if (Node.isMasterNode())
                {
                    if (!forkedFlag)
                    {
                        if (b.getFrozenSignatureCount() >= Node.blockChain.getRequiredConsensus(b.blockNum))
                        {
                            forkedFlag = true;
                        }
                        BlockProtocolMessages.broadcastNewBlock(Node.blockChain.getLastBlock(), null, endpoint);
                    }
                }
                else
                {
                    handleForkedFlag();
                }
                // TODO: Blacklisting point
                // TODO: implement for superblocks
                return;
            }
            else if (b_status != BlockVerifyStatus.Valid)
            {
                Logging.warn(String.Format("Received block #{0} ({1}) which was not valid!", b.blockNum, Crypto.hashToString(b.blockChecksum)));
                // TODO: Blacklisting point
                return;
            }


            // remove signatures without PL entry but not if we're catching up with the network or if the chain is stuck
            if (IxianHandler.getHighestKnownNetworkBlockHeight() < b.blockNum + 5
                && b.timestamp + 3600 > Clock.getNetworkTimestamp())
            {
                if (removeSignaturesWithoutPlEntry(b))
                {
                    Logging.warn(String.Format("Received block #{0} ({1}) which had a signature that wasn't found in the PL!", b.blockNum, Crypto.hashToString(b.blockChecksum)));
                }
                // TODO: Blacklisting point
            }
            if (b.signatures.Count == 0)
            {
                Logging.warn(String.Format("Received block #{0} ({1}) which has no valid signatures!", b.blockNum, Crypto.hashToString(b.blockChecksum)));
                // TODO: Blacklisting point
                return;
            }

            // TODOBLOCK
            lock (localBlockLock)
            {
                if (b.blockNum > Node.blockChain.getLastBlockNum())
                {
                    onBlockReceived_currentBlock(b, endpoint);
                }
            }
        }

        public BlockVerifyStatus verifyBlockBasic(Block b, bool verify_sig = true, RemoteEndpoint endpoint = null)
        {
            // TODO omega remove bottom if section after v12 upgrade
            if (!IxianHandler.isTestNet)
            {
                // upgrade to v12 at exactly block 4650000
                if (b.blockNum < 4650000 && b.version >= BlockVer.v12)
                {
                    return BlockVerifyStatus.Invalid;
                }
                if (b.blockNum >= 4650000 && b.version < BlockVer.v12)
                {
                    return BlockVerifyStatus.Invalid;
                }
            }

            if (b.version > Block.maxVersion)
            {
                Logging.error("Received block {0} with a version higher than this node can handle, discarding the block.", b.blockNum);
                networkUpgraded = true;
                return BlockVerifyStatus.IndeterminateVersionUpgradeBlock;
            } else if (b.version < 0)
            {
                Logging.error("Received block {0} with an invalid version {1}, discarding the block.", b.blockNum, b.version);
                return BlockVerifyStatus.Invalid;
            }

            // first check if lastBlockChecksum and previous block's checksum match, so we can quickly discard an invalid block (possibly from a fork)
            Block prevBlock = Node.blockChain.getBlock(b.blockNum - 1, false, false);

            if (prevBlock != null)
            {
                // checksum doesn't match
                if (!b.lastBlockChecksum.SequenceEqual(prevBlock.blockChecksum))
                {
                    Logging.warn("Received block #{0} with invalid lastBlockChecksum!", b.blockNum);
                    return BlockVerifyStatus.PotentiallyForkedBlock;
                }
                if (b.version >= BlockVer.v7)
                {
                    // if received block's timestamp is lower than previous block's timestamp + 20 seconds
                    if (b.timestamp < prevBlock.timestamp + ConsensusConfig.minBlockTimeDifference)
                    {
                        Logging.warn("Received block #{0} with invalid timestamp {1}, expecting at least {2}!", b.blockNum, b.timestamp, prevBlock.timestamp + ConsensusConfig.minBlockTimeDifference);
                        return BlockVerifyStatus.Invalid;
                    }
                    // if received block's timestamp is higher than network time + 60 seconds
                    if (b.timestamp > Clock.getNetworkTimestamp() + ConsensusConfig.maxBlockNetworkTimeDifference)
                    {
                        Logging.warn("Received block #{0} with invalid timestamp {1}, expecting at most {2}!", b.blockNum, b.timestamp, Clock.getNetworkTimestamp() + ConsensusConfig.maxBlockNetworkTimeDifference);
                        return BlockVerifyStatus.Invalid;
                    }
                }
            }

            Block genesis_block = Node.blockChain.getGenesisBlock();
            if (genesis_block != null)
            {
                if (b.blockNum == 1)
                {
                    if (!b.blockChecksum.SequenceEqual(genesis_block.blockChecksum))
                    {
                        Logging.error("Received block {0} that's different from genesis block, discarding the block.", b.blockNum);
                        return BlockVerifyStatus.PotentiallyForkedBlock;
                    }
                }
                else if (b.lastSuperBlockNum == 1)
                {
                    if (!b.lastSuperBlockChecksum.SequenceEqual(genesis_block.blockChecksum))
                    {
                        Logging.error("Received block {0} that's different from genesis block, discarding the block.", b.blockNum);
                        return BlockVerifyStatus.PotentiallyForkedBlock;
                    }
                }
            }

            if (Node.blockChain.Count > 0 && b.blockNum + 5 <= Node.blockChain.getLastBlockNum())
            {
                Block tmpBlock = Node.blockChain.getBlock(b.blockNum, false, false);
                if (tmpBlock == null)
                {
                    Logging.info("Received an indeterminate past block {0} ({1})", b.blockNum, Crypto.hashToString(b.blockChecksum));
                    return BlockVerifyStatus.IndeterminatePastBlock;
                }
                else if (tmpBlock.blockChecksum.SequenceEqual(b.blockChecksum))
                {
                    Logging.info("Already processed block {0} ({1})", b.blockNum, Crypto.hashToString(b.blockChecksum));
                    return BlockVerifyStatus.AlreadyProcessed;
                }
                Logging.warn("Received a potentially forked block {0} ({1})", b.blockNum, Crypto.hashToString(b.blockChecksum));
                return BlockVerifyStatus.PotentiallyForkedBlock;
            }

            ulong lastBlockNum = IxianHandler.getLastBlockHeight();

            if (b.blockNum <= lastBlockNum + 1 && verify_sig)
            {
                bool skip_sig_verification = false;
                if(pendingSuperBlocks.Count() > 0 && pendingSuperBlocks.OrderBy(x=> x.Key).Last().Key > b.blockNum)
                {
                    //skip_sig_verification = true; // TODO TODO TODO TODO TODO enable this and add additional hardening by verifying block's checksum against the SB segments when fully activating superblocks
                }
                if(b.fromLocalStorage && !Config.fullStorageDataVerification)
                {
                    skip_sig_verification = true;
                }
                Block localBlock = null;
                ulong lastBlockHeight = IxianHandler.getLastBlockHeight();
                if (b.blockNum == lastBlockHeight + 1)
                {
                    localBlock = localNewBlock;
                }else if(b.blockNum + 10 > lastBlockHeight && b.blockNum <= lastBlockHeight)
                {
                    localBlock = Node.blockChain.getBlock(b.blockNum, true, true);
                }
                // Verify signatures
                if (!b.verifySignatures(localBlock, skip_sig_verification))
                {
                    Logging.warn("Block #{0} failed while verifying signatures. There are no valid signatures on the block.", b.blockNum);
                    return BlockVerifyStatus.Indeterminate;
                }
            }

            if (prevBlock == null && lastBlockNum > 1) // block not found but blockChain is not empty, request the missing blocks
            {
                if (!Node.blockSync.synchronizing)
                {
                    // Don't request block 0
                    if (b.blockNum - 1 > 0 && highestNetworkBlockNum < b.blockNum)
                    {
                        if (removeSignaturesWithoutPlEntry(b))
                        {
                            Logging.warn("Received block #{0} ({1}) which had a signature that wasn't found in the PL!", b.blockNum, Crypto.hashToString(b.blockChecksum));
                        }
                        // blocknum is higher than the network's, switching to catch-up mode, but only if half of required consensus is reached on the block
                        if (b.blockNum > lastBlockNum + 1 && b.getFrozenSignatureCount() >= (Node.blockChain.getRequiredConsensus() / 2)) // if at least 2 blocks behind
                        {
                            // TODO TODO TODO TODO uncomment this once sig pruning and sync from superblocks is enabled
                            /*if (b.lastSuperBlockChecksum != null && !generateSuperBlockSegments(b, endpoint))
                            {
                                pendingSuperBlocks.AddOrReplace(b.blockNum, b);
                            }*/
                        }
                    }
                }
                if (b.blockNum > lastBlockNum + 1)
                {
                    if (!Node.blockSync.synchronizing)
                    {
                        BlockProtocolMessages.broadcastGetBlock(lastBlockNum + 1, null, null);
                    }
                    Logging.info("Received an indeterminate future block {0} ({1})", b.blockNum, Crypto.hashToString(b.blockChecksum));
                    return BlockVerifyStatus.IndeterminateFutureBlock;
                }else if(b.blockNum + ConsensusConfig.getRedactedWindowSize() > lastBlockNum)
                {
                    Logging.info("Received an indeterminate past block {0} ({1})", b.blockNum, Crypto.hashToString(b.blockChecksum));
                    return BlockVerifyStatus.IndeterminatePastBlock;
                }
            }

            // Verify sigfreeze
            if (b.blockNum <= lastBlockNum)
            {
                if (!verifySignatureFreezeChecksum(b, endpoint))
                {
                    return BlockVerifyStatus.PotentiallyForkedBlock;
                }
            }

            if (lastBlockNum + 1 == b.blockNum)
            {
                networkUpgraded = false;

                // verify block version, it should never be lower than the previous block
                if (b.version < IxianHandler.getLastBlockVersion())
                {
                    return BlockVerifyStatus.PotentiallyForkedBlock;
                }

                // verify that the upgrade happened on superblock
                if (b.blockNum > 1
                    && b.version >= BlockVer.v10
                    && b.version != IxianHandler.getLastBlockVersion()
                    && b.lastSuperBlockChecksum == null)
                {
                    return BlockVerifyStatus.Invalid;
                }

                // verify difficulty
                if (IxianHandler.getLastBlockHeight() - (ulong)Node.blockChain.Count == 0 || Node.blockChain.Count >= (long)ConsensusConfig.getRedactedWindowSize())
                {
                    //Logging.info("Verifying difficulty for #" + b.blockNum);
                    ulong expectedDifficulty = calculateDifficulty(b.version);
                    if (b.difficulty != expectedDifficulty)
                    {
                        Logging.warn("Received block #{0} ({1}) which had a difficulty {2}, expected difficulty: {3}", b.blockNum, Crypto.hashToString(b.blockChecksum), b.difficulty, expectedDifficulty);
                        return BlockVerifyStatus.Invalid;
                    }

                    // TODO perhaps move the bottom if section to onSuperBlockReceived
                    if((b.lastSuperBlockChecksum != null || b.blockNum == 1) && b.version >= BlockVer.v10)
                    {
                        ulong expectedSignerBits = Node.blockChain.calculateRequiredSignerBits(false, b.version, b.timestamp);
                        if (b.signerBits != expectedSignerBits)
                        {
                            Node.blockChain.clearCachedRequiredSignerDifficulty();
                            Logging.warn("Received block #{0} ({1}) which had a signer difficulty {2}, expected signer difficulty: {3}", b.blockNum, Crypto.hashToString(b.blockChecksum), b.signerBits, expectedSignerBits);
                            return BlockVerifyStatus.Invalid;
                        }
                    }
                }
            }

            // TODO: blacklisting would happen here - whoever sent us an invalid block is problematic
            //  Note: This will need a change in the Network code to tag incoming blocks with sender info.
            return BlockVerifyStatus.Valid;
        }

        public void addWaitingForTransactions(ulong blockNum)
        {
            lock (fetchingTxForBlocks)
            {
                if (!fetchingTxForBlocks.ContainsKey(blockNum))
                {
                    fetchingTxForBlocks.Add(blockNum, 0);
                }else
                {
                    fetchingTxForBlocks[blockNum] = 0;
                }
            }
        }

        public bool isBlockWaitingForTransactions(ulong blockNum)
        {
            lock (fetchingTxForBlocks)
            {
                if (fetchingTxForBlocks.ContainsKey(blockNum))
                {
                    return true;
                }
            }
            return false;
        }

        public BlockVerifyStatus verifyBlockTransactions(Block b, bool ignore_walletstate = false, RemoteEndpoint endpoint = null)
        {
            // Check all transactions in the block against our TXpool, make sure all is legal
            // Note: it is possible we don't have all the required TXs in our TXpool - in this case, request the missing ones and return Indeterminate
            bool hasAllTransactions = true;
            bool fetchTransactions = false;
            lock (fetchingTxForBlocks)
            {
                if (fetchingTxForBlocks.ContainsKey(b.blockNum))
                {
                    long tx_timeout = fetchingTxForBlocks[b.blockNum];
                    long cur_time = Clock.getTimestamp();
                    if (cur_time - tx_timeout > 10)
                    {
                        fetchingTxForBlocks[b.blockNum] = cur_time;
                        if(tx_timeout > 0)
                        {
                            Logging.info("fetchingTxTimeout EXPIRED");
                        }
                        fetchTransactions = true;
                    }
                }
            }
            int txCount = 0;
            int missing = 0;

            if ((ulong)b.transactions.Count > ConsensusConfig.maximumTransactionsPerBlock + 10)
            {
                Logging.warn("Block #{0} has more transactions than the maximumTransactionsPerBlock setting {1}/{2}", b.blockNum, b.transactions.Count, ConsensusConfig.maximumTransactionsPerBlock + 10);
                return BlockVerifyStatus.Invalid;
            }

            List<byte[]> txs_to_fetch = new List<byte[]>();
            Dictionary<byte[], IxiNumber> minusBalances = new Dictionary<byte[], IxiNumber>(new ByteArrayComparer());
            foreach (byte[] txid in b.transactions)
            {
                // Skip fetching staking txids if we're not synchronizing
                if (txid[0] == 0)
                {
                    if (Node.blockSync.synchronizing == false
                        || (Node.blockSync.synchronizing == true && Config.recoverFromFile)
                        || (Node.blockSync.synchronizing == true && b.blockNum > Node.blockSync.wsSyncConfirmedBlockNum)
                        || (Node.blockSync.synchronizing == true && Config.fullStorageDataVerification == true))
                        continue;
                }

                Transaction t = TransactionPool.getUnappliedTransaction(txid);
                if (t == null)
                {
                    if(TransactionPool.hasAppliedTransaction(txid))
                    {
                        Logging.error("Block #{0} includes a transaction that has already been applied in previous.", b.blockNum);
                        return BlockVerifyStatus.PotentiallyForkedBlock;
                    }
                    if (fetchTransactions)
                    {
                        Logging.info("Missing transaction '{0}', adding to fetch queue.", Transaction.getTxIdString(txid));
                        var pii = Node.inventoryCache.add(new InventoryItem(InventoryItemTypes.transaction, txid), endpoint);
                        if (pii.processed || pii.lastRequested == 0)
                        {
                            pii.lastRequested = Clock.getTimestamp();
                            txs_to_fetch.Add(txid);
                        }
                    }else
                    {
                        var pii = Node.inventoryCache.add(new InventoryItem(InventoryItemTypes.transaction, txid), endpoint);
                        if (!pii.processed && Clock.getTimestamp() - pii.lastRequested > 5)
                        {
                            pii.lastRequested = 0;
                        }
                    }
                    hasAllTransactions = false;
                    missing++;
                    continue;
                }

                // lock transaction v1 with block v2
                if (b.version < BlockVer.v2)
                {
                    if (t.version > 1)
                    {
                        Logging.error("Block includes a tx version {{ {0} }} but expected tx version was at most 1!", t.version);
                        return BlockVerifyStatus.Invalid;
                    }
                }
                else if (b.version == BlockVer.v2)
                {
                    if (t.version < 1 || t.version > 2)
                    {
                        Logging.error("Block includes a tx version {{ {0} }} but expected tx version is 1 or 2!", t.version);
                        return BlockVerifyStatus.Invalid;
                    }
                }
                else if (b.version <= BlockVer.v5)
                {
                    if (t.version < 2 || t.version > 3)
                    {
                        Logging.error("Block includes a tx version {{ {0} }} but expected tx version is 2 or 3!", t.version);
                        return BlockVerifyStatus.Invalid;
                    }
                }
                else if (b.version == BlockVer.v6)
                {
                    if (t.version < 3 || t.version > 4)
                    {
                        Logging.error("Block includes a tx version {{ {0} }} but expected tx version is 3 or 4!", t.version);
                        return BlockVerifyStatus.Invalid;
                    }
                }
                else if (b.version == BlockVer.v7)
                {
                    if (t.version < 4 || t.version > 5)
                    {
                        Logging.error("Block includes a tx version {{ {0} }} but expected tx version is 4 or 5!", t.version);
                        return BlockVerifyStatus.Invalid;
                    }
                }
                else if (b.version == BlockVer.v8 || b.version == BlockVer.v9)
                {
                    if (t.version < 5 || t.version > 6)
                    {
                        Logging.error("Block includes a tx version {{ {0} }} but expected tx version is 5 or 6!", t.version);
                        return BlockVerifyStatus.Invalid;
                    }
                }
                else
                {
                    if (t.version < 6 || t.version > 7)
                    {
                        Logging.error("Block includes a tx version {{ {0} }} but expected tx version is 6 or 7!", t.version);
                        return BlockVerifyStatus.Invalid;
                    }
                }

                foreach (var entry in t.fromList)
                {
                    byte[] address = (new Address(t.pubKey.addressNoChecksum, entry.Key)).addressNoChecksum;
                    // TODO TODO TODO TODO plus balances should also be added (and be processed first) to prevent overspending false alarms
                    if (!minusBalances.ContainsKey(address))
                    {
                        minusBalances.Add(address, 0);
                    }

                    try
                    {
                        // TODO: check to see if other transaction types need additional verification
                        if (t.type != (int)Transaction.Type.Genesis
                            && t.type != (int)Transaction.Type.PoWSolution
                            && t.type != (int)Transaction.Type.StakingReward)
                        {
                            txCount++;
                            IxiNumber new_minus_balance = minusBalances[address] + entry.Value;
                            minusBalances[address] = new_minus_balance;
                        }
                    }
                    catch (OverflowException)
                    {
                        // someone is doing something bad with this transaction, so we invalidate the block
                        // TODO: Blacklisting for the transaction originator node
                        Logging.error("Overflow caused by transaction {0}: amount: {1} from: {2}",
                            t.getTxIdString(), t.amount, Base58Check.Base58CheckEncoding.EncodePlain(address));
                        return BlockVerifyStatus.Invalid;
                    }
                }
            }

            if(fetchTransactions)
            {
                TransactionProtocolMessages.broadcastGetTransactions(txs_to_fetch, -(long)b.blockNum, endpoint);
            }

            // Pass #2 verifications for multisigs after all transactions have been received
            if(hasAllTransactions)
            {
                IxiNumber tFeeAmount = 0;
                foreach (byte[] txid in b.transactions)
                {
                    Transaction t = TransactionPool.getUnappliedTransaction(txid);
                    if(t == null)
                    {
                        continue;
                    }

                    tFeeAmount += t.fee;

                    if(b.blockNum >= ConsensusConfig.miningExpirationBlockHeight && t.type == (int)Transaction.Type.PoWSolution)
                    {
                        Logging.error("Block #{0} includes a PoW transaction {1}. Mining has stopped after block #{2}.", b.blockNum, t.getTxIdString(), ConsensusConfig.miningExpirationBlockHeight);
                        return BlockVerifyStatus.Invalid;
                    }
                    if (t.blockHeight > b.blockNum)
                    {
                        Logging.error("Block #{0} includes a transaction {1} which has a higher blockheight.", b.blockNum, t.getTxIdString());
                        return BlockVerifyStatus.Invalid;
                    }
                    if (t.type == (int)Transaction.Type.MultisigTX || t.type == (int)Transaction.Type.ChangeMultisigWallet || t.type == (int)Transaction.Type.MultisigAddTxSignature)
                    {
                        object multisig_data = t.GetMultisigData();
                        byte[] orig_txid = null;
                        if (multisig_data is Transaction.MultisigTxData)
                        {
                            orig_txid = ((Transaction.MultisigTxData)multisig_data).origTXId;
                        }
                        if (orig_txid == null)
                        {
                            orig_txid = t.id;
                        }
                        Address address = new Address(t.pubKey.addressNoChecksum, t.fromList.Keys.First());
                        Wallet from_w = Node.walletState.getWallet(address);
                        int num_valid_multisigs = TransactionPool.getNumRelatedMultisigTransactions(orig_txid, b) + 1;
                        if (num_valid_multisigs < from_w.requiredSigs)
                        {
                            Logging.error("Block #{0} includes a multisig transaction {{ {1} }} which does not have enough signatures to be processed! (Signatures: {2}, Required: {3}",
                                b.blockNum, t.getTxIdString(), num_valid_multisigs, from_w.requiredSigs);
                            return BlockVerifyStatus.Invalid;
                        }
                    }
                }
            }

            //
            if (!hasAllTransactions)
            {
                lock (fetchingTxForBlocks)
                {
                    if (!fetchingTxForBlocks.ContainsKey(b.blockNum))
                    {
                        long cur_time = Clock.getTimestamp();
                        if (missing < b.transactions.Count / 2 && missing < 50)
                        {
                            cur_time = cur_time - 30;
                            fetchingBulkTxForBlocks.AddOrReplace(b.blockNum, cur_time);
                            fetchingTxForBlocks.Add(b.blockNum, cur_time);
                            BlockVerifyStatus status = verifyBlockTransactions(b, ignore_walletstate, endpoint);
                            return status;
                        }
                        else
                        {
                            if(fetchingBulkTxForBlocks.ContainsKey(b.blockNum) && fetchingBulkTxForBlocks[b.blockNum] > cur_time - 10)
                            {
                                return BlockVerifyStatus.Indeterminate;
                            }
                            fetchingBulkTxForBlocks.AddOrReplace(b.blockNum, cur_time);
                            byte includeTransactions = 2;
                            if (Node.blockSync.synchronizing == false
                                || (Node.blockSync.synchronizing == true && Config.recoverFromFile)
                                || (Node.blockSync.synchronizing == true && b.blockNum > Node.blockSync.wsSyncConfirmedBlockNum)
                                || (Node.blockSync.synchronizing == true && Config.fullStorageDataVerification == true))
                            {
                                includeTransactions = 1;
                            }
                            BlockProtocolMessages.broadcastGetBlock(b.blockNum, null, endpoint, includeTransactions);
                        }
                        Logging.info("Block #{0} is missing {1} transactions, which have been requested from the network.", b.blockNum, missing);
                    }
                    if(fetchTransactions)
                    {
                        BlockProtocolMessages.broadcastGetBlock(b.blockNum, null, endpoint, 0);
                    }
                }
                Logging.info("Waiting for missing transactions for Block #{0}.", b.blockNum);
                var pii = Node.inventoryCache.add(new InventoryItemBlock(b.blockChecksum, b.blockNum), endpoint);
                if (pii != null)
                {
                    pii.retryCount = 0;
                    pii.processed = false;
                }
                return BlockVerifyStatus.Indeterminate;
            }
            lock (fetchingTxForBlocks)
            {
                fetchingBulkTxForBlocks.Remove(b.blockNum);
                fetchingTxForBlocks.Remove(b.blockNum);
            }

            if(ignore_walletstate == false)
            {
                // overspending
                foreach (byte[] addr in minusBalances.Keys)
                {
                    Address address = new Address(addr);
                    IxiNumber initial_balance = Node.walletState.getWalletBalance(address);
                    if (initial_balance < minusBalances[addr])
                    {
                        Logging.error("Address {0} is attempting to overspend: Balance: {1}, Total Outgoing: {2}.",
                            address.ToString(), initial_balance, minusBalances[addr]);
                        return BlockVerifyStatus.Invalid;
                    }
                }
            }

            return BlockVerifyStatus.Valid;
        }

        public BlockVerifyStatus verifyBlock(Block b, bool ignore_walletstate = false, RemoteEndpoint endpoint = null)
        {
            var sw = new System.Diagnostics.Stopwatch();
            sw.Start();

            BlockVerifyStatus basicVerification = verifyBlockBasic(b, true, endpoint);

            if (basicVerification != BlockVerifyStatus.Valid)
            {
                return basicVerification;
            }

            if (Node.blockChain.Count > 0 && b.blockNum <= Node.blockChain.getLastBlockNum())
            {
                Block tmpBlock = Node.blockChain.getBlock(b.blockNum, false, false);
                if (tmpBlock == null)
                {
                    Logging.info("Received an indeterminate past block {0} ({1})", b.blockNum, Crypto.hashToString(b.blockChecksum));
                    return BlockVerifyStatus.IndeterminatePastBlock;
                }
                else if (tmpBlock.blockChecksum.SequenceEqual(b.blockChecksum))
                {
                    Logging.info("Already processed block {0} ({1})", b.blockNum, Crypto.hashToString(b.blockChecksum));
                    return BlockVerifyStatus.AlreadyProcessed;
                }
                Logging.warn("Received a potentially forked block {0} ({1})", b.blockNum, Crypto.hashToString(b.blockChecksum));
                return BlockVerifyStatus.PotentiallyForkedBlock;
            }

            BlockVerifyStatus txVerification = verifyBlockTransactions(b, ignore_walletstate, endpoint);
            if (txVerification != BlockVerifyStatus.Valid)
            {
                return txVerification;
            }

            // Note: This part depends on no one else messing with WS while it runs.
            // Sometimes generateNewBlock is called from the other thread and this is invoked by network while
            // the generate thread is paused, so we need to lock
            // Note: This function is also called from BlockSync, which uses it to determine if the blocks it is syncing
            // from neighbors are OK.  However, BlockSync applies blocks before the current WS, so sometimes it doesn't
            // want to check WS checksums
            byte[] ws_checksum = null;
            byte[] rn_checksum = null;
            if (ignore_walletstate == false)
            {

                lock (localBlockLock)
                {
                    ulong last_block_num = Node.blockChain.getLastBlockNum();
                    // ignore wallet state check if it isn't the current block
                    if (b.blockNum <= last_block_num)
                    {
                        Logging.info("Not verifying wallet state for old block {0}", b.blockNum);
                    }
                    else if(b.blockNum == last_block_num + 1)
                    {
                        Node.walletState.setCachedBlockVersion(b.version);
                        Node.regNameState.setCachedBlockVersion(b.version);
                        IxiNumber totalSupplyBefore = Node.walletState.calculateTotalSupply();
                        Node.walletState.beginTransaction(b.blockNum);
                        Node.regNameState.beginTransaction(b.blockNum);
                        if (Config.fullBlockLogging) { Logging.info("Starting WSJ transaction for block verification: {0}", b.blockNum); }
                        if (applyAcceptedBlock(b))
                        {
                            if (b.version < BlockVer.v5 || b.lastSuperBlockChecksum != null)
                            {
                                ws_checksum = Node.walletState.calculateWalletStateChecksum();
                            }
                            else
                            {
                                ws_checksum = Node.walletState.calculateWalletStateDeltaChecksum(b.blockNum, b.version);
                            }

                            if (b.version >= BlockVer.v11)
                            {
                                rn_checksum = Node.regNameState.calculateRegNameStateChecksum(b.blockNum);
                            }
                        } else
                        {
                            Logging.error("Block #{0} failed applying!", b.blockNum);
                        }
                        if (Config.fullBlockLogging) { Logging.info("Reverting WSJ transaction for block verification: {0}", b.blockNum); }
                        Node.walletState.revertTransaction(b.blockNum);
                        Node.regNameState.revertTransaction(b.blockNum);
                        IxiNumber totalSupplyAfter = Node.walletState.calculateTotalSupply();
                        if(totalSupplyBefore != totalSupplyAfter)
                        {
                            Logging.error("Block #{0} did not cleanly revert!", b.blockNum);
                            return BlockVerifyStatus.Invalid;
                        }

                        if (ws_checksum == null || !ws_checksum.SequenceEqual(b.walletStateChecksum))
                        {
                            string block_proposer = "";
                            if (b.blockProposer != null)
                            {
                                block_proposer = Base58Check.Base58CheckEncoding.EncodePlain(b.blockProposer);
                            }
                            Logging.error("Block #{0} failed while verifying transactions: Invalid wallet state checksum! Block's WS checksum: {1}, actual WS checksum: {2}, block proposer: {3}", b.blockNum, Crypto.hashToString(b.walletStateChecksum), Crypto.hashToString(ws_checksum), block_proposer);
                            return BlockVerifyStatus.Invalid;
                        }
                        
                        if (b.version >= BlockVer.v11 && (rn_checksum == null || !rn_checksum.SequenceEqual(b.regNameStateChecksum)))
                        {
                            string block_proposer = "";
                            if (b.blockProposer != null)
                            {
                                block_proposer = Base58Check.Base58CheckEncoding.EncodePlain(b.blockProposer);
                            }
                            Logging.error("Block #{0} failed while verifying transactions: Invalid name state checksum! Block's RN checksum: {1}, actual RN checksum: {2}, block proposer: {3}", b.blockNum, Crypto.hashToString(b.regNameStateChecksum), Crypto.hashToString(rn_checksum), block_proposer);
                            return BlockVerifyStatus.Invalid;
                        }
                    }
                    else
                    {
                        // this should never happen
                        Logging.error("Block #{0} failed while verifying transactions, this is a future block", b.blockNum);
                        return BlockVerifyStatus.Invalid;
                    }
                }
            }


            sw.Stop();
            TimeSpan elapsed = sw.Elapsed;
            //Logging.info(string.Format("VerifyBlock duration: {0}ms", elapsed.TotalMilliseconds));


            // TODO: blacklisting would happen here - whoever sent us an invalid block is problematic
            //  Note: This will need a change in the Network code to tag incoming blocks with sender info.
            return BlockVerifyStatus.Valid;
        }

        private void onBlockReceived_currentBlock(Block b, RemoteEndpoint endpoint)
        {
            if (b.blockNum != Node.blockChain.getLastBlockNum() + 1)
            {
                Logging.warn("Received block #{0}, but next block should be #{1}.", b.blockNum, Node.blockChain.getLastBlockNum() + 1);
                return;
            }
            if(b.version < Node.blockChain.getLastBlockVersion())
            {
                Logging.warn("A current block with a smaller version was received than the last block in the blockchain, rejecting block #{0} with version {1}.", b.blockNum, b.version);
                // TODO: keep a counter - if this happens too often, disconnect the node
                // TODO TODO TODO TODO: disconnect?
                return;
            }
            lock (localBlockLock)
            {
                if (localNewBlock != null)
                {
                    if(localNewBlock.blockChecksum.SequenceEqual(b.blockChecksum))
                    {
                        Logging.info("Block #{0} ({1} sigs) received from the network is the block we are currently working on. Merging signatures  ({2} sigs).", b.blockNum, b.signatures.Count(), localNewBlock.signatures.Count());
                        List<BlockSignature> added_signatures = localNewBlock.addSignaturesFrom(b, false);
                        if (added_signatures != null || localNewBlock.getFrozenSignatureCount() >= Node.blockChain.getRequiredConsensus())
                        {
                            // if addSignaturesFrom returns true, that means signatures were increased, so we re-transmit
                            Logging.info("Block #{0}: Number of signatures increased, re-transmitting. (total signatures: {1}).", b.blockNum, localNewBlock.getFrozenSignatureCount());

                            if (added_signatures != null)
                            {
                                currentBlockStartTime = DateTime.UtcNow;
                                lastBlockStartTime = DateTime.UtcNow.AddSeconds(-blockGenerationInterval * 10);
                                if (Node.isMasterNode())
                                {
                                    foreach (var sig in added_signatures)
                                    {
                                        Node.inventoryCache.setProcessedFlag(InventoryItemTypes.blockSignature, InventoryItemSignature.getHash(sig.recipientPubKeyOrAddress.addressNoChecksum, b.blockChecksum), true);
                                        SignatureProtocolMessages.broadcastBlockSignature(sig, b.blockNum, b.blockChecksum, endpoint, null);
                                    }
                                }
                            }else if(localNewBlock.signatures.Count != b.signatures.Count)
                            {
                                if (Node.isMasterNode())
                                {
                                    BlockProtocolMessages.broadcastNewBlock(localNewBlock, null, endpoint);
                                }
                            }

                            acceptLocalNewBlock();
                        }
                        else if(localNewBlock.signatures.Count != b.signatures.Count)
                        {
                            if (!Node.isMasterNode())
                                return;
                            Logging.info("Block #{0}: Received block has less signatures, re-transmitting local block. (total signatures: {1}).", b.blockNum, localNewBlock.getFrozenSignatureCount());
                            BlockProtocolMessages.broadcastNewBlock(localNewBlock, null, endpoint);
                        }
                    }
                    else
                    {
                        int remoteBlockSigCount = b.getFrozenSignatureCount();
                        int localBlockSigCount = localNewBlock.getFrozenSignatureCount();
                        if(b.blockNum == localNewBlock.blockNum)
                        {
                            bool hasNodeSig = hasElectedNodeSignature(b);
                            if (hasNodeSig)
                            {
                                var remoteTotalSignerDifficulty = b.getTotalSignerDifficulty();
                                var localTotalSignerDifficulty = localNewBlock.getTotalSignerDifficulty();
                                if ((remoteBlockSigCount > localBlockSigCount)
                                    || (remoteBlockSigCount == localBlockSigCount && remoteTotalSignerDifficulty > localTotalSignerDifficulty)
                                    || (hasRequiredSignatureCount(b) && highestNetworkBlockNum > b.blockNum + 5))
                                {
                                    Logging.info("Incoming block #{0} has more signatures and is the same block height, accepting instead of our own. (total signatures: {1}, election offset: {2})", b.blockNum, b.signatures.Count, getElectedNodeOffset());
                                    localNewBlock = b;
                                    currentBlockStartTime = DateTime.UtcNow;
                                    lastBlockStartTime = DateTime.UtcNow.AddSeconds(-blockGenerationInterval * 10);
                                    acceptLocalNewBlock();
                                    return;
                                }
                            }
                        }
                        if (!Node.isMasterNode())
                            return;
                        // discard with a warning, likely spam, resend our local block
                        Logging.info("Incoming block #{0} is different than our own and doesn't have more sigs, discarding and re-transmitting local block. (total signatures: {1}), election offset: {2}.", b.blockNum, b.signatures.Count, getElectedNodeOffset());
                        BlockProtocolMessages.broadcastNewBlock(localNewBlock, null, endpoint);
                    }
                }
                else // localNewBlock == null
                {
                    bool hasNodeSig = hasElectedNodeSignature(b);
                    if (hasNodeSig
                        || b.getFrozenSignatureCount() >= Node.blockChain.getRequiredConsensus()/2 // TODO TODO Omega think about /2 thing
                        || firstBlockAfterSync)
                    {
                        localNewBlock = b;
                        currentBlockStartTime = DateTime.UtcNow;
                        lastBlockStartTime = DateTime.UtcNow.AddSeconds(-blockGenerationInterval * 10);
                        firstBlockAfterSync = false;
                        acceptLocalNewBlock();
                    }
                    else
                    {
                        Logging.warn("Incoming block #{0} doesn't have elected node's sig, waiting for a new block. (total signatures: {1}), election offset: {2}.", b.blockNum, b.signatures.Count, getElectedNodeOffset());
                    }
                }
            }
        }

        public bool hasElectedNodeSignature(Block b)
        {
            int offset = getElectedNodeOffset();
            if (offset != -1 && IxianHandler.getLastBlockHeight() + 2 > IxianHandler.getHighestKnownNetworkBlockHeight())
            {
                var electedNodeAddresses = Node.blockChain.getElectedNodeAddresses(offset);
                foreach (var address in electedNodeAddresses)
                {
                    if (b.hasNodeSignature(new Address(address)))
                    {
                        return true;
                    }
                }
            }
            else
            {
                return true;
            }
            return false;
        }

        // Adds a block to the blacklist
        public void blacklistBlock(Block b)
        {
            lock (blockBlacklist)
            {
                Dictionary<byte[], DateTime> blacklistedBlocks = null;
                if (blockBlacklist.ContainsKey(b.blockNum))
                {
                    blacklistedBlocks = blockBlacklist[b.blockNum];
                }
                else
                {
                    blacklistedBlocks = new Dictionary<byte[], DateTime>(new ByteArrayComparer());
                }
                blacklistedBlocks.AddOrReplace(b.blockChecksum, DateTime.UtcNow);
                blockBlacklist.AddOrReplace(b.blockNum, blacklistedBlocks);
            }
        }

        // Returns true if block is blacklisted
        // When expiration_time is set to 0, it will use the default blockGenerationInterval * 10;
        public bool isBlockBlacklisted(Block b, int expiration_time = 0)
        {
            if(expiration_time == 0)
            {
                expiration_time = blockGenerationInterval * 10;
            }
            lock (blockBlacklist)
            {
                if (blockBlacklist.ContainsKey(b.blockNum))
                {
                    Dictionary<byte[], DateTime> bbl = blockBlacklist[b.blockNum];
                    if (bbl.ContainsKey(b.blockChecksum))
                    {
                        DateTime dt = bbl[b.blockChecksum];
                        if ((DateTime.UtcNow - dt).TotalSeconds > expiration_time)
                        {
                            blockBlacklist[b.blockNum].Remove(b.blockChecksum);
                            if (blockBlacklist[b.blockNum].Count() == 0)
                            {
                                blockBlacklist.Remove(b.blockNum);
                            }
                            return false;
                        }
                        return true;
                    }
                }
            }
            return false;
        }

        private bool removeBlockBlacklist(Block b)
        {
            lock (blockBlacklist)
            {
                if (blockBlacklist.ContainsKey(b.blockNum))
                {
                    Dictionary<byte[], DateTime> bbl = blockBlacklist[b.blockNum];
                    if (bbl.ContainsKey(b.blockChecksum))
                    {
                        blockBlacklist[b.blockNum].Remove(b.blockChecksum);
                        if (blockBlacklist[b.blockNum].Count() == 0)
                        {
                            blockBlacklist.Remove(b.blockNum);
                        }
                        return true;
                    }
                }
            }
            return false;
        }

        // Removes blocks with older block height from the blacklist
        public void cleanupBlockBlacklist()
        {
            ulong blockNum = Node.blockChain.getLastBlockNum();
            lock (blockBlacklist)
            {
                Dictionary<ulong, Dictionary<byte[], DateTime>> tmpList = new Dictionary<ulong, Dictionary<byte[], DateTime>>(blockBlacklist);
                foreach (var i in tmpList)
                {
                    if (i.Key < blockNum)
                    {
                        blockBlacklist.Remove(i.Key);
                    }
                }
            }
        }

        // extracts required signatures from a block according to the election block (blockNum - 6)
        private List<BlockSignature> extractRequiredSignatures(Block block, int max_sig_count)
        {
            Block election_block = Node.blockChain.getBlock(block.blockNum - 6);
            if(election_block == null)
            {
                Logging.warn("Cannot extract required signatures because election block is null");
                return null;
            }


            List<BlockSignature> required_sigs = new List<BlockSignature>();

            int sig_count = 0;

            // Sort the signatures first
            List<BlockSignature> sorted_sigs = null;
            lock (block.signatures)
            {
                if(block.frozenSignatures != null)
                {
                    sorted_sigs = new List<BlockSignature>(block.frozenSignatures);
                }else
                {
                    sorted_sigs = new List<BlockSignature>(block.signatures);
                }
            }
            if(block.version < BlockVer.v10)
            {
                sorted_sigs.Sort((x, y) => _ByteArrayComparer.Compare(x.recipientPubKeyOrAddress.addressNoChecksum, y.recipientPubKeyOrAddress.addressNoChecksum));

                // First add block proposer's sig
                if (block.blockProposer != null)
                {
                    BlockSignature signature = sorted_sigs.Find(x => (x.recipientPubKeyOrAddress.addressWithChecksum.SequenceEqual(block.blockProposer)));
                    if (signature == null)
                    {
                        Logging.error("Error freezing signatures of target block #{0} {1}, cannot find block proposer's signature.", block.blockNum, Crypto.hashToString(block.blockChecksum));
                        return null;
                    }
                    required_sigs.Add(signature);
                    sorted_sigs.Remove(signature);
                    sig_count++;
                }
            }
            else
            {
                //sorted_sigs.Sort((x, y) => Comparer<IxiNumber>.Default.Compare(x.powSolution.difficulty, y.powSolution.difficulty));
                //sorted_sigs = sorted_sigs.OrderBy(x => x.powSolution.difficulty, Comparer<IxiNumber>.Default).ThenBy(x => x.recipientPubKeyOrAddress.addressNoChecksum, new ByteArrayComparer()).ToList();
            }

            var election_block_sigs = election_block.signatures;
            if(election_block.frozenSignatures != null)
            {
                election_block_sigs = election_block.frozenSignatures;
            }
            foreach (var entry in sorted_sigs)
            {
                byte[] address = entry.recipientPubKeyOrAddress.addressNoChecksum;
                foreach (var prev_entry in election_block_sigs)
                {
                    if (address.SequenceEqual(prev_entry.recipientPubKeyOrAddress.addressNoChecksum))
                    {
                        required_sigs.Add(entry);
                        sig_count++;
                        break;
                    }
                }
                if (max_sig_count > 0 && sig_count == max_sig_count)
                {
                    break;
                }
            }

            return required_sigs;
        }

        // Checks if the block has enough signatures for consensus
        // If signature count is lower than 3 it will return true
        // If block version is lower than v5 or blocknum is lower than 7 it will also return true
        public bool hasRequiredSignatureCount(Block block)
        {
            int frozen_sig_count = block.getFrozenSignatureCount();
            if (frozen_sig_count < 3)
                return true;

            if (block.blockNum > 6 && block.version >= BlockVer.v5)
            {
                int required_consensus_count = Node.blockChain.getRequiredConsensus(block.blockNum);
                if (frozen_sig_count < required_consensus_count
                    && !isBlockchainRecoveryMode(block.blockNum, block.timestamp, frozen_sig_count))
                {
                    //Logging.info("Block {0} has less than required signatures ({1} < {2}).", block.blockNum, frozen_sig_count, required_consensus_count);
                    return false;
                }
                if (block.version >= BlockVer.v10 && IxianHandler.getBlockHeader(block.blockNum - 1).version >= BlockVer.v10)
                {
                    IxiNumber required_signer_difficulty = Node.blockChain.getRequiredSignerDifficulty(block, true);
                    IxiNumber frozen_sig_difficulty = block.getTotalSignerDifficulty();
                    if (frozen_sig_difficulty < required_signer_difficulty)
                    {
                        //Logging.info("Block {0} has less than required signatures ({1} < {2}).", block.blockNum, frozen_sig_count, required_consensus_count);
                        return false;
                    }
                }
            }
            return true;
        }

        // verifies all signatures
        public bool verifyBlockSignatures(Block block, RemoteEndpoint endpoint)
        {
            int frozen_sig_count = block.getFrozenSignatureCount();
            if (block.blockNum > 6 && block.version >= BlockVer.v5)
            {
                int required_consensus_count = Node.blockChain.getRequiredConsensus(block.blockNum);

                // verify sig count
                if (frozen_sig_count < required_consensus_count
                    && !isBlockchainRecoveryMode(block.blockNum, block.timestamp, frozen_sig_count))
                {
                    Logging.info("Block {0} has less than required signatures ({1} < {2}).", block.blockNum, frozen_sig_count, required_consensus_count);
                    return false;
                }

                IxiNumber frozen_sig_difficulty = block.getTotalSignerDifficulty();
                IxiNumber required_signer_difficulty = null;
                IxiNumber required_signer_difficulty_adjusted = null;

                if (block.version >= BlockVer.v10 && IxianHandler.getBlockHeader(block.blockNum - 1).version >= BlockVer.v10)
                {
                    required_signer_difficulty = Node.blockChain.getRequiredSignerDifficulty(block, false);
                    required_signer_difficulty_adjusted = Node.blockChain.getRequiredSignerDifficulty(block, true);

                    // verify sig difficulty
                    if (frozen_sig_difficulty < required_signer_difficulty_adjusted)
                    {
                        Logging.info("Block {0} has less than required signer difficulty ({1} < {2}).", block.blockNum, frozen_sig_difficulty, required_signer_difficulty_adjusted);
                        return false;
                    }
                }

                List<BlockSignature> required_sigs = extractRequiredSignatures(block, 0);
                if(required_sigs == null)
                {
                    return false;
                }

                if (required_consensus_count > 2)
                {
                    // verify if over 50% signatures are from the previous block
                    if (required_sigs.Count < (required_consensus_count / 2) + 1
                        && !handleBlockchainRecoveryMode(block, required_sigs.Count, frozen_sig_count, frozen_sig_difficulty, required_signer_difficulty))
                    {
                        Logging.warn("Block {0} has less than 50% + 1 required signers from previous block {1} < {2}.", block.blockNum, required_sigs.Count(), (required_consensus_count / 2) + 1);
                        return false;
                    }
                }

                ulong last_block_num = IxianHandler.getLastBlockHeight();
                if (block.blockNum == last_block_num + 1 || Node.blockSync.synchronizing)
                {
                    // current block

                    // it already has more sigs than the required consensus
                    // it already includes the required signatures

                    return true;
                }else if (block.blockNum == last_block_num - 4)
                {
                    // sigfreezed block


                    if (highestNetworkBlockNum > last_block_num + 5
                        || block.timestamp + 3600 < Clock.getNetworkTimestamp())
                    {
                        // catching up

                        // it already has more sigs than the required consensus
                        // it already includes the required signatures, don't check elected signatures, since the node is catching up

                        return true;
                    }

                    int block_sig_count = frozen_sig_count;

                    if(block_sig_count > ConsensusConfig.maximumBlockSigners)
                    {
                        block_sig_count = ConsensusConfig.maximumBlockSigners;
                    }

                    Block local_block = new Block(Node.blockChain.getBlock(block.blockNum));

                    List<BlockSignature> added_signatures = local_block.addSignaturesFrom(block, false);

                    if(added_signatures != null && added_signatures.Count > 0)
                    {
                        Node.blockChain.updateBlock(local_block, false);
                        if (Node.isMasterNode() && !Node.blockSync.synchronizing)
                        {
                            foreach (var sig in added_signatures)
                            {
                                Node.inventoryCache.setProcessedFlag(InventoryItemTypes.blockSignature, InventoryItemSignature.getHash(sig.recipientPubKeyOrAddress.addressNoChecksum, block.blockChecksum), true);
                                SignatureProtocolMessages.broadcastBlockSignature(sig, block.blockNum, block.blockChecksum, endpoint, null);
                            }
                        }
                    }

                    if (!freezeSignatures(local_block))
                    {
                        return false;
                    }

                    int valid_sig_count = 0;

                    lock (local_block.signatures)
                    {
                        foreach (var localSig in local_block.frozenSignatures)
                        {
                            if (block.containsSignature(localSig.recipientPubKeyOrAddress))
                            {
                                valid_sig_count++;
                            }
                        }
                    }

                    // check if there are 90% valid signatures
                    if (valid_sig_count < Math.Floor(block_sig_count * 0.9))
                    {
                        Logging.warn("Block {0} has less than 90% valid signers ({1} / {2}).", block.blockNum, valid_sig_count, Math.Floor(block_sig_count * 0.9));
                        // TODO TODO TODO TODO bottom section needs to be tested
                        /*lock(localBlockLock)
                        {
                            if(localNewBlock != null)
                            {
                                blacklistBlock(localNewBlock);
                            }
                        }*/
                        return false;
                    }

                    return true;
                }
                else
                {
                    // should never happen
                    Logging.error("Verifying signatures for block #{0} that isn't sigfreezed or the current block.", block.blockNum);
                    return false;
                }
            }
            else
            {
                if (frozen_sig_count >= Node.blockChain.getRequiredConsensus(block.blockNum))
                {
                    return true;
                }
            }
            return false;
        }

        // Freezes signature for the specified target block
        public bool freezeSignatures(Block target_block)
        {
            int required_consensus_count = Node.blockChain.getRequiredConsensus(target_block.blockNum, false);
            IxiNumber required_difficulty = Node.blockChain.getRequiredSignerDifficulty(target_block, false);
            IxiNumber required_difficulty_adjusted = Node.blockChain.getRequiredSignerDifficulty(target_block, true);

            List<BlockSignature> frozen_block_sigs = null;
            if (highestNetworkBlockNum > target_block.blockNum + 10
                || target_block.timestamp + 3600 < Clock.getNetworkTimestamp())
            {
                // catching up
                frozen_block_sigs = extractRequiredSignatures(target_block, required_consensus_count);
            }
            else
            {
                frozen_block_sigs = extractRequiredSignatures(target_block, (required_consensus_count / 2) + 1);
            }

            if (frozen_block_sigs == null)
            {
                return false;
            }

            int required_signature_count = frozen_block_sigs.Count;
            int sig_count = frozen_block_sigs.Count;

            lock (target_block.signatures)
            {
                if(target_block.version < BlockVer.v10)
                {
                    PresenceOrderedEnumerator poe = PresenceList.getElectedSignerList(target_block.blockChecksum, ConsensusConfig.maximumBlockSigners * 2);
                    foreach (byte[] address in poe)
                    {
                        BlockSignature signature = target_block.signatures.Find(x => x.recipientPubKeyOrAddress.addressNoChecksum.SequenceEqual(address));
                        if (signature != null && frozen_block_sigs.Find(x => x.recipientPubKeyOrAddress.addressNoChecksum.SequenceEqual(address)) == null)
                        {
                            frozen_block_sigs.Add(signature);
                            sig_count++;
                        }
                        if (sig_count == ConsensusConfig.maximumBlockSigners)
                        {
                            break;
                        }
                    }
                }
                else
                {
                    foreach(BlockSignature sig in target_block.signatures)
                    {
                        var address = sig.recipientPubKeyOrAddress;
                        if (frozen_block_sigs.Find(x => x.recipientPubKeyOrAddress.addressNoChecksum.SequenceEqual(address.addressNoChecksum)) == null)
                        {
                            if(PresenceList.getPresenceByAddress(address) == null)
                            {
                                continue;
                            }
                            frozen_block_sigs.Add(sig);
                            sig_count++;
                        }
                        if (sig_count == ConsensusConfig.maximumBlockSigners)
                        {
                            break;
                        }
                    }
                }

                int required_consensus_count_adjusted = Node.blockChain.getRequiredConsensus(target_block.blockNum, true);




                IxiNumber frozen_block_sigs_difficulty = 0;
                foreach(var frozen_block_sig in frozen_block_sigs)
                {
                    if(frozen_block_sig.powSolution != null)
                    {
                        frozen_block_sigs_difficulty += frozen_block_sig.powSolution.difficulty;
                    }else
                    {
                        frozen_block_sigs_difficulty += 1;
                    }
                }

                if (frozen_block_sigs.Count < required_consensus_count_adjusted
                    && !handleBlockchainRecoveryMode(target_block, required_signature_count, frozen_block_sigs.Count, frozen_block_sigs_difficulty, required_difficulty))
                {
                    Logging.warn("Error freezing signatures of target block #{0} {1}, cannot freeze enough signatures to pass consensus, difficulty: {2} < {3} count: {4} < {5}.", target_block.blockNum, Crypto.hashToString(target_block.blockChecksum), frozen_block_sigs_difficulty, required_difficulty, frozen_block_sigs.Count, required_consensus_count_adjusted);
                    return false;
                }

                if (target_block.version >= BlockVer.v10
                    && IxianHandler.getBlockHeader(target_block.blockNum - 1).version >= BlockVer.v10)
                {
                    if (frozen_block_sigs_difficulty < required_difficulty_adjusted)
                    {
                        Logging.warn("Error freezing signatures of target block #{0} {1}, cannot freeze enough signatures to pass consensus, difficulty: {2} < {3} count: {4} < {5}.", target_block.blockNum, Crypto.hashToString(target_block.blockChecksum), frozen_block_sigs_difficulty, required_difficulty, frozen_block_sigs.Count, required_consensus_count_adjusted);
                        return false;
                    }
                }

                if (target_block.blockNum != 1 && target_block.version >= BlockVer.v10 && IxianHandler.getBlockHeader(target_block.blockNum - 1).version >= BlockVer.v10)
                {
                    frozen_block_sigs = frozen_block_sigs.OrderBy(x => x.powSolution.difficulty, Comparer<IxiNumber>.Default).ThenBy(x => x.recipientPubKeyOrAddress.addressNoChecksum, new ByteArrayComparer()).ToList();
                }

                Node.blockChain.setFrozenSignatures(target_block, frozen_block_sigs);
                if (target_block.version < BlockVer.v10)
                {
                    if (target_block.blockProposer == null)
                    {
                        target_block.blockProposer = target_block.signatures[0].recipientPubKeyOrAddress.addressWithChecksum;
                    }
                    if (!target_block.verifyBlockProposer())
                    {
                        Node.blockChain.setFrozenSignatures(target_block, null);
                        Logging.error("Error verifying block proposer while freezing signatures on block {0} ({1})", target_block.blockNum, Crypto.hashToString(target_block.blockChecksum));
                        return false;
                    }
                }
                return true;
            }
        }

        private bool isBlockchainRecoveryMode(ulong blockNum, long blockTimestamp, int totalBlockSignatures)
        {
            Block prevBlock = IxianHandler.getBlockHeader(blockNum - 1);
            if (prevBlock == null || prevBlock.timestamp + ConsensusConfig.blockChainRecoveryTimeout > blockTimestamp)
            {
                return false;
            }

            if (totalBlockSignatures < 3)
            {
                return false;
            }

            return true;
        }

        private bool handleBlockchainRecoveryMode(Block curBlock, int requiredSignatureCount, int totalBlockSignatures, IxiNumber totalSignerDifficulty, IxiNumber requiredSignerDifficulty) 
        {
            if (!isBlockchainRecoveryMode(curBlock.blockNum, curBlock.timestamp, totalBlockSignatures))
            {
                return false;
            }

            int requiredConsensus = Node.blockChain.getRequiredConsensus(curBlock.blockNum, false);
            int requiredConsensusAdj = Node.blockChain.getRequiredConsensus(curBlock.blockNum, true);

            int missingRequiredSigs = ((requiredConsensus / 2 ) + 1) - requiredSignatureCount;
            int missingSigs = requiredConsensusAdj - totalBlockSignatures;

            // no missing sigs, no need for recovery mode
            if (missingRequiredSigs <= 0 && missingSigs <= 0)
            {
                return false;
            }

            // missing sigs and no block for a period of time, run recovery checks

            IxiNumber recoveryRequiredSignerDifficulty = 0;
            if (missingRequiredSigs > 0)
            {
                recoveryRequiredSignerDifficulty = missingRequiredSigs * requiredSignerDifficulty * ConsensusConfig.blockChainRecoveryMissingRequiredSignerRatio / 100;
                if (totalSignerDifficulty < recoveryRequiredSignerDifficulty)
                {
                    return false;
                }
            }

            if (missingSigs > 0)
            {
                if (totalSignerDifficulty < recoveryRequiredSignerDifficulty + (missingSigs * IxianHandler.getMinSignerPowDifficulty(curBlock.blockNum, curBlock.timestamp)) * ConsensusConfig.blockChainRecoveryMissingSignerMultiplier)
                {
                    return false;
                }
            }

            Logging.warn("Recovery mode activated for block #{0} {1}, missing required sigs:{2}, missing sigs: {3}, cur time: {4}, block time: {5}, total signer difficulty: {6}, requiredSignerDifficultyAdjusted: {7}.",
                curBlock.blockNum, Crypto.hashToString(curBlock.calculateChecksum()), missingRequiredSigs, missingSigs, Clock.getNetworkTimestamp(), curBlock.timestamp, totalSignerDifficulty, requiredSignerDifficulty);

            return true;
        }

        public bool acceptLocalNewBlock()
        {
            bool block_accepted = false;
            bool requestBlockAgain = false;
            ulong requestBlockNum = 0;

            var sw = new System.Diagnostics.Stopwatch();
            sw.Start();

            Block last_block = IxianHandler.getLastBlock();
            ulong last_block_num = 0;
            if (last_block != null)
            {
                last_block_num = last_block.blockNum;
            }

            lock (localBlockLock)
            {
                if (localNewBlock == null) return false;

                Block target_block = null;
                if (localNewBlock.blockNum > 11)
                {
                    target_block = Node.blockChain.getBlock(localNewBlock.blockNum - 5);
                    if (target_block.frozenSignatures == null || !verifySignatureFreezeChecksum(localNewBlock, null))
                    {
                        freezeSignatures(target_block);
                    }
                }

                if (!verifySignatureFreezeChecksum(localNewBlock, null))
                {
                    Logging.warn(String.Format("Signature freeze checksum verification failed on current localNewBlock #{0}, waiting for the correct target block.", localNewBlock.blockNum));
                    TimeSpan current_block_processing_time = DateTime.UtcNow - currentBlockStartTime;
                    Random rnd = new Random();
                    if (current_block_processing_time.TotalSeconds > (blockGenerationInterval * 4) + rnd.Next(30)) // can't get target block for 4 block times + random seconds, we don't want all nodes sending at once
                    {
                        blacklistBlock(localNewBlock);
                        localNewBlock = null;
                    }
                    return false;
                }else
                {
                    // Check if the local block has enough signatures for consensus
                    if (hasRequiredSignatureCount(localNewBlock))
                    {
                        if (target_block != null && !verifyBlockSignatures(target_block, null))
                        {
                            blacklistBlock(localNewBlock);
                            localNewBlock = null;
                            Node.blockChain.setFrozenSignatures(target_block, null);
                            Logging.warn("The target block #{0} yields less than required signatures {1} < {2}", target_block.blockNum, target_block.getFrozenSignatureCount(), Node.blockChain.getRequiredConsensus(target_block.blockNum));
                            return false;
                        }
                    }

                    if (localNewBlock.blockNum + 5 >= IxianHandler.getHighestKnownNetworkBlockHeight())
                    {
                        if (Node.isMasterNode() && localNewBlock.blockNum > 7)
                        {
                            BlockSignature signature_data = localNewBlock.applySignature(PresenceList.getPowSolution()); // applySignature() will return signature_data, if signature was applied and null, if signature was already present from before
                            if (signature_data != null) 
                            {
                                foreach (var sig in localNewBlock.signatures)
                                {
                                    Node.inventoryCache.setProcessedFlag(InventoryItemTypes.blockSignature, InventoryItemSignature.getHash(sig.recipientPubKeyOrAddress.addressNoChecksum, localNewBlock.blockChecksum), true);
                                    SignatureProtocolMessages.broadcastBlockSignature(sig, localNewBlock.blockNum, localNewBlock.blockChecksum, null, null);
                                }
                                BlockProtocolMessages.broadcastNewBlock(localNewBlock, null, null);
                            }
                        }
                    }
                }

                if (hasRequiredSignatureCount(localNewBlock) && verifyBlockSignatures(localNewBlock, null))
                {
                    if (verifyBlock(localNewBlock) != BlockVerifyStatus.Valid)
                    {
                        if (Node.blockChain.getBlock(localNewBlock.blockNum) == null)
                        {
                            Logging.error("We have an invalid block #{0} in verifyBlockAcceptance, requesting the block again.", localNewBlock.blockNum);
                            requestBlockNum = localNewBlock.blockNum;
                            localNewBlock = null;
                            requestBlockAgain = true;
                        }
                        else
                        {
                            Logging.error("We have an invalid block #{0} in verifyBlockAcceptance.");
                            localNewBlock = null;
                            return false;
                        }
                    }

                    if (localNewBlock != null)
                    {
                        if (localNewBlock.blockNum != last_block_num + 1)
                        {
                            Logging.warn("Tried to apply an unexpected block #{0}, expected #{1}. Stack trace: {2}", localNewBlock.blockNum, last_block_num + 1, Environment.StackTrace);
                            // block has already been applied or ahead, waiting for new blocks
                            localNewBlock = null;
                            return false;
                        }

                        Node.walletState.beginTransaction(localNewBlock.blockNum, false);
                        Node.regNameState.beginTransaction(localNewBlock.blockNum, false);
                        // accept this block, apply its transactions, recalc consensus, etc
                        if (applyAcceptedBlock(localNewBlock) == true)
                        {
                            bool ws_checksum_ok = false;
                            byte[] ws_checksum = null;
                            if (localNewBlock.version >= BlockVer.v5 && localNewBlock.lastSuperBlockChecksum == null)
                            {
                                // no need to re-verify as verifyBlock already does this
                                ws_checksum_ok = true;
                            }
                            else
                            {
                                ws_checksum = Node.walletState.calculateWalletStateChecksum();
                                if (ws_checksum.SequenceEqual(localNewBlock.walletStateChecksum))
                                {
                                    ws_checksum_ok = true;
                                }
                            }

                            bool rn_checksum_ok = false;
                            byte[] rn_checksum = null;
                            if (localNewBlock.version < BlockVer.v11 || localNewBlock.lastSuperBlockChecksum == null)
                            {
                                // no need to re-verify as verifyBlock already does this
                                rn_checksum_ok = true;
                            }
                            else
                            {
                                rn_checksum = Node.regNameState.calculateRegNameStateChecksum(localNewBlock.blockNum);
                                if (rn_checksum.SequenceEqual(localNewBlock.regNameStateChecksum))
                                {
                                    rn_checksum_ok = true;
                                }
                            }

                            if (!ws_checksum_ok)
                            {
                                Logging.error("After applying block #{0} v{1}, walletStateChecksum is incorrect, shutting down!. Block's WS: {2}, actual WS: {3}", localNewBlock.blockNum,
                                    localNewBlock.version, Crypto.hashToString(localNewBlock.walletStateChecksum), Crypto.hashToString(ws_checksum));
                                // TODO TODO perhaps try reverting the block
                                operating = false;
                                Node.stop();
                                return false;
                            } else if (!rn_checksum_ok)
                            {
                                Logging.error("After applying block #{0} v{1}, RegNameStateChecksum is incorrect, shutting down!. Block's RN: {2}, actual RN: {3}", localNewBlock.blockNum,
                                    localNewBlock.version, Crypto.hashToString(localNewBlock.regNameStateChecksum), Crypto.hashToString(rn_checksum));
                                // TODO TODO perhaps try reverting the block
                                operating = false;
                                Node.stop();
                                return false;
                            }
                            else
                            {
                                Node.walletState.commitTransaction(localNewBlock.blockNum);
                                Node.regNameState.commitTransaction(localNewBlock.blockNum);
                                // append current block
                                Node.blockChain.appendBlock(localNewBlock);

                                currentBlockStartTime = DateTime.UtcNow;
                                lastBlockStartTime = DateTime.UtcNow;
                                last_block = localNewBlock;
                                last_block_num = localNewBlock.blockNum;
                                Block current_block = localNewBlock;
                                localNewBlock = null;
                                forkedFlag = false;

                                try
                                {
                                    if (Config.blockNotifyCommand != "")
                                    {
                                        IxiUtils.executeProcess(Config.blockNotifyCommand, current_block.blockNum.ToString(), false);
                                    }
                                }
                                catch (Exception e)
                                {
                                    Logging.error("Exception occurred in BlockProcessor:acceptLocalNewBlock: " + e);
                                }

                                pendingSuperBlocks.Remove(current_block.blockNum);

                                if (current_block.blockNum > 5)
                                {
                                    // append sigfreezed block
                                    Block tmp_block = Node.blockChain.getBlock(current_block.blockNum - 5);
                                    if (tmp_block != null)
                                    {
                                        if (tmp_block.frozenSignatures != null)
                                        {
                                            // TODO TODO should frozen sigs really be copied to signatures and nulled?
                                            //tmp_block.signatures = tmp_block.frozenSignatures;
                                            //tmp_block.setFrozenSignatures(null);
                                        }
                                        Node.blockChain.updateBlock(tmp_block);
                                    }
                                }

                                block_accepted = true;

                                // Adjust block generation time to get close to the block generation interval target
                                Block tmp_prev_block = Node.blockChain.getBlock(current_block.blockNum - 1);
                                if (tmp_prev_block != null)
                                {
                                    averageBlockGenerationInterval = (averageBlockGenerationInterval + (current_block.timestamp - tmp_prev_block.timestamp)) / 2;

                                    if (averageBlockGenerationInterval > ConsensusConfig.blockGenerationInterval + 1)
                                    {
                                        blockGenerationInterval = ConsensusConfig.minBlockTimeDifference + 1;
                                    }
                                    else if (averageBlockGenerationInterval + 1 < ConsensusConfig.blockGenerationInterval)
                                    {
                                        blockGenerationInterval = ConsensusConfig.blockGenerationInterval;
                                    }
                                }

                                if (Node.miner.searchMode == BlockSearchMode.latestBlock)
                                {
                                    Node.miner.forceSearchForBlock();
                                }

                                Logging.info(String.Format("Accepted block #{0}.", current_block.blockNum));
                                current_block.logBlockDetails();

                                // Reset transaction limits
                                //TransactionPool.resetSocketTransactionLimits();

                                IxianHandler.status = NodeStatus.ready;

                                if (highestNetworkBlockNum > last_block_num)
                                {
                                    BlockProtocolMessages.broadcastGetBlock(last_block_num + 1, null, null, 1);
                                }
                                else
                                {
                                    highestNetworkBlockNum = 0;
                                }

                                CoreProtocolMessage.addToInventory(new char[] { 'W' }, new InventoryItemBlock(current_block.blockChecksum, current_block.blockNum), null);

                                if (Node.miner.searchMode != BlockSearchMode.latestBlock)
                                {
                                    Node.miner.checkActiveBlockSolved();
                                }

                                // Broadcast blockheight only if the node is synchronized
                                if (!Node.blockSync.synchronizing)
                                {
                                    BlockProtocolMessages.broadcastBlockHeight(last_block_num, current_block.blockChecksum);
                                }

                                cleanupBlockBlacklist();
                                if (last_block_num % Config.saveWalletStateEveryBlock == 0)
                                {
                                    if (last_block.version >= BlockVer.v11)
                                    {
                                        Node.regNamesMemoryStorage.saveToDisk(last_block_num);
                                    }
                                    WalletStateStorage.saveWalletState(last_block_num);
                                }

                                if (Config.enableChainReorgTest)
                                {
                                    chainReorgTest(last_block_num);
                                }
                            }
                        }
                        else if (Node.blockChain.getBlock(localNewBlock.blockNum) == null)
                        {
                            Logging.error(String.Format("Couldn't apply accepted block #{0}.", localNewBlock.blockNum));
                            localNewBlock.logBlockDetails();
                            requestBlockNum = localNewBlock.blockNum;
                            Node.walletState.revertTransaction(localNewBlock.blockNum);
                            Node.regNameState.revertTransaction(localNewBlock.blockNum);
                            Node.blockChain.revertBlockTransactions(localNewBlock);
                            localNewBlock = null;
                            requestBlockAgain = true;
                        }
                        else
                        {
                            localNewBlock = null;
                        }
                    }
                }
            }

            sw.Stop();
            TimeSpan elapsed = sw.Elapsed;
            Logging.info(string.Format("VerifyBlockAcceptance took: {0}ms {1}", elapsed.TotalMilliseconds, block_accepted));



            // Check if we should request the block again
            if (requestBlockAgain && requestBlockNum > 0)
            {
                // Show a notification
                Logging.error(string.Format("Requesting block {0} again due to previous mismatch.", requestBlockNum));
                // Request the block again
                BlockProtocolMessages.broadcastGetBlock(requestBlockNum);
            }

            return block_accepted;
        }

        ulong lastChainReorgBlockNumTest = 0;
        public bool chainReorgTest(ulong block_num)
        {
            if (lastChainReorgBlockNumTest < block_num)
            {
                lastChainReorgBlockNumTest = block_num;
                lock (localBlockLock)
                {
                    Node.blockChain.revertLastBlock(false);
                    if (IxianHandler.isTestNet && block_num > 10)
                    {
                        Node.blockChain.revertLastBlock(false);
                        Node.blockChain.revertLastBlock(false);
                        Node.blockChain.revertLastBlock(false);
                        Node.blockChain.revertLastBlock(false);
                        Node.blockChain.revertLastBlock(false);
                        Node.blockChain.revertLastBlock(false);
                        Node.blockChain.revertLastBlock(false);
                    }
                }
                return true;
            }
            return false;
        }

        public bool verifySignatureFreezeChecksum(Block b, RemoteEndpoint endpoint, bool fetchMissingBlockOnFail = true)
        {
            if(Node.blockChain.Count <= 5)
            {
                return true;
            }

            bool nextBlock = false;
            if (IxianHandler.getLastBlockHeight() + 1 == b.blockNum)
            {
                nextBlock = true;
            }
            
            if (b.signatureFreezeChecksum != null)
            {
                Block targetBlock = Node.blockChain.getBlock(b.blockNum - 5);
                if (targetBlock == null)
                {
                    // this shouldn't be possible
                    Logging.error("Block verification can't be done since we are missing sigfreeze checksum target block {0}.", b.blockNum - 5);
                    if(nextBlock && fetchMissingBlockOnFail)
                    {
                        BlockProtocolMessages.broadcastGetBlock(b.blockNum - 5, null, endpoint);
                    }
                    return false;
                }
                byte[] sigFreezeChecksum = targetBlock.calculateSignatureChecksum();
                if (!b.signatureFreezeChecksum.SequenceEqual(sigFreezeChecksum) || !targetBlock.verifyBlockProposer())
                {
                    Logging.warn("Block sigFreeze verification failed for #{0}. Checksum is {1}, but should be {2}. Requesting block #{3}",
                        b.blockNum, Crypto.hashToString(b.signatureFreezeChecksum), Crypto.hashToString(sigFreezeChecksum), b.blockNum - 5);
                    if (nextBlock && fetchMissingBlockOnFail)
                    {
                        BlockProtocolMessages.broadcastGetBlock(b.blockNum - 5, null, endpoint);
                    }
                    return false;
                }
            }
            else if (b.blockNum > 7)
            {
                // this shouldn't be possible
                Block targetBlock = Node.blockChain.getBlock(b.blockNum - 5);
                Logging.error("Block sigFreeze verification failed for #{0}. Checksum is empty but should be {1}. Requesting block #{2}",
                    b.blockNum, Crypto.hashToString(targetBlock.calculateSignatureChecksum()), b.blockNum - 5);
                if (nextBlock && fetchMissingBlockOnFail)
                {
                    BlockProtocolMessages.broadcastGetBlock(b.blockNum, endpoint);
                }
                return false;
            }

            return true;
        }

        // Applies the block
        // Returns false if walletstate is not correct
        public bool applyAcceptedBlock(Block b, bool generating_new = false)
        {
            try
            {
                if (Node.blockChain.getBlock(b.blockNum) != null)
                {
                    Logging.warn("Block #{0} has already been applied. Stack trace: {1}", b.blockNum, Environment.StackTrace);
                    return false;
                }

                // Distribute staking rewards first
                if (Config.fullBlockLogging) { Logging.info("Applying block #{0} -> distributingStakingRewards (version {1})", b.blockNum, b.version); }
                try
                {
                    distributeStakingRewards(b);
                }
                catch (Exception)
                {
                    return false;
                }

                // Remove Expired Names
                if (b.version >= BlockVer.v11)
                {
                    if (b.blockNum > ConsensusConfig.rnGracePeriodInBlocks)
                    {
                        Node.regNameState.removeExpiredNames(b.blockNum - ConsensusConfig.rnGracePeriodInBlocks);
                    }
                }
                
                // Apply transactions from block
                if (!TransactionPool.applyTransactionsFromBlock(b, generating_new))
                {
                    return false;
                }

                if (b.version < BlockVer.v10)
                {
                    // Apply transaction fees
                    if (Config.fullBlockLogging) { Logging.info("Applying block #{0} -> applyTransactionFeeRewards (version {1})", b.blockNum, b.version); }
                    applyTransactionFeeRewards(b);

                    if (b.blockNum < 10)
                    {
                        updateWalletStatePublicKeys(b.blockNum);
                    }
                }
                else if (b.version >= BlockVer.v11)
                {
                    IxiNumber nameRewardAmount = calculateNameReward(b.blockNum);
                    if (nameRewardAmount > 0 && !Node.regNameState.decreaseRewardPool(nameRewardAmount))
                    {
                        Logging.error("Error while decreasing reward pool.");
                        return false;
                    }
                }

                return true;
            }catch(Exception e)
            {
                Logging.error("Exception occurred in applyAcceptedBlock(): " + e);
            }
            return false;
        }

        public static IxiNumber calculateNameReward(ulong blockHeight)
        {
            ulong highestExpirationBlockHeight = Node.regNameState.getHighestExpirationBlockHeight();
            if (highestExpirationBlockHeight <= blockHeight)
            {
                return Node.regNameState.getRewardPool();
            }
            IxiNumber nameRewardAmount = Node.regNameState.getRewardPool() / (highestExpirationBlockHeight - blockHeight);
            return nameRewardAmount;
        }

        public IxiNumber calculateTotalTransactionFeeReward(Block targetBlock)
        {
            // Calculate the total transactions amount and number of transactions in the target block
            IxiNumber tAmount = 0;
            IxiNumber tFeeAmount = 0;

            ulong txcount = 0;
            foreach (byte[] txid in targetBlock.transactions)
            {
                Transaction tx = TransactionPool.getAppliedTransaction(txid, targetBlock.blockNum);
                if (tx != null)
                {
                    if (tx.type == (int)Transaction.Type.Normal)
                    {
                        tAmount += tx.amount;
                        tFeeAmount += tx.fee;
                        txcount++;
                    }
                    else if (tx.type == (int)Transaction.Type.MultisigTX)
                    {
                        Transaction.MultisigTxData ms_data = (Transaction.MultisigTxData)tx.GetMultisigData();
                        if (ms_data.origTXId == null)
                        {
                            tAmount += tx.amount;
                        }
                        tFeeAmount += tx.fee;
                        txcount++;
                    }
                    else if (tx.type == (int)Transaction.Type.ChangeMultisigWallet || tx.type == (int)Transaction.Type.MultisigAddTxSignature)
                    {
                        tFeeAmount += tx.fee;
                        txcount++;
                    }
                }
                else
                {
                    Logging.error("Error calculating transaction fee reward, transaction {0} is missing", txid);
                    throw new Exception(String.Format("Error calculating transaction fee reward, transaction {0} is missing", txid));
                }
            }
            return tFeeAmount;
        }

        public void applyTransactionFeeRewards(Block b)
        {
            byte[] sigfreezechecksum = null;
            lock (localBlockLock)
            {
                // Should never happen
                if (b == null)
                {
                    Logging.warn("Applying fee rewards: block is null.");
                    return;
                }
                if (b.blockNum > 1)
                {
                    sigfreezechecksum = Node.blockChain.getBlock(b.blockNum - 1).signatureFreezeChecksum;
                }
            }

            // Ignore blocks before #7
            if (b.blockNum < 7)
            {
                return;
            }

            if (sigfreezechecksum == null)
            {
                Logging.warn("Block {0} does not have sigfreeze checksum.", b.blockNum);
                return;
            }

            // Obtain the 6th last block, aka target block
            Block targetBlock = null;

            targetBlock = Node.blockChain.getBlock(b.blockNum - 6);
            if (targetBlock == null)
                return;

            byte[] targetSigFreezeChecksum = targetBlock.calculateSignatureChecksum();

            if (sigfreezechecksum.SequenceEqual(targetSigFreezeChecksum) == false)
            {
                Logging.warn("Signature freeze mismatch for block {0}. Current block height: {1}", targetBlock.blockNum, b.blockNum);
                // TODO: fetch the block again or re-sync
                return;
            }

            // Calculate the total transactions amount and number of transactions in the target block
            IxiNumber totalFeeAmount = targetBlock.totalFee;
            if (totalFeeAmount == 0)
            {
                totalFeeAmount = calculateTotalTransactionFeeReward(targetBlock);
                targetBlock.totalFee = totalFeeAmount;
                if (targetBlock.totalFee != 0)
                {
                    Logging.warn("Total fee amount for block #{0} was 0, recalculated.", targetBlock.blockNum);
                }
            }

            ulong txcount = targetBlock.getTransactionsCount();

            // Check if there are any transactions processed in the target block
            if(txcount < 1)
            { 
                return;
            }

            // Check the amount
            if(totalFeeAmount == (long) 0)
            {
                return;
            }

            IxiNumber foundation_balance_after = 0;
            IxiNumber foundationAward = 0;
            if (b.version < BlockVer.v8)
            {
                // Calculate the total fee amount
                foundationAward = totalFeeAmount * ConsensusConfig.foundationFeePercent / 100;

                // Award foundation fee
                Wallet foundation_wallet = Node.walletState.getWallet(ConsensusConfig.foundationAddress);
                IxiNumber foundation_balance_before = foundation_wallet.balance;
                foundation_balance_after = foundation_balance_before + foundationAward;
                Node.walletState.setWalletBalance(ConsensusConfig.foundationAddress, foundation_balance_after);
                //Logging.info(string.Format("Awarded {0} IXI to foundation", foundationAward.ToString()));

                // Subtract the foundation award from total fee amount
                totalFeeAmount = totalFeeAmount - foundationAward;
            }

            List<BlockSignature> target_block_sigs = null;
            if(targetBlock.frozenSignatures != null)
            {
                target_block_sigs = targetBlock.frozenSignatures;
            }else
            {
                target_block_sigs = targetBlock.signatures;
            }

            ulong numSigs = (ulong)target_block_sigs.Count();
            if(numSigs < 1)
            {
                // Something is not right, there are no signers on this block
                Logging.error("Transaction fee: no signatures on block!");
                return;
            }

            // Calculate the award per signer
            IxiNumber sigs = new IxiNumber(numSigs);

            IxiNumber tAward = IxiNumber.divRem(totalFeeAmount, sigs, out IxiNumber remainder);

            if (b.version < BlockVer.v8)
            {
                // Division of fee amount and sigs left a remainder, distribute that to the foundation wallet
                if (remainder > (long) 0)
                {
                    foundation_balance_after = foundation_balance_after + remainder;
                    Node.walletState.setWalletBalance(ConsensusConfig.foundationAddress, foundation_balance_after);
                    //Logging.info(string.Format("Awarded {0} IXI to foundation from fee division remainder", foundationAward.ToString()));
                    remainder = 0;
                }
            }

            // Go through each signature in the block
            foreach (BlockSignature sig in target_block_sigs)
            {
                // Generate the corresponding Ixian address
                Address sigAddress = sig.recipientPubKeyOrAddress;

                // Update the walletstate and deposit the award
                Wallet signer_wallet = Node.walletState.getWallet(sigAddress);
                IxiNumber balance_before = signer_wallet.balance;
                IxiNumber balance_after = balance_before + tAward;
                // Add remainder to the first signer
                if (b.version >= BlockVer.v8)
                {
                    if (remainder > (long)0)
                    {
                        balance_after = balance_after + remainder;
                        remainder = 0;
                    }
                }
                Node.walletState.setWalletBalance(sigAddress, balance_after);
                if(!Node.walletState.inTransaction)
                {
                    WalletStorage ws = IxianHandler.getWalletStorage();
                    if (signer_wallet.id.addressNoChecksum.SequenceEqual(ws.getPrimaryAddress().addressNoChecksum))
                    {
                        SortedDictionary<Address, Transaction.ToEntry> to_list = new SortedDictionary<Address, Transaction.ToEntry>(new AddressComparer());
                        to_list.Add(sigAddress, new Transaction.ToEntry(Transaction.getExpectedVersion(b.version), tAward));
                        string address = ws.getPrimaryAddress().ToString();
                        Activity activity = new Activity(ws.getSeedHash(), address, ConsensusConfig.ixianInfiniMineAddress.ToString(), to_list, (int)ActivityType.TxFeeReward, Encoding.UTF8.GetBytes("TXFEEREWARD-" + b.blockNum + "-" + address), tAward.ToString(), b.timestamp, (int)ActivityStatus.Final, b.blockNum, "");
                        ActivityStorage.insertActivity(activity);
                    }
                }
                //Logging.info(string.Format("Awarded {0} IXI to {1}", tAward.ToString(), addr.ToString()));
            }          
        }

        // returns false if this is a multisig transaction and not enough signatures - in this case, it should not be added to the block
        // returns true for all other transaction types
        private ulong includeMultisigTransactions(Transaction transaction, Dictionary<byte[], IxiNumber> minusBalances)
        {
            // NOTE: this function is called exclusively from generateNewBlock(), so we do not need to lock anything - 'localNewBlock' is alredy locked.
            // If this is called from anywhere else, add a lock here!
            // multisig transactions must be complete before they are added
            object multisig_data = transaction.GetMultisigData();
            byte[] orig_txid = transaction.id;
            Address address = new Address(transaction.pubKey.addressNoChecksum, transaction.fromList.Keys.First());
            Wallet from_w = Node.walletState.getWallet(address);
            List<byte[]> related_tx_ids = TransactionPool.getRelatedMultisigTransactions(orig_txid, null);
            int num_valid_multisigs = related_tx_ids.Count() + 1;
            if (num_valid_multisigs >= from_w.requiredSigs)
            {
                localNewBlock.addTransaction(orig_txid);
                IxiNumber total_amount = transaction.amount + transaction.fee;
                foreach (byte[] txid in related_tx_ids)
                {
                    Transaction tx = TransactionPool.getUnappliedTransaction(txid);
                    if(!verifyFromListBalance(tx, minusBalances))
                    {
                        minusBalances[address.addressNoChecksum] -= total_amount;
                        return 0;
                    }
                    total_amount += tx.amount + tx.fee;
                    localNewBlock.addTransaction(txid);
                }
                // include the multisig transaction
                return (ulong)related_tx_ids.Count() + 1;
            } else
            {
                // skip the multisig transaction
                return 0;
            }
        }

        public bool verifyFromListBalance(Transaction transaction, Dictionary<byte[], IxiNumber> minusBalances)
        {
            foreach (var entry in transaction.fromList)
            {
                Address address = new Address(transaction.pubKey.addressNoChecksum, entry.Key);
                // TODO TODO TODO TODO plus balances should also be added (and be processed first) to prevent overspending false alarms
                if (!minusBalances.ContainsKey(address.addressNoChecksum))
                {
                    minusBalances.Add(address.addressNoChecksum, 0);
                }

                // prevent overspending
                if (transaction.type != (int)Transaction.Type.Genesis
                    && transaction.type != (int)Transaction.Type.PoWSolution
                    && transaction.type != (int)Transaction.Type.StakingReward)
                {
                    IxiNumber new_minus_balance = minusBalances[address.addressNoChecksum] + entry.Value;
                    IxiNumber from_balance = Node.walletState.getWalletBalance(address);

                    if (from_balance < new_minus_balance)
                    {
                        // TODO TODO TODO TODO TODO, it might not be the best idea to remove overspent transaction here as the block isn't confirmed yet,
                        // we should do this after the block has been confirmed
                        TransactionPool.removeUnappliedTransaction(transaction.id);
                        return false;
                    }
                    minusBalances[address.addressNoChecksum] = new_minus_balance;
                }

            }
            return true;
        }

        private IxiNumber generateNewBlockTransactions(ulong block_num, int block_version)
        {
            ulong total_transactions = 1;
            IxiNumber total_amount = 0;

            List<Transaction> unapplied_transactions = TransactionPool.getUnappliedTransactions().ToList<Transaction>();
            unapplied_transactions = unapplied_transactions.OrderBy(x => x.id, new ByteArrayComparer()).ToList(); // TODO add fee/weight

            // TODO TODO optimize this
            List<Transaction> pool_transactions = new List<Transaction>();
            if (block_num < ConsensusConfig.miningExpirationBlockHeight)
            {
                pool_transactions.AddRange(unapplied_transactions.Where(x => x.type == (int)Transaction.Type.PoWSolution).ToList<Transaction>()); // add PoW first
            }
            pool_transactions.AddRange(unapplied_transactions.Where(x => x.type == (int)Transaction.Type.ChangeMultisigWallet)); // then add MS wallet changes
            pool_transactions.AddRange(unapplied_transactions.Where(x => x.type == (int)Transaction.Type.MultisigTX)); // then add MS TXs
            pool_transactions.AddRange(unapplied_transactions.Where(x => x.type != (int)Transaction.Type.PoWSolution && x.type != (int)Transaction.Type.ChangeMultisigWallet && x.type != (int)Transaction.Type.MultisigTX && x.type != (int)Transaction.Type.MultisigAddTxSignature)); // finally add all other TXs

            ulong normal_transactions = 0; // Keep a counter of normal transactions for the limiter

            Dictionary<byte[], IxiNumber> minusBalances = new Dictionary<byte[], IxiNumber>(new ByteArrayComparer());

            Dictionary<ulong, List<object[]>> blockSolutionsDictionary = new Dictionary<ulong, List<object[]>>();

            IxiNumber total_fee = 0;

            foreach (var transaction in pool_transactions)
            {
                // Check if we reached the transaction limit for this block
                if (normal_transactions >= Config.maxTransactionsPerBlockToInclude)
                {
                    // Limit all other transactions
                    break;
                }

                // lock transaction v6 with block v10
                if (block_version >= BlockVer.v10 && (transaction.version < 6 || transaction.version > 7))
                {
                    if (Node.blockChain.getLastBlockVersion() >= BlockVer.v9 && transaction.version < 6)
                    {
                        TransactionPool.removeUnappliedTransaction(transaction.id);
                    }
                    continue;
                }

                // Verify that the transaction is actually valid at this point
                // no need as the tx is already in the pool and was verified when received
                //if (TransactionPool.verifyTransaction(transaction) == false)
                //    continue;

                // Skip adding staking rewards
                if (transaction.type == (int)Transaction.Type.StakingReward)
                {
                    TransactionPool.removeUnappliedTransaction(transaction.id);
                    continue;
                }

                ulong minBh = 0;
                if (localNewBlock.blockNum > ConsensusConfig.getRedactedWindowSize(localNewBlock.version))
                {
                    minBh = localNewBlock.blockNum - ConsensusConfig.getRedactedWindowSize(localNewBlock.version);
                }
                // Check the block height
                if (minBh > transaction.blockHeight)
                {
                    TransactionPool.removeUnappliedTransaction(transaction.id);
                    continue;
                }
                if(transaction.blockHeight > localNewBlock.blockNum)
                {
                    continue;
                }

                // Special case for PoWSolution transactions
                if (transaction.type == (int)Transaction.Type.PoWSolution)
                {
                    // TODO: pre-validate the transaction in such a way it doesn't affect performance
                    ulong powBlockNum = 0;
                    byte[] nonce = null;
                    if (!TransactionPool.verifyPoWTransaction(transaction, out powBlockNum, out nonce, block_version))
                    {
                        TransactionPool.removeUnappliedTransaction(transaction.id);
                        continue;
                    }
                    else
                    {
                        // Check if we already have a key matching the block number
                        if (blockSolutionsDictionary.ContainsKey(powBlockNum) == false)
                        {
                            blockSolutionsDictionary[powBlockNum] = new List<object[]>();
                        }
                        if (block_version >= BlockVer.v2)
                        {
                            byte[] tmp_address = transaction.pubKey.addressNoChecksum;
                            if (!blockSolutionsDictionary[powBlockNum].Exists(x => ((byte[])x[0]).SequenceEqual(tmp_address) && ((byte[])x[1]).SequenceEqual(nonce)))
                            {
                                // Add the miner to the block number dictionary reward list
                                blockSolutionsDictionary[powBlockNum].Add(new object[3] { tmp_address, nonce, transaction });
                            }
                            else
                            {
                                TransactionPool.removeUnappliedTransaction(transaction.id);
                                continue;
                            }
                        }
                    }
                }

                if (!verifyFromListBalance(transaction, minusBalances))
                {
                    continue;
                }

                // do not include in block RN transactions with lower than configured price
                if (transaction.type == (int)Transaction.Type.RegName && !Node.regNameState.verifyTransaction(transaction, ConsensusConfig.rnPricePerUnit))
                {
                    continue;
                }

                IxiNumber total_tx_amount = transaction.amount + transaction.fee;
                total_fee += transaction.fee;

                if (transaction.type == (int)Transaction.Type.MultisigTX || transaction.type == (int)Transaction.Type.ChangeMultisigWallet)
                {
                    if (normal_transactions >= Config.maxTransactionsPerBlockToInclude - 251)
                    {
                        continue;
                    }
                    ulong ms_transactions = includeMultisigTransactions(transaction, minusBalances);
                    if (ms_transactions == 0)
                    {
                        continue;
                    }
                    total_transactions += ms_transactions;
                    normal_transactions += ms_transactions;
                }
                else if (transaction.type != (int)Transaction.Type.MultisigAddTxSignature)
                {
                    localNewBlock.addTransaction(transaction.id);
                    total_transactions++;
                    normal_transactions++;
                }

                total_amount += total_tx_amount;
            }


            Logging.info("\t\t|- Transactions: {0} \t\t Amount: {1} \t\t Fee: {2}", total_transactions, total_amount, total_fee);

            return total_fee;
        }

        public bool generateSuperBlockSegments(Block super_block, bool new_block, RemoteEndpoint endpoint = null)
        {
            lock (superBlockLock)
            {
                ulong cur_block_height = super_block.blockNum;

                // TODO TODO TODO implement getLastSuperBlockNum with sync properly
                if (!new_block && Node.blockChain.getLastSuperBlockNum() > 0 && Node.blockChain.getLastSuperBlockNum() != super_block.lastSuperBlockNum)
                {
                    return false;
                }

                if (cache_currentSuperBlockSegments != null)
                {
                    if(cache_currentSuperBlockSegments.ContainsKey(cur_block_height - 1) 
                        && cache_currentSuperBlockSegments[cur_block_height - 1].blockChecksum.SequenceEqual(Node.blockChain.getBlock(cur_block_height - 1).blockChecksum)
                        && cache_currentSuperBlockSegments.ContainsKey(super_block.lastSuperBlockNum + 1))
                    {
                        Logging.info("Setting cached superblock segments to received superblock #{0}", super_block.blockNum);
                        super_block.superBlockSegments = cache_currentSuperBlockSegments;
                        super_block.lastSuperBlockNum = cache_lastSuperBlockNum;
                        super_block.lastSuperBlockChecksum = cache_lastSuperBlockChecksum;
                        return true;
                    }
                    else
                    {
                        cache_currentSuperBlockSegments = null;
                        cache_lastSuperBlockNum = 0;
                        cache_lastSuperBlockChecksum = null;
                    }
                }

                Logging.info("Generating superblock segments for block #{0}", super_block.blockNum);

                super_block.superBlockSegments.Clear();
                for (ulong i = cur_block_height - 1; i > 0; i--)
                {
                    Block b = Node.blockChain.getBlock(i, true);
                    if (b == null)
                    {
                        Logging.error("Unable to find block {0} while creating superblock {1}.", i, cur_block_height);
                        BlockProtocolMessages.broadcastGetBlock(i, endpoint);
                        return false;
                    }

                    if (b.version > BlockVer.v4 && b.lastSuperBlockChecksum != null)
                    {
                        super_block.lastSuperBlockNum = b.blockNum;
                        super_block.lastSuperBlockChecksum = b.blockChecksum;
                        break;
                    }

                    if (b.signatureFreezeChecksum != null && i > 5)
                    {
                        Block target_block = Node.blockChain.getBlock(i - 5, true);
                        if (target_block == null)
                        {
                            Logging.error("Unable to find target block {0} while creating superblock {1}.", i - 5, super_block.blockNum);
                            BlockProtocolMessages.broadcastGetBlock(i - 5, endpoint);
                            return false;
                        }
                        else if (!target_block.calculateSignatureChecksum().SequenceEqual(b.signatureFreezeChecksum))
                        {
                            Logging.error("Target block's {0} signatures don't match sigfreeze, while creating superblock {1}.", i - 5, super_block.blockNum);
                            SignatureProtocolMessages.broadcastGetBlockSignatures(target_block.blockNum, target_block.blockChecksum, endpoint);
                            return false;
                        }
                    }

                    SuperBlockSegment seg = new SuperBlockSegment(b.blockNum, b.blockChecksum);

                    super_block.superBlockSegments.Add(b.blockNum, seg);

                }

                if (super_block.lastSuperBlockChecksum == null)
                {
                    Block b = Node.blockChain.getBlock(1, true);
                    if (b == null)
                    {
                        Logging.error("Unable to find genesis block for superblock {0}.", super_block.blockNum);
                        return false;
                    }
                    super_block.lastSuperBlockNum = b.blockNum;
                    super_block.lastSuperBlockChecksum = b.blockChecksum;
                }

                cache_currentSuperBlockSegments = super_block.superBlockSegments;
                cache_lastSuperBlockNum = super_block.lastSuperBlockNum;
                cache_lastSuperBlockChecksum = super_block.lastSuperBlockChecksum;
            }

            return true;
        }

        // Generate a new block
        public void generateNewBlock(int block_version)
        {
            if (!Node.isMasterNode())
            {
                Block last_block = Node.blockChain.getLastBlock();
                if (last_block != null)
                {
                    Network.BlockProtocolMessages.broadcastGetBlock(last_block.blockNum + 1);
                }
                return;
            }

            lock (localBlockLock)
            {
                try
                {
                    Logging.info("GENERATING NEW BLOCK");

                    // Create a new block and add all the transactions in the pool
                    localNewBlock = new Block();
                    localNewBlock.timestamp = Clock.getNetworkTimestamp();
                    if(IxianHandler.getLastBlockVersion() < BlockVer.v10)
                    {
                        localNewBlock.blockProposer = IxianHandler.getWalletStorage().getPrimaryAddress().addressWithChecksum;
                    }

                    Block last_block = Node.blockChain.getLastBlock();
                    if (last_block != null)
                    {
                        localNewBlock.blockNum = last_block.blockNum + 1;
                        localNewBlock.lastBlockChecksum = last_block.blockChecksum;
                    }
                    else
                    {
                        // genesis block
                        localNewBlock.blockNum = 1;
                        localNewBlock.lastBlockChecksum = null;
                    }

                    localNewBlock.version = block_version;

                    Node.walletState.setCachedBlockVersion(block_version);
                    Node.regNameState.setCachedBlockVersion(block_version);

                    Logging.info("\t\t|- Block Number: {0}", localNewBlock.blockNum);

                    ulong stakingRewardBlockNum = 0;
                    if (block_version >= BlockVer.v10)
                    {
                        if (localNewBlock.blockNum > ConsensusConfig.rewardMaturity)
                        {
                            stakingRewardBlockNum = localNewBlock.blockNum - ConsensusConfig.rewardMaturity;
                        }
                    }
                    else
                    {
                        if (localNewBlock.blockNum >= 10)
                        {
                            stakingRewardBlockNum = localNewBlock.blockNum - 6;
                        }
                    }

                    // Apply staking transactions to block. 
                    List<Transaction> staking_transactions = generateStakingTransactions(stakingRewardBlockNum, block_version, localNewBlock.timestamp);
                    foreach (Transaction transaction in staking_transactions)
                    {
                        localNewBlock.addTransaction(transaction.id);
                    }
                    staking_transactions.Clear();

                    // Prevent calculations if we don't have 5 fully generated blocks yet
                    if (localNewBlock.blockNum > 5)
                    {
                        // Apply signature freeze
                        localNewBlock.signatureFreezeChecksum = getSignatureFreeze(localNewBlock, localNewBlock.version);
                    }

                    if (localNewBlock.blockNum % ConsensusConfig.superblockInterval == 0)
                    {
                        // superblock

                        // collect all blocks up to last superblock (or genesis block if no superblock yet exists)
                        if (!generateSuperBlockSegments(localNewBlock, true))
                        {
                            Logging.error("Error generating segments for superblock {0}.", localNewBlock.blockNum);
                            localNewBlock = null;
                            return;
                        }

                        // Calculate signer difficulty
                        localNewBlock.signerBits = Node.blockChain.calculateRequiredSignerBits(false, localNewBlock.version, localNewBlock.timestamp);
                    }
                    else
                    {
                        localNewBlock.totalFee = generateNewBlockTransactions(localNewBlock.blockNum, block_version);
                        if (localNewBlock.blockNum == 1)
                        {
                            // Calculate signer difficulty
                            localNewBlock.signerBits = Node.blockChain.calculateRequiredSignerBits(false, localNewBlock.version, localNewBlock.timestamp);
                        }
                    }

                    if (localNewBlock.version >= BlockVer.v11)
                    {
                        IxiNumber nameRewardAmount = calculateNameReward(localNewBlock.blockNum);
                        localNewBlock.totalFee += nameRewardAmount;
                    }

                    // Calculate mining difficulty
                    localNewBlock.difficulty = calculateDifficulty(block_version);

                    // Simulate applying a block to see what the walletstate would look like
                    Node.walletState.beginTransaction(localNewBlock.blockNum);
                    Node.regNameState.beginTransaction(localNewBlock.blockNum);
                    if (!applyAcceptedBlock(localNewBlock, true))
                    {
                        Logging.error("Unable to apply a snapshot of a newly generated block {0}.", localNewBlock.blockNum);
                        Node.walletState.revertTransaction(localNewBlock.blockNum);
                        Node.regNameState.revertTransaction(localNewBlock.blockNum);
                        localNewBlock = null;
                        return;
                    }
                    
                    if (localNewBlock.lastSuperBlockChecksum == null)
                    {
                        localNewBlock.setWalletStateChecksum(Node.walletState.calculateWalletStateDeltaChecksum(localNewBlock.blockNum, localNewBlock.version));
                    }
                    else
                    {
                        localNewBlock.setWalletStateChecksum(Node.walletState.calculateWalletStateChecksum());
                    }

                    if (localNewBlock.version >= BlockVer.v11)
                    {
                        localNewBlock.setRegNameStateChecksum(Node.regNameState.calculateRegNameStateChecksum(localNewBlock.blockNum));
                    }
                    
                    Logging.info("While generating new block: Node's blockversion: {0}", localNewBlock.version);
                    Logging.info("While generating new block: WS Checksum: {0}", Crypto.hashToString(localNewBlock.walletStateChecksum));
                    if (localNewBlock.regNameStateChecksum != null)
                    {
                        Logging.info("While generating new block: RN Checksum: {0}", Crypto.hashToString(localNewBlock.regNameStateChecksum));
                    }
                    Node.walletState.revertTransaction(localNewBlock.blockNum);
                    Node.regNameState.revertTransaction(localNewBlock.blockNum);

                    localNewBlock.blockChecksum = localNewBlock.calculateChecksum();

                    removeBlockBlacklist(localNewBlock);

                    BlockSignature signature_data = localNewBlock.applySignature(PresenceList.getPowSolution());
                    if (signature_data != null)
                    {
                        Node.inventoryCache.setProcessedFlag(InventoryItemTypes.blockSignature, InventoryItemSignature.getHash(signature_data.recipientPubKeyOrAddress.addressNoChecksum, localNewBlock.blockChecksum), true);
                    }else
                    {
                        Logging.error("Could not apply signature on a newly generated block {0}.", localNewBlock.blockNum);
                        localNewBlock = null;
                        return;
                    }

                    localNewBlock.logBlockDetails();

                    currentBlockStartTime = DateTime.UtcNow;
                    lastBlockStartTime = DateTime.UtcNow.AddSeconds(-blockGenerationInterval * 10); // TODO TODO TODO make sure that this is ok

                    // Broadcast the new block
                    BlockProtocolMessages.broadcastNewBlock(localNewBlock, null, null, true);

                    if (verifyBlock(localNewBlock) != BlockVerifyStatus.Valid)
                    {
                        Logging.error("Error occurred while verifying the newly generated block {0}.", localNewBlock.blockNum);
                        localNewBlock = null;
                        return;
                    }

                    if (localNewBlock.blockNum < 8)
                    {
                        acceptLocalNewBlock();
                    }
                }catch(Exception e)
                {
                    Logging.error("Exception occurred while generating block {0}: {1}", IxianHandler.getLastBlockHeight() + 1, e);
                    localNewBlock = null;
                }
            }
        }

        public static ulong calculateDifficulty(int version)
        {
            if (version == BlockVer.v0)
            {
                return calculateDifficulty_v0();
            }
            else if (version == BlockVer.v1)
            {
                return calculateDifficulty_v1();
            }else if(version == BlockVer.v2)
            {
                return calculateDifficulty_v2();
            }else if(version < BlockVer.v10)
            {
                return calculateDifficulty_v3();
            }
            else // >= 10
            {
                return calculateDifficulty_v4();
            }
        }

        // Calculate the current mining difficulty
        public static ulong calculateDifficulty_v0()
        {
            ulong current_difficulty = 14;
            if (Node.blockChain.getLastBlockNum() > 1)
            {
                Block previous_block = Node.blockChain.getBlock(Node.blockChain.getLastBlockNum());
                if (previous_block != null)
                    current_difficulty = previous_block.difficulty;

                // Increase or decrease the difficulty according to the number of solved blocks in the redacted window
                ulong solved_blocks = Node.blockChain.getSolvedBlocksCount(ConsensusConfig.getRedactedWindowSize(BlockVer.v0));
                ulong window_size = ConsensusConfig.getRedactedWindowSize(BlockVer.v0);

                // Special consideration for early blocks
                if (Node.blockChain.getLastBlockNum() < window_size)
                {
                    window_size = Node.blockChain.getLastBlockNum();
                }

                if (solved_blocks > window_size / 2)
                {
                    current_difficulty++;
                }
                else
                {
                    current_difficulty--;
                }

                // Set some limits
                if (current_difficulty > 256)
                    current_difficulty = 256;
                else if (current_difficulty < 14)
                    current_difficulty = 14;

            }

            return current_difficulty;
        }

        // Calculate the current mining difficulty
        public static ulong calculateDifficulty_v1()
        {
            ulong current_difficulty = 0xA2CB1211629F6141; // starting difficulty (requires approx 180 Khashes to find a solution)
            if (Node.blockChain.getLastBlockNum() > 1)
            {
                Block previous_block = Node.blockChain.getBlock(Node.blockChain.getLastBlockNum());
                if (previous_block != null)
                    current_difficulty = previous_block.difficulty;

                // Increase or decrease the difficulty according to the number of solved blocks in the redacted window
                ulong solved_blocks = Node.blockChain.getSolvedBlocksCount(ConsensusConfig.getRedactedWindowSize(BlockVer.v1));
                ulong window_size = ConsensusConfig.getRedactedWindowSize(BlockVer.v1);

                // Special consideration for early blocks
                if (Node.blockChain.getLastBlockNum() < window_size)
                {
                    window_size = Node.blockChain.getLastBlockNum();
                }
                // 
                BigInteger target_hashes_per_block = MiningUtils.getTargetHashcountPerBlock(current_difficulty);
                BigInteger actual_hashes_per_block = target_hashes_per_block * solved_blocks / (window_size / 2);
                ulong target_difficulty = 0;
                if (actual_hashes_per_block != 0)
                {
                    // find an appropriate difficulty for actual hashes:
                    target_difficulty = MiningUtils.calculateTargetDifficulty(actual_hashes_per_block);
                }
                // we jump hafway to the target difficulty each time
                ulong next_difficulty = 0;
                if (target_difficulty > current_difficulty)
                {
                    next_difficulty = current_difficulty + (target_difficulty - current_difficulty) / 2;
                }
                else if (target_difficulty < current_difficulty)
                {
                    next_difficulty = current_difficulty - (current_difficulty - target_difficulty) / 2;
                }
                else
                {
                    //difficulties are equal
                    next_difficulty = current_difficulty;
                }
                // TODO: maybe pretty-fy the hashrate (ie: 15 MH/s, rather than 15000000 H/s) also could prettify the difficulty number
                Logging.info(String.Format("Estimated network hash rate is {0} H/s (previous was: {1} H/s). Difficulty adjusts from {2} -> {3}.",
                    (actual_hashes_per_block / 60).ToString(),
                    (target_hashes_per_block / 60).ToString(),
                    current_difficulty, next_difficulty));
                current_difficulty = next_difficulty;
            }

            return current_difficulty;
        }

        public static ulong calculateDifficulty_v2()
        {
            ulong current_difficulty = 0xA2CB1211629F6141; // starting difficulty (requires approx 180 Khashes to find a solution)
            if (Node.blockChain.getLastBlockNum() > 1)
            {
                Block previous_block = Node.blockChain.getBlock(Node.blockChain.getLastBlockNum());
                if (previous_block != null)
                    current_difficulty = previous_block.difficulty;

                // Increase or decrease the difficulty according to the number of solved blocks in the redacted window
                ulong solved_blocks = Node.blockChain.getSolvedBlocksCount(ConsensusConfig.getRedactedWindowSize(BlockVer.v2));
                ulong window_size = ConsensusConfig.getRedactedWindowSize(BlockVer.v2);

                // Special consideration for early blocks
                if (Node.blockChain.getLastBlockNum() < window_size)
                {
                    window_size = Node.blockChain.getLastBlockNum();
                }
                // 
                BigInteger target_hashes_per_block = MiningUtils.getTargetHashcountPerBlock(current_difficulty);
                BigInteger actual_hashes_per_block = target_hashes_per_block * solved_blocks / (window_size / 2);
                ulong target_difficulty = 0;
                if (actual_hashes_per_block != 0)
                {
                    // find an appropriate difficulty for actual hashes:
                    target_difficulty = MiningUtils.calculateTargetDifficulty(actual_hashes_per_block);
                }
                else
                {
                    // set our minimum difficulty
                    target_difficulty = 0xA2CB1211629F6141;
                }
                // we amortize the change by 32th of the redacted window
                // The reason behind this is:
                //   Whenever difficulty changes, old blocks in the redacted window retain their assigned difficulty from when they were accepted into the chain.
                //   Therefore, it is possible there are still window_size-1 *easier* blocks in the redacted window, ready to be solved. The new difficulty will only
                //   be valid for the currently-accepting-block.
                //   This means, that the number of solved blocks vs unsolved will keep rising for a while, even if we ramp up the difficulty significantly. This causes
                //   "spikes" and drops in the difficulty curve and we don't want that.
                ulong next_difficulty = 0;
                ulong amortization = window_size / 32;
                if (amortization == 0) amortization = 1;
                ulong delta = 0;
                if (target_difficulty > current_difficulty)
                {
                    delta = (target_difficulty - current_difficulty) / amortization;
                    next_difficulty = current_difficulty + delta;
                }
                else if (target_difficulty < current_difficulty)
                {
                    delta = (current_difficulty - target_difficulty) / amortization;
                    next_difficulty = current_difficulty - delta;
                }
                else
                {
                    //difficulties are equal
                    next_difficulty = current_difficulty;
                }
                // clamp to minimum
                if (next_difficulty < 0xA2CB1211629F6141)
                {
                    delta = 0;
                    next_difficulty = 0xA2CB1211629F6141;
                }
                // TODO: maybe pretty-fy the hashrate (ie: 15 MH/s, rather than 15000000 H/s) also could prettify the difficulty number
                Logging.info(String.Format("Estimated network hash rate is {0} H/s (previous was: {1} H/s). Difficulty adjusts from {2} -> {3}. (Delta: {4}{5})",
                    (actual_hashes_per_block / 60).ToString(),
                    (target_hashes_per_block / 60).ToString(),
                    current_difficulty, next_difficulty,
                    target_difficulty > current_difficulty ? "+" : "-", delta));
                current_difficulty = next_difficulty;
            }

            return current_difficulty;
        }

        private static BigInteger calculateEstimatedHashRate()
        {
            // to get the EHR, we'll take PoW solutions from last 10 block and calculate the total hashrate, in the event of ~45-55% of solved blocks, we should get a relatively accurate result
            ulong last_block_num = IxianHandler.getLastBlockHeight();
            BigInteger hash_rate = 0;
            uint i = 0;
            for (i = 0; i < 10; i++)
            {
                Block b = Node.blockChain.getBlock(last_block_num - i, false, true);
                List<Transaction> b_txs = TransactionPool.getFullBlockTransactions(b).FindAll(x => x.type == (int)Transaction.Type.PoWSolution); // TODO TODO optimize this to fetch only PoW transactions - fetch tx by type
                foreach (Transaction tx in b_txs)
                {
                    Block pow_b = Node.blockChain.getBlock(tx.powSolution.blockNum, false, false);
                    if (pow_b == null)
                    {
                        continue;
                    }
                    hash_rate += MiningUtils.getTargetHashcountPerBlock(pow_b.difficulty);
                }
            }
            hash_rate = hash_rate / (i / 2); // i / 2 since every second block has to be full
            if (hash_rate == 0)
            {
                hash_rate = 1000;
            }
            return hash_rate;
        }

        // returns number of different solved blocks via PoW in last block
        private static long countLastBlockPowSolutions()
        {
            Block b = Node.blockChain.getLastBlock();
            List<Transaction> b_txs = TransactionPool.getFullBlockTransactions(b).FindAll(x => x.type == (int)Transaction.Type.PoWSolution);
            Dictionary<ulong, ulong> solved_blocks = new Dictionary<ulong, ulong>();
            foreach (Transaction tx in b_txs)
            {
                ulong pow_block_num = tx.powSolution.blockNum;
                solved_blocks.AddOrReplace(pow_block_num, pow_block_num);
            }
            return solved_blocks.LongCount();
        }

        public static ulong calculateDifficulty_v3()
        {
            // TODO cache
            ulong min_difficulty = 0xA2CB1211629F6141; // starting/min difficulty (requires approx 180 Khashes to find a solution)
            ulong current_difficulty = min_difficulty;

            if(Node.blockChain.getLastBlockNum() <= 10)
            {
                return current_difficulty;
            }
            Block previous_block = Node.blockChain.getLastBlock();
            if (previous_block != null)
                current_difficulty = previous_block.difficulty;

            // Increase or decrease the difficulty according to the number of solved blocks in the redacted window
            ulong solved_blocks = Node.blockChain.getSolvedBlocksCount(ConsensusConfig.getRedactedWindowSize(BlockVer.v2));
            ulong window_size = ConsensusConfig.getRedactedWindowSize(BlockVer.v2);

            // Special consideration for early blocks
            if (Node.blockChain.getLastBlockNum() < window_size)
            {
                window_size = Node.blockChain.getLastBlockNum();
            }

            ulong next_difficulty = min_difficulty;
            BigInteger current_hashes_per_block = 0;
            BigInteger previous_hashes_per_block = MiningUtils.getTargetHashcountPerBlock(current_difficulty);

            // if there are more than 3/4 of solved blocks, max out the difficulty
            if (solved_blocks > window_size * 0.75f)
            {
                next_difficulty = ulong.MaxValue;
            }
            else if (solved_blocks < window_size * 0.25f)
            {
                // if there are less than 25% of solved blocks, set min difficulty
                next_difficulty = min_difficulty;
            }else
            {
                if (solved_blocks < window_size * 0.48f)
                {
                    // if there are between 25% and 48% of solved blocks, ideally use estimated hashrate * 0.7 for difficulty
                    current_hashes_per_block = calculateEstimatedHashRate() * 7 / 10; // * 0.7f
                    next_difficulty = MiningUtils.calculateTargetDifficulty(current_hashes_per_block);
                }
                else if (solved_blocks < window_size * 0.53f)
                {
                    // if there are between 48% and 53% of solved blocks, ideally use estimated hashrate * 1.5 for difficulty
                    current_hashes_per_block = calculateEstimatedHashRate() * 15 / 10; // * 1.5f
                    next_difficulty = MiningUtils.calculateTargetDifficulty(current_hashes_per_block);
                }
                else
                {
                    // otherwise there's between 53% and 75% solved blocks, use estimated hashrate * (10 + (n / 10)) for difficulty, where n is number of blocks solved over 50%
                    // to get estimated hashrate, use previous block's hashrate
                    long n = (long)solved_blocks - (long)(window_size * 0.50f);
                    long solutions_in_previous_block = countLastBlockPowSolutions();
                    long previous_n = 0;
                    if(window_size < ConsensusConfig.getRedactedWindowSize())
                    {
                        previous_n = (long)solved_blocks - solutions_in_previous_block - (long)((window_size - 1) * 0.50f);
                    }else
                    {
                        previous_n = (long)solved_blocks - solutions_in_previous_block - (long)(window_size * 0.50f);
                    }
                    BigInteger estimated_hash_rate = previous_hashes_per_block / (10 + (previous_n / 10));
                    next_difficulty = MiningUtils.calculateTargetDifficulty(estimated_hash_rate * (10 + (n / 10)));
                }

            }
            
            // clamp to minimum
            if (next_difficulty < min_difficulty)
            {
                next_difficulty = min_difficulty;
            }

            // TODO: maybe pretty-fy the hashrate (ie: 15 MH/s, rather than 15000000 H/s) also could prettify the difficulty number
            Logging.info(String.Format("Estimated network hash rate is {0} H/s (previous was: {1} H/s). Difficulty adjusts from {2} -> {3}. (Delta: {4})",
                (current_hashes_per_block / 60).ToString(),
                (previous_hashes_per_block / 60).ToString(),
                current_difficulty, next_difficulty,
                current_difficulty - next_difficulty));
            current_difficulty = next_difficulty;

            return current_difficulty;
        }

        public static ulong calculateDifficulty_v4()
        {
            // TODO cache
            ulong minDifficulty = 0xA2CB1211629F6141; // starting/min difficulty (requires approx 180 Khashes to find a solution)
            ulong currentDifficulty = minDifficulty;

            if (Node.blockChain.getLastBlockNum() <= 10)
            {
                return currentDifficulty;
            }
            Block previousBlock = Node.blockChain.getLastBlock();
            if (previousBlock != null)
                currentDifficulty = previousBlock.difficulty;

            // Increase or decrease the difficulty according to the number of solved blocks in the redacted window
            ulong solvedBlocks = Node.blockChain.getSolvedBlocksCount(ConsensusConfig.getRedactedWindowSize(BlockVer.v10));
            ulong windowSize = ConsensusConfig.getRedactedWindowSize(BlockVer.v10);

            // Special consideration for early blocks
            if (Node.blockChain.getLastBlockNum() < windowSize)
            {
                windowSize = Node.blockChain.getLastBlockNum();
            }

            ulong solvedBlocksRatio = (solvedBlocks * 1000) / windowSize; // * 1000 for 0.1 precision

            ulong nextDifficulty = currentDifficulty;
            BigInteger currentHashesPerBlock = MiningUtils.getTargetHashcountPerBlock(currentDifficulty);
            BigInteger estimatedHashesPerBlock = 0;

            if (solvedBlocksRatio < 300)
            {
                // if there are less than 30% of solved blocks, set min difficulty
                nextDifficulty = minDifficulty;
            }else if (solvedBlocksRatio > 700)
            {
                // if there are less than 70% of solved blocks, set max difficulty
                nextDifficulty = ulong.MaxValue;
            }
            else
            {
                estimatedHashesPerBlock = calculateEstimatedHashRate();
                if (solvedBlocksRatio < 480)
                {
                    // if there are between 30% and 48% of solved blocks, estimated hashrate * solved blocks ratio
                    estimatedHashesPerBlock = estimatedHashesPerBlock * solvedBlocksRatio / 1000;
                }
                else if (solvedBlocksRatio < 510)
                {
                    // if there are between 48% and 51% of solved blocks, estimated hashrate
                    //current_hashes_per_block = current_hashes_per_block;
                }
                else
                {
                    // otherwise there's between 51% and 70% solved blocks, estimated hashrate * (1 + solved blocks ratio)
                    estimatedHashesPerBlock = estimatedHashesPerBlock * (1000 + solvedBlocksRatio) / 1000;
                }

                if(currentDifficulty != minDifficulty && currentDifficulty != ulong.MaxValue)
                {
                    if (estimatedHashesPerBlock > currentHashesPerBlock * 4)
                    {
                        estimatedHashesPerBlock = currentHashesPerBlock * 4;
                    }
                    else if (estimatedHashesPerBlock < currentHashesPerBlock / 4)
                    {
                        estimatedHashesPerBlock = currentHashesPerBlock / 4;
                    }
                }

                nextDifficulty = MiningUtils.calculateTargetDifficulty(estimatedHashesPerBlock);
            }

            // clamp to minimum
            if (nextDifficulty < minDifficulty)
            {
                nextDifficulty = minDifficulty;
            }

            // TODO: maybe pretty-fy the hashrate (ie: 15 MH/s, rather than 15000000 H/s) also could prettify the difficulty number
            Logging.info("Estimated network hash rate is {0} H/s (previous was: {1} H/s). Difficulty adjusts from {2} -> {3}. (Delta: {4})",
                (estimatedHashesPerBlock / 60).ToString(),
                (currentHashesPerBlock / 60).ToString(),
                currentDifficulty, nextDifficulty,
                currentDifficulty - nextDifficulty);

            return nextDifficulty;
        }


        // Retrieve the signature freeze of the 5th last block
        public byte[] getSignatureFreeze(Block freezing_block, int block_ver)
        {
            Block target_block = Node.blockChain.getBlock(freezing_block.blockNum - 5);
            if (target_block == null)
            {
                BlockProtocolMessages.broadcastGetBlock(target_block.blockNum);
                return null;
            }

            if (block_ver >= BlockVer.v5 && freezing_block.blockNum > 11)
            {
                bool froze_signatures = freezeSignatures(target_block);
                if(!froze_signatures 
                    || (target_block.getFrozenSignatureCount() < Node.blockChain.getRequiredConsensus(target_block.blockNum) && !isBlockchainRecoveryMode(target_block.blockNum, target_block.timestamp, target_block.getFrozenSignatureCount())))
                {
                    BlockProtocolMessages.broadcastGetBlock(target_block.blockNum);
                    if(froze_signatures)
                        Logging.warn("Freezing the target block #{0} yields less than required signatures {1} < {2}", target_block.blockNum, target_block.getFrozenSignatureCount(), Node.blockChain.getRequiredConsensus(target_block.blockNum));
                    Node.blockChain.setFrozenSignatures(target_block, null);
                    throw new Exception("Freezing the target block yields less than required signatures");
                }
            }
            return target_block.calculateSignatureChecksum();
        }

        // Generate all the staking transactions for this block
        public List<Transaction> generateStakingTransactions(ulong targetBlockNum, int block_version, long block_timestamp = 0)
        {
            List<Transaction> transactions = new List<Transaction>();

            // Prevent distribution if we don't have enough fully generated blocks yet
            if (block_version >= BlockVer.v10)
            {
                if (targetBlockNum < 2)
                {
                    return transactions;
                }
                if (Node.blockChain.getBlock(targetBlockNum - 1).version < BlockVer.v10)
                {
                    return transactions;
                }
            }
            else
            {
                if (Node.blockChain.getLastBlockNum() < 10)
                {
                    return transactions;
                }
            }


            Block targetBlock = Node.blockChain.getBlock(targetBlockNum);
            if (targetBlock == null)
            {
                return null;
            }

            IxiNumber totalIxis = Node.walletState.calculateTotalSupply();
            if (Config.fullBlockLogging) { Logging.info("Applying block #{0} -> generateStakingTransactions (total supply = {1})", targetBlockNum, totalIxis.getAmount()); }

            //Logging.info(String.Format("totalIxis = {0}", totalIxis.ToString()));
            IxiNumber newIxis = ConsensusConfig.calculateSigningRewardForBlock(targetBlockNum, totalIxis);
            if(block_version >= BlockVer.v10)
            {
                if (targetBlock.totalFee == 0)
                {
                    targetBlock.totalFee = calculateTotalTransactionFeeReward(targetBlock);
                    if (targetBlock.totalFee != 0)
                    {
                        Logging.warn("Total fee amount for block #{0} while generating staking transactions was 0, recalculated.", targetBlock.blockNum);
                        Node.blockChain.updateBlock(targetBlock);
                    }
                }
                newIxis += targetBlock.totalFee;
            }

            //Console.ForegroundColor = ConsoleColor.Magenta;
            //Console.WriteLine("----STAKING REWARDS for #{0} TOTAL {1} IXIs----", targetBlock.blockNum, newIxis.ToString());

            // Retrieve the list of signature wallets
            var signatureWallets = targetBlock.getSignaturesWalletAddressesWithDifficulty();

            IxiNumber totalIxisStaked = new IxiNumber(0);
            Address[] stakeWallets = new Address[signatureWallets.Count];
            BigInteger[] stakes = new BigInteger[signatureWallets.Count];
            BigInteger[] awards = new BigInteger[signatureWallets.Count];
            BigInteger[] awardRemainders = new BigInteger[signatureWallets.Count];
            // First pass, go through each wallet to find its balance
            int stakers = 0;
            foreach (var wallet_addr_diff in signatureWallets)
            {
                Address wallet_addr = wallet_addr_diff.address;
                IxiNumber difficulty = wallet_addr_diff.difficulty;
                if(block_version >= BlockVer.v10)
                {
                    totalIxisStaked += difficulty;
                    //Logging.info(String.Format("wallet {0} stakes {1} IXI", Base58Check.Base58CheckEncoding.EncodePlain(wallet_addr), wallet.balance.ToString()));
                    stakes[stakers] = difficulty.getAmount();
                    stakeWallets[stakers] = wallet_addr;
                    stakers += 1;
                }
                else if(block_version < BlockVer.v5)
                {
                    Wallet wallet = Node.walletState.getWallet(wallet_addr);
                    if (wallet.balance.getAmount() > 0)
                    {
                        totalIxisStaked += wallet.balance;
                        //Logging.info(String.Format("wallet {0} stakes {1} IXI", Base58Check.Base58CheckEncoding.EncodePlain(wallet_addr), wallet.balance.ToString()));
                        stakes[stakers] = wallet.balance.getAmount();
                        stakeWallets[stakers] = wallet_addr;
                        stakers += 1;
                    }
                }
                else
                {
                    totalIxisStaked += 1;
                    //Logging.info(String.Format("wallet {0} stakes {1} IXI", Base58Check.Base58CheckEncoding.EncodePlain(wallet_addr), wallet.balance.ToString()));
                    stakes[stakers] = (new IxiNumber(1)).getAmount();
                    stakeWallets[stakers] = wallet_addr;
                    stakers += 1;
                }
            }
            //Logging.info(String.Format("Stakers: {0}, totalIxisStaked = {1}", stakers, totalIxisStaked.ToString()));

            if (totalIxisStaked.getAmount() <= 0)
            {
                Logging.warn("No IXI were staked or a logic error occurred - total IXI staked returned: {0}", totalIxisStaked.getAmount());
                return null;
            }

            // Second pass, determine awards by stake
            //Logging.info("Determining awards");

            BigInteger totalAwarded = 0;
            for (int i = 0; i < stakers; i++)
            {
                BigInteger p = (newIxis.getAmount() * stakes[i] * 100) / totalIxisStaked.getAmount();
                //Logging.info(String.Format("staker[{0}]: p = {1}", i, p.ToString()));
                awardRemainders[i] = p % 100;
                //Logging.info(String.Format("staker[{0}]: awardRemainder = {1}", i, awardRemainders[i].ToString()));
                p = p / 100;
                awards[i] = p;
                //Logging.info(String.Format("staker[{0}]: award = {1}", i, awards[i].ToString()));
                totalAwarded += p;
            }
            //Logging.info(String.Format("totalAwarded = {0}", totalAwarded.ToString()));

            // Third pass, distribute remainders, if any
            // This essentially "rounds up" the awards for the stakers closest to the next whole amount,
            // until we bring the award difference down to zero.
            //Logging.info("Determining remainders");
            BigInteger diffAward = newIxis.getAmount() - totalAwarded;
            //Logging.info(String.Format("diffAward = {0}", diffAward.ToString()));
            if (diffAward > 0)
            {
                int[] descRemaindersIndexes = awardRemainders
                    .Select((v, pos) => new KeyValuePair<BigInteger, int>(v, pos))
                    .OrderByDescending(x => x.Key)
                    .Select(x => x.Value).ToArray();
                int currRemainderAward = 0;
                while (diffAward > 0)
                {
                    awards[descRemaindersIndexes[currRemainderAward]] += 1;
                    //Logging.info(String.Format("Increasing reward {0} by 1, to: {1}", descRemaindersIndexes[currRemainderAward], awards[descRemaindersIndexes[currRemainderAward]].ToString()));
                    currRemainderAward += 1;
                    diffAward -= 1;
                }
            }

            if (block_version < 2)
            {
                for (int i = 0; i < stakers; i++)
                {
                    IxiNumber award = new IxiNumber(awards[i]);
                    //Logging.info(String.Format("Final reward for staker {0}: {1}", i, award.ToString()));
                    if (award > (long)0)
                    {
                        Address wallet_addr = stakeWallets[i];
                        //Console.WriteLine("----> Awarding {0} to {1}", award, wallet_addr);

                        Transaction tx = new Transaction((int)Transaction.Type.StakingReward, award, new IxiNumber(0), wallet_addr, ConsensusConfig.ixianInfiniMineAddress, BitConverter.GetBytes(targetBlock.blockNum), null, Node.blockChain.getLastBlockNum(), 0, block_timestamp);

                        transactions.Add(tx);

                    }

                }
            }else
            {
                IDictionary<Address, Transaction.ToEntry> to_list;
                if (block_version >= BlockVer.v10)
                {
                    to_list = new Dictionary<Address, Transaction.ToEntry>(new AddressComparer());
                }
                else
                {
                    to_list = new SortedDictionary<Address, Transaction.ToEntry>(new AddressComparer());
                }
                for (int i = 0; i < stakers; i++)
                {
                    IxiNumber award = new IxiNumber(awards[i]);
                    //Logging.info(String.Format("Final reward for staker {0}: {1}", i, award.ToString()));
                    if (award > (long)0)
                    {
                        Address wallet_addr = stakeWallets[i];
                        //Console.WriteLine("----> Awarding {0} to {1}", award, wallet_addr);
                        to_list.Add(wallet_addr, new Transaction.ToEntry(Transaction.getExpectedVersion(block_version), award));

                    }

                }
                if(to_list.Count > 0)
                {
                    byte[] data = null;
                    if (block_version >= BlockVer.v10)
                    {
                        data = targetBlock.blockNum.GetIxiVarIntBytes();
                    }
                    else
                    {
                        data = BitConverter.GetBytes(targetBlock.blockNum);
                    }

                    to_list.First().Value.data = data;
                }
                Transaction tx = new Transaction((int)Transaction.Type.StakingReward, new IxiNumber(0), to_list, ConsensusConfig.ixianInfiniMineAddress, null, Node.blockChain.getLastBlockNum(), 0, block_timestamp);

                transactions.Add(tx);
            }
            //Console.WriteLine("------");
            //Console.ResetColor();


            return transactions;
        }


        // Distribute the staking rewards according to the 5th last block signatures
        public bool distributeStakingRewards(Block b)
        {
            int blockVersion = b.version;
            ulong stakingRewardBlockNum;
            if (blockVersion >= BlockVer.v10)
            {
                // Prevent distribution if we don't have enough fully generated blocks yet
                if (b.blockNum <= ConsensusConfig.rewardMaturity + 1)
                {
                    return false;
                }
                stakingRewardBlockNum = b.blockNum - ConsensusConfig.rewardMaturity;
                if (Node.blockChain.getBlock(stakingRewardBlockNum - 1).version < BlockVer.v10)
                {
                    return false;
                }
            }
            else
            {
                // Prevent distribution if we don't have 10 fully generated blocks yet
                if (Node.blockChain.getLastBlockNum() < 10)
                {
                    return false;
                }
                stakingRewardBlockNum = b.blockNum - 6;
            }

            if (!Node.walletState.inTransaction)
            {
                if (Config.fullBlockLogging) { Logging.info("Applying block #{0} -> distributingStakingRewards (transaction = {1})", b.blockNum, Node.walletState.inTransaction); }
                List<Transaction> transactions = generateStakingTransactions(stakingRewardBlockNum, blockVersion, b.timestamp);
                if (Config.fullBlockLogging) { Logging.info("Applying block #{0} -> distributingStakingRewards: generated {1} staking transactions:", b.blockNum, transactions.Count); }
                foreach (Transaction transaction in transactions)
                {
                    if (Config.fullBlockLogging) { Logging.info("Applying block #{0} -> Staking transaction {{ {1} }} -> {2} IxiCash to {3} recipients", b.blockNum, transaction.getTxIdString(), transaction.amount.getAmount(), transaction.toList.Count); }
                    TransactionPool.addTransaction(transaction, true);
                }
            }
            if (Config.fullBlockLogging) { Logging.info("Applying block #{0} -> distributingStakingRewards (done)", b.blockNum); }
            return true;
        }

        public bool hasNewBlock()
        {
            return localNewBlock != null;
        }

        public Block getLocalBlock()
        {
            return localNewBlock;
        }

        public bool addSignatureToBlock(BlockSignature blockSig, RemoteEndpoint endpoint)
        {
            ulong last_block_num = Node.blockChain.getLastBlockNum();
            ulong block_num = blockSig.blockNum;
            byte[] checksum = blockSig.blockHash;
            if (block_num > last_block_num - 5 && block_num <= last_block_num)
            {
                Block b = Node.blockChain.getBlock(block_num, false, false);
                if (b != null && b.blockChecksum.SequenceEqual(checksum))
                {
                    return b.addSignature(blockSig);
                }else
                {
                    BlockProtocolMessages.broadcastGetBlock(block_num, null, endpoint);
                }
            }
            else if (block_num == last_block_num + 1)
            {
                lock (Node.blockProcessor.localBlockLock)
                {
                    Block b = Node.blockProcessor.getLocalBlock();
                    if (b != null && b.blockChecksum.SequenceEqual(checksum))
                    {
                        bool sig_added = b.addSignature(blockSig);
                        if (sig_added)
                        {
                            currentBlockStartTime = DateTime.UtcNow;
                            lastBlockStartTime = DateTime.UtcNow.AddSeconds(-blockGenerationInterval * 10);
                        }
                        return sig_added;
                    }
                    else
                    {
                        BlockProtocolMessages.broadcastGetBlock(block_num, null, endpoint);
                    }
                }
            }
            return false;
        }


        // Updates the walletstate public keys. Called from BlockProcessor applyAcceptedBlock()
        public bool updateWalletStatePublicKeys(ulong blockNum)
        {
            Block targetBlock = Node.blockChain.getBlock(blockNum - 6, false);
            if (targetBlock == null)
            {
                return false;
            }

            List<BlockSignature> sigs = null;
            if (targetBlock.frozenSignatures != null)
            {
                sigs = targetBlock.frozenSignatures;
            }
            else
            {
                sigs = targetBlock.signatures;
            }

            List<BlockSignature> sigsToRemove = new List<BlockSignature>();
            foreach (BlockSignature sig in sigs)
            {
                byte[] signerPubKey = sig.recipientPubKeyOrAddress.pubKey;
                if (signerPubKey == null)
                {
                    Address signerAddress = sig.recipientPubKeyOrAddress;
                    Wallet signerWallet = Node.walletState.getWallet(signerAddress);
                    if (signerWallet.publicKey == null)
                    {
                        Logging.error("Signer wallet's pubKey entry is null, expecting a non-null entry for address: " + sig.ToString());
                        sigsToRemove.Add(sig);
                        continue;
                    }
                }
                else
                {
                    Wallet signerWallet = Node.walletState.getWallet(sig.recipientPubKeyOrAddress);
                    if (signerWallet.publicKey == null)
                    {
                        // Set the WS public key
                        Node.walletState.setWalletPublicKey(sig.recipientPubKeyOrAddress, signerPubKey);
                    }
                }
            }

            foreach (BlockSignature sig in sigsToRemove)
            {
                targetBlock.signatures.Remove(sig);
            }

            if (sigsToRemove.Count > 0)
            {
                return false;
            }

            return true;
        }

        public void resetSuperBlockCache()
        {
            lock (superBlockLock)
            {
                cache_currentSuperBlockSegments = null;
                cache_lastSuperBlockNum = 0;
                cache_lastSuperBlockChecksum = null;
                pendingSuperBlocks.Clear();
            }
        }

        /// <summary>
        ///  Determines highest network block height depending on 2/3rd of connected servers block heights.
        /// </summary>
        public ulong determineHighestNetworkBlockNum()
        {
            List<ulong> blockHeights = NetworkClientManager.getBlockHeights();
            blockHeights.AddRange(NetworkServer.getBlockHeights());

            if (blockHeights.Count() < 1)
            {
                return 0;
            }

            blockHeights.Sort();

            int thirdCount = (int)Math.Floor((decimal)blockHeights.Count / 3);

            var blockHeightsMajority = blockHeights;

            if (thirdCount >= 1 && blockHeights.Count > thirdCount)
            {
                blockHeightsMajority = blockHeights.Skip(thirdCount).Take(thirdCount).ToList();
            }

            ulong netBh = blockHeightsMajority.Max();

            if (Node.blockChain == null)
            {
                return netBh;
            }

            Block lastBlock = Node.blockChain.getLastBlock();
            if (lastBlock == null)
            {
                return netBh;
            }

            ulong maxBlocksGenerated = (ulong)(Clock.getNetworkTimestamp() - lastBlock.timestamp) / (ulong)ConsensusConfig.minBlockTimeDifference;
            ulong maxBlockHeight = lastBlock.blockNum + maxBlocksGenerated;
            if (maxBlockHeight < netBh)
            {
                return maxBlockHeight;
            }
            return netBh;
        }
    }
}
