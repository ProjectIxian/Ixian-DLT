// Copyright (C) 2017-2020 Ixian OU
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

using DLT;
using DLT.Meta;
using DLTNode.Meta;
using IXICore;
using IXICore.Meta;
using IXICore.Network;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;

namespace DLTNode
{
    class APIServer : GenericAPIServer
    {
        public APIServer(List<string> listen_URLs, Dictionary<string, string> authorized_users = null, List<string> allowed_IPs = null)
        {
            // Start the API server
            start(listen_URLs, authorized_users, allowed_IPs);
        }

        protected override bool processRequest(HttpListenerContext context, string methodName, Dictionary<string, object> parameters)
        {
            JsonResponse response = null;

            if (methodName.Equals("getbalance", StringComparison.OrdinalIgnoreCase))
            {
                response = onGetBalance(parameters);
            }

            if (methodName.Equals("getblock", StringComparison.OrdinalIgnoreCase))
            {
                response = onGetBlock(parameters);
            }

            if (methodName.Equals("getlastblocks", StringComparison.OrdinalIgnoreCase))
            {
                response = onGetLastBlocks();
            }

            if (methodName.Equals("getfullblock", StringComparison.OrdinalIgnoreCase))
            {
                response = onGetFullBlock(parameters);
            }

            if (methodName.Equals("gettransaction", StringComparison.OrdinalIgnoreCase))
            {
                response = onGetTransaction(parameters);
            }

            if (methodName.Equals("stress", StringComparison.OrdinalIgnoreCase))
            {
                response = onStress(parameters);
            }

            if (methodName.Equals("getwallet", StringComparison.OrdinalIgnoreCase))
            {
                response = onGetWallet(parameters);
            }

            if (methodName.Equals("walletlist", StringComparison.OrdinalIgnoreCase))
            {
                response = onWalletList();
            }

            if (methodName.Equals("pl", StringComparison.OrdinalIgnoreCase))
            {
                response = onPl();
            }

            if (methodName.Equals("tx", StringComparison.OrdinalIgnoreCase))
            {
                response = onTx(parameters);
            }

            if (methodName.Equals("txu", StringComparison.OrdinalIgnoreCase))
            {
                response = onTxu(parameters);
            }

            if (methodName.Equals("txa", StringComparison.OrdinalIgnoreCase))
            {
                response = onTxa(parameters);
            }

            if (methodName.Equals("status", StringComparison.OrdinalIgnoreCase))
            {
                response = onStatus(parameters);
            }

            if (methodName.Equals("minerstats", StringComparison.OrdinalIgnoreCase))
            {
                response = onMinerStats();
            }

            if (methodName.Equals("supply", StringComparison.OrdinalIgnoreCase))
            {
                response = onSupply();
            }

            if (methodName.Equals("countnodeversions", StringComparison.OrdinalIgnoreCase))
            {
                response = onCountNodeVersions();
            }

            if (methodName.Equals("setBlockSelectionAlgorithm", StringComparison.OrdinalIgnoreCase))
            {
                response = onSetBlockSelectionAlgorithm(parameters);
            }

            if (methodName.Equals("verifyminingsolution", StringComparison.OrdinalIgnoreCase))
            {
                response = onVerifyMiningSolution(parameters);
            }

            if (methodName.Equals("submitminingsolution", StringComparison.OrdinalIgnoreCase))
            {
                response = onSubmitMiningSolution(parameters);
            }

            if (methodName.Equals("getminingblock", StringComparison.OrdinalIgnoreCase))
            {
                response = onGetMiningBlock(parameters);
            }

            if (methodName.Equals("getblockcount", StringComparison.OrdinalIgnoreCase))
            {
                response = onGetBlockCount();
            }

            if (methodName.Equals("getbestblockhash", StringComparison.OrdinalIgnoreCase))
            {
                response = onGetBestBlockHash();
            }

            if (methodName.Equals("gettxoutsetinfo", StringComparison.OrdinalIgnoreCase))
            {
                response = onGetTxOutsetInfo();
            }

            if (methodName.Equals("rescanblockchain", StringComparison.OrdinalIgnoreCase))
            {
                response = onRescanBlockchain(parameters);
            }

            if (response == null)
            {
                return false;
            }

            // Set the content type to plain to prevent xml parsing errors in various browsers
            context.Response.ContentType = "application/json";

            sendResponse(context.Response, response);

            context.Response.Close();

            return true;
        }

        private Dictionary<string, string> blockToJsonDictionary(Block block)
        {
            Dictionary<string, string> blockData = new Dictionary<string, string>();

            blockData.Add("Block Number", block.blockNum.ToString());
            blockData.Add("Version", block.version.ToString());
            blockData.Add("Block Checksum", Crypto.hashToString(block.blockChecksum));
            blockData.Add("Last Block Checksum", Crypto.hashToString(block.lastBlockChecksum));
            blockData.Add("Wallet State Checksum", Crypto.hashToString(block.walletStateChecksum));
            blockData.Add("Sig freeze Checksum", Crypto.hashToString(block.signatureFreezeChecksum));
            blockData.Add("PoW field", Crypto.hashToString(block.powField));
            blockData.Add("Timestamp", block.timestamp.ToString());
            blockData.Add("Difficulty", block.difficulty.ToString());
            blockData.Add("Hashrate", (MiningUtils.getTargetHashcountPerBlock(block.difficulty) / 60).ToString());
            blockData.Add("Compacted Sigs", block.compactedSigs.ToString());
            blockData.Add("Signature count", block.signatures.Count.ToString());
            blockData.Add("Total Signer Difficulty", block.getTotalSignerDifficulty().ToString());
            blockData.Add("Transaction count", block.transactions.Count.ToString());
            blockData.Add("Transaction amount", TransactionPool.getTotalTransactionsValueInBlock(block).ToString());
            blockData.Add("Signatures", JsonConvert.SerializeObject(block.signatures));
            if(block.frozenSignatures != null)
            {
                blockData.Add("Frozen Signatures", JsonConvert.SerializeObject(block.frozenSignatures));
                blockData.Add("Frozen Signature count", block.frozenSignatures.Count.ToString());
            }
            blockData.Add("Sig Checksum", Crypto.hashToString(block.calculateSignatureChecksum()));
            blockData.Add("Signer Bits", block.signerBits.ToString());
            List<string> txids = new List<string>();
            foreach(byte[] tx in block.transactions)
            {
                txids.Add(Transaction.txIdV8ToLegacy(tx));
            }
            blockData.Add("TX IDs", JsonConvert.SerializeObject(txids));
            blockData.Add("Last Superblock", block.lastSuperBlockNum.ToString());
            blockData.Add("Last Superblock checksum", Crypto.hashToString(block.lastSuperBlockChecksum));

            return blockData;
        }

        public JsonResponse onGetBlock(Dictionary<string, object> parameters)
        {
            JsonError error = null;

            string blocknum_string = "0";
            if (parameters.ContainsKey("num"))
            {
                blocknum_string = (string)parameters["num"];
            }
            ulong block_num = 0;
            try
            {
                block_num = Convert.ToUInt64(blocknum_string);
            }
            catch (OverflowException)
            {
                block_num = 0;
            }

            Block block = Node.blockChain.getBlock(block_num, Config.storeFullHistory);
            if (block == null)
            {
                return new JsonResponse { result = null, error = new JsonError { code = (int)RPCErrorCode.RPC_INVALID_PARAMETER, message = "Block not found." } };
            }

            if(parameters.ContainsKey("bytes") && (string)parameters["bytes"] == "1")
            {
                return new JsonResponse { result = Crypto.hashToString(block.getBytes()), error = error };
            }
            else
            {
                return new JsonResponse { result = blockToJsonDictionary(block), error = error };
            }
        }

        public JsonResponse onGetLastBlocks()
        {
            JsonError error = null;

            Dictionary<string, string>[] blocks = new Dictionary<string, string>[10];
            long blockCnt = Node.blockChain.Count > 10 ? 10 : Node.blockChain.Count;
            ulong iStart = 0;
            lock(Node.blockProcessor.localBlockLock)
            {
                if(Node.blockProcessor.localNewBlock != null)
                {
                    blocks[0] = blockToJsonDictionary(Node.blockProcessor.localNewBlock);
                    iStart = 1;
                }
            }
            for (ulong i = iStart; i < (ulong)blockCnt; i++)
            {
                Block block = Node.blockChain.getBlock(Node.blockChain.getLastBlockNum() - (i - iStart));
                if (block == null)
                {
                    error = new JsonError { code = (int)RPCErrorCode.RPC_INTERNAL_ERROR, message = "An unknown error occurred, while getting one of the last 10 blocks." };
                    return new JsonResponse { result = null, error = error };
                }

                blocks[i] = blockToJsonDictionary(block);
            }

            return new JsonResponse { result = blocks, error = error };
        }

        public JsonResponse onGetFullBlock(Dictionary<string, object> parameters)
        {
            JsonError error = null;

            string blocknum_string = "0";
            if (parameters.ContainsKey("num"))
            {
                blocknum_string = (string)parameters["num"];
            }
            ulong block_num = 0;
            try
            {
                block_num = Convert.ToUInt64(blocknum_string);
            }
            catch (OverflowException)
            {
                block_num = 0;
            }
            Dictionary<string, string> blockData = null;
            Block block = Node.blockChain.getBlock(block_num, Config.storeFullHistory);
            if (block == null)
            {
                return new JsonResponse { result = null, error = new JsonError { code = (int)RPCErrorCode.RPC_INVALID_PARAMETER, message = "Block not found." } };
            }
            else
            {

                blockData = blockToJsonDictionary(block);

                blockData.Add("Transactions", JsonConvert.SerializeObject(TransactionPool.getFullBlockTransactionsAsArray(block)));
                blockData.Add("Superblock segments", JsonConvert.SerializeObject(block.superBlockSegments));
            }

            return new JsonResponse { result = blockData, error = error };
        }

        public JsonResponse onGetTransaction(Dictionary<string, object> parameters)
        {
            if (!parameters.ContainsKey("id"))
            {
                JsonError error = new JsonError { code = (int)RPCErrorCode.RPC_INVALID_PARAMETER, message = "Parameter 'id' is missing" };
                return new JsonResponse { result = null, error = error };
            }

            byte[] txid = Transaction.txIdLegacyToV8((string)parameters["id"]);
            Transaction t = TransactionPool.getUnappliedTransaction(txid);
            if(t == null)
            {
                t = TransactionPool.getAppliedTransaction(txid, 0, Config.storeFullHistory);
            }
            if (t == null)
            {
                return new JsonResponse { result = null, error = new JsonError { code = (int)RPCErrorCode.RPC_INVALID_PARAMETER, message = "Transaction not found." } };
            }

            return new JsonResponse { result = t.toDictionary(), error = null };
        }

        public JsonResponse onGetBalance(Dictionary<string, object> parameters)
        {
            if (!parameters.ContainsKey("address"))
            {
                JsonError error = new JsonError { code = (int)RPCErrorCode.RPC_INVALID_PARAMETER, message = "Parameter 'address' is missing" };
                return new JsonResponse { result = null, error = error };
            }

            byte[] address = Base58Check.Base58CheckEncoding.DecodePlain((string)parameters["address"]);

            IxiNumber balance = Node.walletState.getWalletBalance(new Address(address));

            return new JsonResponse { result = balance.ToString(), error = null };
        }

        public JsonResponse onStress(Dictionary<string, object> parameters)
        {
            JsonError error = null;

            string type_string = "txspam"; 
            if (parameters.ContainsKey("type"))
            {
                type_string = (string)parameters["type"];
            }

            int txnum = 100;
            if (parameters.ContainsKey("num"))
            {
                string txnumstr = (string)parameters["num"];
                try
                {
                    txnum = Convert.ToInt32(txnumstr);
                }
                catch (OverflowException)
                {
                    txnum = 0;
                }
            }

            string to = null;
            if (parameters.ContainsKey("to"))
            {
                to = (string)parameters["to"];
            }

            int threads = 1;
            if (parameters.ContainsKey("threads"))
            {
                threads = Convert.ToInt32((string)parameters["threads"]);
            }
            // Used for performing various tests.
            StressTest.start(type_string, txnum, to, threads);

            return new JsonResponse { result = "Stress test started", error = error };
        }

        public JsonResponse onGetWallet(Dictionary<string, object> parameters)
        {
            if (!parameters.ContainsKey("id"))
            {
                JsonError error = new JsonError { code = (int)RPCErrorCode.RPC_INVALID_PARAMETER, message = "Parameter 'id' is missing" };
                return new JsonResponse { result = null, error = error };
            }

            // Show own address, balance and blockchain synchronization status
            byte[] address = Base58Check.Base58CheckEncoding.DecodePlain((string)parameters["id"]);
            Wallet w = Node.walletState.getWallet(new Address(address));

            Dictionary<string, string> walletData = new Dictionary<string, string>();
            walletData.Add("id", w.id.ToString());
            walletData.Add("balance", w.balance.ToString());
            walletData.Add("type", w.type.ToString());
            walletData.Add("requiredSigs", w.requiredSigs.ToString());
            if (w.allowedSigners != null)
            {
                if (w.allowedSigners != null)
                {
                    walletData.Add("allowedSigners", "(" + (w.allowedSigners.Count + 1) + " keys): " +
                        w.allowedSigners.Aggregate(w.id.ToString(), (aggr, n) => aggr += "," + n.ToString(), aggr => aggr)
                        );
                }
                else
                {
                    walletData.Add("allowedSigners", "null");
                }
            }
            else
            {
                walletData.Add("allowedSigners", "null");
            }
            if (w.data != null)
            {
                walletData.Add("extraData", Crypto.hashToString(w.data));
            }
            else
            {
                walletData.Add("extraData", "null");
            }
            if (w.publicKey != null)
            {
                walletData.Add("publicKey", Crypto.hashToString(w.publicKey));
            }
            else
            {
                walletData.Add("publicKey", "null");
            }

            return new JsonResponse { result = walletData, error = null };
        }

        public JsonResponse onWalletList()
        {
            JsonError error = null;

            // Show a list of wallets - capped to 50
            Wallet[] wallets = Node.walletState.debugGetWallets();
            List<Dictionary<string, string>> walletStates = new List<Dictionary<string, string>>();
            foreach (Wallet w in wallets)
            {
                Dictionary<string, string> walletData = new Dictionary<string, string>();
                walletData.Add("id", w.id.ToString());
                walletData.Add("balance", w.balance.ToString());
                walletData.Add("type", w.type.ToString());
                walletData.Add("requiredSigs", w.requiredSigs.ToString());
                if (w.allowedSigners != null)
                {
                    if (w.allowedSigners != null)
                    {
                        walletData.Add("allowedSigners", "(" + (w.allowedSigners.Count + 1) + " keys): " +
                            w.allowedSigners.Aggregate(w.id.ToString(), (aggr, n) => aggr += "," + n.ToString(), aggr => aggr)
                            );
                    }
                    else
                    {
                        walletData.Add("allowedSigners", "null");
                    }
                }
                else
                {
                    walletData.Add("allowedSigners", "null");
                }
                if (w.data != null)
                {
                    walletData.Add("extraData", w.data.ToString());
                }
                else
                {
                    walletData.Add("extraData", "null");
                }
                if (w.publicKey != null)
                {
                    walletData.Add("publicKey", Crypto.hashToString(w.publicKey));
                }
                else
                {
                    walletData.Add("publicKey", "null");
                }
                walletStates.Add(walletData);
            }

            return new JsonResponse { result = walletStates, error = error };
        }

        public JsonResponse onPl()
        {
            JsonError error = null;

            List<Presence> presences = PresenceList.getPresences();
            // Show a list of presences
            lock (presences)
            {
                return new JsonResponse { result = presences, error = error };
            }

        }

        public JsonResponse onTx(Dictionary<string, object> parameters)
        {
            JsonError error = null;

            string fromIndex = "0";
            if (parameters.ContainsKey("fromIndex"))
            {
                fromIndex = (string)parameters["fromIndex"];
            }

            string count = "50";
            if (parameters.ContainsKey("count"))
            {
                count = (string)parameters["count"];
            }

            int int_count = Int32.Parse(count);
            int int_from_index = Int32.Parse(fromIndex);
            long full_tx_count = TransactionPool.getAppliedTransactionCount();
            List<Transaction> transactions = TransactionPool.getAppliedTransactions(int_from_index, int_count).ToList();
            int tx_count = transactions.Count;
            if (tx_count < int_count)
            {
                if(int_from_index >= full_tx_count)
                {
                    int_from_index = (int)((long)int_from_index - full_tx_count);
                }else
                {
                    int_from_index = 0;
                }
                transactions.AddRange(TransactionPool.getUnappliedTransactions(int_from_index, int_count - tx_count));
            }

            Dictionary<string, Dictionary<string, object>> tx_list = new Dictionary<string, Dictionary<string, object>>();

            foreach (Transaction t in transactions)
            {
                tx_list.Add(Transaction.txIdV8ToLegacy(t.id), t.toDictionary());
            }

            return new JsonResponse { result = tx_list, error = error };
        }

        public JsonResponse onTxu(Dictionary<string, object> parameters)
        {
            JsonError error = null;

            string fromIndex = "0";
            if (parameters.ContainsKey("fromIndex"))
            {
                fromIndex = (string)parameters["fromIndex"];
            }

            string count = "50";
            if (parameters.ContainsKey("count"))
            {
                count = (string)parameters["count"];
            }

            Transaction[] transactions = TransactionPool.getUnappliedTransactions(Int32.Parse(fromIndex), Int32.Parse(count)).ToArray();

            Dictionary<string, Dictionary<string, object>> tx_list = new Dictionary<string, Dictionary<string, object>>();

            foreach (Transaction t in transactions)
            {
                tx_list.Add(Transaction.txIdV8ToLegacy(t.id), t.toDictionary());
            }

            return new JsonResponse { result = tx_list, error = error };
        }

        public JsonResponse onTxa(Dictionary<string, object> parameters)
        {
            JsonError error = null;

            string fromIndex = "0";
            if (parameters.ContainsKey("fromIndex"))
            {
                fromIndex = (string)parameters["fromIndex"];
            }

            string count = "50";
            if (parameters.ContainsKey("count"))
            {
                count = (string)parameters["count"];
            }

            Transaction[] transactions = TransactionPool.getAppliedTransactions(Int32.Parse(fromIndex), Int32.Parse(count)).ToArray();

            Dictionary<string, Dictionary<string, object>> tx_list = new Dictionary<string, Dictionary<string, object>>();

            foreach (Transaction t in transactions)
            {
                tx_list.Add(Transaction.txIdV8ToLegacy(t.id), t.toDictionary());
            }

            return new JsonResponse { result = tx_list, error = error };
        }

        public JsonResponse onStatus(Dictionary<string, object> parameters)
        {
            JsonError error = null;

            Dictionary<string, object> networkArray = new Dictionary<string, object>();

            networkArray.Add("Core Version", CoreConfig.version);
            networkArray.Add("Node Version", CoreConfig.productVersion);
            networkArray.Add("Network type", IxianHandler.networkType.ToString());
            networkArray.Add("My time", Clock.getTimestamp());
            networkArray.Add("Network time difference", Clock.networkTimeDifference);
            networkArray.Add("Real network time difference", Clock.realNetworkTimeDifference);
            networkArray.Add("My External IP", IxianHandler.publicIP);
            networkArray.Add("My Listening Port", IxianHandler.publicPort);
            //networkArray.Add("Listening interface", context.Request.RemoteEndPoint.Address.ToString());
            networkArray.Add("Node Deprecation Block Limit", Config.nodeDeprecationBlock);

            string dltStatus = "Active";
            if (Node.blockSync.synchronizing)
                dltStatus = "Synchronizing";

            if (Node.blockChain.getTimeSinceLastBlock() > 1800) // if no block for over 1800 seconds
            {
                dltStatus = "ErrorLongTimeNoBlock";
            }

            if (Node.blockProcessor.networkUpgraded)
            {
                dltStatus = "ErrorForkedViaUpgrade";
            }

            networkArray.Add("Update", checkUpdate());

            networkArray.Add("DLT Status", dltStatus);

            networkArray.Add("Core Status", IxianHandler.status);

            string bpStatus = "Stopped";
            if (Node.blockProcessor.operating)
                bpStatus = "Running";
            networkArray.Add("Block Processor Status", bpStatus);

            Block last_block = Node.blockChain.getLastBlock();
            if(last_block != null)
            {
                networkArray.Add("Block Height", last_block.blockNum);
                networkArray.Add("Block Version", last_block.version);
                networkArray.Add("Block Signature Count", last_block.getFrozenSignatureCount());
                networkArray.Add("Block Total Signer Difficulty", last_block.getTotalSignerDifficulty());
            }
            else
            {
                networkArray.Add("Block Height", 0);
                networkArray.Add("Block Version", 0);
                networkArray.Add("Block Signature Count", 0);
                networkArray.Add("Block Total Signer Difficulty", 0);
            }

            networkArray.Add("Block generated seconds ago", Node.blockChain.getTimeSinceLastBlock());
            networkArray.Add("Network Block Height", IxianHandler.getHighestKnownNetworkBlockHeight());
            networkArray.Add("Node Type", PresenceList.myPresenceType);
            networkArray.Add("Connectable", NetworkServer.isConnectable());

            if (parameters.ContainsKey("vv") || parameters.ContainsKey("verbose"))
            {
                networkArray.Add("Required Consensus", Node.blockChain.getRequiredConsensus());
                networkArray.Add("Signer Difficulty", Node.blockChain.getRequiredSignerDifficulty(false));
                networkArray.Add("Signer Bits", Node.blockChain.getRequiredSignerBits());
                networkArray.Add("Signer Hashrate", Node.signerPowMiner.lastHashRate);
                networkArray.Add("Signer Last PoW Solution", Node.signerPowMiner.lastSignerPowSolution);

                networkArray.Add("Wallets", Node.walletState.numWallets);
                networkArray.Add("Presences", PresenceList.getTotalPresences());
                networkArray.Add("Supply", Node.walletState.calculateTotalSupply().ToString());
                networkArray.Add("Applied TX Count", TransactionPool.getAppliedTransactionCount());
                networkArray.Add("Unapplied TX Count", TransactionPool.getUnappliedTransactionCount());

                networkArray.Add("Masters", PresenceList.countPresences('M'));
                networkArray.Add("Relays", PresenceList.countPresences('R'));
                networkArray.Add("Clients", PresenceList.countPresences('C'));
            }

            if (parameters.ContainsKey("vv"))
            {
                Dictionary<string, object> queues = new Dictionary<string, object>();
                queues.Add("RcvLow", NetworkQueue.getLowPriorityMessageCount());
                queues.Add("RcvMedium", NetworkQueue.getMediumPriorityMessageCount());
                queues.Add("RcvHigh", NetworkQueue.getHighPriorityMessageCount());
                queues.Add("SendClients", NetworkServer.getQueuedMessageCount());
                queues.Add("SendServers", NetworkClientManager.getQueuedMessageCount());
                queues.Add("Logging", Logging.getRemainingStatementsCount());
                queues.Add("Pending Transactions", PendingTransactions.pendingTransactionCount());
                queues.Add("Storage", Node.storage.getQueuedQueryCount());
                queues.Add("Inventory", Node.inventoryCache.getItemCount());
                queues.Add("Inventory Processed", Node.inventoryCache.getProcessedItemCount());
                queues.Add("Activity", ActivityStorage.getQueuedQueryCount());

                networkArray.Add("Queues", queues);

                networkArray.Add("WS Checksum", Crypto.hashToString(Node.walletState.calculateWalletStateChecksum()));
            }

            networkArray.Add("Blockchain Scanning Active", ActivityScanner.isActive());
            networkArray.Add("Activity Scanner Last Block", ActivityScanner.getLastBlockNum());

            networkArray.Add("Network Clients", NetworkServer.getConnectedClients());
            networkArray.Add("Network Servers", NetworkClientManager.getConnectedClients(true));

            return new JsonResponse { result = networkArray, error = error };
        }

        public JsonResponse onMinerStats()
        {
            JsonError error = null;

            Dictionary<string, Object> minerArray = new Dictionary<string, Object>();

            // TODO TODO use Node.blockchain.getSolvedBlocksCount instead
            List<int> blocksCount = Node.miner.getBlocksCount();

            // Last hashrate
            minerArray.Add("Hashrate", Node.miner.lastHashRate);

            // Mining block search mode
            minerArray.Add("Search Mode", Node.miner.searchMode.ToString());

            // Current block
            minerArray.Add("Current Block", Node.miner.currentBlockNum);

            // Current block difficulty
            minerArray.Add("Current Difficulty", Node.miner.currentBlockDifficulty);

            // Show how many blocks calculated
            minerArray.Add("Solved Blocks (Local)", Node.miner.getSolvedBlocksCount());
            minerArray.Add("Solved Blocks (Network)", blocksCount[1]);

            // Number of empty blocks
            minerArray.Add("Empty Blocks", blocksCount[0]);

            // Last solved block number
            minerArray.Add("Last Solved Block", Node.miner.lastSolvedBlockNum);

            // Last block solved mins ago
            minerArray.Add("Last Solved Block Time", Node.miner.getLastSolvedBlockRelativeTime());

            return new JsonResponse { result = minerArray, error = error };
        }

        public JsonResponse onSupply()
        {
            JsonError error = null;

            string supply = Node.walletState.calculateTotalSupply().ToString();

            return new JsonResponse { result = supply, error = error };
        }

        private JsonResponse onCountNodeVersions()
        {
            Dictionary<string, int> versions = new Dictionary<string, int>();

            List<Presence> presences = PresenceList.getPresences();

            lock (presences)
            {
                foreach (var entry in presences)
                {
                    foreach (var pa_entry in entry.addresses)
                    {
                        if (!versions.ContainsKey(pa_entry.nodeVersion))
                        {
                            versions.Add(pa_entry.nodeVersion, 0);
                        }
                        versions[pa_entry.nodeVersion]++;
                    }
                }
            }

            return new JsonResponse { result = versions, error = null };
        }

        private JsonResponse onSetBlockSelectionAlgorithm(Dictionary<string, object> parameters)
        {
            if (!parameters.ContainsKey("algorithm"))
            {
                JsonError error = new JsonError { code = (int)RPCErrorCode.RPC_INVALID_PARAMETER, message = "Parameter 'algorithm' is missing" };
                return new JsonResponse { result = null, error = error };
            }

            int algo = int.Parse((string)parameters["algorithm"]);
            if(algo == -1)
            {
                Node.miner.pause = true;
            }else if (algo == (int)BlockSearchMode.lowestDifficulty)
            {
                Node.miner.pause = false;
                Node.miner.searchMode = BlockSearchMode.lowestDifficulty;
            }
            else if (algo == (int)BlockSearchMode.randomLowestDifficulty)
            {
                Node.miner.pause = false;
                Node.miner.searchMode = BlockSearchMode.randomLowestDifficulty;
            }
            else if (algo == (int)BlockSearchMode.latestBlock)
            {
                Node.miner.pause = false;
                Node.miner.searchMode = BlockSearchMode.latestBlock;
            }
            else if (algo == (int)BlockSearchMode.random)
            {
                Node.miner.pause = false;
                Node.miner.searchMode = BlockSearchMode.random;
            }else
            {
                return new JsonResponse { result = null, error = new JsonError() { code = (int)RPCErrorCode.RPC_INVALID_PARAMS, message = "Invalid algorithm was specified" } };
            }


            return new JsonResponse { result = "", error = null };
        }


        // Verifies a mining solution based on the block's difficulty
        // It does not submit it to the network.
        private JsonResponse onVerifyMiningSolution(Dictionary<string, object> parameters)
        {
            // Check that all the required query parameters are sent
            if (!parameters.ContainsKey("nonce"))
            {
                JsonError error = new JsonError { code = (int)RPCErrorCode.RPC_INVALID_PARAMETER, message = "Parameter 'nonce' is missing" };
                return new JsonResponse { result = null, error = error };
            }

            if (!parameters.ContainsKey("blocknum"))
            {
                JsonError error = new JsonError { code = (int)RPCErrorCode.RPC_INVALID_PARAMETER, message = "Parameter 'blocknum' is missing" };
                return new JsonResponse { result = null, error = error };
            }

            if (!parameters.ContainsKey("diff"))
            {
                JsonError error = new JsonError { code = (int)RPCErrorCode.RPC_INVALID_PARAMETER, message = "Parameter 'diff' is missing" };
                return new JsonResponse { result = null, error = error };
            }

            string nonce = (string)parameters["nonce"];
            if (nonce.Length < 1 || nonce.Length > 128)
            {
                Logging.info("Received incorrect verify nonce from miner.");
                return new JsonResponse { result = null, error = new JsonError() { code = (int)RPCErrorCode.RPC_INVALID_PARAMS, message = "Invalid nonce was specified" } };
            }

            ulong blocknum = ulong.Parse((string)parameters["blocknum"]);
            Block block = Node.blockChain.getBlock(blocknum, false, false);
            if (block == null)
            {
                Logging.info("Received incorrect verify block number from miner.");
                return new JsonResponse { result = null, error = new JsonError() { code = (int)RPCErrorCode.RPC_INVALID_PARAMS, message = "Invalid block number specified" } };
            }

            ulong blockdiff = ulong.Parse((string)parameters["diff"]);

            Address solver_address = IxianHandler.getWalletStorage().getPrimaryAddress();

            bool verify_result;
            if (block.version < BlockVer.v10)
            {
                verify_result = Miner.verifyNonce_v3(nonce, blocknum, solver_address.addressWithChecksum, blockdiff);
            }
            else 
            {
                verify_result = Miner.verifyNonce_v3(nonce, blocknum, solver_address.addressNoChecksum, blockdiff);
            }

            if (verify_result)
            {
                Logging.info("Received verify share: {0} #{1} - PASSED with diff {2}", nonce, blocknum, blockdiff);
            }
            else
            {
                Logging.info("Received verify share: {0} #{1} - REJECTED with diff {2}", nonce, blocknum, blockdiff);
            }

            return new JsonResponse { result = verify_result, error = null };
        }

        // Verifies and submits a mining solution to the network
        private JsonResponse onSubmitMiningSolution(Dictionary<string, object> parameters)
        {
            // Check that all the required query parameters are sent
            if (!parameters.ContainsKey("nonce"))
            {
                JsonError error = new JsonError { code = (int)RPCErrorCode.RPC_INVALID_PARAMETER, message = "Parameter 'nonce' is missing" };
                return new JsonResponse { result = null, error = error };
            }

            if (!parameters.ContainsKey("blocknum"))
            {
                JsonError error = new JsonError { code = (int)RPCErrorCode.RPC_INVALID_PARAMETER, message = "Parameter 'blocknum' is missing" };
                return new JsonResponse { result = null, error = error };
            }


            string nonce = (string)parameters["nonce"];
            if (nonce.Length < 1 || nonce.Length > 128)
            {
                Logging.info("Received incorrect nonce from miner.");
                return new JsonResponse { result = null, error = new JsonError() { code = (int)RPCErrorCode.RPC_INVALID_PARAMS, message = "Invalid nonce was specified" } };
            }

            ulong blocknum = ulong.Parse((string)parameters["blocknum"]);
            Block block = Node.blockChain.getBlock(blocknum, false, false);
            if (block == null)
            {
                Logging.info("Received incorrect block number from miner.");
                return new JsonResponse { result = null, error = new JsonError() { code = (int)RPCErrorCode.RPC_INVALID_PARAMS, message = "Invalid block number specified" } };
            }

            Logging.info("Received miner share: {0} #{1}", nonce, blocknum);

            Address solver_address = IxianHandler.getWalletStorage().getPrimaryAddress();
            bool verify_result;
            if (block.version < BlockVer.v10)
            {
                verify_result = Miner.verifyNonce_v3(nonce, blocknum, solver_address.addressWithChecksum, block.difficulty);
            }
            else
            {
                verify_result = Miner.verifyNonce_v3(nonce, blocknum, solver_address.addressNoChecksum, block.difficulty);
            }

            bool send_result = false;

            // Solution is valid, try to submit it to network
            if (verify_result == true)
            {
                if (Miner.sendSolution(Crypto.stringToHash(nonce), blocknum))
                {
                    Logging.info("Miner share {0} ACCEPTED.", nonce);
                    send_result = true;
                }
            }
            else
            {
                Logging.warn("Miner share {0} REJECTED.", nonce);
            }

            return new JsonResponse { result = send_result, error = null };
        }

        // Returns an empty PoW block based on the search algorithm provided as a parameter
        private JsonResponse onGetMiningBlock(Dictionary<string, object> parameters)
        {
            if (!parameters.ContainsKey("algo"))
            {
                JsonError error = new JsonError { code = (int)RPCErrorCode.RPC_INVALID_PARAMETER, message = "Parameter 'algo' is missing" };
                return new JsonResponse { result = null, error = error };
            }

            int algo = int.Parse((string)parameters["algo"]);
            BlockSearchMode searchMode = BlockSearchMode.randomLowestDifficulty;

            if (algo == (int)BlockSearchMode.lowestDifficulty)
            {
                searchMode = BlockSearchMode.lowestDifficulty;
            }
            else if (algo == (int)BlockSearchMode.randomLowestDifficulty)
            {
                searchMode = BlockSearchMode.randomLowestDifficulty;
            }
            else if (algo == (int)BlockSearchMode.latestBlock)
            {
                searchMode = BlockSearchMode.latestBlock;
            }
            else if (algo == (int)BlockSearchMode.random)
            {
                searchMode = BlockSearchMode.random;
            }
            else
            {
                return new JsonResponse { result = null, error = new JsonError() { code = (int)RPCErrorCode.RPC_INVALID_PARAMS, message = "Invalid algorithm was specified" } };
            }

            Block block = Miner.getMiningBlock(searchMode);
            if(block == null)
            {
                return new JsonResponse { result = null, error = new JsonError() { code = (int)RPCErrorCode.RPC_INTERNAL_ERROR, message = "Cannot retrieve mining block" } };
            }

            Address solver_address = IxianHandler.getWalletStorage().getPrimaryAddress();

            Dictionary<string, Object> resultArray = new Dictionary<string, Object>
            {
                { "num", block.blockNum }, // Block number
                { "ver", block.version }, // Block version
                { "dif", block.difficulty }, // Block difficulty
                { "chk", block.blockChecksum }, // Block checksum
                { "adr", solver_address.ToString() } // Solver address
            };

            return new JsonResponse { result = resultArray, error = null };
        }

        // returns highest local blockheight
        private JsonResponse onGetBlockCount()
        {
            return new JsonResponse { result = IxianHandler.getLastBlockHeight(), error = null };
        }

        // returns highest local blockheight
        private JsonResponse onGetBestBlockHash()
        {
            return new JsonResponse { result = Crypto.hashToString(IxianHandler.getLastBlock().blockChecksum), error = null };
        }

        // returns statistics about the unspent transactions
        private JsonResponse onGetTxOutsetInfo()
        {
            Block block = IxianHandler.getLastBlock();
            Transaction[] unapplied_txs = TransactionPool.getUnappliedTransactions();

            long txouts = 0;
            IxiNumber total_amount = new IxiNumber(0);

            foreach(Transaction tx in unapplied_txs)
            {
                txouts += tx.toList.Count();
                total_amount += tx.amount + tx.fee;
            }

            Dictionary<string, Object> result_array = new Dictionary<string, Object>
            {
                { "height", block.blockNum }, // Block height
                { "bestblock", Crypto.hashToString(block.blockChecksum) }, // Block checksum
                { "transactions", unapplied_txs.LongCount() }, // Number of transactions
                { "txouts", txouts }, // Number of transaction outputs
                { "total_amount", total_amount.ToString() } // Total amount
            };

            return new JsonResponse { result = result_array, error = null };
        }

        // Performs a rescan of the blockchain and fills the activity cache
        private JsonResponse onRescanBlockchain(Dictionary<string, object> parameters)
        {

            string fromIndex = "0";
            if (parameters.ContainsKey("from"))
            {
                fromIndex = (string)parameters["from"];
            }

            // Start the activity scanner
            if(!ActivityScanner.start(UInt64.Parse(fromIndex)))
            {
                JsonError error = new JsonError { code = (int)RPCErrorCode.RPC_MISC_ERROR, message = "Activity scanner is already running" };
                return new JsonResponse { result = null, error = error };
            }

            return new JsonResponse { result = "Started activity rescan.", error = null };
        }

        private string checkUpdate()
        {
            if (!UpdateVerify.ready && !UpdateVerify.error) return "";
            if (UpdateVerify.ready)
            {
                if (UpdateVerify.error) return "";
                if (UpdateVerify.serverVersion.CompareTo(Config.version) > 0) return UpdateVerify.serverVersion;
            }
            return "";
        }
    }
}