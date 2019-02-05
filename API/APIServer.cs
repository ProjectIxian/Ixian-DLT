using DLT;
using DLT.Meta;
using DLT.Network;
using DLTNode.API;
using IXICore;
using IXICore.Utils;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Text;

namespace DLTNode
{
    class JsonError
    {
        public int code = 0;
        public string message = null;
    }

    class JsonResponse
    {
        public object result = null;
        public JsonError error = null;
        public string id = null;
    }

    class APIServer : GenericAPIServer
    {

        private JsonRpc jsonRpc = new JsonRpc();

        public APIServer()
        {
            // Start the API server
            start(String.Format("http://localhost:{0}/", Config.apiPort), Config.apiUsers);
        }

        public void sendResponse(HttpListenerResponse responseObject, JsonResponse response)
        {
            string responseString = JsonConvert.SerializeObject(response);

            byte[] buffer = Encoding.UTF8.GetBytes(responseString);

            try
            {
                responseObject.ContentLength64 = buffer.Length;
                Stream output = responseObject.OutputStream;
                output.Write(buffer, 0, buffer.Length);
                output.Close();
            }
            catch (Exception e)
            {
                Logging.error(String.Format("APIServer: {0}", e));
            }
        }

        public void sendResponse(HttpListenerResponse responseObject, byte[] buffer)
        {
            try
            {
                responseObject.ContentLength64 = buffer.Length;
                Stream output = responseObject.OutputStream;
                output.Write(buffer, 0, buffer.Length);
                output.Close();
            }
            catch (Exception e)
            {
                Logging.error(String.Format("APIServer: {0}", e));
            }
        }
        protected override void onUpdate(HttpListenerContext context)
        {
            try
            {
                if (Config.verboseConsoleOutput)
                    Console.Write("*");

                if (context.Request.Url.Segments.Length < 2)
                {
                    // We will now show an embedded wallet if the API is called with no parameters
                    sendWallet(context);
                    return;
                }

                string methodName = context.Request.Url.Segments[1].Replace("/", "");

                if (methodName == null)
                {
                    JsonError error = new JsonError { code = (int)RPCErrorCode.RPC_INVALID_REQUEST, message = "Unknown action." };                    
                    sendResponse(context.Response, new JsonResponse { error = error });
                    return;
                }

                try
                {
                    parseRequest(context, methodName);
                }
                catch (Exception e)
                {
                    context.Response.ContentType = "application/json";
                    JsonError error = new JsonError { code = (int)RPCErrorCode.RPC_INTERNAL_ERROR, message = "Unknown error occured, see log for details." };
                    sendResponse(context.Response, new JsonResponse { error = error });
                    Logging.error(string.Format("Exception occured in API server while processing '{0}'. {1}", methodName, e));

                }
            }
            catch (Exception)
            {
                //continueRunning = false;
                //Logging.error(string.Format("Error in API server {0}", e.ToString()));
            }

        }

        // Send the embedded wallet html file
        private void sendWallet(HttpListenerContext context)
        {
            var assembly = Assembly.GetExecutingAssembly();
            var resourceName = "DLTNode.Embedded.wallet.html";

            // Fetch the wallet html file from the exe
            using (Stream stream = assembly.GetManifestResourceStream(resourceName))
            using (StreamReader reader = new StreamReader(stream))
            {
                // Replace the js API location
                string result = reader.ReadToEnd().Replace("#IXIAN#NODE#URL#", listenURL);
                // Set the content type to html to show the wallet page
                context.Response.ContentType = "text/html";
                sendResponse(context.Response, result);
            }
        }

        private void parseRequest(HttpListenerContext context, string methodName)
        {
            HttpListenerRequest request = context.Request;

            JsonResponse response = null;

            if (methodName.Equals("shutdown", StringComparison.OrdinalIgnoreCase))
            {
                response = onShutdown();
            }

            if (methodName.Equals("reconnect", StringComparison.OrdinalIgnoreCase))
            {
                response = onReconnect();
            }

            if (methodName.Equals("connect", StringComparison.OrdinalIgnoreCase))
            {
                response = onConnect(request);
            }

            if (methodName.Equals("isolate", StringComparison.OrdinalIgnoreCase))
            {
                response = onIsolate();
            }

            if (methodName.Equals("sync", StringComparison.OrdinalIgnoreCase))
            {
                response = onSync();
            }

            if (methodName.Equals("addtransaction", StringComparison.OrdinalIgnoreCase))
            {
                response = onAddTransaction(request);
            }

            if (methodName.Equals("addmultisigtransaction", StringComparison.OrdinalIgnoreCase))
            {
                response = onAddMultiSigTransaction(request);
            }

            if (methodName.Equals("addmultisigkey", StringComparison.OrdinalIgnoreCase))
            {
                response = onAddMultiSigKey(request);
            }

            if (methodName.Equals("delmultisigkey", StringComparison.OrdinalIgnoreCase))
            {
                response = onDelMultiSigKey(request);
            }

            if (methodName.Equals("changemultisigs", StringComparison.OrdinalIgnoreCase))
            {
                response = onChangeMultiSigs(request);
            }

            if (methodName.Equals("getbalance", StringComparison.OrdinalIgnoreCase))
            {
                response = onGetBalance(request);
            }

            if (methodName.Equals("getblock", StringComparison.OrdinalIgnoreCase))
            {
                response = onGetBlock(request);
            }

            if (methodName.Equals("getlastblocks", StringComparison.OrdinalIgnoreCase))
            {
                response = onGetLastBlocks();
            }

            if (methodName.Equals("getfullblock", StringComparison.OrdinalIgnoreCase))
            {
                response = onGetFullBlock(request);
            }

            if (methodName.Equals("gettransaction", StringComparison.OrdinalIgnoreCase))
            {
                response = onGetTransaction(request);
            }

            if (methodName.Equals("gettotalbalance", StringComparison.OrdinalIgnoreCase))
            {
                response = onGetTotalBalance();
            }

            if (methodName.Equals("stress", StringComparison.OrdinalIgnoreCase))
            {
                response = onStress(request);
            }

            if (methodName.Equals("mywallet", StringComparison.OrdinalIgnoreCase))
            {
                response = onMyWallet();
            }

            if (methodName.Equals("mypubkey", StringComparison.OrdinalIgnoreCase))
            {
                response = onMyPubKey();
            }

            if (methodName.Equals("getwallet", StringComparison.OrdinalIgnoreCase))
            {
                response = onGetWallet(request);
            }

            if (methodName.Equals("walletlist", StringComparison.OrdinalIgnoreCase))
            {
                response = onWalletList();
            }

            if (methodName.Equals("pl", StringComparison.OrdinalIgnoreCase))
            {
                response = onPl();
            }

            if (methodName.Equals("clients", StringComparison.OrdinalIgnoreCase))
            {
                response = onClients();
            }

            if (methodName.Equals("servers", StringComparison.OrdinalIgnoreCase))
            {
                response = onServers();
            }

            if (methodName.Equals("tx", StringComparison.OrdinalIgnoreCase))
            {
                response = onTx();
            }

            if (methodName.Equals("txu", StringComparison.OrdinalIgnoreCase))
            {
                response = onTxu();
            }

            if (methodName.Equals("status", StringComparison.OrdinalIgnoreCase))
            {
                response = onStatus();
            }

            if (methodName.Equals("minerstats", StringComparison.OrdinalIgnoreCase))
            {
                response = onMinerStats();
            }

            if (methodName.Equals("blockheight", StringComparison.OrdinalIgnoreCase))
            {
                response = onBlockHeight();
            }

            if (methodName.Equals("supply", StringComparison.OrdinalIgnoreCase))
            {
                response = onSupply();
            }

            if (methodName.Equals("debugsave", StringComparison.OrdinalIgnoreCase))
            {
                response = onDebugSave();
            }

            if (methodName.Equals("debugload", StringComparison.OrdinalIgnoreCase))
            {
                response = onDebugLoad();
            }

            if (methodName.Equals("activity", StringComparison.OrdinalIgnoreCase))
            {
                response = onActivity(request);
            }

            if (methodName.Equals("generatenewaddress", StringComparison.OrdinalIgnoreCase))
            {
                response = onGenerateNewAddress(request);
            }

            bool resources = false;

            if (methodName.Equals("resources", StringComparison.OrdinalIgnoreCase))
            {
                onResources(context);
                resources = true;
            }

            if (!resources)
            {
                // Set the content type to plain to prevent xml parsing errors in various browsers
                context.Response.ContentType = "application/json";

                if (response == null)
                {
                    response = new JsonResponse() { error = new JsonError() { code = (int)RPCErrorCode.RPC_METHOD_NOT_FOUND, message = "Unknown API request '" + methodName + "'" } };
                }
                sendResponse(context.Response, response);
            }
            context.Response.Close();
        }

        public JsonResponse onShutdown()
        {
            JsonError error = null;

            forceShutdown = true;

            return new JsonResponse { result = "Node shutdown", error = error };
        }

        public JsonResponse onReconnect()
        {
            JsonError error = null;

            Node.reconnect();

            return new JsonResponse { result = "Reconnecting node to network now.", error = error };
        }

        public JsonResponse onConnect(HttpListenerRequest request)
        {
            JsonError error = null;

            string to = request.QueryString["to"];

            NetworkClientManager.connectTo(to);

            return new JsonResponse { result = string.Format("Connecting to node {0}", to), error = error };
        }

        public JsonResponse onIsolate()
        {
            JsonError error = null;

            Node.isolate();

            return new JsonResponse { result = "Isolating from network now.", error = error };
        }

        public JsonResponse onSync()
        {
            JsonError error = null;

            Node.synchronize();

            return new JsonResponse { result = "Synchronizing to network now.", error = error };
        }

        public JsonResponse onAddTransaction(HttpListenerRequest request)
        {
            // Add a new transaction. This test allows sending and receiving from arbitrary addresses
            IxiNumber from_amount = 0;
            IxiNumber fee = CoreConfig.transactionPrice;

            string primary_address = request.QueryString["primaryAddress"];
            byte[] primary_address_bytes = null;
            if (primary_address == null)
            {
                primary_address_bytes = Node.walletStorage.getPrimaryAddress();
            }else
            {
                primary_address_bytes = Base58Check.Base58CheckEncoding.DecodePlain(primary_address);
            }


            SortedDictionary<byte[], IxiNumber> fromList = new SortedDictionary<byte[], IxiNumber>(new ByteArrayComparer());
            if (request.QueryString["from"] != null)
            {
                string[] from_split = request.QueryString["from"].Split('-');
                if (from_split.Length > 0)
                {
                    foreach (string single_from in from_split)
                    {
                        string[] single_from_split = single_from.Split('_');
                        byte[] single_from_address = Base58Check.Base58CheckEncoding.DecodePlain(single_from_split[0]);
                        IxiNumber singleFromAmount = new IxiNumber(single_from_split[1]);
                        if (singleFromAmount < 0 || singleFromAmount == 0)
                        {
                            from_amount = 0;
                            break;
                        }
                        from_amount += singleFromAmount;
                        fromList.Add(single_from_address, singleFromAmount);
                    }
                }
                // Only create a transaction if there is a valid amount
                if (from_amount < 0 || from_amount == 0)
                {
                    return new JsonResponse { result = null, error = new JsonError() { code = (int)RPCErrorCode.RPC_INVALID_PARAMS, message = "Invalid from amount was specified" } };
                }
            }

            IxiNumber to_amount = 0;
            SortedDictionary<byte[], IxiNumber> toList = new SortedDictionary<byte[], IxiNumber>(new ByteArrayComparer());
            string[] to_split = request.QueryString["to"].Split('-');
            if (to_split.Length > 0)
            {
                foreach (string single_to in to_split)
                {
                    string[] single_to_split = single_to.Split('_');
                    byte[] single_to_address = Base58Check.Base58CheckEncoding.DecodePlain(single_to_split[0]);
                    if (!Address.validateChecksum(single_to_address))
                    {
                        return new JsonResponse { result = null, error = new JsonError() { code = (int)RPCErrorCode.RPC_INVALID_PARAMS, message = "Invalid to address was specified" } };
                    }
                    IxiNumber singleToAmount = new IxiNumber(single_to_split[1]);
                    if (singleToAmount < 0 || singleToAmount == 0)
                    {
                        return new JsonResponse { result = null, error = new JsonError() { code = (int)RPCErrorCode.RPC_INVALID_PARAMS, message = "Invalid to amount was specified" } };
                    }
                    to_amount += singleToAmount;
                    toList.Add(single_to_address, singleToAmount);
                }
            }

            string fee_string = request.QueryString["fee"];
            if (fee_string != null && fee_string.Length > 0)
            {
                fee = new IxiNumber(fee_string);
            }

            // Only create a transaction if there is a valid amount
            if (to_amount < 0 || to_amount == 0)
            {
                return new JsonResponse { result = null, error = new JsonError() { code = (int)RPCErrorCode.RPC_INVALID_PARAMS, message = "Invalid to amount was specified" } };
            }

            byte[] pubKey = Node.walletStorage.getKeyPair(primary_address_bytes).publicKeyBytes;

            // Check if this wallet's public key is already in the WalletState
            Wallet mywallet = Node.walletState.getWallet(primary_address_bytes);
            if (mywallet.publicKey != null && mywallet.publicKey.SequenceEqual(pubKey))
            {
                // Walletstate public key matches, we don't need to send the public key in the transaction
                pubKey = primary_address_bytes;
            }

            bool adjust_amount = false;
            if (fromList.Count == 0)
            {
                fromList = Node.walletStorage.generateFromList(primary_address_bytes, to_amount + fee, toList.Keys.ToList());
                adjust_amount = true;
            }

            if (fromList == null || fromList.Count == 0)
            {
                return new JsonResponse { result = null, error = new JsonError() { code = (int)RPCErrorCode.RPC_VERIFY_ERROR, message = "From list is empty" } };
            }

            Transaction transaction = new Transaction((int)Transaction.Type.Normal, fee, toList, fromList, null, pubKey, Node.getHighestKnownNetworkBlockHeight(), -1);
            if(adjust_amount)
            {
                IxiNumber total_tx_fee = fee;
                for (int i = 0; i < 2 && transaction.fee != total_tx_fee; i++)
                {
                    total_tx_fee = transaction.fee;
                    fromList = Node.walletStorage.generateFromList(primary_address_bytes, to_amount + total_tx_fee, toList.Keys.ToList());
                    if (fromList == null || fromList.Count == 0)
                    {
                        return new JsonResponse { result = null, error = new JsonError() { code = (int)RPCErrorCode.RPC_VERIFY_ERROR, message = "From list is empty" } };
                    }
                    transaction = new Transaction((int)Transaction.Type.Normal, fee, toList, fromList, null, pubKey, Node.getHighestKnownNetworkBlockHeight(), -1);
                }
            }
            if (to_amount + transaction.fee > Node.walletStorage.getMyTotalBalance(primary_address_bytes))
            {
                return new JsonResponse { result = null, error = new JsonError() { code = (int)RPCErrorCode.RPC_WALLET_INSUFFICIENT_FUNDS, message = "Balance is too low" } };
            }

            if (TransactionPool.addTransaction(transaction))
            {
                TransactionPool.addPendingLocalTransaction(transaction);
                return new JsonResponse { result = transaction.toDictionary(), error = null };
            }

            return new JsonResponse { result = null, error = new JsonError() { code = (int)RPCErrorCode.RPC_VERIFY_ERROR, message = "An unknown error occured while adding the transaction" } };
        }

        public JsonResponse onAddMultiSigTransaction(HttpListenerRequest request)
        {
            JsonError error = null;

            // Add a new transaction. This test allows sending and receiving from arbitrary addresses
            object res = "Incorrect transaction parameters.";


            IxiNumber amount = 0;
            IxiNumber fee = CoreConfig.transactionPrice;
            SortedDictionary<byte[], IxiNumber> toList = new SortedDictionary<byte[], IxiNumber>(new ByteArrayComparer());
            string[] to_split = request.QueryString["to"].Split('-');
            if (to_split.Length > 0)
            {
                foreach (string single_to in to_split)
                {
                    string[] single_to_split = single_to.Split('_');
                    byte[] single_to_address = Base58Check.Base58CheckEncoding.DecodePlain(single_to_split[0]);
                    if (!Address.validateChecksum(single_to_address))
                    {
                        res = "Incorrect to address.";
                        amount = 0;
                        break;
                    }
                    IxiNumber singleToAmount = new IxiNumber(single_to_split[1]);
                    if (singleToAmount < 0 || singleToAmount == 0)
                    {
                        res = "Incorrect amount.";
                        amount = 0;
                        break;
                    }
                    amount += singleToAmount;
                    toList.Add(single_to_address, singleToAmount);
                }
            }
            string fee_string = request.QueryString["fee"];
            if (fee_string != null && fee_string.Length > 0)
            {
                fee = new IxiNumber(fee_string);
            }

            byte[] tx_pub_key = null;

            byte[] from = Base58Check.Base58CheckEncoding.DecodePlain(request.QueryString["from"]);
            string orig_txid = request.QueryString["origtx"];
            if (orig_txid == null)
            {
                orig_txid = "";
                // TODO TODO TODO TODO TODO TODO make this compatible with wallet v3
                Wallet tmp_wallet = Node.walletState.getWallet(from);
                if (tmp_wallet != null && tmp_wallet.publicKey != null)
                {
                    tx_pub_key = from;
                }
                else
                {
                    tx_pub_key = Node.walletStorage.getPrimaryPublicKey();
                }
            }
            else
            {
                Transaction orig_tx = TransactionPool.getTransaction(orig_txid);
                if (orig_tx == null)
                {
                    error = new JsonError { code = (int)RPCErrorCode.RPC_INVALID_PARAMETER, message = "Original tx " + orig_txid + " not found" };
                    return new JsonResponse { result = null, error = error };
                }
                tx_pub_key = orig_tx.pubKey;
            }

            // Only create a transaction if there is a valid amount
            if (amount > 0)
            {
                Transaction transaction = Transaction.multisigTransaction(orig_txid, fee, toList, from, Node.blockChain.getLastBlockNum());
                if(transaction == null)
                {
                    error = new JsonError { code = (int)RPCErrorCode.RPC_WALLET_ERROR, message = "An error occured while creating multisig transaction" };
                    return new JsonResponse { result = null, error = error };
                }
                Wallet wallet = Node.walletState.getWallet(from);
                if (wallet.balance < transaction.amount + transaction.fee)
                {
                    error = new JsonError { code = (int)RPCErrorCode.RPC_WALLET_ERROR, message = "Your account's balance is less than the sending amount + fee." };
                    return new JsonResponse { result = null, error = error };
                }
                else
                {
                    if (TransactionPool.addTransaction(transaction))
                    {
                        TransactionPool.addPendingLocalTransaction(transaction);
                        res = transaction.toDictionary();
                    }
                    else
                    {
                        error = new JsonError { code = (int)RPCErrorCode.RPC_WALLET_ERROR, message = "An error occured while creating multisig transaction" };
                        return new JsonResponse { result = null, error = error };
                    }
                }
            }

            return new JsonResponse { result = res, error = error };
        }

        public JsonResponse onAddMultiSigKey(HttpListenerRequest request)
        {
            JsonError error = null;

            // transaction which alters a multisig wallet
            object res = "Incorrect transaction parameters.";

            byte[] tx_pub_key = null;

            byte[] destWallet = Base58Check.Base58CheckEncoding.DecodePlain(request.QueryString["wallet"]);
            string orig_txid = request.QueryString["origtx"];
            if(orig_txid == null)
            {
                orig_txid = "";
                // TODO TODO TODO TODO TODO TODO make this compatible with wallet v3
                Wallet tmp_wallet = Node.walletState.getWallet(destWallet);
                if (tmp_wallet != null && tmp_wallet.publicKey != null)
                {
                    tx_pub_key = destWallet;
                }
                else
                {
                    tx_pub_key = Node.walletStorage.getPrimaryPublicKey();
                }
            }
            else
            {
                Transaction orig_tx = TransactionPool.getTransaction(orig_txid);
                if(orig_tx == null)
                {
                    error = new JsonError { code = (int)RPCErrorCode.RPC_INVALID_PARAMETER, message = "Original tx "+ orig_txid +" not found" };
                    return new JsonResponse { result = null, error = error };
                }
                tx_pub_key = orig_tx.pubKey;
            }

            string signer = request.QueryString["signer"];
            byte[] signer_address = new Address(Base58Check.Base58CheckEncoding.DecodePlain(signer)).address;
            IxiNumber fee = CoreConfig.transactionPrice;

            Transaction transaction = Transaction.multisigAddKeyTransaction(orig_txid, signer_address, fee, destWallet, Node.blockChain.getLastBlockNum());
            if (TransactionPool.addTransaction(transaction))
            {
                TransactionPool.addPendingLocalTransaction(transaction);
                res = transaction.toDictionary();
            }
            else
            {
                res = "There was an error adding the transaction.";
            }

            return new JsonResponse { result = res, error = error };
        }

        public JsonResponse onDelMultiSigKey(HttpListenerRequest request)
        {
            JsonError error = null;

            // transaction which alters a multisig wallet
            object res = "Incorrect transaction parameters.";

            byte[] tx_pub_key = null;

            byte[] destWallet = Base58Check.Base58CheckEncoding.DecodePlain(request.QueryString["wallet"]);
            string orig_txid = request.QueryString["origtx"];
            if (orig_txid == null)
            {
                orig_txid = "";
                // TODO TODO TODO TODO TODO TODO make this compatible with wallet v3
                Wallet tmp_wallet = Node.walletState.getWallet(destWallet);
                if (tmp_wallet != null && tmp_wallet.publicKey != null)
                {
                    tx_pub_key = destWallet;
                }
                else
                {
                    tx_pub_key = Node.walletStorage.getPrimaryPublicKey();
                }
            }
            else
            {
                Transaction orig_tx = TransactionPool.getTransaction(orig_txid);
                if (orig_tx == null)
                {
                    error = new JsonError { code = (int)RPCErrorCode.RPC_INVALID_PARAMETER, message = "Original tx " + orig_txid + " not found" };
                    return new JsonResponse { result = null, error = error };
                }
                tx_pub_key = orig_tx.pubKey;
            }

            string signer = request.QueryString["signer"];
            byte[] signer_address = new Address(Base58Check.Base58CheckEncoding.DecodePlain(signer)).address;

            IxiNumber fee = CoreConfig.transactionPrice;

            Transaction transaction = Transaction.multisigDelKeyTransaction(orig_txid, signer_address, fee, destWallet, Node.blockChain.getLastBlockNum());
            if (TransactionPool.addTransaction(transaction))
            {
                TransactionPool.addPendingLocalTransaction(transaction);
                res = transaction.toDictionary();
            }
            else
            {
                res = "There was an error adding the transaction.";
            }

            return new JsonResponse { result = res, error = error };
        }

        public JsonResponse onChangeMultiSigs(HttpListenerRequest request)
        {
            JsonError error = null;

            // transaction which alters a multisig wallet
            object res = "Incorrect transaction parameters.";

            byte[] tx_pub_key = null;

            byte[] destWallet = Base58Check.Base58CheckEncoding.DecodePlain(request.QueryString["wallet"]);
            string orig_txid = request.QueryString["origtx"];
            if (orig_txid == null)
            {
                orig_txid = "";
                // TODO TODO TODO TODO TODO TODO make this compatible with wallet v3
                Wallet tmp_wallet = Node.walletState.getWallet(destWallet);
                if (tmp_wallet != null && tmp_wallet.publicKey != null)
                {
                    tx_pub_key = destWallet;
                }
                else
                {
                    tx_pub_key = Node.walletStorage.getPrimaryPublicKey();
                }
            }
            else
            {
                Transaction orig_tx = TransactionPool.getTransaction(orig_txid);
                if (orig_tx == null)
                {
                    error = new JsonError { code = (int)RPCErrorCode.RPC_INVALID_PARAMETER, message = "Original tx " + orig_txid + " not found" };
                    return new JsonResponse { result = null, error = error };
                }
                tx_pub_key = orig_tx.pubKey;
            }

            string sigs = request.QueryString["sigs"];
            IxiNumber fee = CoreConfig.transactionPrice;
            if (byte.TryParse(sigs, out byte reqSigs))
            {

                Transaction transaction = Transaction.multisigChangeReqSigs(orig_txid, reqSigs, fee, destWallet, Node.blockChain.getLastBlockNum());
                if (TransactionPool.addTransaction(transaction))
                {
                    TransactionPool.addPendingLocalTransaction(transaction);
                    res = transaction.toDictionary();
                }
                else
                {
                    res = "There was an error adding the transaction.";
                }
            }

            return new JsonResponse { result = res, error = error };
        }

        public JsonResponse onGetBalance(HttpListenerRequest request)
        {
            JsonError error = null;

            byte[] address = Base58Check.Base58CheckEncoding.DecodePlain(request.QueryString["address"]);

            IxiNumber balance = Node.walletStorage.getMyTotalBalance(address);
            balance -= TransactionPool.getPendingSendingTransactionsAmount(address);

            return new JsonResponse { result = balance.ToString(), error = error };
        }

        public JsonResponse onGetBlock(HttpListenerRequest request)
        {
            JsonError error = null;

            string blocknum_string = request.QueryString["num"];
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
            }else
            {
                blockData = new Dictionary<string, string>();

                blockData.Add("Block Number", block.blockNum.ToString());
                blockData.Add("Version", block.version.ToString());
                blockData.Add("Block Checksum", Crypto.hashToString(block.blockChecksum));
                blockData.Add("Last Block Checksum", Crypto.hashToString(block.lastBlockChecksum));
                blockData.Add("Wallet State Checksum", Crypto.hashToString(block.walletStateChecksum));
                blockData.Add("Sig freeze Checksum", Crypto.hashToString(block.signatureFreezeChecksum));
                blockData.Add("PoW field", Crypto.hashToString(block.powField));
                blockData.Add("Timestamp", block.timestamp.ToString());
                blockData.Add("Difficulty", block.difficulty.ToString());
                blockData.Add("Signature count", block.signatures.Count.ToString());
                blockData.Add("Transaction count", block.transactions.Count.ToString());
                blockData.Add("Transaction amount", block.getTotalTransactionsValue().ToString());
                blockData.Add("Signatures", JsonConvert.SerializeObject(block.signatures));
                blockData.Add("TX IDs", JsonConvert.SerializeObject(block.transactions));
            }


            return new JsonResponse { result = blockData, error = error };
        }

        public JsonResponse onGetLastBlocks()
        {
            JsonError error = null;

            Dictionary<string, string>[] blocks = new Dictionary<string, string>[10];
            long blockCnt = Node.blockChain.Count > 10 ? 10 : Node.blockChain.Count;
            for (ulong i = 0; i < (ulong)blockCnt; i++)
            {
                Block block = Node.blockChain.getBlock(Node.blockChain.getLastBlockNum() - i);
                if (block == null)
                {
                    error = new JsonError { code = (int)RPCErrorCode.RPC_INVALID_PARAMETER, message = "An unknown error occured, while getting one of the last 10 blocks." };
                    return new JsonResponse { result = null, error = error };
                }

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
                blockData.Add("Signature count", block.signatures.Count.ToString());
                blockData.Add("Transaction count", block.transactions.Count.ToString());
                blockData.Add("Transaction amount", block.getTotalTransactionsValue().ToString());
                blockData.Add("Signatures", JsonConvert.SerializeObject(block.signatures));
                blockData.Add("TX IDs", JsonConvert.SerializeObject(block.transactions));

                blocks[i] = blockData;
            }

            return new JsonResponse { result = blocks, error = error };
        }

        public JsonResponse onGetFullBlock(HttpListenerRequest request)
        {
            JsonError error = null;

            string blocknum_string = request.QueryString["num"];
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

                blockData = new Dictionary<string, string>();

                blockData.Add("Block Number", block.blockNum.ToString());
                blockData.Add("Version", block.version.ToString());
                blockData.Add("Block Checksum", Crypto.hashToString(block.blockChecksum));
                blockData.Add("Last Block Checksum", Crypto.hashToString(block.lastBlockChecksum));
                blockData.Add("Wallet State Checksum", Crypto.hashToString(block.walletStateChecksum));
                blockData.Add("Sig freeze Checksum", Crypto.hashToString(block.signatureFreezeChecksum));
                blockData.Add("PoW field", Crypto.hashToString(block.powField));
                blockData.Add("Timestamp", block.timestamp.ToString());
                blockData.Add("Difficulty", block.difficulty.ToString());
                blockData.Add("Signature count", block.signatures.Count.ToString());
                blockData.Add("Transaction count", block.transactions.Count.ToString());
                blockData.Add("Transaction amount", block.getTotalTransactionsValue().ToString());
                blockData.Add("Signatures", JsonConvert.SerializeObject(block.signatures));
                blockData.Add("TX IDs", JsonConvert.SerializeObject(block.transactions));
                blockData.Add("Transactions", JsonConvert.SerializeObject(block.getFullTransactionsAsArray()));
            }

            return new JsonResponse { result = blockData, error = error };
        }

        public JsonResponse onGetTransaction(HttpListenerRequest request)
        {
            string txid_string = request.QueryString["id"];
            Transaction t = TransactionPool.getTransaction(txid_string, 0, Config.storeFullHistory);
            if (t == null)
            {
                return new JsonResponse { result = null, error = new JsonError { code = (int)RPCErrorCode.RPC_INVALID_PARAMETER, message = "Transaction not found." } };
            }

            return new JsonResponse { result = t.toDictionary(), error = null };
        }

        public JsonResponse onGetTotalBalance()
        {
            return new JsonResponse { result = Node.walletStorage.getMyTotalBalance(Node.walletStorage.getPrimaryAddress()).ToString(), error = null };
        }

        public JsonResponse onStress(HttpListenerRequest request)
        {
            JsonError error = null;

            string type_string = request.QueryString["type"];
            if (type_string == null)
            {
                type_string = "txspam";
            }

            int txnum = 0;
            string txnumstr = request.QueryString["num"];
            if (txnumstr != null)
            {
                try
                {
                    txnum = Convert.ToInt32(txnumstr);
                }
                catch (OverflowException)
                {
                    txnum = 0;
                }
            }


            // Used for performing various tests.
            StressTest.start(type_string, txnum);

            return new JsonResponse { result = "Stress test started", error = error };
        }

        public JsonResponse onMyWallet()
        {
            JsonError error = null;

            // Show own address, balance and blockchain synchronization status
            List<Address> address_list = Node.walletStorage.getMyAddresses();

            Dictionary<string, string> address_balance_list = new Dictionary<string, string>();

            foreach (Address addr in address_list)
            {
                address_balance_list.Add(addr.ToString(), Node.walletState.getWalletBalance(addr.address).ToString());
            }

            return new JsonResponse { result = address_balance_list, error = error };
        }

        public JsonResponse onMyPubKey()
        {
            JsonError error = null;

            return new JsonResponse { result = Crypto.hashToString(Node.walletStorage.getPrimaryPublicKey()), error = error };
        }

        public JsonResponse onGetWallet(HttpListenerRequest request)
        {
            JsonError error = null;

            // Show own address, balance and blockchain synchronization status
            byte[] address = Base58Check.Base58CheckEncoding.DecodePlain(request.QueryString["id"]);
            Wallet w = Node.walletState.getWallet(address);

            Dictionary<string, string> walletData = new Dictionary<string, string>();
            walletData.Add("id", Base58Check.Base58CheckEncoding.EncodePlain(w.id));
            walletData.Add("balance", w.balance.ToString());
            walletData.Add("type", w.type.ToString());
            walletData.Add("requiredSigs", w.requiredSigs.ToString());
            if (w.allowedSigners != null)
            {
                if (w.allowedSigners != null)
                {
                    walletData.Add("allowedSigners", "(" + (w.allowedSigners.Length + 1) + " keys): " +
                        w.allowedSigners.Aggregate(Base58Check.Base58CheckEncoding.EncodePlain(w.id), (aggr, n) => aggr += "," + Base58Check.Base58CheckEncoding.EncodePlain(n), aggr => aggr)
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

            return new JsonResponse { result = walletData, error = error };
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
                walletData.Add("id", Base58Check.Base58CheckEncoding.EncodePlain(w.id));
                walletData.Add("balance", w.balance.ToString());
                walletData.Add("type", w.type.ToString());
                walletData.Add("requiredSigs", w.requiredSigs.ToString());
                if (w.allowedSigners != null)
                {
                    if (w.allowedSigners != null)
                    {
                        walletData.Add("allowedSigners", "(" + (w.allowedSigners.Length + 1) + " keys): " +
                            w.allowedSigners.Aggregate(Base58Check.Base58CheckEncoding.EncodePlain(w.id), (aggr, n) => aggr += "," + Base58Check.Base58CheckEncoding.EncodePlain(n), aggr => aggr)
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
                    walletData.Add("publicKey", w.publicKey.ToString());
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

            // Show a list of presences
            lock (PresenceList.presences)
            {
                var json = PresenceList.presences;
                return new JsonResponse { result = json, error = error };
            }

        }

        public JsonResponse onClients()
        {
            JsonError error = null;

            String[] res = NetworkServer.getConnectedClients();

            return new JsonResponse { result = res, error = error };
        }
        public JsonResponse onServers()
        {
            JsonError error = null;

            String[] res = NetworkClientManager.getConnectedClients();

            return new JsonResponse { result = res, error = error };
        }

        public JsonResponse onTx()
        {
            JsonError error = null;

            Transaction[] transactions = TransactionPool.getLastTransactions();

            Dictionary<string, Dictionary<string, object>> tx_list = new Dictionary<string, Dictionary<string, object>>();

            foreach (Transaction t in transactions)
            {
                tx_list.Add(t.id, t.toDictionary());
            }

            return new JsonResponse { result = tx_list, error = error };
        }

        public JsonResponse onTxu()
        {
            JsonError error = null;

            Transaction[] transactions = TransactionPool.getUnappliedTransactions();

            Dictionary<string, Dictionary<string, object>> tx_list = new Dictionary<string, Dictionary<string, object>>();

            foreach(Transaction t in transactions)
            {
                tx_list.Add(t.id, t.toDictionary());
            }
            
            return new JsonResponse { result = tx_list, error = error };
        }

        public JsonResponse onStatus()
        {
            JsonError error = null;

            Dictionary<string, object> networkArray = new Dictionary<string, object>();

            networkArray.Add("Node Version", Config.version);
            string netType = "mainnet";
            if (Config.isTestNet)
            {
                netType = "testnet";
            }
            networkArray.Add("Network type", netType);
            networkArray.Add("My time", Clock.getTimestamp());
            networkArray.Add("Network time difference", Core.networkTimeDifference);
            networkArray.Add("My External IP", Config.publicServerIP);
            //networkArray.Add("Listening interface", context.Request.RemoteEndPoint.Address.ToString());
            networkArray.Add("Queues", "Rcv: " + NetworkQueue.getQueuedMessageCount() + ", RcvTx: " + NetworkQueue.getTxQueuedMessageCount()
                + ", SendClients: " + NetworkServer.getQueuedMessageCount() + ", SendServers: " + NetworkClientManager.getQueuedMessageCount()
                + ", Storage: " + Storage.getQueuedQueryCount() + ", Logging: " + Logging.getRemainingStatementsCount() + ", Pending Transactions: " + TransactionPool.pendingTransactionCount());
            networkArray.Add("Node Deprecation Block Limit", Config.compileTimeBlockNumber + Config.deprecationBlockOffset);

            string dltStatus = "Active";
            if (Node.blockSync.synchronizing)
                dltStatus = "Synchronizing";
            networkArray.Add("DLT Status", dltStatus);

            string bpStatus = "Stopped";
            if (Node.blockProcessor.operating)
                bpStatus = "Running";
            networkArray.Add("Block Processor Status", bpStatus);

            networkArray.Add("Block Height", Node.blockChain.getLastBlockNum());
            networkArray.Add("Block Version", Node.blockChain.getLastBlockVersion());
            networkArray.Add("Required Consensus", Node.blockChain.getRequiredConsensus());
            networkArray.Add("Wallets", Node.walletState.numWallets);
            networkArray.Add("Presences", PresenceList.getTotalPresences());
            networkArray.Add("Supply", Node.walletState.calculateTotalSupply().ToString());
            networkArray.Add("Applied TX Count", TransactionPool.getTransactionCount() - TransactionPool.getUnappliedTransactions().Count());
            networkArray.Add("Unapplied TX Count", TransactionPool.getUnappliedTransactions().Count());

            networkArray.Add("WS Checksum", Crypto.hashToString(Node.walletState.calculateWalletStateChecksum()));
            networkArray.Add("WS Delta Checksum", Crypto.hashToString(Node.walletState.calculateWalletStateChecksum(true)));

            networkArray.Add("Network Clients", NetworkServer.getConnectedClients());
            networkArray.Add("Network Servers", NetworkClientManager.getConnectedClients());

            networkArray.Add("Masters", PresenceList.countPresences('M'));
            networkArray.Add("Relays", PresenceList.countPresences('R'));
            networkArray.Add("Clients", PresenceList.countPresences('C'));
            networkArray.Add("Workers", PresenceList.countPresences('W'));


            return new JsonResponse { result = networkArray, error = error };
        }

        public JsonResponse onMinerStats()
        {
            JsonError error = null;

            Dictionary<string, Object> minerArray = new Dictionary<string, Object>();

            List<int> blocksCount = Node.miner.getBlocksCount();

            // Last hashrate
            minerArray.Add("Hashrate", Node.miner.lastHashRate);

            // Last hashrate
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

        public JsonResponse onBlockHeight()
        {
            JsonError error = null;

            ulong blockheight = Node.blockChain.getLastBlockNum();

            return new JsonResponse { result = blockheight, error = error };
        }

        public JsonResponse onSupply()
        {
            JsonError error = null;

            string supply = Node.walletState.calculateTotalSupply().ToString();

            return new JsonResponse { result = supply, error = error };
        }

        public JsonResponse onDebugSave()
        {
            JsonError error = null;

            string outstring = "Failed";
            if (DebugSnapshot.save())
                outstring = "Debug Snapshot SAVED";
            else
                error = new JsonError { code = 400, message = "failed" };

            return new JsonResponse { result = outstring, error = error };
        }

        public JsonResponse onDebugLoad()
        {
            JsonError error = null;

            string outstring = "Failed";
            if (DebugSnapshot.load())
                outstring = "Debug Snapshot LOADED";
            else
                error = new JsonError { code = 400, message = "failed" };

            return new JsonResponse { result = outstring, error = error };
        }

        public JsonResponse onActivity(HttpListenerRequest request)
        {
            JsonError error = null;

            string fromIndex = request.QueryString["fromIndex"];
            if (fromIndex == null)
            {
                fromIndex = "0";
            }

            string count = request.QueryString["count"];
            if (count == null)
            {
                count = "50";
            }

            List<Activity> res = ActivityStorage.getActivitiesBySeedHash(Node.walletStorage.getSeedHash(), Int32.Parse(fromIndex), Int32.Parse(count), true);

            return new JsonResponse { result = res, error = error };
        }

        public void onResources(HttpListenerContext context)
        {
            string name = "";
            for (int i = 2; i < context.Request.Url.Segments.Count(); i++)
            {
                name += context.Request.Url.Segments[i];
            }

            if (name != null && name.Length > 1 && !name.EndsWith("/"))
            {
                name = name.Replace('/', Path.DirectorySeparatorChar);
                if (File.Exists("html" + Path.DirectorySeparatorChar + name))
                {
                    if (name.EndsWith(".css", StringComparison.OrdinalIgnoreCase))
                    {
                        context.Response.ContentType = "text/css";
                    }else if(name.EndsWith(".js", StringComparison.OrdinalIgnoreCase))
                    {
                        context.Response.ContentType = "text/javascript";
                    }
                    else
                    {
                        context.Response.ContentType = "application/octet-stream";
                    }
                    sendResponse(context.Response, File.ReadAllBytes("html" + Path.DirectorySeparatorChar + name));
                    return;
                }
            }
            // 404
            context.Response.ContentType = "text/plain";
            context.Response.StatusCode = 404;
            context.Response.StatusDescription = "404 File not found";
            sendResponse(context.Response, "404 File not found");
        }

        public JsonResponse onGenerateNewAddress(HttpListenerRequest request)
        {
            string base_address_str = request.QueryString["address"];
            byte[] base_address = null;
            if (base_address_str == null)
            {
                base_address = Node.walletStorage.getPrimaryAddress();
            }else
            {
                base_address = Base58Check.Base58CheckEncoding.DecodePlain(base_address_str);
            }

            Address new_address = Node.walletStorage.generateNewAddress(new Address(base_address));
            if (new_address != null)
            {
                return new JsonResponse { result = new_address.ToString(), error = null };
            }
            else
            {
                return new JsonResponse { result = null, error = new JsonError() { code = (int)RPCErrorCode.RPC_WALLET_ERROR, message = "Error occured while generating a new address" } };
            }
        }
    }
}