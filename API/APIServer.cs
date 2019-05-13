using DLT;
using DLT.Meta;
using DLT.Network;
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

    //! Bitcoin RPC error codes
    enum RPCErrorCode
    {
        //! Standard JSON-RPC 2.0 errors
        RPC_INVALID_REQUEST = -32600,
        RPC_METHOD_NOT_FOUND = -32601,
        RPC_INVALID_PARAMS = -32602,
        RPC_INTERNAL_ERROR = -32603,
        RPC_PARSE_ERROR = -32700,

        //! General application defined errors
        RPC_MISC_ERROR = -1,  //! std::exception thrown in command handling
        RPC_FORBIDDEN_BY_SAFE_MODE = -2,  //! Server is in safe mode, and command is not allowed in safe mode
        RPC_TYPE_ERROR = -3,  //! Unexpected type was passed as parameter
        RPC_INVALID_ADDRESS_OR_KEY = -5,  //! Invalid address or key
        RPC_OUT_OF_MEMORY = -7,  //! Ran out of memory during operation
        RPC_INVALID_PARAMETER = -8,  //! Invalid, missing or duplicate parameter
        RPC_DATABASE_ERROR = -20, //! Database error
        RPC_DESERIALIZATION_ERROR = -22, //! Error parsing or validating structure in raw format
        RPC_VERIFY_ERROR = -25, //! General error during transaction or block submission
        RPC_VERIFY_REJECTED = -26, //! Transaction or block was rejected by network rules
        RPC_VERIFY_ALREADY_IN_CHAIN = -27, //! Transaction already in chain
        RPC_IN_WARMUP = -28, //! Client still warming up

        //! Aliases for backward compatibility
        RPC_TRANSACTION_ERROR = RPC_VERIFY_ERROR,
        RPC_TRANSACTION_REJECTED = RPC_VERIFY_REJECTED,
        RPC_TRANSACTION_ALREADY_IN_CHAIN = RPC_VERIFY_ALREADY_IN_CHAIN,

        //! P2P client errors
        RPC_CLIENT_NOT_CONNECTED = -9,  //! Bitcoin is not connected
        RPC_CLIENT_IN_INITIAL_DOWNLOAD = -10, //! Still downloading initial blocks
        RPC_CLIENT_NODE_ALREADY_ADDED = -23, //! Node is already added
        RPC_CLIENT_NODE_NOT_ADDED = -24, //! Node has not been added before

        //! Wallet errors
        RPC_WALLET_ERROR = -4,  //! Unspecified problem with wallet (key not found etc.)
        RPC_WALLET_INSUFFICIENT_FUNDS = -6,  //! Not enough funds in wallet or account
        RPC_WALLET_INVALID_ACCOUNT_NAME = -11, //! Invalid account name
        RPC_WALLET_KEYPOOL_RAN_OUT = -12, //! Keypool ran out, call keypoolrefill first
        RPC_WALLET_UNLOCK_NEEDED = -13, //! Enter the wallet passphrase with walletpassphrase first
        RPC_WALLET_PASSPHRASE_INCORRECT = -14, //! The wallet passphrase entered was incorrect
        RPC_WALLET_WRONG_ENC_STATE = -15, //! Command given in wrong wallet encryption state (encrypting an encrypted wallet etc.)
        RPC_WALLET_ENCRYPTION_FAILED = -16, //! Failed to encrypt the wallet
        RPC_WALLET_ALREADY_UNLOCKED = -17, //! Wallet is already unlocked
    };

    class JsonRpcRequest
    {
        public int jsonrpc = 0;
        public string id = null;
        public string method = null;
        public object @params = null;
    }

    class APIServer : GenericAPIServer
    {
        public APIServer()
        {
            // Start the API server
            start(String.Format("http://localhost:{0}/", Config.apiPort), Config.apiUsers);
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

            if (methodName.Equals("createrawtransaction", StringComparison.OrdinalIgnoreCase))
            {
                response = onCreateRawTransaction(request);
            }

            if (methodName.Equals("decoderawtransaction", StringComparison.OrdinalIgnoreCase))
            {
                response = onDecodeRawTransaction(request);
            }

            if (methodName.Equals("signrawtransaction", StringComparison.OrdinalIgnoreCase))
            {
                response = onSignRawTransaction(request);
            }

            if (methodName.Equals("sendrawtransaction", StringComparison.OrdinalIgnoreCase))
            {
                response = onSendRawTransaction(request);
            }

            if (methodName.Equals("calculatetransactionfee", StringComparison.OrdinalIgnoreCase))
            {
                if (request.QueryString["autofee"] != null)
                {
                    response = new JsonResponse { result = null, error = new JsonError() { code = (int)RPCErrorCode.RPC_INVALID_PARAMETER, message = "Automatic fee generation is invalid for `calculatetransactionfee`." } };
                } else
                {
                    response = onCalculateTransactionFee(request);
                }
            }

            if (methodName.Equals("addmultisigtransaction", StringComparison.OrdinalIgnoreCase))
            {
                response = onAddMultiSigTransaction(request);
            }

            if (methodName.Equals("addmultisigtxsignature", StringComparison.OrdinalIgnoreCase))
            {
                response = onAddMultiSigTxSignature(request);
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
                response = onTx(request);
            }

            if (methodName.Equals("txu", StringComparison.OrdinalIgnoreCase))
            {
                response = onTxu(request);
            }

            if (methodName.Equals("txa", StringComparison.OrdinalIgnoreCase))
            {
                response = onTxa(request);
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

            if (methodName.Equals("countnodeversions", StringComparison.OrdinalIgnoreCase))
            {
                response = onCountNodeVersions(request);
            }

            if (methodName.Equals("setBlockSelectionAlgorithm", StringComparison.OrdinalIgnoreCase))
            {
                response = onSetBlockSelectionAlgorithm(request);
            }

            if (methodName.Equals("verifyminingsolution", StringComparison.OrdinalIgnoreCase))
            {
                response = onVerifyMiningSolution(request);
            }

            if (methodName.Equals("submitminingsolution", StringComparison.OrdinalIgnoreCase))
            {
                response = onSubmitMiningSolution(request);
            }

            if (methodName.Equals("getminingblock", StringComparison.OrdinalIgnoreCase))
            {
                response = onGetMiningBlock(request);
            }

            if(methodName.Equals("getwalletbackup", StringComparison.OrdinalIgnoreCase))
            {
                response = onGetWalletBackup(request);
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

            Node.forceShutdown = true;

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

            NetworkClientManager.connectTo(to, null);

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
            object r = createTransactionHelper(request);
            Transaction transaction = null;
            if(r is JsonResponse)
            {
                // there was an error
                return (JsonResponse)r;
            } else if (r is Transaction)
            {
                transaction = (Transaction)r;
            } else
            {
                return new JsonResponse { result = null, error = new JsonError() { code = (int)RPCErrorCode.RPC_INTERNAL_ERROR,
                    message = String.Format("There was an error while creating the transaction: Unexpected object: {0}", r.GetType().Name) } };
            }
            if (TransactionPool.addTransaction(transaction))
            {
                PendingTransactions.addPendingLocalTransaction(transaction);
                return new JsonResponse { result = transaction.toDictionary(), error = null };
            }

            return new JsonResponse { result = null, error = new JsonError() { code = (int)RPCErrorCode.RPC_VERIFY_ERROR, message = "An unknown error occured while adding the transaction" } };
        }

        public JsonResponse onCreateRawTransaction(HttpListenerRequest request)
        {
            // Create a transaction, but do not add it to the TX pool on the node. Useful for:
            // - offline transactions
            // - manually adjusting fee
            object r = createTransactionHelper(request, false);
            Transaction transaction = null;
            if (r is JsonResponse)
            {
                // there was an error
                return (JsonResponse)r;
            }
            else if (r is Transaction)
            {
                transaction = (Transaction)r;
            }
            else
            {
                return new JsonResponse
                {
                    result = null,
                    error = new JsonError()
                    {
                        code = (int)RPCErrorCode.RPC_INTERNAL_ERROR,
                        message = String.Format("There was an error while creating the transaction: Unexpected object: {0}", r.GetType().Name)
                    }
                };
            }
            if (request.QueryString["json"] != null)
            {
                return new JsonResponse { result = transaction.toDictionary(), error = null };
            }
            else
            {
                return new JsonResponse { result = Crypto.hashToString(transaction.getBytes()), error = null };
            }
        }

        public JsonResponse onDecodeRawTransaction(HttpListenerRequest request)
        {
            JsonError error = null;

            // transaction which alters a multisig wallet
            object res = "Incorrect transaction parameters.";

            string raw_transaction_hex = request.QueryString["transaction"];
            if (raw_transaction_hex == null)
            {
                error = new JsonError { code = (int)RPCErrorCode.RPC_INVALID_PARAMETER, message = "transaction parameter is missing" };
                return new JsonResponse { result = null, error = error };
            }
            Transaction raw_transaction = new Transaction(Crypto.stringToHash(raw_transaction_hex));
            return new JsonResponse { result = raw_transaction.toDictionary(), error = null };
        }

        public JsonResponse onSignRawTransaction(HttpListenerRequest request)
        {
            JsonError error = null;

            // transaction which alters a multisig wallet
            object res = "Incorrect transaction parameters.";

            string raw_transaction_hex = request.QueryString["transaction"];
            if (raw_transaction_hex == null)
            {
                error = new JsonError { code = (int)RPCErrorCode.RPC_INVALID_PARAMETER, message = "transaction parameter is missing" };
                return new JsonResponse { result = null, error = error };
            }
            Transaction raw_transaction = new Transaction(Crypto.stringToHash(raw_transaction_hex));
            raw_transaction.signature = raw_transaction.getSignature(raw_transaction.checksum);
            return new JsonResponse { result = Crypto.hashToString(raw_transaction.getBytes()), error = null };
        }

        public JsonResponse onSendRawTransaction(HttpListenerRequest request)
        {
            JsonError error = null;

            // transaction which alters a multisig wallet
            object res = "Incorrect transaction parameters.";

            string raw_transaction_hex = request.QueryString["transaction"];
            if (raw_transaction_hex == null)
            {
                error = new JsonError { code = (int)RPCErrorCode.RPC_INVALID_PARAMETER, message = "transaction parameter is missing" };
                return new JsonResponse { result = null, error = error };
            }
            Transaction raw_transaction = new Transaction(Crypto.stringToHash(raw_transaction_hex));

            if (TransactionPool.addTransaction(raw_transaction))
            {
                PendingTransactions.addPendingLocalTransaction(raw_transaction);
                return new JsonResponse { result = raw_transaction.toDictionary(), error = null };
            }

            return new JsonResponse { result = null, error = new JsonError() { code = (int)RPCErrorCode.RPC_VERIFY_ERROR, message = "An unknown error occured while adding the transaction" } };
        }



        public JsonResponse onCalculateTransactionFee(HttpListenerRequest request)
        {
            // Create a dummy transaction, just so that we can calculate the appropriate fee required to process this (minimum fee)
            object r = createTransactionHelper(request);
            Transaction transaction = null;
            if (r is JsonResponse)
            {
                // there was an error
                return (JsonResponse)r;
            }
            else if (r is Transaction)
            {
                transaction = (Transaction)r;
            }
            else
            {
                return new JsonResponse
                {
                    result = null,
                    error = new JsonError()
                    {
                        code = (int)RPCErrorCode.RPC_INTERNAL_ERROR,
                        message = String.Format("There was an error while creating the transaction: Unexpected object: {0}", r.GetType().Name)
                    }
                };
            }
            return new JsonResponse { result = transaction.fee.ToString(), error = null };
        }


        public JsonResponse onAddMultiSigTxSignature(HttpListenerRequest request)
        {
            JsonError error = null;

            // transaction which alters a multisig wallet
            object res = "Incorrect transaction parameters.";

            byte[] destWallet = Base58Check.Base58CheckEncoding.DecodePlain(request.QueryString["wallet"]);
            if(destWallet == null)
            {
                error = new JsonError { code = (int)RPCErrorCode.RPC_INVALID_PARAMETER, message = "wallet parameter is missing" };
                return new JsonResponse { result = null, error = error };
            }
            string orig_txid = request.QueryString["origtx"];
            if (orig_txid == null)
            {
                error = new JsonError { code = (int)RPCErrorCode.RPC_INVALID_PARAMETER, message = "origtx parameter is missing" };
                return new JsonResponse { result = null, error = error };
            }
            // no need to check if orig_txid exists as it may not (yet) because we're C/W node, TODO TODO in the future we could query a M/H node

            IxiNumber fee = CoreConfig.transactionPrice;

            Transaction transaction = Transaction.multisigAddTxSignature(orig_txid, fee, destWallet, Node.blockChain.getLastBlockNum());
            if (TransactionPool.addTransaction(transaction))
            {
                PendingTransactions.addPendingLocalTransaction(transaction);
                res = transaction.toDictionary();
            }
            else
            {
                error = new JsonError { code = (int)RPCErrorCode.RPC_INTERNAL_ERROR, message = "There was an error adding the transaction." };
                res = null;
            }

            return new JsonResponse { result = res, error = error };
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

            byte[] from = Base58Check.Base58CheckEncoding.DecodePlain(request.QueryString["from"]);
            if(Node.walletState.getWallet(from).type != WalletType.Multisig)
            {
                error = new JsonError { code = (int)RPCErrorCode.RPC_WALLET_ERROR, message = "The specified 'from' wallet is not a multisig wallet." };
                return new JsonResponse { result = null, error = error };
            }
            // Only create a transaction if there is a valid amount
            if (amount > 0)
            {
                Transaction transaction = Transaction.multisigTransaction(fee, toList, from, Node.blockChain.getLastBlockNum());
                if(transaction == null)
                {
                    error = new JsonError { code = (int)RPCErrorCode.RPC_INTERNAL_ERROR, message = "An error occured while creating multisig transaction" };
                    return new JsonResponse { result = null, error = error };
                }
                Wallet wallet = Node.walletState.getWallet(from);
                if (wallet.balance < transaction.amount + transaction.fee)
                {
                    error = new JsonError { code = (int)RPCErrorCode.RPC_WALLET_INSUFFICIENT_FUNDS, message = "Your account's balance is less than the sending amount + fee." };
                    return new JsonResponse { result = null, error = error };
                }
                else
                {
                    if (TransactionPool.addTransaction(transaction))
                    {
                        PendingTransactions.addPendingLocalTransaction(transaction);
                        res = transaction.toDictionary();
                    }
                    else
                    {
                        error = new JsonError { code = (int)RPCErrorCode.RPC_INTERNAL_ERROR, message = "An error occured while creating multisig transaction" };
                        return new JsonResponse { result = null, error = error };
                    }
                }
            }

            return new JsonResponse { result = res, error = error };
        }

        public JsonResponse onAddMultiSigKey(HttpListenerRequest request)
        {
            // transaction which alters a multisig wallet
            byte[] destWallet = Base58Check.Base58CheckEncoding.DecodePlain(request.QueryString["wallet"]);
            if (destWallet == null)
            {
                return new JsonResponse { result = null, error = new JsonError() { code = (int)RPCErrorCode.RPC_INVALID_PARAMS, message = "Parameter 'wallet' is missing." } };
            }

            string signer = request.QueryString["signer"];
            if (signer == null)
            {
                return new JsonResponse { result = null, error = new JsonError() { code = (int)RPCErrorCode.RPC_INVALID_PARAMS, message = "Parameter 'signer' is missing." } };
            }
            byte[] signer_address = new Address(Base58Check.Base58CheckEncoding.DecodePlain(signer)).address;
            IxiNumber fee = CoreConfig.transactionPrice;

            Transaction transaction = Transaction.multisigAddKeyTransaction(signer_address, fee, destWallet, Node.blockChain.getLastBlockNum());
            if (TransactionPool.addTransaction(transaction))
            {
                PendingTransactions.addPendingLocalTransaction(transaction);
                return new JsonResponse { result = transaction.toDictionary(), error = null };
            }
            return new JsonResponse { result = null, error = new JsonError() { code = (int)RPCErrorCode.RPC_INTERNAL_ERROR, message = "Error while creating the transaction." } };
        }

        public JsonResponse onDelMultiSigKey(HttpListenerRequest request)
        {
            // transaction which alters a multisig wallet
            object res = "Incorrect transaction parameters.";

            byte[] destWallet = Base58Check.Base58CheckEncoding.DecodePlain(request.QueryString["wallet"]);
            if (destWallet == null)
            {
                return new JsonResponse { result = null, error = new JsonError() { code = (int)RPCErrorCode.RPC_INVALID_PARAMS, message = "Parameter 'wallet' is missing." } };
            }

            string signer = request.QueryString["signer"];
            if (signer == null)
            {
                return new JsonResponse { result = null, error = new JsonError() { code = (int)RPCErrorCode.RPC_INVALID_PARAMS, message = "Parameter 'signer' is missing." } };
            }
            byte[] signer_address = new Address(Base58Check.Base58CheckEncoding.DecodePlain(signer)).address;

            IxiNumber fee = CoreConfig.transactionPrice;

            Transaction transaction = Transaction.multisigDelKeyTransaction(signer_address, fee, destWallet, Node.blockChain.getLastBlockNum());
            if (TransactionPool.addTransaction(transaction))
            {
                PendingTransactions.addPendingLocalTransaction(transaction);
                return new JsonResponse { result = transaction.toDictionary(), error = null };
            }
            return new JsonResponse { result = null, error = new JsonError() { code = (int)RPCErrorCode.RPC_INTERNAL_ERROR, message = "Error while creating the transaction." } };
        }

        public JsonResponse onChangeMultiSigs(HttpListenerRequest request)
        {
            // transaction which alters a multisig wallet
            object res = "Incorrect transaction parameters.";

            byte[] destWallet = Base58Check.Base58CheckEncoding.DecodePlain(request.QueryString["wallet"]);
            if (destWallet == null)
            {
                return new JsonResponse { result = null, error = new JsonError() { code = (int)RPCErrorCode.RPC_INVALID_PARAMS, message = "Parameter 'wallet' is missing." } };
            }

            string sigs = request.QueryString["sigs"];
            if (sigs == null)
            {
                return new JsonResponse { result = null, error = new JsonError() { code = (int)RPCErrorCode.RPC_INVALID_PARAMS, message = "Parameter 'sigs' is missing." } };
            }

            IxiNumber fee = CoreConfig.transactionPrice;
            if (byte.TryParse(sigs, out byte reqSigs))
            {

                Transaction transaction = Transaction.multisigChangeReqSigs(reqSigs, fee, destWallet, Node.blockChain.getLastBlockNum());
                if (TransactionPool.addTransaction(transaction))
                {
                    PendingTransactions.addPendingLocalTransaction(transaction);
                    return new JsonResponse { result = transaction.toDictionary(), error = null };
                }
            } else
            {
                return new JsonResponse { result = null, error = new JsonError() { code = (int)RPCErrorCode.RPC_INVALID_PARAMETER, message = "Parameter 'sigs' must be a number between 1 and 255." } };
            }

            return new JsonResponse { result = null, error = new JsonError() { code = (int)RPCErrorCode.RPC_INTERNAL_ERROR, message = "Error while creating the transaction." } };
        }

        public JsonResponse onGetBalance(HttpListenerRequest request)
        {
            JsonError error = null;

            byte[] address = Base58Check.Base58CheckEncoding.DecodePlain(request.QueryString["address"]);

            IxiNumber balance = Node.walletState.getWalletBalance(address);

            return new JsonResponse { result = balance.ToString(), error = error };
        }

        public JsonResponse onGetBlock(HttpListenerRequest request)
        {
            JsonError error = null;

            string blocknum_string = request.QueryString["num"];
            string bytes = request.QueryString["bytes"];
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

            if(bytes == "1")
            {
                return new JsonResponse { result = Crypto.hashToString(block.getBytes()), error = error };
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
                blockData.Add("Transaction amount", TransactionPool.getTotalTransactionsValueInBlock(block).ToString());
                blockData.Add("Signatures", JsonConvert.SerializeObject(block.signatures));
                blockData.Add("TX IDs", JsonConvert.SerializeObject(block.transactions));
                return new JsonResponse { result = blockData, error = error };
            }
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
                    error = new JsonError { code = (int)RPCErrorCode.RPC_INTERNAL_ERROR, message = "An unknown error occured, while getting one of the last 10 blocks." };
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
                blockData.Add("Transaction amount", TransactionPool.getTotalTransactionsValueInBlock(block).ToString());
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
                blockData.Add("Hashrate", (Miner.getTargetHashcountPerBlock(block.difficulty) / 60).ToString());
                blockData.Add("Signature count", block.signatures.Count.ToString());
                blockData.Add("Transaction count", block.transactions.Count.ToString());
                blockData.Add("Transaction amount", TransactionPool.getTotalTransactionsValueInBlock(block).ToString());
                blockData.Add("Signatures", JsonConvert.SerializeObject(block.signatures));
                blockData.Add("TX IDs", JsonConvert.SerializeObject(block.transactions));
                blockData.Add("Transactions", JsonConvert.SerializeObject(TransactionPool.getFullBlockTransactionsAsArray(block)));
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
            IxiNumber balance = Node.walletStorage.getMyTotalBalance(Node.walletStorage.getPrimaryAddress());
            // TODO TODO TODO TODO adapt the following line for v3 wallets
            balance -= PendingTransactions.getPendingSendingTransactionsAmount(null);
            return new JsonResponse { result = balance.ToString(), error = null };
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

        public JsonResponse onTx(HttpListenerRequest request)
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

            Transaction[] transactions = TransactionPool.getLastTransactions().Skip(Int32.Parse(fromIndex)).Take(Int32.Parse(count)).ToArray();

            Dictionary<string, Dictionary<string, object>> tx_list = new Dictionary<string, Dictionary<string, object>>();

            foreach (Transaction t in transactions)
            {
                tx_list.Add(t.id, t.toDictionary());
            }

            return new JsonResponse { result = tx_list, error = error };
        }

        public JsonResponse onTxu(HttpListenerRequest request)
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

            Transaction[] transactions = TransactionPool.getUnappliedTransactions().Skip(Int32.Parse(fromIndex)).Take(Int32.Parse(count)).ToArray();

            Dictionary<string, Dictionary<string, object>> tx_list = new Dictionary<string, Dictionary<string, object>>();

            foreach (Transaction t in transactions)
            {
                tx_list.Add(t.id, t.toDictionary());
            }

            return new JsonResponse { result = tx_list, error = error };
        }

        public JsonResponse onTxa(HttpListenerRequest request)
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

            Transaction[] transactions = TransactionPool.getAppliedTransactions().Skip(Int32.Parse(fromIndex)).Take(Int32.Parse(count)).ToArray();

            Dictionary<string, Dictionary<string, object>> tx_list = new Dictionary<string, Dictionary<string, object>>();

            foreach (Transaction t in transactions)
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
                + ", Storage: " + Storage.getQueuedQueryCount() + ", Logging: " + Logging.getRemainingStatementsCount() + ", Pending Transactions: " + PendingTransactions.pendingTransactionCount());
            networkArray.Add("Node Deprecation Block Limit", Config.compileTimeBlockNumber + Config.deprecationBlockOffset);

            string dltStatus = "Active";
            if (Node.blockSync.synchronizing)
                dltStatus = "Synchronizing";

            if (Node.blockChain.getTimeSinceLastBLock() > 1800) // if no block for over 1800 seconds
            {
                dltStatus = "ErrorLongTimeNoBlock";
            }

            if (Node.blockProcessor.networkUpgraded)
            {
                dltStatus = "ErrorForkedViaUpgrade";
            }

            networkArray.Add("DLT Status", dltStatus);

            string bpStatus = "Stopped";
            if (Node.blockProcessor.operating)
                bpStatus = "Running";
            networkArray.Add("Block Processor Status", bpStatus);

            networkArray.Add("Block Height", Node.blockChain.getLastBlockNum());
            networkArray.Add("Block Version", Node.blockChain.getLastBlockVersion());
            networkArray.Add("Network Block Height", Node.getHighestKnownNetworkBlockHeight());
            networkArray.Add("Required Consensus", Node.blockChain.getRequiredConsensus());
            networkArray.Add("Wallets", Node.walletState.numWallets);
            networkArray.Add("Presences", PresenceList.getTotalPresences());
            networkArray.Add("Supply", Node.walletState.calculateTotalSupply().ToString());
            networkArray.Add("Applied TX Count", TransactionPool.getTransactionCount() - TransactionPool.getUnappliedTransactions().Count());
            networkArray.Add("Unapplied TX Count", TransactionPool.getUnappliedTransactions().Count());
            networkArray.Add("Node Type", Node.getNodeType());
            networkArray.Add("Connectable", NetworkServer.isConnectable());

            networkArray.Add("WS Checksum", Crypto.hashToString(Node.walletState.calculateWalletStateChecksum()));
            networkArray.Add("WS Delta Checksum", Crypto.hashToString(Node.walletState.calculateWalletStateChecksum(0, true)));

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

            int type = -1;
            if(request.QueryString["type"] != null)
            {
                type = Int32.Parse(request.QueryString["type"]);
            }

            List<Activity> res = null;

            if (type == -1)
            {
                res = ActivityStorage.getActivitiesBySeedHash(Node.walletStorage.getSeedHash(), Int32.Parse(fromIndex), Int32.Parse(count), true);
            }else
            {
                res = ActivityStorage.getActivitiesBySeedHashAndType(Node.walletStorage.getSeedHash(), (ActivityType) type, Int32.Parse(fromIndex), Int32.Parse(count), true);
            }

            return new JsonResponse { result = res, error = error };
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

            Address new_address = Node.walletStorage.generateNewAddress(new Address(base_address), null);
            if (new_address != null)
            {
                return new JsonResponse { result = new_address.ToString(), error = null };
            }
            else
            {
                return new JsonResponse { result = null, error = new JsonError() { code = (int)RPCErrorCode.RPC_WALLET_ERROR, message = "Error occured while generating a new address" } };
            }
        }

        private JsonResponse onCountNodeVersions(HttpListenerRequest request)
        {
            Dictionary<string, int> versions = new Dictionary<string, int>();

            lock (PresenceList.presences)
            {
                foreach (var entry in PresenceList.presences)
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

        private JsonResponse onSetBlockSelectionAlgorithm(HttpListenerRequest request)
        {
            int algo = int.Parse(request.QueryString["algorithm"]);
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
        private JsonResponse onVerifyMiningSolution(HttpListenerRequest request)
        {
            // Check that all the required query parameters are sent
            if (request.QueryString["nonce"] == null || request.QueryString["blocknum"] == null || request.QueryString["diff"] == null)
            {
                return new JsonResponse { result = null, error = new JsonError() { code = (int)RPCErrorCode.RPC_INVALID_PARAMS, message = "Missing query parameters" } };
            }

            string nonce = request.QueryString["nonce"];
            if (nonce.Length < 1 || nonce.Length > 128)
            {
                Logging.info("Received incorrect verify nonce from miner.");
                return new JsonResponse { result = null, error = new JsonError() { code = (int)RPCErrorCode.RPC_INVALID_PARAMS, message = "Invalid nonce was specified" } };
            }

            ulong blocknum = ulong.Parse(request.QueryString["blocknum"]);
            Block block = Node.blockChain.getBlock(blocknum);
            if (block == null)
            {
                Logging.info("Received incorrect verify block number from miner.");
                return new JsonResponse { result = null, error = new JsonError() { code = (int)RPCErrorCode.RPC_INVALID_PARAMS, message = "Invalid block number specified" } };
            }

            ulong blockdiff = ulong.Parse(request.QueryString["diff"]);

            byte[] solver_address = Node.walletStorage.getPrimaryAddress();
            bool verify_result = Miner.verifyNonce_v2(nonce, blocknum, solver_address, blockdiff);
            if(verify_result)
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
        private JsonResponse onSubmitMiningSolution(HttpListenerRequest request)
        {
            // Check that all the required query parameters are sent
            if (request.QueryString["nonce"] == null || request.QueryString["blocknum"] == null)
            {
                return new JsonResponse { result = null, error = new JsonError() { code = (int)RPCErrorCode.RPC_INVALID_PARAMS, message = "Missing query parameters" } };
            }

            string nonce = request.QueryString["nonce"];
            if (nonce.Length < 1 || nonce.Length > 128)
            {
                Logging.info("Received incorrect nonce from miner.");
                return new JsonResponse { result = null, error = new JsonError() { code = (int)RPCErrorCode.RPC_INVALID_PARAMS, message = "Invalid nonce was specified" } };
            }

            ulong blocknum = ulong.Parse(request.QueryString["blocknum"]);
            Block block = Node.blockChain.getBlock(blocknum);
            if (block == null)
            {
                Logging.info("Received incorrect block number from miner.");
                return new JsonResponse { result = null, error = new JsonError() { code = (int)RPCErrorCode.RPC_INVALID_PARAMS, message = "Invalid block number specified" } };
            }

            Logging.info("Received miner share: {0} #{1}", nonce, blocknum);

            byte[] solver_address = Node.walletStorage.getPrimaryAddress();
            bool verify_result = Miner.verifyNonce_v2(nonce, blocknum, solver_address, block.difficulty);
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
        private JsonResponse onGetMiningBlock(HttpListenerRequest request)
        {
            int algo = int.Parse(request.QueryString["algo"]);
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

            JsonError error = null;
            byte[] solver_address = Node.walletStorage.getPrimaryAddress();

            Dictionary<string, Object> resultArray = new Dictionary<string, Object>
            {
                { "num", block.blockNum }, // Block number
                { "ver", block.version }, // Block version
                { "dif", block.difficulty }, // Block difficulty
                { "chk", block.blockChecksum }, // Block checksum
                { "adr", solver_address } // Solver address
            };

            return new JsonResponse { result = resultArray, error = error };
        }

  
        // This is a bit hacky way to return useful error values
        // returns either Transaction or JsonResponse
        private object createTransactionHelper(HttpListenerRequest request, bool sign_transaction = true)
        {
            IxiNumber from_amount = 0;
            IxiNumber fee = CoreConfig.transactionPrice;

            string r_auto_fee = request.QueryString["autofee"];
            bool auto_fee = false;
            if (r_auto_fee != null && (r_auto_fee.ToLower() == "true" || r_auto_fee == "1"))
            {
                auto_fee = true;
            }

            string primary_address = request.QueryString["primaryAddress"];
            byte[] primary_address_bytes = null;
            if (primary_address == null)
            {
                primary_address_bytes = Node.walletStorage.getPrimaryAddress();
            }
            else
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
                        if (!Node.walletStorage.isMyAddress(single_from_address))
                        {
                            return new JsonResponse { result = null, error = new JsonError() { code = (int)RPCErrorCode.RPC_INVALID_PARAMS, message = "Invalid from address was specified" } };
                        }
                        byte[] single_from_nonce = Node.walletStorage.getAddress(single_from_address).nonce;
                        IxiNumber singleFromAmount = new IxiNumber(single_from_split[1]);
                        if (singleFromAmount < 0 || singleFromAmount == 0)
                        {
                            from_amount = 0;
                            break;
                        }
                        from_amount += singleFromAmount;
                        fromList.Add(single_from_nonce, singleFromAmount);
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
                lock (PendingTransactions.pendingTransactions)
                {
                    fromList = Node.walletStorage.generateFromList(primary_address_bytes, to_amount + fee, toList.Keys.ToList(), PendingTransactions.pendingTransactions.Select(x => (Transaction)x[0]).ToList());
                }
                adjust_amount = true;
            }

            if (fromList == null || fromList.Count == 0)
            {
                return new JsonResponse { result = null, error = new JsonError() { code = (int)RPCErrorCode.RPC_VERIFY_ERROR, message = "From list is empty" } };
            }

            Transaction transaction = new Transaction((int)Transaction.Type.Normal, fee, toList, fromList, null, pubKey, Node.getHighestKnownNetworkBlockHeight(), -1, sign_transaction);
            //Logging.info(String.Format("Intial transaction size: {0}.", transaction.getBytes().Length));
            //Logging.info(String.Format("Intial transaction set fee: {0}.", transaction.fee));
            if (adjust_amount) //true only if automatically generating from address
            {
                IxiNumber total_tx_fee = fee;
                for (int i = 0; i < 2 && transaction.fee != total_tx_fee; i++)
                {
                    total_tx_fee = transaction.fee;
                    lock (PendingTransactions.pendingTransactions)
                    {
                        fromList = Node.walletStorage.generateFromList(primary_address_bytes, to_amount + total_tx_fee, toList.Keys.ToList(), PendingTransactions.pendingTransactions.Select(x => (Transaction)x[0]).ToList());
                    }
                    if (fromList == null || fromList.Count == 0)
                    {
                        return new JsonResponse { result = null, error = new JsonError() { code = (int)RPCErrorCode.RPC_VERIFY_ERROR, message = "From list is empty" } };
                    }
                    transaction = new Transaction((int)Transaction.Type.Normal, fee, toList, fromList, null, pubKey, Node.getHighestKnownNetworkBlockHeight(), -1, sign_transaction);
                }
            }
            else if (auto_fee) // true if user specified both a valid from address and the parameter autofee=true
            {
                // fee is taken from the first specified address
                byte[] first_address = fromList.Keys.First();
                fromList[first_address] = fromList[first_address] + transaction.fee;
                if (fromList[first_address] > Node.walletState.getWalletBalance((new Address(transaction.pubKey, first_address)).address))
                {
                    return new JsonResponse { result = null, error = new JsonError() { code = (int)RPCErrorCode.RPC_WALLET_INSUFFICIENT_FUNDS, message = "Balance is too low" } };
                }
                transaction = new Transaction((int)Transaction.Type.Normal, fee, toList, fromList, null, pubKey, Node.getHighestKnownNetworkBlockHeight(), -1, sign_transaction);
            }
            //Logging.info(String.Format("Transaction size after automatic adjustments: {0}.", transaction.getBytes().Length));
            //Logging.info(String.Format("Transaction fee after automatic adjustments: {0}.", transaction.fee));
            // verify that all "from amounts" match all "to_amounts" and that the fee is included in "from_amounts"
            // we need to recalculate "from_amount"
            from_amount = fromList.Aggregate(new IxiNumber(), (sum, next) => sum + next.Value, sum => sum);
            if (from_amount != (to_amount + transaction.fee))
            {
                return new JsonResponse { result = null, error = new JsonError() { code = (int)RPCErrorCode.RPC_TRANSACTION_ERROR, message = "From amounts (incl. fee) do not match to amounts. If you haven't accounted for the transaction fee in the from amounts, use the parameter 'autofee' to have the node do it automatically." } };
            }
            if (to_amount + transaction.fee > Node.walletStorage.getMyTotalBalance(primary_address_bytes))
            {
                return new JsonResponse { result = null, error = new JsonError() { code = (int)RPCErrorCode.RPC_WALLET_INSUFFICIENT_FUNDS, message = "Balance is too low" } };
            }

            // the transaction appears valid
            return transaction;
        }

        // Returns an empty PoW block based on the search algorithm provided as a parameter
        private JsonResponse onGetWalletBackup(HttpListenerRequest request)
        {
            List<byte> wallet = new List<byte>();
            wallet.AddRange(Node.walletStorage.getRawWallet());
            return new JsonResponse { result = "IXIHEX" + Crypto.hashToString(wallet.ToArray()), error = null };
        }
    }
}