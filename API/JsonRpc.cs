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
using System.Text;

namespace DLTNode.API
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

    class JsonRpc
    {

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
                Logging.error(String.Format("APIServer: {0}", e.ToString()));
            }
        }

        public JsonResponse processRequest(HttpListenerContext context)
        {
            string  body = new StreamReader(context.Request.InputStream).ReadToEnd();
            JsonRpcRequest requestObject = JsonConvert.DeserializeObject<JsonRpcRequest>(body);

            JsonResponse response = new JsonResponse();

            switch(requestObject.method)
            {
                case "addnode":
                    response = addnode((Dictionary<string, string>)requestObject.@params);
                    break;

                case "backupwallet":
                    response = backupwallet((string)requestObject.@params);
                    break;

                case "createrawtransaction":
                    response = createrawtransaction((Dictionary<int, object>)requestObject.@params);
                    break;

                case "decoderawtransaction":
                    response = decoderawtransaction((string)requestObject.@params);
                    break;

                case "getbalance":
                    response = getbalance();
                    break;

                case "getbestblockhash":
                    response = getbestblockhash();
                    break;

                case "getblock":
                    response = getblock(Crypto.stringToHash((string)requestObject.@params));
                    break;

                case "getblockcount":
                    response = getblockcount();
                    break;

                case "getblockhash":
                    response = getblockhash((ulong)requestObject.@params);
                    break;

                case "getconnectioncount":
                    response = getconnectioncount();
                    break;

                case "getdifficulty":
                    response = getdifficulty();
                    break;

                case "getinfo":
                    response = getinfo();
                    break;

                case "getnewaddress":
                    response = getnewaddress();
                    break;

                    
            }

            response.id = requestObject.id;
            return response;
        }

        public void addmultisigaddress()
        {

        }

        public JsonResponse addnode(Dictionary<string, string> parameters)
        {
            string type = (string)parameters["type"];
            string address = (string)parameters["address"];

            JsonError rpcError = null;

            if (type == "add" || type == "onetry")
            {
                if(!PeerStorage.addPeerToPeerList(address, null))
                {
                    rpcError = new JsonError { code = (int)RPCErrorCode.RPC_CLIENT_NODE_ALREADY_ADDED, message = "Error: Node already added." };
                }
            }
            else if(type == "remove")
            {
                if(!PeerStorage.removePeer(address))
                {
                    rpcError = new JsonError { code = (int)RPCErrorCode.RPC_CLIENT_NODE_NOT_ADDED, message = "Error: Node has not been added." };
                }
            }
            return new JsonResponse { result = null, error = rpcError };
        }

        public JsonResponse backupwallet(string parameters)
        {
            string destination = parameters;

            JsonError rpcError = null;

            if (!Node.walletStorage.backup(destination))
            {
                rpcError = new JsonError { code = (int)RPCErrorCode.RPC_WALLET_ERROR, message = "Error: Wallet backup failed!" };
            }
            return new JsonResponse { result = null, error = rpcError };
        }

        public void createmultisig(Dictionary<object, object> parameters)
        {

        }

        public JsonResponse createrawtransaction(Dictionary<int, object> parameters)
        {
            Dictionary<string, string> walletAmountArr = (Dictionary<string, string>)parameters[1];

            SortedDictionary<byte[], IxiNumber> toList = new SortedDictionary<byte[], IxiNumber>(new ByteArrayComparer());

            foreach (KeyValuePair<string, string> walletAmount in walletAmountArr)
            {
                IxiNumber amount = new IxiNumber(walletAmount.Value);
                toList.Add(Base58Check.Base58CheckEncoding.DecodePlain(walletAmount.Key), amount);
            }

            byte[] pubKey = null;
            if (Node.walletState.getWallet(Node.walletStorage.address).publicKey == null)
            {
                pubKey = Node.walletStorage.publicKey;
            }

            Transaction t = new Transaction((int)Transaction.Type.Normal, CoreConfig.transactionPrice, toList, Node.walletStorage.address, null, pubKey, Node.blockChain.getLastBlockNum());
            t.signature = null;

            return new JsonResponse { result = Crypto.hashToString(t.getBytes()), error = null };
        }

        public JsonResponse decoderawtransaction(string hexString)
        {
            Transaction t = new Transaction(Crypto.stringToHash(hexString));

            Dictionary<string, object> tDic = new Dictionary<string, object>();

            tDic.Add("txid", t.id);
            tDic.Add("hash", t.checksum);
            tDic.Add("size", t.getBytes().Length);
            tDic.Add("vsize", t.getBytes().Length);
            tDic.Add("version", t.version);
            tDic.Add("vin", t.from);

            Dictionary<string, object> vout = new Dictionary<string, object>();

            Dictionary<string, string> addresses = new Dictionary<string, string>();

            foreach(var to in t.toList)
            {
                addresses.Add(Base58Check.Base58CheckEncoding.EncodePlain(to.Key), to.Value.ToString());
            }

            vout.Add("value", t.amount);


            vout.Add("addresses", addresses);

            tDic.Add("vout", vout);


            return new JsonResponse { result = JsonConvert.SerializeObject(tDic), error = null };
        }

        public JsonResponse getbalance()
        {
            return new JsonResponse { result = Node.walletState.getWalletBalance(Node.walletStorage.address).ToString(), error = null };
        }

        public JsonResponse getbestblockhash()
        {
            return new JsonResponse { result = Node.blockChain.getLastBlockChecksum(), error = null };
        }

        public JsonResponse getblock(byte[] hash)
        {
            Block b = Node.blockChain.getBlockByHash(hash, true);

            Dictionary<string, object> bDic = new Dictionary<string, object>();
            bDic.Add("hash", b.blockChecksum);
            bDic.Add("confirmations", b.signatures.Count);
            bDic.Add("size", b.getBytes().Count());
            bDic.Add("strippedsize", b.getBytes().Count());
            bDic.Add("height", b.blockNum);
            bDic.Add("version", b.version);
            bDic.Add("versionHex", Crypto.hashToString(BitConverter.GetBytes(b.version)));

            bDic.Add("tx", b.transactions);

            bDic.Add("time", b.timestamp);
            bDic.Add("mediantime", b.timestamp);
            bDic.Add("difficulty", b.difficulty);
            bDic.Add("previousblockhash", b.lastBlockChecksum);
            if(Node.blockChain.getLastBlockNum() > b.blockNum)
            {
                bDic.Add("nextblockhash", Node.blockChain.getBlock(b.blockNum + 1, true).blockChecksum);
            }

            return new JsonResponse { result = bDic, error = null };
        }

        public JsonResponse getblockcount()
        {
            return new JsonResponse { result = Node.blockChain.getLastBlockNum(), error = null };
        }

        public JsonResponse getblockhash(ulong blockHeight)
        {
            return new JsonResponse { result = Node.blockChain.getBlock(blockHeight, true), error = null };
        }

        public JsonResponse getconnectioncount()
        {
            return new JsonResponse { result = NetworkClientManager.getConnectedClients().Count() + NetworkServer.getConnectedClients().Count(), error = null };
        }

        public JsonResponse getdifficulty()
        {
            return new JsonResponse { result = Node.blockChain.getBlock(Node.blockChain.getLastBlockNum(), true).difficulty, error = null };
        }

        public JsonResponse getinfo()
        {
            Dictionary<string, object> iDic = new Dictionary<string, object>();
            iDic.Add("version", Config.version);
            iDic.Add("protocolversion", CoreConfig.protocolVersion);
            iDic.Add("walletversion", Node.walletState.version);
            iDic.Add("balance", Node.walletState.getWalletBalance(Node.walletStorage.address).ToString());
            iDic.Add("blocks", Node.blockChain.getLastBlockNum());
            iDic.Add("timeoffset", Core.networkTimeDifference);
            iDic.Add("connections", NetworkClientManager.getConnectedClients().Count() + NetworkServer.getConnectedClients().Count());

            iDic.Add("proxy", "");

            iDic.Add("difficulty", Node.blockChain.getBlock(Node.blockChain.getLastBlockNum()).difficulty);
            iDic.Add("testnet", Config.isTestNet);
            iDic.Add("keypoololdest", 0);
            iDic.Add("keypoolsize", 0);
            iDic.Add("paytxfee", CoreConfig.transactionPrice);
            iDic.Add("relayfee", CoreConfig.relayPriceInitial);
            iDic.Add("unlocked_until", "");
            iDic.Add("errors", "");

            return new JsonResponse { result = iDic, error = null };
        }
        
        public JsonResponse getmininginfo()
        {
            Dictionary<string, object> iDic = new Dictionary<string, object>();
            iDic.Add("blocks", Node.blockChain.getLastBlockNum());
            iDic.Add("currentblocksize", 0);
            iDic.Add("currentblocktx", 0);
            iDic.Add("difficulty", Node.blockChain.getBlock(Node.blockChain.getLastBlockNum()).difficulty);
            iDic.Add("errors", "");
            iDic.Add("pooledtx", TransactionPool.getTransactionCount());
            iDic.Add("hashespersec", Node.miner.lastHashRate);

            return new JsonResponse { result = iDic, error = null };
        }

        public JsonResponse getnewaddress()
        {

            byte[] address = Node.walletStorage.generateNewAddress();

            return new JsonResponse { result = Base58Check.Base58CheckEncoding.EncodePlain(address), error = null };
        }

        public JsonResponse gettransaction(string txid)
        {
            Transaction t = TransactionPool.getTransaction(txid, true);
            Dictionary<string, object> iDic = new Dictionary<string, object>();
            iDic.Add("amount", t.amount);
            iDic.Add("fee", t.fee);
            iDic.Add("confirmations", t.applied != 0 ? 1 : 0);
            if (t.applied != 0)
            {
                Block b = Node.blockChain.getBlock(t.applied);
                iDic.Add("blockhash", b.blockChecksum);
                iDic.Add("blockindex", b.blockNum);
                iDic.Add("blocktime", b.timestamp);
            }
            iDic.Add("txid", t.id);
            iDic.Add("time", t.timeStamp);
            iDic.Add("timereceived", t.timeStamp);
            iDic.Add("comment", t.data);
            iDic.Add("from", t.from);

            Dictionary<string, string> toList = new Dictionary<string, string>();

            foreach (var entry in t.toList)
            {
                toList.Add(Base58Check.Base58CheckEncoding.EncodePlain(entry.Key), entry.Value.ToString());
            }


            iDic.Add("to", toList);

            return new JsonResponse { result = iDic, error = null };
        }

        public void help()
        {

        }

        public void keypoolrefill()
        {

        }

        public void listsinceblock()
        {

        }

        public void listtransactions()
        {

        }

        public void listunspent()
        {

        }

        public void listlockunspent()
        {

        }

        public void lockunspent()
        {

        }

        public void sendfrom()
        {

        }

        public void sendmany()
        {

        }

        public void sendrawtransaction()
        {

        }

        public void sendtoaddress()
        {

        }

        public void setaccount()
        {

        }

        public void setgenerate()
        {

        }

        public void settxfee()
        {

        }

        public void signmessage()
        {

        }

        public void signrawtransaction()
        {

        }

        public void stop()
        {

        }

        public void submitblock()
        {

        }

        public void validateaddress()
        {

        }

        public void verifymessage()
        {

        }

        public void walletlock()
        {

        }

        public void walletpassphrase()
        {

        }

        public void walletpassphrasechange()
        {

        }
    }
}
