using DLT;
using DLT.Meta;
using DLT.Network;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DLTNode
{
    class APIServer
    {
        public HttpListener listener;
        private Thread apiControllerThread;
        private bool continueRunning;
        private string listenURL;

        public bool forceShutdown = false;

        public APIServer()
        {
            start();
        }

        public void start()
        {
            continueRunning = true;

            apiControllerThread = new Thread(apiLoop);
            apiControllerThread.Start();
        }

        public void stop()
        {
            continueRunning = false;
            try
            {
                // Stop the listener
                listener.Stop();
            }
            catch(Exception)
            {
                Logging.info("API server already stopped.");
            }
        }


        private void apiLoop()
        {
            // Prepare the listen url string
            listenURL = String.Format("http://localhost:{0}/", Config.apiPort);

            // Start a listener on the loopback interface
            listener = new HttpListener();
            try
            {
                listener.Prefixes.Add(listenURL);
                listener.Start();
            }
            catch(Exception ex)
            {
                Logging.error("Cannot initialize API server! The error was: " + ex.Message);
                return;
            }

            while (continueRunning)
            {
                try
                {
                    Console.Write("*");

                    HttpListenerContext context = listener.GetContext();

                    if (context.Request.Url.Segments.Length < 2)
                    {
                        //sendError(context, "{\"message\":\"no parameters supplied\"}");

                        // We will now show an embedded wallet if the API is called with no parameters
                        sendWallet(context);

                        continue;
                    }
                    string methodName = context.Request.Url.Segments[1].Replace("/", "");

                    if (methodName == null)
                    {
                        sendError(context, "{\"message\":\"invalid parameters\"}");
                        continue;
                    }

                    try
                    {
                        if (parseRequest(context, methodName) == false)
                        {
                            sendError(context, "{\"message\":\"error\"}");
                        }
                    }
                    catch(Exception e)
                    {
                        sendError(context, "{\"message\":\"error\"}");
                        Logging.error(string.Format("Error in API server {0}", e.ToString()));

                    }
                }
                catch(Exception)
                {
                    continueRunning = false;
                    //Logging.error(string.Format("Error in API server {0}", e.ToString()));
                }
            }

            // Stop the listener
            //listener.Stop();

            Thread.Yield();
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

        private void sendError(HttpListenerContext context, string errorString)
        {
            sendResponse(context.Response, errorString);
        }

        private bool parseRequest(HttpListenerContext context, string methodName)
        {
            HttpListenerRequest request = context.Request;
            // Set the content type to plain to prevent xml parsing errors in various browsers
            context.Response.ContentType = "application/json";


            if (methodName.Equals("shutdown", StringComparison.OrdinalIgnoreCase))
            {
                string responseString = JsonConvert.SerializeObject("Node shutdown");
                sendResponse(context.Response, responseString);

                forceShutdown = true;

                return true;
            }

            if (methodName.Equals("reconnect", StringComparison.OrdinalIgnoreCase))
            {
                string responseString = JsonConvert.SerializeObject("Reconnecting node to network now.");
                sendResponse(context.Response, responseString);

                Node.reconnect();

                return true;
            }

            if (methodName.Equals("connect", StringComparison.OrdinalIgnoreCase))
            {
                string to = request.QueryString["to"];

                NetworkClientManager.connectTo(to);

                string responseString = JsonConvert.SerializeObject(string.Format("Connecting to node {0}", to));
                sendResponse(context.Response, responseString);

                return true;
            }

            if (methodName.Equals("isolate", StringComparison.OrdinalIgnoreCase))
            {
                string responseString = JsonConvert.SerializeObject("Isolating from network now.");
                sendResponse(context.Response, responseString);

                Node.isolate();

                return true;
            }



            if (methodName.Equals("sync", StringComparison.OrdinalIgnoreCase))
            {
                string responseString = JsonConvert.SerializeObject("Synchronizing to network now.");
                sendResponse(context.Response, responseString);

                Node.synchronize();

                return true;
            }

            if (methodName.Equals("addtransaction", StringComparison.OrdinalIgnoreCase))
            {
                // Add a new transaction. This test allows sending and receiving from arbitrary addresses
                string responseString = "Incorrect transaction parameters.";

                string to = request.QueryString["to"];
                string amount_string = request.QueryString["amount"];
                IxiNumber amount = new IxiNumber(amount_string) - Config.transactionPrice; // Subtract the fee
                IxiNumber fee = Config.transactionPrice;

                // Only create a transaction if there is a valid amount
                if(amount > (long)0)
                {
                    string from = Node.walletStorage.address;

                    TransactionPool.internalNonce++;
                    ulong nonce = TransactionPool.internalNonce;

                    if (!Address.validateChecksum(to))
                    {
                        responseString = "Incorrect to address.";
                    }
                    else
                    {
                        Transaction transaction = new Transaction(amount, fee, to, from, nonce);
                        if (TransactionPool.addTransaction(transaction))
                        {
                            responseString = JsonConvert.SerializeObject(transaction);
                        }
                        else
                        {
                            responseString = "There was an error adding the transaction.";
                        }
                    }
                }

                // Respond with the transaction details
                sendResponse(context.Response, responseString);

                return true;
            }

            if (methodName.Equals("getbalance", StringComparison.OrdinalIgnoreCase))
            {
                string address = request.QueryString["address"];

                IxiNumber balance = Node.walletState.getWalletBalance(address);

                // Respond with the transaction details
                string responseString = JsonConvert.SerializeObject(balance.ToString());
                sendResponse(context.Response, responseString);

                return true;
            }

            if (methodName.Equals("getblock", StringComparison.OrdinalIgnoreCase))
            {
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

                Block block = Node.blockChain.getBlock(block_num);
                if (block == null)
                    return false;

                Dictionary<string, string> blockData = new Dictionary<string, string>();

                blockData.Add("Block Number", block.blockNum.ToString());
                blockData.Add("Block Checksum", block.blockChecksum);
                blockData.Add("Last Block Checksum", block.lastBlockChecksum);
                blockData.Add("Wallet State Checksum", block.walletStateChecksum);
                blockData.Add("Sig freeze Checksum", block.signatureFreezeChecksum);
                blockData.Add("PoW field", block.powField);
                blockData.Add("Difficulty", block.difficulty.ToString());
                blockData.Add("Signature count", block.signatures.Count.ToString());
                blockData.Add("Transaction count", block.transactions.Count.ToString());
                blockData.Add("Transaction amount", block.getTotalTransactionsValue().ToString());
                blockData.Add("Signatures", JsonConvert.SerializeObject(block.signatures));
                blockData.Add("TX IDs", JsonConvert.SerializeObject(block.transactions));

                // Respond with the block details
                string responseString = JsonConvert.SerializeObject(blockData);
                sendResponse(context.Response, responseString);
                return true;
            }

            if (methodName.Equals("getlastblocks", StringComparison.OrdinalIgnoreCase))
            {
                Dictionary<string, string>[] blocks = new Dictionary<string, string>[10];
                int blockCnt = Node.blockChain.Count > 10 ? 10 : Node.blockChain.Count;
                for (ulong i = 0; i < (ulong)blockCnt; i++)
                {
                    Block block = Node.blockChain.getBlock(Node.blockChain.getLastBlockNum() - i);
                    if (block == null)
                        return false;

                    Dictionary<string, string> blockData = new Dictionary<string, string>();

                    blockData.Add("Block Number", block.blockNum.ToString());
                    blockData.Add("Block Checksum", block.blockChecksum);
                    blockData.Add("Last Block Checksum", block.lastBlockChecksum);
                    blockData.Add("Wallet State Checksum", block.walletStateChecksum);
                    blockData.Add("Sig freeze Checksum", block.signatureFreezeChecksum);
                    blockData.Add("PoW field", block.powField);
                    blockData.Add("Difficulty", block.difficulty.ToString());
                    blockData.Add("Signature count", block.signatures.Count.ToString());
                    blockData.Add("Transaction count", block.transactions.Count.ToString());
                    blockData.Add("Transaction amount", block.getTotalTransactionsValue().ToString());
                    blockData.Add("Signatures", JsonConvert.SerializeObject(block.signatures));
                    blockData.Add("TX IDs", JsonConvert.SerializeObject(block.transactions));

                    blocks[i] = blockData;
                }
                // Respond with the block details
                string responseString = JsonConvert.SerializeObject(blocks).ToString();
                sendResponse(context.Response, responseString);
                return true;
            }

            if (methodName.Equals("getfullblock", StringComparison.OrdinalIgnoreCase))
            {
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

                Block block = Node.blockChain.getBlock(block_num);
                if (block == null)
                    return false;

                Dictionary<string, string> blockData = new Dictionary<string, string>();

                blockData.Add("Block Number", block.blockNum.ToString());
                blockData.Add("Block Checksum", block.blockChecksum);
                blockData.Add("Last Block Checksum", block.lastBlockChecksum);
                blockData.Add("Wallet State Checksum", block.walletStateChecksum);
                blockData.Add("Sig freeze Checksum", block.signatureFreezeChecksum);
                blockData.Add("PoW field", block.powField);
                blockData.Add("Difficulty", block.difficulty.ToString());
                blockData.Add("Signature count", block.signatures.Count.ToString());
                blockData.Add("Transaction count", block.transactions.Count.ToString());
                blockData.Add("Transaction amount", block.getTotalTransactionsValue().ToString());
                blockData.Add("Signatures", JsonConvert.SerializeObject(block.signatures));
                blockData.Add("TX IDs", JsonConvert.SerializeObject(block.transactions));
                blockData.Add("Transactions", JsonConvert.SerializeObject(block.getFullTransactionsAsArray()));

                // Respond with the block details
                string responseString = JsonConvert.SerializeObject(blockData);
                sendResponse(context.Response, responseString);
                return true;
            }

            if (methodName.Equals("stress", StringComparison.OrdinalIgnoreCase))
            {
                string type_string = request.QueryString["type"];
                if(type_string == null)
                {
                    type_string = "txspam";
                }

                int txnum = 0;
                string txnumstr = request.QueryString["num"];
                if(txnumstr != null)
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

                string responseString = JsonConvert.SerializeObject("Stress test");
                sendResponse(context.Response, responseString);
                return true;
            }

            if (methodName.Equals("mywallet", StringComparison.OrdinalIgnoreCase))
            {
                // Show own address, balance and blockchain synchronization status
                string address = Node.walletStorage.getWalletAddress();
                IxiNumber balance = Node.walletState.getWalletBalance(address);
                string sync_status = "ready";
                if (Node.blockSync.synchronizing)
                    sync_status = "sync";

                string[] statArray = new String[3];
                statArray[0] = address;
                statArray[1] = balance.ToString();
                statArray[2] = sync_status;

                string responseString = JsonConvert.SerializeObject(statArray);
                sendResponse(context.Response, responseString);
                return true;
            }

            if (methodName.Equals("walletlist", StringComparison.OrdinalIgnoreCase))
            {
                // Show a list of wallets - capped to 50
                Wallet[] wallets = Node.walletState.debugGetWallets();
                string[][] walletStates = new string[wallets.Length][];
                int count = 0;
                foreach (Wallet w in wallets)
                {
                    walletStates[count] = new string[3];
                    walletStates[count][0] = w.id;
                    walletStates[count][1] = w.balance.ToString();
                    walletStates[count][2] = w.nonce.ToString();
                    count++;
                }

                string responseString = JsonConvert.SerializeObject(walletStates);
                sendResponse(context.Response, responseString);
                return true;
            }

            if (methodName.Equals("pl", StringComparison.OrdinalIgnoreCase))
            {
                string responseString = "None";

                // Show a list of presences
                lock (PresenceList.presences)
                {
                    var json = PresenceList.presences;
                    responseString = JsonConvert.SerializeObject(json, Formatting.None, new JsonSerializerSettings { NullValueHandling = NullValueHandling.Ignore, Formatting = Formatting.Indented });
                }

                sendResponse(context.Response, responseString);

                return true;
            }

            if (methodName.Equals("clients", StringComparison.OrdinalIgnoreCase))
            {
                String[] res = NetworkServer.getConnectedClients();
                string responseString = JsonConvert.SerializeObject(res);
                sendResponse(context.Response, responseString);
                return true;
            }

            if (methodName.Equals("servers", StringComparison.OrdinalIgnoreCase))
            {
                String[] res = NetworkClientManager.getConnectedClients();
                string responseString = JsonConvert.SerializeObject(res);
                sendResponse(context.Response, responseString);
                return true;
            }

            if (methodName.Equals("tx", StringComparison.OrdinalIgnoreCase))
            {
                // Show a list of transactions
                Transaction[] transactions = TransactionPool.getAllTransactions();
                string[][] formattedTransactions = new string[transactions.Length][];
                int count = 0;
                foreach (Transaction t in transactions)
                {
                    formattedTransactions[count] = new string[4];
                    formattedTransactions[count][0] = t.id;
                    formattedTransactions[count][1] = string.Format("{0}", t.amount);
                    formattedTransactions[count][2] = t.timeStamp;
                    formattedTransactions[count][3] = t.applied.ToString();

                    count++;
                }

                string responseString = JsonConvert.SerializeObject(formattedTransactions);
                sendResponse(context.Response, responseString);

                return true;
            }

            if (methodName.Equals("txu", StringComparison.OrdinalIgnoreCase))
            {
                // Show a list of unapplied transactions
                Transaction[] transactions = TransactionPool.getUnappliedTransactions();
                string[][] formattedTransactions = new string[transactions.Length][];
                int count = 0;
                foreach (Transaction t in transactions)
                {
                    formattedTransactions[count] = new string[4];
                    formattedTransactions[count][0] = t.id;
                    formattedTransactions[count][1] = string.Format("{0}", t.amount);
                    formattedTransactions[count][2] = t.timeStamp;
                    formattedTransactions[count][3] = t.applied.ToString();

                    count++;
                }

                string responseString = JsonConvert.SerializeObject(formattedTransactions);
                sendResponse(context.Response, responseString);

                return true;
            }

            if (methodName.Equals("status", StringComparison.OrdinalIgnoreCase))
            {
                Dictionary<string, Object> networkArray = new Dictionary<string, Object>();

                networkArray.Add("Node Version", Config.version);
                networkArray.Add("My External IP", Config.publicServerIP);
                networkArray.Add("Listening interface", context.Request.RemoteEndPoint.Address.ToString());
                networkArray.Add("Receive Network Queue", NetworkQueue.getQueuedMessageCount());
                networkArray.Add("Receive Network Tx Queue", NetworkQueue.getTxQueuedMessageCount());
                networkArray.Add("Send Network Queue (clients)", NetworkServer.getQueuedMessageCount());
                networkArray.Add("Send Network Queue (servers)", NetworkClientManager.getQueuedMessageCount());
                networkArray.Add("Node Deprecation Block Limit", Config.compileTimeBlockNumber + Config.deprecationBlockOffset);

                string dltStatus = "Active";
                if (Node.blockSync.synchronizing)
                    dltStatus = "Synchronizing";
                networkArray.Add("DLT Status", dltStatus);

                string bpStatus = "Stopped";
                if (Node.blockProcessor.operating)
                    bpStatus = "Running";
                networkArray.Add("Block Processor Status", bpStatus);

                networkArray.Add("Network Clients", NetworkServer.getConnectedClients());
                networkArray.Add("Network Servers", NetworkClientManager.getConnectedClients());

                networkArray.Add("Block Height", Node.blockChain.getLastBlockNum());
                networkArray.Add("WS Checksum", Node.walletState.calculateWalletStateChecksum());
                networkArray.Add("WS Delta Checksum", Node.walletState.calculateWalletStateChecksum(true));
                networkArray.Add("Wallets", Node.walletState.numWallets);
                networkArray.Add("Presences", PresenceList.getTotalPresences());
                networkArray.Add("Supply", Node.walletState.calculateTotalSupply().ToString());
                networkArray.Add("TX Count", TransactionPool.getAllTransactions().Count());
                networkArray.Add("Unapplied TX Count", TransactionPool.getUnappliedTransactions().Count());

                string responseString = JsonConvert.SerializeObject(networkArray);
                sendResponse(context.Response, responseString);
                return true;
            }

            if (methodName.Equals("blockheight", StringComparison.OrdinalIgnoreCase))
            {
                ulong blockheight = Node.blockChain.getLastBlockNum();
                string responseString = JsonConvert.SerializeObject(blockheight);
                sendResponse(context.Response, responseString);
                return true;
            }
            
            if (methodName.Equals("myip", StringComparison.OrdinalIgnoreCase))
            {
                string clientIp = context.Request.RemoteEndPoint.Address.ToString();
                string responseString = clientIp;
                sendResponse(context.Response, responseString);
                return true;
            }

            if (methodName.Equals("chkaddress", StringComparison.OrdinalIgnoreCase))
            {
                string address = request.QueryString["address"];

                string chkaddress = Address.generateChecksumAddress(address);

                // Respond with the transaction details
                string responseString = JsonConvert.SerializeObject(chkaddress);
                sendResponse(context.Response, responseString);

                return true;
            }

            if (methodName.Equals("supply", StringComparison.OrdinalIgnoreCase))
            {
                string supply = Node.walletState.calculateTotalSupply().ToString();
                string responseString = JsonConvert.SerializeObject(supply);
                sendResponse(context.Response, responseString);
                return true;
            }


            return false;
        }

        private void sendResponse(HttpListenerResponse responseObject, string responseString)
        {
            byte[] buffer = System.Text.Encoding.UTF8.GetBytes(responseString);

            try
            {
                responseObject.ContentLength64 = buffer.Length;
                System.IO.Stream output = responseObject.OutputStream;
                output.Write(buffer, 0, buffer.Length);
                output.Close();
            }
            catch(Exception e)
            {
                Logging.error(String.Format("HTTP API: {0}", e.ToString()));
            }
        }

    }
}
