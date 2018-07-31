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
                    catch(Exception)
                    {
                        sendError(context, "{\"message\":\"error\"}");
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
                string responseString = "Incorrect transaction parameters";

                string to = request.QueryString["to"];
                string amount_string = request.QueryString["amount"];
                ulong amount = 0;

                try
                {
                    amount = Convert.ToUInt64(amount_string);
                }
                catch (Exception e)
                {
                    amount = 0;
                    Logging.warn(string.Format("Exception on addtransaction API request: {0}", e.ToString()));
                }

                // Only create a transaction if there is a valid amount
                if(amount > 0)
                {
                    string from = Node.walletStorage.address;
                    Transaction transaction = new Transaction(amount, to, from);
                    TransactionPool.addTransaction(transaction);
                    responseString = JsonConvert.SerializeObject(transaction);
                }


                // Respond with the transaction details
                sendResponse(context.Response, responseString);

                return true;
            }

            if (methodName.Equals("getbalance", StringComparison.OrdinalIgnoreCase))
            {
                string address = request.QueryString["address"];

                ulong balance = WalletState.getBalanceForAddress(address);

                // Respond with the transaction details
                string responseString = JsonConvert.SerializeObject(balance);
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

                string[][] blockData = new string[10][];

                blockData[0] = new string[2] { "Block Number", block.blockNum.ToString() };
                blockData[1] = new string[2] { "Block Checksum", block.blockChecksum };
                blockData[2] = new string[2] { "Last Block Checksum", block.lastBlockChecksum };
                blockData[3] = new string[2] { "Wallet State Checksum", block.walletStateChecksum };

                // Respond with the block details
                string responseString = JsonConvert.SerializeObject(blockData);
                sendResponse(context.Response, responseString);
                return true;
            }

            if (methodName.Equals("test1", StringComparison.OrdinalIgnoreCase))
            {
                // Used for performing various tests.
                
                string responseString = JsonConvert.SerializeObject("Test 1 complete");
                sendResponse(context.Response, responseString);
                return true;
            }

            if (methodName.Equals("stats", StringComparison.OrdinalIgnoreCase))
            {
                // Show performance counters and statistics
                string[] statArray = new String[2];
                statArray[0] = "DLT";
                statArray[1] = "Active";
                if (Node.blockProcessor.synchronizing)
                    statArray[1] = "Synchronizing";

                string responseString = JsonConvert.SerializeObject(statArray);
                sendResponse(context.Response, responseString);
                return true;
            }


            if (methodName.Equals("mywallet", StringComparison.OrdinalIgnoreCase))
            {
                // Show own address, balance and blockchain synchronization status
                string address = Node.walletStorage.getWalletAddress();
                ulong balance = WalletState.getDeltaBalanceForAddress(address);
                string sync_status = "ready";
                if (Node.blockProcessor.synchronizing)
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
                // Show a list of wallets - capped to 10
                lock (WalletState.wallets)
                {
                    string[][] walletStates = new string[WalletState.wallets.Count][];
                    int count = 0;
                    foreach (Wallet w in WalletState.wallets)
                    {
                        walletStates[count] = new string[2];
                        walletStates[count][0] = w.id;
                        walletStates[count][1] = w.balance.ToString();
                        count++;
                        if (count >= 10) break;
                    }

                    string responseString = JsonConvert.SerializeObject(walletStates);
                    sendResponse(context.Response, responseString);
                }
                return true;
            }

            if (methodName.Equals("pl", StringComparison.OrdinalIgnoreCase))
            {
                // Show a list of presences
                lock (PresenceList.presences)
                {
                    var json = PresenceList.presences;
                    string responseString = JsonConvert.SerializeObject(json, Formatting.None, new JsonSerializerSettings { NullValueHandling = NullValueHandling.Ignore, Formatting = Formatting.Indented });
                    sendResponse(context.Response, responseString);
                }
                return true;
            }

            if (methodName.Equals("clients", StringComparison.OrdinalIgnoreCase))
            {
                String[] res = NetworkServer.getConnectedClients();
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
                    formattedTransactions[count] = new string[3];
                    formattedTransactions[count][0] = t.id;
                    formattedTransactions[count][1] = string.Format("{0}", t.amount);
                    formattedTransactions[count][2] = t.timeStamp;

                    count++;
                }

                string responseString = JsonConvert.SerializeObject(formattedTransactions);
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
                Console.WriteLine("HTTP API: {0}", e.ToString());
            }
        }

    }
}
