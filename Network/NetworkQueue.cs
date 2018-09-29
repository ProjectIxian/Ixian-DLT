using DLT;
using DLT.Meta;
using DLT.Network;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DLT
{
    namespace Network
    {
        struct QueueMessage
        {
            public ProtocolMessageCode code;
            public byte[] data;
            public Socket skipSocket;
        }

        struct QueueMessageRaw
        {
            public byte[] data;
            public Socket socket;
            public RemoteEndpoint endpoint;
        }

        class NetworkQueue
        {
            private static bool shouldStop = false; // flag to signal shutdown of threads

            // Internal queue message entity with socket and remoteendpoint support
            struct QueueMessageRecv
            {
                public ProtocolMessageCode code;
                public byte[] data;
                public Socket socket;
                public RemoteEndpoint endpoint;
            }

            // Maintain a queue of messages to receive
            private static List<QueueMessageRecv> queueMessages = new List<QueueMessageRecv>();


            public static int getQueuedMessageCount()
            {
                lock (queueMessages)
                {
                    return queueMessages.Count;
                }
            }


            public static void receiveProtocolMessage(ProtocolMessageCode code, byte[] data, Socket socket, RemoteEndpoint endpoint)
            {
                QueueMessageRecv message = new QueueMessageRecv
                {
                    code = code,
                    data = data,
                    socket = socket,
                    endpoint = endpoint
                };

                lock (queueMessages)
                {

                    if (code == ProtocolMessageCode.newTransaction || code == ProtocolMessageCode.transactionData || code == ProtocolMessageCode.newBlock || code == ProtocolMessageCode.blockData)
                    {
                        if (queueMessages.Exists(x => x.code == message.code && message.data.SequenceEqual(x.data) /*&& x.socket == message.socket && x.endpoint == message.endpoint*/))
                        {
                            //Logging.warn(string.Format("Attempting to add a duplicate message (code: {0}) to the network queue", code));
                            return;
                        }
                    }

                    if (queueMessages.Count() > 20)
                    {
                        if (code != ProtocolMessageCode.newTransaction)
                        {
                            queueMessages.Insert(5, message);
                            return;
                        }
                    }
                    
                    // Add it to the queue
                    queueMessages.Add(message);
                    
                }
            }


            // Start the network queue
            public static void start()
            {
                shouldStop = false;
                queueMessages.Clear();

                // Multi-threaded network queue parsing
                for (int i = 0; i < 1; i++)
                {
                    Thread queue_thread = new Thread(queueThreadLoop);
                    queue_thread.Start();
                }
                
                Logging.info("Network queue thread started.");
            }

            // Signals all the queue threads to stop
            public static bool stop()
            {
                shouldStop = true;
                return true;
            }

            // Actual network queue logic
            public static void queueThreadLoop()
            {
                // Prepare an special message object to use while receiving and parsing, without locking up the queue messages
                QueueMessageRecv active_message = new QueueMessageRecv();

                while (!shouldStop)
                {
                    bool message_found = false;
                    lock(queueMessages)
                    {
                        if(queueMessages.Count > 0)
                        {
                            // Pick the oldest message
                            QueueMessageRecv candidate = queueMessages[0];
                            active_message.code = candidate.code;
                            active_message.data = candidate.data;
                            active_message.socket = candidate.socket;
                            active_message.endpoint = candidate.endpoint;
                            // Remove it from the queue
                            queueMessages.Remove(candidate);
                            message_found = true;
                        }
                    }

                    if(message_found)
                    {
                        // Active message set, attempt to parse it
                        ProtocolMessage.parseProtocolMessage(active_message.code, active_message.data, active_message.socket, active_message.endpoint);
                    }
                    else
                    {
                        // No active message
                        // Sleep for 10ms to prevent cpu waste
                        Thread.Sleep(10);
                    }

                }
                Logging.info("Network queue thread stopped.");
            }


        }
    }
}
