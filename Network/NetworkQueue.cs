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

        class NetworkQueue
        {
            private static bool shouldStop = false; // flag to signal shutdown of threads


            // Maintain a queue of messages to send
            private static List<QueueMessage> queueMessages = new List<QueueMessage>();


            public static int getQueuedMessageCount()
            {
                return queueMessages.Count;
            }

            // Broadcast a protocol message across clients and nodes
            public static void broadcastProtocolMessage(ProtocolMessageCode code, byte[] data, Socket skipSocket = null)
            {
                if (data == null)
                {
                    Logging.warn(string.Format("Invalid protocol message data for {0}", code));
                    return;
                }

                QueueMessage message = new QueueMessage();
                message.code = code;
                message.data = data;
                message.skipSocket = skipSocket;

                lock (queueMessages)
                {
                    if (queueMessages.Exists(x => x.code == message.code && x.data == message.data && x.skipSocket == message.skipSocket))
                    {
                        Logging.warn(string.Format("Attempting to add a duplicate message (code: {0}) to the network queue", code));
                    }
                    else
                    {
                        queueMessages.Add(message);
                    }
                }
            }


            // Start the network queue
            public static void start()
            {
                shouldStop = false;
                queueMessages.Clear();

                Thread queue_thread = new Thread(queueThreadLoop);
                queue_thread.Start();

                Logging.info("Network queue thread started.");
            }

            // Signals all the mining threads to stop
            public static bool stop()
            {
                shouldStop = true;
                return true;
            }

            // Actual network queue logic
            public static void queueThreadLoop()
            {
                // Prepare an special message object to use while sending, without locking up the queue messages
                QueueMessage active_message = new QueueMessage();

                while (!shouldStop)
                {
                    bool message_found = false;
                    lock(queueMessages)
                    {
                        if(queueMessages.Count > 0)
                        {
                            // Pick the oldest message
                            QueueMessage candidate = queueMessages[0];
                            active_message.code = candidate.code;
                            active_message.data = candidate.data;
                            active_message.skipSocket = candidate.skipSocket;
                            // Remove it from the queue
                            queueMessages.Remove(candidate);
                            message_found = true;
                        }
                    }

                    if(message_found)
                    {
                        // Active message set, attempt to broadcast it
                     //   NetworkClientManager.broadcastData(active_message.code, active_message.data, active_message.skipSocket);
                        NetworkServer.broadcastData(active_message.code, active_message.data, active_message.skipSocket);
                    }
                    else
                    {
                        // No active message
                        // Sleep for 100ms to prevent cpu waste
                        Thread.Sleep(100);
                    }

                }
                Logging.info("Network queue thread stopped.");
            }


        }
    }
}
