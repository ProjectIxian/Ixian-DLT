using DLT.Meta;
using DLT.Network;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DLT
{
    public class RemoteEndpoint
    {
        public IPEndPoint remoteIP;
        public Socket clientSocket;
        public RemoteEndpointState state;
        public bool inIO;
        // Maintain two threads for handling data receiving and sending
        private Thread recvThread = null;
        private Thread sendThread = null;
        private Thread parseThread = null;

        public Presence presence = null;
        public PresenceAddress presenceAddress = null;

        public bool running = false;

        // Maintain a queue of messages to send
        private List<QueueMessage> sendQueueMessages = new List<QueueMessage>();

        // Maintain a queue of raw received data
        private List<QueueMessageRaw> recvRawQueueMessages = new List<QueueMessageRaw>();


        public void start()
        {

            presence = null;
            presenceAddress = null;
            state = RemoteEndpointState.Initial;

            running = true;
            clientSocket.Blocking = true;

            // Start receive thread
            recvThread = new Thread(new ThreadStart(recvLoop));
            recvThread.Start();

            // Start send thread
            sendThread = new Thread(new ThreadStart(sendLoop));
            sendThread.Start();

            // Start parse thread
            parseThread = new Thread(new ThreadStart(parseLoop));
            parseThread.Start();
        }

        // Aborts all related endpoint threads and data
        public void abort()
        {         
            state = RemoteEndpointState.Closed;
            running = false;
            lock (sendQueueMessages)
            {
                sendQueueMessages.Clear();
            }

            lock(recvRawQueueMessages)
            {
                recvRawQueueMessages.Clear();
            }
            // Abort all related threads
            recvThread.Abort();
            sendThread.Abort();
            parseThread.Abort();
        }

        // Receive thread
        private void recvLoop()
        {
            while(running)
            {
                // Let the protocol handler receive and handle messages
                try
                {
                    byte[] data = ProtocolMessage.readSocketData(clientSocket, this);
                    if(data !=null )
                        //ProtocolMessage.readProtocolMessage(data, clientSocket, this);
                        parseDataInternal(data, clientSocket, this);
                }
                catch (Exception e)
                {
                    Logging.warn(string.Format("recvRE: Disconnected client: {0}", e.ToString()));
                    state = RemoteEndpointState.Closed;
                }

                // Sleep a while to prevent cpu cycle waste
                Thread.Sleep(10);

                // Check if the client disconnected
                if (state == RemoteEndpointState.Closed)
                {
                    running = false;
                }
            }

            // Remove corresponding address from presence list
            if (presence != null && presenceAddress != null)
            {
                PresenceList.removeAddressEntry(presence.wallet, presenceAddress);
            }

            // Close the client socket
            if (clientSocket != null)
            {
                try
                {
                    clientSocket.Shutdown(SocketShutdown.Both);
                    clientSocket.Close();
                }
                catch (Exception e)
                {
                    Logging.warn(string.Format("recvRE: Could not shutdown client socket: {0}", e.ToString()));
                }
            }

            // Remove this endpoint from the network server
            NetworkServer.removeEndpoint(this);

            Thread.Yield();
        }


        // Send thread
        private void sendLoop()
        {
            // Prepare an special message object to use while sending, without locking up the queue messages
            QueueMessage active_message = new QueueMessage();

            while (running)
            {
                bool message_found = false;
                lock (sendQueueMessages)
                {
                    if (sendQueueMessages.Count > 0)
                    {
                        // Pick the oldest message
                        QueueMessage candidate = sendQueueMessages[0];
                        active_message.code = candidate.code;
                        active_message.data = candidate.data;
                        active_message.skipSocket = candidate.skipSocket;
                        // Remove it from the queue
                        sendQueueMessages.Remove(candidate);
                        message_found = true;
                    }
                }

                if (message_found)
                {
                    // Active message set, attempt to send it
                    sendDataInternal(active_message.code, active_message.data);
                }
                // Sleep for 10ms to prevent cpu waste
                Thread.Sleep(10);
            }

            Thread.Yield();
        }

        // Parse thread
        private void parseLoop()
        {
            // Prepare an special message object to use while sending, without locking up the queue messages
            QueueMessageRaw active_message = new QueueMessageRaw();

            while (running)
            {
                try
                {
                    bool message_found = false;
                    lock (recvRawQueueMessages)
                    {
                        if (recvRawQueueMessages.Count > 0)
                        {
                            // Pick the oldest message
                            QueueMessageRaw candidate = recvRawQueueMessages[0];
                            active_message.data = candidate.data;
                            active_message.socket = candidate.socket;
                            active_message.endpoint = candidate.endpoint;
                            // Remove it from the queue
                            recvRawQueueMessages.Remove(candidate);
                            message_found = true;
                        }
                    }

                    if (message_found)
                    {
                        // Active message set, attempt to send it
                        ProtocolMessage.readProtocolMessage(active_message.data, active_message.socket, this);
                    }
                }
                catch (Exception e)
                {
                    Logging.error("Exception occured in parseLoopRE: " + e);
                }
                // Sleep for 10ms to prevent cpu waste
                Thread.Sleep(10);
            }

            Thread.Yield();
        }

        private void parseDataInternal(byte[] data, Socket socket, RemoteEndpoint endpoint)
        {
            QueueMessageRaw message = new QueueMessageRaw();
            message.data = data;
            message.socket = socket;
            message.endpoint = endpoint;

            lock(recvRawQueueMessages)
            {
                recvRawQueueMessages.Add(message);
            }
        }


        // Internal function that sends data through the socket
        private void sendDataInternal(ProtocolMessageCode code, byte[] data)
        {
            byte[] ba = ProtocolMessage.prepareProtocolMessage(code, data);
            try
            {
                for (int sentBytes = 0; sentBytes < ba.Length;)
                {
                    sentBytes += clientSocket.Send(ba, sentBytes, ba.Length - sentBytes, SocketFlags.None);
                    if (sentBytes < ba.Length)
                    {
                        Logging.warn("sendRE: Socket issue?");
                        Thread.Sleep(5);
                    }
                    // TODO TODO TODO timeout
                }
                if (clientSocket.Connected == false)
                {
                    Logging.warn("sendRE: Failed senddata to remote endpoint. Closing.");
                    state = RemoteEndpointState.Closed;
                }
            }
            catch (Exception e)
            {
                Logging.warn(String.Format("sendRE: Socket exception, closing {0}", e));
                state = RemoteEndpointState.Closed;

            }
        }


        // Sends data over the network
        public void sendData(ProtocolMessageCode code, byte[] data)
        {
            if (data == null)
            {
                Logging.warn(string.Format("Invalid protocol message data for {0}", code));
                return;
            }

            QueueMessage message = new QueueMessage();
            message.code = code;
            message.data = data;
            message.skipSocket = null;

            lock (sendQueueMessages)
            {
                if (message.code != ProtocolMessageCode.keepAlivePresence && sendQueueMessages.Exists(x => x.code == message.code && message.data.SequenceEqual(x.data)))
                {
                    Logging.warn(string.Format("Attempting to add a duplicate message (code: {0}) to the network queue", code));                
                }
                else
                {
                    if (sendQueueMessages.Count > 6)
                    {
                        // Prioritize certain messages if the queue is large
                        if (message.code != ProtocolMessageCode.newTransaction)
                        {
                            sendQueueMessages.Insert(3, message);
                        }
                        else
                        {
                            sendQueueMessages.Add(message);
                        }
                    }
                    else
                    {
                        sendQueueMessages.Add(message);
                    }
                }
            }

        }

    }
}
