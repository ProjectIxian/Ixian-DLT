using DLT.Meta;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DLTNode.Network
{
    class NetDump
    {
        private static NetDump _singletonInstance;

        public static NetDump Instance {
            get
            {
                if(_singletonInstance == null)
                {
                    _singletonInstance = new NetDump();
                }
                return _singletonInstance;
            }
        }

        private readonly Object outputBufferLock = new Object();
        private Queue<byte[]> outputQueueReceived;
        private Queue<byte[]> outputQueueSent;
        private Thread outputWriter;
        private BufferedStream outputReceived;
        private BufferedStream outputSent;
        
        public bool running { get; private set; }

        private NetDump()
        {
            outputQueueReceived = new Queue<byte[]>();
            outputQueueSent = new Queue<byte[]>();
            outputWriter = new Thread(outputWriterWorker);
            outputWriter.Name = "NetworkDumper";
        }

        public void appendReceived(byte[] data, int count)
        {
            if (!running) return;
            lock(outputBufferLock)
            {
                if (outputQueueReceived.Count > 50) return;
                outputQueueReceived.Enqueue(data.Take(count).ToArray());
            }
        }

        public void appendSent(byte[] data)
        {
            if (!running) return;
            lock (outputBufferLock)
            {
                if (outputQueueSent.Count > 50) return;
                outputQueueSent.Enqueue(data.ToArray());
            }
        }

        public void start(Stream receivedFile, Stream sentFile)
        {
            Logging.info("Network dump thread starting...");
            outputReceived = new BufferedStream(receivedFile);
            outputSent = new BufferedStream(sentFile);
            running = true;
            outputWriter.Start();
        }

        public void shutdown()
        {
            Logging.info("Stopping network dump thread...");
            running = false;
            if (outputWriter != null && outputWriter.ThreadState == ThreadState.Running)
            {
                outputWriter.Join();
            }
            Logging.info("Network dump thread stopped, flushing remaining messages.");
            lock(outputBufferLock)
            {
                while(outputQueueReceived.Count>0)
                {
                    writeOutputReceived();
                }
                while(outputQueueSent.Count>0)
                {
                    writeOutputSent();
                }
            }
            if(outputReceived != null)
            {
                outputReceived.Flush();
                outputReceived = null;
            }
            if(outputSent != null)
            {
                outputSent.Flush();
                outputSent = null;
            }
        }

        private void outputWriterWorker()
        {
            while(running)
            {
                int num_items = 0;
                if(outputQueueReceived.Count > 0)
                {
                    writeOutputReceived();
                    num_items++;
                    if (num_items > 20) break;
                }
                num_items = 0;
                if(outputQueueSent.Count > 0)
                {
                    writeOutputSent();
                    num_items++;
                    if (num_items > 20) break;
                }
                Thread.Sleep(250);
            }
        }

        private void writeOutputReceived()
        {
            lock (outputBufferLock)
            {
                if (outputQueueReceived.Count > 0)
                {
                    byte[] next_to_write = outputQueueReceived.Dequeue();
                    if (outputReceived != null)
                    {
                        outputReceived.Write(next_to_write, 0, next_to_write.Length);
                    }
                }
            }
        }

        private void writeOutputSent()
        {
            lock (outputBufferLock)
            {
                if (outputQueueSent.Count > 0)
                {
                    byte[] next_to_write = outputQueueSent.Dequeue();
                    if (outputReceived != null)
                    {
                        outputSent.Write(next_to_write, 0, next_to_write.Length);
                    }
                }
            }
        }
    }
}
