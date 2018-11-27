using DLT.Meta;
using System;
using System.IO;

namespace DLT.Meta
{

    public class WalletStateStorage
    {
        public static string baseFilename = "ws" + Path.DirectorySeparatorChar + "wsStorage.dat";

        public static void saveWalletState(ulong blockNum)
        {
            if(!Directory.Exists("ws"))
            {
                Directory.CreateDirectory("ws");
            }
            FileStream fs = File.Open(baseFilename + "." + blockNum, FileMode.Create, FileAccess.Write, FileShare.None);
            fs.Write(BitConverter.GetBytes(Node.walletState.version), 0, 4);

            DLT.WsChunk[] chunk = Node.walletState.getWalletStateChunks(0, blockNum);
            fs.Write(BitConverter.GetBytes(chunk[0].wallets.LongLength), 0, 8);

            foreach (var entry in chunk[0].wallets)
            {
                byte[] entryBytes = entry.getBytes();
                fs.Write(BitConverter.GetBytes(entryBytes.Length), 0, 4);
                fs.Write(entryBytes, 0, entryBytes.Length);
            }
            fs.Close();
        }

        public static ulong restoreWalletState(ulong blockNum = 0)
        {
            if (blockNum == 0)
            {
                blockNum = DLT.Meta.Storage.getLastBlockNum();
                if (blockNum > 0)
                {
                    blockNum = ((ulong)((DLT.Meta.Storage.getLastBlockNum() - 6) / 1000)) * 1000;
                }
                if(blockNum == 0)
                {
                    return 0;
                }
            }
            FileStream fs = null;
            while (fs == null)
            {
                if (File.Exists(baseFilename + "." + blockNum))
                {
                    fs = File.Open(baseFilename + "." + blockNum, FileMode.Open, FileAccess.Read, FileShare.None);
                }else
                {
                    if (blockNum < 1000)
                    {
                        return 0;
                    }
                    blockNum = blockNum - 1000;
                }
            }
            try
            {
                Node.walletState.clear();
                byte[] walletVersionBytes = new byte[4];
                fs.Read(walletVersionBytes, 0, 4);
                Node.walletState.version = BitConverter.ToInt32(walletVersionBytes, 0);

                byte[] walletCountBytes = new byte[8];
                fs.Read(walletCountBytes, 0, 8);

                long walletCount = BitConverter.ToInt64(walletCountBytes, 0);

                DLT.Wallet[] wallets = new DLT.Wallet[25];
                for (long i = 0, j = 0; i < walletCount; i++, j++)
                {
                    byte[] lenBytes = new byte[4];
                    fs.Read(lenBytes, 0, 4);

                    int len = BitConverter.ToInt32(lenBytes, 0);

                    byte[] entryBytes = new byte[len];
                    fs.Read(entryBytes, 0, len);

                    wallets[j] = new DLT.Wallet(entryBytes);
                    if (j == 24 || i == walletCount - 1)
                    {
                        for (int k = 24; k > j; k--)
                        {
                            wallets[k] = null;
                        }
                        Node.walletState.setWalletChunk(wallets);
                        j = 0;
                    }

                }
                fs.Close();
            }catch(Exception e)
            {
                fs.Close();
                Logging.error("An exception occured while reading file '" + (baseFilename + "." + blockNum) + "': " + e);
                File.Delete(baseFilename + "." + blockNum);
                Node.walletState.clear();
                restoreWalletState();
            }

            return blockNum;
        }

    }
}
