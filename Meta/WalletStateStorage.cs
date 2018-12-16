using DLT.Meta;
using System;
using System.IO;

namespace DLT.Meta
{

    public class WalletStateStorage
    {
        public static string path = Config.dataFolderPath + Path.DirectorySeparatorChar + "ws";

        public static void saveWalletState(ulong blockNum)
        {
            Node.checkDataFolder();

            string db_path = path + Path.DirectorySeparatorChar + "0000" + Path.DirectorySeparatorChar + blockNum + ".dat";

            FileStream fs = File.Open(db_path, FileMode.Create, FileAccess.Write, FileShare.None);
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
                if(DLT.Meta.Storage.getLastBlockNum() <= 6)
                {
                    return 0;
                }
                blockNum = DLT.Meta.Storage.getLastBlockNum() - 6;
                if(blockNum == 0)
                {
                    return 0;
                }
            }
            blockNum = ((ulong)(blockNum / 1000)) * 1000;
            string db_path = "";

            FileStream fs = null;
            while (fs == null)
            {
                db_path = path + Path.DirectorySeparatorChar + "0000" + Path.DirectorySeparatorChar + blockNum + ".dat";
                if (File.Exists(db_path))
                {
                    fs = File.Open(db_path, FileMode.Open, FileAccess.Read, FileShare.None);
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
                Logging.error("An exception occured while reading file '" + db_path + "': " + e);
                File.Delete(db_path);
                Node.walletState.clear();
                restoreWalletState();
            }

            return blockNum;
        }

        public static void deleteCache()
        {
            string db_path = path + Path.DirectorySeparatorChar + "0000" + Path.DirectorySeparatorChar;
            string[] fileNames = Directory.GetFiles(db_path);
            foreach (string fileName in fileNames)
            {
                File.Delete(fileName);
            }
        }
    }
}
