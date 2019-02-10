using DLT;
using DLT.Meta;
using IXICore;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DLTNode
{
    class DLTWalletStorage : WalletStorage
    {
        public DLTWalletStorage()
        {
            filename = "ixian.wal";
            readWallet();
        }

        public DLTWalletStorage(string file_name)
        {
            filename = file_name;
            readWallet();
        }

        private void displayBackupText()
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine("");
            Console.WriteLine("!! Always remember to keep a backup of your ixian.wal file and your password.");
            Console.WriteLine("!! In case of a lost file you will not be able to access your funds.");
            Console.WriteLine("!! Never give your ixian.wal and/or password to anyone.");
            Console.WriteLine("");
            Console.ResetColor();
        }

        // Requests the user to type a new password
        private string requestNewPassword(string banner)
        {
            Console.WriteLine();
            Console.Write(banner);
            try
            {
                string pass = getPasswordInput();

                if (pass.Length < 10)
                {
                    Console.WriteLine("Password needs to be at least 10 characters. Try again.");
                    return "";
                }

                Console.Write("Type it again to confirm: ");

                string passconfirm = getPasswordInput();

                if (pass.Equals(passconfirm, StringComparison.Ordinal))
                {
                    return pass;
                }
                else
                {
                    Console.WriteLine("Passwords don't match, try again.");

                    // Passwords don't match
                    return "";
                }

            }
            catch (Exception)
            {
                // Handle exceptions
                return "";
            }
        }

        // Handles console password input
        public string getPasswordInput()
        {
            StringBuilder sb = new StringBuilder();
            while (true)
            {
                ConsoleKeyInfo i = Console.ReadKey(true);
                if (i.Key == ConsoleKey.Enter)
                {
                    Console.WriteLine();
                    break;
                }
                else if (i.Key == ConsoleKey.Backspace)
                {
                    if (sb.Length > 0)
                    {
                        sb.Remove(sb.Length - 1, 1);
                        Console.Write("\b \b");
                    }
                }
                else if (i.KeyChar != '\u0000')
                {
                    sb.Append(i.KeyChar);
                    Console.Write("*");
                }
            }
            return sb.ToString();
        }



        protected void readWallet_v1(BinaryReader reader)
        {
            // Read the encrypted keys
            int b_privateKeyLength = reader.ReadInt32();
            byte[] b_privateKey = reader.ReadBytes(b_privateKeyLength);

            int b_publicKeyLength = reader.ReadInt32();
            byte[] b_publicKey = reader.ReadBytes(b_publicKeyLength);

            byte[] b_last_nonce = null;
            if (reader.BaseStream.Position < reader.BaseStream.Length)
            {
                int b_last_nonceLength = reader.ReadInt32();
                b_last_nonce = reader.ReadBytes(b_last_nonceLength);
            }

            bool success = false;
            while (!success)
            {
                displayBackupText();

                // NOTE: This is only permitted on the testnet for dev/testing purposes!
                string password = "";
                if (Config.dangerCommandlinePasswordCleartextUnsafe != "" && Config.isTestNet)
                {
                    Logging.warn("Attempting to unlock the wallet with a password from commandline!");
                    password = Config.dangerCommandlinePasswordCleartextUnsafe;
                }
                if (password.Length < 10)
                {
                    Logging.flush();
                    Console.Write("Enter wallet password: ");
                    password = getPasswordInput();
                }
                success = true;
                try
                {
                    // Decrypt
                    privateKey = CryptoManager.lib.decryptWithPassword(b_privateKey, password);
                    publicKey = CryptoManager.lib.decryptWithPassword(b_publicKey, password);
                    walletPassword = password;
                }
                catch (Exception)
                {
                    Logging.error(string.Format("Incorrect password"));
                    Logging.flush();
                    success = false;
                }

            }

            Address addr = new Address(publicKey);
            lastAddress = address = addr.address;

            masterSeed = address;
            seedHash = address;
            derivedMasterSeed = masterSeed;

            IxianKeyPair kp = new IxianKeyPair();
            kp.privateKeyBytes = privateKey;
            kp.publicKeyBytes = publicKey;
            kp.addressBytes = address;
            lock (myKeys)
            {
                myKeys.Add(address, kp);
            }
            lock (myAddresses)
            {
                AddressData ad = new AddressData() { nonce = new byte[1] { 0 }, keyPair = kp };
                myAddresses.Add(address, ad);

                if (b_last_nonce != null)
                {
                    byte[] last_nonce_bytes = CryptoManager.lib.decryptWithPassword(b_last_nonce, walletPassword);
                    bool last_address_found = false;
                    while (last_address_found == false)
                    {
                        if (kp.lastNonceBytes != null && last_nonce_bytes.SequenceEqual(kp.lastNonceBytes))
                        {
                            last_address_found = true;
                        }
                        else
                        {
                            generateNewAddress(addr, false);
                        }
                    }
                }
            }
        }

        protected void readWallet_v3(BinaryReader reader)
        {
            // Read the master seed
            int b_master_seed_length = reader.ReadInt32();
            byte[] b_master_seed = reader.ReadBytes(b_master_seed_length);

            string password = "";

            bool success = false;
            while (!success)
            {
                displayBackupText();
                // NOTE: This is only permitted on the testnet for dev/testing purposes!
                if (Config.dangerCommandlinePasswordCleartextUnsafe != "" && Config.isTestNet)
                {
                    Logging.warn("Attempting to unlock the wallet with a password from commandline!");
                    password = Config.dangerCommandlinePasswordCleartextUnsafe;
                }
                if (password.Length < 10)
                {
                    Logging.flush();
                    Console.Write("Enter wallet password: ");
                    password = getPasswordInput();
                }
                success = true;
                try
                {
                    // Decrypt
                    masterSeed = CryptoManager.lib.decryptWithPassword(b_master_seed, password);
                    seedHash = Crypto.sha512sqTrunc(masterSeed);
                    walletPassword = password;
                }
                catch (Exception)
                {
                    Logging.error(string.Format("Incorrect password"));
                    Logging.flush();
                    success = false;
                }

            }

            int key_count = reader.ReadInt32();
            for (int i = 0; i < key_count; i++)
            {
                int len = reader.ReadInt32();
                if (reader.BaseStream.Position + len > reader.BaseStream.Length)
                {
                    Logging.error("Wallet file is corrupt, expected more data than available.");
                    break;
                }
                byte[] enc_private_key = reader.ReadBytes(len);

                len = reader.ReadInt32();
                if (reader.BaseStream.Position + len > reader.BaseStream.Length)
                {
                    Logging.error("Wallet file is corrupt, expected more data than available.");
                    break;
                }
                byte[] enc_public_key = reader.ReadBytes(len);

                len = reader.ReadInt32();
                if (reader.BaseStream.Position + len > reader.BaseStream.Length)
                {
                    Logging.error("Wallet file is corrupt, expected more data than available.");
                    break;
                }
                byte[] enc_nonce = null;
                if (len > 0)
                {
                    enc_nonce = reader.ReadBytes(len);
                }

                byte[] dec_private_key = CryptoManager.lib.decryptWithPassword(enc_private_key, password);
                byte[] dec_public_key = CryptoManager.lib.decryptWithPassword(enc_public_key, password);
                byte[] tmp_address = (new Address(dec_public_key)).address;

                IxianKeyPair kp = new IxianKeyPair();
                kp.privateKeyBytes = dec_private_key;
                kp.publicKeyBytes = dec_public_key;
                kp.addressBytes = tmp_address;
                if (enc_nonce != null)
                {
                    kp.lastNonceBytes = CryptoManager.lib.decryptWithPassword(enc_nonce, password);
                }

                if (privateKey == null)
                {
                    privateKey = dec_private_key;
                    publicKey = dec_public_key;
                    lastAddress = address = tmp_address;
                }

                lock (myKeys)
                {
                    myKeys.Add(tmp_address, kp);
                }
                lock (myAddresses)
                {
                    AddressData ad = new AddressData() { nonce = new byte[1] { 0 }, keyPair = kp };
                    myAddresses.Add(tmp_address, ad);
                }
            }

            int seed_len = reader.ReadInt32();
            byte[] enc_derived_seed = reader.ReadBytes(seed_len);
            derivedMasterSeed = CryptoManager.lib.decryptWithPassword(enc_derived_seed, password);
        }

        // Try to read wallet information from the file
        new protected bool readWallet()
        {

            if (File.Exists(filename) == false)
            {
                Logging.log(LogSeverity.error, "Cannot read wallet file.");

                // Generate a new wallet
                return generateWallet();
            }

            // create a backup of the new wallet file
            if (!File.Exists(filename + ".bak"))
            {
                File.Copy(filename, filename + ".bak");
            }

            Logging.log(LogSeverity.info, "Wallet file found, reading data...");
            Logging.flush();

            BinaryReader reader;
            try
            {
                reader = new BinaryReader(new FileStream(filename, FileMode.Open));
            }
            catch (IOException e)
            {
                Logging.log(LogSeverity.error, String.Format("Cannot open wallet file. {0}", e.Message));
                return false;
            }

            try
            {
                // Read the wallet version
                walletVersion = reader.ReadInt32();
                if (walletVersion == 1 || walletVersion == 2)
                {
                    readWallet_v1(reader);
                }
                else if (walletVersion == 3)
                {
                    readWallet_v3(reader);
                }
                else
                {
                    Logging.error("Unknown wallet version {0}", walletVersion);
                    walletVersion = 0;
                    return false;
                }

                // Wait for any pending log messages to be written
                Logging.flush();

                Console.WriteLine();
                Console.Write("Your IXIAN address is ");
                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine(Base58Check.Base58CheckEncoding.EncodePlain(address));
                Console.ResetColor();
                Console.WriteLine();

            }
            catch (IOException e)
            {
                Logging.error("Cannot read from wallet file. {0}", e.Message);
                return false;
            }

            reader.Close();

            // Check if we should change the password of the wallet
            if (Config.changePass == true)
            {
                // Request a new password
                string new_password = "";
                while (new_password.Length < 10)
                {
                    new_password = requestNewPassword("Enter a new password for your wallet: ");
                }
                writeWallet(new_password);
            }

            Logging.info("Public Node Address: {0}", Base58Check.Base58CheckEncoding.EncodePlain(address));

            return true;
        }

        new public bool backup(string destination)
        {
            File.Copy(filename, destination);
            return true;
        }

        new public bool writeWallet(string password)
        {
            if (walletVersion == 1 || walletVersion == 2)
            {
                return writeWallet_v1(walletPassword);
            }
            if (walletVersion == 3)
            {
                return writeWallet_v3(walletPassword);
            }
            return false;
        }

        // Write the wallet to the file
        protected bool writeWallet_v1(string password)
        {
            if (password.Length < 10)
                return false;

            // Encrypt data first
            byte[] b_privateKey = CryptoManager.lib.encryptWithPassword(privateKey, password);
            byte[] b_publicKey = CryptoManager.lib.encryptWithPassword(publicKey, password);

            BinaryWriter writer;
            try
            {
                writer = new BinaryWriter(new FileStream(filename, FileMode.Create));
            }
            catch (IOException e)
            {
                Logging.error("Cannot create wallet file. {0}", e.Message);
                return false;
            }

            try
            {
                writer.Write(walletVersion);

                // Write the address keypair
                writer.Write(b_privateKey.Length);
                writer.Write(b_privateKey);

                writer.Write(b_publicKey.Length);
                writer.Write(b_publicKey);

                if (myKeys.First().Value.lastNonceBytes != null)
                {
                    byte[] b_last_nonce = CryptoManager.lib.encryptWithPassword(myKeys.First().Value.lastNonceBytes, password);
                    writer.Write(b_last_nonce.Length);
                    writer.Write(b_last_nonce);
                }

            }

            catch (IOException e)
            {
                Logging.error("Cannot write to wallet file. {0}", e.Message);
                return false;
            }

            writer.Close();

            return true;
        }

        // Write the wallet to the file
        protected bool writeWallet_v3(string password)
        {
            if (password.Length < 10)
                return false;

            BinaryWriter writer;
            try
            {
                writer = new BinaryWriter(new FileStream(filename, FileMode.Create));
            }
            catch (IOException e)
            {
                Logging.error("Cannot create wallet file. {0}", e.Message);
                return false;
            }

            try
            {
                writer.Write(walletVersion);

                // Write the master seed
                byte[] enc_master_seed = CryptoManager.lib.encryptWithPassword(masterSeed, password);
                writer.Write(enc_master_seed.Length);
                writer.Write(enc_master_seed);

                lock (myKeys)
                {
                    writer.Write(myKeys.Count());

                    foreach (var entry in myKeys)
                    {
                        byte[] enc_private_key = CryptoManager.lib.encryptWithPassword(entry.Value.privateKeyBytes, password);
                        writer.Write(enc_private_key.Length);
                        writer.Write(enc_private_key);

                        byte[] enc_public_key = CryptoManager.lib.encryptWithPassword(entry.Value.publicKeyBytes, password);
                        writer.Write(enc_public_key.Length);
                        writer.Write(enc_public_key);

                        if (entry.Value.lastNonceBytes != null)
                        {
                            byte[] enc_nonce = CryptoManager.lib.encryptWithPassword(entry.Value.lastNonceBytes, password);
                            writer.Write(enc_nonce.Length);
                            writer.Write(enc_nonce);
                        }
                        else
                        {
                            writer.Write((int)0);
                        }
                    }
                }

                byte[] enc_derived_master_seed = CryptoManager.lib.encryptWithPassword(derivedMasterSeed, password);
                writer.Write(enc_derived_master_seed.Length);
                writer.Write(enc_derived_master_seed);
            }

            catch (IOException e)
            {
                Logging.error("Cannot write to wallet file. {0}", e.Message);
                return false;
            }

            writer.Close();

            return true;
        }

        // Generate a new wallet with matching private/public key pairs
        protected bool generateWallet()
        {
            Logging.info("A new wallet will be generated for you.");

            Logging.flush();

            displayBackupText();

            // Request a password
            // NOTE: This can only be done in testnet to enable automatic testing!
            string password = "";
            if (Config.dangerCommandlinePasswordCleartextUnsafe != "" && Config.isTestNet)
            {
                Logging.warn("TestNet detected and wallet password has been specified on the command line!");
                password = Config.dangerCommandlinePasswordCleartextUnsafe;
                // Also note that the commandline password still has to be >= 10 characters
            }
            while (password.Length < 10)
            {
                Logging.flush();
                password = requestNewPassword("Enter a password for your new wallet: ");
            }


            walletVersion = 1;
            walletPassword = password;

            Logging.log(LogSeverity.info, "Generating primary wallet keys, this may take a while, please wait...");

            //IxianKeyPair kp = generateNewKeyPair(false);
            IxianKeyPair kp = CryptoManager.lib.generateKeys(CoreConfig.defaultRsaKeySize, true);

            if (kp == null)
            {
                Logging.error("Error creating wallet, unable to generate a new keypair.");
                return false;
            }

            privateKey = kp.privateKeyBytes;
            publicKey = kp.publicKeyBytes;

            Address addr = new Address(publicKey);
            lastAddress = address = addr.address;

            masterSeed = address;
            seedHash = address;
            derivedMasterSeed = masterSeed;

            kp.addressBytes = address;

            myKeys.Add(address, kp);
            myAddresses.Add(address, new AddressData() { keyPair = kp, nonce = new byte[1] { 0 } });


            Logging.info(String.Format("Public Key: {0}", Crypto.hashToString(publicKey)));
            Logging.info(String.Format("Public Node Address: {0}", Base58Check.Base58CheckEncoding.EncodePlain(address)));

            // Wait for any pending log messages to be written
            Logging.flush();

            Console.WriteLine();
            Console.Write("Your IXIAN address is ");
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine(Base58Check.Base58CheckEncoding.EncodePlain(address));
            Console.ResetColor();
            Console.WriteLine();

            // Write the new wallet data to the file
            if (writeWallet(password))
            {
                // create a backup of the new wallet file
                if (!File.Exists(filename + ".bak"))
                {
                    File.Copy(filename, filename + ".bak");
                }
                return true;
            }
            return false;
        }

    }
}
