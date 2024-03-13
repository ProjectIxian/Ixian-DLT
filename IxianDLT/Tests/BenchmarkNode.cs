// Copyright (C) 2017-2022 Ixian OU
// This file is part of Ixian DLT - www.github.com/ProjectIxian/Ixian-DLT
//
// Ixian DLT is free software: you can redistribute it and/or modify
// it under the terms of the MIT License as published
// by the Open Source Initiative.
//
// Ixian DLT is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// MIT License for more details.

using DLT.Meta;
using DLT.Network;
using IXICore;
using IXICore.Meta;
using IXICore.Network;
using IXICore.RegNames;
using IXICore.Utils;
using System;
using System.Collections.Generic;

namespace DLTNode
{
    class BenchmarkNode : IxianNode
    {
        public static bool running = false;

        public BenchmarkNode()
        {
            IxianHandler.init(Config.version, this, NetworkType.main);
            init();
        }
        
        // Perform basic initialization of node
        private void init()
        {
            running = true;

         /*   // Load or Generate the wallet
            if (!initWallet())
            {
                running = false;
                return;
            }*/

            Logging.info("Preparing BenchmarkNode");
        }

        private bool initWallet()
        {
            WalletStorage walletStorage = new WalletStorage(Config.walletFile);

            Logging.flush();

            if (!walletStorage.walletExists())
            {
                ConsoleHelpers.displayBackupText();

                // Request a password
                string password = "";
                while (password.Length < 10)
                {
                    Logging.flush();
                    password = ConsoleHelpers.requestNewPassword("Enter a password for your new wallet: ");
                    if (IxianHandler.forceShutdown)
                    {
                        return false;
                    }
                }
                walletStorage.generateWallet(password);
            }
            else
            {
                ConsoleHelpers.displayBackupText();

                bool success = false;
                while (!success)
                {

                    string password = "";
                    if (password.Length < 10)
                    {
                        Logging.flush();
                        Console.Write("Enter wallet password: ");
                        password = ConsoleHelpers.getPasswordInput();
                    }
                    if (IxianHandler.forceShutdown)
                    {
                        return false;
                    }
                    if (walletStorage.readWallet(password))
                    {
                        success = true;
                    }
                }
            }


            if (walletStorage.getPrimaryPublicKey() == null)
            {
                return false;
            }

            // Wait for any pending log messages to be written
            Logging.flush();

            Console.WriteLine();
            Console.WriteLine("Your IXIAN addresses are: ");
            Console.ForegroundColor = ConsoleColor.Green;
            foreach (var entry in walletStorage.getMyAddressesBase58())
            {
                Console.WriteLine(entry);
            }
            Console.ResetColor();
            Console.WriteLine();

            if (Config.onlyShowAddresses)
            {
                return false;
            }

            if (walletStorage.viewingWallet)
            {
                Logging.error("Viewing-only wallet {0} cannot be used as the primary wallet.", walletStorage.getPrimaryAddress().ToString());
                return false;
            }

            IxianHandler.addWallet(walletStorage);

            return true;
        }

        static public void stop()
        {
            IxianHandler.forceShutdown = true;
        }

        public void start()
        {

        }

        public override ulong getLastBlockHeight()
        {
            return 1000000;
        }

        public override bool isAcceptingConnections()
        {
            return false;
        }

        public override ulong getHighestKnownNetworkBlockHeight()
        {
            return 1000000;
        }

        public override int getLastBlockVersion()
        {
            return Block.maxVersion - 1;          
        }

        public override bool addTransaction(Transaction tx, bool force_broadcast)
        {
            return true;
        }

        public override Block getLastBlock()
        {
            throw new NotImplementedException();
        }

        public override Wallet getWallet(Address id)
        {
            return new Wallet(id, 0);
        }

        public override IxiNumber getWalletBalance(Address id)
        {
            return 0;
        }

        public override void shutdown()
        {
            IxianHandler.forceShutdown = true;
        }

        public override void parseProtocolMessage(ProtocolMessageCode code, byte[] data, RemoteEndpoint endpoint)
        {
            ProtocolMessage.parseProtocolMessage(code, data, endpoint);
        }

        public override Block getBlockHeader(ulong blockNum)
        {
            return BlockHeaderStorage.getBlockHeader(blockNum);
        }

        public override IxiNumber getMinSignerPowDifficulty(ulong blockNum)
        {
            return 1;
        }

        public override byte[] calculateRegNameChecksumFromUpdatedDataRecords(byte[] name, List<RegisteredNameDataRecord> dataRecords, ulong sequence, Address nextPkHash)
        {
            throw new NotImplementedException();
        }

        public override byte[] calculateRegNameChecksumForRecovery(byte[] name, Address recoveryHash, ulong sequence, Address nextPkHash)
        {
            throw new NotImplementedException();
        }
    }
}
