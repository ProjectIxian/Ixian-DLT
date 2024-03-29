﻿// Copyright (C) 2017-2020 Ixian OU
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

using IXICore;
using IXICore.Meta;
using IXICore.Network;
using IXICore.Utils;
using System;
using System.Linq;
using System.Threading;

namespace DLT.Meta
{
    public class StatsConsoleScreen
    {
        private DateTime startTime;

        private Thread thread = null;
        private bool running = false;

        private int consoleWidth = 61;
        private uint drawCycle = 0; // Keep a count of screen draw cycles as a basic method of preventing visual artifacts
        private ThreadLiveCheck TLC;

        public StatsConsoleScreen()
        {
            if (!Config.verboseOutput)
            {
                Console.Clear();
            }

            Console.CursorVisible = ConsoleHelpers.verboseConsoleOutput;

            // Start thread
            TLC = new ThreadLiveCheck();
            running = true;
            thread = new Thread(new ThreadStart(threadLoop));
            thread.Name = "Stats_Console_Thread";
            thread.Start();

            startTime = DateTime.UtcNow;
        }

        // Shutdown console thread
        public void stop()
        {
            running = false;
        }

        private void threadLoop()
        {
            while (running)
            {
                TLC.Report();
                if (ConsoleHelpers.verboseConsoleOutput == false)
                {
                    try
                    {
                        // Clear the screen every 10 seconds to prevent any persisting visual artifacts
                        if (drawCycle > 5)
                        {
                            clearScreen();
                            drawCycle = 0;
                        }
                        else
                        {
                            drawScreen();
                            drawCycle++;
                        }
                    }
                    catch (Exception e)
                    {
                        Logging.error("Error in StatsConsoleScreen.threadLoop: {0}", e);
                    }
                }

                Thread.Sleep(2000);
            }
        }

        private void drawWebSocketStatus()
        {
            // Check if the Config has a WebSocket URL set
            if (!string.IsNullOrWhiteSpace(Config.websocketUrl))
            {
                bool isConnected = Node.IsWebSocketConnected();
                bool isReconnecting = Node.IsWebSocketReconnecting();
                var (ReconnectionAttempts, MaxReconnectionAttempts) = Node.WebSocketReconnectionAttempts();

                string wsStatus = isConnected ? "Connected" : "Disconnected";
                if (!isConnected && isReconnecting)
                {
                    wsStatus += $" - Reconnecting ({ReconnectionAttempts}/{MaxReconnectionAttempts})...";
                }

                string lastPong = Node.GetLastWebSocketPongTime() == DateTime.MinValue ? "N/A" : $"{(DateTime.UtcNow - Node.GetLastWebSocketPongTime()).TotalSeconds:F0}s ago";
                int processedRequests = Node.GetWebSocketProcessedRequests();
            Console.ForegroundColor = ConsoleColor.DarkGreen;

                writeLine("");
                writeLine($" WebSocket Status:     {wsStatus}");
                writeLine($" Last Pong:            {lastPong}");
                writeLine($" Processed Requests:   {processedRequests}");
                Console.ResetColor();
            }
        }



        public void clearScreen()
        {
            //Console.BackgroundColor = ConsoleColor.DarkGreen;
            Console.Clear();
            drawScreen();
        }



        public void drawScreen()
        {
            if (Node.storage.isUpgrading())
            {
                Console.Clear();
            }

            Console.SetCursorPosition(0, 0);


            string server_version = checkForUpdate();
            bool update_avail = false;
            if (!server_version.StartsWith("("))
            {
                if (server_version.CompareTo(Config.version) > 0)
                {
                    update_avail = true;
                }
            }

            int connectionsOut = NetworkClientManager.getConnectedClients(true).Count();
            int connectionsIn = NetworkServer.getConnectedClients().Count();

            Console.ForegroundColor = ConsoleColor.DarkGreen;


            writeLine(" ██╗██╗  ██╗██╗ █████╗ ███╗   ██╗    ██████╗ ██╗  ████████╗ ");
            writeLine(" ██║╚██╗██╔╝██║██╔══██╗████╗  ██║    ██╔══██╗██║  ╚══██╔══╝ ");
            writeLine(" ██║ ╚███╔╝ ██║███████║██╔██╗ ██║    ██║  ██║██║     ██║    ");
            writeLine(" ██║ ██╔██╗ ██║██╔══██║██║╚██╗██║    ██║  ██║██║     ██║    ");
            writeLine(" ██║██╔╝ ██╗██║██║  ██║██║ ╚████║    ██████╔╝███████╗██║    ");
            writeLine(" ╚═╝╚═╝  ╚═╝╚═╝╚═╝  ╚═╝╚═╝  ╚═══╝    ╚═════╝ ╚══════╝╚═╝    ");
            Console.ResetColor();

            writeLine(" {0}", (Config.version + " BETA ").PadLeft(59));

            Console.ForegroundColor = ConsoleColor.DarkGreen;
            writeLine(" {0}", ("IXIWSS Edition").PadLeft(58));
            Console.ResetColor();


            writeLine(" {0}", ("http://localhost:" + Config.apiPort + "/"));
            writeLine("────────────────────────────────────────────────────────────");

            if (update_avail)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                writeLine(" An update (" + server_version + ") of Ixian DLT is available");
                writeLine(" Please visit https://www.ixian.io");
                Console.ResetColor();
            }
            else
            {
                if (!NetworkServer.isConnectable() && connectionsOut == 0)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    writeLine(" Your node isn't connectable from the internet.");
                    writeLine(" Please set-up port forwarding for port " + IxianHandler.publicPort + ". ");
                    writeLine(" Make sure you can connect to: " + IxianHandler.getFullPublicAddress());
                    Console.ResetColor();
                }
                else
                {
                    writeLine(" Thank you for running an Ixian DLT node.");
                    writeLine(" For help please visit https://www.ixian.io");
                }
            }
            writeLine("────────────────────────────────────────────────────────────");

            if (Node.storage.isUpgrading())
            {
                writeLine(" Upgrading database: " + Node.storage.upgradePercentage() + " %");
            }

            if (Node.serverStarted == false)
            {
                return;
            }

            // Node status
            ulong lastBlockNum = 0;
            string lastBlockChecksum = "";
            int sigCount = 0;
            Block b = Node.blockChain.getLastBlock();
            if (b != null)
            {
                lastBlockNum = b.blockNum;
                sigCount = b.signatures.Count();
                lastBlockChecksum = Crypto.hashToString(b.blockChecksum).Substring(0, 6);
            }

            Console.Write(" Status:               ");
            Console.ForegroundColor = ConsoleColor.Green;

            string dltStatus = "active";
            // Console.ResetColor();

            string dltStatusDetail = "";

            if (Node.blockSync.synchronizing)
            {
                ulong targetBlockNum = Node.blockSync.syncTargetBlockNum;
                float syncPercent = 0;
                if (targetBlockNum > 0)
                    syncPercent = 100.0f / targetBlockNum * lastBlockNum;

                Console.ForegroundColor = ConsoleColor.Yellow;
                dltStatus = String.Format("synchronizing {0:0.00}%", syncPercent);
                dltStatusDetail = String.Format("                       target block {0}", targetBlockNum);
            }


            string connectionsInStr = "-";  // Default to no inbound connections accepted
            if (NetworkServer.isRunning())
            {
                // If the server is running, show the number of inbound connections
                connectionsInStr = String.Format("{0}", connectionsIn);
            }

            if (connectionsIn + connectionsOut < 1)
            {
                Console.ResetColor();
                dltStatus = "connecting   ";
            }

            if (Node.blockChain.getTimeSinceLastBlock() > 1800) // if no block for over 1800 seconds
            {
                Console.ForegroundColor = ConsoleColor.Red;
                dltStatus = "No fully signed block received for over 30 minutes";
            }

            if (Clock.networkTimeDifference != Clock.realNetworkTimeDifference && connectionsOut > 2)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                dltStatus = "Please make sure that your computer's date and time are correct";
            }

            if (Node.blockProcessor.networkUpgraded)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                dltStatus = "Network has been upgraded, please download a newer version of Ixian DLT";
            }



            writeLine(dltStatus);
            Console.ResetColor();

            writeLine(dltStatusDetail);

            writeLine(" Last Block:           {0} ({1} sigs) - {2}...", lastBlockNum, sigCount, lastBlockChecksum);
            writeLine(" Last Block Added:     {0}s ago", Node.blockChain.getTimeSinceLastBlock());
            writeLine(" Connections (I/O):    {0}", connectionsInStr + "/" + connectionsOut);
            writeLine(" Presences:            {0}", PresenceList.getTotalPresences());
            writeLine(" Transaction Pool:     {0}", TransactionPool.getUnappliedTransactionCount());
            writeLine(" Signer Hashrate:      {0}", Node.signerPowMiner.lastHashRate);

            // Mining status
            string mineStatus = "stopped";
            if (Node.miner.lastHashRate > 0)
                mineStatus = "active ";
            if (Node.miner.pause)
                mineStatus = "paused ";

            writeLine("");
            writeLine(" Mining:               {0}", mineStatus);
            writeLine(" Hashrate:             {0}", Node.miner.lastHashRate);
            writeLine(" Solved Blocks:        {0}", Node.miner.getSolvedBlocksCount());
            writeLine("");
            drawWebSocketStatus();
            writeLine("────────────────────────────────────────────────────────────");



            TimeSpan elapsed = DateTime.UtcNow - startTime;

            writeLine(" Running for {0} days {1}h {2}m {3}s", elapsed.Days, elapsed.Hours, elapsed.Minutes, elapsed.Seconds);
            writeLine("");
            writeLine(" Press V to toggle stats. Esc key to exit.");

        }

        private void writeLine(string str, params object[] arguments)
        {
            Console.WriteLine(string.Format(str, arguments).PadRight(consoleWidth));
        }

        private string checkForUpdate()
        {
            if (!UpdateVerify.ready && !UpdateVerify.error) return "(checking)";
            if (UpdateVerify.ready)
            {
                if (UpdateVerify.error) return "(error)";
                return UpdateVerify.serverVersion;
            }
            return "(not checked)";
        }
    }
}
