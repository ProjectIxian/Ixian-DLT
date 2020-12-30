// Copyright (C) 2017-2020 Ixian OU
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

using IXICore.Meta;
using System;
using System.IO;

namespace DLTNode
{
    class DebugSnapshot
    {
        private static string filename = "debug.snapshot";


        public static bool save()
        {
            BinaryWriter writer;
            try
            {
                writer = new BinaryWriter(new FileStream(filename, FileMode.Create));
            }
            catch (IOException e)
            {
                Logging.log(LogSeverity.error, String.Format("Cannot create snapshot file. {0}", e.Message));
                return false;
            }

            try
            {
                System.Int32 version = 1; // Set the snapshot version
                writer.Write(version);

            }

            catch (IOException e)
            {
                Logging.log(LogSeverity.error, String.Format("Cannot write to snapshot file. {0}", e.Message));
                return false;
            }

            writer.Close();

            return true;
        }


        public static bool load()
        {
            if (File.Exists(filename) == false)
            {
                Logging.log(LogSeverity.error, "Cannot read snapshot file.");
                return false;
            }

            BinaryReader reader;
            try
            {
                reader = new BinaryReader(new FileStream(filename, FileMode.Open));
            }
            catch (IOException e)
            {
                Logging.log(LogSeverity.error, String.Format("Cannot open snapshot file. {0}", e.Message));
                return false;
            }

            try
            {
                // Read the wallet version
                System.Int32 version = reader.ReadInt32();

                if (version != 1)
                {
                    Logging.error(string.Format("Snapshot version mismatch, expecting {0}, got {1}", 1, version));
                    return false;
                }

            }
            catch (IOException e)
            {
                Logging.log(LogSeverity.error, String.Format("Cannot read from snapshot file. {0}", e.Message));
                return false;
            }

            reader.Close();
            return true;
        }



    }
}
