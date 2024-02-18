using IXICore;
using IXICore.Meta;
using IXICore.Network;
using IXICore.RegNames;
using System;
using System.Collections.Generic;

namespace UnitTests
{
    public class DummyIxianNode : IxianNode
    {
        public override bool addTransaction(Transaction tx, bool force_broadcast)
        {
            throw new NotImplementedException();
        }

        public override byte[] calculateRegNameChecksumForRecovery(byte[] name, Address recoveryHash, Address nextPkHash)
        {
            throw new NotImplementedException();
        }

        public override byte[] calculateRegNameChecksumFromUpdatedDataRecords(byte[] name, List<RegisteredNameDataRecord> dataRecords, Address nextPkHash)
        {
            throw new NotImplementedException();
        }

        public override Block getBlockHeader(ulong blockNum)
        {
            throw new NotImplementedException();
        }

        public override ulong getHighestKnownNetworkBlockHeight()
        {
            throw new NotImplementedException();
        }

        public override Block getLastBlock()
        {
            throw new NotImplementedException();
        }

        public override ulong getLastBlockHeight()
        {
            return 1;
        }

        public override int getLastBlockVersion()
        {
            return Block.maxVersion;
        }

        public override IxiNumber getMinSignerPowDifficulty(ulong blockNum)
        {
            throw new NotImplementedException();
        }

        public override Wallet getWallet(Address id)
        {
            throw new NotImplementedException();
        }

        public override IxiNumber getWalletBalance(Address id)
        {
            throw new NotImplementedException();
        }

        public override bool isAcceptingConnections()
        {
            throw new NotImplementedException();
        }

        public override void parseProtocolMessage(ProtocolMessageCode code, byte[] data, RemoteEndpoint endpoint)
        {
            throw new NotImplementedException();
        }

        public override void shutdown()
        {
            throw new NotImplementedException();
        }
    }
}
