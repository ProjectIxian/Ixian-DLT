using DLT.Meta;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DLT
{
    class NodeLegacy
    {


        // Called when starting a node
        // Upgrades database, wallet or any other generated files
        public static void upgrade()
        {

            return;
        }


        // Conveniency method
        public static bool isLegacy()
        {
            return Legacy.isLegacy(Node.blockChain.getLastBlockNum());
        }

    }
}
