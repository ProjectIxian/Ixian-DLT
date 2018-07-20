using DLT.Meta;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace DLT
{
    namespace Network {
        public class NetworkUtils {

            public static string resolveHostname(string hostname) {
                try
                {
                    IPHostEntry hostEntry;
                    hostEntry = Dns.GetHostEntry(hostname);

                    // TODO: handle multi-ip hostnames
                    foreach (var ip in hostEntry.AddressList)
                    {
                        // TODO: handle IPv6 as well
                        if (ip.AddressFamily == AddressFamily.InterNetwork)
                        {
                            return ip.ToString();
                        }
                    }
                }
                catch (Exception)
                {
                    return hostname;
                }

                return "";
            }

            // Retrieves all connectable neighbors of this node
            public static string[] getNeighbors() {
                List<String> result = new List<String>();

                // Retrieve the network manager clients
                string[] network_clients = NetworkClientManager.getConnectedClients();
                foreach (string client in network_clients)
                {
                    result.Add(client);
                }

                // Retrieve the network server clients as well
                lock (NetworkServer.neighborClients)
                {
                    string[] network_server_clients = NetworkServer.neighborClients.ToArray();
                    foreach (string client in network_server_clients)
                    {
                        result.Add(client);
                    }
                }

                return result.ToArray();
            }


            // Prepare neighbor data for sending
            public static byte[] getNeighborsData() {
                string[] neighbors = getNeighbors();

                using (MemoryStream m = new MemoryStream())
                {
                    using (BinaryWriter writer = new BinaryWriter(m))
                    {
                        // Write the number of neighbors
                        int num_neighbors = neighbors.Count();
                        writer.Write(num_neighbors);

                        // Write each connected neighbor
                        foreach (string neighbor in neighbors)
                        {
                            writer.Write(neighbor);
                        }
                    }
                    return m.ToArray();
                }
            }

            // Process received neighbor data
            public static void processNeighborsData(byte[] bytes) {
                using (MemoryStream m = new MemoryStream(bytes))
                {
                    using (BinaryReader reader = new BinaryReader(m))
                    {
                        // Read the number of neighbors
                        int num_neighbors = reader.ReadInt32();
                        if (num_neighbors < 0)
                            return;
                        try
                        {
                            for (int i = 0; i < num_neighbors; i++)
                            {
                                string neighbor = reader.ReadString();

                                Console.WriteLine("Adding neighbor {0}", neighbor);
                                //NetworkClientManager.connectTo(neighbor);
                            }
                        }
                        catch (Exception e)
                        {
                            Logging.error(string.Format("Error reading neighbor data: {0}", e.ToString()));
                        }

                    }
                }
            }
        }

    }

}
