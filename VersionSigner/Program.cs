using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Security.Cryptography;
using System.IO;

namespace VersionSigner
{
    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                if (args.Count() < 1)
                {
                    Console.WriteLine("Usage: sign string");
                    return;
                }
                if (args[0] == "generate")
                {
                    Console.WriteLine("Generating RSA key...");
                    if (File.Exists("private_key.xml"))
                    {
                        Console.WriteLine("Refusing to overwrite existing private key.");
                        return;
                    }
                    RSACryptoServiceProvider rsa = new RSACryptoServiceProvider(8192);
                    string xml = rsa.ToXmlString(true);
                    using (var f = File.CreateText("private_key.xml"))
                    {
                        f.Write(xml);
                        f.Close();
                    }
                }
                if (args[0] == "sign")
                {
                    if (args.Count() < 2)
                    {
                        Console.WriteLine("Usage: sign string");
                        return;
                    }
                    if (!File.Exists("private_key.xml"))
                    {
                        Console.WriteLine("There is no private key. Call with 'generate' first.");
                        return;
                    }
                    RSACryptoServiceProvider rsa = new RSACryptoServiceProvider();
                    using (var f = File.OpenText("private_key.xml"))
                    {
                        rsa.FromXmlString(f.ReadToEnd());
                        f.Close();
                    }
                    string sign_data = args[1];
                    byte[] sign_data_bytes = Encoding.UTF8.GetBytes(sign_data);
                    byte[] signature = rsa.SignData(sign_data_bytes, HashAlgorithmName.SHA512, RSASignaturePadding.Pkcs1);
                    string base64_signature = Convert.ToBase64String(signature);
                    using (var f = File.CreateText("update.txt"))
                    {
                        f.Write(sign_data);
                        f.Write(";");
                        f.Write(base64_signature);
                        f.Close();
                    }
                    string pub_xml = rsa.ToXmlString(false);
                    Console.WriteLine("Pubkey:");
                    Console.WriteLine(pub_xml);

                }
            } catch(Exception ex)
            {
                Console.WriteLine("Error occured: " + ex.Message);
            }
        }
    }
}
