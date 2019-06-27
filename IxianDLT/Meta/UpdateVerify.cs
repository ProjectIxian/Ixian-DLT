using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

namespace DLT
{
    namespace Meta
    {
        public class UpdateVerify
        {
            private static readonly object threadLock = new object();
            private static Thread fetchUpdateThread;
            // artificially set last check so far into the past that next check will happen in ~2 min, no matter what the interval is
            private static DateTime lastVersionCheck = DateTime.Now.AddSeconds(-Config.checkVersionSeconds + 10);

            public static readonly string IXIANSignPubKey = "<RSAKeyValue><Modulus>ncgfBMNQVDxEJSM0zmUke/HHhAi22fs0EXHGU8fhBjFH2tF1rAAi6cnjDBYBxAZzrL8DVwueLX9qPTHfz09YMkBiM4y8BB4kYw1zggSV7zAuxbpp2OKvFqcvzhiGIT0qOWAeqt62RGsls3G4RST+znPTgoZ44lL8HsHRpCHc4in6turL0Nzv02YgYZwuy85SMselUIOG93HjcLjpNJsBoBQkSxgHW89xq3FtmWWrOa35ayw/5R948+tXNGrUezq7/IDpTm7XYXTZuA6GOfsOsPWZO3F4pxb6nYEJ8kmPhX5MvCfrChH7r4M23NAYtNWO/WWJvbprBIGGCdN3noX0B0rsjDuySYGFVvBKNvXBsgpuDjcJluLKuPh/bALkzerzsf+5/D/8AQI8oi48t0jDSRZHiA5FtBtXJQhbgH3iTrOInfENIUaxkas7uIZEtblwU/XbTYDYbn9hX1Q9g8sk/IhY+cKzpp2cpBO6XDHKRw0Ptdr8bo5927lR+eIpC7XmDxAnzQlpT4hdWY6MkXNPQ3DriNnIgW5uPZiF2rPdsmczoD0/7c4QhKFpqJvSDsk+aiQVHo+cPGeac1XcQwnN6RkZS7XJjqUjnRcwJyYUnJcxfFr/sHNotwpvoCfwznhx97VwJ+mV+oR6BmP4sdNDLVixNFjubhjW3UhqQc+yWKRRSxbGptJ38zID9SEEcK/AqzxqJm/d+XbLA28WVnKEHs3kmilqyNaG++P5h3EHy4ekHKTaHgHjxUS2akwmnAjJmgu6ti49OzaPuXwMbRXbXaIHJcgrJNNLCW4IWTBNKqOzK3Oilb6E7hVVrxQ+qiWrkao0ZPxEHUl+oCtcIL5kEvlOpIr9kJ2u4MdScfqyP8bvaMwujAAzLRZSc/fkWHa7lP2bRCWBLnAPdZK+JxIvBoKmltvebZTSbNCIhvnHLOnuRiuQ3kv4VJxxMJJfcSlhEfAo/klQY+MsM3bqze/Hfqk/es23RDhHJF67nyMBmCCVJhGMKIEcgCXtfqclGRtWNsPzUxLXdWyk5uAeU2c7MSkKtibAIyYH0Qc6F1Y+gGHu4kT3XFO2U3SsGr3c+aQBPULlNFZyteRddEYzQKzG5+RtCmi73SKP2GVseih8/wKa2QOSlGQaF+ZmEQiRKazj5dpOeUOHlvsonxtED+qfWYO5b056csCFwjuNEZraO/d4mIRX7+o83EG9jnbwUgFqH5NwWugGsbNOLIxvcwjpkQZ8t86VAGe7cnscCkihkfBf7MJzJmGrdmwE4lR8CwM/25pP4l19uPTnHznB1R7P9JiCxSSy0TO8EmwgaCdee6iB6IDbVsS1o53Hilr83ZAL+Dun1fLZuDcrCwsX2QgQSQ==</Modulus><Exponent>AQAB</Exponent></RSAKeyValue>";

            public static readonly string updateUrl = "https://www.ixian.io/update.txt";

            /// <summary>
            ///  Fetching version information is currently in progress.
            /// </summary>
            public static bool inProgress { get; private set; } = false;
            /// <summary>
            ///  Resulting version string - fetched from ixian.io
            /// </summary>
            public static string serverVersion { get; private set; } = "";
            /// <summary>
            ///  Version string is ready in `serverVersion`.
            /// </summary>
            public static bool ready { get; private set; } = false;
            /// <summary>
            ///  There was an error while fetching version information, or it was signed incorrectly.
            /// </summary>
            public static bool error { get; private set; } = false;

            /// <summary>
            ///  Initiate version check asynchronously.
            /// </summary>
            public static void checkVersion()
            {
                lock (threadLock)
                {
                    if((DateTime.Now - lastVersionCheck).TotalSeconds > Config.checkVersionSeconds)
                    {
                        if (inProgress) return;
                        lastVersionCheck = DateTime.Now;
                        fetchUpdateThread = new Thread(fetchUpdateVersion);
                        inProgress = true;
                        ready = false;
                        error = false;
                        serverVersion = "";
                        fetchUpdateThread.Start();
                    }
                }
            }

            private static void fetchUpdateVersion()
            {
                System.Net.Http.HttpClient httpClient = new System.Net.Http.HttpClient();
                try
                {
                    var httpGetTask = httpClient.GetStringAsync(updateUrl);
                    httpGetTask.Wait();
                    string[] updateStrings = httpGetTask.Result.Split(';');
                    string versionText = updateStrings[0];
                    string signature = updateStrings[1];
                    if (!checkSignature(versionText, signature))
                    {
                        throw new Exception("Incorrect signature for the retrieved version text!");
                    }
                    serverVersion = versionText;
                }
                catch (Exception ex)
                {
                    Logging.warn(String.Format("Error while checking {0} for version update: {1}", updateUrl, ex.Message));
                    error = true;
                }
                finally
                {
                    inProgress = false;
                    ready = true;
                }
            }


            private static bool checkSignature(string version, string base64Sig)
            {
                byte[] signatureBytes = Convert.FromBase64String(base64Sig);
                byte[] versionBytes = ASCIIEncoding.ASCII.GetBytes(version);
                RSACryptoServiceProvider r = new RSACryptoServiceProvider();
                r.FromXmlString(IXIANSignPubKey);
                return r.VerifyData(versionBytes, signatureBytes, HashAlgorithmName.SHA512, RSASignaturePadding.Pkcs1);
            }
        }
    }
}
