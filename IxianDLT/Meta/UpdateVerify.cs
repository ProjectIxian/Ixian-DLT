using System;
using System.Text;
using System.Threading;
using System.Security.Cryptography;

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

            public static readonly string IXIANSignPubKey = "<RSAKeyValue><Modulus>uoqtGaAoQBTkZUuxuBsusUsHYb5sy0iYJPuE+zhM9ZMySCQK3PevZvhqj9bdEv82A/cl0lYWTWk9ZU2sLAm1n6DLFwCwC8AWShv3Il8y8L4oCbDMfyBWMY9yPq5XSQnplrfR5rER/MWt+XQX/cJeEOd0prNgIYSPluo7u+h+fM2CzMfv5vFtN/E1HSxKUVhP8wkGmOoxXo1EfIqzEHlg4BO0z+hNEIdiXopvwOVkDbetWnOXnwOkOC+bcvPWFP3RYfFiez7GStJFVJhs1lc7wM3PmotzoO7/4NyuxVueydRBERhGbq+KV0FfggPshMh/+srpbu6etiLyW7KtZX8ARgxasZVHNggIRygM4LYLk/ppcvHCohEfmYsrTo1Bk0CQe1JFIbFIWXLK+5VDhpVc0w1HpeyjuB/fL8vEHBhOfuNL8frhfpFWTzzPF2IBd59E6T8TQCM/K2DBuxWAEws8nqcNpYsjocuRw2OZmbjzPcerCo2haVaezT3YbNZHAflzSxI+VmURyuXBw+76IV63gJTwGWpZ4A+/D6ubOuqrssfTBRYdjJLTecb3D7aBd2JpKVPVfSN61zSrkt++eEgkikkFVSqQ2ILGB7azzaGPCm4RL0ZMa47BSfMcPSMM4oN91mbVWSWaspCQe0TjbeIR9jl18Y1jcvHloiX8rVC5YKWPPYG8YuGcKq8U0Hp2lkeCnXqKfvUGoH/e6ufkwBCqJZle2S+wmRHKFlEuMHxs1sUEgleZcnQQ6lXYuwpoIXKB6NxMrSTPWM+QHWVnU0tL0X3MYIvCxT6olYV1H9Rfrm3p9lP1vJME7H0sudypXlsUzddMQtqLAjpS7Sgl1RJ7CwnYQa/nfrMYokaWhXdHeT0XpdANyiRdlklrHE19Jlb4Z/wpK+P4zYfrx/8Mj6J+ktxJuyHQVj1wVwH9qmsWfVvwJ4xPKBQcfS4aJ6iUnGwY1HuywETGgx4eAp4vsVjaR7VCE/JPA4kwLCKAcXAT2BwNMC3rCG+4XouyZuZjYLGGTuRkqpXQ1mTBKXSg6t9iUsc25V9k4jS1d+y7qumDT6jqsLavvvwkBKpu1ONgew9Pqbc9GLXBmXSHIhUdFiFF9d3cvWv5m3QhRKdCW0jQ8xUAlxgZW27a+09YppVwvex6/8fmhx9nldpC1I24EyyrBXWgIABAvxv0gAo0CzPTLIOh1c8oOapMaie2aAR3Epfna+q/Z/h5ofrx9HmP+xAxhKHOHrRWo+EybceoWh/mmMRjnqmm1G4DrHsW90Z56qZWdX4QGMXpP9ZhVvxqFj1IxWkzOHk5XAOq44UQq2hFT46bAbe+s5JSOGfvMzzagFC5LuuAeelFTUfRR7wFClM5kQ==</Modulus><Exponent>AQAB</Exponent></RSAKeyValue>";

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
