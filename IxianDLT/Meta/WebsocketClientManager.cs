using System;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using IXICore.Meta;
using Newtonsoft.Json.Linq;

namespace DLT.Meta
{
    public class WebSocketClientManager
    {
       private static readonly Lazy<WebSocketClientManager> lazy =
            new Lazy<WebSocketClientManager>(() => new WebSocketClientManager());

        public static WebSocketClientManager Instance { get { return lazy.Value; } }

        private ClientWebSocket webSocket;
        private Uri serverUri;

        public delegate void MessageReceivedHandler(string message);
        public event MessageReceivedHandler OnMessageReceived;
        public int ProcessedRequests { get; private set; } = 0;
        public bool IsConnected { get; private set; } = false;
        public bool IsReconnecting { get; private set; } = false;
        private bool awaitingPong = false;
        private int HeartbeatDuration = 60000;
        private int TryReconnectDuration = 10000;
        public int MaxReconnectAttempts = 100;
        public int ReconnectionAttempts = 0;
        public DateTime LastPongTime { get; private set; } = DateTime.MinValue;

        // Make the constructor private to prevent external instantiation.
        private WebSocketClientManager()
        {
            webSocket = new ClientWebSocket();
        }

        // Add a public setup method to initialize the WebSocketClientManager with a server URL.
        public void Setup(string serverUrl)
        {
            serverUri = new Uri(serverUrl);
        }


        public class ParsedMessage
        {
            public string command { get; set; }
            public string type { get; set; }
            public string id { get; set; }
            public string message { get; set; }

            public object data { get; set; }
        }



        public async Task ConnectAsync()
        {

            try
            {
                await webSocket.ConnectAsync(serverUri, CancellationToken.None);
                IsConnected = webSocket.State == WebSocketState.Open;
                ReconnectionAttempts = 0;
                IsReconnecting = false;


                Logging.info("Connected to the WebSocket server.\n");

                await AuthenticateAsync();

                _ = Task.Run(ReceiveAsync);
                _ = Task.Run(StartHeartbeat);


            }
            catch (Exception ex)
            {
                Logging.error($"Error initializing WebSocketClientManager: {ex.Message}");
                await Reconnect();

            }

        }
        private async Task StartHeartbeat()
        {
            while (true) // Consider a condition to exit this loop if the application is shutting down
            {
                // Check if the WebSocket is not in an open state
                if (webSocket.State != WebSocketState.Open)
                {


                    Logging.error("WebSocket state is not open. Attempting to reconnect...");
                    IsConnected = false;
                    awaitingPong = false; // Reset awaitingPong to avoid immediately entering reconnection logic upon reconnection
                    await Reconnect();

                }
                else
                {
                    // If in open state and not awaiting pong, send a ping
                    if (!awaitingPong)
                    {
                        ParsedMessage pingMessage = new ParsedMessage
                        {
                            command = "HandlePing",
                            data = (object)null,
                            type = "request",
                            message = "",
                            id = null // Consider generating a unique ID for tracking
                        };
                        await SendMessageAsync(pingMessage);
                        awaitingPong = true;
                        LastPongTime = DateTime.UtcNow;
                    }
                    else
                    {
                        var elapsedSinceLastPong = (DateTime.UtcNow - LastPongTime).TotalMilliseconds;
                        // Check if the elapsed time since the last pong is greater than the heartbeat duration
                        if (elapsedSinceLastPong > HeartbeatDuration)
                        {
                            Logging.info($"Pong not received within {HeartbeatDuration} ms. Attempting to reconnect...");
                            IsConnected = false;
                            awaitingPong = false; // Reset awaitingPong to avoid immediately entering reconnection logic upon reconnection
                            await Reconnect();
                        }
                    }
                }

                // Wait for the next cycle
                await Task.Delay(HeartbeatDuration);
            }
        }



        private async Task Reconnect()
        {
            // Attempt to reconnect with a backoff strategy
            while (!IsConnected && ReconnectionAttempts < MaxReconnectAttempts)
            {
                ReconnectionAttempts++;
                IsReconnecting = true;
                Logging.info($"Attempting to reconnect, attempt {ReconnectionAttempts}.");

                // Dispose of the previous ClientWebSocket and create a new one
                if (webSocket != null || (webSocket.State != WebSocketState.Closed && webSocket.State != WebSocketState.None))
                {
                    webSocket.Dispose();  // Dispose the existing instance if it exists
                }
                webSocket = new ClientWebSocket();  // Create a new instance

                try
                {
                    await Task.Delay(TryReconnectDuration * ReconnectionAttempts); // Increasing delay with each attempt
                    await ConnectAsync(); // Attempt to reconnect
                }
                catch (Exception ex)
                {
                    Logging.error($"Reconnect attempt failed: {ex.Message}");
                }

                if (IsConnected) break; // Exit the loop if successfully reconnected
            }

            // Reset the reconnecting flag once the loop exits, either due to successful connection or max attempts reached
            IsReconnecting = false;
            Logging.warn("Max attempts reached to reconnect to the websocket server. please restart your node");
        }


        private async Task AuthenticateAsync()
        {
            ParsedMessage authData = new ParsedMessage
            {
                command = "authenticate",
                type = "request",
                data = new { username = Config.websocketUsername, password = Config.websocketPassword },
                message = "",
                id = null
            };
            await SendMessageAsync(authData);
        }

        public async Task SendMessageAsync(ParsedMessage data)
        {
            string message = Newtonsoft.Json.JsonConvert.SerializeObject(data);
            ArraySegment<byte> bytesToSend = new ArraySegment<byte>(Encoding.UTF8.GetBytes(message));
            await webSocket.SendAsync(bytesToSend, WebSocketMessageType.Text, true, CancellationToken.None);
            if (!(data.command == "HandlePing" || data.command == "authenticate" || data.type == "ping" || data.type == "pong" || data.command == "HandleSync"))
            {
                ProcessedRequests++;
            }
        }


        public async Task ReceiveAsync()
        {
            var buffer = new byte[1024 * 4];
            while (webSocket.State == WebSocketState.Open)
            {
                var result = await webSocket.ReceiveAsync(new ArraySegment<byte>(buffer), CancellationToken.None);
                if (result.MessageType == WebSocketMessageType.Close)
                {
                    await SetDisconnectedState();
                }
                else
                {
                    var message = Encoding.UTF8.GetString(buffer, 0, result.Count);
                    var parsedMessage = ParseMessage(message);

                    if (parsedMessage.command == "pong")
                    {
                        HandlePong(parsedMessage.id);
                    }
                    OnMessageReceived?.Invoke(message);
                }
            }

            // If the loop exits, the connection is considered closed.
            await SetDisconnectedState();
        }

        public async Task SetDisconnectedState()
        {
            await webSocket.CloseAsync(WebSocketCloseStatus.NormalClosure, string.Empty, CancellationToken.None);
            IsConnected = false;
            Logging.info("WebSocket connection lost. Attempting to reconnect...");
            await Reconnect();
        }

        public void HandlePong(string messageId)
        {
            if (awaitingPong)
            {
                awaitingPong = false; // Acknowledge the pong response
                LastPongTime = DateTime.UtcNow;
                Logging.info($"Pong received from ixiwss. id - {messageId}.");
                // Reset reconnection attempts since we received a pong
                if (ReconnectionAttempts != 0)
                {
                    ReconnectionAttempts = 0;

                }
            }
            else
            {
                Logging.warn("Received a Websocket pong without requesting with ping");
            }
        }


        public ParsedMessage ParseMessage(string message)
        {
            var msg = JObject.Parse(message);
            return new ParsedMessage
            {
                command = (string)msg["command"],
                type = (string)msg["type"],
                id = (string)msg["id"],
                message = (string)msg["message"],
                data = msg["data"] // Extract data
            };
        }


    }
}
