using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NLog;
using System.Net.Sockets;
using System.IO;

namespace Demo
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Fluentd Connection Demo");
            Console.WriteLine("-----------------------");
            Console.WriteLine("Please choose an option:");
            Console.WriteLine("1. Send structured log via NLog (Uses MsgPack, requires 'forward' input in Fluentd)");
            Console.WriteLine("2. Send raw JSON string via TCP (Requires 'tcp' input with 'format json' in Fluentd)");
            
            Console.Write("Enter choice (1 or 2): ");
            var choice = Console.ReadLine();

            string host = "192.168.100.17";
            int port = 24224;

            Console.Write($"Enter Fluentd Host [{host}]: ");
            var inputHost = Console.ReadLine();
            if (!string.IsNullOrWhiteSpace(inputHost)) host = inputHost;

            Console.Write($"Enter Fluentd Port [{port}]: ");
            var inputPort = Console.ReadLine();
            if (!string.IsNullOrWhiteSpace(inputPort) && int.TryParse(inputPort, out int p)) port = p;

            if (choice == "1")
            {
                SendNLogMessage(host, port);
            }
            else if (choice == "2")
            {
                SendRawJson(host, port);
            }
            else
            {
                Console.WriteLine("Invalid choice.");
            }

            Console.WriteLine("\nPress any key to exit...");
            Console.ReadKey();
        }

        static void SendNLogMessage(string host, int port)
        {
            Console.WriteLine($"\nConfiguring NLog to send to {host}:{port}...");

            var config = new NLog.Config.LoggingConfiguration();
            using (var fluentdTarget = new NLog.Targets.Fluentd())
            {
                fluentdTarget.Host = host;
                fluentdTarget.Port = port;
                fluentdTarget.Tag = "ceet-ufps-rd-gve-engineering";
                fluentdTarget.AppName = "ceet-ufps-rd-gve-engineering";
                fluentdTarget.UseTcpJson = true; // Use TCP JSON mode to match Java LogstashTcpSocketAppender
                fluentdTarget.IncludeAllProperties = true; // Send properties as fields
                
                // You can also try JsonLayout if you want the 'message' field to be JSON string
                // fluentdTarget.Layout = new NLog.Layouts.JsonLayout() { IncludeAllProperties = true };

                config.AddTarget("fluentd", fluentdTarget);
                config.LoggingRules.Add(new NLog.Config.LoggingRule("demo", LogLevel.Debug, fluentdTarget));
                
                var loggerFactory = new LogFactory(config);
                var logger = loggerFactory.GetLogger("demo");

                // Create a log event with properties
                var eventInfo = new LogEventInfo(LogLevel.Info, "demo", "Begin Routing Request");
                // eventInfo.Properties["UserId"] = 101;
                // eventInfo.Properties["UserName"] = "Tester";
                // eventInfo.Properties["Action"] = "Login";
                // eventInfo.Properties["Details"] = "Verifying ES acceptance";

                logger.Log(eventInfo);
                Console.WriteLine("Message sent via NLog.");
            }
        }

        static void SendRawJson(string host, int port)
        {
            Console.WriteLine($"\nSending raw JSON to {host}:{port}...");
            try
            {
                using (var client = new TcpClient(host, port))
                using (var stream = client.GetStream())
                using (var writer = new StreamWriter(stream, Encoding.UTF8))
                {
                    // Construct a JSON object
                    // Note: Ensure this matches the expected format of your Fluentd source
                    var timestamp = DateTime.UtcNow.ToString("yyyy-MM-dd'T'HH:mm:ss.fff'Z'");
                    string json = $@"{{""@timestamp"":""2026-01-20T12:59:48.593Z"",""@version"":""1"",""SW_CTX"":""N/A"",""TID"":""N/A"",""hostname"":""DONG"",""appName"":""ceet-ufps-rd-gve-engineering"",""level"":""INFO"",""level_value"":20000,""logger_name"":""Microsoft.AspNetCore.Hosting.Diagnostics"",""thread_name"":"".NET TP Worker"",""message"":""Request finished HTTP/1.1 GET http://localhost:18849/ - 200 - text/html;+charset=utf-8 5668.8544ms""}}";

                    // Remove newlines to send as single line JSON
                    // json = json.Replace(Environment.NewLine, " ").Replace("\n", " ").Replace("\r", " ");

                    
                    writer.Write(json + "\n"); // 只加 \n，不加 \r
                    writer.Flush();
                    
                    Console.WriteLine("Data sent: " + json);
                    Console.WriteLine("Check your Fluentd/ES to verify reception.");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error sending data: " + ex.Message);
                Console.WriteLine("Ensure Fluentd is running and listening on the specified port (TCP input).");
            }
        }
    }
}
