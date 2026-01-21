// NLog.Targets.Fluentd
// 
// Copyright (c) 2014 Moriyoshi Koizumi and contributors.
// 
// This file is licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//    http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Net.Sockets;
using System.Diagnostics;
using System.Reflection;
using MsgPack;
using MsgPack.Serialization;

namespace NLog.Targets
{
    internal class OrdinaryDictionarySerializer : MessagePackSerializer<IDictionary<string, object>>
    {
        private readonly SerializationContext embeddedContext;

        internal OrdinaryDictionarySerializer(SerializationContext ownerContext, SerializationContext embeddedContext) : base(ownerContext)
        {
            this.embeddedContext = embeddedContext ?? ownerContext;
        }

        protected override void PackToCore(Packer packer, IDictionary<string, object> objectTree)
        {
            packer.PackMapHeader(objectTree);
            foreach (KeyValuePair<string, object> pair in objectTree)
            {
                packer.PackString(pair.Key);
                if (pair.Value == null)
                {
                    packer.PackNull();
                }
                else
                {
                    packer.Pack(pair.Value, this.embeddedContext);
                }
            }
        }

        protected void UnpackTo(Unpacker unpacker, IDictionary<string, object> dict, long mapLength)
        {
            for (long i = 0; i < mapLength; i++)
            {
                string key;
                MessagePackObject value;
                if (!unpacker.ReadString(out key))
                {
                    throw new InvalidMessagePackStreamException("string expected for a map key");
                }
                if (!unpacker.ReadObject(out value))
                {
                    throw new InvalidMessagePackStreamException("unexpected EOF");
                }
                if (unpacker.LastReadData.IsNil)
                {
                    dict.Add(key, null);
                }
                else if (unpacker.IsMapHeader)
                {
                    long innerMapLength = value.AsInt64();
                    var innerDict = new Dictionary<string, object>();
                    UnpackTo(unpacker, innerDict, innerMapLength);
                    dict.Add(key, innerDict);
                }
                else if (unpacker.IsArrayHeader)
                {
                    long innerArrayLength = value.AsInt64();
                    var innerArray = new List<object>();
                    UnpackTo(unpacker, innerArray, innerArrayLength);
                    dict.Add(key, innerArray);
                }
                else
                {
                    dict.Add(key, value.ToObject());
                }
            }
        }

        protected void UnpackTo(Unpacker unpacker, IList<object> array, long arrayLength)
        {
            for (long i = 0; i < arrayLength; i++)
            {
                MessagePackObject value;
                if (!unpacker.ReadObject(out value))
                {
                    throw new InvalidMessagePackStreamException("unexpected EOF");
                }
                if (unpacker.IsMapHeader)
                {
                    long innerMapLength = value.AsInt64();
                    var innerDict = new Dictionary<string, object>();
                    UnpackTo(unpacker, innerDict, innerMapLength);
                    array.Add(innerDict);
                }
                else if (unpacker.IsArrayHeader)
                {
                    long innerArrayLength = value.AsInt64();
                    var innerArray = new List<object>();
                    UnpackTo(unpacker, innerArray, innerArrayLength);
                    array.Add(innerArray);
                }
                else
                {
                    array.Add(value.ToObject());
                }
            }
        }

        public void UnpackTo(Unpacker unpacker, IDictionary<string, object> collection)
        {
            long mapLength;
            if (!unpacker.ReadMapLength(out mapLength))
            {
                throw new InvalidMessagePackStreamException("map header expected");
            }
            UnpackTo(unpacker, collection, mapLength);
        }

        protected override IDictionary<string, object> UnpackFromCore(Unpacker unpacker)
        {
            if (!unpacker.IsMapHeader)
            {
                throw new InvalidMessagePackStreamException("map header expected");
            }

            var retval = new Dictionary<string, object>();
            UnpackTo(unpacker, retval);
            return retval;
        }

        public void UnpackTo(Unpacker unpacker, object collection)
        {
            var dictionary = collection as IDictionary<string, object>;
            if (dictionary == null)
                throw new NotSupportedException();
            UnpackTo(unpacker, dictionary);
        }
    }

    internal class FluentdEmitter
    {
        private static DateTime unixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        private readonly Packer packer;
        private readonly SerializationContext serializationContext;
        private readonly Stream destination;

        public void Emit(DateTime timestamp, string tag, IDictionary<string, object> data)
        {
            long unixTimestamp = timestamp.ToUniversalTime().Subtract(unixEpoch).Ticks / 10000000;
            this.packer.PackArrayHeader(3);
            this.packer.PackString(tag, Encoding.UTF8);
            this.packer.Pack((ulong)unixTimestamp);
            this.packer.Pack(data, serializationContext);
            this.destination.Flush();    // Change to packer.Flush() when packer is upgraded
        }

        public FluentdEmitter(Stream stream)
        {
            this.destination = stream;
            this.packer = Packer.Create(destination);
            var embeddedContext = new SerializationContext(this.packer.CompatibilityOptions);
            embeddedContext.Serializers.Register(new OrdinaryDictionarySerializer(embeddedContext, null));
            this.serializationContext = new SerializationContext(PackerCompatibilityOptions.PackBinaryAsRaw);
            this.serializationContext.Serializers.Register(new OrdinaryDictionarySerializer(this.serializationContext, embeddedContext));
        }
    }

    [Target("Fluentd")]
    public class Fluentd : NLog.Targets.TargetWithLayout
    {
        public string Host { get; set; }

        public int Port { get; set; }

        public string Tag { get; set; }

        public bool NoDelay { get; set; }

        public int ReceiveBufferSize { get; set; }

        public int SendBufferSize { get; set; }

        public int SendTimeout { get; set; }

        public int ReceiveTimeout { get; set; }

        public bool LingerEnabled { get; set; }

        public int LingerTime { get; set; }

        public bool EmitStackTraceWhenAvailable { get; set; }

        public bool IncludeAllProperties { get; set; }

        public string AppName { get; set; }

        public bool UseTcpJson { get; set; }

        private TcpClient client;

        private Stream stream;

        private FluentdEmitter emitter;

        protected override void InitializeTarget()
        {
            base.InitializeTarget();
        }

        private void InitializeClient()
        {
            this.client = new TcpClient();
            this.client.NoDelay = this.NoDelay;
            this.client.ReceiveBufferSize = this.ReceiveBufferSize;
            this.client.SendBufferSize = this.SendBufferSize;
            this.client.SendTimeout = this.SendTimeout;
            this.client.ReceiveTimeout = this.ReceiveTimeout;
            this.client.LingerState = new LingerOption(this.LingerEnabled, this.LingerTime);
        }

        protected void EnsureConnected()
        {
            if (this.client == null)
            {
                InitializeClient();
                ConnectClient();
            }
            else if (!this.client.Connected)
            {
                Cleanup();
                InitializeClient();
                ConnectClient();
            }
        }

        private void ConnectClient()
        {
            this.client.Connect(this.Host, this.Port);
            this.stream = this.client.GetStream();
            if (!this.UseTcpJson)
            {
                this.emitter = new FluentdEmitter(this.stream);
            }
        }

        protected void Cleanup()
        {
            try
            {
                this.stream?.Dispose();
                this.client?.Close();
            }
            catch (Exception ex)
            {
                NLog.Common.InternalLogger.Warn("Fluentd Close - " + ex.ToString());
            }
            finally
            {
                this.stream = null;
                this.client = null;
                this.emitter = null;
            }
        }

        protected override void Dispose(bool disposing)
        {
            Cleanup();
            base.Dispose(disposing);
        }

        protected override void CloseTarget()
        {
            Cleanup();
            base.CloseTarget();
        }

        protected override void Write(LogEventInfo logEvent)
        {
            if (this.UseTcpJson)
            {
                WriteTcpJson(logEvent);
            }
            else
            {
                WriteMsgPack(logEvent);
            }
        }

        private void WriteMsgPack(LogEventInfo logEvent)
        {
            var record = new Dictionary<string, object> {
                { "level", logEvent.Level.Name },
                { "message", Layout.Render(logEvent) },
                { "logger_name", logEvent.LoggerName },
                { "sequence_id", logEvent.SequenceID },
                { "tag", this.Tag },
                { "appName", this.AppName },
                { "@timestamp", logEvent.TimeStamp.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.ffff") }
            };
            if (this.EmitStackTraceWhenAvailable && logEvent.HasStackTrace)
            {
                var transcodedFrames = new List<Dictionary<string, object>>();
                StackTrace stackTrace = logEvent.StackTrace;
                foreach (StackFrame frame in stackTrace.GetFrames())
                {
                    var transcodedFrame = new Dictionary<string, object>
                    {
                        { "filename", frame.GetFileName() },
                        { "line", frame.GetFileLineNumber() },
                        { "column", frame.GetFileColumnNumber() },
                        { "method", frame.GetMethod().ToString() },
                        { "il_offset", frame.GetILOffset() },
                        { "native_offset", frame.GetNativeOffset() },
                    };
                    transcodedFrames.Add(transcodedFrame);
                }
                record.Add("stacktrace", transcodedFrames);
            }
            if (this.IncludeAllProperties && logEvent.Properties.Count > 0)
            {
                foreach (var property in logEvent.Properties)
                {
                    var propertyKey = property.Key.ToString();
                    if (string.IsNullOrEmpty(propertyKey))
                        continue;

                    if (record.ContainsKey(propertyKey))
                        continue;

                    record[propertyKey] = SerializePropertyValue(propertyKey, property.Value);
                }
            }

            try
            {
                EnsureConnected();
            }
            catch (Exception ex)
            {
                NLog.Common.InternalLogger.Warn("Fluentd Connect - " + ex.ToString());
                throw;  // Notify NLog of failure
            }

            try
            {
                this.emitter?.Emit(logEvent.TimeStamp, this.Tag, record);
            }
            catch (Exception ex)
            {
                NLog.Common.InternalLogger.Warn("Fluentd Emit - " + ex.ToString());
                throw;  // Notify NLog of failure
            }
        }

        private void WriteTcpJson(LogEventInfo logEvent)
        {
            var renderedMessage = Layout != null ? Layout.Render(logEvent) : logEvent.FormattedMessage;

            object swCtxProperty;
            object tidProperty;
            object appNameProperty;

            logEvent.Properties.TryGetValue("SW_CTX", out swCtxProperty);
            logEvent.Properties.TryGetValue("TID", out tidProperty);
            logEvent.Properties.TryGetValue("appName", out appNameProperty);

            var swCtx = swCtxProperty != null ? swCtxProperty.ToString() : "N/A";
            var tid = tidProperty != null ? tidProperty.ToString() : "N/A";
            var appName = appNameProperty != null ? appNameProperty.ToString() : this.AppName;
            var threadName = System.Threading.Thread.CurrentThread.Name;
            if (string.IsNullOrEmpty(threadName))
            {
                threadName = System.Threading.Thread.CurrentThread.ManagedThreadId.ToString();
            }

            var levelValue = GetLevelValue(logEvent.Level);

            var sb = new StringBuilder();
            sb.Append("{");
            sb.AppendFormat("\"@timestamp\":\"{0}\",", logEvent.TimeStamp.ToUniversalTime().ToString("yyyy-MM-dd'T'HH:mm:ss.fff'Z'"));
            sb.Append("\"@version\":\"1\",");
            sb.AppendFormat("\"hostname\":\"{0}\",", EscapeJson(Environment.MachineName));
            sb.AppendFormat("\"appName\":\"{0}\",", EscapeJson(appName));
            sb.AppendFormat("\"level\":\"{0}\",", logEvent.Level.Name.ToUpper());
            sb.AppendFormat("\"level_value\":{0},", levelValue);
            sb.AppendFormat("\"logger_name\":\"{0}\",", EscapeJson(logEvent.LoggerName));
            sb.AppendFormat("\"thread_name\":\"{0}\",", EscapeJson(threadName));
            sb.AppendFormat("\"message\":\"{0}\",", EscapeJson(renderedMessage));
            sb.AppendFormat("\"sequence_id\":{0},", logEvent.SequenceID);
            sb.AppendFormat("\"tag\":\"{0}\"", EscapeJson(this.Tag));

            if (logEvent.Exception != null)
            {
                sb.AppendFormat(",\"stack_trace\":\"{0}\"", EscapeJson(logEvent.Exception.ToString()));
            }
            
            // Append extra properties if needed
            if (this.IncludeAllProperties && logEvent.Properties.Count > 0)
            {
                foreach (var property in logEvent.Properties)
                {
                    var propertyKey = property.Key.ToString();
                    if (string.IsNullOrEmpty(propertyKey) || propertyKey == "appName")
                        continue;

                    sb.AppendFormat(",\"{0}\":\"{1}\"", EscapeJson(propertyKey), EscapeJson(property.Value?.ToString() ?? ""));
                }
            }

            sb.Append("}\n"); // End JSON and add newline

            try
            {
                EnsureConnected();
                // Send JSON string as bytes
                byte[] data = Encoding.UTF8.GetBytes(sb.ToString());
                this.stream.Write(data, 0, data.Length);
                this.stream.Flush();
            }
            catch (Exception ex)
            {
                NLog.Common.InternalLogger.Warn("Fluentd Emit TCP JSON - " + ex.ToString());
                throw; 
            }
        }

        private static string EscapeJson(string s)
        {
            if (s == null) return "";
            // Basic JSON escaping
            return s.Replace("\\", "\\\\").Replace("\"", "\\\"").Replace("\n", "\\n").Replace("\r", "\\r").Replace("\t", "\\t");
        }


        private static int GetLevelValue(LogLevel level)
        {
            if (level == LogLevel.Trace)
                return 10000;
            if (level == LogLevel.Debug)
                return 20000;
            if (level == LogLevel.Info)
                return 20000;
            if (level == LogLevel.Warn)
                return 30000;
            if (level == LogLevel.Error)
                return 40000;
            if (level == LogLevel.Fatal)
                return 50000;
            return 0;
        }

        private static object SerializePropertyValue(string propertyKey, object propertyValue)
        {
            if (propertyValue == null || Convert.GetTypeCode(propertyValue) != TypeCode.Object || propertyValue is decimal)
            {
                return propertyValue;   // immutable
            }
            else
            {
                return propertyValue.ToString();
            }
        }

        public Fluentd()
        {
            this.Host = "127.0.0.1";
            this.Port = 24224;
            this.ReceiveBufferSize = 8192;
            this.SendBufferSize = 8192;
            this.ReceiveTimeout = 1000;
            this.SendTimeout = 1000;
            this.LingerEnabled = true;
            this.LingerTime = 1000;
            this.EmitStackTraceWhenAvailable = false;
            this.IncludeAllProperties = false;
            this.UseTcpJson = false;
            this.Tag = Assembly.GetCallingAssembly().GetName().Name;
        }
    }
}
