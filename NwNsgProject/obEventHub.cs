using Microsoft.Azure.EventHubs;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;
using System.Collections.Generic;

namespace nsgFunc
{
    public partial class Util
    {
        private static Lazy<EventHubClient> LazyEventHubConnection = new Lazy<EventHubClient>(() =>
        {
            string EventHubConnectionString = GetEnvironmentVariable("eventHubConnection");
            string EventHubName = GetEnvironmentVariable("eventHubName");

            var connectionStringBuilder = new EventHubsConnectionStringBuilder(EventHubConnectionString)
            {
                EntityPath = EventHubName
            };
            var eventHubClient = EventHubClient.CreateFromConnectionString(connectionStringBuilder.ToString());

            return eventHubClient;
        });

        public static async Task<int> obEventHub(string newClientContent, ILogger log)
        {
            var eventHubClient = LazyEventHubConnection.Value;
            int bytesSent = 0;

            foreach (var bundleOfMessages in bundleMessageListsJson(newClientContent, log))
            {
                await eventHubClient.SendAsync(new EventData(Encoding.UTF8.GetBytes(bundleOfMessages)));
                bytesSent += bundleOfMessages.Length;
            }
            return bytesSent;
        }

        static System.Collections.Generic.IEnumerable<string> bundleMessageListsJson(string newClientContent, ILogger log)
        {
            foreach (var messageList in denormalizedRecords(newClientContent, null, log))
            {
                var outgoingRecords = new OutgoingRecords();
                outgoingRecords.records = messageList;

                var outgoingJson = JsonConvert.SerializeObject(outgoingRecords, new JsonSerializerSettings
                {
                    NullValueHandling = NullValueHandling.Ignore
                });

                yield return outgoingJson;
            }
        }
        
        class ThrowTest3
{
    static void ProcessString(string s)
    {
        if (s == null)
        {
            throw new ArgumentNullException();
        }
    }

    static void Main()
    {
        try
        {
            string s = null;
            ProcessString(s);
        }
        // Most specific:
        catch (ArgumentNullException e)
        {
            Console.WriteLine("{0} First exception caught.", e);
        }
        // Least specific:
        catch (Exception e)
        {
            Console.WriteLine("{0} Second exception caught.", e);
        }
    }
}
/*
 Output:
 System.ArgumentNullException: Value cannot be null.
 at Test.ThrowTest3.ProcessString(String s) ... First exception caught.
*/
        
    }
}
