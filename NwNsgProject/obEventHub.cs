﻿using Microsoft.Azure.EventHubs;
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
            try 
            {
            var connectionStringBuilder = new EventHubsConnectionStringBuilder(EventHubConnectionString)
            {
                EntityPath = EventHubName
            };
            }
            catch (Exception e) {
                Console.WriteLine("{0} evh 1 error", e);
            }
            
            try
            {
            var eventHubClient = EventHubClient.CreateFromConnectionString(connectionStringBuilder.ToString());
            }
            catch (Exception e) {
                Console.WriteLine("{0} evh 2 error", e);
            }

            return eventHubClient;
        });

        public static async Task<int> obEventHub(string newClientContent, ILogger log)
        {
            
            var eventHubClient = LazyEventHubConnection.Value;
            int bytesSent = 0;

            foreach (var bundleOfMessages in bundleMessageListsJson(newClientContent, log))
            {
                try 
                {
                await eventHubClient.SendAsync(new EventData(Encoding.UTF8.GetBytes(bundleOfMessages)));
                bytesSent += bundleOfMessages.Length;
                }
                catch (Exception e) {
                    Console.WriteLine("{0} evh 3 error", e);
                }
            }
            
            return bytesSent;
        }

        static System.Collections.Generic.IEnumerable<string> bundleMessageListsJson(string newClientContent, ILogger log)
        {
            try 
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
            } catch (Exception e) {
                Console.WriteLine("{0} evh 4 error", e); 
                                  }
        }
        
    }
}
