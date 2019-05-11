//-----------------------------------------------------------------------
// <copyright file="ChangeFeedProcessor.cs" company="Microsoft">
//      Copyright (c) Microsoft Corporation. All rights reserved. 
// </copyright>
// <author>Serena Davis</author>
//-----------------------------------------------------------------------


namespace ChangeFeedFunctions
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using Newtonsoft.Json;
    using System.Configuration;
    using System.Text;
    using Microsoft.Azure.Documents;
    //using Microsoft.Azure.EventHubs;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Azure.WebJobs.Host;
    using Microsoft.ServiceBus.Messaging;

    /// <summary>
    /// Processes events using Cosmos DB Change Feed.
    /// </summary>
    public static class ChangeFeedProcessorBulk
    {
        /// Name of the Event Hub.
        private static readonly string EventHubName = "event-hub1";

        [FunctionName("ChangeFeedProcessorBulk")]
        public static void Run(
            //change database name below if different than specified in the lab
            [CosmosDBTrigger(databaseName: "caseflow",
            //change the collection name below if different than specified in the lab
            collectionName: "cases",
            ConnectionStringSetting = "DBconnection",
            LeaseConnectionStringSetting = "DBconnection",
            LeaseCollectionName = "leases",
            CreateLeaseCollectionIfNotExists = true)]IReadOnlyList<Document> documents, TraceWriter log)
        {
            // Create variable to hold connection string to enable event hub namespace access.
#pragma warning disable CS0618 // Type or member is obsolete
            //string eventHubNamespaceConnection = ConfigurationSettings.AppSettings["EventHubNamespaceConnection"];
            string eventHubNamespaceConnection = "Endpoint = sb://tvkchangefeedeventhub7675.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=***";

            // Build connection string to access event hub within event hub namespace.

            // Create event hub client to send change feed events to event hub.
            //EventHubClient eventHubClient = EventHubClient.CreateFromConnectionString(eventHubConnectionStringBuilder.ToString());
            var eventHubConnectionString = @"Endpoint=sb://tvkeventhub.servicebus.windows.net/;SharedAccessKeyName=MyEventHub;SharedAccessKey=zmkLdp1bFnxKlL7/PdXlGJaIFSTqoLoa4trUf1wixAM=;EntityPath=eventhub";
            var eventHubClient = EventHubClient.CreateFromConnectionString(eventHubConnectionString);

            var events = new List<string>();
            // Iterate through modified documents from change feed.
            foreach (var doc in documents)
            {
                // Convert documents to json.
                //string json = JsonConvert.SerializeObject(doc);
                string json = "EndTime";

                //EventData data = new EventData(Encoding.UTF8.GetBytes(json));

                events.Add(String.Format(json));
                Console.Write(json);
            }

            // Create event hub connection.

            using (var ms = new MemoryStream())
            using (var sw = new StreamWriter(ms))
            {
                // Wrap events into JSON array:
                sw.Write("[");
                for (int i = 0; i < events.Count; ++i)
                {
                    if (i > 0)
                    {
                        sw.Write(',');
                    }
                    sw.Write(events[i]);
                }
                sw.Write("]");

                sw.Flush();
                ms.Position = 0;

                // Send JSON to event hub.
                EventData eventData = new EventData(ms);
                eventHubClient.Send(eventData);
            }



        }
    }
}