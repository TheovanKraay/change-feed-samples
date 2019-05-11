
namespace ChangeFeedFunctions
{
    using System.Collections.Generic;
    using System.Text;
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.EventHubs;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Azure.WebJobs.Host;
    using Newtonsoft.Json;

    /// <summary>
    /// Processes events using Cosmos DB Change Feed.
    /// </summary>
    public static class ChangeFeedProcessor
    {
        private static readonly string EventHubName = "event-hub1";

        [FunctionName("ChangeFeedProcessor")]
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

            string eventHubNamespaceConnection = "Endpoint = sb://tvkchangefeedeventhub7675.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=***";

            // Build connection string to access event hub within event hub namespace.
            EventHubsConnectionStringBuilder eventHubConnectionStringBuilder =
                new EventHubsConnectionStringBuilder(eventHubNamespaceConnection)
                {
                    EntityPath = EventHubName
                };

            // Create event hub client to send change feed events to event hub.
            EventHubClient eventHubClient = EventHubClient.CreateFromConnectionString(eventHubConnectionStringBuilder.ToString());

            // Iterate through modified documents from change feed.
            foreach (var doc in documents)
            {
                // Convert documents to json.
                string json = JsonConvert.SerializeObject(doc);
                EventData data = new EventData(Encoding.UTF8.GetBytes(json));

                // Use Event Hub client to send the change events to event hub.
                eventHubClient.SendAsync(data);
            }
        }
    }
}