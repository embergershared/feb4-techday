using Azure.Identity;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Azure.Core;

namespace QueueSender
{
    internal class Program
    {
        private static async Task Main(string[] args)
        {
            #region Initialization
            // Configuration
            var config = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
                .AddEnvironmentVariables()
                .AddCommandLine(args)
                .Build();

            // .NET Logging
            using var loggerFactory =
                LoggerFactory.Create(builder =>
                    builder.AddSimpleConsole(options =>
                    {
                        options.IncludeScopes = true;
                        options.SingleLine = true;
                        options.TimestampFormat = "HH:mm:ss ";
                    }));

            // Get a Logger for Program
            var logger = loggerFactory.CreateLogger<Program>();
            #endregion
            
            #region Getting Azure credential
            logger.LogInformation("Getting Azure credential...");

            // Reference: https://learn.microsoft.com/en-us/dotnet/azure/sdk/authentication/best-practices?tabs=aspdotnet
            var runsInAzure = false; // This is for demo purposes
            var userAssignedClientId = config.GetValue<string>("USER_ASSIGNED_CLIENT_ID"); // This is for demo purposes

            TokenCredential credential;

            if (runsInAzure)
            {
                // In Azure, use Managed Identity to authenticate
                var usesUserAssignedIdentity = !string.IsNullOrEmpty(userAssignedClientId);
                credential = usesUserAssignedIdentity ? new ManagedIdentityCredential() :  new ManagedIdentityCredential(clientId: "userAssignedClientId");
                var aiType = usesUserAssignedIdentity ? "User" : "System";
                logger.LogInformation($"Azure credential obtained using {aiType} Assigned Managed Identity credential.");
            }
            else
            {
                // local development environment authentication
                credential = new AzureCliCredential();
                logger.LogInformation("Azure credential obtained using Azure CLI credential.");
            }
            #endregion

            #region Service Bus Client & Sender
            logger.LogInformation("Creating Service Bus client and sender...");
            // The Service Bus client types are safe to cache and use as a singleton for the lifetime
            // of the application, which is best practice when messages are being published or read
            // regularly.
            //
            // Set the transport type to AmqpWebSockets so that the ServiceBusClient uses the port 443. 
            // If you use the default AmqpTcp, ensure that ports 5671 and 5672 are open.
            var sbNamespace = config.GetValue<string>("SERVICEBUS_NS_NAME");
            var sbQueue = config.GetValue<string>("QUEUE_NAME");
            var fullyQualifiedNamespace = $"{sbNamespace}.{Constants.SbPublicSuffix}";

            var clientOptions = new ServiceBusClientOptions
            {
                TransportType = ServiceBusTransportType.AmqpWebSockets
            };

            var serviceBusClient = new ServiceBusClient(
                fullyQualifiedNamespace,
                credential,
                clientOptions);
            
            var serviceBusSender = serviceBusClient.CreateSender(sbQueue);
            logger.LogInformation("Service Bus client for namespace: {} and sender to queue: {} created.", sbNamespace, sbQueue);
            #endregion

            #region Send a batch of messages
            // Ask the user to enter the number of messages to be sent to the queue
            Console.Write("Enter the number of messages to send: ");
            if (!int.TryParse(Console.ReadLine(), out var numOfMessages))
            {
                Console.WriteLine("Invalid input. Please enter a valid number.");
                return;
            }
            var timeStamp = DateTime.UtcNow.ToLocalTime().ToString("yyyy-MM-dd HH:mm:ss");

            using var messageBatch = await serviceBusSender.CreateMessageBatchAsync();

            // Create the message batch
            for (var i = 1; i <= numOfMessages; i++)
            {
                // try adding a message to the batch
                if (!messageBatch.TryAddMessage(new ServiceBusMessage($"Time stamp: {timeStamp}, Message sent in batch with number: {i}")))
                {
                    // if it is too large for the batch
                    throw new Exception($"The message {i} is too large to fit in the batch.");
                }
            }

            // Send the message batch to the queue in Azure Service Bus
            try
            {
                // Use the sender client to send the batch of messages to the Service Bus queue
                await serviceBusSender.SendMessagesAsync(messageBatch);
                logger.LogInformation("A batch of {} messages has been published to the queue.", numOfMessages);
            }
            finally
            {
                // Calling DisposeAsync on client types is required to ensure that network
                // resources and other unmanaged objects are properly cleaned up.
                await serviceBusSender.DisposeAsync();
                await serviceBusClient.DisposeAsync();
            }
            #endregion

            Console.WriteLine("Press any key to end the application");
            Console.ReadKey();
        }
    }
}
