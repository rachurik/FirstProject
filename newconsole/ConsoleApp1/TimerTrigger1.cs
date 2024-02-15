using System;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using System.Text.RegularExpressions;
using Google.Api.Gax.ResourceNames;
using Google.Cloud.Logging.V2;
using Google.Cloud.PubSub.V1;

namespace my.Function
{
    public class TimerTrigger1
    {
        private readonly ILogger _logger;

         private static readonly string WorkspaceId = "b2abed1e-dd56-4ae6-826e-99bb1d0804e1";
       
        private static readonly string SharedKey = "EZNLtQUeTZQlUpBlQqLdR2R2N82ihN2YJyRFZH5qjNoHTlNaCIGfCpznMmFgPNIkfpGbw0dRhT3bM1lJ+O+O+g==";

        private static readonly string LogType = "GCP_IAM";
        private static readonly int ScriptExecutionIntervalMinutes = 5;
        private static readonly int MaxPeriodMinutes = 60 * 12;
        private static string LogAnalyticsUri = "https://b2abed1e-dd56-4ae6-826e-99bb1d0804e1.ods.opinsights.azure.com";
        public TimerTrigger1(ILoggerFactory loggerFactory)
        {
            _logger = loggerFactory.CreateLogger<TimerTrigger1>();
             if (string.IsNullOrWhiteSpace(LogAnalyticsUri))
            {
                LogAnalyticsUri = $"https://{WorkspaceId}.ods.opinsights.azure.com";
            }

            string pattern = @"https:\/\/([\w\-]+)\.ods\.opinsights\.azure.([a-zA-Z\.]+)$";
            Match match = Regex.Match(LogAnalyticsUri, pattern);
            if (!match.Success)
            {
                throw new Exception("Invalid Log Analytics Uri.");
            }
        }

        [Function("TimerTrigger1")]
        public async Task Run([TimerTrigger("0 */1 * * * *")] TimerInfo myTimer)
        {
            _logger.LogInformation($"C# Timer trigger function executed at: {DateTime.Now}");

            //  LoggingServiceV2Client gcpCli = new LoggingServiceV2ClientBuilder().Build();

            // LogName logName = new("cogent-scion-303202","projects/cogent-scion-303202/logs/cloudaudit.googleapis.com%2Fdata_access");
            // ProjectName projectName = new("cogent-scion-303202");
            
   
            if (myTimer.ScheduleStatus is not null)
            {
                _logger.LogInformation($"Next timer schedule at: {myTimer.ScheduleStatus.Next}");
            }

            Environment.SetEnvironmentVariable("GOOGLE_APPLICATION_CREDENTIALS", Path.Combine(Environment.CurrentDirectory, "local.settings.json"));
            string projectId = "cogent-scion-303202";
            string topicId = "TestPubSubTopic";
            string subscriptionId = "TestPubSubTopic-sub";
            ProjectName projectName = ProjectName.FromProject(projectId);

            var subscriptionName = new SubscriptionName(projectId, subscriptionId);
            while (true)
            {
                var subscriber = await SubscriberClient.CreateAsync(subscriptionName);
                try
                {
                    await subscriber.StartAsync((msg, ct) =>
                    {
                        Console.WriteLine(msg.Data.ToStringUtf8());
                        Console.WriteLine(LogAnalyticsUri.ToString());
                    AzureSentinelConnector sentinel = new AzureSentinelConnector(LogAnalyticsUri, WorkspaceId, SharedKey, LogType, 3000);
                    StateManager stateManager = new(Environment.GetEnvironmentVariable("AzureWebJobsStorage"));
                    (string lastTs, string endtime) = GetLastTs(stateManager, _logger);

                        string filt = ""; // TODO: Define the filter based on lastTs and endtime

                        string lastMaxTs = null;
                       
                        if (!string.IsNullOrEmpty(lastMaxTs))
                        {
                            _logger.LogInformation($"Saving max last timestamp - {lastMaxTs}");
                            stateManager.Post(lastMaxTs);
                        }
                        else
                        {
                            _logger.LogInformation("No new events found");
                            stateManager.Post(endtime);
                        }
                     return Task.FromResult(SubscriberClient.Reply.Ack);
                    });
                }
                catch (Exception e)
                {
                    Console.WriteLine($"Exception: {e}");
                }
            }
        }

        private static (string, string) GetLastTs(StateManager stateManager, ILogger log)
        {
            log.LogInformation("Getting last timestamp");
            string lastTs = stateManager.Get();
            DateTime now = DateTime.UtcNow;
            if (!string.IsNullOrEmpty(lastTs))
            {
                lastTs = DateTime.Parse(lastTs).ToUniversalTime().ToString("o");
                log.LogInformation($"Last timestamp - {lastTs}");
            }
            else
            {
                lastTs = now.AddMinutes(-ScriptExecutionIntervalMinutes).ToString("o");
                log.LogInformation("Last timestamp is not known");
            }
            double diffSeconds = (now - DateTime.Parse(lastTs)).TotalSeconds;
            DateTime endtime;
            if (diffSeconds > MaxPeriodMinutes * 60)
            {
                DateTime oldLastTs = DateTime.Parse(lastTs);
                lastTs = oldLastTs.AddMilliseconds(1).ToString("o");
                endtime = Min(oldLastTs.AddMinutes(MaxPeriodMinutes), now.AddMinutes(-1));
                log.LogInformation($"Last timestamp {oldLastTs:o} is older than max search period ({MaxPeriodMinutes} minutes). Getting data (from {lastTs} to {endtime:o})");
            }
            else
            {
                lastTs = DateTime.Parse(lastTs).AddMilliseconds(1).ToString("o");
                endtime = now.AddMinutes(-1);
                log.LogInformation($"Getting data from {lastTs} to {endtime:o}");
            }
            return (lastTs, endtime.ToString("o"));
        }

        private static DateTime Min(DateTime a, DateTime b)
        {
            return a < b ? a : b;
        }
    }
}
