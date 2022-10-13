using System;
using System.IO;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using sogoapi.data.Models;

namespace Sogo.SendToTransactionQueue;

public class SendToTransactionQueuePayload
{
    public bool IsNewRoundFee { get; set; }
    
    public int TokenCost { get; set; }
    public Round Round { get; set; }
    public string Source { get; set; }
}

public static class SendToTransactionQueue
{
    private static string _sbConnectionString;
    private static string _transactionQueueSbName;
    private static string _mslEntityId = "adceb3ea-52b8-4fa9-8279-633beca45417";

    [FunctionName("SendToTransactionQueue")]
    public static async Task<IActionResult> RunAsync(
        [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req, ILogger log)
    {
        log.LogInformation("C# HTTP trigger function processed a request.");

        string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
        SendToTransactionQueuePayload data = JsonConvert.DeserializeObject<SendToTransactionQueuePayload>(requestBody);

        _sbConnectionString = GetEnvironmentVariable("sbConnectionString");
        _transactionQueueSbName = GetEnvironmentVariable("transactionQueueSbName");
        ServiceBusClient sbClient = new ServiceBusClient(_sbConnectionString);

        if (data.IsNewRoundFee
            && data.Round != null
            && data.Round.EntityId == _mslEntityId //<== MSL only!
            && data.Source == "mobile_app")
        {
            //send to Transaction queue (note: the transaction service bus queue automatically
            //REMOVES duplicates to avoid charging a golfer multiple times. So its possible that 
            //the item you are placing in the queue now, will be ignored by the queue
            
            var transactionQueueObj =
                new
                {
                    TokenCost = data.TokenCost,
                    TaskType = "calc_round_fee",
                    TransactionId = string.Empty,
                    EntityId = data.Round.EntityId,
                    GolferId = data.Round.GolferId,
                    GolferEmail = data.Round.GolferEmail,
                    GolferFirstName = data.Round.GolferFirstName,
                    GolferLastName = data.Round.GolferLastName,
                    Round = data.Round
                };

            var json = JsonConvert.SerializeObject(transactionQueueObj);
            var sender = sbClient.CreateSender(_transactionQueueSbName);
            var msg = new ServiceBusMessage(json);
                        
            //!! THIS PROPERTY CONTROLS WHETHER A MESSAGE SENT TO "TRANSACTION" QUEUE IS
            //!! ONLY PROCESSED ONCE OR NOT. THE QUEUE IS SETUP TO **IGNORE** DUPLICATE MESSAGES
            //!! THE WAY IT DOES THAT IS VIA THIS MESSAGEID (BELOW). IF ITS THE SAME MESSAGE ID 
            //!! (AFTER THE FIRST SEND) THEN THE QUEUE WILL IGNORE IT COMPLETELY
            //!! THE FORMAT OF MESSAGEID IS: "<msl scorecardId>/new-round-fee"
            //!! eg. "123543234/new-round-fee". THE FIRST TIME THE QUEUE SEES THIS IT *WILL*
            //PROCESS THE MESSAGE. THE SECOND+ TIMES -> IT WILL COMPLETELY IGNORE THE MESSAGE
            msg.MessageId = $"{data.Round.ThirdPartyScorecardId}/new-round-fee";

            await sender.SendMessageAsync(msg);            
       
            log.LogInformation(
                $"Placing << MSL >> round in transaction queue to charge a new round fee");

            return new OkObjectResult(new
            {
                result = "success",
                roundId = data.Round.Id
            });
        }

        return new BadRequestObjectResult($"Unable to send round {data.Round.Id} to transaction queue for processing new round fee");
    }
    
    private static string GetEnvironmentVariable(string name)
    {
        return Environment.GetEnvironmentVariable(name, EnvironmentVariableTarget.Process);
    }
}