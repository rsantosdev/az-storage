using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using Newtonsoft.Json;
using System;
using System.Threading.Tasks;

namespace Console.Queues
{
    class Program
    {
        private const string name = "envia-email";

        static async Task Main(string[] args)
        {
            // Criar o objeto da conta
            var storageAccount = CloudStorageAccount.Parse("suaConnectionString");

            // Criar QueueClient
            var queueClient = storageAccount.CreateCloudQueueClient();

            // Envia uma mensagem pra fila
            await EnviaMensagemJson(queueClient);
            //await EnviaMensagem(queueClient);

            // Recupera numero de mensagens na fila
            await ContaMensagens(queueClient);

            // Checar mensagens sem marcalas como lida
            //await ChecaMensagens(queueClient);

            // Processar as mensagens
            await ProcessaMensagensJson(queueClient);
            //await ProcessaMensagens(queueClient);

            // Recupera numero de mensagens na fila
            await ContaMensagens(queueClient);
        }

        private static async Task ChecaMensagens(CloudQueueClient queueClient)
        {
            var queue = queueClient.GetQueueReference(name);

            var messsages = await queue.PeekMessagesAsync(10);
            foreach (var msg in messsages)
            {
                // Envio de Email
                var content = msg.AsString;
                System.Console.WriteLine($"Enviando email do cliente: {content}");
            }
        }

        private static async Task ProcessaMensagens(CloudQueueClient queueClient)
        {
            var queue = queueClient.GetQueueReference(name);

            var messsages = await queue.GetMessagesAsync(10);
            //var messsages = await queue.GetMessagesAsync(10, TimeSpan.FromSeconds(5), new QueueRequestOptions(), new OperationContext());
            foreach (var msg in messsages)
            {
                // Informações da Mensagem
                System.Console.WriteLine($"Próxima leitura disponivel: {msg.NextVisibleTime.Value.ToLocalTime()}");

                // Envio de Email
                var content = msg.AsString;
                System.Console.WriteLine($"Enviando email do cliente: {content}");
                await Task.Delay(TimeSpan.FromSeconds(1));
                System.Console.WriteLine($"Mensagem enviada!");

                // Apaga mensagem da Queue
                // msg.DequeueCount -> Quantas vezes essa mensagem ja foi processada
                await queue.DeleteMessageAsync(msg);
            }
        }

        private static async Task ProcessaMensagensJson(CloudQueueClient queueClient)
        {
            var queue = queueClient.GetQueueReference(name);

            var messsages = await queue.GetMessagesAsync(10);
            //var messsages = await queue.GetMessagesAsync(10, TimeSpan.FromSeconds(5), new QueueRequestOptions(), new OperationContext());
            foreach (var msg in messsages)
            {
                // Informações da Mensagem
                System.Console.WriteLine($"Próxima leitura disponivel: {msg.NextVisibleTime.Value.ToLocalTime()}");

                // Envio de Email
                var emailModel = JsonConvert.DeserializeObject<EmailModel>(msg.AsString);
                System.Console.WriteLine($"Enviando email do cliente: {emailModel.UserId} / {emailModel.Email}");
                await Task.Delay(TimeSpan.FromSeconds(1));
                System.Console.WriteLine($"Mensagem enviada!");

                // Apaga mensagem da Queue
                // msg.DequeueCount -> Quantas vezes essa mensagem ja foi processada
                await queue.DeleteMessageAsync(msg);
            }
        }

        private static async Task ContaMensagens(CloudQueueClient queueClient)
        {
            var queue = queueClient.GetQueueReference(name);
            await queue.FetchAttributesAsync();
            System.Console.WriteLine($"Mensagens na fila: {queue.ApproximateMessageCount}");
        }

        static async Task EnviaMensagem(CloudQueueClient queueClient)
        {
            var queue = queueClient.GetQueueReference(name);
            await queue.CreateIfNotExistsAsync();

            var rnd = new Random();
            var message = new CloudQueueMessage($"clientId-{rnd.Next(100)}");
            await queue.AddMessageAsync(message);
            System.Console.WriteLine($"Mensagem enviada para fila: {message.Id}");
        }

        static async Task EnviaMensagemJson(CloudQueueClient queueClient)
        {
            var queue = queueClient.GetQueueReference(name);
            await queue.CreateIfNotExistsAsync();

            var emailModel = new EmailModel { UserId = 55, Email = "rsantos@braziliandevs.com" };
            var message = new CloudQueueMessage(JsonConvert.SerializeObject(emailModel));
            await queue.AddMessageAsync(message);
            System.Console.WriteLine($"Mensagem enviada para fila: {message.Id}");
        }
    }
}
