using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Threading.Tasks;

namespace Console.Storage
{
    class Program
    {
        const string name = "pastacurso";

        static async Task Main(string[] args)
        {
            // Criar o objeto da conta
            var storageAccount = CloudStorageAccount.Parse("suaConnectionString");

            // Criar BlobClient
            var blobClient = storageAccount.CreateCloudBlobClient();

            // Listar Containers
            await ListaContainers(blobClient);

            // Criar Container
            await CriaContainer(blobClient);

            // Criar Blob e Fazer Upload
            await CriaBlob(blobClient);

            // Fazer Download do blob
            await DownloadBlob(blobClient);

            // Gerar SAS Token para acesso publico
            CriaSasToken(blobClient);
        }

        static void CriaSasToken(CloudBlobClient blobClient)
        {
            var container = blobClient.GetContainerReference(name);
            var blob = container.GetBlockBlobReference("curso.txt");

            var sasToken = blob.GetSharedAccessSignature(new SharedAccessBlobPolicy
            {
                SharedAccessStartTime = DateTime.Today.AddDays(-1),
                SharedAccessExpiryTime = DateTime.Today.AddDays(+1),
                Permissions = SharedAccessBlobPermissions.Read
            });

            System.Console.WriteLine($"Sas Token: {sasToken}");
        }

        static async Task DownloadBlob(CloudBlobClient blobClient)
        {
            var container = blobClient.GetContainerReference(name);
            var blob = container.GetBlockBlobReference("curso.txt");

            var content = await blob.DownloadTextAsync();
            System.Console.WriteLine($"Conteudo: {content}");
            System.Console.WriteLine($"Tipo Conteudo: {blob.Properties.ContentType}");
        }

        static async Task CriaBlob(CloudBlobClient blobClient)
        {
            var container = blobClient.GetContainerReference(name);
            var blob = container.GetBlockBlobReference("curso.txt");

            var exists = await blob.ExistsAsync();
            if (!exists)
            {
                await blob.UploadTextAsync("Hello blobs on Azure!");
            }
            //else
            //{
            //    blob.Properties.ContentType = "text/plain";
            //    await blob.SetPropertiesAsync();
            //}

            System.Console.WriteLine($"Url para download: {blob.Uri}");
        }

        static async Task CriaContainer(CloudBlobClient blobClient)
        {
            var container = blobClient.GetContainerReference(name);
            await container.CreateIfNotExistsAsync();
            System.Console.WriteLine($"Container {name} created!");
        }

        static async Task ListaContainers(CloudBlobClient blobClient)
        {
            var containers = await blobClient.ListContainersSegmentedAsync(null);
            foreach (var c in containers.Results)
            {
                System.Console.WriteLine(c.Name);
            }
        }
    }
}
