using System;
using Azure;
using System.Text;
using Azure.Storage.Files.Shares;
using Azure.Storage.Files.Shares.Models;

namespace my.Function
{
    public class StateManager
    {
        private ShareClient shareClient;
        private ShareFileClient fileClient;

        public StateManager(string connectionString, string shareName = "funcstatemarkershare", string filePath = "funcstatemarkerfile")
        {
            this.shareClient = new ShareClient(connectionString, shareName);
            this.fileClient = this.shareClient.GetRootDirectoryClient().GetFileClient(filePath);
        }

        public void Post(string markerText)
        {
            try
            {
                byte[] byteArray = Encoding.UTF8.GetBytes(markerText);
                using (MemoryStream stream = new MemoryStream(byteArray))
                {
                    this.fileClient.Upload(stream);
                }
            }
            catch (RequestFailedException ex) when (ex.ErrorCode == ShareErrorCode.ShareNotFound)
            {
                this.shareClient.Create();
                byte[] byteArray = Encoding.UTF8.GetBytes(markerText);
                using (MemoryStream stream = new MemoryStream(byteArray))
                {
                    this.fileClient.Upload(stream);
                }
            }
        }

        public string Get()
        {
            try
            {
                ShareFileDownloadInfo download = this.fileClient.Download();
                using (MemoryStream stream = new MemoryStream())
                {
                    download.Content.CopyTo(stream);
                    return Encoding.UTF8.GetString(stream.ToArray());
                }
            }
            catch (RequestFailedException ex) when (ex.ErrorCode == ShareErrorCode.ShareNotFound || ex.ErrorCode == ShareErrorCode.ResourceNotFound)
            {
                return null;
            }
        }
    }
}

