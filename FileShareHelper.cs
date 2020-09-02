using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.File;
using NLog.Common;
using System;
using System.Threading.Tasks;

namespace NLog.Extensions.AzureFileSystem
{
    public static class FileShareHelper
    {
        public static CloudFileShare InitializeFileShare(IAzureFileSystemTarget target, CloudFileShare fileShare, string folderName, string fileName)
        {
            return AsyncHelper.RunSync(async () => await InitializeFileShareAsync(target, fileShare, folderName, fileName));
        }

        public static async Task<CloudFileShare> InitializeFileShareAsync(IAzureFileSystemTarget target, CloudFileShare fileShare, string folderName, string fileName)
        {
            if (fileShare == null)
            {
                fileShare = await CreateStorageAccountFromConnectionStringAsync(target)
                    .ConfigureAwait(false);
            }

            folderName = folderName.Replace(@"\", @"/");

            var rootDir = fileShare.GetRootDirectoryReference();

            // Get a reference to the directory.
            var folder = rootDir.GetDirectoryReference(folderName);
            var folderExists = await folder.ExistsAsync().ConfigureAwait(false);

            if (!folderExists)
            {
                var levels = folderName.Split(new char[] { '/' }, StringSplitOptions.RemoveEmptyEntries);
                var currentLevel = string.Empty;

                for (int i = 0; i < levels.Length; i++)
                {
                    if (currentLevel == string.Empty) currentLevel = levels[i];
                    else currentLevel += @"/" + levels[i];

                    var currentFolder = rootDir.GetDirectoryReference(currentLevel);
                    if (!await currentFolder.ExistsAsync().ConfigureAwait(false))
                    {
                        await currentFolder.CreateAsync().ConfigureAwait(false);
                        InternalLogger.Trace($"AzureFileSystemTarget - Folder {currentLevel} Initialized");
                    }
                }
            }

            return fileShare;
        }

        /// <summary>
        ///     Validates the connection string information in app.config and throws an exception if it looks like
        ///     the user hasn't updated this to valid values.
        /// </summary>
        /// <param name="storageConnectionString">The storage connection string</param>
        /// <returns>CloudStorageAccount object</returns>
        private static async Task<CloudFileShare> CreateStorageAccountFromConnectionStringAsync(IAzureFileSystemTarget target)
        {
            CloudFileShare fileShare;
            try
            {
                var storageAccount = CloudStorageAccount.Parse(target.StorageConnectionString);
                InternalLogger.Trace("AzureFileSystemTarget - Storage Connection Initialized");

                // Create a CloudFileClient object for credentialed access to Azure Files.
                var fileClient = storageAccount.CreateCloudFileClient();
                InternalLogger.Trace("AzureFileSystemTarget - File client Connection Initialized");

                // Get a reference to the file share we created previously.
                fileShare = fileClient.GetShareReference(target.AzureFileShareName);
                InternalLogger.Trace("AzureFileSystemTarget - File client Connection Initialized");

                var fileShareExits = await fileShare.ExistsAsync().ConfigureAwait(false);
                if (!fileShareExits)
                {
                    InternalLogger.Error("AzureFileSystemTarget(Name={0}): failed init: {1}",
                        $"There is no share with name {target.AzureFileShareName} defined in storage account.");
                    throw new ArgumentException(nameof(target.AzureFileShareName));
                }

                InternalLogger.Trace("AzureFileSystemTarget - File share Connection Initialized");
            }
            catch (Exception ex)
            {
                InternalLogger.Error(ex, "AzureFileSystemTarget(Name={0}): failed init: {1}",
                    target.Name,
                    "Invalid storage account information provided. Please confirm the AccountName and AccountKey are valid in the nlog config file.");
                throw;
            }

            return fileShare;
        }
    }
}
