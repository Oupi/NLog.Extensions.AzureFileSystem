using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.File;
using NLog.Common;
using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace NLog.Extensions.AzureFileSystem
{
    public class FileShareManager
    {
        private readonly ReaderWriterLockSlim _fileLock = new ReaderWriterLockSlim();
        private readonly IAzureFileSystemTarget target;
        private CloudFileShare _fileShare;

        public FileShareManager(IAzureFileSystemTarget target)
        {
            this.target = target;
        }

        private async Task<CloudFileDirectory> InitializeFolderAsync(string folderName)
        {
            if (_fileShare == null)
            {
                _fileShare = await CreateStorageAccountFromConnectionStringAsync(target)
                    .ConfigureAwait(false);
            }

            folderName = folderName.Replace(@"\", @"/");

            var rootDir = _fileShare.GetRootDirectoryReference();

            // Get a reference to the directory.
            var folder = string.IsNullOrWhiteSpace(folderName) ? rootDir : rootDir.GetDirectoryReference(folderName);
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

            return folder;
        }
        
        /// <summary>
        ///     Validates the connection string information in app.config and throws an exception if it looks like
        ///     the user hasn't updated this to valid values.
        /// </summary>
        /// <param name="storageConnectionString">The storage connection string</param>
        /// <returns>CloudStorageAccount object</returns>
        private async Task<CloudFileShare> CreateStorageAccountFromConnectionStringAsync(IAzureFileSystemTarget target)
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

        public void LogMessageToAzureFile(string logMessage, string folderName, string fileName)
        {
            AsyncHelper.RunSync(async () => await LogMessageToAzureFileAsync(logMessage, folderName, fileName));
        }
        public async Task LogMessageToAzureFileAsync(string logMessage, string folderName, string fileName)
        {
            if (!_fileLock.IsWriteLockHeld)
            {
                _fileLock.EnterWriteLock();
            }

            try
            {
                var folder = await InitializeFolderAsync(folderName);
                var sourceFile = folder.GetFileReference(fileName);
                var sourceFileExists = await sourceFile.ExistsAsync().ConfigureAwait(false);
                var messageBytes = Encoding.UTF8.GetBytes(logMessage);

                //Ensure that the file exists.
                if (!sourceFileExists)
                {
                    await sourceFile.CreateAsync(messageBytes.Length).ConfigureAwait(false);

                    InternalLogger.Trace($"AzureFileSystemTarget - File {fileName} Created");
                }
                else
                {
                    await sourceFile.ResizeAsync(sourceFile.Properties.Length + messageBytes.Length)
                        .ConfigureAwait(false);
                }

                using (var cloudStream = await sourceFile.OpenWriteAsync(null).ConfigureAwait(false))
                {
                    cloudStream.Seek(messageBytes.Length * -1, SeekOrigin.End);
                    await cloudStream.WriteAsync(messageBytes, 0, messageBytes.Length).ConfigureAwait(false);
                }
            }
            finally
            {
                if (_fileLock.IsWriteLockHeld)
                {
                    _fileLock.ExitWriteLock();
                }
            }
        }
    }
}