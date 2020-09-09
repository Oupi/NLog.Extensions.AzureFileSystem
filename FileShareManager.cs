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
        private readonly IAzureFileSystemTarget _target;
        private CloudFileShare _fileShare;

        /// <summary>
        /// Creates FileShareManager object that handles Azure FileShare operations.
        /// </summary>
        /// <param name="target">NLog target implementing IAzureFileSystemTarget interface</param>
        public FileShareManager(IAzureFileSystemTarget target)
        {
            this._target = target;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="folderName">Folder path inside file share. Can be one or multi level. If folder does not exist it is created.</param>
        /// <returns>Azure CloudFileDirectory object.</returns>
        private async Task<CloudFileDirectory> InitializeFolderAsync(string folderName)
        {
            if (_fileShare == null)
            {
                _fileShare = await CreateStorageAccountFromConnectionStringAsync()
                    .ConfigureAwait(false);
            }

            folderName = folderName.Replace(@"\", @"/");

            var rootDir = _fileShare.GetRootDirectoryReference();

            // Get a reference to the directory.
            var folder = string.IsNullOrWhiteSpace(folderName) ? rootDir : rootDir.GetDirectoryReference(folderName);

            if (!await folder.ExistsAsync().ConfigureAwait(false))
            {
                var levels = folderName.Split(new char[] { '/' }, StringSplitOptions.RemoveEmptyEntries);
                var currentLevel = levels[0];

                for (int i = 0; i < levels.Length; i++)
                {
                    if (i > 0) currentLevel += @"/" + levels[i];

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
        /// <returns>CloudStorageAccount object</returns>
        private async Task<CloudFileShare> CreateStorageAccountFromConnectionStringAsync()
        {
            CloudFileShare fileShare;

            try
            {
                var storageAccount = CloudStorageAccount.Parse(_target.StorageConnectionString);
                InternalLogger.Trace("AzureFileSystemTarget - Storage Connection Initialized");

                // Create a CloudFileClient object for credentialed access to Azure Files.
                var fileClient = storageAccount.CreateCloudFileClient();
                InternalLogger.Trace("AzureFileSystemTarget - File client Connection Initialized");

                // Get a reference to the file share we created previously.
                fileShare = fileClient.GetShareReference(_target.AzureFileShareName);
                InternalLogger.Trace("AzureFileSystemTarget - File client Connection Initialized");

                var fileShareExits = await fileShare.ExistsAsync().ConfigureAwait(false);
                if (!fileShareExits)
                {
                    InternalLogger.Error("AzureFileSystemTarget(Name={0}): failed init: {1}",
                        $"There is no share with name {_target.AzureFileShareName} defined in storage account.");
                    throw new ArgumentException(nameof(_target.AzureFileShareName));
                }

                InternalLogger.Trace("AzureFileSystemTarget - File share Connection Initialized");
            }
            catch (Exception ex)
            {
                InternalLogger.Error(ex, "AzureFileSystemTarget(Name={0}): failed init: {1}",
                    _target.Name,
                    "Invalid storage account information provided. Please confirm the AccountName and AccountKey are valid in the nlog config file.");
                throw;
            }

            return fileShare;
        }

        /// <summary>
        /// Logs log message to the file share.
        /// </summary>
        /// <param name="logMessage">Message text.</param>
        /// <param name="folderName">Folder path inside file share. Can be one or multi level. If folder does not exist it is created.</param>
        /// <param name="fileName">Name of the file.</param>
        public void LogMessageToAzureFile(string logMessage, string folderName, string fileName)
        {
            AsyncHelper.RunSync(async () => await LogMessageToAzureFileAsync(logMessage, folderName, fileName));
        }

        /// <summary>
        /// Logs log message to the file share asynchronously.
        /// </summary>
        /// <param name="logMessage">Message text.</param>
        /// <param name="folderName">Folder path inside file share. Can be one or multi level. If folder does not exist it is created.</param>
        /// <param name="fileName">Name of the file.</param>
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