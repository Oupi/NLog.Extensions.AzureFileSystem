#region

using System;
using System.IO;
using System.Text;
using System.Threading;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.File;
using NLog.Common;
using NLog.Config;
using NLog.Layouts;
using NLog.Targets;

#endregion

namespace NLog.Extensions.AzureFileSystem
{
    [Target("AzureFileSystem")]
    public class AzureFileSystemTarget : TargetWithLayout
    {
        private readonly ReaderWriterLockSlim _fileLock = new ReaderWriterLockSlim();
        private CloudFileShare _fileShare;

        /// <summary>
        ///     Azure storage connection string
        /// </summary>
        [RequiredParameter]
        public string StorageConnectionString { get; set; }

        /// <summary>
        ///     Azure file system share name
        /// </summary>
        [RequiredParameter]
        public string AzureFileShareName { get; set; }

        /// <summary>
        ///     Folder in which the logs are created
        /// </summary>
        [RequiredParameter]
        public Layout AzureFileShareFolder { get; set; }

        /// <summary>
        ///     File name pattern for log files
        /// </summary>
        [RequiredParameter]
        public Layout AzureFileName { get; set; }

        protected override void Write(LogEventInfo logEvent)
        {
            if (string.IsNullOrEmpty(logEvent.Message))
            {
                return;
            }

            var folderName = RenderLogEvent(AzureFileShareFolder, logEvent);
            var fileName = RenderLogEvent(AzureFileName, logEvent);
            var layoutMessage = Layout.Render(logEvent);
            var logMessage = string.Concat(layoutMessage, Environment.NewLine);

            try
            {
                InitializeFileShare(folderName);
                LogMessageToAzureFile(logMessage, folderName, fileName);
            }
            catch (StorageException ex)
            {
                InternalLogger.Error(ex, "AzureFileSystemTarget: failed writing to file: {0} in folder: {1}", fileName,
                    folderName);
                throw;
            }
        }

        private void InitializeFileShare(string folderName)
        {
            if (_fileShare == null)
            {
                _fileShare = CreateStorageAccountFromConnectionString(StorageConnectionString);
            }

            var rootDir = _fileShare.GetRootDirectoryReference();
            // Get a reference to the directory.
            var folder = rootDir.GetDirectoryReference(folderName);
            var folderExists = folder.ExistsAsync().GetAwaiter().GetResult();

            // Ensure that the directory exists.
            if (!folderExists)
            {
                folder.CreateAsync().GetAwaiter().GetResult();

                InternalLogger.Trace($"AzureFileSystemTarget - Folder {folderName} Initialized");
            }
        }

        private void LogMessageToAzureFile(string logMessage, string folderName, string fileName)
        {
            var rootDir = _fileShare.GetRootDirectoryReference();
            // Get a reference to the directory.
            var folder = rootDir.GetDirectoryReference(folderName);
            var sourceFile = folder.GetFileReference(fileName);
            var sourceFileExists = sourceFile.ExistsAsync().GetAwaiter().GetResult();
            var messageBytes = Encoding.UTF8.GetBytes(logMessage);

            if (!_fileLock.IsWriteLockHeld)
            {
                _fileLock.EnterWriteLock();
            }

            try
            {
                //Ensure that the file exists.
                if (!sourceFileExists)
                {
                    sourceFile.CreateAsync(messageBytes.Length).GetAwaiter().GetResult();

                    InternalLogger.Trace($"AzureFileSystemTarget - File {fileName} Created");
                }
                else
                {
                    sourceFile.ResizeAsync(sourceFile.Properties.Length + messageBytes.Length).GetAwaiter().GetResult();
                }

                using (var cloudStream = sourceFile.OpenWriteAsync(null).GetAwaiter().GetResult())
                {
                    cloudStream.Seek(messageBytes.Length * -1, SeekOrigin.End);
                    cloudStream.WriteAsync(messageBytes, 0, messageBytes.Length).GetAwaiter().GetResult();
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

        /// <summary>
        ///     Validates the connection string information in app.config and throws an exception if it looks like
        ///     the user hasn't updated this to valid values.
        /// </summary>
        /// <param name="storageConnectionString">The storage connection string</param>
        /// <returns>CloudStorageAccount object</returns>
        private CloudFileShare CreateStorageAccountFromConnectionString(string storageConnectionString)
        {
            CloudFileShare fileShare;
            try
            {
                var storageAccount = CloudStorageAccount.Parse(storageConnectionString);
                InternalLogger.Trace("AzureFileSystemTarget - Storage Connection Initialized");

                // Create a CloudFileClient object for credentialed access to Azure Files.
                var fileClient = storageAccount.CreateCloudFileClient();
                InternalLogger.Trace("AzureFileSystemTarget - File client Connection Initialized");

                // Get a reference to the file share we created previously.
                fileShare = fileClient.GetShareReference(AzureFileShareName);
                InternalLogger.Trace("AzureFileSystemTarget - File client Connection Initialized");

                var fileShareExits = fileShare.ExistsAsync().GetAwaiter().GetResult();
                if (!fileShareExits)
                {
                    InternalLogger.Error("AzureFileSystemTarget(Name={0}): failed init: {1}",
                        $"There is no share with name {AzureFileShareName} defined in storage account.");
                    throw new ArgumentException(nameof(AzureFileShareName));
                }

                InternalLogger.Trace("AzureFileSystemTarget - File share Connection Initialized");
            }
            catch (Exception ex)
            {
                InternalLogger.Error(ex, "AzureFileSystemTarget(Name={0}): failed init: {1}",
                    Name,
                    "Invalid storage account information provided. Please confirm the AccountName and AccountKey are valid in the nlog config file.");
                throw;
            }

            return fileShare;
        }
    }
}