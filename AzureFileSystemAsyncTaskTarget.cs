#region

using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.File;
using NLog.Common;
using NLog.Config;
using NLog.Layouts;
using NLog.Targets;

#endregion

namespace NLog.Extensions.AzureFileSystem
{
    [Target("AzureFileSystemAsync")]
    public class AzureFileSystemAsyncTaskTarget : AsyncTaskTarget, IAzureFileSystemTarget
    {
        private readonly ReaderWriterLockSlim _fileLock = new ReaderWriterLockSlim();
        private CloudFileShare _fileShare;

        public AzureFileSystemAsyncTaskTarget()
        {
            IncludeEventProperties = true;
        }

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

        #region Overrides of AsyncTaskTarget

        /// <inheritdoc />
        protected override async Task WriteAsyncTask(LogEventInfo logEvent, CancellationToken cancellationToken)
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
                _fileShare = await FileShareHelper.InitializeFileShareAsync(this, _fileShare, folderName, fileName);
                await LogMessageToAzureFileAsync(logMessage, folderName, fileName).ConfigureAwait(false);
            }
            catch (StorageException ex)
            {
                InternalLogger.Error(ex, "AzureFileSystemTarget: failed writing to file: {0} in folder: {1}", fileName,
                    folderName);
            }
        }

        #endregion

        private async Task LogMessageToAzureFileAsync(string logMessage, string folderName, string fileName)
        {
            if (!_fileLock.IsWriteLockHeld)
            {
                _fileLock.EnterWriteLock();
            }

            try
            {
                var rootDir = _fileShare.GetRootDirectoryReference();
                // Get a reference to the directory.
                var folder = rootDir.GetDirectoryReference(folderName);
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