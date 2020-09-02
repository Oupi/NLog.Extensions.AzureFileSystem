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
    [Target("AzureFileSystem")]
    public class AzureFileSystemTarget : TargetWithLayout, IAzureFileSystemTarget
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
                _fileShare = FileShareHelper.InitializeFileShare(this, _fileShare, folderName, fileName);
                LogMessageToAzureFile(logMessage, folderName, fileName);
            }
            catch (StorageException ex)
            {
                InternalLogger.Error(ex, "AzureFileSystemTarget: failed writing to file: {0} in folder: {1}", fileName,
                    folderName);
                throw;
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
    }
}