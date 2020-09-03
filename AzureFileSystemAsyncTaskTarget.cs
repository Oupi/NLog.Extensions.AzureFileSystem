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
        private FileShareManager _fileShareManager;

        public AzureFileSystemAsyncTaskTarget()
        {
            _fileShareManager = new FileShareManager(this);
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
                await _fileShareManager.LogMessageToAzureFileAsync(logMessage, folderName, fileName).ConfigureAwait(false);
            }
            catch (StorageException ex)
            {
                InternalLogger.Error(ex, "AzureFileSystemTarget: failed writing to file: {0} in folder: {1}", fileName,
                    folderName);
            }
        }

        #endregion
    }
}