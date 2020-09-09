using NLog.Layouts;

namespace NLog.Extensions.AzureFileSystem
{
    public interface IAzureFileSystemTarget
    {
        Layout AzureFileName { get; set; }
        Layout AzureFileShareFolder { get; set; }
        string AzureFileShareName { get; set; }
        string StorageConnectionString { get; set; }
        string Name { get; set; }
    }
}