/*
 *
 * (c) Copyright Talegen, LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
*/

namespace Talegen.Storage.Net.Aws
{
    using System.Collections.Generic;
    using System.IO;
    using System.Reflection;
    using System.Text;
    using Amazon.S3;
    using Amazon.S3.IO;
    using Amazon.S3.Model;
    using Talegen.Common.Core.Extensions;
    using Talegen.Common.Core.Properties;
    using Talegen.Storage.Net.Core;

    /// <summary>
    /// This class implements the AWS Storage Service interface. This class cannot be inherited.
    /// </summary>
    /// <seealso cref="Talegen.Storage.Net.Core.IStorageService" />
    public sealed class AwsStorageService : IStorageService
    {
        #region Public Constructors

        /// <summary>
        /// Initializes a new instance of the <see cref="AwsStorageService" /> class.
        /// </summary>
        /// <param name="context">Contains an instance of the storage context.</param>
        public AwsStorageService(AwsS3StorageContext context)
        {
            this.Context = context ?? throw new ArgumentNullException(nameof(context));
            this.StorageId = context.RootWorkspaceLocalFolderPath;
            this.RootPath = context.RootWorkspaceLocalFolderPath;
            this.client = new AmazonS3Client(this.Context.Config) ?? throw new Exception("Unable to initialize the AWS S3 client.");
            this.Initialize();
        }

        #endregion

        #region Private Properties

        /// <summary>
        /// Contains the upload limit buffer size.
        /// </summary>
        private const int UploadLimit = 4194304;

        /// <summary>
        /// Contains the internal S3 client.
        /// </summary>
        private IAmazonS3 client;

        #endregion

        #region Public Properties

        /// <summary>
        /// Gets the storage context.
        /// </summary>
        /// <value>Contains the storage context.</value>
        public AwsS3StorageContext Context { get; }

        /// <summary>
        /// Gets the storage identifier.
        /// </summary>
        /// <returns>Contains the storage identifier.</returns>
        public string StorageId { get; }

        /// <summary>
        /// Gets or sets the name of the storage provider.
        /// </summary>
        /// <value>Gets or sets the name of the storage provider.</value>
        public string RootPath { get; set; }

        #endregion

        #region Public Methods

        /// <summary>
        /// </summary>
        /// <param name="sourceFilePath"></param>
        /// <param name="targetFilePath"></param>
        /// <param name="overwrite"></param>
        public void CopyFile(string sourceFilePath, string targetFilePath, bool overwrite = true)
        {
            // copy source file to target file in S3 bucket.
            if (string.IsNullOrWhiteSpace(sourceFilePath))
            {
                throw new ArgumentNullException(nameof(sourceFilePath));
            }

            if (string.IsNullOrWhiteSpace(targetFilePath))
            {
                throw new ArgumentNullException(nameof(targetFilePath));
            }

            this.VerifyDirectoryNameRoot(sourceFilePath);
            this.VerifyDirectoryNameRoot(targetFilePath);

            this.LateralCopyInternal(sourceFilePath, targetFilePath, overwrite);
        }

        /// <summary>
        /// This method is used to create a new directory.
        /// </summary>
        /// <param name="subFolderName">Contains the new directory name.</param>
        /// <param name="silentExists">Contains a value indicating whether to ignore the directory exists exception.</param>
        /// <returns></returns>
        public string CreateDirectory(string subFolderName, bool silentExists = false)
        {
            string newPathResult = string.Empty;

            try
            {
                S3DirectoryInfo newDirectoryInfo = this.GetS3DirectoryInfo(subFolderName, this.Context.TargetBucketName);
                newDirectoryInfo.Create();
                newPathResult = newDirectoryInfo.FullName;
            }
            catch (Exception ex)
            {
                if (!silentExists)
                {
                    throw new StorageException($"Unable to create directory {subFolderName}.", ex);
                }
            }

            return newPathResult;
        }

        /// <summary>
        /// This method is used to delete an existing directory.
        /// </summary>
        /// <param name="subFolderName">Contains the name of the directory to delete.</param>
        /// <param name="recursive">Contains a value indicating whether to delete the directory recursively.</param>
        /// <param name="silentNoExist">Contains a value indicating whether to ignore the directory does not exist exception.</param>
        public void DeleteDirectory(string subFolderName, bool recursive = true, bool silentNoExist = true)
        {
            try
            {
                S3DirectoryInfo directoryInfo = this.GetS3DirectoryInfo(subFolderName);
                directoryInfo.Delete(recursive);
            }
            catch (Exception ex)
            {
                throw new StorageException($"An unexpected error occurred while deleting directory {subFolderName}.", ex);
            }
        }

        /// <summary>
        /// This method is used to delete an existing file.
        /// </summary>
        /// <param name="filePath">Contains the file path to delete.</param>
        /// <param name="deleteDirectory">Contains a value indicating whether to delete the directory containing the file if contents are empty.</param>
        public void DeleteFile(string filePath, bool deleteDirectory = false)
        {
            try
            {
                S3FileInfo fileInfo = this.GetS3FileInfo(filePath);
                fileInfo.Delete();

                if (deleteDirectory && !fileInfo.Exists)
                {
                    var directoryName = Path.GetDirectoryName(filePath);

                    if (!string.IsNullOrWhiteSpace(directoryName))
                    {
                        this.DeleteDirectory(directoryName);
                    }
                }
            }
            catch (Exception ex)
            {
                throw new StorageException($"An unexpected error occurred while deleting file {filePath}.", ex);
            }
        }

        /// <summary>
        /// This method is used to delete a list of files.
        /// </summary>
        /// <param name="filePathNames">Contains a list of file paths to delete.</param>
        /// <param name="deleteDirectory">Contains a value indicating whether to delete the directory containing the file if contents are empty.</param>
        public void DeleteFiles(List<string> filePathNames, bool deleteDirectory = false)
        {
            if (filePathNames == null)
            {
                throw new ArgumentNullException(nameof(filePathNames));
            }

            foreach (string filePath in filePathNames)
            {
                this.DeleteFile(filePath, deleteDirectory);
            }
        }

        /// <summary>
        /// This method is used to empty an existing directory without removing it.
        /// </summary>
        /// <param name="subFolderName">Contains the name of the directory to empty.</param>
        public void EmptyDirectory(string subFolderName)
        {
            // delete the contents of the directory but do not remove the directory.
            S3DirectoryInfo directoryInfo = this.GetS3DirectoryInfo(subFolderName);
            var items = directoryInfo.GetFileSystemInfos();

            try
            {
                foreach (var item in items)
                {
                    if (item.Type == FileSystemType.Directory)
                    {
                        var dirItem = item as S3DirectoryInfo;
                        dirItem?.Delete(true);
                    }
                    else
                    {
                        item.Delete();
                    }
                }
            }
            catch (Exception ex)
            {
                throw new StorageException(string.Format("An error occurred while emptying directory '{0}'.", subFolderName), ex);
            }
        }

        /// <summary>
        /// This method is used to determine if a directory exists.
        /// </summary>
        /// <param name="subFolderName">Contains the name of the directory to check.</param>
        /// <param name="silentExists">Contains a value indicating whether to ignore a storage exception.</param>
        /// <returns>Returns a value indicating whether the directory exists.</returns>
        public bool DirectoryExists(string subFolderName, bool silentExists)
        {
            bool exists = false;
            try
            {
                exists = this.DirectoryExists(subFolderName);
            }
            catch (Exception ex)
            {
                if (!silentExists)
                {
                    throw new StorageException($"Unable to determine if directory {subFolderName} exists.", ex);
                }
            }

            return exists;
        }

        /// <summary>
        /// This method is used to determine if a directory exists.
        /// </summary>
        /// <param name="subFolderName">Contains the name of the directory to check.</param>
        /// <returns>Returns a value indicating whether the directory exists.</returns>
        public bool DirectoryExists(string subFolderName)
        {
            if (string.IsNullOrWhiteSpace(subFolderName))
            {
                throw new ArgumentNullException(nameof(subFolderName));
            }

            this.VerifyDirectoryNameRoot(subFolderName);

            S3DirectoryInfo dirInfo = new S3DirectoryInfo(this.client, this.Context.BucketName, subFolderName);
            return dirInfo.Exists;
        }

        /// <summary>
        /// This method is used to determine if a file object exists.
        /// </summary>
        /// <param name="filePath">Contains the name of the file object to check.</param>
        /// <returns>Returns a value indicating whether the file object exists.</returns>
        public bool FileExists(string filePath)
        {
            bool result = false;

            if (string.IsNullOrWhiteSpace(filePath))
            {
                throw new ArgumentNullException(nameof(filePath));
            }

            this.VerifyDirectoryNameRoot(filePath);

            string directoryName = Path.GetDirectoryName(filePath).ConvertToString();
            string fileName = Path.GetFileName(filePath);

            this.VerifyDirectoryNameRoot(directoryName);

            try
            {
                GetObjectMetadataRequest request = new GetObjectMetadataRequest
                {
                    BucketName = this.Context.BucketName,
                    Key = fileName
                };

                S3FileInfo fileInfo = new S3FileInfo(this.client, this.Context.BucketName, filePath);
                result = fileInfo.Exists;
            }
            catch (Exception ex)
            {
                throw new StorageException($"An error occurred determining if {fileName} exists.", ex);
            }

            return result;
        }

        /// <summary>
        /// This method is used to create a hash of the file contents.
        /// </summary>
        /// <param name="filePath">Contains the path to the file to hash.</param>
        /// <returns>Returns a hash of the file contents.</returns>
        public string FileHash(string filePath)
        {
            string result = string.Empty;
            try
            {
                var fileInfo = this.GetS3FileInfo(filePath);
                string tempFilePath = Path.GetTempFileName();
                FileInfo localTempFile = fileInfo.CopyToLocal(tempFilePath, true);

                if (localTempFile.Exists)
                {
                    result = localTempFile.ToHashString(HashMethod.SHA256);
                    localTempFile.Delete(); // clean-up
                }
            }
            catch (Exception ex)
            {
                throw new StorageException(string.Format("An error occurred while retrieving '{0}' for hash calculation.", filePath), ex);
            }

            return result;
        }

        /// <summary>
        /// This method is used to get the files from a directory.
        /// </summary>
        /// <param name="subFolderName">Contains the directory name to get the files.</param>
        /// <param name="searchPattern">Contains an optional file name search pattern. If not specified, *.* is used.</param>
        /// <param name="searchOption">Contains search options. If not specified, all sub-folders are searched for the file pattern.</param>
        /// <returns>Returns a list of files in the directory path.</returns>
        public List<string> FindFiles(string subFolderName, string searchPattern = "*.*", SearchOption searchOption = SearchOption.AllDirectories)
        {
            S3DirectoryInfo dirInfo = this.GetS3DirectoryInfo(subFolderName);
            var items = dirInfo.GetFiles(searchPattern, searchOption);
            return items.Select(item => item.Name).ToList();
        }

        /// <summary>
        /// This method is used to move a file.
        /// </summary>
        /// <param name="sourceFilePath">Contains a the path to the file that will be moved.</param>
        /// <param name="targetFilePath">Contains the path to the target where the file is to be moved.</param>
        /// <param name="overwrite">Contains a value indicating if the target should be overwritten if it already exists. Default is true.</param>
        /// <exception cref="ArgumentNullException"></exception>
        /// <exception cref="StorageException"></exception>
        public void MoveFile(string sourceFilePath, string targetFilePath, bool overwrite = true)
        {
            if (string.IsNullOrWhiteSpace(sourceFilePath))
            {
                throw new ArgumentNullException(nameof(sourceFilePath));
            }

            if (string.IsNullOrWhiteSpace(targetFilePath))
            {
                throw new ArgumentNullException(nameof(targetFilePath));
            }

            try
            {
                S3FileInfo item = this.GetS3FileInfo(sourceFilePath);
                S3FileInfo targetItem = this.GetS3FileInfo(targetFilePath, this.Context.TargetBucketName);
                item.MoveTo(targetItem);
            }
            catch (Exception ex)
            {
                throw new StorageException(string.Format("An error occurred during move of file '{0}' to destination '{1}', in target bucket '{2}'.", sourceFilePath, targetFilePath, this.Context.TargetBucketName), ex);
            }
        }

        /// <summary>
        /// This method is used to read all the bytes from a binary file.
        /// </summary>
        /// <param name="path">Contains the path to the file to load and return.</param>
        /// <returns>Returns a byte array containing the binary bytes of the target file.</returns>
        /// <exception cref="ArgumentNullException"></exception>
        /// <exception cref="StorageException"></exception>
        public byte[] ReadBinaryFile(string path)
        {
            if (string.IsNullOrWhiteSpace(path))
            {
                throw new ArgumentNullException(nameof(path));
            }

            this.VerifyDirectoryNameRoot(path);

            using MemoryStream stream = new MemoryStream();
            byte[] result;

            try
            {
                this.ReadBinaryFile(path, stream);
                result = stream.ToArray();
            }
            catch (Exception ex)
            {
                throw new StorageException(string.Format("An error occurred while reading binary file '{0}'.", path), ex);
            }

            return result;
        }

        /// <summary>
        /// This method is used to read all the bytes from a binary file to a provided stream.
        /// </summary>
        /// <param name="path">Contains the path to the file to load into the stream.</param>
        /// <param name="outputStream">The stream to write the file to.</param>
        /// <exception cref="ArgumentNullException"></exception>
        /// <exception cref="IOException"></exception>
        /// <exception cref="StorageException"></exception>
        public void ReadBinaryFile(string path, Stream outputStream)
        {
            if (string.IsNullOrWhiteSpace(path))
            {
                throw new ArgumentNullException(nameof(path));
            }

            if (outputStream == null)
            {
                throw new ArgumentNullException(nameof(outputStream));
            }

            if (!outputStream.CanWrite)
            {
                throw new IOException("Can't write to specified output stream.");
            }

            this.VerifyDirectoryNameRoot(path);

            try
            {
                S3FileInfo fileItem = this.GetS3FileInfo(path);

                if (fileItem.Exists)
                {
                    using Stream stream = fileItem.OpenRead();
                    stream.CopyTo(outputStream);
                }
                else
                {
                    throw new StorageException(string.Format("The file '{0}' was not found.", path));
                }
            }
            catch (Exception ex)
            {
                throw new StorageException(string.Format("An error occurred while reading binary file '{0}'.", path), ex);
            }
        }

        /// <summary>
        /// This method is used to read all the bytes from a text file.
        /// </summary>
        /// <param name="path">Contains the path to the file to load and return.</param>
        /// <param name="encoding">Contains the text encoding type. If none is specified, Encoding.Default is used.</param>
        /// <returns>Returns a string containing the content of the target file.</returns>
        /// <exception cref="ArgumentNullException"></exception>
        public string ReadTextFile(string path, Encoding encoding = null!)
        {
            if (string.IsNullOrWhiteSpace(path))
            {
                throw new ArgumentNullException(nameof(path));
            }

            this.VerifyDirectoryNameRoot(path);

            Encoding encoder = encoding ?? Encoding.Default;
            return encoder.GetString(this.ReadBinaryFile(path));
        }

        /// <summary>
        /// This method is used to write content to the specified path.
        /// </summary>
        /// <param name="path">Contains the fully qualified path, including file name, to the location in which the binary content shall be written.</param>
        /// <param name="content">Contains a byte array of the content to be written.</param>
        /// <returns>Returns a value indicating whether the write was successful.</returns>
        /// <exception cref="ArgumentNullException"></exception>
        public bool WriteBinaryFile(string path, byte[] content)
        {
            if (string.IsNullOrWhiteSpace(path))
            {
                throw new ArgumentNullException(nameof(path));
            }

            this.VerifyDirectoryNameRoot(path);

            using MemoryStream stream = new MemoryStream(content);
            return this.WriteBinaryFile(path, stream);
        }

        /// <summary>
        /// This method is used to write content to the specified path.
        /// </summary>
        /// <param name="path">Contains the fully qualified path, including file name, to the location in which the binary content shall be written.</param>
        /// <param name="inputStream">Contains a stream of the content to be written.</param>
        /// <returns>Returns a value indicating whether the write was successful.</returns>
        /// <exception cref="ArgumentNullException"></exception>
        /// <exception cref="IOException"></exception>
        /// <exception cref="StorageException"></exception>
        public bool WriteBinaryFile(string path, Stream inputStream)
        {
            if (string.IsNullOrWhiteSpace(path))
            {
                throw new ArgumentNullException(nameof(path));
            }

            if (inputStream == null)
            {
                throw new ArgumentNullException(nameof(inputStream));
            }

            if (!inputStream.CanRead)
            {
                throw new IOException("The specified stream cannot be read from.");
            }

            this.VerifyDirectoryNameRoot(path);
            bool result;

            try
            {
                S3FileInfo fileItem = this.GetS3FileInfo(path);

                using (Stream stream = fileItem.OpenWrite())
                {
                    if (inputStream.Length <= UploadLimit)
                    {
                        inputStream.CopyTo(stream);
                    }
                }

                result = fileItem.Exists;
            }
            catch (Exception ex)
            {
                throw new StorageException(string.Format("An error occurred writing binary stream to '{0}'.", path), ex);
            }

            return result;
        }

        /// <summary>
        /// This method is used to write content to the specified path.
        /// </summary>
        /// <param name="path">Contains the fully qualified path, including file name, to the location in which the text content shall be written.</param>
        /// <param name="content">Contains a string of the content to be written.</param>
        /// <param name="encoding">Contains the text file encoding. If none specified, Encoding.Default is used.</param>
        /// <returns>Returns a value indicating whether the write was successful.</returns>
        /// <exception cref="ArgumentNullException"></exception>
        public bool WriteTextFile(string path, string content, Encoding encoding = null!)
        {
            if (string.IsNullOrWhiteSpace(path))
            {
                throw new ArgumentNullException(nameof(path));
            }

            Encoding encoder = encoding ?? Encoding.Default;
            return this.WriteBinaryFile(path, encoder.GetBytes(content));
        }

        #endregion

        #region Private Methods

        /// <summary>
        /// This method is used to create a new <see cref="S3FileInfo" /> object for a given path.
        /// </summary>
        /// <param name="filePath">Contains the file path to abstract.</param>
        /// <param name="bucketName">Contains an optional S3 bucket name.</param>
        /// <returns>Returns a new <see cref="S3FileInfo" /> object.</returns>
        private S3FileInfo GetS3FileInfo(string filePath, string bucketName = "")
        {
            return new S3FileInfo(this.client, !string.IsNullOrWhiteSpace(bucketName) ? bucketName : this.Context.BucketName, filePath);
        }

        /// <summary>
        /// This method is used to create a new <see cref="S3DirectoryInfo" /> object for a given path.
        /// </summary>
        /// <param name="directoryPath">Contains the directory path to abstract.</param>
        /// <param name="bucketName">Contains an optional S3 bucket name.</param>
        /// <returns>Returns a new <see cref="S3DirectoryInfo" /> object.</returns>
        private S3DirectoryInfo GetS3DirectoryInfo(string directoryPath, string bucketName = "")
        {
            return new S3DirectoryInfo(this.client, !string.IsNullOrWhiteSpace(bucketName) ? bucketName : this.Context.BucketName, directoryPath);
        }

        /// <summary>
        /// Initializes the internal storage service.
        /// </summary>
        private void Initialize()
        {
        }

        /// <summary>
        /// Verifies the directory name root path.
        /// </summary>
        /// <param name="subFolderName">Name of the sub folder to verify.</param>
        /// <exception cref="StorageException">
        /// An exception is thrown if the root folder specified in <paramref name="subFolderName" /> is not rooted with workspace root folder path.
        /// </exception>
        private void VerifyDirectoryNameRoot(string subFolderName)
        {
            // if the folder name specified has an absolute path...
            if (!string.IsNullOrWhiteSpace(subFolderName) && Path.IsPathRooted(subFolderName))
            {
                // a root path was defined in subFolderName, ensure it is under this.RootPath
                if (!Path.GetFullPath(subFolderName).StartsWith(Path.GetFullPath(this.RootPath)))
                {
                    throw new StorageException(string.Format("The folder \"{0}\" is not under the root path \"{1}\".", subFolderName, this.RootPath));
                }
            }
        }

        /// <summary>
        /// Laterals the copy internal.
        /// </summary>
        /// <param name="sourceFilePath">The source file path.</param>
        /// <param name="destFilePath">The dest file path.</param>
        /// <param name="overwrite">Contains a value indicating if the target should be overwritten if it already exists.</param>
        /// <returns>Returns a value indicating copy success.</returns>
        private bool LateralCopyInternal(string sourceFilePath, string destFilePath, bool overwrite)
        {
            string sourceDirectoryPath = Path.GetDirectoryName(sourceFilePath).ConvertToString();
            string sourceFileName = Path.GetFileName(sourceFilePath);
            string destDirectoryPath = Path.GetDirectoryName(destFilePath).ConvertToString();
            string destFileName = Path.GetFileName(destFilePath);
            bool result = false;

            // Ensure that the source file exists
            if (this.FileExists(sourceFilePath))
            {
                S3DirectoryInfo targetDirInfo = this.GetS3DirectoryInfo(destDirectoryPath, this.Context.TargetBucketName);

                // if the target folder does not exist, create it.
                if (!targetDirInfo.Exists)
                {
                    targetDirInfo.Create();
                }

                try
                {
                    S3FileInfo sourceFileInfo = this.GetS3FileInfo(sourceFilePath, this.Context.BucketName);
                    var newFileInfo = sourceFileInfo.CopyTo(this.Context.TargetBucketName, destFilePath, overwrite);
                    result = newFileInfo.Exists;
                }
                catch (Exception ex)
                {
                    throw new StorageException($"Unable to copy file {sourceFilePath} to {destFilePath}", ex);
                }
            }
            else
            {
                throw new StorageException(string.Format("The source file '{0}' was not found in bucket '{1}'.", sourceFilePath, this.Context.BucketName));
            }

            return result;
        }

        #endregion
    }
}