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
    using System.Text;
    using Talegen.Common.Core.Extensions;
    using Talegen.Storage.Net.Core;
    using AWSSDK;
    using Amazon.S3;
    using Amazon.S3.Model;

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
        /// 
        /// </summary>
        /// <param name="sourceFilePath"></param>
        /// <param name="targetFilePath"></param>
        /// <param name="overwrite"></param>
        public void CopyFile(string sourceFilePath, string targetFilePath, bool overwrite = true)
        {
            // copy source file to target file in S3 bucket.

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
                AsyncHelper.RunSync(() => this.client.MakeObjectPublicAsync(this.Context.BucketName, $"{this.RootPath}/{subFolderName}", true));    
                newPathResult = $"{this.RootPath}/{subFolderName}";
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
                // if not recursive, throw exception if the directory contains contents
                if (!recursive)
                {
                    // TODO: implement check on directory contents.
                }

                var response = AsyncHelper.RunSync(() => this.client.DeleteObjectAsync(this.Context.BucketName, $"{this.RootPath}/{subFolderName}"));
        
                if (response.HttpStatusCode == System.Net.HttpStatusCode.NotFound && !silentNoExist)
                {
                    throw new StorageException($"Unable to delete directory {subFolderName}.");
                }   
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
                var response = AsyncHelper.RunSync(() => this.client.DeleteObjectAsync(this.Context.BucketName, $"{this.RootPath}/{filePath}"));
        
                if (response.HttpStatusCode == System.Net.HttpStatusCode.NotFound)
                {
                    throw new Exception($"Unable to delete file {filePath}.");
                }
                else if (deleteDirectory)
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
        /// This method is used to determine if a directory exists.
        /// </summary>
        /// <param name="subFolderName">Contains the name of the directory to check.</param>
        /// <param name="silentExists">Contains a value indicating whether to ignore a storage exception.</param>
        /// <returns>Returns a value indicating whether the directory exists.</returns>
        public bool DirectoryExists(string subFolderName, bool silentExists = false)
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

            ListObjectsV2Request request = new ListObjectsV2Request
            {
                BucketName = this.Context.BucketName,
                Prefix = $"{this.RootPath}/{subFolderName}",
            };

            var result = AsyncHelper.RunSync(() => this.client.ListObjectsV2Async(request));
            return (result != null && result.S3Objects != null && result.S3Objects.Count > 0);
        }


        public void EmptyDirectory(string subFolderName)
        {
            throw new NotImplementedException();
        }

        public bool FileExists(string filePath)
        {
            throw new NotImplementedException();
        }

        public string FileHash(string filePath)
        {
            throw new NotImplementedException();
        }

        public List<string> FindFiles(string subFolderName, string searchPattern = "*.*", SearchOption searchOption = SearchOption.AllDirectories)
        {
            throw new NotImplementedException();
        }

        public void MoveFile(string sourceFilePath, string targetFilePath, bool overwrite = true)
        {
            throw new NotImplementedException();
        }

        public byte[] ReadBinaryFile(string path)
        {
            throw new NotImplementedException();
        }

        public void ReadBinaryFile(string path, Stream outputStream)
        {
            throw new NotImplementedException();
        }

        public string ReadTextFile(string path, Encoding encoding = null)
        {
            throw new NotImplementedException();
        }

        public bool WriteBinaryFile(string path, byte[] content)
        {
            throw new NotImplementedException();
        }

        public bool WriteBinaryFile(string path, Stream inputStream)
        {
            throw new NotImplementedException();
        }

        public bool WriteTextFile(string path, string content, Encoding encoding = null)
        {
            throw new NotImplementedException();
        }
        #endregion

        #region Private Methods
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
            if (Path.IsPathRooted(subFolderName))
            {
                // a root path was defined in subFolderName, ensure it is under this.RootPath
                if (!Path.GetFullPath(subFolderName).StartsWith(Path.GetFullPath(this.RootPath)))
                {
                    throw new StorageException(string.Format("The folder \"{0}\" is not under the root path \"{1}\".", subFolderName, this.RootPath));
                }
            }
        }
        #endregion
    }
}