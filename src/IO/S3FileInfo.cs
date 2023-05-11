﻿/*******************************************************************************
 *  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *  Licensed under the Apache License, Version 2.0 (the "License"). You may not use
 *  this file except in compliance with the License. A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 *  or in the "license" file accompanying this file.
 *  This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 *  CONDITIONS OF ANY KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations under the License.
 * *****************************************************************************
 *    __  _    _  ___
 *   (  )( \/\/ )/ __)
 *   /__\ \    / \__ \
 *  (_)(_) \/\/  (___/
 *
 *  AWS SDK for .NET
 *  API Version: 2006-03-01
 *
 */

namespace Amazon.S3.IO
{
    using System.Globalization;
    using Amazon.S3.Model;
    using Talegen.Common.Core.Extensions;

    /// <summary>
    /// Mimics the System.IO.FileInfo for a file in S3. It exposes properties and methods manipulating files in S3.
    /// </summary>
    public sealed class S3FileInfo : IS3FileSystemInfo
    {
        private readonly IAmazonS3 s3Client;
        private readonly string bucket;
        private readonly string key;

        /// <summary>
        /// Initialize a new instance of the S3FileInfo class for the specified S3 bucket and S3 object key.
        /// </summary>
        /// <param name="s3Client">S3 client which is used to access the S3 resources.</param>
        /// <param name="bucket">Name of the S3 bucket.</param>
        /// <param name="key">The S3 object key.</param>
        /// <exception cref="T:System.ArgumentNullException"></exception>
        public S3FileInfo(IAmazonS3 s3Client, string bucket, string key)
        {
            if (s3Client == null)
            {
                throw new ArgumentNullException(nameof(s3Client));
            }
            if (string.IsNullOrEmpty(bucket))
            {
                throw new ArgumentNullException(nameof(bucket));
            }
            if (string.IsNullOrEmpty(key) || String.Equals(key, "\\"))
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (key.EndsWith("\\", StringComparison.Ordinal))
            {
                throw new ArgumentException("key is a directory name");
            }

            this.s3Client = s3Client;
            this.bucket = bucket;
            this.key = key;
        }

        /// <summary>
        /// Returns the parent S3DirectoryInfo.
        /// </summary>
        public S3DirectoryInfo Directory
        {
            get
            {
                int index = this.key.LastIndexOf('\\');
                string directoryName = string.Empty;

                if (index >= 0)
                {
                    directoryName = this.key.Substring(0, index);
                }

                return new S3DirectoryInfo(this.s3Client, this.bucket, directoryName);
            }
        }

        /// <summary>
        /// The full name of parent directory.
        /// </summary>
        public string DirectoryName => this.Directory.FullName;

        /// <summary>
        /// Checks S3 if the file exists in S3 and return true if it does.
        /// </summary>
        /// <exception cref="T:System.Net.WebException"></exception>
        /// <exception cref="T:Amazon.S3.AmazonS3Exception"></exception>
        public bool Exists
        {
            get
            {
                return this.ExistsWithBucketCheck(out _);
            }
        }

        internal bool ExistsWithBucketCheck(out bool bucketExists)
        {
            bucketExists = true;

            try
            {
                var request = new GetObjectMetadataRequest
                {
                    BucketName = this.bucket,
                    Key = S3Helper.EncodeKey(this.key)
                };

                ((Amazon.Runtime.Internal.IAmazonWebServiceRequest)request).AddBeforeRequestHandler(S3Helper.FileIoRequestEventHandler);

                // If the object doesn't exist then a "NotFound" will be thrown

                AsyncHelper.RunSync(() => this.s3Client.GetObjectMetadataAsync(request));
            }
            catch (AmazonS3Exception e)
            {
                if (string.Equals(e.ErrorCode, "NoSuchBucket"))
                {
                    bucketExists = false;
                }
                else if (string.Equals(e.ErrorCode, "NotFound"))
                {
                    bucketExists = false;
                }
                throw;
            }

            return bucketExists;
        }

        /// <summary>
        /// Gets the file's extension.
        /// </summary>
        public string Extension
        {
            get
            {
                string result;

                int index = this.Name.LastIndexOf('.');

                if (index == -1 || this.Name.Length <= (index + 1))
                {
                    result = string.Empty;
                }
                else
                {
                    result = this.Name.Substring(index + 1);
                }

                return result;
            }
        }

        /// <summary>
        /// Returns the full path including the bucket.
        /// </summary>
        public string FullName => string.Format(CultureInfo.InvariantCulture, "{0}:\\{1}", bucket, key);

        /// <summary>
        /// Returns the last time the file was modified.
        /// </summary>
        /// <exception cref="T:System.Net.WebException"></exception>
        /// <exception cref="T:Amazon.S3.AmazonS3Exception"></exception>
        public DateTime LastWriteTime
        {
            get
            {
                DateTime ret = DateTime.MinValue;

                if (this.Exists)
                {
                    //ret = s3Client.GetObjectMetadata(new GetObjectMetadataRequest()
                    //        .WithBucketName(bucket)
                    //        .WithKey(S3Helper.EncodeKey(key))
                    //        .WithBeforeRequestHandler(S3Helper.FileIORequestEventHandler) as GetObjectMetadataRequest)
                    //    .LastModified.ToLocalTime();
                    var request = new GetObjectMetadataRequest
                    {
                        BucketName = this.bucket,
                        Key = S3Helper.EncodeKey(this.key),
                    };

                    ((Amazon.Runtime.Internal.IAmazonWebServiceRequest)request).AddBeforeRequestHandler(S3Helper.FileIoRequestEventHandler);

                    var response = AsyncHelper.RunSync(() => this.s3Client.GetObjectMetadataAsync(request));

                    ret = response.LastModified.ToLocalTime();
                }

                return ret;
            }
        }

        /// <summary>
        /// Returns the last time the file was modified in UTC.
        /// </summary>
        /// <exception cref="T:System.Net.WebException"></exception>
        /// <exception cref="T:Amazon.S3.AmazonS3Exception"></exception>
        public DateTime LastWriteTimeUtc
        {
            get
            {
                DateTime ret = DateTime.MinValue;

                if (this.Exists)
                {
                    //ret = s3Client.GetObjectMetadata(new GetObjectMetadataRequest()
                    //        .WithBucketName(bucket)
                    //        .WithKey(S3Helper.EncodeKey(key))
                    //        .WithBeforeRequestHandler(S3Helper.FileIORequestEventHandler) as GetObjectMetadataRequest)
                    //    .LastModified;
                    var request = new GetObjectMetadataRequest
                    {
                        BucketName = this.bucket,
                        Key = S3Helper.EncodeKey(this.key),
                    };

                    ((Amazon.Runtime.Internal.IAmazonWebServiceRequest)request).AddBeforeRequestHandler(S3Helper.FileIoRequestEventHandler);

                    var response = AsyncHelper.RunSync(() => this.s3Client.GetObjectMetadataAsync(request));
                    ret = response.LastModified;
                }

                return ret;
            }
        }

        /// <summary>
        /// Returns the content length of the file.
        /// </summary>
        /// <exception cref="T:System.Net.WebException"></exception>
        /// <exception cref="T:Amazon.S3.AmazonS3Exception"></exception>
        public long Length
        {
            get
            {
                long ret = 0;

                if (this.Exists)
                {
                    //ret = s3Client.GetObjectMetadata(new GetObjectMetadataRequest()
                    //        .WithBucketName(bucket)
                    //        .WithKey(S3Helper.EncodeKey(key))
                    //        .WithBeforeRequestHandler(S3Helper.FileIORequestEventHandler) as GetObjectMetadataRequest)
                    //    .ContentLength;
                    var request = new GetObjectMetadataRequest
                    {
                        BucketName = this.bucket,
                        Key = S3Helper.EncodeKey(this.key),
                    };

                    ((Amazon.Runtime.Internal.IAmazonWebServiceRequest)request).AddBeforeRequestHandler(S3Helper.FileIoRequestEventHandler);

                    var response = AsyncHelper.RunSync(() => this.s3Client.GetObjectMetadataAsync(request));

                    ret = response.ContentLength;
                }
                return ret;
            }
        }

        /// <summary>
        /// Returns the file name without its directory name.
        /// </summary>
        public string Name
        {
            get
            {
                int index = this.key.LastIndexOf('\\');
                return this.key.Substring(index + 1);
            }
        }

        /// <summary>
        /// Returns the type of file system element.
        /// </summary>
        public FileSystemType Type => FileSystemType.File;

        /// <summary>
        /// Copies this file's content to the file indicated by the S3 bucket and object key. If the file already exists in S3 than an ArgumentException is thrown.
        /// </summary>
        /// <param name="newBucket">S3 bucket to copy the file to.</param>
        /// <param name="newKey">S3 object key to copy the file to.</param>
        /// <exception cref="T:System.ArgumentException"></exception>
        /// <exception cref="T:System.Net.WebException"></exception>
        /// <exception cref="T:Amazon.S3.AmazonS3Exception"></exception>
        /// <returns>S3FileInfo of the newly copied file.</returns>
        public S3FileInfo CopyTo(string newBucket, string newKey)
        {
            return this.CopyTo(newBucket, newKey, false);
        }

        /// <summary>
        /// Copies this file's content to the file indicated by the S3 bucket and object key. If the file already exists in S3 and overwrite is set to false
        /// than an ArgumentException is thrown.
        /// </summary>
        /// <param name="newBucket">S3 bucket to copy the file to.</param>
        /// <param name="newKey">S3 object key to copy the file to.</param>
        /// <param name="overwrite">Determines whether the file can be overwritten.</param>
        /// <exception cref="T:System.IO.IOException">If the file already exists in S3 and overwrite is set to false.</exception>
        /// <exception cref="T:System.ArgumentException"></exception>
        /// <exception cref="T:System.Net.WebException"></exception>
        /// <exception cref="T:Amazon.S3.AmazonS3Exception"></exception>
        /// <returns>S3FileInfo of the newly copied file.</returns>
        public S3FileInfo CopyTo(string newBucket, string newKey, bool overwrite)
        {
            if (string.IsNullOrEmpty(newBucket))
            {
                throw new ArgumentException("A bucket is required to copy an object", nameof(newBucket));
            }

            S3FileInfo ret;

            if (string.IsNullOrEmpty(newKey) || newKey.EndsWith("\\", StringComparison.Ordinal))
            {
                ret = this.CopyTo(new S3DirectoryInfo(this.s3Client, newBucket, newKey), overwrite);
            }
            else
            {
                ret = this.CopyTo(new S3FileInfo(this.s3Client, newBucket, newKey), overwrite);
            }

            return ret;
        }

        /// <summary>
        /// Copies this file to the target directory. If the file already exists in S3 than an ArgumentException is thrown.
        /// </summary>
        /// <param name="dir">Target directory where to copy the file to.</param>
        /// <exception cref="T:System.ArgumentException">If the directory does not exist.</exception>
        /// <exception cref="T:System.IO.IOException">If the file already exists in S3.</exception>
        /// <exception cref="T:System.Net.WebException"></exception>
        /// <exception cref="T:Amazon.S3.AmazonS3Exception"></exception>
        /// <returns>S3FileInfo of the newly copied file.</returns>
        public S3FileInfo CopyTo(S3DirectoryInfo dir)
        {
            return this.CopyTo(dir, false);
        }

        /// <summary>
        /// Copies this file to the target directory. If the file already exists in S3 and overwrite is set to false than an ArgumentException is thrown.
        /// </summary>
        /// <param name="dir">Target directory where to copy the file to.</param>
        /// <param name="overwrite">Determines whether the file can be overwritten.</param>
        /// <exception cref="T:System.ArgumentException">If the directory does not exist.</exception>
        /// <exception cref="T:System.IO.IOException">If the file already exists in S3 and overwrite is set to false.</exception>
        /// <exception cref="T:System.Net.WebException"></exception>
        /// <exception cref="T:Amazon.S3.AmazonS3Exception"></exception>
        /// <returns>S3FileInfo of the newly copied file.</returns>
        public S3FileInfo CopyTo(S3DirectoryInfo dir, bool overwrite)
        {
            if (!dir.Exists)
            {
                throw new ArgumentException("directory does not exist", nameof(dir));
            }

            return this.CopyTo(new S3FileInfo(dir.S3Client, dir.BucketName, string.Format(CultureInfo.InvariantCulture, "{0}{1}", dir.ObjectKey, this.Name)), overwrite);
        }

        /// <summary>
        /// Copies this file to the location indicated by the passed in S3FileInfo. If the file already exists in S3 than an ArgumentException is thrown.
        /// </summary>
        /// <param name="file">The target location to copy this file to.</param>
        /// <exception cref="T:System.IO.IOException">If the file already exists in S3.</exception>
        /// <exception cref="T:System.Net.WebException"></exception>
        /// <exception cref="T:Amazon.S3.AmazonS3Exception"></exception>
        /// <returns>S3FileInfo of the newly copied file.</returns>
        public S3FileInfo CopyTo(S3FileInfo file)
        {
            return this.CopyTo(file, false);
        }

        /// <summary>
        /// Copies this file to the location indicated by the passed in S3FileInfo. If the file already exists in S3 and overwrite is set to false than an
        /// ArgumentException is thrown.
        /// </summary>
        /// <param name="file">The target location to copy this file to.</param>
        /// <param name="overwrite">Determines whether the file can be overwritten.</param>
        /// <exception cref="T:System.IO.IOException">If the file already exists in S3 and overwrite is set to false.</exception>
        /// <exception cref="T:System.Net.WebException"></exception>
        /// <exception cref="T:Amazon.S3.AmazonS3Exception"></exception>
        /// <returns>S3FileInfo of the newly copied file.</returns>
        public S3FileInfo CopyTo(S3FileInfo file, bool overwrite)
        {
            if (!overwrite)
            {
                if (file.Exists)
                {
                    throw new IOException("File already exists");
                }
            }

            if (this.SameClient(file))
            {
                var request = new CopyObjectRequest
                {
                    DestinationBucket = file.BucketName,
                    DestinationKey = S3Helper.EncodeKey(file.ObjectKey),
                    SourceBucket = this.bucket,
                    SourceKey = S3Helper.EncodeKey(this.key)
                };

                ((Amazon.Runtime.Internal.IAmazonWebServiceRequest)request).AddBeforeRequestHandler(S3Helper.FileIoRequestEventHandler);
                AsyncHelper.RunSync(() => this.s3Client.CopyObjectAsync(request));
            }
            else
            {
                var getObjectRequest = new GetObjectRequest
                {
                    BucketName = bucket,
                    Key = S3Helper.EncodeKey(key)
                };

                ((Amazon.Runtime.Internal.IAmazonWebServiceRequest)getObjectRequest).AddBeforeRequestHandler(S3Helper.FileIoRequestEventHandler);
                var getObjectResponse = AsyncHelper.RunSync(() => this.s3Client.GetObjectAsync(getObjectRequest));

                using (Stream stream = getObjectResponse.ResponseStream)
                {
                    var putObjectRequest = new PutObjectRequest
                    {
                        BucketName = file.BucketName,
                        Key = S3Helper.EncodeKey(file.ObjectKey),
                        InputStream = stream
                    };

                    ((Amazon.Runtime.Internal.IAmazonWebServiceRequest)putObjectRequest).AddBeforeRequestHandler(S3Helper.FileIoRequestEventHandler);
                    AsyncHelper.RunSync(() => file.S3Client.PutObjectAsync(putObjectRequest));
                }
            }

            return file;
        }

        /// <summary>
        /// Copies from S3 to the local file system. If the file already exists on the local file system than an ArgumentException is thrown.
        /// </summary>
        /// <param name="destFileName">The path where to copy the file to.</param>
        /// <exception cref="T:System.IO.IOException">If the file already exists locally.</exception>
        /// <exception cref="T:System.Net.WebException"></exception>
        /// <exception cref="T:Amazon.S3.AmazonS3Exception"></exception>
        /// <returns>FileInfo for the local file where file is copied to.</returns>
        public FileInfo CopyToLocal(string destFileName)
        {
            return this.CopyToLocal(destFileName, false);
        }

        /// <summary>
        /// Copies from S3 to the local file system. If the file already exists on the local file system and overwrite is set to false than an ArgumentException
        /// is thrown.
        /// </summary>
        /// <param name="destFileName">The path where to copy the file to.</param>
        /// <param name="overwrite">Determines whether the file can be overwritten.</param>
        /// <exception cref="T:System.IO.IOException">If the file already exists locally and overwrite is set to false.</exception>
        /// <exception cref="T:System.Net.WebException"></exception>
        /// <exception cref="T:Amazon.S3.AmazonS3Exception"></exception>
        /// <returns>FileInfo for the local file where file is copied to.</returns>
        public FileInfo CopyToLocal(string destFileName, bool overwrite)
        {
            if (!overwrite)
            {
                if (new FileInfo(destFileName).Exists)
                {
                    throw new IOException("File already exists");
                }
            }

            var getObjectRequest = new GetObjectRequest
            {
                BucketName = this.bucket,
                Key = S3Helper.EncodeKey(this.key)
            };

            ((Amazon.Runtime.Internal.IAmazonWebServiceRequest)getObjectRequest).AddBeforeRequestHandler(S3Helper.FileIoRequestEventHandler);

            using (var getObjectResponse = AsyncHelper.RunSync(() => this.s3Client.GetObjectAsync(getObjectRequest)))
            {
                AsyncHelper.RunSync(() =>

                    // ReSharper disable once AccessToDisposedClosure
                    getObjectResponse?.WriteResponseStreamToFileAsync(destFileName, false, CancellationToken.None));
            }

            return new FileInfo(destFileName);
        }

        /// <summary>
        /// Copies the file from the local file system to S3. If the file already exists in S3 than an ArgumentException is thrown.
        /// </summary>
        /// <param name="srcFileName">Location of the file on the local file system to copy.</param>
        /// <exception cref="T:System.IO.IOException">If the file already exists in S3.</exception>
        /// <exception cref="T:System.Net.WebException"></exception>
        /// <exception cref="T:Amazon.S3.AmazonS3Exception"></exception>
        /// <returns>S3FileInfo where the file is copied to.</returns>
        public S3FileInfo CopyFromLocal(string srcFileName)
        {
            return this.CopyFromLocal(srcFileName, false);
        }

        /// <summary>
        /// Copies the file from the local file system to S3. If the file already exists in S3 and overwrite is set to false than an ArgumentException is thrown.
        /// </summary>
        /// <param name="srcFileName">Location of the file on the local file system to copy.</param>
        /// <param name="overwrite">Determines whether the file can be overwritten.</param>
        /// <exception cref="T:System.IO.IOException">If the file already exists in S3 and overwrite is set to false.</exception>
        /// <exception cref="T:System.Net.WebException"></exception>
        /// <exception cref="T:Amazon.S3.AmazonS3Exception"></exception>
        /// <returns>S3FileInfo where the file is copied to.</returns>
        public S3FileInfo CopyFromLocal(string srcFileName, bool overwrite)
        {
            if (!overwrite)
            {
                if (this.Exists)
                {
                    throw new IOException("File already exists");
                }
            }

            var putObjectRequest = new PutObjectRequest
            {
                BucketName = bucket,
                Key = S3Helper.EncodeKey(this.key),
                FilePath = srcFileName
            };

            ((Amazon.Runtime.Internal.IAmazonWebServiceRequest)putObjectRequest).AddBeforeRequestHandler(S3Helper.FileIoRequestEventHandler);

            AsyncHelper.RunSync(() => this.s3Client.PutObjectAsync(putObjectRequest));

            return this;
        }

        /// <summary>
        /// Returns a Stream that can be used to write data to S3.
        /// </summary>
        /// <remarks><note>The content will not be written to S3 until the Stream is closed.</note></remarks>
        /// <exception cref="T:System.Net.WebException"></exception>
        /// <exception cref="T:Amazon.S3.AmazonS3Exception"></exception>
        /// <returns>Stream to write content to.</returns>
        public Stream Create()
        {
            return new S3FileStream(this.S3Client, this.BucketName, this.ObjectKey, FileMode.Create, FileAccess.Write);
        }

        /// <summary>
        /// Returns a StreamWriter that can be used to write data to S3.
        /// </summary>
        /// <remarks><note>The content will not be written to S3 until the Stream is closed.</note></remarks>
        /// <exception cref="T:System.Net.WebException"></exception>
        /// <exception cref="T:Amazon.S3.AmazonS3Exception"></exception>
        /// <returns>Stream to write content to.</returns>
        public StreamWriter CreateText()
        {
            return new StreamWriter(this.Create());
        }

        /// <summary>
        /// Deletes the from S3.
        /// </summary>
        /// <exception cref="T:System.Net.WebException"></exception>
        /// <exception cref="T:Amazon.S3.AmazonS3Exception"></exception>
        public void Delete()
        {
            if (this.Exists)
            {
                var deleteObjectRequest = new DeleteObjectRequest
                {
                    BucketName = this.bucket,
                    Key = S3Helper.EncodeKey(this.key)
                };

                ((Amazon.Runtime.Internal.IAmazonWebServiceRequest)deleteObjectRequest).AddBeforeRequestHandler(S3Helper.FileIoRequestEventHandler);
                AsyncHelper.RunSync(() => this.s3Client.DeleteObjectAsync(deleteObjectRequest));

                this.Directory.Create();
            }
        }

        /// <summary>
        /// Moves the file to a a new location in S3.
        /// </summary>
        /// <param name="targetBucket">Bucket to move the file to.</param>
        /// <param name="targetKey">Object key to move the file to.</param>
        /// <exception cref="T:System.IO.IOException">If the file already exists in S3.</exception>
        /// <exception cref="T:System.ArgumentException"></exception>
        /// <exception cref="T:System.Net.WebException"></exception>
        /// <exception cref="T:Amazon.S3.AmazonS3Exception"></exception>
        /// <returns>S3FileInfo for the target location.</returns>
        public S3FileInfo MoveTo(string targetBucket, string targetKey)
        {
            S3FileInfo ret = this.CopyTo(targetBucket, targetKey, false);
            this.Delete();
            return ret;
        }

        /// <summary>
        /// Moves the file to a a new location in S3.
        /// </summary>
        /// <param name="path">The target directory to copy to.</param>
        /// <exception cref="T:System.IO.IOException">If the file already exists in S3.</exception>
        /// <exception cref="T:System.ArgumentException"></exception>
        /// <exception cref="T:System.Net.WebException"></exception>
        /// <exception cref="T:Amazon.S3.AmazonS3Exception"></exception>
        /// <returns>S3FileInfo for the target location.</returns>
        public S3FileInfo MoveTo(S3DirectoryInfo path)
        {
            S3FileInfo ret = this.CopyTo(path, false);
            this.Delete();
            return ret;
        }

        /// <summary>
        /// Moves the file to a a new location in S3.
        /// </summary>
        /// <param name="file">The target file to copy to.</param>
        /// <exception cref="T:System.IO.IOException">If the file already exists in S3.</exception>
        /// <exception cref="T:System.ArgumentException"></exception>
        /// <exception cref="T:System.Net.WebException"></exception>
        /// <exception cref="T:Amazon.S3.AmazonS3Exception"></exception>
        /// <returns>S3FileInfo for the target location.</returns>
        public S3FileInfo MoveTo(S3FileInfo file)
        {
            S3FileInfo ret = this.CopyTo(file, false);
            this.Delete();
            return ret;
        }

        /// <summary>
        /// Moves the file from S3 to the local file system in the location indicated by the path parameter.
        /// </summary>
        /// <param name="path">The location on the local file system to move the file to.</param>
        /// <exception cref="T:System.IO.IOException">If the file already exists locally.</exception>
        /// <exception cref="T:System.ArgumentException"></exception>
        /// <exception cref="T:System.Net.WebException"></exception>
        /// <exception cref="T:Amazon.S3.AmazonS3Exception"></exception>
        /// <returns>FileInfo for the local file where file is moved to.</returns>
        public FileInfo MoveToLocal(string path)
        {
            FileInfo ret = this.CopyToLocal(path, false);
            this.Delete();
            return ret;
        }

        /// <summary>
        /// Moves the file from the local file system to S3 in this directory. If the file already exists in S3 than an ArgumentException is thrown.
        /// </summary>
        /// <param name="path">The local file system path where the files are to be moved.</param>
        /// <exception cref="T:System.IO.IOException">If the file already exists locally.</exception>
        /// <exception cref="T:System.ArgumentException"></exception>
        /// <exception cref="T:System.Net.WebException"></exception>
        /// <exception cref="T:Amazon.S3.AmazonS3Exception"></exception>
        /// <returns>S3FileInfo where the file is moved to.</returns>
        public S3FileInfo MoveFromLocal(string path)
        {
            S3FileInfo ret = this.CopyFromLocal(path, false);
            File.Delete(path);
            return ret;
        }

        /// <summary>
        /// Moves the file from the local file system to S3 in this directory. If the file already exists in S3 and overwrite is set to false than an
        /// ArgumentException is thrown.
        /// </summary>
        /// <param name="path">The local file system path where the files are to be moved.</param>
        /// <param name="overwrite">Determines whether the file can be overwritten.</param>
        /// <exception cref="T:System.IO.IOException">If the file already exists in S3 and overwrite is set to false.</exception>
        /// <exception cref="T:System.ArgumentException"></exception>
        /// <exception cref="T:System.Net.WebException"></exception>
        /// <exception cref="T:Amazon.S3.AmazonS3Exception"></exception>
        /// <returns>S3FileInfo where the file is moved to.</returns>
        public S3FileInfo MoveFromLocal(string path, bool overwrite)
        {
            S3FileInfo ret = this.CopyFromLocal(path, overwrite);
            File.Delete(path);
            return ret;
        }

        /// <summary>
        /// Returns a Stream for reading the contents of the file.
        /// </summary>
        /// <exception cref="T:System.IO.IOException">The file is already open.</exception>
        /// <exception cref="T:System.Net.WebException"></exception>
        /// <exception cref="T:Amazon.S3.AmazonS3Exception"></exception>
        /// <returns>Stream to read from.</returns>
        public Stream OpenRead()
        {
            return new S3FileStream(this.S3Client, this.BucketName, this.ObjectKey, FileMode.Open, FileAccess.Read);
        }

        /// <summary>
        /// Returns a StreamReader for reading the contents of the file.
        /// </summary>
        /// <exception cref="T:System.IO.IOException">The file is already open.</exception>
        /// <exception cref="T:System.Net.WebException"></exception>
        /// <exception cref="T:Amazon.S3.AmazonS3Exception"></exception>
        /// <returns>StreamReader to read from.</returns>
        public StreamReader OpenText()
        {
            return new StreamReader(this.OpenRead());
        }

        /// <summary>
        /// Returns a Stream for writing to S3. If the file already exists it will be overwritten.
        /// </summary>
        /// <remarks><note>The content will not be written to S3 until the Stream is closed.</note></remarks>
        /// <exception cref="T:System.IO.IOException">The file is already open.</exception>
        /// <exception cref="T:System.Net.WebException"></exception>
        /// <exception cref="T:Amazon.S3.AmazonS3Exception"></exception>
        /// <returns>Stream to write from.</returns>
        public Stream OpenWrite()
        {
            return new S3FileStream(this.S3Client, this.BucketName, this.ObjectKey, FileMode.OpenOrCreate, FileAccess.Write);
        }

        /// <summary>
        /// Replaces the destination file with the content of this file and then deletes the original file. If a backup location is specified then the content
        /// of destination file is backup to it.
        /// </summary>
        /// <param name="destinationBucket">Destination bucket of this file will be copy to.</param>
        /// <param name="destinationKey">Destination object key of this file will be copy to.</param>
        /// <param name="backupBucket">Backup bucket to store the contents of the destination file.</param>
        /// <param name="backupKey">Backup object key to store the contents of the destination file.</param>
        /// <exception cref="T:System.ArgumentException"></exception>
        /// <exception cref="T:System.IO.IOException"></exception>
        /// <exception cref="T:System.Net.WebException"></exception>
        /// <exception cref="T:Amazon.S3.AmazonS3Exception"></exception>
        /// <returns>S3FileInfo of the destination file.</returns>
        public S3FileInfo Replace(string destinationBucket, string destinationKey, string backupBucket, string backupKey)
        {
            if (string.IsNullOrEmpty(destinationBucket))
            {
                throw new ArgumentException("A bucket is required to replace an object", nameof(destinationBucket));
            }

            S3FileInfo destinationInfo;

            if (string.IsNullOrEmpty(destinationKey))
            {
                destinationInfo = new S3FileInfo(this.s3Client, destinationBucket, this.Name);
            }
            else if (destinationKey.EndsWith("\\", StringComparison.Ordinal))
            {
                destinationInfo = new S3FileInfo(this.s3Client, destinationBucket, destinationKey + this.Name);
            }
            else
            {
                destinationInfo = new S3FileInfo(this.s3Client, destinationBucket, destinationKey);
            }

            S3FileInfo backupInfo = null;

            if (!string.IsNullOrEmpty(backupBucket))
            {
                if (string.IsNullOrEmpty(backupKey))
                {
                    backupInfo = new S3FileInfo(this.s3Client, backupBucket, this.Name);
                }
                else if (backupKey.EndsWith("\\", StringComparison.Ordinal))
                {
                    backupInfo = new S3FileInfo(this.s3Client, backupBucket, backupKey + this.Name);
                }
                else
                {
                    backupInfo = new S3FileInfo(this.s3Client, backupBucket, backupKey);
                }
            }

            return this.Replace(destinationInfo, backupInfo);
        }

        /// <summary>
        /// Replaces the destination file with the content of this file and then deletes the original file. If a backupDir is specified then the content of
        /// destination file is backup to it.
        /// </summary>
        /// <param name="destDir">Where the contents of this file will be copy to.</param>
        /// <param name="backupDir">If specified the destFile is backup to it.</param>
        /// <exception cref="T:System.ArgumentException"></exception>
        /// <exception cref="T:System.IO.IOException"></exception>
        /// <exception cref="T:System.Net.WebException"></exception>
        /// <exception cref="T:Amazon.S3.AmazonS3Exception"></exception>
        /// <returns>S3FileInfo of the destination file.</returns>
        public S3FileInfo Replace(S3DirectoryInfo destDir, S3DirectoryInfo backupDir)
        {
            return this.Replace(
                new S3FileInfo(destDir.S3Client, destDir.BucketName, string.Format(CultureInfo.InvariantCulture, "{0}{1}", destDir.ObjectKey, this.Name)),
                backupDir == null ? null : new S3FileInfo(backupDir.S3Client, backupDir.BucketName, string.Format(CultureInfo.InvariantCulture, "{0}{1}", backupDir.ObjectKey, this.Name)));
        }

        /// <summary>
        /// Replaces the destination file with the content of this file and then deletes the original file. If a backupFile is specified then the content of
        /// destination file is backup to it.
        /// </summary>
        /// <param name="destFile">Where the contents of this file will be copy to.</param>
        /// <param name="backupFile">If specified the destFile is backup to it.</param>
        /// <exception cref="T:System.ArgumentException"></exception>
        /// <exception cref="T:System.IO.IOException"></exception>
        /// <exception cref="T:System.Net.WebException"></exception>
        /// <exception cref="T:Amazon.S3.AmazonS3Exception"></exception>
        /// <returns>S3FileInfo of the destination file.</returns>
        public S3FileInfo Replace(S3FileInfo destFile, S3FileInfo backupFile)
        {
            if (string.Equals(this.BucketName, destFile.BucketName) && string.Equals(this.ObjectKey, destFile.ObjectKey))
            {
                throw new ArgumentException("Destination file can not be the same as the source file when doing a replace.", nameof(destFile));
            }

            if (backupFile != null)
            {
                destFile.CopyTo(backupFile, true);
            }

            S3FileInfo ret = this.CopyTo(destFile, true);
            this.Delete();

            return ret;
        }

        /// <summary>
        /// Replaces the content of the destination file on the local file system with the content from this file from S3. If a backup file is specified then
        /// the content of the destination file is backup to it.
        /// </summary>
        /// <param name="destinationFileName"></param>
        /// <param name="destinationBackupFileName"></param>
        /// <exception cref="T:System.ArgumentException"></exception>
        /// <exception cref="T:System.IO.IOException"></exception>
        /// <exception cref="T:System.Net.WebException"></exception>
        /// <exception cref="T:Amazon.S3.AmazonS3Exception"></exception>
        /// <returns></returns>
        public FileInfo ReplaceLocal(string destinationFileName, string destinationBackupFileName)
        {
            if (!string.IsNullOrEmpty(destinationBackupFileName))
            {
                File.Replace(destinationFileName, destinationBackupFileName, null);
            }

            return this.CopyToLocal(destinationFileName, true);
        }

        /// <summary>
        /// Implement the ToString method.
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return this.FullName;
        }

        internal IAmazonS3 S3Client
        {
            get
            {
                return this.s3Client;
            }
        }

        internal string BucketName
        {
            get
            {
                return this.bucket;
            }
        }

        internal string ObjectKey
        {
            get
            {
                return this.key;
            }
        }

        private bool SameClient(S3FileInfo otherFile)
        {
            return this.s3Client.Equals(otherFile.S3Client);
        }
    }
}