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
    using System;
    using Amazon.S3;
    using Talegen.Storage.Net.Core;

    /// <summary>
    /// This class contains properties related to the storage service interfaces within the application.
    /// </summary>
    public class AwsS3StorageContext : IStorageContext
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="AwsS3StorageContext" /> class.
        /// </summary>
        /// <param name="config">Contains an instance of the <see cref="Amazon.S3.AmazonS3Config" /> class.</param>
        /// <param name="bucketName">Contains the bucket name.</param>
        public AwsS3StorageContext(AmazonS3Config config, string bucketName)
        {
            this.Config = config;
            this.WorkspaceUri = new Uri(config.ServiceURL); // new Uri(config.ServiceURL);
            this.UniqueWorkspace = true;
            this.BucketName = bucketName;
        }

        /// <summary>
        /// Gets the AWS SDK client settings.
        /// </summary>
        /// <value>Contains the AWS SDK client settings.</value>
        public AmazonS3Config Config { get; }

        /// <summary>
        /// Gets the bucket name.
        /// </summary>
        /// <value>Contains the bucket name.</value>
        public string BucketName { get; }

        #region IStorageContext Members
        /// <summary>
        /// Gets the storage type.
        /// </summary>
        public string StorageType => "AwsS3";

        /// <summary>
        /// Gets a value indicating whether this instance is unique workspace.
        /// </summary>
        /// <returns>Contains the value indicating whether this instance is unique workspace.</returns>
        public bool UniqueWorkspace { get; }

        /// <summary>
        /// Gets the workspace URI.
        /// </summary>
        /// <value>Contains the workspace URI.</value>
        public Uri WorkspaceUri { get; }
        
        /// <summary>
        /// Gets the root workspace local folder path.
        /// </summary>
        /// <returns>Contains the root workspace local folder path.</returns>
        public string RootWorkspaceLocalFolderPath => this.WorkspaceUri?.ToString();
        #endregion
    }
}