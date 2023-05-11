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
    using Common.Core.Extensions;
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
        /// <param name="targetBucketName">Contains an optional target bucket name if different.</param>
        public AwsS3StorageContext(AmazonS3Config config, string bucketName, string targetBucketName = "")
        {
            this.Config = config ?? throw new ArgumentNullException(nameof(config));
            this.WorkspaceUri = new Uri(config.ServiceURL);
            this.UniqueWorkspace = true;

            if (!string.IsNullOrWhiteSpace(bucketName))
            {
                this.BucketName = bucketName;
            }
            else
            {
                throw new ArgumentNullException(nameof(bucketName));
            }

            // default target to bucket name.
            this.TargetBucketName = string.IsNullOrWhiteSpace(targetBucketName) ? this.BucketName : targetBucketName;
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

        /// <summary>
        /// Gets or sets the target bucket name.
        /// </summary>
        public string TargetBucketName { get; }

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
        public string RootWorkspaceLocalFolderPath => this.WorkspaceUri.ConvertToString();

        #endregion
    }
}