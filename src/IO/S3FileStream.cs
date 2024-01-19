/*******************************************************************************
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
    using System;
    using System.IO;
    using Amazon.S3.Model;
    using Amazon.Util;
    using Talegen.Common.Core.Extensions;

    internal class S3FileStream : Stream
    {
        private static readonly int Meg = (int)Math.Pow(2, 20);
        private static readonly int MinSize = 5 * Meg;

        private readonly S3FileInfo file;
        private readonly MemoryStream buffer;

        private readonly bool canRead = true;
        private readonly bool canSeek = true;
        private readonly bool canWrite = true;
        private readonly long startPosition = 0;
        private bool disposed = false;

        private bool bucketExist;
        private readonly bool fileExist;
        private long lastWriteCounter = 0;
        private long lastFlushMarker = -1;

        internal S3FileStream(IAmazonS3 s3Client, string bucket, string key, FileMode mode)
            : this(s3Client, bucket, key, mode, FileAccess.ReadWrite, MinSize)
        { }

        internal S3FileStream(IAmazonS3 s3Client, string bucket, string key, FileMode mode, FileAccess access)
            : this(s3Client, bucket, key, mode, access, 0)
        { }

        internal S3FileStream(IAmazonS3 s3Client, string bucket, string key, FileMode mode, FileAccess access, int buffersize)
        {
            this.file = new S3FileInfo(s3Client, bucket, key);
            this.buffer = new MemoryStream(buffersize);

            this.fileExist = this.file.ExistsWithBucketCheck(out this.bucketExist);

            if ((access & FileAccess.Read) != FileAccess.Read)
            {
                this.canRead = false;
            }
            if ((access & FileAccess.Write) != FileAccess.Write)
            {
                this.canWrite = false;
            }

            switch (mode)
            {
                case FileMode.Append:
                    if ((access & FileAccess.Write) != FileAccess.Write)
                    {
                        throw new ArgumentException("Append requires Write access");
                    }

                    this.PopulateData();
                    this.buffer.Seek(0, SeekOrigin.End);
                    this.startPosition = this.buffer.Position;
                    break;

                case FileMode.Create:
                    if ((access & FileAccess.Write) != FileAccess.Write)
                    {
                        throw new ArgumentException("Create requires Write access");
                    }
                    break;

                case FileMode.CreateNew:
                    if (this.fileExist)
                    {
                        throw new IOException("CreateNew requires the file not to already exist");
                    }
                    if ((access & FileAccess.Write) != FileAccess.Write)
                    {
                        throw new ArgumentException("Create requires Write access");
                    }
                    break;

                case FileMode.Open:
                    if (!this.fileExist)
                    {
                        throw new IOException("Open requires the file to already exist");
                    }

                    this.PopulateData();
                    break;

                case FileMode.OpenOrCreate:
                    if (this.fileExist)
                    {
                        if ((access & FileAccess.Write) != FileAccess.Write)
                        {
                            throw new ArgumentException("Create requires Write access");
                        }
                    }
                    break;

                case FileMode.Truncate:
                    if (!this.fileExist)
                    {
                        throw new IOException("Truncate requires the file to already exist");
                    }
                    if ((access & FileAccess.Write) != FileAccess.Write)
                    {
                        throw new ArgumentException("Truncate requires Write access");
                    }
                    break;

                default:
                    throw new ArgumentException("Invalid value", nameof(mode));
            }
        }

        public override bool CanRead => this.canRead;

        public override bool CanSeek => this.canSeek;

        public override bool CanTimeout { get; } = true;

        public override bool CanWrite => this.canWrite;

        public override long Length => this.buffer.Length;

        public string Name => this.file.FullName;

        public override long Position
        {
            get => this.buffer.Position;
            set
            {
                if (!this.CanSeek)
                {
                    throw new NotSupportedException("The stream does not support seeking");
                }
                if (value < this.startPosition)
                {
                    throw new ArgumentOutOfRangeException(nameof(value), "Can not seek to specified location");
                }

                this.buffer.Position = value;
            }
        }

        public override int ReadTimeout
        {
            get => this.buffer.ReadTimeout;
            set => this.buffer.ReadTimeout = value;
        }

        public override int WriteTimeout
        {
            get => this.buffer.WriteTimeout;
            set => this.buffer.WriteTimeout = value;
        }

        public override IAsyncResult BeginRead(byte[] targetBuffer, int offset, int count, AsyncCallback callback, object state)
        {
            if (!this.canRead)
            {
                throw new NotSupportedException("Stream does not support reading");
            }
            return this.buffer.BeginRead(targetBuffer, offset, count, callback, state);
        }

        public override IAsyncResult BeginWrite(byte[] targetBuffer, int offset, int count, AsyncCallback callback, object state)
        {
            if (!this.canWrite)
            {
                throw new NotSupportedException("Stream does not support writing");
            }
            return this.buffer.BeginWrite(targetBuffer, offset, count, callback, state);
        }

        protected override void Dispose(bool disposing)
        {
            if (!this.disposed)
            {
                if (disposing)
                {
                    this.Flush(true);
                    this.buffer.Dispose();
                }

                this.disposed = true;
            }
        }

        public override int EndRead(IAsyncResult asyncResult)
        {
            return this.buffer.EndRead(asyncResult);
        }

        public override void EndWrite(IAsyncResult asyncResult)
        {
            this.buffer.EndWrite(asyncResult);
        }

        public override void Flush()
        {
            this.Flush(true);
        }

        public void Flush(bool flushToS3)
        {
            if (this.canWrite && flushToS3 && this.lastWriteCounter != this.lastFlushMarker)
            {
                long pos = this.Position;

                try
                {
                    if (!this.bucketExist)
                    {
                        AsyncHelper.RunSync(() => this.file.S3Client.PutBucketAsync(new PutBucketRequest { BucketName = this.file.BucketName }));
                        this.bucketExist = true;
                    }

                    var request = new PutObjectRequest
                    {
                        BucketName = this.file.BucketName,
                        Key = S3Helper.EncodeKey(this.file.ObjectKey),
                        InputStream = this.buffer,
                        AutoCloseStream = false
                    };
                    ((Amazon.Runtime.Internal.IAmazonWebServiceRequest)request).AddBeforeRequestHandler(S3Helper.FileIoRequestEventHandler);

                    try
                    {
                        this.buffer.Seek(0, SeekOrigin.Begin);
                        AsyncHelper.RunSync(() => this.file.S3Client.PutObjectAsync(request));
                    }
                    catch (AmazonS3Exception e)
                    {
                        if (!string.Equals(e.ErrorCode, "NoSuchBucket"))
                            throw;

                        // Bucket no longer exists so create and retry put
                        this.file.Directory.Create();

                        this.buffer.Seek(0, SeekOrigin.Begin);
                        AsyncHelper.RunSync(() => this.file.S3Client.PutObjectAsync(request));
                    }
                    this.lastFlushMarker = this.lastWriteCounter;
                }
                finally
                {
                    this.buffer.Seek(pos, SeekOrigin.Begin);
                }
            }
        }

        public override int Read(byte[] sourceBuffer, int offset, int count)
        {
            if (!this.canRead)
            {
                throw new NotSupportedException("Stream does not support reading");
            }
            return this.buffer.Read(sourceBuffer, offset, count);
        }

        public override int ReadByte()
        {
            if (!this.canRead)
            {
                throw new NotSupportedException("Stream does not support reading");
            }
            return this.buffer.ReadByte();
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            if (!this.canSeek)
            {
                throw new NotSupportedException("Stream does not support seeking");
            }
            if (this.LessThanStart(offset, origin))
            {
                throw new ArgumentOutOfRangeException(nameof(offset), "Attempting to seek to a restricted section");
            }
            return this.buffer.Seek(offset, origin);
        }

        public override void SetLength(long value)
        {
            if (!this.canWrite && !this.canSeek)
            {
                throw new NotSupportedException();
            }

            this.buffer.SetLength(value);
        }

        public override void Write(byte[] targetBuffer, int offset, int count)
        {
            if (!this.canWrite)
            {
                throw new NotSupportedException("Stream does not support writing");
            }
            this.buffer.Write(targetBuffer, offset, count);
            this.lastWriteCounter++;
        }

        public override void WriteByte(byte value)
        {
            if (!this.canWrite)
            {
                throw new NotSupportedException("Stream does not support writing");
            }

            this.buffer.WriteByte(value);
            this.lastWriteCounter++;
        }

        private void PopulateData()
        {
            var getObjectRequest = new GetObjectRequest
            {
                BucketName = this.file.BucketName,
                Key = S3Helper.EncodeKey(this.file.ObjectKey)
            };
            ((Amazon.Runtime.Internal.IAmazonWebServiceRequest)getObjectRequest).AddBeforeRequestHandler(S3Helper.FileIoRequestEventHandler);
            var getObjectResponse = AsyncHelper.RunSync(() => this.file.S3Client.GetObjectAsync(getObjectRequest));
            using (Stream data = getObjectResponse.ResponseStream)
            {
                byte[] tempBuffer = new byte[AWSSDKUtils.DefaultBufferSize];
                int bytesRead;
                while ((bytesRead = data.Read(tempBuffer, 0, tempBuffer.Length)) > 0)
                {
                    this.buffer.Write(tempBuffer, 0, bytesRead);
                }
            }

            this.buffer.Position = 0;
        }

        private bool LessThanStart(long offset, SeekOrigin origin)
        {
            bool ret = false;

            ret |= ((origin == SeekOrigin.Begin) && (offset < this.startPosition));
            ret |= ((origin == SeekOrigin.Current) && (offset < this.startPosition - this.Position));
            ret |= ((origin == SeekOrigin.End) && (offset < this.startPosition - this.Length));

            return ret;
        }
    }
}