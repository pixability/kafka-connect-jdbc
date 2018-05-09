/*
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.connect.jdbc.sink;

import com.amazonaws.AmazonClientException;
import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

class S3OutputStream extends OutputStream {
  private static final Logger log = LoggerFactory.getLogger(S3OutputStream.class);

  private final AmazonS3 s3;
  private final String bucket;
  private final String key;
  private final ProgressListener progressListener = new ConnectProgressListener();
  private final int partSize;
  private boolean closed;
  private ByteBuffer buffer;
  private MultipartUpload multiPartUpload;

  S3OutputStream(String bucket, String key, int partSize, AmazonS3 s3) {
    this.s3 = s3;
    this.partSize = partSize;
    this.key = key;
    this.bucket = bucket;
    this.buffer = ByteBuffer.allocate(this.partSize);
  }

  @Override
  public void write(int b) throws IOException {
    buffer.put((byte) b);
    if (!buffer.hasRemaining()) {
      uploadPart();
    }
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    if (b == null) {
      throw new NullPointerException();
    } else if (off < 0 || off > b.length || len < 0 || (off + len) > b.length || (off + len) < 0) {
      throw new IndexOutOfBoundsException();
    } else if (len == 0) {
      return;
    }

    if (buffer.remaining() < len) {
      int firstPart = buffer.remaining();
      buffer.put(b, off, firstPart);
      uploadPart();
      write(b, off + firstPart, len - firstPart);
    } else {
      buffer.put(b, off, len);
    }
  }

  private void uploadPart() throws IOException {
    uploadPart(partSize);
    buffer.clear();
  }

  private void uploadPart(int size) throws IOException {
    if (multiPartUpload == null) {
      log.debug("New multi-part upload for bucket '{}' key '{}'", bucket, key);
      multiPartUpload = newMultipartUpload();
    }

    try {
      multiPartUpload.uploadPart(new ByteArrayInputStream(buffer.array()), size);
    } catch (Exception e) {
      // TODO: elaborate on the exception interpretation. We might be able to retry.
      if (multiPartUpload != null) {
        multiPartUpload.abort();
        log.debug("Multipart upload aborted for bucket '{}' key '{}'.", bucket, key);
      }
      throw new IOException("Part upload failed: ", e.getCause());
    }
  }

  public void commit() throws IOException {
    if (closed) {
      log.warn("Tried to commit data for bucket '{}' key '{}' "
          + "on a closed stream. Ignoring.", bucket, key);
      return;
    }

    try {
      if (buffer.hasRemaining()) {
        uploadPart(buffer.position());
      }
      multiPartUpload.complete();
      log.debug("Upload complete for bucket '{}' key '{}'", bucket, key);
    } catch (Exception e) {
      log.error("Multipart upload failed to complete for bucket"
          + " '{}' key '{}'", bucket, key);
      throw new DataException("Multipart upload failed to complete.", e);
    } finally {
      buffer.clear();
      multiPartUpload = null;
      close();
    }
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }
    closed = true;
    if (multiPartUpload != null) {
      multiPartUpload.abort();
      log.debug("Multipart upload aborted for bucket '{}' key '{}'.", bucket, key);
    }
    super.close();
  }

  private MultipartUpload newMultipartUpload() throws IOException {
    InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(bucket, key);
    try {
      return new MultipartUpload(s3.initiateMultipartUpload(initRequest).getUploadId());
    } catch (AmazonClientException e) {
      // TODO: elaborate on the exception interpretation. If this is an AmazonServiceException,
      // there's more info to be extracted.
      throw new IOException("Unable to initiate MultipartUpload: " + e, e);
    }
  }

  private class MultipartUpload {
    private final String uploadId;
    private final List<PartETag> partETags;

    public MultipartUpload(String uploadId) {
      this.uploadId = uploadId;
      this.partETags = new ArrayList<>();
      log.debug("Initiated multi-part upload for bucket "
          + "'{}' key '{}' with id '{}'", bucket, key, uploadId);
    }

    public void uploadPart(ByteArrayInputStream inputStream, int partSize) {
      int currentPartNumber = partETags.size() + 1;
      UploadPartRequest request = new UploadPartRequest()
          .withBucketName(bucket)
          .withKey(key)
          .withUploadId(uploadId)
          .withInputStream(inputStream)
          .withPartNumber(currentPartNumber)
          .withPartSize(partSize)
          .withGeneralProgressListener(progressListener);
      log.debug("Uploading part {} for id '{}'", currentPartNumber, uploadId);
      partETags.add(s3.uploadPart(request).getPartETag());
    }

    public void complete() {
      log.debug("Completing multi-part upload for key '{}', id '{}'", key, uploadId);
      CompleteMultipartUploadRequest completeRequest =
          new CompleteMultipartUploadRequest(bucket, key, uploadId, partETags);
      s3.completeMultipartUpload(completeRequest);
    }

    public void abort() {
      log.warn("Aborting multi-part upload with id '{}'", uploadId);
      try {
        s3.abortMultipartUpload(new AbortMultipartUploadRequest(bucket, key, uploadId));
      } catch (Exception e) {
        // ignoring failure on abort.
        log.warn("Unable to abort multipart upload, you may need to purge uploaded parts: ", e);
      }
    }
  }

  // Dummy listener for now, just logs the event progress.
  private static class ConnectProgressListener implements ProgressListener {
    public void progressChanged(ProgressEvent progressEvent) {
      log.debug("Progress event: " + progressEvent);
    }
  }

}