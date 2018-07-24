/*
 * Copyright 2016 Confluent Inc.
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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.auth.AWSCredentials;
import io.confluent.connect.jdbc.sink.metadata.FieldsMetadata;
import io.confluent.connect.jdbc.sink.metadata.SchemaPair;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.avro.file.CodecFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import io.confluent.connect.jdbc.sink.dialect.DbDialect;
import io.confluent.connect.avro.AvroData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BulkWriter extends JdbcDbWriter {
  final static Logger log = LoggerFactory.getLogger(BulkWriter.class);

  static final class AvroFileWriter {
    private final String tableName;
    private final JdbcSinkConfig config;
    private final DbDialect dbDialect;
    private final DbStructure dbStructure;
    private final Connection connection;
    private final AvroData avroData;
    private final AmazonS3 s3;
    private final AWSCredentials credentials;
    private final String bucketName;
    private final String pathPrefix;
    private final int partSize;
    private final String versionColumn;
    private final CodecFactory avroCodec;
    private final String stageName;

    private SchemaPair currentSchemaPair;
    private FieldsMetadata fieldsMetadata;

    private S3OutputStream s3OutputStream;
    private String avroFile;

    private Integer kafkaPartition;
    private long kafkaOffset;

    private int recordCount;

    private final DataFileWriter<Object> writer = new DataFileWriter<>(new GenericDatumWriter<>());

    // write records into per-table and per-schema avro files
    //
    // for a given file if the schema changes we need to flush and
    // close the file, then we need to generate the SQL statements that
    // will stage the file and load into the target table:
    //
    // 1. flush and close the file
    // 2. CREATE OR REPLACE TEMPORARY STAGE tempStage
    // 3. PUT file://avroFile @tempStage/avroFile
    // 4. CREATE OR REPLACE TEMPORARY TABLE tempTable
    // 5. COPY INTO tempTable FROM (SELECT fields FROM @tempStage/avroFile)
    // 6. MERGE INTO tableName USING (SELECT DISTINCT columns FROM tempTable)
    //      ON ... WHEN MATCHED ... WHEN NOT MATCHED ...

    AvroFileWriter(JdbcSinkConfig config, String tableName, DbDialect dbDialect, DbStructure dbStructure, Connection connection) {
      this.tableName = tableName;
      this.config = config;
      this.dbDialect = dbDialect;
      this.dbStructure = dbStructure;
      this.connection = connection;
      this.avroData = config.avroData;
      this.avroCodec = CodecFactory.fromString(config.getString(JdbcSinkConfig.AVRO_CODEC));
      this.s3 = config.s3;
      this.credentials = config.credentials;
      this.versionColumn = config.versionField;
      this.bucketName = config.getString(JdbcSinkConfig.S3_BUCKET);
      this.pathPrefix = config.getString(JdbcSinkConfig.S3_PREFIX);
      this.partSize = config.getInt(JdbcSinkConfig.S3_PART_SIZE);
      this.stageName = this.tableName.toUpperCase();
    }

    void add(SinkRecord record) throws SQLException, IOException {
      final SchemaPair schemaPair = new SchemaPair(record.keySchema(), record.valueSchema());

      if (currentSchemaPair != null && !schemaPair.valueSchema.equals(currentSchemaPair.valueSchema)) {
        // flush and close avro data file
        commit();

        // reset
        currentSchemaPair = null;
      }

      if (currentSchemaPair == null) {
        currentSchemaPair = schemaPair;

        // re-initialize everything that depends on the record schema
        fieldsMetadata = FieldsMetadata.extract(tableName, config.pkMode, config.pkFields,
            config.fieldsWhitelist, config.versionField, currentSchemaPair);
        dbStructure.createOrAmendIfNecessary(config, connection, tableName, fieldsMetadata);

        kafkaPartition = record.kafkaPartition();
        kafkaOffset = record.kafkaOffset();

        Statement statement = connection.createStatement();
        String createStageSql = dbDialect.getStageQuery(stageName, bucketName, pathPrefix, credentials);
        log.debug("createStageSql: {}", createStageSql);
        statement.execute(createStageSql);
        statement.close();

        open();
      }

      log.trace("add {}", record);
      Object value = avroData.fromConnectData(record.valueSchema(), record.value());
      writer.append(value);
      if (++recordCount >= config.batchSize) {
        log.trace("writing batch {}", recordCount);
        commit();
        open();
      }
    }

    void commit() throws SQLException, IOException {
      log.trace("commit {} records", recordCount);

      if (recordCount == 0) return;

      // flush and close avro data file
      close();

      // generate and execute put - copy - merge sequence
      Statement statement = connection.createStatement();

      switch (config.insertMode) {
        case MERGE:
          // copy target is a temporary table
          String tempTableName = String.format("temp_%s_%d_%d_", tableName, kafkaPartition, kafkaOffset);
          log.debug("Temp table name {}", tempTableName);

          String createTempSql = dbDialect.getCreateQuery(tempTableName, fieldsMetadata.allFields.values(), true);
          log.debug("{}: createTempSql={}", tempTableName, createTempSql);
          statement.addBatch(createTempSql);

          String copyTempSql = dbDialect.getCopyQuery(tempTableName, fieldsMetadata.allFields.values(), stageName, avroFile);
          log.debug("{}: copyTempSql={}", tempTableName, copyTempSql);
          statement.addBatch(copyTempSql);

          String mergeIntoSql = dbDialect.getMergeQuery(tableName, tempTableName,
              fieldsMetadata.keyFieldNames, versionColumn, fieldsMetadata.nonKeyFieldNames);
          log.debug("{}: mergeIntoSql={}", tempTableName, mergeIntoSql);
          statement.addBatch(mergeIntoSql);

          break;

        case COPY:
          String copySql = dbDialect.getCopyQuery(tableName, fieldsMetadata.allFields.values(), stageName, avroFile);
          log.debug("{}_{}_{}: copyTempSql={}", tableName, kafkaPartition, kafkaOffset, copySql);
          statement.addBatch(copySql);

          break;
      }

      int[] updateCounts = statement.executeBatch();
      log.debug("{}_{}_{}: Got updateCounts={}", tableName, kafkaPartition, kafkaOffset, updateCounts);
      statement.close();
    }

    private void close() throws IOException {
      writer.flush();
      s3OutputStream.commit();
      writer.close();
    }

    private void open() throws IOException {
      avroFile = String.format("%s_%d_%d.avro", tableName, kafkaPartition, kafkaOffset);

      String bucketKey = (pathPrefix != null && !pathPrefix.isEmpty()) ? pathPrefix + "/" + avroFile : avroFile;

      s3OutputStream = new S3OutputStream(bucketName, bucketKey, partSize, s3);

      writer.setCodec(avroCodec);
      writer.create(avroData.fromConnectSchema(currentSchemaPair.valueSchema), s3OutputStream);

      recordCount = 0;
    }
  }

  BulkWriter(final JdbcSinkConfig config, DbDialect dbDialect, DbStructure dbStructure) {
    super(config, dbDialect, dbStructure);
  }

  @Override
  void write(final Collection<SinkRecord> records) throws SQLException {
    final Connection connection = cachedConnectionProvider.getValidConnection();

    final Map<String, AvroFileWriter> bufferByTable = new HashMap<>();

    for (SinkRecord record : records) {
      final String table = destinationTable(record);
      AvroFileWriter writer = bufferByTable.get(table);
      if (writer == null) {
        writer = new AvroFileWriter(config, table, dbDialect, dbStructure, connection);
        bufferByTable.put(table, writer);
      }
      try {
        writer.add(record);
      } catch (IOException e) {
        throw new SQLException(e);
      }
    }

    for (AvroFileWriter writer : bufferByTable.values()) {
      try {
        writer.commit();
      } catch (IOException e) {
        throw new SQLException(e);
      }
    }

    connection.commit();
  }
}
