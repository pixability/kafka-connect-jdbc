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

import io.confluent.connect.jdbc.sink.metadata.FieldsMetadata;
import io.confluent.connect.jdbc.sink.metadata.SchemaPair;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.avro.file.CodecFactory;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import io.confluent.connect.jdbc.sink.dialect.DbDialect;
import io.confluent.connect.avro.AvroData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BulkWriter extends JdbcDbWriter {
  final static Logger log = LoggerFactory.getLogger(BulkWriter.class);

  static AvroData avroData;

  static final class AvroFileWriter {
    private final String tableName;
    private final JdbcSinkConfig config;
    private final DbDialect dbDialect;
    private final DbStructure dbStructure;
    private final Connection connection;

    private SchemaPair currentSchemaPair;
    private FieldsMetadata fieldsMetadata;

    private GZIPOutputStream gzipOutputStream;
    private FileOutputStream fileOutputStream;
    private Path avroFile;

    Integer kafkaPartition;
    long kafkaOffset;

    int recordCount;

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

      if (avroData == null) {
        avroData = new AvroData(config.getInt(JdbcSinkConfig.SCHEMA_CACHE_SIZE));
      }
    }

    void add(SinkRecord record) throws SQLException, IOException {
      final SchemaPair schemaPair = new SchemaPair(record.keySchema(), record.valueSchema());

      if (currentSchemaPair != null && !schemaPair.equals(currentSchemaPair)) {
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

        open();
      }

      log.trace("add {}", record);
      Object value = avroData.fromConnectData(record.valueSchema(), record.value());
      writer.append(value);
      if (++recordCount >= config.batchSize) {
        commit();
        open();
      }
    }

    void commit() throws SQLException, IOException {
      if (recordCount == 0) return;

      // flush and close avro data file
      close();

      // generate and execute put - copy - merge sequence
      Statement statement = connection.createStatement();

      // copy target is a temporary table
      String tempTableName = String.format("t_%s_%d_%d", tableName, kafkaPartition, kafkaOffset);
      log.debug("Temp table name {}", tempTableName);
      String createTempSql = dbDialect.getCreateQuery(tempTableName, fieldsMetadata.allFields.values(), true);
      log.debug("createTempSql: {}", createTempSql);
      statement.addBatch(createTempSql);

      String putFileSql = dbDialect.getPutQuery(tempTableName, avroFile.toFile());
      log.debug("putFileSql: {}", putFileSql);
      statement.addBatch(putFileSql);

      String copyTempSql = dbDialect.getCopyQuery(tempTableName, fieldsMetadata.allFields.values(), avroFile.toFile());
      log.debug("copyTempSql: {}", copyTempSql);
      statement.addBatch(copyTempSql);

      String mergeIntoSql = dbDialect.getMergeQuery(tableName, tempTableName, fieldsMetadata.keyFieldNames, "version",
          fieldsMetadata.nonKeyFieldNames);
      log.debug("mergeIntoSql: {}", mergeIntoSql);
      statement.addBatch(mergeIntoSql);

      int[] updateCounts = statement.executeBatch();
      log.debug("Got updateCounts={}", updateCounts);
    }

    private void close() throws IOException {
      writer.flush();
      gzipOutputStream.close();
      writer.close();
    }

    private void open() throws IOException {
      avroFile = Files.createTempFile(tableName, ".avro.gz");

      fileOutputStream = new FileOutputStream(avroFile.toFile());
      gzipOutputStream = new GZIPOutputStream(fileOutputStream);

      writer.setCodec(CodecFactory.fromString(config.getString(JdbcSinkConfig.AVRO_CODEC)));
      writer.create(avroData.fromConnectSchema(currentSchemaPair.valueSchema), gzipOutputStream);

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
      final String table = destinationTable(record.topic());
      AvroFileWriter buffer = bufferByTable.get(table);
      if (buffer == null) {
        buffer = new AvroFileWriter(config, table, dbDialect, dbStructure, connection);
        bufferByTable.put(table, buffer);
      }
      try {
        buffer.add(record);
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
