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

package io.confluent.connect.jdbc.sink.dialect;

import com.amazonaws.auth.AWSCredentials;
import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static io.confluent.connect.jdbc.sink.dialect.StringBuilderUtil.joinToBuilder;
import static io.confluent.connect.jdbc.sink.dialect.StringBuilderUtil.nCopiesToBuilder;

public class SnowflakeDialect extends DbDialect {
  public SnowflakeDialect() {
    super("\"", "\"");
  }

  @Override
  protected String escaped(String identifier) {
    return identifier.toUpperCase();
  }

  @Override
  protected String getSqlType(String schemaName, Map<String, String> parameters, Schema.Type type) {
    if (schemaName != null) {
      switch (schemaName) {
        case Decimal.LOGICAL_NAME:
          return "NUMBER(38," + parameters.get(Decimal.SCALE_FIELD) + ")";
        case Date.LOGICAL_NAME:
          return "DATE";
        case Time.LOGICAL_NAME:
          return "TIME";
        case Timestamp.LOGICAL_NAME:
          return "TIMESTAMP_LTZ";
      }
    }
    switch (type) {
      case INT8:
        return "NUMBER(3,0)";
      case INT16:
        return "NUMBER(5,0)";
      case INT32:
        return "NUMBER(10,0)";
      case INT64:
        return "NUMBER(19,0)";
      case FLOAT32:
        return "FLOAT";
      case FLOAT64:
        return "FLOAT";
      case STRING:
        return "VARCHAR";
      case BOOLEAN:
        return "BOOLEAN";
      case BYTES:
        return "BINARY";
      case ARRAY:
        return "ARRAY";
      case MAP:
        return "OBJECT";
      case STRUCT:
        return "VARIANT";
    }
    return super.getSqlType(schemaName, parameters, type);
  }

  @Override
  protected String getCreateSql(boolean isTempTable) {
    return isTempTable ? "CREATE OR REPLACE TEMPORARY TABLE" : "CREATE TABLE IF NOT EXISTS";
  }

  @Override
  public List<String> getAlterTable(String tableName, Collection<SinkRecordField> fields) {
    final List<String> queries = new ArrayList<>(fields.size());
    for (SinkRecordField field : fields) {
      queries.addAll(super.getAlterTable(tableName, Collections.singleton(field)));
    }
    return queries;
  }

  @Override
  public String getStageQuery(final String stageName, final String bucketName, final String pathPrefix, final AWSCredentials credentials) {
    final StringBuilder builder = new StringBuilder();

    builder.append("CREATE OR REPLACE STAGE ").append(stageName);

    builder.append(" URL='s3://").append(bucketName);
    if (pathPrefix != null && !pathPrefix.isEmpty()) {
      builder.append("/").append(pathPrefix);
    }
    builder.append("'");

    builder.append(" CREDENTIALS=(aws_key_id='")
        .append(credentials.getAWSAccessKeyId())
        .append("' aws_secret_key='")
        .append(credentials.getAWSSecretKey())
        .append("')");

    builder.append(" FILE_FORMAT=(TYPE=AVRO)");

    return builder.toString();
  }

  @Override
  public String getMergeQuery(final String table,
                              final String tempTable,
                              final Collection<String> keyColumns,
                              final String versionColumn,
                              final Collection<String> columns) {
    final String tableName = escaped(table);
    final String tempTableName = escaped(tempTable);

    final StringBuilder builder = new StringBuilder();

    builder.append("MERGE INTO ");
    builder.append(tableName);
    builder.append(" target USING (SELECT DISTINCT ");
    joinToBuilder(builder, ",", keyColumns, columns, escaper());
    builder.append(" FROM ");
    builder.append(tempTableName);
    builder.append(") source ON ");
    joinToBuilder(builder, " and ", keyColumns, new StringBuilderUtil.Transform<String>() {
      @Override
      public void apply(StringBuilder builder, String col) {
        builder.append("target.").append(escaped(col));
        builder.append("=");
        builder.append("source.").append(escaped(col));
      }
    });

    if (columns != null && columns.size() > 0) {
      builder.append(" WHEN MATCHED");
      if (versionColumn != null && !versionColumn.isEmpty()) {
        builder.append(" AND ");
        builder.append("source.").append(escaped(versionColumn));
        builder.append(" > ");
        builder.append("target.").append(escaped(versionColumn));
      }
      builder.append(" THEN UPDATE SET ");
      joinToBuilder(builder, ",", columns, new StringBuilderUtil.Transform<String>() {
        @Override
        public void apply(StringBuilder builder, String col) {
          builder.append(escaped(col)).append("=source.").append(escaped(col));
        }
      });
    }

    builder.append(" WHEN NOT MATCHED THEN INSERT (");
    joinToBuilder(builder, ",", keyColumns, columns, escaper());
    builder.append(") VALUES (");
    joinToBuilder(builder, ",", keyColumns, columns, prefixedEscaper("source."));
    builder.append(")");

    return builder.toString();
  }

  @Override
  public String getCopyQuery(final String tableName, final Collection<SinkRecordField> fields, final String stageName, final String fileName) {
    ArrayList<String> columns = new ArrayList<>();
    for (SinkRecordField field : fields) {
      columns.add(field.name());
    }

    final StringBuilder builder = new StringBuilder();

    builder.append("COPY INTO ").append(escaped(tableName)).append(" (");
    joinToBuilder(builder, ",", columns, escaper());
    builder.append(") FROM (SELECT ");
    joinToBuilder(builder, ",", fields, new StringBuilderUtil.Transform<SinkRecordField>() {
      @Override
      public void apply(StringBuilder builder, SinkRecordField col) {
        if (col.schemaName() != null) {
          switch (col.schemaName()) {
            case Time.LOGICAL_NAME:
            case Date.LOGICAL_NAME:
              throw new ConnectException("Unsupported type for column value: " + col.schemaType());
            case Timestamp.LOGICAL_NAME:
              builder.append("to_timestamp(cast($1:")
                  .append(col.name())
                  .append("/1000 as integer),0)");
              return;
            case Decimal.LOGICAL_NAME:
              builder.append("$1:").append(col.name());
              return;
          }
        }
        switch (col.schemaType()) {
          case BOOLEAN:
            builder.append("cast($1:").append(col.name()).append(" as integer)");
            return;
        }
        builder.append("$1:").append(col.name());
      }
    });
    builder.append(" FROM @").append(stageName).append(") FILES=(");
    builder.append("'/").append(fileName).append("')");
    builder.append(" ON_ERROR=SKIP_FILE");

    return builder.toString();
  }

  @Override
  public String getUpsertQuery(final String table, Collection<String> keyCols, Collection<String> cols) {
    final StringBuilder builder = new StringBuilder();
    builder.append("merge into ");
    final String tableName = escaped(table);
    builder.append(tableName);
    builder.append(" using (select ");
    int index = 1;
    for (String keyCol : keyCols) {
      if (index > 1) builder.append(',');
      builder.append('$').append(index).append(" as ").append(escaped(keyCol));
      index++;
    }
    if (cols != null) {
      for (String col : cols) {
        if (keyCols.size() > 0) builder.append(',');
        builder.append('$').append(index).append(" as ").append(escaped(col));
        index++;
      }
    }
    builder.append(" from (values (");
    nCopiesToBuilder(builder, ",", "?", index - 1);
    builder.append("))) as incoming on ");
    joinToBuilder(builder, " and ", keyCols, new StringBuilderUtil.Transform<String>() {
      @Override
      public void apply(StringBuilder builder, String col) {
        builder.append(tableName).append(".").append(escaped(col)).append("=incoming.").append(escaped(col));
      }
    });
    if (cols != null && cols.size() > 0) {
      builder.append(" when matched then update set ");
      joinToBuilder(builder, ",", cols, new StringBuilderUtil.Transform<String>() {
        @Override
        public void apply(StringBuilder builder, String col) {
          builder.append(tableName).append(".").append(escaped(col)).append("=incoming.").append(escaped(col));
        }
      });
    }
    builder.append(" when not matched then insert(");
    joinToBuilder(builder, ",", cols, keyCols, prefixedEscaper(tableName + "."));
    builder.append(") values(");
    joinToBuilder(builder, ",", cols, keyCols, prefixedEscaper("incoming."));
    builder.append(")");
    return builder.toString();
  }
}
