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

import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;

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
          return "TIMESTAMP";
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
  public List<String> getAlterTable(String tableName, Collection<SinkRecordField> fields) {
    final List<String> queries = new ArrayList<>(fields.size());
    for (SinkRecordField field : fields) {
      queries.addAll(super.getAlterTable(tableName, Collections.singleton(field)));
    }
    return queries;
  }

  @Override
  public String getUpsertQuery(final String table, Collection<String> keyCols, Collection<String> cols) {
    final StringBuilder builder = new StringBuilder();
    builder.append("merge into ");
    final String tableName = escaped(table);
    builder.append(tableName);
    builder.append(" using (select ");
    joinToBuilder(builder, ", ", keyCols, cols, escaper());
    builder.append(" from (values (");
    nCopiesToBuilder(builder, ",", "?", keyCols.size() + (cols == null ? 0 : cols.size()));
    builder.append(")) incoming on (");
    joinToBuilder(builder, " and ", keyCols, new StringBuilderUtil.Transform<String>() {
      @Override
      public void apply(StringBuilder builder, String col) {
        builder.append(tableName).append(".").append(escaped(col)).append("=incoming.").append(escaped(col));
      }
    });
    builder.append(")");
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
