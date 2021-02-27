/*
 * Copyright (c) 2020 Software AG, Darmstadt, Germany and/or its licensors
 *
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.terracotta.store.export;

import com.terracottatech.store.definition.*;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class AvroSchema {

  public static final String REC_KEY = "RecKey";
  private final Map<CellDefinition<?>, String> uniqueFieldNames;
  private final String datasetName;
  private final boolean appendTypeToSchemaFieldName;
  private final String filterCellName;
  private Schema schema;

  public AvroSchema(String datasetName, boolean appendTypeToSchemaFieldName, CellDefinition<?> filterCell) {
    uniqueFieldNames = new HashMap<>();
    this.datasetName = datasetName;
    this.appendTypeToSchemaFieldName = appendTypeToSchemaFieldName;
    this.filterCellName = filterCell == null ? "" : filterCell.name();
  }

  public Schema createSchema(Set<CellDefinition<?>> uniqueCells) {
    uniqueFieldNames.clear();
//    {
//      "type": "record",
//      "name": "HandshakeRequest", "namespace":"org.apache.avro.ipc",
//      "fields": [
//        {"name": "clientHash", "type": {"type": "fixed", "name": "MD5", "size": 16}},
//        {"name": "clientProtocol", "type": ["null", "string"]},
//        {"name": "serverHash", "type": "MD5"},
//        {"name": "meta", "type": ["null", {"type": "map", "values": "bytes"}]}
//      ]
//    }
//    SchemaBuilder
//        .record("HandshakeRequest")
//        .namespace("org.apache.avro.ipc")
//        .fields()
//          .name("clientHash").type().fixed("MD5").size(16).noDefault()
//          .name("clientProtocol").type().nullable().stringType().noDefault()
//          .name("serverHash").type("MD5").noDefault()
//          .name("meta").type().nullable().map().values().bytesType().noDefault()
//        .endRecord();

    SchemaBuilder.FieldAssembler<Schema> fields = SchemaBuilder.record(datasetName).fields();
    for (CellDefinition<?> cell : uniqueCells) {
      String fieldName = generateUniqueFieldName(cell, uniqueCells);
      if (getAvroType(cell) != null && !fieldName.isEmpty()) {
        uniqueFieldNames.put(cell, fieldName); // referenced during file write
        boolean nullable = !fieldName.equals(REC_KEY);
        SchemaBuilder.FieldAssembler<Schema> f = addAvroField(fields, fieldName, cell, nullable);
        if (f != null) {
          fields = f;
        }
      }
    }
    schema = fields.endRecord();
    return schema;
  }

  private static SchemaBuilder.FieldAssembler<Schema> addAvroField(
        SchemaBuilder.FieldAssembler<Schema> fields,
        String name, CellDefinition<?> cell, Boolean nullable) {

    SchemaBuilder.BaseFieldTypeBuilder<Schema> field = fields.name(name).type();
    if (nullable) {
      field = ((SchemaBuilder.FieldTypeBuilder<Schema>)field).nullable();
    }
    switch (cell.type().asEnum()) {
      case CHAR:
      case STRING:
      case LIST: // as json
      case MAP:  // as json
        return field.stringType().noDefault();
      case BOOL:
        return field.booleanType().noDefault();
      case INT:
        return field.intType().noDefault();
      case LONG:
        return field.longType().noDefault();
      case DOUBLE:
        return field.doubleType().noDefault();
      case BYTES:
        return field.bytesType().noDefault();
      default:
        return null;
    }
  }

  public String toString() {
    return schema.toString(true);
  }

  public Map<CellDefinition<?>, String> getUniqueFieldNames () {
    return uniqueFieldNames;
  }

  private String generateUniqueFieldName(CellDefinition<?> cell, Set<CellDefinition<?>> cells) {

    // Returns a unique schema Field name based on the cell definition name and cell type.
    // When same-named cells are found in the set (valid in TC), the field name becomes <name>_<type>.
    // When the cell name is already unique, the field name is simply the cell name.

    String fieldName;
    if (cell.name().equals(REC_KEY) ||
        cell.name().equals(filterCellName) ||
        (!appendTypeToSchemaFieldName && cells.stream().filter(s -> s.name().equals(cell.name())).count() == 1)) {
      fieldName = cell.name();
    } else {
      fieldName = String.format("%s_%s", cell.name(), getTcType(cell) );
    }
    // make name valid for avro schema
    fieldName = fieldName.replace(' ', '_');
    return fieldName;
  }

  public static String getAvroType(CellDefinition<?> cell) {
    switch (cell.type().asEnum()) {
      case BOOL:
        return "boolean";
      case CHAR:
      case STRING:
        return "string";
      case INT:
        return "int";
      case LONG:
        return "long";
      case DOUBLE:
        return "double";
      case BYTES:
        return "bytes";
      case LIST:
        return "array";
      case MAP:
        return "map";
      default:
        return null;
    }
  }

  public static String getTcType(CellDefinition<?> cell)  {
    switch (cell.type().asEnum()) {
      case BOOL:
        return "BOOLEAN";
      case CHAR:
        return "CHAR";
      case INT:
        return "INT";
      case LONG:
        return "LONG";
      case DOUBLE:
        return "DOUBLE";
      case STRING:
        return "STRING";
      case BYTES:
        return "BYTES";
      case LIST:
        return "LIST";
      case MAP:
        return "MAP";
      default:
        return "";
    }
  }
}
