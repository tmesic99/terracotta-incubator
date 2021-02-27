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
import org.junit.Test;

import java.util.LinkedHashSet;
import java.util.Set;

import static com.terracottatech.store.Type.LONG;
import static org.terracotta.store.export.AvroSchema.REC_KEY;

public class AvroSchemaTest {

  @Test
  public void createAvroSchemaTest() {

    Set<CellDefinition<?>> cells = new LinkedHashSet<>(); //preserves insertion ordering
    cells.add(CellDefinition.define(REC_KEY, LONG));
    cells.add(CellDefinition.defineDouble("double"));
    cells.add(CellDefinition.defineInt("int"));
    cells.add(CellDefinition.defineLong("long"));
    cells.add(CellDefinition.defineChar("char"));
    cells.add(CellDefinition.defineBool("bool"));
    cells.add(CellDefinition.defineBytes("bytes"));
    cells.add(CellDefinition.defineString("same_name"));
    cells.add(CellDefinition.defineDouble("same_name"));

    cells.add(CellDefinition.defineList("list"));
    cells.add(CellDefinition.defineMap("map"));

    String datasetName = "myDataset";
    boolean appendTypeToSchemaFieldName = false;
    CellDefinition<?> filterCell = null;//CellDefinition.defineInt("int");

    AvroSchema avroSchema = new AvroSchema(datasetName, appendTypeToSchemaFieldName, filterCell);
    Schema schema = avroSchema.createSchema(cells);
    System.out.println(avroSchema.toString());
  }
}
