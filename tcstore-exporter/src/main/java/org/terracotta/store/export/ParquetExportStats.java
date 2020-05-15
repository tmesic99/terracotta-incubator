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

import com.terracottatech.store.definition.CellDefinition;

import java.util.HashMap;
import java.util.Map;

public class ParquetExportStats
{
    private boolean exportSuccess = false;
    private long fullRecordWrites;
    private long partialRecordWrites;
    private long failedRecordWrites;
    private long recordsProcessed;
    private long stringsTruncated;
    private long arraysNullified;
    private Map<CellDefinition<?>, Integer> schemaAbsentCellNoWriteCounts = new HashMap<>();

    public long getFullRecordWrites() {
        return fullRecordWrites;
    }

    public void setFullRecordWrites(long fullRecordWrites) {
        this.fullRecordWrites = fullRecordWrites;
    }

    public long getPartialRecordWrites() {
        return partialRecordWrites;
    }

    public void setPartialRecordWrites(long partialRecordWrites) {
        this.partialRecordWrites = partialRecordWrites;
    }

    public long getFailedRecordWrites() {
        return failedRecordWrites;
    }

    public void setFailedRecordWrites(long failedRecordWrites) {
        this.failedRecordWrites = failedRecordWrites;
    }

    public long getRecordsProcessed() {
        return recordsProcessed;
    }

    public void setRecordsProcessed(long recordsProcessed) {
        this.recordsProcessed = recordsProcessed;
    }

    public long getStringsTruncated() {
        return stringsTruncated;
    }

    public void setStringsTruncated(long stringsTruncated) {
        this.stringsTruncated = stringsTruncated;
    }

    public long getArraysNullified() {
        return arraysNullified;
    }

    public void setArraysNullified(long arraysNullified) {
        this.arraysNullified = arraysNullified;
    }

    public Map<CellDefinition<?>, Integer> getSchemaAbsentCellNoWriteCounts() {
        return schemaAbsentCellNoWriteCounts;
    }

    public void addSchemaAbsentCellCounts(CellDefinition<?> cellDef, Integer count) {
        this.schemaAbsentCellNoWriteCounts.put(cellDef, count);
    }
    public boolean getExportSuccess() {
        return exportSuccess;
    }

    public void setExportSuccess(boolean exportStatus) {
        this.exportSuccess = exportStatus;
    }
}
