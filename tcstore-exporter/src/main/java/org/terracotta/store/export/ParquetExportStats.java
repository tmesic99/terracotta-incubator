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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Reports information related to the outcome of the export operation.
 *
 */
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
    private List<String> filenames;
    private List<String> filenamePaths;

    /**
     * @return the number of records that were completely written (all of the cells were accounted for in the output
     * schema).
     */
    public long getFullRecordWrites() {
        return fullRecordWrites;
    }

    protected void setFullRecordWrites(long fullRecordWrites) {
        this.fullRecordWrites = fullRecordWrites;
    }

    /**
     * @return the number of records that were partially written (some of the cells were NOT accounted for in the output
     * schema).
     */
    public long getPartialRecordWrites() {
        return partialRecordWrites;
    }

    protected void setPartialRecordWrites(long partialRecordWrites) {
        this.partialRecordWrites = partialRecordWrites;
    }

    /**
     * @return the number of records that were not written to the output file due to error
     */
    public long getFailedRecordWrites() {
        return failedRecordWrites;
    }

    protected void setFailedRecordWrites(long failedRecordWrites) {
        this.failedRecordWrites = failedRecordWrites;
    }

    /**
     * @return the total number of records that were processed (which matched criteria for export)
     */
    public long getRecordsProcessed() {
        return recordsProcessed;
    }

    protected void setRecordsProcessed(long recordsProcessed) {
        this.recordsProcessed = recordsProcessed;
    }

    /**
     * @return the number of times a String value of a cell was truncated to match max string length set in
     * export options
     * @see ParquetOptions#setMaxStringLength(Integer)
     */
    public long getStringsTruncated() {
        return stringsTruncated;
    }

    protected void setStringsTruncated(long stringsTruncated) {
        this.stringsTruncated = stringsTruncated;
    }

    /**
     * @return the number of times a byte array value of a cell was nullified in order to match max byte[] length set in
     * export options
     * @see ParquetOptions#setMaxByteArraySize(Integer)
     */
    public long getByteArraysNullified() {
        return arraysNullified;
    }

    protected void setByteArraysNullified(long arraysNullified) {
        this.arraysNullified = arraysNullified;
    }

    /**
     * @return the number of encountered cells that did not match the output schema (i.e. from the sampled cells).
     */
    public Map<CellDefinition<?>, Integer> getSchemaAbsentCellNoWriteCounts() {
        return Collections.unmodifiableMap(schemaAbsentCellNoWriteCounts);
    }

    protected void addSchemaAbsentCellCounts(CellDefinition<?> cellDef, Integer count) {
        this.schemaAbsentCellNoWriteCounts.put(cellDef, count);
    }

    /**
     * @return whether the export file was successfully created
     */
    public boolean getExportSuccess() {
        return exportSuccess;
    }

    protected void setExportSuccess(boolean exportStatus) {
        this.exportSuccess = exportStatus;
    }

    /**
     * @return the name(s) of the generated parquet file(s).
     */
    public List<String> getFilenames() {
        return filenames;
    }

    public void setFilenames(List<String> filenames) {
        this.filenames = filenames;
    }

    /**
     * @return the full path names of the generated parquet file(s).
     */
    public List<String> getFilenamePaths() {
        return filenamePaths;
    }

    public void setFilenamePaths(List<String> filenamePaths) {
        this.filenamePaths = filenamePaths;
    }
}
