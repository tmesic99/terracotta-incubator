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

import com.terracottatech.store.Record;
import com.terracottatech.store.Type;
import com.terracottatech.store.definition.CellDefinition;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Options for the export of Dataset contents to a parquet file.
 *
 * Parquet file generation best practices:
 *     https://docs.dremio.com/advanced-administration/parquet-files.html?h=parquet
 *
 *     Also see https://parquet.apache.org/documentation/latest/
 */
public class ParquetOptions {

    // Parquet file generation best practices:
    //   https://docs.dremio.com/advanced-administration/parquet-files.html?h=parquet
    //   A single row group per file.
    //   A target of 1MB-25MB column stripes for most datasets (ideally).
    //   Note: By default, Dremio uses 256 MB row groups on the Parquet files that it generates.
    //   Implement pages using the following:
    //       Snappy compression
    //       A target of ~100K page size.
    // Also see https://parquet.apache.org/documentation/latest/

    private CompressionCodecName compression = CompressionCodecName.SNAPPY;
    private Integer pageSize = 100 * 1024;      // 100K
    //private Integer pageSize = 4 * 1024 * 1024;  // 4MB
    //private Integer pageSize = ParquetWriter.DEFAULT_PAGE_SIZE;  // 1048576 == 1MB
    private Integer rowGroupSize = 16 * 1024 * 1024;  // 16 MB
    //Integer rowGroupSize = ParquetWriter.DEFAULT_BLOCK_SIZE;  // 134217728 == 128 MB
    //Integer rowGroupSize = 1024 * 1024 * 1024;   // 1073741824 == 1 GB;
    private Integer schemaSampleSize = 5;
    private Boolean appendTypeToSchemaFieldName = false;
    private String filterCellName = "";
    private Type<?> filterCellType = null;
    private Boolean doNotAbortIfFilterCellMissing = false;
    private Integer maxOutputColumns = 800;
    private Boolean maxOutputColumnsNoAbort = false;
    private Boolean maxOutputColumnsUseMultiFile = false;
    private Boolean logStreamPlan = false;
    private CellDefinition<?> filterCell;
    private final Set<CellDefinition<?>> includeCells = new HashSet<>();
    private final Set<CellDefinition<?>> excludeCells = new HashSet<>();
    private Integer maxStringLength = -1;
    private Integer maxByteArraySize = -1;
    private Predicate<Record<?>> schemaSampleFilter;

    /**
     * Add a cell to the "include list" of cells that should be included in the output
     *
     * If there is no 'include list' or 'exclude list' then all cells will be candidates for output
     *
     * @param cellDef the included cell's cell definition
     * @see #addExcludeCellDefinition(CellDefinition)
     */
    public void addIncludeCellDefinition(CellDefinition<?> cellDef) {
        includeCells.add(cellDef);
    }

    /**
     * Add a cell to the 'exclude list' of cells that should be excluded from the output
     *
     * If an 'include list' is used, an 'exclude list' will be ignored.
     *
     * If there is no 'include list' or 'exclude list' then all cells will be candidates for output
     *
     * @param cellDef the excluded cell's cell definition
     * @see #addIncludeCellDefinition(CellDefinition)
     */
    public void addExcludeCellDefinition(CellDefinition<?> cellDef) {
        excludeCells.add(cellDef);
    }

    /**
     * Identify the cell on which to base a filter predicate (in order to output a subset of records)
     * @param cell the cell to predicate the export of records upon
     */
    public void setFilterCell(CellDefinition<?> cell) {
        this.filterCell = cell;
        if (cell != null) {
            filterCellName = cell.name();
            filterCellType = cell.type();
        }
    }

    public Boolean isFilterCell(CellDefinition<?> cell) {
        return filterCell != null && filterCell.equals(cell);
    }

    public CellDefinition<?> getFilterCell() {
        return filterCell;
    }

    public CompressionCodecName getCompression() {
        return compression;
    }

    /**
     * @param compression parquet file output setting
     */
    public void setCompression(CompressionCodecName compression) {
        this.compression = compression;
    }

    public Integer getPageSize() {
        return pageSize;
    }

    /**
     * @param pageSize parquet file output setting
     */
    public void setPageSize(Integer pageSize) {
        this.pageSize = pageSize;
    }

    public Integer getRowGroupSize() {
        return rowGroupSize;
    }

    /**
     * @param rowGroupSize  parquet file output setting
     */
    public void setRowGroupSize(Integer rowGroupSize) {
        this.rowGroupSize = rowGroupSize;
    }

    public Integer getSchemaSampleSize() {
        return schemaSampleSize;
    }

    /**
     * @param schemaSampleSize the number of records to sample to discover the schema for output (the union of all
     *                         cells found on those records will become the assumed schema). Default is 5.
     */
    public void setSchemaSampleSize(Integer schemaSampleSize) {
        this.schemaSampleSize = schemaSampleSize;
    }

    public Boolean getAppendTypeToSchemaFieldName() {
        return appendTypeToSchemaFieldName;
    }

    /**
     * @param appendTypeToSchemaFieldName whether output column names should have the cell's type information appended
     *                                    to their name.  E.g. "foo_INT" rather than just "foo".
     */
    public void setAppendTypeToSchemaFieldName(Boolean appendTypeToSchemaFieldName) {
        this.appendTypeToSchemaFieldName = appendTypeToSchemaFieldName;
    }

    public String getFilterCellName() {
        return filterCellName;
    }

    public Type<?> getFilterCellType() {
        return filterCellType;
    }

    public Boolean getDoNotAbortIfFilterCellMissing() {
        return doNotAbortIfFilterCellMissing;
    }

    /**
     * @param doNotAbortIfFilterCellMissing whether or not the export should fail if the indicated filter predicate
     *                                      cell is missing from records
     */
    public void setDoNotAbortIfFilterCellMissing(Boolean doNotAbortIfFilterCellMissing) {
        this.doNotAbortIfFilterCellMissing = doNotAbortIfFilterCellMissing;
    }

    public Integer getMaxOutputColumns() {
        return maxOutputColumns;
    }

    /**
     * The maximum number of columns to write to a single parquet file.  Default maximum is 800.
     * @param maxOutputColumns
     * @see #setMaxOutputColumnsNoAbort(Boolean)
     * @see #setMaxOutputColumnsUseMultiFile(Boolean)
     */
    public void setMaxOutputColumns(Integer maxOutputColumns) {
        this.maxOutputColumns = maxOutputColumns;
    }

    public Boolean getMaxOutputColumnsNoAbort() {
        return maxOutputColumnsNoAbort;
    }

    /**
     * @param maxOutputColumnsNoAbort whether the export process should fail if too many cells are encountered on
     *                                records
     * @see #setMaxOutputColumns(Integer)
     */
    public void setMaxOutputColumnsNoAbort(Boolean maxOutputColumnsNoAbort) {
        this.maxOutputColumnsNoAbort = maxOutputColumnsNoAbort;
    }

    public Boolean getMaxOutputColumnsUseMultiFile() {
        return maxOutputColumnsUseMultiFile;
    }

    /**
     * If the records contain a large number of cells, you may wish to output records into multiple parquet files, each
     * with a subset of the cells.  Each file will contain the record key and if configured, the filter cell as well.
     * @param maxOutputColumnsUseMultiFile whether large records should be split across multiple files.
     */
    public void setMaxOutputColumnsUseMultiFile(Boolean maxOutputColumnsUseMultiFile) {
        this.maxOutputColumnsUseMultiFile = maxOutputColumnsUseMultiFile;
    }

    public Boolean getLogStreamPlan() {
        return logStreamPlan;
    }

    /**
     * @param logStreamPlan whether the query plan for the stream processing should be output to the log
     */
    public void setLogStreamPlan(Boolean logStreamPlan) {
        this.logStreamPlan = logStreamPlan;
    }

    public Set<CellDefinition<?>> getIncludeCells() {
        return includeCells;
    }

    public Set<CellDefinition<?>> getExcludeCells() {
        return excludeCells;
    }

    public Integer getMaxStringLength() {
        return maxStringLength;
    }

    /**
     * @param maxStringLength the maximum length of String to be output.  Default is -1, meaning no maximum enforced
     */
    public void setMaxStringLength(Integer maxStringLength) {
        this.maxStringLength = maxStringLength;
    }

    public Integer getMaxByteArraySize() {
        return maxByteArraySize;
    }

    /**
     * @param maxByteArraySize the maximum size of byte[]s to be output.  Default is -1, meaning no maximum enforced
     */
    public void setMaxByteArraySize(Integer maxByteArraySize) {
        this.maxByteArraySize = maxByteArraySize;
    }

    public Predicate<Record<?>> getSchemaSampleFilter() {
        return schemaSampleFilter;
    }

    /**
     * @param schemaSampleFilter construct the parquet schema based on cells contained in records that match the supplied predicate
     */
    public void setSchemaSampleFilter(Predicate<Record<?>> schemaSampleFilter) {
        this.schemaSampleFilter = schemaSampleFilter;
    }
}

