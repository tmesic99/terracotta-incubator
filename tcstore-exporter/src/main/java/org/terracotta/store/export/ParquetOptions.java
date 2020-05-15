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

import com.terracottatech.store.Type;
import com.terracottatech.store.definition.CellDefinition;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

public class ParquetOptions {

    private static final Logger LOG = LoggerFactory.getLogger(ParquetOptions.class);

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
    private Type filterCellType = null;
    private Boolean doNotAbortIfFilterCellMissing = false;
    private Integer maxOutputColumns = 800;
    private Boolean maxOutputColumnsNoAbort = false;
    private Boolean maxOutputColumnsUseMultiFile = false;
    private Boolean logStreamPlan = false;
    private CellDefinition<?> filterCell;
    private Set<CellDefinition<?>> whiteListCells = new HashSet<>();
    private Set<CellDefinition<?>> blackListCells = new HashSet<>();
    private Integer maxStringLength = -1;
    private Integer maxByteArraySize = -1;

    public void addWhiteListCell(String name, Type type) {
        try {
            whiteListCells.add(CellDefinition.define(name.trim(), type));
        } catch (Exception ex) {
            LOG.warn("Exception adding Export Allowed cell: " + ex.getMessage());
        }
    }
    public void addBlackListCell(String name, Type type) {
        try {
            blackListCells.add(CellDefinition.define(name.trim(), type));
        } catch (Exception ex) {
            LOG.warn("Exception adding Non-Export Allowed cell: " + ex.getMessage());
        }
    }

    public void setFilterCell(String name, Type type) {
        filterCellName = name;
        filterCellType = type;
        try {
            filterCell = CellDefinition.define(name.trim(), type);
        } catch (Exception ex) {
            filterCell = null;
            LOG.warn("Exception setting range filter cell: " + ex.getMessage());
            LOG.warn("Range Filter cannot be used");
        }
    }
    public Boolean isFilterCell(CellDefinition<?> cell) {
        return filterCell == null ? false : filterCell.equals(cell);
    }

    public CellDefinition<?> getFilterCell() {
        return filterCell;
    }

    public CompressionCodecName getCompression() {
        return compression;
    }

    public void setCompression(CompressionCodecName compression) {
        this.compression = compression;
    }

    public Integer getPageSize() {
        return pageSize;
    }

    public void setPageSize(Integer pageSize) {
        this.pageSize = pageSize;
    }

    public Integer getRowGroupSize() {
        return rowGroupSize;
    }

    public void setRowGroupSize(Integer rowGroupSize) {
        this.rowGroupSize = rowGroupSize;
    }

    public Integer getSchemaSampleSize() {
        return schemaSampleSize;
    }

    public void setSchemaSampleSize(Integer schemaSampleSize) {
        this.schemaSampleSize = schemaSampleSize;
    }

    public Boolean getAppendTypeToSchemaFieldName() {
        return appendTypeToSchemaFieldName;
    }

    public void setAppendTypeToSchemaFieldName(Boolean appendTypeToSchemaFieldName) {
        this.appendTypeToSchemaFieldName = appendTypeToSchemaFieldName;
    }

    public String getFilterCellName() {
        return filterCellName;
    }

    public Type getFilterCellType() {
        return filterCellType;
    }

    public Boolean getDoNotAbortIfFilterCellMissing() {
        return doNotAbortIfFilterCellMissing;
    }

    public void setDoNotAbortIfFilterCellMissing(Boolean doNotAbortIfFilterCellMissing) {
        this.doNotAbortIfFilterCellMissing = doNotAbortIfFilterCellMissing;
    }

    public Integer getMaxOutputColumns() {
        return maxOutputColumns;
    }

    public void setMaxOutputColumns(Integer maxOutputColumns) {
        this.maxOutputColumns = maxOutputColumns;
    }

    public Boolean getMaxOutputColumnsNoAbort() {
        return maxOutputColumnsNoAbort;
    }

    public void setMaxOutputColumnsNoAbort(Boolean maxOutputColumnsNoAbort) {
        this.maxOutputColumnsNoAbort = maxOutputColumnsNoAbort;
    }

    public Boolean getMaxOutputColumnsUseMultiFile() {
        return maxOutputColumnsUseMultiFile;
    }

    public void setMaxOutputColumnsUseMultiFile(Boolean maxOutputColumnsUseMultiFile) {
        this.maxOutputColumnsUseMultiFile = maxOutputColumnsUseMultiFile;
    }

    public Boolean getLogStreamPlan() {
        return logStreamPlan;
    }

    public void setLogStreamPlan(Boolean logStreamPlan) {
        this.logStreamPlan = logStreamPlan;
    }

    public Set<CellDefinition<?>> getWhiteListCells() {
        return whiteListCells;
    }

    public Set<CellDefinition<?>> getBlackListCells() {
        return blackListCells;
    }

    public Integer getMaxStringLength() {
        return maxStringLength;
    }

    public void setMaxStringLength(Integer maxStringLength) {
        this.maxStringLength = maxStringLength;
    }

    public Integer getMaxByteArraySize() {
        return maxByteArraySize;
    }

    public void setMaxByteArraySize(Integer maxByteArraySize) {
        this.maxByteArraySize = maxByteArraySize;
    }
}

