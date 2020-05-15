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

import com.terracottatech.store.Cell;
import com.terracottatech.store.Dataset;
import com.terracottatech.store.Record;
import com.terracottatech.store.Type;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.definition.DoubleCellDefinition;
import com.terracottatech.store.definition.IntCellDefinition;
import com.terracottatech.store.definition.LongCellDefinition;
import com.terracottatech.store.manager.DatasetManager;
import com.terracottatech.store.stream.RecordStream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;

public class DatasetParquetFileExporter
{
    private static final Logger LOG = LoggerFactory.getLogger(DatasetParquetFileExporter.class);

    private ParquetSchema schemas;
    private DatasetManager dsManager;
    private ParquetOptions options;
    private String datasetName;
    private Type datasetType;
    private String outputFileFolder;

    private List<String> crcFileNames;
    private Map<Schema, ParquetWriter<GenericData.Record>> schemaWriterMap;

    public DatasetParquetFileExporter(DatasetManager dsManager,
                                      String datasetName,
                                      Type datasetType,
                                      String outputFileFolder,
                                      ParquetOptions options) throws Exception
    {
        this.dsManager = requireNonNull(dsManager);
        this.options = requireNonNull(options);
        this.datasetName = datasetName;
        this.datasetType = datasetType;
        this.outputFileFolder = outputFileFolder;

        java.nio.file.Path folder = java.nio.file.Paths.get(this.outputFileFolder);
        if (!(Files.exists(folder) && Files.isDirectory((folder)))) {
            String error = "Folder '" + outputFileFolder + "' does not exist or is not a directory.";
            LOG.error(error);
            throw new Exception();
        }
        schemas = new ParquetSchema(dsManager, datasetName, datasetType, options);
    }

    public ParquetExportStats exportDataset()
    {
        LOG.info("Exporting all dataset records");
        return export(null); // no filter
    }

    public ParquetExportStats exportDataset(Predicate<Record<?>> filterPredicate)
    {
        LOG.info("Exporting dataset records using custom filter");
        return export(filterPredicate);
    }

    public ParquetExportStats exportDataset(Double lowRangeValue, Double highRangeValue) {
        // Supported behaviour includes::
        //   If filter cell has not been defined then abort or continue
        //   If filter cell has been defined but has an unsupported type then abort or continue
        //   If filter cell has been defined but does not exist then abort or continue
        //   If filter cell has been defined and exists then conntinue
        //   'Abort' or 'continue' is based upon whether the 'Ignore Abort' option has been set

        ParquetExportStats stats = new ParquetExportStats();
        Boolean useFilterCell = schemas.getUsingFilterCell();
        String filterCellName = options.getFilterCellName();
        Type filterCellType = options.getFilterCellType();
        Predicate<Record<?>> rangeFilter = null;

        if (filterCellName.isEmpty() && filterCellType == null) {
            LOG.warn("No Range Filter has been configured.");
        }
        else if (!useFilterCell) {
            LOG.warn("Filter cell '" + filterCellName + "' of type '" + filterCellType + "' not found in dataset.");
        }
        else {
            // filter cell is loaded in the schema...
            if (filterCellType.equals(Type.LONG)) {
                LongCellDefinition cell = CellDefinition.defineLong(filterCellName);
                rangeFilter = (cell.value().isGreaterThanOrEqualTo(lowRangeValue.longValue()).and(cell.value().isLessThan(highRangeValue.longValue())));
            }
            else if (filterCellType.equals(Type.INT)) {
                IntCellDefinition cell = CellDefinition.defineInt(filterCellName);
                rangeFilter = (cell.value().isGreaterThanOrEqualTo(lowRangeValue.intValue()).and(cell.value().isLessThan(highRangeValue.intValue())));
            }
            else if (filterCellType.equals(Type.DOUBLE)) {
                DoubleCellDefinition cell = CellDefinition.defineDouble(filterCellName);
                rangeFilter = (cell.value().isGreaterThanOrEqualTo(lowRangeValue).and(cell.value().isLessThan(highRangeValue)));
            }
            else {
                LOG.warn("Unsupported cell type '" + filterCellType + "' for Range Filter.");
            }
        }
        if (useFilterCell) {
            LOG.info("Exporting dataset records with '" + filterCellName + "' >= " + lowRangeValue + " and < " + highRangeValue);
            stats = export(rangeFilter);
        }
        else if (options.getDoNotAbortIfFilterCellMissing()) {
            LOG.info("Abort override option has been set.  Exporting entire dataset to file.");
            stats = export(null);
        }
        else {
            LOG.warn("Aborting.  No records written.");
        }
        return stats;
    }

    private ParquetExportStats export(Predicate<Record<?>> filterPredicate)
    {
        ParquetExportStats stats = new ParquetExportStats();

        try (Dataset<?> dataset = dsManager.getDataset(datasetName, datasetType)) {

            // Since we support the ability to split a dataset across multiple parquet files - each with its
            // own defined schema, multiple writers are required to support the number
            // of defined schemas (1:1 --> writer:schema).  The writers get closed at the
            // end of the dataset processing.

            initializeWriters();

            try (RecordStream<?> records = dataset.reader().records()) {

                LOG.info(String.format("Exporting to Parquet: dataset=%s, folder=%s",
                        datasetName, outputFileFolder));

                // For logging progress and final results
                AtomicLong fullRecordWrites = new AtomicLong(0L);
                AtomicLong partialRecordWrites = new AtomicLong(0L);
                AtomicLong failedRecordWrites = new AtomicLong(0L);
                AtomicLong recordsProcessed = new AtomicLong(0L);
                AtomicLong stringsTruncated = new AtomicLong(0L);
                AtomicLong arraysNullified = new AtomicLong(0L);
                Map<CellDefinition<?>, Integer> schemaAbsentCells = new HashMap<>();
                final int statusCheckpoint = 100000;
                final Boolean usingFilter = schemas.getUsingFilterCell();

                Integer maxStringLength = options.getMaxStringLength();
                Integer maxByteArraySize = options.getMaxByteArraySize();

                RecordStream<?> working = records;
                if (filterPredicate != null) {
                    working = working.filter(filterPredicate);
                }
                if (options.getLogStreamPlan()) {
                    working = working.explain(o -> LOG.info(o.toString()));
                }
                working.forEach(r ->
                {
                    // A dataset's record's cells can be split across multiple parquet files by virtue of
                    // those cells having been split across mulitple schemas.
                    // Each schema is dedicated to a specific writer and each writer writes
                    // parquet records associated with those same schema

                    Map<Schema, GenericData.Record> schemaRecordMap = new HashMap<>();
                    Map<ParquetWriter<GenericData.Record>, GenericData.Record> writerRecordMap = new HashMap<>();
                    for (Map.Entry<Schema, ParquetWriter<GenericData.Record>> entry : schemaWriterMap.entrySet())
                    {
                        // Create a new record for every unique schema and associate (map) that record
                        // to the schema and to the dedicated writer created for that schema.
                        Schema schema = entry.getKey();
                        GenericData.Record pqRecord = new GenericData.Record(schema);
                        pqRecord.put(ParquetSchema.REC_KEY, r.getKey());
                        schemaRecordMap.put(schema, pqRecord);
                        writerRecordMap.put(entry.getValue(), pqRecord);
                    }

                    boolean fullRecordWrite = true;
                    boolean fullRecordSuccess = true;
                    boolean filterCellFound = false;

                    for (Cell<?> cell : r) {
                        // For every cell in this dataset record, determine which schemas it
                        // belongs to and then write the cell value to that schema's writer's record.
                        CellDefinition<?> cellDef = cell.definition();
                        // Can only add cells that are defined in a schema
                        String fieldName = schemas.getFieldName(cellDef);
                        if (fieldName != null && !fieldName.isEmpty()) {
                            if (usingFilter && !filterCellFound && options.isFilterCell(cellDef)) {
                                filterCellFound = true;
                                // this is the filter cell and it must be written to each file
                                // which means it belongs in every schema and is therefore
                                // written to every writer's record
                                for (Map.Entry<Schema, GenericData.Record> entry : schemaRecordMap.entrySet())
                                    entry.getValue().put(fieldName, cell.value());
                            }
                            else {
                                // this cell can only belong to a single schema
                                Schema s = schemas.getSchema(cellDef);

                                if (cell.definition().type().equals(Type.STRING))
                                {
                                    String string = (String)cell.value();
                                    if (maxStringLength >= 0 && string.length() > maxStringLength) {
                                        string = string.substring(0, Math.max(0, maxStringLength - 1));
                                        stringsTruncated.incrementAndGet();
                                    }
                                    schemaRecordMap.get(s).put(fieldName, string);
                                }
                                else if (cell.definition().type().equals(Type.BYTES))
                                {
                                    byte[] array = (byte[])cell.value();
                                    if (maxByteArraySize >= 0 && array.length > maxByteArraySize) {
                                        array = null;
                                        arraysNullified.incrementAndGet();
                                    }
                                    schemaRecordMap.get(s).put(fieldName, array);
                                }
                                else
                                    schemaRecordMap.get(s).put(fieldName, cell.value());
                            }
                        }
                        else {
                            // This cell was not found in any one of the schemas.  This is likely due to having
                            // used too small a sample size for schema discovery resulting in this cell not having
                            // been identified in the sampled records.
                            fullRecordWrite = false;
                            schemaAbsentCells.merge(cellDef, 1, Integer::sum);
                        }
                    }
                    // Write this dataset record to the parquet file(s)
                    for (Map.Entry<ParquetWriter<GenericData.Record>, GenericData.Record> entry : writerRecordMap.entrySet()) {
                        try {
                            entry.getKey().write(entry.getValue());
                        }
                        catch (Exception ex) {
                            fullRecordSuccess = false;
                            LOG.error("Exception writing record (key=" + r.getKey().toString() + "): " + ex.getMessage());
                        }
                    }
                    // Results for writing this single record across multiple files
                    if (fullRecordSuccess) {
                        if (fullRecordWrite)
                            fullRecordWrites.incrementAndGet();
                        else
                            partialRecordWrites.incrementAndGet();
                    }
                    else {
                        failedRecordWrites.incrementAndGet();
                    }

                    if(recordsProcessed.incrementAndGet() % statusCheckpoint == 0) {
                        LOG.info(String.format("%,d records processed", recordsProcessed.get()));
                    }
                });

                stats.setExportSuccess(true);
                stats.setRecordsProcessed(recordsProcessed.get());
                stats.setFullRecordWrites(fullRecordWrites.get());
                stats.setPartialRecordWrites(partialRecordWrites.get());
                stats.setFailedRecordWrites(failedRecordWrites.get());
                stats.setStringsTruncated(stringsTruncated.get());
                stats.setArraysNullified(arraysNullified.get());

                LOG.info(String.format("%,d records processed.  Processing complete.", stats.getRecordsProcessed()));
                LOG.info(String.format("%,d complete records written to parquet file", stats.getFullRecordWrites()));
                LOG.info(String.format("%,d partial records written to parquet file", stats.getPartialRecordWrites()));
                LOG.info(String.format("%,d records failed writing to parquet file", stats.getFailedRecordWrites()));
                for (Map.Entry<CellDefinition<?>, Integer> entry : schemaAbsentCells.entrySet()) {
                    String message = String.format("%,d occurrences of cell '%s (%s)' not written to parquet file (cell definition missing from schema).",
                            entry.getValue(), entry.getKey().name(), schemas.getTcType(entry.getKey().type()));
                    LOG.info(message);
                    stats.addSchemaAbsentCellCounts(entry.getKey(), entry.getValue());
                }
                LOG.info(String.format("%,d string values were truncated (max character length = %,d)", stats.getStringsTruncated(), maxStringLength));
                LOG.info(String.format("%,d large-size byte arrays were omitted (max array length = %,d)", stats.getArraysNullified(), maxByteArraySize));
            }
        }
        catch (Exception ex) {
            LOG.error("Exception exporting dataset: " + ex.getMessage());
        }
        finally {
            closeWriters();
            cleanup(); // deletes the autogenerated .crc files
        }
        return stats;
    }

    private void initializeWriters() throws Exception
    {
        // We have to create a writer for each defined schema.  There can be multiple
        // schemas because Dremio has a limit of 800 columns per table which translates to a
        // limit of 800 cells per dataset.  Anything more than that would be written to
        // one or more parallel parquet files (each with its own schema).

        crcFileNames = new ArrayList<>();
        schemaWriterMap = new HashMap<>();

        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss-SSS");
        String dateFragment = dateFormat.format(new Date()); // added to filename to make it unique

        int index = 1;
        int numSchemas = schemas.getSchemas().size();
        for (Schema schema : schemas.getSchemas()) {
            try {
                String fileNumber = ""; // add to filename to make is unique if >1 schemas were created
                if (numSchemas > 1) {
                    fileNumber = Integer.toString(index);
                    index++;
                }
                String fileName = establishOutputFileName(dateFragment, fileNumber);
                ParquetWriter<GenericData.Record> writer = initializeWriter(schema, fileName);

                crcFileNames.add(String.format(".%s.crc", fileName)); // for end of run cleanup
                schemaWriterMap.put(schema, writer); // key map used during dataset processing
            }
            catch (Exception ex) {
                LOG.info("Error encountered initializing Parquet Writers: " + ex.getMessage());
                throw ex;
            }
        }
    }

    private ParquetWriter<GenericData.Record> initializeWriter(Schema schema, String fileName) throws Exception
    {
        ParquetWriter<GenericData.Record> writer;
        try {
            Path parquetFilePath = new Path(this.outputFileFolder, fileName);
            writer = AvroParquetWriter.<GenericData.Record>builder(parquetFilePath)
                    .withWriteMode(org.apache.parquet.hadoop.ParquetFileWriter.Mode.CREATE)
                    .withSchema(schema)
                    .withConf(getConfiguration())
                    .withCompressionCodec(options.getCompression())
                    .withPageSize(options.getPageSize()) //For compression
                    .withRowGroupSize(options.getRowGroupSize()) //For write buffering (Page size)
                    .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0) //PARQUET_2_0
                    .build();
        }
        catch (Exception ex)
        {
            LOG.error("Exception creating Parquet File Writer: " + ex.getMessage());
            throw ex;
        }
        return writer;
    }

    private void closeWriters()
    {
        schemaWriterMap.forEach((k, v) -> {
            try {
                v.close();
            }
            catch (Exception ex) {
                LOG.error("Exception closing the parquet writer: " + ex.getMessage());
            }
        });
    }

    private Configuration getConfiguration()
    {
        Configuration conf = new Configuration();
        return conf;


        // For future dev/integration with Azure possibly?
        /*
        // These settings will upload files directly to S3.  Taken from:
        // https://stackoverflow.com/questions/47355038/how-to-generate-parquet-file-using-pure-java-including-date-decimal-types-an

        conf.set("fs.s3a.access.key", "ACCESSKEY");
        conf.set("fs.s3a.secret.key", "SECRETKEY");
        //Below are some other helpful settings
        //conf.set("fs.s3a.endpoint", "s3.amazonaws.com");
        //conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
        //conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()); // Not needed unless you reference the hadoop-hdfs library.
        //conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName()); // Uncomment if you get "No FileSystem for scheme: file" errors
        Path filePath = new Path("s3a://your-bucket-name/examplefolder/data.parquet");
        */

        //https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html
        //https://hadoop.apache.org/docs/current/hadoop-azure/index.html
        //https://hadoop.apache.org/docs/current/hadoop-azure-datalake/index.html
    }

    private String establishOutputFileName(String dateFragment, String fileNumber)
    {
        if (fileNumber.isEmpty()) {
            return String.format("%s_%s.parquet", this.datasetName, dateFragment);
        }
        else {
            return String.format("%s_%s_%s.parquet", this.datasetName, dateFragment, fileNumber);
        }
    }

    private void cleanup()
    {
        LOG.debug("Cleanup");
        for (String crcFile : crcFileNames)
        {
            try
            {
                File file = new File(this.outputFileFolder, crcFile);
                if(!file.delete())
                    LOG.warn("failed to delete .crc file");
            }
            catch (Exception ex)
            {
                LOG.warn("Exception deleting .crc file: " + ex.getMessage());
            }
        }
    }

    public ParquetSchema getSchemas() {
        return schemas;
    }
}
