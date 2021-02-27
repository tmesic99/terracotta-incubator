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
import com.terracottatech.store.definition.*;
import com.terracottatech.store.manager.DatasetManager;
import org.junit.Test;

import java.net.URI;
import java.util.function.Predicate;

public class DatasetParquetFileExporterTest {

    //@Test
    public void cliShowHelpTest() {
        String[] args = new String[]{"-h"};
        ExportToParquetCmd.main(args);
        /*
            terracotta-tcstore-exporter.jar -s <server> -d <datasetName> -t <datasetType> -o <outputFolder>
                                   [-ss <schemaSampleSize>] [-a]
                                   [-fn <cellName> -ft <cellType> -flv <lowValue> -fhv <highValue>] [-ia]
                                   [-mc <maxColumns>] [-mcia] [-mcmf]
                                   [-ms <maxStringLength>] [-mb <maxByteLength>] [-i <cellname>, <celltype> [, ...]] [-e <cellname>, <celltype> [, ...] [-p]
            Examples:
            java -jar terracotta-tcstore-exporter.jar -s terracotta://localhost:9410 -d DS1 -t LONG -o C:\temp
            java -jar terracotta-tcstore-exporter.jar -s terracotta://localhost:9410 -d DS1 -t LONG -o C:\temp -fn InstantDateKey -ft LONG -flv 0 -fhv 20000000 -p
            java -jar terracotta-tcstore-exporter.jar -s terracotta://localhost:9410 -d DS1 -t LONG -o C:\temp -fn CustomIncrementorKey -ft DOUBLE -flv 0.0 -fhv 1000.0 -ms 256 -e "PdfCell,BYTES" -p

            -s      Server URI connection
            -d      Dataset Name
            -t      Dataset Type [BOOL | CHAR | INT | LONG | DOUBLE | STRING | BYTES]
            -o      Output File Folder where exported file(s) will be written
            [-ss]   Number of dataset records to sample on which to base the Schema (default value is 5 records)
            [-a]    Always append the Cell Type to the Schema Field Name (default is to only append when required to avoid field name clashes)
            [-fn]   Cell Name used as a Range Filter to apply to the queried dataset records
            [-ft]   Cell Type of the Range Filter [INT | LONG | DOUBLE]
            [-flv]  Low Value for the Range Filter
            [-fhv]  High Value for the Range Filter
            [-ia]   Do Not Abort the Export if the Filter Cell is not found
            [-mc]   Maximum Allowed Columns (i.e. unique cell definitions) in the Output File (default value is 800)
            [-mcia] Do Not Abort the export when the number of cells exceed the Maximum Allowed Columns
            [-mcmf] Generate Multiple Output Files when the number of cells exceed the Maximum Allowed Columns (default is to write all cells to a single file)
            [-ms]   For string values, the Maximum String Length in number of characters that will be exported, truncated otherwise (default is no strings are truncated)
            [-mb]   For byte arrays, the Maximum Byte Length in byte count that will be exported, null otherwise (default is all arrays are exported)
            [-i]    Include Cells - a comma-separated list of cell definitions as <cellname, celltype> to be included in the export.  Only these cells will be exported.  All other cell types will be excluded.
            [-e]    Exclude Cells - a comma-separated list of cell definitions as <cellname, celltype> to be exlcuded from the export.  This list is ignored if Include List (-i) is specified
            [-p]    Log the Details of the Stream Plan
         */
    }

    @Test
    public void cliGoodArgsTest() {
        String[] args = new String[]{
                "-s", "terracotta://localhost:9410",
                "-d", "DS1",
                "-t", "LONG",
                "-o", "C:\\users\\tmes\\temp",

                "-ss", "10",
                "-a",
                //"-fn", "C0001",
                //"-ft", "LONG",
                //"-flv", "1",
                //"-fhv", "10",
                //"-ia",

                //"-mc", "5",
                //"-mcia",
                //"-mcmf",

                //"-i", "C0001,LONG, C0002,INT, C0003,BOOL",
                //"-e", "C0001,LONG, C0003,DOUBLE, C0003,BOOL",
                //"-ms", "0",
                //"-mb", "0",

                //"-p"
        };
        ExportToParquetCmd.main(args);
    }

    //@Test
    public void cliBadArgsTest() {
        String[] args = new String[]{
                "-s", "",
                "-d", "",
                "-t", "LONGxxxx",
                "-o", "C:\\tempxxxxx",
                "-ss", "10xxx",
                "-fn", "C5",
                "-ft", "DOUBLExxx",
                "-flv", "-10xxxx",
                "-fhv", "2000000000xxxxx",
                "-mc", "2",
                "-i", "C1,LONGzz, C2,DOUBLEzz, C3,BOOLzz",
                "-e", "C41,,,,LONG",
                "-ms", "256xxx",
                "-mb", "14000xxx",
                "xxxx",
                "zzzz",
        };
        ExportToParquetCmd.main(args);
    }

    //@Test
    public void apiOptionsTest() {
        String uri = "terracotta://localhost:9410";
        String datasetName = "DS1";
        Type<?> datasetType = Type.LONG;
        String outputFileFolder = "C:\\temp";

        ParquetOptions options = new ParquetOptions();

        // Range Filter
        //options.setFilterCell(CellDefinition.define("C0001", Type.LONG));
        //options.setDoNotAbortIfFilterCellMissing(true);
        //Double rangeLowValue = 1.0;
        //Double rangeHighValue = 10000.0;

        // Parquet Schema Discovery
        //options.setSchemaSampleSize(10); //default == 5
        //options.setAppendTypeToSchemaFieldName(true); // default == false --> only append to resolve name clashes

        // Maximum Cells/Columns
        //options.setMaxOutputColumns(900);
        //options.setMaxOutputColumnsNoAbort(true); //default == false --> abort if max columns exist
        //options.setMaxOutputColumnsUseMultiFile(true); // default == false, if 'doNotAbort', use a single file

        // Caps on size of cell data (i.e. Value) (i.e. string/byte arrays)
        //options.setMaxStringLength(256); //default == -1 --> no limit
        //options.setMaxByteArraySize(1024*4); // default == -1 --> no limit

        // Cell Include-listing - only include these cells in the schema/parquet file
        //options.addIncludeCellDefinition(CellDefinition.define("C0001", Type.LONG));
        //options.addIncludeCellDefinition(CellDefinition.define("C0004", Type.BOOL));

        // Cell Exclude-listing - exclude cells from the schema/parquet file (if no whitelisting defined)
        //options.addExcludeCellDefinition(CellDefinition.define("C0001",  Type.LONG));
        //options.addExcludeCellDefinition(CellDefinition.define("PdfCell",  Type.BYTES));

        options.setLogStreamPlan(true);

        try (DatasetManager dsManager = DatasetManager.clustered((new URI(uri))).build()) {
            DatasetParquetFileExporter exporter = new DatasetParquetFileExporter(dsManager, datasetName, datasetType, outputFileFolder, options);

            // No Range Filter
            ParquetExportStats stats = exporter.exportDataset();

            // Range Filter
            //ParquetExportStats stats = exporter.exportDataset(rangeLowValue, rangeHighValue);
            //ParquetExportStats stats = exporter.exportDataset(1.0, 101.0);
            //ParquetExportStats stats = exporter.exportDataset(1.0, 1001.0);
            //ParquetExportStats stats = exporter.exportDataset(1.0, 10001.0);
        }
        catch (Exception ex) {
        }
    }

    //@Test
    public void apiCustomFilterTest() {
        String uri = "terracotta://localhost:9410";
        String datasetName = "DS1";
        Type<?> datasetType = Type.LONG;
        String outputFileFolder = "C:\\temp";

        ParquetOptions options = new ParquetOptions();

        try (DatasetManager dsManager = DatasetManager.clustered((new URI(uri))).build()) {
            DatasetParquetFileExporter exporter = new DatasetParquetFileExporter(dsManager, datasetName, datasetType, outputFileFolder, options);
            // custom filter example (e.g. only write records with even numbered keys)
            Predicate<Record<?>> filter = (r -> (((Long)r.getKey()) % 2) == 0); //DS1 dataset is type long
            exporter.exportDataset(filter);
        }
        catch (Exception ex) {
        }
    }

    //@Test
    public void apiSchemaFilterTest() {
        String uri = "terracotta://localhost:9410";
        String datasetName = "DS1";
        Type<?> datasetType = Type.LONG;
        String outputFileFolder = "C:\\temp";

        ParquetOptions options = new ParquetOptions();

        // Schema-Sampling Filter Examples

        //BoolCellDefinition cell = CellDefinition.defineBool("C0010");
        //options.setSchemaSampleFilter(cell.value().is(true));

        //LongCellDefinition cell = CellDefinition.defineLong("C0001");
        //options.setSchemaSampleFilter(cell.value().isGreaterThanOrEqualTo(400L));

        LongCellDefinition c1 = CellDefinition.defineLong("C0001");
        BoolCellDefinition c2 = CellDefinition.defineBool("C0020");
        StringCellDefinition c3 = CellDefinition.defineString("C0015");
        options.setSchemaSampleFilter(c1.exists().and(c2.exists().and(c3.exists())));

        try (DatasetManager dsManager = DatasetManager.clustered((new URI(uri))).build()) {
            DatasetParquetFileExporter exporter = new DatasetParquetFileExporter(dsManager, datasetName, datasetType, outputFileFolder, options);
            exporter.exportDataset();
        }
        catch (Exception ex) {
        }
    }
}
