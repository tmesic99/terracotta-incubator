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
import com.terracottatech.store.manager.DatasetManager;
import org.junit.Test;

import java.net.URI;
import java.util.function.Predicate;

public class DatasetParquetFileExporterTest {

    //@Test
    public void CommandLine_ShowHelp_Test()
    {
        String[] args = new String[]{"-h"};
        ExportToParquetCmd.main(args);
    }

    //@Test
    public void CommandLine_Good_Args_Test()
    {
        String[] args = new String[]{
                "-s", "terracotta://localhost:9410",
                "-d", "DS1",
                "-t", "LONG",
                "-o", "C:\\temp",

                "-ss", "10",
                "-a",
                "-fn", "C0001",
                "-ft", "LONG",
                "-flv", "1",
                "-fhv", "10",
                "-ia",

                //"-mc", "4",
                //"-mcia",
                //"-mcmf",

                //"-e", "C0001,LONG, C0002,DOUBLE, C0003,BOOL",
                //"-n", "C0001,LONG, C0003,DOUBLE, C0003,BOOL",
                //"-ms", "0",
                //"-mb", "0",

                //"-p"
        };
        ExportToParquetCmd.main(args);
    }

    //@Test
    public void CommandLine_Bad_Args_Test()
    {
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
                "-e", "C1,LONGzz, C2,DOUBLEzz, C3,BOOLzz",
                "-n", "C41,,,,LONG",
                "-ms", "256xxx",
                "-mb", "14000xxx",
                "xxxx",
                "zzzz",
        };
        ExportToParquetCmd.main(args);
    }

    //@Test
    public void Api_Options_Test()
    {
        String uri = "terracotta://localhost:9410";
        String datasetName = "DS1";
        Type datasetType = Type.LONG;
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

        // Include/Exclude cells and large data (i.e. string/byte arrays)
        //options.setMaxStringLength(256); //default == -1 --> no limit
        //options.setMaxByteArraySize(1024*4); // default == -1 --> no limit

        // Cell Whitelisting - only include these cells in the schema/parquet file
        //options.addWhiteListCellDefinition(CellDefinition.define("C0001", Type.LONG));
        //options.addWhiteListCellDefinition(CellDefinition.define("C0004", Type.BOOL));

        // Cell Blacklisting - exclude cells from the schema/parquet file (if no whitelisting defined)
        //options.addBlackListCellDefinition(CellDefinition.define("C0001",  Type.LONG));
        //options.addBlackListCellDefinition(CellDefinition.define("PdfCell",  Type.BYTES));

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
        catch (Exception ex)
        {
        }
    }

    //@Test
    public void Api_Custom_Filter_Test()
    {
        String uri = "terracotta://localhost:9410";
        String datasetName = "DS1";
        Type datasetType = Type.LONG;
        String outputFileFolder = "C:\\temp";

        ParquetOptions options = new ParquetOptions();

        try (DatasetManager dsManager = DatasetManager.clustered((new URI(uri))).build()) {
            DatasetParquetFileExporter exporter = new DatasetParquetFileExporter(dsManager, datasetName, datasetType, outputFileFolder, options);
            // custom filter example (e.g. only write records with even numbered keys)
            Predicate<Record<?>> filter = (r -> (((Long)r.getKey()) % 2) == 0); //DS1 dataset is type long
            exporter.exportDataset(filter);
        }
        catch (Exception ex)
        {
        }
    }
}
