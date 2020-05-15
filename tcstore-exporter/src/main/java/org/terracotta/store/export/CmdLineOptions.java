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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CmdLineOptions
{
    private final static Logger LOG = LoggerFactory.getLogger(CmdLineOptions.class);

    private final static String TYPE_BOOL = "BOOL";
    private final static String TYPE_CHAR = "CHAR";
    private final static String TYPE_INT = "INT";
    private final static String TYPE_LONG = "LONG";
    private final static String TYPE_DOUBLE = "DOUBLE";
    private final static String TYPE_STRING = "STRING";
    private final static String TYPE_BYTES = "BYTES";

    private final static String HELP = "-h";
    private final static String SERVER_URI = "-s";
    private final static String SERVER_URI_DESC = "Server URI connection";
    private final static String DATASET_NAME = "-d";
    private final static String DATASET_NAME_DESC = "Dataset Name";
    private final static String DATASET_TYPE = "-t";
    private final static String DATASET_TYPE_DESC = "Dataset Type [BOOL | CHAR | INT | LONG | DOUBLE | STRING | BYTES]";
    private final static String OUTPUT_FOLDER = "-o";
    private final static String OUTPUT_FOLDER_DESC = "Output File Folder where exported file(s) will be written";

    private final static String DISCOVER_SCHEMA_SAMPLE_SIZE = "-ss";
    private final static String DISCOVER_SCHEMA_SAMPLE_SIZE_DESC = "Number of dataset records to sample on which to base the Schema (default value is 5 records)";
    private final static String APPEND_CELL_TYPE_TO_SCHEMA_FIELD_NAME = "-a";
    private final static String APPEND_CELL_TYPE_TO_SCHEMA_FIELD_NAME_DESC = "Always append the Cell Type to the Schema Field Name (default is to only append when required to avoid field name clashes)";

    private final static String FILTER_CELL_NAME = "-fn";
    private final static String FILTER_CELL_NAME_DESC = "Cell Name used as a Range Filter to apply to the queried dataset records";
    private final static String FILTER_CELL_TYPE = "-ft";
    private final static String FILTER_CELL_TYPE_DESC = "Cell Type of the Range Filter [INT | LONG | DOUBLE]";
    private final static String LO_FILTER_VALUE = "-flv";
    private final static String LO_FILTER_VALUE_DESC = "Low Value for the Range Filter";
    private final static String HI_FILTER_VALUE = "-fhv";
    private final static String HI_FILTER_VALUE_DESC = "High Value for the Range Filter ";
    private final static String IGNORE_ABORT_IF_FILTER_CELL_MISSING = "-ia";
    private final static String IGNORE_ABORT_IF_FILTER_CELL_MISSING_DESC = "Do Not Abort the Export if the Filter Cell is not found";

    private final static String MAX_COLUMNS = "-mc";
    private final static String MAX_COLUMNS_DESC = "Maximum Allowed Columns (i.e. unique cell definitions) in the Output File (default value is 800)";
    private final static String MAX_COLUMNS_NO_ABORT = "-mcia";
    private final static String MAX_COLUMNS_NO_ABORT_DESC = "Do Not Abort the export when the number of cells exceed the Maximum Allowed Columns";
    private final static String MAX_COLUMNS_NO_ABORT_MULTIFILE = "-mcmf";
    private final static String MAX_COLUMNS_NO_ABORT_MULTIFILE_DESC = "Generate Multiple Output Files when the number of cells exceed the Maximum Allowed Columns (default is to write all cells to a single file)";

    private final static String LOG_STREAM_PLAN = "-p";
    private final static String LOG_STREAM_PLAN_DESC = "Log the Details of the Stream Plan";

    private final static String EXPORTABLE_CELLS = "-e";
    private final static String EXPORTABLE_CELLS_DESC = "Exportable Cells - a comma-separated list of cell definitions as <cellname, celltype> to be exported";
    private final static String NONEXPORTABLE_CELLS = "-n";
    private final static String NONEXPORTABLE_CELLS_DESC = "Non-Exportable Cells - a comma-separated list of cell definitions as <cellname, celltype> to be blocked from export";
    private final static String MAX_STRING_LENGTH = "-ms";
    private final static String MAX_STRING_LENGTH_DESC = "For string values, the Maximum String Length in number of characters that will be exported, truncated otherwise (default is no strings are truncated)";
    private final static String MAX_BYTE_COUNT = "-mb";
    private final static String MAX_BYTE_COUNT_DESC = "For byte arrays, the Maximum Byte Length in byte count that will be exported, null otherwise (default is all arrays are exported)";

    private Boolean showHelp = false;
    private static StringBuilder validationErrors;

    //mandatory params
    private String serverUri = "";
    private String datasetName = "";
    private String datasetType = "";
    private String output = "";
    //optional params
    private Integer schemaSampleSize = 5;
    private Boolean appendTypeToSchemaFieldName = false;
    private String filterCellName = "";
    private String filterCellType = "";
    private Double filterLowValue = null;
    private Double filterHighValue = null;
    private Boolean useFilterRange = false;
    private Boolean doNotAbortIfFilterCellMissing = false;
    private Integer maxOutputColumns = 800;
    private Boolean maxOutputColumnsNoAbort = false;
    private Boolean maxOutputColumnsUseMultiFile = false;
    private Boolean logStreamPlan = false;
    private List<String> whiteListCellsNamesTypes = new ArrayList<>();
    private List<String> blackListCellsNamesTypes = new ArrayList<>();
    private Integer maxStringLength = -1;
    private Integer maxByteArraySize = -1;


    public CmdLineOptions(String[] args)
    {
        try {
            validationErrors = new StringBuilder();
            String format = "%s --> %s";

            int count = args.length;
            if (count == 1 && args[0].equals(HELP)) {
                LOG.info(args[0]);
                showHelp = true;
                return;
            }

            LOG.info("Processing command line arguments:");
            for (int i = 0; i < count; i++) {
                String arg = args[i];
                switch (arg) {
                    case SERVER_URI:
                        if (++i < count)
                            serverUri = args[i];
                        LOG.info(String.format(format, arg, serverUri));
                        break;

                    case DATASET_NAME:
                        if (++i < count)
                            datasetName = args[i];
                        LOG.info(String.format(format, arg, datasetName));
                        break;

                    case DATASET_TYPE:
                        if (++i < count)
                            datasetType = args[i];
                        LOG.info(String.format(format, arg, datasetType));
                        break;

                    case OUTPUT_FOLDER:
                        if (++i < count)
                            output = args[i];
                        LOG.info(String.format(format, arg, output));
                        break;

                    case DISCOVER_SCHEMA_SAMPLE_SIZE: {
                        String value = "";
                        if (++i < count) {
                            value = args[i];
                            try {
                                schemaSampleSize = Integer.parseInt(args[i]);
                            } catch (Exception ex) {
                                validationErrors.append("Bad format for Schema Sample Size (" + DISCOVER_SCHEMA_SAMPLE_SIZE + ")\n");
                            }
                        }
                        LOG.info(String.format(format, arg, value));
                        break;
                    }
                    case APPEND_CELL_TYPE_TO_SCHEMA_FIELD_NAME:
                        appendTypeToSchemaFieldName = true;
                        LOG.info(arg);
                        break;

                    case FILTER_CELL_NAME:
                        if (++i < count)
                            filterCellName = args[i];
                        LOG.info(String.format(format, arg, filterCellName));
                        break;

                    case FILTER_CELL_TYPE:
                        if (++i < count)
                            filterCellType = args[i];
                        LOG.info(String.format(format, arg, filterCellType));
                        break;

                    case LO_FILTER_VALUE: {
                        String value = "";
                        if (++i < count) {
                            value = args[i];
                            try {
                                filterLowValue = Double.parseDouble((args[i]));
                            } catch (Exception ex) {
                                validationErrors.append("Bad format for Low Filter Value (" + LO_FILTER_VALUE + ")\n");
                            }
                        }
                        LOG.info(String.format(format, arg, value));
                        break;
                    }
                    case HI_FILTER_VALUE: {
                        String value = "";
                        if (++i < count) {
                            value = args[i];
                            try {
                                filterHighValue = Double.parseDouble(args[i]);
                            } catch (Exception ex) {
                                validationErrors.append("Bad format for High Filter Value (" + HI_FILTER_VALUE + ")\n");
                            }
                        }
                        LOG.info(String.format(format, arg, value));
                        break;
                    }
                    case IGNORE_ABORT_IF_FILTER_CELL_MISSING:
                        doNotAbortIfFilterCellMissing = true;
                        LOG.info(arg);
                        break;

                    case MAX_COLUMNS: {
                        String value = "";
                        if (++i < count) {
                            value = args[i];
                            try {
                                maxOutputColumns = Integer.parseInt(args[i]);
                            } catch (Exception ex) {
                                validationErrors.append("Bad format for Max Columns (" + MAX_COLUMNS + ")\n");
                            }
                        }
                        LOG.info(String.format(format, arg, value));
                        break;
                    }
                    case MAX_COLUMNS_NO_ABORT:
                        maxOutputColumnsNoAbort = true;
                        LOG.info(arg);
                        break;

                    case MAX_COLUMNS_NO_ABORT_MULTIFILE:
                        maxOutputColumnsUseMultiFile = true;
                        LOG.info(arg);
                        break;

                    case LOG_STREAM_PLAN:
                        logStreamPlan = true;
                        LOG.info(arg);
                        break;

                    case MAX_STRING_LENGTH: {
                        String value = "";
                        if (++i < count) {
                            value = args[i];
                            try {
                                maxStringLength = Integer.parseInt(args[i]);
                            } catch (Exception ex) {
                                validationErrors.append("Bad format for Maximum String Length (" + MAX_STRING_LENGTH + ")\n");
                            }
                        }
                        LOG.info(String.format(format, arg, value));
                        break;
                    }
                    case MAX_BYTE_COUNT: {
                        String value = "";
                        if (++i < count) {
                            value = args[i];
                            try {
                                maxByteArraySize = Integer.parseInt(args[i]);
                            } catch (Exception ex) {
                                validationErrors.append("Bad format for Maximum Byte Count (" + MAX_BYTE_COUNT + ")\n");
                            }
                        }
                        LOG.info(String.format(format, arg, value));
                        break;
                    }
                    case EXPORTABLE_CELLS: {
                        String value = "";
                        if (++i < count) {
                            value = args[i];
                            try {
                                whiteListCellsNamesTypes = new ArrayList<>(Arrays.asList(value.split(",")));
                            } catch (Exception ex) {
                                validationErrors.append("Bad format for Exportable Cells list (" + EXPORTABLE_CELLS + ")\n");
                            }
                        }
                        LOG.info(String.format(format, arg, value));
                        break;
                    }
                    case NONEXPORTABLE_CELLS: {
                        String value = "";
                        if (++i < count) {
                            value = args[i];
                            try {
                                blackListCellsNamesTypes = new ArrayList<>(Arrays.asList(value.split(",")));
                            } catch (Exception ex) {
                                validationErrors.append("Bad format for Non-Exportable Cells list (" + NONEXPORTABLE_CELLS + ")\n");
                            }
                        }
                        LOG.info(String.format(format, arg, value));
                        break;
                    }
                    default:
                        LOG.warn("UNRECOGNIZED option: " + arg);
                        break;
                }
            }
        }
        catch (Exception ex)
        {
            validationErrors.append(ex.getMessage());
            LOG.error("Exception parsing command line arguments: " + ex.getMessage());
        }
    }

    public String Validate()
    {
        //  Parameter Validation is performed here including value types, consistency, etc.
        //  'Advanced' validation (e.g Terrcaotta license checking) is not performed.

        if (showHelp)
            return "";

        if (serverUri.isEmpty())
            validationErrors.append("Server URI (" + SERVER_URI + ") value not specified\n");
        if (datasetName.isEmpty())
            validationErrors.append("Dataset Name (" + DATASET_NAME + ") value not specified\n");
        if (getType(datasetType) == null)
            validationErrors.append("Dataset Type (" + DATASET_TYPE + ") value not specified or not supported\n");
        if (output.isEmpty()) {
            validationErrors.append("Output Folder (" + OUTPUT_FOLDER + ") value not specified\n");
        }
        else {
            java.nio.file.Path folder = java.nio.file.Paths.get(output);
            if (!(Files.exists(folder) && Files.isDirectory((folder)))) {
                validationErrors.append("Output Folder '" + output + "' does not exist or is not a directory\n");
            }
        }
        if (schemaSampleSize < 1)
            validationErrors.append("Sample Size (" + DISCOVER_SCHEMA_SAMPLE_SIZE + ") must be > 0\n");

        // For the optional range filter, all four of the filter-related options must be specified for a valid configuration
        if (filterCellName.isEmpty() &&
            filterCellType.isEmpty() &&
            filterLowValue == null &&
            filterHighValue == null)
            useFilterRange = false;
        else {
            // At least one of the four filter-related properties was set.
            // Therefore, check that all properties are of good form.
            useFilterRange = true;

            if (filterCellName.isEmpty())
                validationErrors.append("Filter Cell Name (" + FILTER_CELL_NAME + ") value not specified\n");

            Type type = getType(filterCellType);
            if (type == null)
                validationErrors.append("Filter Cell Type (" + FILTER_CELL_TYPE + ") value not specified or is invalid\n");
            else {
                if (type != Type.DOUBLE && type != Type.LONG && type != Type.INT)
                    validationErrors.append("Unsupported Filter Cell Type (" + FILTER_CELL_TYPE + ") specified\n");
            }
            if (filterLowValue == null)
                validationErrors.append("Low Filter Value (" + LO_FILTER_VALUE + ") not specified or invalid\n");
            if (filterHighValue == null)
                validationErrors.append("High Filter Value (" + HI_FILTER_VALUE + ") not specified or invalid\n");
            if (filterLowValue != null &&
                filterHighValue != null &&
                filterHighValue <= filterLowValue)
                validationErrors.append("High Filter Value (" + HI_FILTER_VALUE + ") must be greater than the Low Filter Value (" + LO_FILTER_VALUE + ")\n");
        }
        if (whiteListCellsNamesTypes.size() > 0)
        {
            Integer count = whiteListCellsNamesTypes.size();
            if (count %2 > 0)
                validationErrors.append("Incorrect number of entries in the Exportable Cell List (" + EXPORTABLE_CELLS + ").  Cells must be specified as comma-separated <name>,<type> pairs.\n");
            for (int i = 0; i < count; i++) {
                String cellName = whiteListCellsNamesTypes.get(i).trim();
                if (cellName.isEmpty())
                    validationErrors.append("Blank cell name found in Exportable Cell List (" + EXPORTABLE_CELLS + ")\n");
                if (++i < count) {
                    String cellType = whiteListCellsNamesTypes.get(i).trim();
                    Type type = getType(cellType);
                    if (type == null)
                        validationErrors.append("Exportable Cell Type (" + EXPORTABLE_CELLS + ") '" + cellType + "' not specified or is invalid\n");
                }
            }
        }
        if (blackListCellsNamesTypes.size() > 0)
        {
            if (whiteListCellsNamesTypes != null)
                LOG.warn("Non-Exportable Cell list will be ignored because Exportable Cell list was specifed");
            int count = blackListCellsNamesTypes.size();
            if (count %2 > 0)
                validationErrors.append("Incorrect number of entries in the Non-Exportable Cell List (" + NONEXPORTABLE_CELLS + ").  Cells must be specified as comma-separated <name>,<type> pairs.\n");
            for (int i = 0; i < count; i++) {
                String cellName = blackListCellsNamesTypes.get(i).trim();
                if (cellName.isEmpty())
                    validationErrors.append("Blank cell name found in Non-Exportable Cell List (" + EXPORTABLE_CELLS + ")\n");
                if (++i < count) {
                    String cellType = blackListCellsNamesTypes.get(i).trim();
                    Type type = getType(cellType);
                    if (type == null)
                        validationErrors.append("Non-Exportable Cell Type (" + NONEXPORTABLE_CELLS + ") '" + cellType + "' not specified or is invalid\n");
                }
            }
        }
        if (maxOutputColumns <= 2)
            validationErrors.append("Maximum Output columns (" + MAX_COLUMNS + ") must be > 2\n");

        return validationErrors.toString();
    }


    public String getHelp()
    {

        return "\n" +
                "Export a dataset to a parquet file.  Use the cells present in the dataset's records\n" +
                "to deduce the schema (i.e. field names and types) required for the parquet file.\n\n" +
                "terracotta-parquet.jar -s server -d datasetName -t datasetType -o outputFolder\n" +
                "                       [-ss schemaSampleSize] [-a]\n" +
                "                       [-fn cellName -ft cellType -flv lowValue -fhv highValue] [-ia]\n" +
                "                       [-mc maxColumns] [-mcia] [-mcmf]\n" +
                "                       [-ms maxStringLength] [-mb maxByteLength] [-e <cellname, celltype>,...] [--n <cellname, celltype>,...] [-p]\n\n" +
                "Examples:\n" +
                "java - jar terracotta-parquet.jar -s terracotta://localhost:9410 -d DS1 -t LONG -o C:\\temp\n" +
                "java - jar terracotta-parquet.jar -s terracotta://localhost:9410 -d DS1 -t LONG -o C:\\temp -fn InstantDateKey -ft LONG -flv 0 -fhv 20000000 -p\n" +
                "java - jar terracotta-parquet.jar -s terracotta://localhost:9410 -d DS1 -t LONG -o C:\\temp -fn CustomIncrementorKey -ft DOUBLE -flv 0.0 -fhv 1000.0 -ms 256 -n \"PdfCell,BYTES\" -p\n\n" +
                helpEntry(SERVER_URI, SERVER_URI_DESC, true) +
                helpEntry(DATASET_NAME, DATASET_NAME_DESC, true) +
                helpEntry(DATASET_TYPE, DATASET_TYPE_DESC, true) +
                helpEntry(OUTPUT_FOLDER, OUTPUT_FOLDER_DESC, true) +
                helpEntry(DISCOVER_SCHEMA_SAMPLE_SIZE, DISCOVER_SCHEMA_SAMPLE_SIZE_DESC, false) +
                helpEntry(APPEND_CELL_TYPE_TO_SCHEMA_FIELD_NAME, APPEND_CELL_TYPE_TO_SCHEMA_FIELD_NAME_DESC, false) +
                helpEntry(FILTER_CELL_NAME, FILTER_CELL_NAME_DESC, false) +
                helpEntry(FILTER_CELL_TYPE, FILTER_CELL_TYPE_DESC, false) +
                helpEntry(LO_FILTER_VALUE, LO_FILTER_VALUE_DESC, false) +
                helpEntry(HI_FILTER_VALUE, HI_FILTER_VALUE_DESC, false) +
                helpEntry(IGNORE_ABORT_IF_FILTER_CELL_MISSING, IGNORE_ABORT_IF_FILTER_CELL_MISSING_DESC, false) +
                helpEntry(MAX_COLUMNS, MAX_COLUMNS_DESC, false) +
                helpEntry(MAX_COLUMNS_NO_ABORT, MAX_COLUMNS_NO_ABORT_DESC, false) +
                helpEntry(MAX_COLUMNS_NO_ABORT_MULTIFILE, MAX_COLUMNS_NO_ABORT_MULTIFILE_DESC, false) +
                helpEntry(MAX_STRING_LENGTH, MAX_STRING_LENGTH_DESC, false) +
                helpEntry(MAX_BYTE_COUNT, MAX_BYTE_COUNT_DESC, false) +
                helpEntry(EXPORTABLE_CELLS, EXPORTABLE_CELLS_DESC, false) +
                helpEntry(NONEXPORTABLE_CELLS, NONEXPORTABLE_CELLS_DESC, false) +
                helpEntry(LOG_STREAM_PLAN, LOG_STREAM_PLAN_DESC, false);
    }

    String helpEntry (String option, String name, Boolean mandatory)
    {
        if (!mandatory)
            option = "[" + option + "]";
        return String.format("%-8s%s\n", option, name);
    }

    public Type getType(String type)
    {
        switch (type) {
            case TYPE_BOOL:
                return Type.BOOL;
            case TYPE_CHAR:
                return Type.CHAR;
            case TYPE_INT:
                return Type.INT;
            case TYPE_LONG:
                return Type.LONG;
            case TYPE_DOUBLE:
                return Type.DOUBLE;
            case TYPE_STRING:
                return Type.STRING;
            case TYPE_BYTES:
                return Type.BYTES;
            default:
                return null;
        }
    }

    public Boolean getShowHelp() {
        return showHelp;
    }

    public String getServerUri() {
        return serverUri;
    }

    public String getDatasetName() {
        return datasetName;
    }

    public String getDatasetType() {
        return datasetType;
    }

    public String getOutput() {
        return output;
    }

    public Integer getSchemaSampleSize() {
        return schemaSampleSize;
    }

    public Boolean getAppendTypeToSchemaFieldName() {
        return appendTypeToSchemaFieldName;
    }

    public String getFilterCellName() {
        return filterCellName;
    }

    public String getFilterCellType() {
        return filterCellType;
    }

    public Double getFilterLowValue() {
        return filterLowValue;
    }

    public Double getFilterHighValue() {
        return filterHighValue;
    }

    public Boolean getUseFilterRange() {
        return useFilterRange;
    }

    public Boolean getDoNotAbortExportIfFilterCellMissing() {
        return doNotAbortIfFilterCellMissing;
    }

    public Integer getMaxOutputColumns() {
        return maxOutputColumns;
    }

    public Boolean getMaxOutputColumnsNoAbort() {
        return maxOutputColumnsNoAbort;
    }

    public Boolean getMaxOutputColumnsUseMultiFile() {
        return maxOutputColumnsUseMultiFile;
    }

    public Boolean getLogStreamPlan() {
        return logStreamPlan;
    }

    public List<String> getWhiteListCellsNamesTypes() {
        return whiteListCellsNamesTypes;
    }

    public List<String> getBlackListCellsNamesTypes() {
        return blackListCellsNamesTypes;
    }

    public Integer getMaxStringLength() {
        return maxStringLength;
    }

    public Integer getMaxByteArraySize() {
        return maxByteArraySize;
    }
}