// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.analysis;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.property.fileformat.FileFormatProperties;
import org.apache.doris.load.RoutineLoadDesc;
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.load.routineload.AbstractDataSourceProperties;
import org.apache.doris.load.routineload.RoutineLoadDataSourcePropertyFactory;
import org.apache.doris.load.routineload.RoutineLoadJob;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.resource.workloadgroup.WorkloadGroup;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

/*
 Create routine Load statement,  continually load data from a streaming app

 syntax:
      CREATE ROUTINE LOAD [database.]name on table
      [load properties]
      [PROPERTIES
      (
          desired_concurrent_number = xxx,
          max_error_number = xxx,
          k1 = v1,
          ...
          kn = vn
      )]
      FROM type of routine load
      [(
          k1 = v1,
          ...
          kn = vn
      )]

      load properties:
          load property [[,] load property] ...

      load property:
          column separator | columns_mapping | partitions | where

      column separator:
          COLUMNS TERMINATED BY xxx
      columns_mapping:
          COLUMNS (c1, c2, c3 = c1 + c2)
      partitions:
          PARTITIONS (p1, p2, p3)
      where:
          WHERE c1 > 1

      type of routine load:
          KAFKA
*/
public class CreateRoutineLoadStmt extends DdlStmt implements NotFallbackInParser {
    private static final Logger LOG = LogManager.getLogger(CreateRoutineLoadStmt.class);

    // routine load properties
    public static final String DESIRED_CONCURRENT_NUMBER_PROPERTY = "desired_concurrent_number";
    public static final String CURRENT_CONCURRENT_NUMBER_PROPERTY = "current_concurrent_number";
    // max error number in ten thousand records
    public static final String MAX_ERROR_NUMBER_PROPERTY = "max_error_number";
    public static final String MAX_FILTER_RATIO_PROPERTY = "max_filter_ratio";
    // the following 3 properties limit the time and batch size of a single routine load task
    public static final String MAX_BATCH_INTERVAL_SEC_PROPERTY = "max_batch_interval";
    public static final String MAX_BATCH_ROWS_PROPERTY = "max_batch_rows";
    public static final String MAX_BATCH_SIZE_PROPERTY = "max_batch_size";
    public static final String EXEC_MEM_LIMIT_PROPERTY = "exec_mem_limit";

    public static final String FORMAT = "format"; // the value is csv or json, default is csv
    public static final String STRIP_OUTER_ARRAY = "strip_outer_array";
    public static final String JSONPATHS = "jsonpaths";
    public static final String JSONROOT = "json_root";
    public static final String NUM_AS_STRING = "num_as_string";
    public static final String FUZZY_PARSE = "fuzzy_parse";

    public static final String PARTIAL_COLUMNS = "partial_columns";

    public static final String WORKLOAD_GROUP = "workload_group";

    private static final String NAME_TYPE = "ROUTINE LOAD NAME";
    public static final String ENDPOINT_REGEX = "[-A-Za-z0-9+&@#/%?=~_|!:,.;]+[-A-Za-z0-9+&@#/%=~_|]";
    public static final String SEND_BATCH_PARALLELISM = "send_batch_parallelism";
    public static final String LOAD_TO_SINGLE_TABLET = "load_to_single_tablet";

    private AbstractDataSourceProperties dataSourceProperties;


    private static final ImmutableSet<String> PROPERTIES_SET = new ImmutableSet.Builder<String>()
            .add(DESIRED_CONCURRENT_NUMBER_PROPERTY)
            .add(MAX_ERROR_NUMBER_PROPERTY)
            .add(MAX_FILTER_RATIO_PROPERTY)
            .add(MAX_BATCH_INTERVAL_SEC_PROPERTY)
            .add(MAX_BATCH_ROWS_PROPERTY)
            .add(MAX_BATCH_SIZE_PROPERTY)
            .add(FORMAT)
            .add(JSONPATHS)
            .add(STRIP_OUTER_ARRAY)
            .add(NUM_AS_STRING)
            .add(FUZZY_PARSE)
            .add(JSONROOT)
            .add(LoadStmt.STRICT_MODE)
            .add(LoadStmt.TIMEZONE)
            .add(EXEC_MEM_LIMIT_PROPERTY)
            .add(SEND_BATCH_PARALLELISM)
            .add(LOAD_TO_SINGLE_TABLET)
            .add(PARTIAL_COLUMNS)
            .add(WORKLOAD_GROUP)
            .add(LoadStmt.KEY_ENCLOSE)
            .add(LoadStmt.KEY_ESCAPE)
            .build();

    private final LabelName labelName;
    private String tableName;
    private final List<ParseNode> loadPropertyList;
    private final Map<String, String> jobProperties;
    private final String typeName;

    // the following variables will be initialized after analyze
    // -1 as unset, the default value will set in RoutineLoadJob
    private String name;
    private String dbName;
    private RoutineLoadDesc routineLoadDesc;
    private int desiredConcurrentNum = 1;
    private long maxErrorNum = -1;
    private double maxFilterRatio = -1;
    private long maxBatchIntervalS = -1;
    private long maxBatchRows = -1;
    private long maxBatchSizeBytes = -1;
    private boolean strictMode = true;
    private long execMemLimit = 2 * 1024 * 1024 * 1024L;
    private String timezone = TimeUtils.DEFAULT_TIME_ZONE;
    private int sendBatchParallelism = 1;
    private boolean loadToSingleTablet = false;

    private FileFormatProperties fileFormatProperties;

    private String workloadGroupName = "";

    /**
     * support partial columns load(Only Unique Key Columns)
     */
    @Getter
    private boolean isPartialUpdate = false;

    private String comment = "";

    private LoadTask.MergeType mergeType;

    private boolean isMultiTable = false;

    public static final Predicate<Long> DESIRED_CONCURRENT_NUMBER_PRED = (v) -> v > 0L;
    public static final Predicate<Long> MAX_ERROR_NUMBER_PRED = (v) -> v >= 0L;
    public static final Predicate<Double> MAX_FILTER_RATIO_PRED = (v) -> v >= 0 && v <= 1;
    public static final Predicate<Long> MAX_BATCH_INTERVAL_PRED = (v) -> v >= 1;
    public static final Predicate<Long> MAX_BATCH_ROWS_PRED = (v) -> v >= 200000;
    public static final Predicate<Long> MAX_BATCH_SIZE_PRED = (v) -> v >= 100 * 1024 * 1024
                                                            && v <= (long) (1024 * 1024 * 1024) * 10;
    public static final Predicate<Long> EXEC_MEM_LIMIT_PRED = (v) -> v >= 0L;
    public static final Predicate<Long> SEND_BATCH_PARALLELISM_PRED = (v) -> v > 0L;

    public CreateRoutineLoadStmt(LabelName labelName, String tableName, List<ParseNode> loadPropertyList,
                                 Map<String, String> jobProperties, String typeName,
                                 Map<String, String> dataSourceProperties, LoadTask.MergeType mergeType,
                                 String comment) {
        this.labelName = labelName;
        if (StringUtils.isBlank(tableName)) {
            this.isMultiTable = true;
        }
        this.tableName = tableName;
        this.loadPropertyList = loadPropertyList;
        this.jobProperties = jobProperties == null ? Maps.newHashMap() : jobProperties;
        this.typeName = typeName.toUpperCase();
        this.dataSourceProperties = RoutineLoadDataSourcePropertyFactory
                .createDataSource(typeName, dataSourceProperties, this.isMultiTable);
        this.mergeType = mergeType;
        this.isPartialUpdate = this.jobProperties.getOrDefault(PARTIAL_COLUMNS, "false").equalsIgnoreCase("true");
        if (comment != null) {
            this.comment = comment;
        }
        String format = jobProperties.getOrDefault(FileFormatProperties.PROP_FORMAT, "csv");
        fileFormatProperties = FileFormatProperties.createFileFormatProperties(format);
    }

    /*
     * make stmt by nereids
     */
    public CreateRoutineLoadStmt(LabelName labelName, String dbName, String name, String tableName,
            List<ParseNode> loadPropertyList, OriginStatement origStmt, UserIdentity userIdentity,
            Map<String, String> jobProperties, String typeName, RoutineLoadDesc routineLoadDesc,
            int desireTaskConcurrentNum, long maxErrorNum, double maxFilterRatio, long maxBatchIntervalS,
            long maxBatchRows, long maxBatchSizeBytes, long execMemLimit, int sendBatchParallelism, String timezone,
            String workloadGroupName, boolean loadToSingleTablet, boolean strictMode,
            boolean isPartialUpdate, AbstractDataSourceProperties dataSourceProperties,
            FileFormatProperties fileFormatProperties) {
        this.labelName = labelName;
        this.dbName = dbName;
        this.name = name;
        this.tableName = tableName;
        this.loadPropertyList = loadPropertyList;
        this.setOrigStmt(origStmt);
        this.setUserInfo(userIdentity);
        this.jobProperties = jobProperties;
        this.typeName = typeName;
        this.routineLoadDesc = routineLoadDesc;
        this.desiredConcurrentNum = desireTaskConcurrentNum;
        this.maxErrorNum = maxErrorNum;
        this.maxFilterRatio = maxFilterRatio;
        this.maxBatchIntervalS = maxBatchIntervalS;
        this.maxBatchRows = maxBatchRows;
        this.maxBatchSizeBytes = maxBatchSizeBytes;
        this.execMemLimit = execMemLimit;
        this.sendBatchParallelism = sendBatchParallelism;
        this.timezone = timezone;
        this.workloadGroupName = workloadGroupName;
        this.loadToSingleTablet = loadToSingleTablet;
        this.strictMode = strictMode;
        this.isPartialUpdate = isPartialUpdate;
        this.dataSourceProperties = dataSourceProperties;
        this.fileFormatProperties = fileFormatProperties;
    }

    public String getName() {
        return name;
    }

    public String getDBName() {
        return dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getTypeName() {
        return typeName;
    }

    public RoutineLoadDesc getRoutineLoadDesc() {
        return routineLoadDesc;
    }

    public int getDesiredConcurrentNum() {
        return desiredConcurrentNum;
    }

    public long getMaxErrorNum() {
        return maxErrorNum;
    }

    public double getMaxFilterRatio() {
        return maxFilterRatio;
    }

    public long getMaxBatchIntervalS() {
        return maxBatchIntervalS;
    }

    public long getMaxBatchRows() {
        return maxBatchRows;
    }

    public long getMaxBatchSize() {
        return maxBatchSizeBytes;
    }

    public long getExecMemLimit() {
        return execMemLimit;
    }

    public int getSendBatchParallelism() {
        return sendBatchParallelism;
    }

    public boolean isLoadToSingleTablet() {
        return loadToSingleTablet;
    }

    public boolean isStrictMode() {
        return strictMode;
    }

    public String getTimezone() {
        return timezone;
    }

    public LoadTask.MergeType getMergeType() {
        return mergeType;
    }

    public FileFormatProperties getFileFormatProperties() {
        return fileFormatProperties;
    }

    public AbstractDataSourceProperties getDataSourceProperties() {
        return dataSourceProperties;
    }

    public String getComment() {
        return comment;
    }

    public String getWorkloadGroupName() {
        return this.workloadGroupName;
    }

    @Override
    public void analyze() throws UserException {
        super.analyze();
        // check dbName and tableName
        checkDBTable();
        // check name
        try {
            FeNameFormat.checkCommonName(NAME_TYPE, name);
        } catch (AnalysisException e) {
            // 64 is the length of regular expression matching
            // (FeNameFormat.COMMON_NAME_REGEX/UNDERSCORE_COMMON_NAME_REGEX)
            throw new AnalysisException(e.getMessage()
                    + " Maybe routine load job name is longer than 64 or contains illegal characters");
        }
        // check load properties include column separator etc.
        checkLoadProperties();
        // check routine load job properties include desired concurrent number etc.
        checkJobProperties();
        // check data source properties
        checkDataSourceProperties();
        // analyze merge type
        if (routineLoadDesc != null) {
            routineLoadDesc.analyze();
        } else if (mergeType == LoadTask.MergeType.MERGE) {
            throw new AnalysisException("Excepted DELETE ON clause when merge type is MERGE.");
        }
    }

    public void checkDBTable() throws AnalysisException {
        labelName.analyze();
        dbName = labelName.getDbName();
        name = labelName.getLabelName();
        Database db = Env.getCurrentInternalCatalog().getDbOrAnalysisException(dbName);
        if (isPartialUpdate && isMultiTable) {
            throw new AnalysisException("Partial update is not supported in multi-table load.");
        }
        if (isMultiTable) {
            return;
        }
        if (Strings.isNullOrEmpty(tableName)) {
            throw new AnalysisException("Table name should not be null");
        }
        Table table = db.getTableOrAnalysisException(tableName);
        if (mergeType != LoadTask.MergeType.APPEND
                && (table.getType() != Table.TableType.OLAP
                || ((OlapTable) table).getKeysType() != KeysType.UNIQUE_KEYS)) {
            throw new AnalysisException("load by MERGE or DELETE is only supported in unique tables.");
        }
        if (mergeType != LoadTask.MergeType.APPEND
                && !(table.getType() == Table.TableType.OLAP && ((OlapTable) table).hasDeleteSign())) {
            throw new AnalysisException("load by MERGE or DELETE need to upgrade table to support batch delete.");
        }
        if (isPartialUpdate && !((OlapTable) table).getEnableUniqueKeyMergeOnWrite()) {
            throw new AnalysisException("load by PARTIAL_COLUMNS is only supported in unique table MoW");
        }
    }

    public void checkLoadProperties() throws UserException {
        Separator columnSeparator = null;
        // TODO(yangzhengguo01): add line delimiter to properties
        Separator lineDelimiter = null;
        ImportColumnsStmt importColumnsStmt = null;
        ImportWhereStmt precedingImportWhereStmt = null;
        ImportWhereStmt importWhereStmt = null;
        ImportSequenceStmt importSequenceStmt = null;
        PartitionNames partitionNames = null;
        ImportDeleteOnStmt importDeleteOnStmt = null;
        if (loadPropertyList != null) {
            for (ParseNode parseNode : loadPropertyList) {
                if (parseNode instanceof Separator) {
                    // check column separator
                    if (columnSeparator != null) {
                        throw new AnalysisException("repeat setting of column separator");
                    }
                    columnSeparator = (Separator) parseNode;
                    columnSeparator.analyze();
                } else if (parseNode instanceof ImportColumnsStmt) {
                    if (isMultiTable) {
                        throw new AnalysisException("Multi-table load does not support setting columns info");
                    }
                    // check columns info
                    if (importColumnsStmt != null) {
                        throw new AnalysisException("repeat setting of columns info");
                    }
                    importColumnsStmt = (ImportColumnsStmt) parseNode;
                } else if (parseNode instanceof ImportWhereStmt) {
                    // check where expr
                    ImportWhereStmt node = (ImportWhereStmt) parseNode;
                    if (node.isPreceding()) {
                        if (isMultiTable) {
                            throw new AnalysisException("Multi-table load does not support setting columns info");
                        }
                        if (precedingImportWhereStmt != null) {
                            throw new AnalysisException("repeat setting of preceding where predicate");
                        }
                        precedingImportWhereStmt = node;
                    } else {
                        if (importWhereStmt != null) {
                            throw new AnalysisException("repeat setting of where predicate");
                        }
                        importWhereStmt = node;
                    }
                } else if (parseNode instanceof PartitionNames) {
                    // check partition names
                    if (partitionNames != null) {
                        throw new AnalysisException("repeat setting of partition names");
                    }
                    partitionNames = (PartitionNames) parseNode;
                    partitionNames.analyze();
                } else if (parseNode instanceof ImportDeleteOnStmt) {
                    // check delete expr
                    if (importDeleteOnStmt != null) {
                        throw new AnalysisException("repeat setting of delete predicate");
                    }
                    importDeleteOnStmt = (ImportDeleteOnStmt) parseNode;
                } else if (parseNode instanceof ImportSequenceStmt) {
                    // check sequence column
                    if (importSequenceStmt != null) {
                        throw new AnalysisException("repeat setting of sequence column");
                    }
                    importSequenceStmt = (ImportSequenceStmt) parseNode;
                }
            }
        }
        routineLoadDesc = new RoutineLoadDesc(columnSeparator, lineDelimiter, importColumnsStmt,
                precedingImportWhereStmt, importWhereStmt,
                partitionNames, importDeleteOnStmt == null ? null : importDeleteOnStmt.getExpr(), mergeType,
                importSequenceStmt == null ? null : importSequenceStmt.getSequenceColName());
    }

    private void checkJobProperties() throws UserException {
        Optional<String> optional = jobProperties.keySet().stream().filter(
                entity -> !PROPERTIES_SET.contains(entity)).findFirst();
        if (optional.isPresent()) {
            throw new AnalysisException(optional.get() + " is invalid property");
        }

        desiredConcurrentNum = ((Long) Util.getLongPropertyOrDefault(
                jobProperties.get(DESIRED_CONCURRENT_NUMBER_PROPERTY),
                Config.max_routine_load_task_concurrent_num, DESIRED_CONCURRENT_NUMBER_PRED,
                DESIRED_CONCURRENT_NUMBER_PROPERTY + " must be greater than 0")).intValue();

        maxErrorNum = Util.getLongPropertyOrDefault(jobProperties.get(MAX_ERROR_NUMBER_PROPERTY),
                RoutineLoadJob.DEFAULT_MAX_ERROR_NUM, MAX_ERROR_NUMBER_PRED,
                MAX_ERROR_NUMBER_PROPERTY + " should >= 0");

        maxFilterRatio = Util.getDoublePropertyOrDefault(jobProperties.get(MAX_FILTER_RATIO_PROPERTY),
                RoutineLoadJob.DEFAULT_MAX_FILTER_RATIO, MAX_FILTER_RATIO_PRED,
                MAX_FILTER_RATIO_PROPERTY + " should between 0 and 1");

        maxBatchIntervalS = Util.getLongPropertyOrDefault(jobProperties.get(MAX_BATCH_INTERVAL_SEC_PROPERTY),
                RoutineLoadJob.DEFAULT_MAX_INTERVAL_SECOND, MAX_BATCH_INTERVAL_PRED,
                MAX_BATCH_INTERVAL_SEC_PROPERTY + " should >= 1");

        maxBatchRows = Util.getLongPropertyOrDefault(jobProperties.get(MAX_BATCH_ROWS_PROPERTY),
                RoutineLoadJob.DEFAULT_MAX_BATCH_ROWS, MAX_BATCH_ROWS_PRED,
                MAX_BATCH_ROWS_PROPERTY + " should > 200000");

        maxBatchSizeBytes = Util.getLongPropertyOrDefault(jobProperties.get(MAX_BATCH_SIZE_PROPERTY),
                RoutineLoadJob.DEFAULT_MAX_BATCH_SIZE, MAX_BATCH_SIZE_PRED,
                MAX_BATCH_SIZE_PROPERTY + " should between 100MB and 10GB");

        strictMode = Util.getBooleanPropertyOrDefault(jobProperties.get(LoadStmt.STRICT_MODE),
                RoutineLoadJob.DEFAULT_STRICT_MODE,
                LoadStmt.STRICT_MODE + " should be a boolean");
        execMemLimit = Util.getLongPropertyOrDefault(jobProperties.get(EXEC_MEM_LIMIT_PROPERTY),
                RoutineLoadJob.DEFAULT_EXEC_MEM_LIMIT, EXEC_MEM_LIMIT_PRED,
                EXEC_MEM_LIMIT_PROPERTY + " must be greater than 0");

        sendBatchParallelism = ((Long) Util.getLongPropertyOrDefault(jobProperties.get(SEND_BATCH_PARALLELISM),
                ConnectContext.get().getSessionVariable().getSendBatchParallelism(), SEND_BATCH_PARALLELISM_PRED,
                SEND_BATCH_PARALLELISM + " must be greater than 0")).intValue();
        loadToSingleTablet = Util.getBooleanPropertyOrDefault(jobProperties.get(LoadStmt.LOAD_TO_SINGLE_TABLET),
                RoutineLoadJob.DEFAULT_LOAD_TO_SINGLE_TABLET,
                LoadStmt.LOAD_TO_SINGLE_TABLET + " should be a boolean");

        String inputWorkloadGroupStr = jobProperties.get(WORKLOAD_GROUP);
        if (!StringUtils.isEmpty(inputWorkloadGroupStr)) {
            ConnectContext tmpCtx = new ConnectContext();
            if (Config.isCloudMode()) {
                tmpCtx.setCloudCluster(ConnectContext.get().getCloudCluster());
            }
            tmpCtx.setCurrentUserIdentity(ConnectContext.get().getCurrentUserIdentity());
            tmpCtx.getSessionVariable().setWorkloadGroup(inputWorkloadGroupStr);
            List<WorkloadGroup> wgList = Env.getCurrentEnv().getWorkloadGroupMgr()
                    .getWorkloadGroup(tmpCtx);
            if (wgList.size() == 0) {
                throw new UserException("Can not find workload group " + inputWorkloadGroupStr);
            }
            workloadGroupName = inputWorkloadGroupStr;
        }

        if (ConnectContext.get() != null) {
            timezone = ConnectContext.get().getSessionVariable().getTimeZone();
        }
        timezone = TimeUtils.checkTimeZoneValidAndStandardize(jobProperties.getOrDefault(LoadStmt.TIMEZONE, timezone));

        fileFormatProperties.analyzeFileFormatProperties(jobProperties, false);
    }

    private void checkDataSourceProperties() throws UserException {
        this.dataSourceProperties.setTimezone(this.timezone);
        this.dataSourceProperties.analyze();
    }

    @Override
    public StmtType stmtType() {
        return StmtType.CREATE;
    }
}
