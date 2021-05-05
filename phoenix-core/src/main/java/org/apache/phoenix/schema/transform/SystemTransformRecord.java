/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.phoenix.schema.transform;

import org.apache.phoenix.schema.PTable;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * Task params to be used while upserting records in SYSTEM.TRANSFORM table.
 * This POJO is mainly used while upserting(and committing) or generating
 * upsert mutations plan in {@link Transform} class
 */
@edu.umd.cs.findbugs.annotations.SuppressWarnings(
    value = {"EI_EXPOSE_REP", "EI_EXPOSE_REP2"},
    justification = "endTs and startTs are not used for mutation")
public class SystemTransformRecord {
    private final PTable.TransformType transformType;
    private final String schemaName;
    private final String logicalTableName;
    private final String logicalParentName;
    private final String newPhysicalTableName;
    private final String transformStatus;
    private final String transformJobId;
    private final Integer transformRetryCount;
    private final Timestamp startTs;
    private final Timestamp endTs;
    private final String oldMetadata;
    private final String newMetadata;
    private final String transformFunction;

    public SystemTransformRecord(PTable.TransformType transformType,
                                 String schemaName, String logicalTableName, String newPhysicalTableName, String logicalParentName,
                                 String transformStatus, String transformJobId, Integer transformRetryCount, Timestamp startTs,
                                 Timestamp endTs, String oldMetadata, String newMetadata, String transformFunction) {
        this.transformType = transformType;
        this.schemaName = schemaName;
        this.logicalTableName = logicalTableName;
        this.newPhysicalTableName = newPhysicalTableName;
        this.logicalParentName = logicalParentName;
        this.transformStatus = transformStatus;
        this.transformJobId = transformJobId;
        this.transformRetryCount = transformRetryCount;
        this.startTs = startTs;
        this.endTs = endTs;
        this.oldMetadata = oldMetadata;
        this.newMetadata = newMetadata;
        this.transformFunction = transformFunction;
    }

    public String getString() {
        return String.format("transformType: %s, schameName: %s, logicalTableName: %s, newPhysicalTableName: %s, logicalParentName: %s "
                , String.valueOf(transformType), String.valueOf(schemaName), String.valueOf(logicalTableName), String.valueOf(newPhysicalTableName),
                String.valueOf(logicalParentName));
    }

    public PTable.TransformType getTransformType() {
        return transformType;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getLogicalTableName() {
        return logicalTableName;
    }

    public String getLogicalParentName() {
        return logicalParentName;
    }

    public String getNewPhysicalTableName() {
        return newPhysicalTableName;
    }

    public String getTransformStatus() {
        return transformStatus;
    }

    public String getTransformJobId() {
        return transformJobId;
    }

    public Integer getTransformRetryCount() {
        return transformRetryCount;
    }

    public Timestamp getTransformStartTs() {
        return startTs;
    }

    public Timestamp getTransformEndTs() {
        return endTs;
    }

    public String getOldMetadata() {
        return oldMetadata;
    }
    public String getNewMetadata() {
        return newMetadata;
    }
    public String getTransformFunction() { return transformFunction; }

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(
        value = {"EI_EXPOSE_REP", "EI_EXPOSE_REP2"},
        justification = "endTs and startTs are not used for mutation")
    public static class SystemTransformBuilder {

        private PTable.TransformType transformType;
        private String schemaName;
        private String logicalTableName;
        private String logicalParentName;
        private String newPhysicalTableName;
        private String transformStatus;
        private String transformJobId;
        private Integer transformRetryCount;
        private Timestamp startTs;
        private Timestamp endTs;
        private String oldMetadata;
        private String newMetadata;
        private String transformFunction;

        public SystemTransformBuilder setTransformType(PTable.TransformType transformType) {
            this.transformType = transformType;
            return this;
        }

        public SystemTransformBuilder setSchemaName(String schemaName) {
            this.schemaName = schemaName;
            return this;
        }

        public SystemTransformBuilder setLogicalTableName(String tableName) {
            this.logicalTableName = tableName;
            return this;
        }

        public SystemTransformBuilder setLogicalParentName(String name) {
            this.logicalParentName = name;
            return this;
        }

        public SystemTransformBuilder setNewPhysicalTableName(String tableName) {
            this.newPhysicalTableName = tableName;
            return this;
        }

        public SystemTransformBuilder setTransformStatus(String transformStatus) {
            this.transformStatus = transformStatus;
            return this;
        }

        public SystemTransformBuilder setTransformJobId(String transformJobId) {
            this.transformJobId = transformJobId;
            return this;
        }

        public SystemTransformBuilder setOldMetadata(String oldMetadata) {
            this.oldMetadata = oldMetadata;
            return this;
        }

        public SystemTransformBuilder setNewMetadata(String newMetadata) {
            this.newMetadata = newMetadata;
            return this;
        }

        public SystemTransformBuilder setTransformRetryCount(Integer transformRetryCount) {
            this.transformRetryCount = transformRetryCount;
            return this;
        }

        public SystemTransformBuilder setStartTs(Timestamp startTs) {
            this.startTs = startTs;
            return this;
        }

        public SystemTransformBuilder setEndTs(Timestamp endTs) {
            this.endTs = endTs;
            return this;
        }

        public SystemTransformBuilder setTransformFunction(String transformFunction) {
            this.transformFunction = transformFunction;
            return this;
        }

        public SystemTransformRecord build() {
            return new SystemTransformRecord(transformType, schemaName,
                logicalTableName, newPhysicalTableName, logicalParentName, transformStatus, transformJobId, transformRetryCount, startTs, endTs,
                oldMetadata, newMetadata, transformFunction);
        }

        public static SystemTransformRecord build(ResultSet resultSet) throws SQLException {
            int col = 1;
            SystemTransformBuilder builder = new SystemTransformBuilder();
            builder.setSchemaName(resultSet.getString(col++));
            builder.setLogicalTableName(resultSet.getString(col++));
            builder.setNewPhysicalTableName(resultSet.getString(col++));
            builder.setTransformType(PTable.TransformType.fromSerializedValue(resultSet.getByte(col++)));
            builder.setLogicalParentName(resultSet.getString(col++));
            builder.setTransformStatus(resultSet.getString(col++));
            builder.setTransformJobId(resultSet.getString(col++));
            builder.setTransformRetryCount(resultSet.getInt(col++));
            builder.setStartTs(resultSet.getTimestamp(col++));
            builder.setEndTs(resultSet.getTimestamp(col++));
            builder.setOldMetadata(resultSet.getString(col++));
            builder.setNewMetadata(resultSet.getString(col++));
            builder.setTransformFunction(resultSet.getString(col++));

            return builder.build();
        }
    }
}
