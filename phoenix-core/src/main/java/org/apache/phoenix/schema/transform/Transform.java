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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import org.apache.phoenix.coprocessor.MetaDataProtocol;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.schema.MetaDataClient;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableImpl;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.JacksonUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.UpgradeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import org.apache.phoenix.schema.SchemaExtractionProcessor;

import static org.apache.phoenix.schema.PTableType.INDEX;

public class Transform {
    public static final Logger LOGGER = LoggerFactory.getLogger(Transform.class);

    private static String generateNewTableName(String schema, String logicalTableName, long seqNum) {
        // TODO: Support schema versioning as well.
        String newName = String.format("%s_%d", SchemaUtil.getTableName(schema, logicalTableName), seqNum);
        return newName;
    }

    public static void addTransform(PhoenixConnection connection, String tenantId, PTable table, MetaDataClient.MetaProperties changingProperties, long sequenceNum) throws SQLException {
        try {
            String newMetadata = JacksonUtil.getObjectWriter().writeValueAsString(changingProperties);
            String oldMetadata = "";
            String newPhysicalTableName = "";
            SystemTransformRecord.SystemTransformBuilder transformBuilder = new SystemTransformRecord.SystemTransformBuilder();
            String schema = table.getSchemaName()!=null ? table.getSchemaName().getString() : null;
            String logicalTableName = table.getTableName().getString();
            transformBuilder.setSchemaName(schema);
            transformBuilder.setLogicalTableName(logicalTableName);
            transformBuilder.setTenantId(tenantId);
            if (table.getType() == INDEX) {
                transformBuilder.setLogicalParentName(table.getParentName().getString());
            }
            // TODO: add more ways of finding out what transform type this is
            transformBuilder.setTransformType(PTable.TransformType.METADATA_TRANSFORM);
            // TODO: calculate old and new metadata
            transformBuilder.setNewMetadata(newMetadata);
            transformBuilder.setOldMetadata(oldMetadata);
            if (Strings.isNullOrEmpty(newPhysicalTableName)) {
                newPhysicalTableName = generateNewTableName(schema, logicalTableName, sequenceNum);
            }
            transformBuilder.setNewPhysicalTableName(newPhysicalTableName);
            Transform.addTransform(table, changingProperties, transformBuilder.build(), connection);
        } catch (JsonProcessingException ex) {
            LOGGER.error("addTransform failed", ex);
            throw new SQLException("Adding transform failed with JsonProcessingException");
        } catch (SQLException ex) {
            throw ex;
        } catch(Exception ex) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.valueOf("CANNOT_MUTATE_TABLE"))
                    .setSchemaName((table.getSchemaName() == null? null: table.getSchemaName().getString()))
                    .setTableName(table.getName().getString()).build().buildException();
        }
    }

    protected static void addTransform(
            PTable table, MetaDataClient.MetaProperties changedProps, SystemTransformRecord systemTransformParams, PhoenixConnection connection) throws Exception {
        PName newTableName = PNameFactory.newName(systemTransformParams.getNewPhysicalTableName());
        PName newTableNameWithoutSchema = PNameFactory.newName(SchemaUtil.getTableNameFromFullName(systemTransformParams.getNewPhysicalTableName()));
        PTable newTable = new PTableImpl.Builder()
                .setTableName(newTableNameWithoutSchema)
                .setParentTableName(table.getParentTableName())
                .setBaseTableLogicalName(table.getBaseTableLogicalName())
                .setPhysicalTableName(newTableNameWithoutSchema)
                .setAllColumns(table.getColumns())
                .setAppendOnlySchema(table.isAppendOnlySchema())
                .setAutoPartitionSeqName(table.getAutoPartitionSeqName())
                .setBaseColumnCount(table.getBaseColumnCount())
                .setBucketNum(table.getBucketNum())
                .setDefaultFamilyName(table.getDefaultFamilyName())
                .setDisableWAL(table.isWALDisabled())
                .setEstimatedSize(table.getEstimatedSize())
                .setFamilies(table.getColumnFamilies())
                .setImmutableRows(table.isImmutableRows())
                .setIsChangeDetectionEnabled(table.isChangeDetectionEnabled())
                .setIndexType(table.getIndexType())
                .setName(newTableName)
                .setMultiTenant(table.isMultiTenant())
                .setParentName(table.getParentName())
                .setParentSchemaName(table.getParentSchemaName())
                .setPhoenixTTL(table.getPhoenixTTL())
                .setNamespaceMapped(table.isNamespaceMapped())
                .setSchemaName(table.getSchemaName())
                .setPkColumns(table.getPKColumns())
                .setPkName(table.getPKName())
                .setPhoenixTTLHighWaterMark(table.getPhoenixTTLHighWaterMark())
                .setRowKeySchema(table.getRowKeySchema())
                .setStoreNulls(table.getStoreNulls())
                .setTenantId(table.getTenantId())
                .setType(table.getType())
                // SchemaExtractor uses physical name to get the table descriptor from. So we use the existing table here
                .setPhysicalNames(ImmutableList.copyOf(table.getPhysicalNames()))
                .setUpdateCacheFrequency(table.getUpdateCacheFrequency())
                .setTransactionProvider(table.getTransactionProvider())
                .setUseStatsForParallelization(table.useStatsForParallelization())
                // Transformables
                .setImmutableStorageScheme(
                        (changedProps.getImmutableStorageSchemeProp() != null? changedProps.getImmutableStorageSchemeProp():table.getImmutableStorageScheme()))
                .setQualifierEncodingScheme(
                        (changedProps.getColumnEncodedBytesProp() != null? changedProps.getColumnEncodedBytesProp() : table.getEncodingScheme()))
                .build();
        SchemaExtractionProcessor schemaExtractionProcessor = new SchemaExtractionProcessor(systemTransformParams.getTenantId(),
                connection.getQueryServices().getConfiguration(), systemTransformParams.getSchemaName(), newTable);
        String ddl = schemaExtractionProcessor.process();
        //ddl = "CREATE TABLE N000001.N000002_1(ID VARCHAR NOT NULL PRIMARY KEY, \"info\".\"CAR_NUM\" VARCHAR(18), \"test\".\"CAR_NUM\" VARCHAR(18), \"info\".\"CAP_DATE\" VARCHAR, \"info\".\"ORG_ID\" BIGINT, \"info\".\"ORG_NAME\" VARCHAR(255)) BLOOMFILTER=NONE, KEEP_DELETED_CELLS=FALSE, UPDATE_CACHE_FREQUENCY=0, ENCODING_SCHEME=TWO_BYTE_QUALIFIERS, BLOCKSIZE=65536, COMPRESSION=NONE, TTL=2147483647, VERSIONS=1, IMMUTABLE_ROWS=false, DATA_BLOCK_ENCODING=FAST_DIFF, IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, MIN_VERSIONS=0, APPEND_ONLY_SCHEMA=false, DISABLE_WAL=false, MULTI_TENANT=false, BLOCKCACHE=true, REPLICATION_SCOPE=0, PHYSICAL_TABLE_NAME='N000001.N000002_1', IN_MEMORY=false";
        System.out.println(ddl);
        connection.createStatement().execute(ddl);
        upsertTransform(systemTransformParams, connection);

        // TODO: Create the same table in DR buddy
    }

    public static void upsertTransform(
            SystemTransformRecord systemTransformParams, PhoenixConnection connection) throws SQLException {
        try (PreparedStatement stmt = connection.prepareStatement("UPSERT INTO " +
                PhoenixDatabaseMetaData.SYSTEM_TRANSFORM_NAME + " ( " +
                PhoenixDatabaseMetaData.TABLE_SCHEM + ", " +
                PhoenixDatabaseMetaData.LOGICAL_TABLE_NAME + ", " +
                PhoenixDatabaseMetaData.TENANT_ID + "," +
                PhoenixDatabaseMetaData.NEW_PHYS_TABLE_NAME + ", " +
                PhoenixDatabaseMetaData.TRANSFORM_TYPE + ", " +
                PhoenixDatabaseMetaData.LOGICAL_PARENT_NAME + ", " +
                PhoenixDatabaseMetaData.TRANSFORM_STATUS + ", " +
                PhoenixDatabaseMetaData.TRANSFORM_JOB_ID + ", " +
                PhoenixDatabaseMetaData.TRANSFORM_RETRY_COUNT + ", " +
                PhoenixDatabaseMetaData.TRANSFORM_START_TS + ", " +
                PhoenixDatabaseMetaData.TRANSFORM_END_TS + ", " +
                PhoenixDatabaseMetaData.OLD_METADATA + " , " +
                PhoenixDatabaseMetaData.NEW_METADATA + " , " +
                PhoenixDatabaseMetaData.TRANSFORM_FUNCTION +
                " ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)")) {
            int colNum = 1;
            if (systemTransformParams.getSchemaName() != null) {
                stmt.setString(colNum++, systemTransformParams.getSchemaName());
            } else {
                stmt.setNull(colNum++, Types.VARCHAR);
            }
            stmt.setString(colNum++, systemTransformParams.getLogicalTableName());
            if (systemTransformParams.getTenantId() != null) {
                stmt.setString(colNum++, systemTransformParams.getTenantId());
            } else {
                stmt.setNull(colNum++, Types.VARCHAR);
            }
            stmt.setString(colNum++, systemTransformParams.getNewPhysicalTableName());

            stmt.setByte(colNum++, systemTransformParams.getTransformType().getSerializedValue());
            if (systemTransformParams.getLogicalParentName() != null) {
                stmt.setString(colNum++, systemTransformParams.getLogicalParentName());
            } else {
                stmt.setNull(colNum++, Types.VARCHAR);
            }

            stmt.setString(colNum++, systemTransformParams.getTransformStatus());

            if (systemTransformParams.getTransformJobId() != null) {
                stmt.setString(colNum++, systemTransformParams.getTransformJobId());
            } else {
                stmt.setNull(colNum++, Types.VARCHAR);
            }
            stmt.setInt(colNum++, systemTransformParams.getTransformRetryCount());

            stmt.setTimestamp(colNum++, systemTransformParams.getTransformStartTs());

            if (systemTransformParams.getTransformEndTs() != null) {
                stmt.setTimestamp(colNum++, systemTransformParams.getTransformEndTs());
            } else {
                stmt.setNull(colNum++, Types.TIMESTAMP);
            }
            if (systemTransformParams.getOldMetadata() != null) {
                stmt.setString(colNum++, systemTransformParams.getOldMetadata());
            } else {
                stmt.setNull(colNum++, Types.VARCHAR);
            }
            if (systemTransformParams.getNewMetadata() != null) {
                stmt.setString(colNum++, systemTransformParams.getNewMetadata());
            } else {
                stmt.setNull(colNum++, Types.VARCHAR);
            }
            if (systemTransformParams.getTransformFunction() != null) {
                stmt.setString(colNum++, systemTransformParams.getTransformFunction());
            } else {
                stmt.setNull(colNum++, Types.VARCHAR);
            }

            LOGGER.info("Adding transform type: "
                    + systemTransformParams.getString());
            stmt.execute();
        }
    }


    public static SystemTransformRecord getTransformRecord(
            String schema, String logicalTableName, String logicalParentName, String tenantId, PhoenixConnection connection) throws SQLException {
        try (ResultSet resultSet = connection.prepareStatement("SELECT " +
                PhoenixDatabaseMetaData.TENANT_ID + ", " +
                PhoenixDatabaseMetaData.TABLE_SCHEM + ", " +
                PhoenixDatabaseMetaData.LOGICAL_TABLE_NAME + ", " +
                PhoenixDatabaseMetaData.NEW_PHYS_TABLE_NAME + ", " +
                PhoenixDatabaseMetaData.TRANSFORM_TYPE + ", " +
                PhoenixDatabaseMetaData.LOGICAL_PARENT_NAME + ", " +
                PhoenixDatabaseMetaData.TRANSFORM_STATUS + ", " +
                PhoenixDatabaseMetaData.TRANSFORM_JOB_ID + ", " +
                PhoenixDatabaseMetaData.TRANSFORM_RETRY_COUNT + ", " +
                PhoenixDatabaseMetaData.TRANSFORM_START_TS + ", " +
                PhoenixDatabaseMetaData.TRANSFORM_END_TS + ", " +
                PhoenixDatabaseMetaData.OLD_METADATA + " , " +
                PhoenixDatabaseMetaData.NEW_METADATA + " , " +
                PhoenixDatabaseMetaData.TRANSFORM_FUNCTION +
                " FROM " + PhoenixDatabaseMetaData.SYSTEM_TRANSFORM_NAME + " WHERE  " +
                (Strings.isNullOrEmpty(tenantId) ? "" : (PhoenixDatabaseMetaData.TENANT_ID + " ='" + tenantId + "' AND ")) +
                (Strings.isNullOrEmpty(schema) ? "" : (PhoenixDatabaseMetaData.TABLE_SCHEM + " ='" + schema + "' AND ")) +
                PhoenixDatabaseMetaData.LOGICAL_TABLE_NAME + " ='" + logicalTableName + "'" +
                (Strings.isNullOrEmpty(logicalParentName) ? "": (" AND " + PhoenixDatabaseMetaData.LOGICAL_PARENT_NAME + "='" + logicalParentName + "'" ))
        ).executeQuery()) {
            if (resultSet.next()) {
                return SystemTransformRecord.SystemTransformBuilder.build(resultSet);
            }
            return null;
        }
    }

    public static void removeTransformRecord(
            SystemTransformRecord transformRecord, PhoenixConnection connection) throws SQLException {
        System.out.println("DELETE FROM  "
                + PhoenixDatabaseMetaData.SYSTEM_TRANSFORM_NAME + " WHERE " +
                (Strings.isNullOrEmpty(transformRecord.getSchemaName()) ? "" :
                        (PhoenixDatabaseMetaData.TABLE_SCHEM + " ='" + transformRecord.getSchemaName() + "' AND ")) +
                PhoenixDatabaseMetaData.LOGICAL_TABLE_NAME + " ='" + transformRecord.getLogicalTableName() + "' AND " +
                PhoenixDatabaseMetaData.NEW_PHYS_TABLE_NAME + " ='" + transformRecord.getNewPhysicalTableName() + "' AND " +
                PhoenixDatabaseMetaData.TRANSFORM_TYPE + " =" + transformRecord.getTransformType().getSerializedValue());
        connection.prepareStatement("DELETE FROM  "
                        + PhoenixDatabaseMetaData.SYSTEM_TRANSFORM_NAME + " WHERE " +
                (Strings.isNullOrEmpty(transformRecord.getSchemaName()) ? "" :
                        (PhoenixDatabaseMetaData.TABLE_SCHEM + " ='" + transformRecord.getSchemaName() + "' AND ")) +
                PhoenixDatabaseMetaData.LOGICAL_TABLE_NAME + " ='" + transformRecord.getLogicalTableName() + "' AND " +
                PhoenixDatabaseMetaData.NEW_PHYS_TABLE_NAME + " ='" + transformRecord.getNewPhysicalTableName() + "' AND " +
                PhoenixDatabaseMetaData.TRANSFORM_TYPE + " =" + transformRecord.getTransformType().getSerializedValue()
        ).execute();
    }

    public static boolean checkIsTransformNeeded(MetaDataClient.MetaProperties metaProperties, String schemaName,
                                                 PTable table, String logicalTableName, String parentTableName,
                                                 String tenantId, PhoenixConnection connection) throws SQLException {
        boolean isTransformNeeded = isTransformNeeded(metaProperties, table);
        if (isTransformNeeded) {
            SystemTransformRecord existingTransform = Transform.getTransformRecord(schemaName, logicalTableName, parentTableName, tenantId,connection);
            if (existingTransform != null && existingTransform.isActive()) {
                throw new SQLExceptionInfo.Builder(
                        SQLExceptionCode.CANNOT_TRANSFORM_ALREADY_TRANSFORMING_TABLE)
                        .setMessage(" Only one transform at a time is allowed ")
                        .setSchemaName(schemaName).setTableName(logicalTableName).build().buildException();
            }
        }
        return isTransformNeeded;
    }

    private static boolean isTransformNeeded(MetaDataClient.MetaProperties metaProperties, PTable table){
        if (metaProperties.getImmutableStorageSchemeProp()!=null
                && metaProperties.getImmutableStorageSchemeProp() != table.getImmutableStorageScheme()) {
            // Transform is needed
            return true;
        }
        if (metaProperties.getColumnEncodedBytesProp()!=null
                && metaProperties.getColumnEncodedBytesProp() != table.getEncodingScheme()) {
            return true;
        }
        return false;
    }
}


