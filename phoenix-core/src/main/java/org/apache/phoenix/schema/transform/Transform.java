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

import com.google.common.base.Strings;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;

public class Transform {
    public static final Logger LOGGER = LoggerFactory.getLogger(Transform.class);

    public static void addTransform(
            SystemTransformRecord systemTransformParams, PhoenixConnection connection) throws SQLException {
        PreparedStatement stmt;
        stmt = connection.prepareStatement("UPSERT INTO " +
                PhoenixDatabaseMetaData.SYSTEM_TRANSFORM_NAME + " ( " +
                PhoenixDatabaseMetaData.TABLE_SCHEM + ", " +
                PhoenixDatabaseMetaData.LOGICAL_TABLE_NAME + ", " +
                PhoenixDatabaseMetaData.NEW_PHY_TABLE_NAME + ", " +
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
                " ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)");
        if (systemTransformParams.getSchemaName() != null) {
            stmt.setString(1, systemTransformParams.getSchemaName());
        } else {
            stmt.setNull(1, Types.VARCHAR);
        }
        stmt.setString(2, systemTransformParams.getLogicalTableName());

        stmt.setString(3, systemTransformParams.getNewPhysicalTableName());

        stmt.setByte(4, systemTransformParams.getTransformType().getSerializedValue());
        if (systemTransformParams.getLogicalParentName() != null) {
            stmt.setString(5, systemTransformParams.getLogicalParentName());
        } else {
            stmt.setNull(5, Types.VARCHAR);
        }
        if (systemTransformParams.getTransformStatus() != null) {
            stmt.setString(6, systemTransformParams.getTransformStatus());
        } else {
            stmt.setString(6, PTable.TaskStatus.CREATED.toString());
        }
        if (systemTransformParams.getTransformJobId() != null) {
            stmt.setString(7, systemTransformParams.getTransformJobId());
        } else {
            stmt.setNull(7, Types.VARCHAR);
        }
        if (systemTransformParams.getTransformRetryCount() != null) {
            stmt.setInt(8, systemTransformParams.getTransformRetryCount());
        } else {
            stmt.setInt(8, 0);
        }
        if (systemTransformParams.getTransformStartTs() != null) {
            stmt.setTimestamp(9, systemTransformParams.getTransformStartTs());
        } else {
            stmt.setTimestamp(9, new Timestamp(EnvironmentEdgeManager.currentTimeMillis()));
        }
        if (systemTransformParams.getTransformEndTs() != null) {
            stmt.setTimestamp(10, systemTransformParams.getTransformEndTs());
        } else {
            if (systemTransformParams.getTransformStatus() != null && systemTransformParams.getTransformStatus().equals(PTable.TaskStatus.COMPLETED.toString())) {
                stmt.setTimestamp(10, new Timestamp(EnvironmentEdgeManager.currentTimeMillis()));
            } else {
                stmt.setNull(10, Types.TIMESTAMP);
            }
        }
        if (systemTransformParams.getOldMetadata() != null) {
            stmt.setString(11, systemTransformParams.getOldMetadata());
        } else {
            stmt.setNull(11, Types.VARCHAR);
        }
        if (systemTransformParams.getNewMetadata() != null) {
            stmt.setString(12, systemTransformParams.getNewMetadata());
        } else {
            stmt.setNull(12, Types.VARCHAR);
        }
        if (systemTransformParams.getTransformFunction() != null) {
            stmt.setString(13, systemTransformParams.getTransformFunction());
        } else {
            stmt.setNull(13, Types.VARCHAR);
        }

        LOGGER.info("Adding transform type: "
                + systemTransformParams.getString());
        stmt.execute();
    }


    public static SystemTransformRecord getTransformRecord(
            String schema, String logicalTableName, String logicalParentName, PhoenixConnection connection) throws SQLException {
        ResultSet resultSet = connection.prepareStatement("SELECT " +
                PhoenixDatabaseMetaData.TABLE_SCHEM + ", " +
                PhoenixDatabaseMetaData.LOGICAL_TABLE_NAME + ", " +
                PhoenixDatabaseMetaData.NEW_PHY_TABLE_NAME + ", " +
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
                (Strings.isNullOrEmpty(schema) ? "" : (PhoenixDatabaseMetaData.TABLE_SCHEM + " ='" + schema + "' AND ")) +
                        PhoenixDatabaseMetaData.LOGICAL_TABLE_NAME + " ='" + logicalTableName + "'" +
                (Strings.isNullOrEmpty(logicalParentName) ? "": (PhoenixDatabaseMetaData.LOGICAL_PARENT_NAME + "=" + logicalParentName ))
        ).executeQuery();
        if (resultSet.next()) {
            return SystemTransformRecord.SystemTransformBuilder.build(resultSet);
        }
        return null;
    }

    public static boolean isActive(SystemTransformRecord existingTransform) {
        return (existingTransform.getTransformStatus().equals(PTable.TransformStatus.STARTED)
                || existingTransform.getTransformStatus().equals(PTable.TransformStatus.CREATED));
    }
}


