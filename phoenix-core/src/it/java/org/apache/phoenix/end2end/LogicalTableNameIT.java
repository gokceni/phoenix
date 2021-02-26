package org.apache.phoenix.end2end;

import com.google.common.collect.Maps;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.end2end.index.SingleCellIndexIT;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.StringUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LogicalTableNameIT extends ParallelStatsDisabledIT  {
    private static final Logger LOGGER = LoggerFactory.getLogger(LogicalTableNameIT.class);

    private Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);

    @BeforeClass
    public static void doSetup() throws Exception {
        Map<String, String> props = Maps.newConcurrentMap();
        props.put(QueryServices.DROP_METADATA_ATTRIB, Boolean.TRUE.toString());
        props.put(QueryServices.MUTATE_BATCH_SIZE_ATTRIB, Integer.toString(3000));
        //When we run all tests together we are using global cluster(driver)
        //so to make drop work we need to re register driver with DROP_METADATA_ATTRIB property
        destroyDriver();
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
        //Registering real Phoenix driver to have multiple ConnectionQueryServices created across connections
        //so that metadata changes doesn't get propagated across connections
        DriverManager.registerDriver(PhoenixDriver.INSTANCE);
    }

    public LogicalTableNameIT()  {
    }

    private Connection getConnection(Properties props) throws Exception {
        props.setProperty(QueryServices.DROP_METADATA_ATTRIB, Boolean.toString(true));
        // Force real driver to be used as the test one doesn't handle creating
        // more than one ConnectionQueryService
        props.setProperty(QueryServices.EXTRA_JDBC_ARGUMENTS_ATTRIB, StringUtil.EMPTY_STRING);
        // Create new ConnectionQueryServices so that we can set DROP_METADATA_ATTRIB
        String url = QueryUtil.getConnectionUrl(props, config, "PRINCIPAL");
        return DriverManager.getConnection(url, props);
    }

    @Test
    public void testUpdatePhysicalTableNameWithIndex() throws Exception {
        try (Connection conn = getConnection(props)) {
            conn.setAutoCommit(true);
            String tableName = "TBL_" + generateUniqueName();
            String indexName = "IDX_" + generateUniqueName();

            String createTableSql = "CREATE TABLE " + tableName + " (PK1 VARCHAR NOT NULL, V1 VARCHAR, V2 INTEGER, V3 INTEGER "
                    + "CONSTRAINT NAME_PK PRIMARY KEY(PK1)) COLUMN_ENCODED_BYTES=0 ";
            LOGGER.debug(createTableSql);
            conn.createStatement().execute(createTableSql);
            String createIndexSql = "CREATE INDEX " + indexName + " ON " + tableName + " (V1) INCLUDE (V2, V3) ";
            LOGGER.debug(createIndexSql);
            conn.createStatement().execute(createIndexSql);
            String upsert = "UPSERT INTO " + tableName + " (PK1, V1,  V2, V3) VALUES (?,?,?,?)";
            PreparedStatement upsertStmt = conn.prepareStatement(upsert);

            int numOfRows = 2;
            for (int i=1; i <= numOfRows; i++) {
                upsertStmt.setString(1, "PK"+i);
                upsertStmt.setString(2, "V1"+i);
                upsertStmt.setInt(3, i);
                upsertStmt.setInt(4, i+1);
                upsertStmt.executeUpdate();
            }

            // Create another hbase table and add 1 more row
            String newTableName = "NEW_TBL_" + generateUniqueName();
            try (HBaseAdmin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()){
                String snapshotName = new StringBuilder(tableName).append("-Snapshot").toString();
                admin.snapshot(snapshotName, TableName.valueOf(tableName));
                admin.cloneSnapshot(Bytes.toBytes(snapshotName), Bytes.toBytes(newTableName));

                try (HTableInterface htable =
                        conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(
                                Bytes.toBytes(newTableName))) {
                    Put put = new Put(ByteUtil.concat(Bytes.toBytes("PK3")));
                    put.addColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, QueryConstants.EMPTY_COLUMN_BYTES,
                            QueryConstants.EMPTY_COLUMN_VALUE_BYTES);
                    put.addColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, Bytes.toBytes("V1"),
                            Bytes.toBytes("V13"));
                    put.addColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, Bytes.toBytes("V2"),
                            PInteger.INSTANCE.toBytes(3));
                    put.addColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, Bytes.toBytes("V3"),
                            PInteger.INSTANCE.toBytes(4));
                    htable.put(put);
                }
            }
            try (Connection conn2 = getConnection(props)) {
                // Query to cache on the second connection
                String selectTable1 = "SELECT PK1, V1, V2, V3 FROM " + tableName + " ORDER BY PK1 DESC";
                ResultSet rs1 = conn2.createStatement().executeQuery(selectTable1);
                assertTrue(rs1.next());

                // Rename table to point to the new hbase table
                renameAndDropPhysicalTable(conn, "NULL", "NULL", tableName, newTableName);


                // Expires Syscat region server caches
                conn.unwrap(PhoenixConnection.class).getQueryServices().clearTableFromCache(ByteUtil.EMPTY_BYTE_ARRAY, ByteUtil.EMPTY_BYTE_ARRAY , Bytes.toBytes(tableName),
                        HConstants.LATEST_TIMESTAMP);

                SingleCellIndexIT.dumpTable(newTableName);

                // We have to rebuild index for this to work
                IndexToolIT.runIndexTool(true, false, null, tableName, indexName);

                validateTable(conn, tableName);
                validateTable(conn2, tableName);
                validateIndex(conn, indexName);
                validateIndex(conn2, indexName);
            }
        }
    }

    @Test
    public void testUpdatePhysicalTableNameWithViews() throws Exception {
        try (Connection conn = getConnection(props)) {
            conn.setAutoCommit(true);
            String tableName = "TBL_" + generateUniqueName();
            String indexName = "IDX_" + generateUniqueName();
            String view1Name = "VW_" + generateUniqueName();
            String view1IndexName = "VWIDX_" + generateUniqueName();

            String createTableSql = "CREATE TABLE " + tableName + " (PK1 VARCHAR NOT NULL, V1 VARCHAR, V2 INTEGER, V3 INTEGER "
                    + "CONSTRAINT NAME_PK PRIMARY KEY(PK1)) COLUMN_ENCODED_BYTES=0 ";
            LOGGER.debug(createTableSql);
            conn.createStatement().execute(createTableSql);
            String upsert = "UPSERT INTO " + view1Name + " (PK1, V1,  V2, V3, VIEW_COL1, VIEW_COL2) VALUES (?,?,?,?,?,?)";
            PreparedStatement upsertStmt = conn.prepareStatement(upsert);

            String
                    view1DDL =
                    "CREATE VIEW " + view1Name + " ( VIEW_COL1 VARCHAR, VIEW_COL2 VARCHAR) AS SELECT * FROM "
                            + tableName;
            conn.createStatement().execute(view1DDL);
            String indexDDL = "CREATE INDEX " + view1IndexName + " ON " + view1Name + " (V1) include (VIEW_COL2, V3) IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS,COLUMN_ENCODED_BYTES=2";
            conn.createStatement().execute(indexDDL);
            conn.commit();

            int numOfRows = 2;
            for (int i=1; i <= numOfRows; i++) {
                upsertStmt.setString(1, "PK"+i);
                upsertStmt.setString(2, "V1"+i);
                upsertStmt.setInt(3, i);
                upsertStmt.setInt(4, i+1);
                upsertStmt.setString(5, "VIEW_COL1_"+i);
                upsertStmt.setString(6, "VIEW_COL2_"+i);
                upsertStmt.executeUpdate();
            }

            // Create another hbase table and add 1 more row
            String newTableName = "NEW_TBL_" + generateUniqueName();
            try (HBaseAdmin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()){
                String snapshotName = new StringBuilder(tableName).append("-Snapshot").toString();
                admin.snapshot(snapshotName, TableName.valueOf(tableName));
                admin.cloneSnapshot(Bytes.toBytes(snapshotName), Bytes.toBytes(newTableName));

                try (HTableInterface htable =
                        conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(
                                Bytes.toBytes(newTableName))) {
                    Put put = new Put(ByteUtil.concat(Bytes.toBytes("PK3")));
                    put.addColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, QueryConstants.EMPTY_COLUMN_BYTES,
                            QueryConstants.EMPTY_COLUMN_VALUE_BYTES);
                    put.addColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, Bytes.toBytes("V1"),
                            Bytes.toBytes("V13"));
                    put.addColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, Bytes.toBytes("V2"),
                            PInteger.INSTANCE.toBytes(3));
                    put.addColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, Bytes.toBytes("V3"),
                            PInteger.INSTANCE.toBytes(4));
                    put.addColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, Bytes.toBytes("VIEW1_COL1"),
                            Bytes.toBytes("VIEW1_COL1_3"));
                    put.addColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, Bytes.toBytes("VIEW1_COL2"),
                            Bytes.toBytes("VIEW1_COL2_3"));
                    htable.put(put);
                }
            }

            try (Connection conn2 = getConnection(props)) {
                // Query to cache on the second connection
                String selectTable1 = "SELECT PK1, V1, V2, V3 FROM " + tableName + " ORDER BY PK1 DESC";
                ResultSet rs1 = conn2.createStatement().executeQuery(selectTable1);
                assertTrue(rs1.next());

                // Rename table to point to hbase table
                renameAndDropPhysicalTable(conn, "NULL", "NULL", tableName, newTableName);

//                conn.unwrap(PhoenixConnection.class).getQueryServices()
//                        .clearTableFromCache(ByteUtil.EMPTY_BYTE_ARRAY, ByteUtil.EMPTY_BYTE_ARRAY,
//                                Bytes.toBytes(tableName), HConstants.LATEST_TIMESTAMP);
                conn.unwrap(PhoenixConnection.class).getQueryServices().clearCache();
                SingleCellIndexIT.dumpTable(newTableName);

                // We have to rebuild index for this to work
                IndexToolIT.runIndexTool(true, false, null, view1Name, view1IndexName);

                validateIndex(conn, view1IndexName);
                validateIndex(conn2, view1IndexName);
            }
        }
    }

    private void validateTable(Connection connection, String tableName) throws SQLException {
        String selectTable = "SELECT PK1, V1, V2, V3 FROM " + tableName + " ORDER BY PK1 DESC";
        ResultSet rs = connection.createStatement().executeQuery(selectTable);
        assertTrue(rs.next());
        assertEquals("PK3", rs.getString(1));
        assertEquals("V13", rs.getString(2));
        assertEquals(3, rs.getInt(3));
        assertEquals(4, rs.getInt(4));
        assertTrue(rs.next());
        assertEquals("PK2", rs.getString(1));
        assertEquals("V12", rs.getString(2));
        assertEquals(2, rs.getInt(3));
        assertEquals(3, rs.getInt(4));
        assertTrue(rs.next());
        assertEquals("PK1", rs.getString(1));
        assertEquals("V11", rs.getString(2));
        assertEquals(1, rs.getInt(3));
        assertEquals(2, rs.getInt(4));
    }

    private void validateIndex(Connection connection, String tableName) throws SQLException {
        String selectTable = "SELECT * FROM " + tableName ;
        ResultSet rs = connection.createStatement().executeQuery(selectTable);
        assertTrue(rs.next());
        assertEquals("PK1", rs.getString(2));
        assertEquals("V11", rs.getString(1));
        assertEquals(1, rs.getInt(3));
        assertEquals(2, rs.getInt(4));
        assertTrue(rs.next());
        assertEquals("PK2", rs.getString(2));
        assertEquals("V12", rs.getString(1));
        assertEquals(2, rs.getInt(3));
        assertEquals(3, rs.getInt(4));
        assertTrue(rs.next());
        assertEquals("PK3", rs.getString(2));
        assertEquals("V13", rs.getString(1));
        assertEquals(3, rs.getInt(3));
        assertEquals(4, rs.getInt(4));
    }

    public static void renameAndDropPhysicalTable(Connection conn, String tenantId, String schema, String tableName, String physicalName) throws Exception {
        String
                changeName =
                String.format(
                        "UPSERT INTO SYSTEM.CATALOG (TENANT_ID, TABLE_SCHEM, TABLE_NAME, COLUMN_NAME, COLUMN_FAMILY, PHYSICAL_TABLE_NAME) VALUES (%s, %s, '%s', NULL, NULL, '%s')",
                        tenantId, schema, tableName, physicalName);
        conn.createStatement().execute(changeName);
        conn.commit();

        Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin();
        TableName hTableName = TableName.valueOf(tableName);
        admin.disableTable(hTableName);
        admin.deleteTable(hTableName);
    }
}
