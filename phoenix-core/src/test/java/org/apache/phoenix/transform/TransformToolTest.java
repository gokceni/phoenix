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
package org.apache.phoenix.transform;

import org.apache.phoenix.end2end.TransformToolIT;
import org.apache.phoenix.mapreduce.transform.TransformTool;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.transform.SystemTransformRecord;
import org.apache.phoenix.thirdparty.org.apache.commons.cli.CommandLine;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.apache.phoenix.mapreduce.index.IndexTool.INVALID_TIME_RANGE_EXCEPTION_MESSAGE;
import static org.apache.phoenix.mapreduce.transform.TransformTool.PARTIAL_TRANSFORM_NOT_APPLICABLE;
import static org.apache.phoenix.mapreduce.transform.TransformTool.PARTIAL_TRANSFORM_NOT_COMPATIBLE;
import static org.apache.phoenix.mapreduce.transform.TransformTool.TRANSFORM_NOT_APPLICABLE;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransformToolTest extends BaseTest {

    TransformTool tt;
    private String dataTable;
    private String indexTable;
    private String schema;
    private String tenantId;
    @Mock
    PTable pDataTable;
    PTable pIndexTable;
    SystemTransformRecord str;

    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    @Before
    public void setup() {
        tt = new TransformTool();
        schema = generateUniqueName();
        dataTable = generateUniqueName();
        indexTable = generateUniqueName();
        tenantId = generateUniqueName();
        pDataTable = mock(PTable.class);
        pIndexTable = mock(PTable.class);
        str = mock(SystemTransformRecord.class);
        when(pIndexTable.getType()).thenReturn(PTableType.INDEX);
        when(pIndexTable.getIndexType()).thenReturn(PTable.IndexType.GLOBAL);
        when(pDataTable.getType()).thenReturn(PTableType.TABLE);
        when(pDataTable.isTransactional()).thenReturn(false);
        TransformTool mockTransform = mock(TransformTool.class);
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testParseOptions_timeRange_timeRangeNotNull() throws Exception {
        Long startTime = 10L;
        Long endTime = 15L;
        String [] args =
                TransformToolIT.getArgValues(schema,
                        dataTable, indexTable, tenantId,
                        startTime , endTime);
        CommandLine cmdLine = tt.parseOptions(args);
        tt.populateTransformToolAttributesAndValidate(cmdLine, false);
        assertEquals(startTime, tt.getStartTime());
        assertEquals(endTime, tt.getEndTime());
    }

    @Test
    public void testParseOptions_timeRange_null() throws Exception {
        String [] args =
                TransformToolIT.getArgValues(schema,
                        dataTable, indexTable, tenantId,
                        null , null);
        CommandLine cmdLine = tt.parseOptions(args);
        tt.populateTransformToolAttributesAndValidate(cmdLine, false);
        Assert.assertNull(tt.getStartTime());
        Assert.assertNull(tt.getEndTime());
    }

    @Test
    public void testParseOptions_timeRange_startTimeNotNull() throws Exception {
        Long startTime = 10L;
        String [] args =
                TransformToolIT.getArgValues(schema,
                        dataTable, indexTable, tenantId,
                        startTime , null);
        CommandLine cmdLine = tt.parseOptions(args);
        tt.populateTransformToolAttributesAndValidate(cmdLine, false);
        assertEquals(startTime, tt.getStartTime());
        assertEquals(null, tt.getEndTime());
    }

    @Test
    public void testParseOptions_timeRange_endTimeNotNull() throws Exception {
        Long endTime = 15L;
        String [] args =
                TransformToolIT.getArgValues(schema,
                        dataTable, indexTable, tenantId,
                        null , endTime);
        CommandLine cmdLine = tt.parseOptions(args);
        tt.populateTransformToolAttributesAndValidate(cmdLine, false);
        assertEquals(null, tt.getStartTime());
        assertEquals(endTime, tt.getEndTime());
    }

    @Test
    public void testParseOptions_timeRange_startTimeNullEndTimeInFuture() throws Exception {
        Long endTime = EnvironmentEdgeManager.currentTimeMillis() + 100000;
        String [] args = TransformToolIT.getArgValues(schema,
                        dataTable, indexTable, tenantId,
                        null , endTime);
        CommandLine cmdLine = tt.parseOptions(args);
        exceptionRule.expect(RuntimeException.class);
        exceptionRule.expectMessage(INVALID_TIME_RANGE_EXCEPTION_MESSAGE);
        tt.populateTransformToolAttributesAndValidate(cmdLine, false);
    }

    @Test
    public void testParseOptions_timeRange_endTimeNullStartTimeInFuture() throws Exception {
        Long startTime = EnvironmentEdgeManager.currentTimeMillis() + 100000;
        String [] args =
                TransformToolIT.getArgValues(schema,
                        dataTable, indexTable, tenantId,
                        startTime , null);
        CommandLine cmdLine = tt.parseOptions(args);
        exceptionRule.expect(RuntimeException.class);
        exceptionRule.expectMessage(INVALID_TIME_RANGE_EXCEPTION_MESSAGE);
        tt.populateTransformToolAttributesAndValidate(cmdLine, false);
    }

    @Test(timeout = 10000 /* 10 secs */)
    public void testParseOptions_timeRange_startTimeInFuture() throws Exception {
        Long startTime = EnvironmentEdgeManager.currentTimeMillis() + 100000;
        Long endTime = EnvironmentEdgeManager.currentTimeMillis() + 200000;
        String [] args =
                TransformToolIT.getArgValues(schema,
                        dataTable, indexTable, tenantId,
                        startTime , endTime);
        CommandLine cmdLine = tt.parseOptions(args);
        exceptionRule.expect(RuntimeException.class);
        exceptionRule.expectMessage(INVALID_TIME_RANGE_EXCEPTION_MESSAGE);
        tt.populateTransformToolAttributesAndValidate(cmdLine, false);
    }

    @Test(timeout = 10000 /* 10 secs */)
    public void testParseOptions_timeRange_endTimeInFuture() throws Exception {
        Long startTime = EnvironmentEdgeManager.currentTimeMillis();
        Long endTime = EnvironmentEdgeManager.currentTimeMillis() + 100000;
        String [] args =
                TransformToolIT.getArgValues(schema,
                        dataTable, indexTable, tenantId,
                        startTime , endTime);
        CommandLine cmdLine = tt.parseOptions(args);
        exceptionRule.expect(RuntimeException.class);
        exceptionRule.expectMessage(INVALID_TIME_RANGE_EXCEPTION_MESSAGE);
        tt.populateTransformToolAttributesAndValidate(cmdLine, false);
    }

    @Test
    public void testParseOptions_timeRange_startTimeEqEndTime() throws Exception {
        Long startTime = 10L;
        Long endTime = 10L;
        String [] args =
                TransformToolIT.getArgValues(schema,
                        dataTable, indexTable, tenantId,
                        startTime , endTime);
        CommandLine cmdLine = tt.parseOptions(args);
        exceptionRule.expect(RuntimeException.class);
        exceptionRule.expectMessage(INVALID_TIME_RANGE_EXCEPTION_MESSAGE);
        tt.populateTransformToolAttributesAndValidate(cmdLine, false);
    }

    @Test
    public void testParseOptions_timeRange_startTimeGtEndTime() throws Exception {
        Long startTime = 10L;
        Long endTime = 1L;
        String [] args =
                TransformToolIT.getArgValues(schema,
                        dataTable, indexTable, tenantId,
                        startTime , endTime);
        CommandLine cmdLine = tt.parseOptions(args);
        exceptionRule.expect(RuntimeException.class);
        exceptionRule.expectMessage(INVALID_TIME_RANGE_EXCEPTION_MESSAGE);
        tt.populateTransformToolAttributesAndValidate(cmdLine, false);
    }

    @Test
    public void testTransactionalTable_globalIndex() throws Exception {
        when(pDataTable.isTransactional()).thenReturn(true);
        exceptionRule.expect(RuntimeException.class);
        exceptionRule.expectMessage(TRANSFORM_NOT_APPLICABLE);
        String [] args =
                TransformToolIT.getArgValues(schema,
                        dataTable, indexTable, tenantId,
                        null , null);
        CommandLine cmdLine = tt.parseOptions(args);
        tt.validateTransform(pDataTable, pIndexTable, str);
    }

    @Test
    public void testTransformLocalIndex_notApplicable() throws Exception {
        when(pIndexTable.getIndexType()).thenReturn(PTable.IndexType.LOCAL);
        exceptionRule.expect(IllegalArgumentException.class);
        exceptionRule.expectMessage(TRANSFORM_NOT_APPLICABLE);
        tt.validateTransform(pDataTable, pIndexTable, str);
    }

    @Test
    public void testTransform_notApplicable() throws Exception {
        when(pIndexTable.getIndexType()).thenReturn(PTable.IndexType.LOCAL);
        exceptionRule.expect(IllegalArgumentException.class);
        exceptionRule.expectMessage(TRANSFORM_NOT_APPLICABLE);
        tt.validateTransform(pDataTable, pIndexTable, str);
    }

    @Test
    public void testPartialTransform_notCompatible() throws Exception {
        String [] args =
                TransformToolIT.getArgList(schema,
                        dataTable, indexTable, tenantId,
                        null , null, true, true, true, true).toArray(new String[0]);
        CommandLine cmdLine = tt.parseOptions(args);
        exceptionRule.expect(IllegalArgumentException.class);
        exceptionRule.expectMessage(PARTIAL_TRANSFORM_NOT_COMPATIBLE);
        tt.populateTransformToolAttributesAndValidate(cmdLine, false);
    }

    @Test
    public void testPartialTransform_notApplicable() throws Exception {
        String [] args =
                TransformToolIT.getArgList(schema,
                        dataTable, indexTable, tenantId,
                        null , null, false, false, false, true).toArray(new String[0]);
        CommandLine cmdLine = tt.parseOptions(args);
        exceptionRule.expect(IllegalArgumentException.class);
        exceptionRule.expectMessage(PARTIAL_TRANSFORM_NOT_APPLICABLE);
        tt.populateTransformToolAttributesAndValidate(cmdLine, false);
    }
}
