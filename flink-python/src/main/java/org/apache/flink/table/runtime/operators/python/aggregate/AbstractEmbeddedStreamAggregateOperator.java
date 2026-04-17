/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.operators.python.aggregate;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.python.util.ProtoUtils;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.python.embedded.AbstractEmbeddedPythonFunctionOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.python.PythonAggregateFunctionInfo;
import org.apache.flink.table.runtime.dataview.DataViewSpec;
import org.apache.flink.table.runtime.operators.python.utils.StreamRecordRowDataWrappingCollector;
import org.apache.flink.table.runtime.typeutils.PythonTypeUtils;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import pemja.core.object.PyIterator;

import java.util.Arrays;
import java.util.stream.Collectors;

import static org.apache.flink.python.PythonOptions.PYTHON_METRIC_ENABLED;
import static org.apache.flink.python.PythonOptions.PYTHON_PROFILE_ENABLED;
import static org.apache.flink.python.PythonOptions.STATE_CACHE_SIZE;
import static org.apache.flink.python.PythonOptions.MAP_STATE_READ_CACHE_SIZE;
import static org.apache.flink.python.PythonOptions.MAP_STATE_WRITE_CACHE_SIZE;
import static org.apache.flink.table.runtime.typeutils.PythonTypeUtils.toProtoType;

/**
 * Base class for embedded Python aggregate operators. Mirrors the responsibilities of {@link
 * AbstractPythonStreamAggregateOperator} but runs Python functions in-process via pemja rather than
 * through a Beam runner.
 */
@Internal
public abstract class AbstractEmbeddedStreamAggregateOperator
        extends AbstractEmbeddedPythonFunctionOperator<RowData>
        implements OneInputStreamOperator<RowData, RowData>, BoundedOneInput {

    private static final long serialVersionUID = 1L;

    /** The aggregate functions to be executed. */
    protected final PythonAggregateFunctionInfo[] aggregateFunctions;

    /** The data view specifications for each aggregate function. */
    protected final DataViewSpec[][] dataViewSpecs;

    /** The input logical type. */
    protected final RowType inputType;

    /** The output logical type. */
    protected final RowType outputType;

    /** The array of the key indexes. */
    protected final int[] grouping;

    /** The index of a count aggregate used to calculate the number of accumulated rows. */
    protected final int indexOfCountStar;

    /** Generate retract messages if true. */
    protected final boolean generateUpdateBefore;

    /** The maximum NUMBER of the states cached in Python side. */
    private final int stateCacheSize;

    /** The maximum number of cached entries in a single Python MapState. */
    private final int mapStateReadCacheSize;

    private final int mapStateWriteCacheSize;

    /** Converters for each input field (RowData field -> Python-compatible object). */
    protected transient PythonTypeUtils.DataConverter[] inputConverters;

    /** Converters for each output field (Python result -> RowData field). */
    protected transient PythonTypeUtils.DataConverter[] outputConverters;

    /** The collector used to collect records. */
    protected transient StreamRecordRowDataWrappingCollector rowDataWrapper;

    /** Reusable output row. */
    protected transient GenericRowData reuseResultRowData;

    public AbstractEmbeddedStreamAggregateOperator(
            Configuration config,
            RowType inputType,
            RowType outputType,
            PythonAggregateFunctionInfo[] aggregateFunctions,
            DataViewSpec[][] dataViewSpecs,
            int[] grouping,
            int indexOfCountStar,
            boolean generateUpdateBefore) {
        super(config);
        this.inputType = Preconditions.checkNotNull(inputType);
        this.outputType = Preconditions.checkNotNull(outputType);
        this.aggregateFunctions = Preconditions.checkNotNull(aggregateFunctions);
        this.dataViewSpecs = Preconditions.checkNotNull(dataViewSpecs);
        this.grouping = Preconditions.checkNotNull(grouping);
        this.indexOfCountStar = indexOfCountStar;
        this.generateUpdateBefore = generateUpdateBefore;
        this.stateCacheSize = config.get(STATE_CACHE_SIZE);
        this.mapStateReadCacheSize = config.get(MAP_STATE_READ_CACHE_SIZE);
        this.mapStateWriteCacheSize = config.get(MAP_STATE_WRITE_CACHE_SIZE);
    }

    @Override
    public void open() throws Exception {
        super.open();
        rowDataWrapper = new StreamRecordRowDataWrappingCollector(output);
        reuseResultRowData = new GenericRowData(outputType.getFieldCount());

        inputConverters =
                inputType.getFields().stream()
                        .map(RowType.RowField::getType)
                        .map(PythonTypeUtils::toDataConverter)
                        .toArray(PythonTypeUtils.DataConverter[]::new);
        outputConverters =
                outputType.getFields().stream()
                        .map(RowType.RowField::getType)
                        .map(PythonTypeUtils::toDataConverter)
                        .toArray(PythonTypeUtils.DataConverter[]::new);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void processElement(StreamRecord<RowData> element) throws Exception {
        RowData value = element.getValue();
        Object[] args = new Object[inputType.getFieldCount() + 1];
        args[0] = value.getRowKind().toByteValue();
        for (int i = 0; i < inputType.getFieldCount(); i++) {
            args[i + 1] = inputConverters[i].toExternal(value, i);
        }
        interpreter.invokeMethod("operation", "process_element", (Object) args);
        elementCount++;
        checkInvokeFinishBundleByCount();
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void invokeFinishBundle() throws Exception {
        PyIterator results =
                (PyIterator)
                        interpreter.invokeMethod("operation", "finish_bundle_func");
        if (results != null) {
            while (results.hasNext()) {
                Object[] result = (Object[]) results.next();
                byte rowKindByte = ((Number) result[0]).byteValue();
                for (int i = 0; i < outputType.getFieldCount(); i++) {
                    reuseResultRowData.setField(
                            i, outputConverters[i].toInternal(result[i + 1]));
                }
                reuseResultRowData.setRowKind(RowKind.fromByteValue(rowKindByte));
                rowDataWrapper.collect(reuseResultRowData);
            }
            results.close();
        }
        elementCount = 0;
        lastFinishBundleTime = getProcessingTimeService().getCurrentProcessingTime();
        if (bundleFinishedCallback != null) {
            bundleFinishedCallback.run();
        }
    }

    @Override
    public void endInput() throws Exception {
        invokeFinishBundle();
        if (interpreter != null) {
            interpreter.invokeMethod("operation", "close");
        }
    }

    /** Returns the key type projected from the input type using the grouping indices. */
    protected RowType getKeyType() {
        return (RowType) Projection.of(grouping).project(inputType);
    }

    /**
     * Returns the name of the Python factory function used to create the aggregate operation (e.g.
     * {@code "create_group_aggregate_operation_from_proto"}).
     */
    protected abstract String getPythonFactoryFunctionName();

    /**
     * Gets the proto representation of the Python user-defined aggregate functions to be executed.
     */
    protected FlinkFnApi.UserDefinedAggregateFunctions getUserDefinedFunctionsProto() {
        FlinkFnApi.UserDefinedAggregateFunctions.Builder builder =
                FlinkFnApi.UserDefinedAggregateFunctions.newBuilder();
        builder.setMetricEnabled(config.get(PYTHON_METRIC_ENABLED));
        builder.setProfileEnabled(config.get(PYTHON_PROFILE_ENABLED));
        builder.addAllGrouping(Arrays.stream(grouping).boxed().collect(Collectors.toList()));
        builder.setGenerateUpdateBefore(generateUpdateBefore);
        builder.setIndexOfCountStar(indexOfCountStar);
        builder.setKeyType(toProtoType(getKeyType()));
        builder.setStateCacheSize(stateCacheSize);
        builder.setMapStateReadCacheSize(mapStateReadCacheSize);
        builder.setMapStateWriteCacheSize(mapStateWriteCacheSize);
        for (int i = 0; i < aggregateFunctions.length; i++) {
            DataViewSpec[] specs = null;
            if (i < dataViewSpecs.length) {
                specs = dataViewSpecs[i];
            }
            builder.addUdfs(
                    ProtoUtils.createUserDefinedAggregateFunctionProto(
                            aggregateFunctions[i], specs));
        }
        builder.addAllJobParameters(
                getRuntimeContext().getGlobalJobParameters().entrySet().stream()
                        .map(
                                entry ->
                                        FlinkFnApi.JobParameter.newBuilder()
                                                .setKey(entry.getKey())
                                                .setValue(entry.getValue())
                                                .build())
                        .collect(Collectors.toList()));
        return builder.build();
    }
}
