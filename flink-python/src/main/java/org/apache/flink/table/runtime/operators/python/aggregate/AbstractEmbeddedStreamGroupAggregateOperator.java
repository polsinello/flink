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
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.SimpleTimerService;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.python.PythonAggregateFunctionInfo;
import org.apache.flink.table.runtime.dataview.DataViewSpec;
import org.apache.flink.table.runtime.functions.CleanupState;
import org.apache.flink.table.runtime.typeutils.PythonTypeUtils;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;

/**
 * Base class for embedded Python group aggregate and group table aggregate operators. Adds timer
 * support for state cleanup and keyed state management on top of {@link
 * AbstractEmbeddedStreamAggregateOperator}.
 */
@Internal
public abstract class AbstractEmbeddedStreamGroupAggregateOperator
        extends AbstractEmbeddedStreamAggregateOperator
        implements Triggerable<RowData, VoidNamespace>, CleanupState {

    private static final long serialVersionUID = 1L;

    /** The minimum time in milliseconds until state which was not updated will be retained. */
    private final long minRetentionTime;

    /** The maximum time in milliseconds until state which was not updated will be retained. */
    private final long maxRetentionTime;

    /**
     * Indicates whether state cleaning is enabled. Can be calculated from the {@code
     * minRetentionTime}.
     */
    private final boolean stateCleaningEnabled;

    private transient TimerService timerService;

    /** Holds the latest registered cleanup timer. */
    private transient ValueState<Long> cleanupTimeState;

    /** Converters for key fields (RowData field -> Python-compatible object). */
    private transient PythonTypeUtils.DataConverter[] keyConverters;

    public AbstractEmbeddedStreamGroupAggregateOperator(
            Configuration config,
            RowType inputType,
            RowType outputType,
            PythonAggregateFunctionInfo[] aggregateFunctions,
            DataViewSpec[][] dataViewSpecs,
            int[] grouping,
            int indexOfCountStar,
            boolean generateUpdateBefore,
            long minRetentionTime,
            long maxRetentionTime) {
        super(
                config,
                inputType,
                outputType,
                aggregateFunctions,
                dataViewSpecs,
                grouping,
                indexOfCountStar,
                generateUpdateBefore);
        this.minRetentionTime = minRetentionTime;
        this.maxRetentionTime = maxRetentionTime;
        this.stateCleaningEnabled = minRetentionTime > 1;
    }

    @Override
    public void open() throws Exception {
        InternalTimerService<VoidNamespace> internalTimerService =
                getInternalTimerService(
                        "state-clean-timer", VoidNamespaceSerializer.INSTANCE, this);
        timerService = new SimpleTimerService(internalTimerService);
        initCleanupTimeState();

        RowType keyType = getKeyType();
        keyConverters =
                keyType.getFields().stream()
                        .map(RowType.RowField::getType)
                        .map(PythonTypeUtils::toDataConverter)
                        .toArray(PythonTypeUtils.DataConverter[]::new);

        super.open();
    }

    @Override
    public void openPythonInterpreter() {
        interpreter.exec(
                "from pyflink.fn_execution.embedded.operation_utils import "
                        + getPythonFactoryFunctionName());
        interpreter.set("proto", getUserDefinedFunctionsProto().toByteArray());
        interpreter.set("keyed_state_backend", getKeyedStateBackend());

        RowType keyType = getKeyType();
        String[] keyFieldTypeNames = new String[keyType.getFieldCount()];
        for (int i = 0; i < keyType.getFieldCount(); i++) {
            keyFieldTypeNames[i] = keyType.getTypeAt(i).toString();
        }
        interpreter.set("key_row_type_info", keyFieldTypeNames);

        // RowDataSerializer for the key row type. Python-side set_current_key calls
        // `.toBinaryRow(generic_row)` on this to get a BinaryRowData whose hashCode
        // matches the upstream shuffle's binary key — otherwise GenericRowData's
        // Arrays.hashCode diverges from BinaryRowData's binary-buffer hash and the
        // KeyedStateBackend's key-group check fails at runtime.
        interpreter.set(
                "j_key_row_serializer",
                new RowDataSerializer(keyType));

        // Input RowType schema proto — Python builds per-field converters from it so
        // nested ROW fields arrive in user UDAF `accumulate` as pyflink.table.Row
        // (attribute access), temporal PyJObjects become native datetime/date/time, etc.
        interpreter.set(
                "input_type_proto",
                org.apache.flink.table.runtime.typeutils.PythonTypeUtils
                        .toProtoType(inputType)
                        .getRowSchema()
                        .toByteArray());

        // Output RowType schema proto — symmetric to input. Python yields Row objects
        // (with nested Row fields); this proto drives per-field to_external conversion
        // so nested rows come back to Java in the `[row_kind, [fields]]` shape that
        // PythonTypeUtils.RowDataConverter.toInternalImpl expects (it casts the second
        // slot to Object[] and field-by-field deserialises). Without this the Java side
        // raises ClassCastException: PyObject cannot be cast to [Ljava/lang/Object;.
        interpreter.set(
                "output_type_proto",
                org.apache.flink.table.runtime.typeutils.PythonTypeUtils
                        .toProtoType(outputType)
                        .getRowSchema()
                        .toByteArray());

        interpreter.exec(
                String.format(
                        "operation = %s(proto, keyed_state_backend, key_row_type_info, "
                                + "j_key_row_serializer, input_type_proto, output_type_proto)",
                        getPythonFactoryFunctionName()));
        interpreter.invokeMethod("operation", "open");
    }

    @SuppressWarnings("unchecked")
    @Override
    public void processElement(StreamRecord<RowData> element) throws Exception {
        long currentTime = timerService.currentProcessingTime();
        registerProcessingCleanupTimer(currentTime);
        super.processElement(element);
    }

    /** Invoked when an event-time timer fires. */
    @Override
    public void onEventTime(InternalTimer<RowData, VoidNamespace> timer) {}

    /** Invoked when a processing-time timer fires. */
    @SuppressWarnings("unchecked")
    @Override
    public void onProcessingTime(InternalTimer<RowData, VoidNamespace> timer) throws Exception {
        if (stateCleaningEnabled) {
            RowData key = timer.getKey();
            Object[] keyArgs = new Object[getKeyType().getFieldCount()];
            for (int i = 0; i < keyArgs.length; i++) {
                keyArgs[i] = keyConverters[i].toExternal(key, i);
            }
            interpreter.invokeMethod("operation", "on_timer", (Object) keyArgs);
            elementCount++;
            checkInvokeFinishBundleByCount();
        }
    }

    @Override
    protected FlinkFnApi.UserDefinedAggregateFunctions getUserDefinedFunctionsProto() {
        FlinkFnApi.UserDefinedAggregateFunctions.Builder builder =
                super.getUserDefinedFunctionsProto().toBuilder();
        builder.setStateCleaningEnabled(stateCleaningEnabled);
        return builder.build();
    }

    private void initCleanupTimeState() {
        if (stateCleaningEnabled) {
            ValueStateDescriptor<Long> inputCntDescriptor =
                    new ValueStateDescriptor<>("PythonAggregateCleanupTime", Types.LONG);
            cleanupTimeState = getRuntimeContext().getState(inputCntDescriptor);
        }
    }

    private void registerProcessingCleanupTimer(long currentTime) throws Exception {
        if (stateCleaningEnabled) {
            synchronized (getKeyedStateBackend()) {
                getKeyedStateBackend().setCurrentKey(getCurrentKey());
                registerProcessingCleanupTimer(
                        cleanupTimeState,
                        currentTime,
                        minRetentionTime,
                        maxRetentionTime,
                        timerService);
            }
        }
    }
}
