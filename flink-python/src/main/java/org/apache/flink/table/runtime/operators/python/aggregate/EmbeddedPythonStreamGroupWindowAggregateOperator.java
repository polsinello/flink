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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.functions.python.PythonAggregateFunctionInfo;
import org.apache.flink.table.runtime.dataview.DataViewSpec;
import org.apache.flink.table.runtime.groupwindow.NamedWindowProperty;
import org.apache.flink.table.runtime.groupwindow.ProctimeAttribute;
import org.apache.flink.table.runtime.groupwindow.RowtimeAttribute;
import org.apache.flink.table.runtime.groupwindow.WindowEnd;
import org.apache.flink.table.runtime.groupwindow.WindowProperty;
import org.apache.flink.table.runtime.groupwindow.WindowStart;
import org.apache.flink.table.runtime.operators.window.Window;
import org.apache.flink.table.runtime.operators.window.groupwindow.assigners.GroupWindowAssigner;
import org.apache.flink.table.runtime.typeutils.PythonTypeUtils;
import org.apache.flink.table.runtime.typeutils.serializers.python.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import pemja.core.object.PyIterator;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.time.ZoneId;

import static org.apache.flink.table.runtime.typeutils.PythonTypeUtils.toProtoType;
import static org.apache.flink.table.runtime.util.TimeWindowUtil.toEpochMills;
import static org.apache.flink.table.runtime.util.TimeWindowUtil.toEpochMillsForTimer;
import static org.apache.flink.table.runtime.util.TimeWindowUtil.toUtcTimestampMills;

/**
 * Embedded (thread-mode) Python group window aggregate operator. The embedded counterpart of
 * {@link PythonStreamGroupWindowAggregateOperator}: instead of serializing records to a Beam
 * runner over gRPC, it invokes the Python aggregate operation directly via pemja while passing the
 * Java {@code KeyedStateBackend} through so Python can read/write accumulator state.
 *
 * <p>Extends {@link AbstractEmbeddedStreamAggregateOperator} directly because window aggregates
 * use window-namespaced timers (via {@link InternalTimerService} parametrized on the window type),
 * not the VoidNamespace cleanup timers that {@link AbstractEmbeddedStreamGroupAggregateOperator}
 * provides.
 *
 * <p>Data flow:
 * <ul>
 *   <li>{@code processElement}: sends {@code (NORMAL_RECORD, row_tuple, null, watermark, null)} to
 *       Python. Python yields zero or more output records: {@code NORMAL_RECORD} entries are
 *       emitted downstream, and {@code TRIGGER_TIMER} entries cause Java to register/delete window
 *       timers on the keyed state backend.
 *   <li>{@code onEventTime}/{@code onProcessingTime}: sends
 *       {@code (TRIGGER_TIMER, null, timestamp, watermark, timer_tuple)} to Python. Only
 *       {@code NORMAL_RECORD} records are expected back.
 * </ul>
 */
@Internal
public class EmbeddedPythonStreamGroupWindowAggregateOperator<K, W extends Window>
        extends AbstractEmbeddedStreamAggregateOperator implements Triggerable<K, W> {

    private static final long serialVersionUID = 1L;

    @VisibleForTesting static final byte NORMAL_RECORD = 0;

    @VisibleForTesting static final byte TRIGGER_TIMER = 1;

    @VisibleForTesting static final byte REGISTER_EVENT_TIMER = 0;

    @VisibleForTesting static final byte REGISTER_PROCESSING_TIMER = 1;

    @VisibleForTesting static final byte DELETE_EVENT_TIMER = 2;

    @VisibleForTesting static final byte DELETE_PROCESSING_TIMER = 3;

    /** True if the count(*) agg is inserted by the planner. */
    private final boolean countStarInserted;

    /** Index of the time field in the input row (or -1 for processing-time windows). */
    private final int inputTimeFieldIndex;

    /** Allowed lateness for elements. */
    private final long allowedLateness;

    /** Shift timezone for window start/end property conversion. */
    private final ZoneId shiftTimeZone;

    /** Window assigner used for generating the window serializer and for the proto config. */
    private final GroupWindowAssigner<W> windowAssigner;

    /** Window type (tumbling / sliding / session). */
    private final FlinkFnApi.GroupWindow.WindowType windowType;

    /** True if row time (event time), false if processing time. */
    private final boolean isRowTime;

    /** True for time windows, false for count windows. */
    private final boolean isTimeWindow;

    /** Window size (tumbling / sliding); 0 for session. */
    private final long size;

    /** Window slide (sliding only); 0 otherwise. */
    private final long slide;

    /** Session window gap; 0 for tumbling / sliding. */
    private final long gap;

    /** Named window properties (WINDOW_START, WINDOW_END, etc.) appended to output rows. */
    private final FlinkFnApi.GroupWindow.WindowProperty[] namedProperties;

    /** Serializer for window namespaces used by timers; obtained from the window assigner. */
    private transient TypeSerializer<W> windowSerializer;

    /** Internal timer service keyed on window namespace. */
    private transient InternalTimerService<W> internalTimerService;

    /** Converters for key fields (RowData -> Python-compatible objects). */
    private transient PythonTypeUtils.DataConverter[] keyConverters;

    /** Serializer used to convert a GenericRowData key to BinaryRowData for setCurrentKey. */
    private transient RowDataSerializer keySerializer;

    /** Number of fields in the key row. */
    private transient int keyLength;

    public EmbeddedPythonStreamGroupWindowAggregateOperator(
            Configuration config,
            RowType inputType,
            RowType outputType,
            PythonAggregateFunctionInfo[] aggregateFunctions,
            DataViewSpec[][] dataViewSpecs,
            int[] grouping,
            int indexOfCountStar,
            boolean generateUpdateBefore,
            boolean countStarInserted,
            int inputTimeFieldIndex,
            GroupWindowAssigner<W> windowAssigner,
            FlinkFnApi.GroupWindow.WindowType windowType,
            boolean isRowTime,
            boolean isTimeWindow,
            long size,
            long slide,
            long gap,
            long allowedLateness,
            NamedWindowProperty[] namedProperties,
            ZoneId shiftTimeZone) {
        super(
                config,
                inputType,
                outputType,
                aggregateFunctions,
                dataViewSpecs,
                grouping,
                indexOfCountStar,
                generateUpdateBefore);
        this.countStarInserted = countStarInserted;
        this.inputTimeFieldIndex = inputTimeFieldIndex;
        this.windowAssigner = windowAssigner;
        this.windowType = windowType;
        this.isRowTime = isRowTime;
        this.isTimeWindow = isTimeWindow;
        this.size = size;
        this.slide = slide;
        this.gap = gap;
        this.allowedLateness = allowedLateness;
        this.shiftTimeZone = shiftTimeZone;

        this.namedProperties = new FlinkFnApi.GroupWindow.WindowProperty[namedProperties.length];
        for (int i = 0; i < namedProperties.length; i++) {
            WindowProperty namedProperty = namedProperties[i].getProperty();
            if (namedProperty instanceof WindowStart) {
                this.namedProperties[i] = FlinkFnApi.GroupWindow.WindowProperty.WINDOW_START;
            } else if (namedProperty instanceof WindowEnd) {
                this.namedProperties[i] = FlinkFnApi.GroupWindow.WindowProperty.WINDOW_END;
            } else if (namedProperty instanceof RowtimeAttribute) {
                this.namedProperties[i] = FlinkFnApi.GroupWindow.WindowProperty.ROW_TIME_ATTRIBUTE;
            } else if (namedProperty instanceof ProctimeAttribute) {
                this.namedProperties[i] = FlinkFnApi.GroupWindow.WindowProperty.PROC_TIME_ATTRIBUTE;
            } else {
                throw new RuntimeException("Unexpected property " + namedProperty);
            }
        }
    }

    // The static factories below are invoked via reflection from
    // StreamExecPythonGroupWindowAggregate — their signatures must stay in lockstep with the
    // matching createXxxGroupWindowAggregateOperator methods on PythonStreamGroupWindowAggregateOperator.

    public static <K, W extends Window>
            EmbeddedPythonStreamGroupWindowAggregateOperator<K, W>
                    createTumblingGroupWindowAggregateOperator(
                            Configuration config,
                            RowType inputType,
                            RowType outputType,
                            PythonAggregateFunctionInfo[] aggregateFunctions,
                            DataViewSpec[][] dataViewSpecs,
                            int[] grouping,
                            int indexOfCountStar,
                            boolean generateUpdateBefore,
                            boolean countStarInserted,
                            int inputTimeFieldIndex,
                            GroupWindowAssigner<W> windowAssigner,
                            boolean isRowTime,
                            boolean isTimeWindow,
                            long size,
                            long allowedLateness,
                            NamedWindowProperty[] namedProperties,
                            ZoneId shiftTimeZone) {
        return new EmbeddedPythonStreamGroupWindowAggregateOperator<>(
                config,
                inputType,
                outputType,
                aggregateFunctions,
                dataViewSpecs,
                grouping,
                indexOfCountStar,
                generateUpdateBefore,
                countStarInserted,
                inputTimeFieldIndex,
                windowAssigner,
                FlinkFnApi.GroupWindow.WindowType.TUMBLING_GROUP_WINDOW,
                isRowTime,
                isTimeWindow,
                size,
                0,
                0,
                allowedLateness,
                namedProperties,
                shiftTimeZone);
    }

    public static <K, W extends Window>
            EmbeddedPythonStreamGroupWindowAggregateOperator<K, W>
                    createSlidingGroupWindowAggregateOperator(
                            Configuration config,
                            RowType inputType,
                            RowType outputType,
                            PythonAggregateFunctionInfo[] aggregateFunctions,
                            DataViewSpec[][] dataViewSpecs,
                            int[] grouping,
                            int indexOfCountStar,
                            boolean generateUpdateBefore,
                            boolean countStarInserted,
                            int inputTimeFieldIndex,
                            GroupWindowAssigner<W> windowAssigner,
                            boolean isRowTime,
                            boolean isTimeWindow,
                            long size,
                            long slide,
                            long allowedLateness,
                            NamedWindowProperty[] namedProperties,
                            ZoneId shiftTimeZone) {
        return new EmbeddedPythonStreamGroupWindowAggregateOperator<>(
                config,
                inputType,
                outputType,
                aggregateFunctions,
                dataViewSpecs,
                grouping,
                indexOfCountStar,
                generateUpdateBefore,
                countStarInserted,
                inputTimeFieldIndex,
                windowAssigner,
                FlinkFnApi.GroupWindow.WindowType.SLIDING_GROUP_WINDOW,
                isRowTime,
                isTimeWindow,
                size,
                slide,
                0,
                allowedLateness,
                namedProperties,
                shiftTimeZone);
    }

    public static <K, W extends Window>
            EmbeddedPythonStreamGroupWindowAggregateOperator<K, W>
                    createSessionGroupWindowAggregateOperator(
                            Configuration config,
                            RowType inputType,
                            RowType outputType,
                            PythonAggregateFunctionInfo[] aggregateFunctions,
                            DataViewSpec[][] dataViewSpecs,
                            int[] grouping,
                            int indexOfCountStar,
                            boolean generateUpdateBefore,
                            boolean countStarInserted,
                            int inputTimeFieldIndex,
                            GroupWindowAssigner<W> windowAssigner,
                            boolean isRowTime,
                            long gap,
                            long allowedLateness,
                            NamedWindowProperty[] namedProperties,
                            ZoneId shiftTimeZone) {
        return new EmbeddedPythonStreamGroupWindowAggregateOperator<>(
                config,
                inputType,
                outputType,
                aggregateFunctions,
                dataViewSpecs,
                grouping,
                indexOfCountStar,
                generateUpdateBefore,
                countStarInserted,
                inputTimeFieldIndex,
                windowAssigner,
                FlinkFnApi.GroupWindow.WindowType.SESSION_GROUP_WINDOW,
                isRowTime,
                true,
                0,
                0,
                gap,
                allowedLateness,
                namedProperties,
                shiftTimeZone);
    }

    @Override
    public void open() throws Exception {
        windowSerializer = windowAssigner.getWindowSerializer(new ExecutionConfig());
        internalTimerService = getInternalTimerService("window-timers", windowSerializer, this);

        super.open();

        RowType keyType = getKeyType();
        keyLength = keyType.getFieldCount();
        keyConverters =
                keyType.getFields().stream()
                        .map(RowType.RowField::getType)
                        .map(PythonTypeUtils::toDataConverter)
                        .toArray(PythonTypeUtils.DataConverter[]::new);
        // Build an internal RowData serializer for the key type (used to convert the
        // Python-delivered key tuple back to BinaryRowData when Java registers a timer).
        keySerializer = (RowDataSerializer) PythonTypeUtils.toInternalSerializer(keyType);
    }

    @Override
    public void openPythonInterpreter() {
        interpreter.exec(
                "from pyflink.fn_execution.embedded.operation_utils import "
                        + getPythonFactoryFunctionName());
        interpreter.set("proto", getUserDefinedFunctionsProto().toByteArray());
        interpreter.set("keyed_state_backend", getKeyedStateBackend());
        // Pass the input row schema so Python can build field-level converters for
        // temporal types (e.g. TIMESTAMP_LTZ rowtime -> Python datetime). Without this
        // the rowtime field arrives as a pemja PyJObject wrapping a java.time.Instant
        // and GroupWindowAggFunction.process_element fails reading it.
        // toProtoType(rowType) returns a FieldType wrapping a Schema; unwrap via
        // getRowSchema() so Python parses directly as FlinkFnApi.Schema.
        interpreter.set(
                "input_type_proto", toProtoType(inputType).getRowSchema().toByteArray());
        // The Python state backend re-acquires per-window state via
        // getPartitionedState(windowObj, windowSerializer, descriptor); pass the
        // serializer so Python can bind state to the right namespace.
        interpreter.set("j_window_namespace_serializer", windowSerializer);

        RowType keyType = getKeyType();
        String[] keyFieldTypeNames = new String[keyType.getFieldCount()];
        for (int i = 0; i < keyType.getFieldCount(); i++) {
            keyFieldTypeNames[i] = keyType.getTypeAt(i).toString();
        }
        interpreter.set("key_row_type_info", keyFieldTypeNames);

        // See AbstractEmbeddedStreamGroupAggregateOperator for context — window
        // aggregates have the same GenericRowData vs BinaryRowData hashCode
        // divergence problem when setCurrentKey is called on the Java backend.
        interpreter.set(
                "j_key_row_serializer",
                new org.apache.flink.table.runtime.typeutils.RowDataSerializer(keyType));

        interpreter.exec(
                String.format(
                        "operation = %s(proto, keyed_state_backend, "
                                + "key_row_type_info, input_type_proto, "
                                + "j_window_namespace_serializer, j_key_row_serializer)",
                        getPythonFactoryFunctionName()));
        interpreter.invokeMethod("operation", "open");
    }

    @Override
    public void processElement(StreamRecord<RowData> element) throws Exception {
        RowData value = element.getValue();

        Object[] rowTuple = new Object[inputType.getFieldCount() + 1];
        rowTuple[0] = value.getRowKind().toByteValue();
        for (int i = 0; i < inputType.getFieldCount(); i++) {
            rowTuple[i + 1] = inputConverters[i].toExternal(value, i);
        }

        Object[] args =
                new Object[] {
                    NORMAL_RECORD, rowTuple, null, internalTimerService.currentWatermark(), null
                };

        PyIterator results =
                (PyIterator)
                        interpreter.invokeMethod("operation", "process_element", (Object) args);
        emitResults(results);

        elementCount++;
        checkInvokeFinishBundleByCount();
    }

    @Override
    public void onEventTime(InternalTimer<K, W> timer) throws Exception {
        fireTimer(timer, REGISTER_EVENT_TIMER);
    }

    @Override
    public void onProcessingTime(InternalTimer<K, W> timer) throws Exception {
        fireTimer(timer, REGISTER_PROCESSING_TIMER);
    }

    private void fireTimer(InternalTimer<K, W> timer, byte timerType) throws Exception {
        RowData key = (RowData) timer.getKey();
        W window = timer.getNamespace();
        long timestamp = toUtcTimestampMills(timer.getTimestamp(), shiftTimeZone);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        windowSerializer.serialize(window, new DataOutputViewStreamWrapper(baos));
        byte[] encodedNamespace = baos.toByteArray();

        Object[] keyTuple = new Object[keyLength];
        for (int i = 0; i < keyLength; i++) {
            keyTuple[i] = keyConverters[i].toExternal(key, i);
        }

        Object[] timerTuple = new Object[] {timerType, keyTuple, encodedNamespace};

        Object[] args =
                new Object[] {
                    TRIGGER_TIMER,
                    null,
                    timestamp,
                    internalTimerService.currentWatermark(),
                    timerTuple
                };

        PyIterator results =
                (PyIterator)
                        interpreter.invokeMethod("operation", "process_element", (Object) args);
        emitResults(results);

        elementCount++;
        checkInvokeFinishBundleByCount();
    }

    @SuppressWarnings("unchecked")
    private void emitResults(PyIterator results) throws Exception {
        if (results == null) {
            return;
        }
        try {
            int propOffset = outputType.getFieldCount() - namedProperties.length;
            while (results.hasNext()) {
                Object[] result = (Object[]) results.next();
                // result: (record_type, row_kind_byte, field_values_or_None, timer_tuple_or_None)
                byte recordType = ((Number) result[0]).byteValue();
                if (recordType == NORMAL_RECORD) {
                    byte rowKindByte = ((Number) result[1]).byteValue();
                    Object[] fieldValues = (Object[]) result[2];
                    for (int i = 0; i < outputType.getFieldCount(); i++) {
                        if (i >= propOffset) {
                            FlinkFnApi.GroupWindow.WindowProperty prop =
                                    namedProperties[i - propOffset];
                            long epochMillis = ((Number) fieldValues[i]).longValue();
                            if (prop == FlinkFnApi.GroupWindow.WindowProperty.WINDOW_START
                                    || prop
                                            == FlinkFnApi.GroupWindow.WindowProperty.WINDOW_END) {
                                reuseResultRowData.setField(
                                        i, TimestampData.fromEpochMillis(epochMillis));
                            } else {
                                // ROW_TIME_ATTRIBUTE or PROC_TIME_ATTRIBUTE
                                reuseResultRowData.setField(
                                        i,
                                        TimestampData.fromEpochMillis(
                                                toEpochMills(epochMillis, shiftTimeZone)));
                            }
                        } else {
                            reuseResultRowData.setField(
                                    i, outputConverters[i].toInternal(fieldValues[i]));
                        }
                    }
                    reuseResultRowData.setRowKind(RowKind.fromByteValue(rowKindByte));
                    rowDataWrapper.collect(reuseResultRowData);
                } else {
                    // TRIGGER_TIMER: Java must register / delete a window timer
                    Object[] timerTuple = (Object[]) result[3];
                    byte timerOperandType = ((Number) timerTuple[0]).byteValue();
                    Object[] keyTuple = (Object[]) timerTuple[1];
                    long timerTimestamp = ((Number) timerTuple[2]).longValue();
                    byte[] encodedNs = (byte[]) timerTuple[3];

                    W window =
                            windowSerializer.deserialize(
                                    new DataInputViewStreamWrapper(
                                            new ByteArrayInputStream(encodedNs)));

                    GenericRowData genericKey = new GenericRowData(keyTuple.length);
                    for (int i = 0; i < keyTuple.length; i++) {
                        genericKey.setField(i, keyConverters[i].toInternal(keyTuple[i]));
                    }
                    BinaryRowData binaryKey = keySerializer.toBinaryRow(genericKey).copy();

                    long shiftedTs = toEpochMillsForTimer(timerTimestamp, shiftTimeZone);
                    synchronized (getKeyedStateBackend()) {
                        setCurrentKey(binaryKey);
                        if (timerOperandType == REGISTER_EVENT_TIMER) {
                            internalTimerService.registerEventTimeTimer(window, shiftedTs);
                        } else if (timerOperandType == REGISTER_PROCESSING_TIMER) {
                            internalTimerService.registerProcessingTimeTimer(window, shiftedTs);
                        } else if (timerOperandType == DELETE_EVENT_TIMER) {
                            internalTimerService.deleteEventTimeTimer(window, shiftedTs);
                        } else if (timerOperandType == DELETE_PROCESSING_TIMER) {
                            internalTimerService.deleteProcessingTimeTimer(window, shiftedTs);
                        } else {
                            throw new RuntimeException(
                                    String.format(
                                            "Unsupported timerOperandType %s.",
                                            timerOperandType));
                        }
                    }
                }
            }
        } finally {
            results.close();
        }
    }

    @Override
    protected void invokeFinishBundle() throws Exception {
        // Window aggregate results are emitted inline with each processElement / timer fire, so
        // there is nothing to flush here. Keep bundle counters consistent with the base infra.
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

    @Override
    protected String getPythonFactoryFunctionName() {
        return "create_group_window_aggregate_operation_from_proto";
    }

    @Override
    protected FlinkFnApi.UserDefinedAggregateFunctions getUserDefinedFunctionsProto() {
        FlinkFnApi.UserDefinedAggregateFunctions.Builder builder =
                super.getUserDefinedFunctionsProto().toBuilder();
        builder.setCountStarInserted(countStarInserted);

        FlinkFnApi.GroupWindow.Builder windowBuilder = FlinkFnApi.GroupWindow.newBuilder();
        windowBuilder.setWindowType(windowType);
        windowBuilder.setIsTimeWindow(isTimeWindow);
        windowBuilder.setIsRowTime(isRowTime);
        windowBuilder.setTimeFieldIndex(inputTimeFieldIndex);
        windowBuilder.setWindowSize(size);
        windowBuilder.setWindowSlide(slide);
        windowBuilder.setWindowGap(gap);
        windowBuilder.setAllowedLateness(allowedLateness);
        for (FlinkFnApi.GroupWindow.WindowProperty prop : namedProperties) {
            windowBuilder.addNamedProperties(prop);
        }
        windowBuilder.setShiftTimezone(shiftTimeZone.getId());
        builder.setGroupWindow(windowBuilder);
        return builder.build();
    }
}
