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

package org.apache.flink.table.runtime.operators.python.processtable;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.operators.python.process.timer.TimerHandler;
import org.apache.flink.streaming.api.operators.python.process.timer.TimerUtils;
import org.apache.flink.table.runtime.operators.python.utils.StreamRecordRowDataWrappingCollector;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.RowRowConverter;
import org.apache.flink.table.functions.python.PythonEnv;
import org.apache.flink.table.runtime.operators.python.AbstractOneInputPythonFunctionOperator;
import org.apache.flink.table.runtime.runners.python.beam.BeamProcessTablePythonFunctionRunner;
import org.apache.flink.table.runtime.typeutils.PythonTypeUtils;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.flink.python.util.ProtoUtils.createFlattenRowTypeCoderInfoDescriptorProto;
import static org.apache.flink.python.util.ProtoUtils.createRowTypeCoderInfoDescriptorProto;

/**
 * Single-input runtime operator for a Python {@link
 * org.apache.flink.table.functions.ProcessTableFunction}. Handles ROW_SEMANTIC_TABLE and
 * single-table SET_SEMANTIC_TABLE PTFs; multi-table-arg PTFs use {@link
 * PythonMultiInputProcessTableOperator}.
 *
 * <p>Implements {@link Triggerable Triggerable&lt;RowData, String&gt;} to receive event-time timer
 * fires keyed on the partition key (when SET_SEMANTIC) and namespaced by timer name. Hidden state
 * ({@code __pyflink_ptf_named_timers__}, {@code __pyflink_ptf_unnamed_timers__}) implements the
 * "named timer replace-on-register" semantics; see {@link PtfTimerRegistration}.
 */
@Internal
public class PythonProcessTableOperator
        extends AbstractOneInputPythonFunctionOperator<RowData, RowData>
        implements Triggerable<RowData, String> {

    private static final long serialVersionUID = 1L;

    /** Wire-format field count for {@code PtfInputRow}; mirror of the Python side. */
    private static final int PTF_INPUT_FIELD_COUNT = 6;

    private static final int PTF_INPUT_ARG_ID = 0;
    private static final int PTF_INPUT_ROW_KIND = 1;
    private static final int PTF_INPUT_EVENT_TIME = 2;
    private static final int PTF_INPUT_WATERMARK = 3;
    private static final int PTF_INPUT_PARTITION_KEY = 4;
    private static final int PTF_INPUT_USER_ROW = 5;

    private static final MapStateDescriptor<String, Long> NAMED_TIMERS_STATE_DESCRIPTOR =
            new MapStateDescriptor<>(
                    "__pyflink_ptf_named_timers__",
                    StringSerializer.INSTANCE,
                    LongSerializer.INSTANCE);

    private static final ListStateDescriptor<Long> UNNAMED_TIMERS_STATE_DESCRIPTOR =
            new ListStateDescriptor<>("__pyflink_ptf_unnamed_timers__", LongSerializer.INSTANCE);

    // ------------------------------------------------------------------------
    // Configuration (set by constructor)
    // ------------------------------------------------------------------------

    private final FlinkFnApi.UserDefinedProcessTableFunction ptfSpec;
    private final RowType inputRowType;
    private final RowType outputRowType;

    /** True if any table arg is SET_SEMANTIC; drives keyed-state usage. */
    private final boolean isKeyed;

    // ------------------------------------------------------------------------
    // Transient runtime state
    // ------------------------------------------------------------------------

    private transient TypeSerializer<RowData> ptfInputRowSerializer;
    private transient TypeSerializer<RowData> outputRowSerializer;
    private transient TypeSerializer<RowData> partitionKeySerializer;
    private transient RowType ptfInputRowType;
    private transient RowType partitionKeyRowType;
    private transient RowData.FieldGetter[] partitionKeyFieldGetters;

    /**
     * Field getter for the {@code on_time} column on the user row, or {@code null} when the PTF
     * has no on_time. Returns a value in milliseconds (TIMESTAMP_LTZ is stored as TimestampData,
     * which we unwrap below).
     */
    @Nullable private transient RowData.FieldGetter onTimeFieldGetter;

    /** Index of the on_time column, -1 when absent. Cached for the TimestampData unwrap. */
    private transient int onTimeColumn;

    /** Reusable byte buffers for serializing input rows and deserializing output rows. */
    protected transient ByteArrayInputStreamWithPos bais;

    protected transient DataInputViewStreamWrapper baisWrapper;
    protected transient ByteArrayOutputStreamWithPos baos;
    protected transient DataOutputViewStreamWrapper baosWrapper;

    /** Output channel; collects deserialized RowData into the downstream operator. */
    private transient StreamRecordRowDataWrappingCollector rowDataWrapper;

    /** Reusable PtfInputRow scratch object. */
    private transient GenericRowData reusableInputRow;

    /** Internal timer service keyed by the partition-key RowData; namespace is the timer name. */
    @Nullable private transient InternalTimerService<String> internalTimerService;

    /** Hidden bookkeeping state for named timers (per partition key). */
    @Nullable private transient MapState<String, Long> namedTimers;

    /** Hidden bookkeeping state for unnamed timer timestamps (per partition key). */
    @Nullable private transient ListState<Long> unnamedTimers;

    // ----- timer-side-channel scaffolding (only populated when isKeyed) ----- //

    /**
     * Type-info of the partition key as a legacy {@link RowTypeInfo}. The Python timer
     * transport ships keys via the legacy {@code Row} format (DataStream-API convention);
     * we convert to/from {@code RowData} at the registration boundary.
     */
    @Nullable private transient RowTypeInfo partitionKeyRowTypeInfo;

    /** Wire-format serializer for the 5-field timer-data Row (op, watermark, ts, key, ns). */
    @Nullable private transient TypeSerializer<Row> timerDataSerializer;

    /** Type-info for the timer-data Row, used to build the Beam coder descriptor. */
    @Nullable private transient TypeInformation<Row> timerDataTypeInfo;

    /** Builds 5-field Row timer payloads for forwarding to Python. */
    @Nullable private transient TimerHandler timerHandler;

    /** Bridges {@code Row} (wire) <-> {@code RowData} (state-backend key) at registration. */
    @Nullable private transient RowRowConverter partitionKeyConverter;

    // ------------------------------------------------------------------------

    public PythonProcessTableOperator(
            Configuration config,
            FlinkFnApi.UserDefinedProcessTableFunction ptfSpec,
            RowType inputRowType,
            RowType outputRowType,
            boolean isKeyed) {
        super(config);
        this.ptfSpec = ptfSpec;
        this.inputRowType = inputRowType;
        this.outputRowType = outputRowType;
        this.isKeyed = isKeyed;
    }

    // ------------------------------------------------------------------------
    // Lifecycle
    // ------------------------------------------------------------------------

    @Override
    public void open() throws Exception {
        // Build partition-key projection metadata first; it determines field 4
        // type of the wire-format ptfInputRowType.
        if (isKeyed) {
            final int[] partitionCols = extractPartitionColumns(ptfSpec);
            partitionKeyRowType = (RowType) Projection.of(partitionCols).project(inputRowType);
            partitionKeySerializer = PythonTypeUtils.toInternalSerializer(partitionKeyRowType);
            partitionKeyFieldGetters = new RowData.FieldGetter[partitionCols.length];
            for (int i = 0; i < partitionCols.length; i++) {
                partitionKeyFieldGetters[i] =
                        RowData.createFieldGetter(
                                inputRowType.getTypeAt(partitionCols[i]), partitionCols[i]);
            }
        }

        // Wire-format ptfInputRowType: field 4 (partition_key) carries the
        // projected partition-key shape, not the full user row shape.
        final LogicalType partitionKeyField =
                isKeyed ? partitionKeyRowType.copy(true) : inputRowType.copy(true);

        ptfInputRowType =
                RowType.of(
                        new LogicalType[] {
                            new IntType(false), // arg_id
                            new TinyIntType(false), // row_kind
                            new BigIntType(false), // event_time_ms
                            new BigIntType(false), // watermark_ms
                            partitionKeyField, // partition_key
                            inputRowType.copy(false) // user_row
                        },
                        new String[] {
                            "arg_id", "row_kind", "event_time", "watermark", "partition_key",
                            "user_row"
                        });
        ptfInputRowSerializer = PythonTypeUtils.toInternalSerializer(ptfInputRowType);
        outputRowSerializer = PythonTypeUtils.toInternalSerializer(outputRowType);

        if (isKeyed) {
            internalTimerService =
                    getInternalTimerService(
                            "ptf-user-timers",
                            Utf8LengthPrefixedStringSerializer.INSTANCE,
                            this);
            namedTimers = getRuntimeContext().getMapState(NAMED_TIMERS_STATE_DESCRIPTOR);
            unnamedTimers = getRuntimeContext().getListState(UNNAMED_TIMERS_STATE_DESCRIPTOR);

            // Timer side channel: the worker emits timer-set requests over Beam's userstate.Timer
            // transport; PtfTimerRegistration decodes them and converts the wire Row key to RowData.
            final DataType partitionKeyDataType =
                    TypeConversions.fromLogicalToDataType(partitionKeyRowType);
            partitionKeyConverter = RowRowConverter.create(partitionKeyDataType);
            partitionKeyConverter.open(
                    getContainingTask().getEnvironment().getUserCodeClassLoader().asClassLoader());
            partitionKeyRowTypeInfo =
                    (RowTypeInfo) TypeConversions.fromDataTypeToLegacyInfo(partitionKeyDataType);
            timerDataTypeInfo = TimerUtils.createTimerDataTypeInfo(partitionKeyRowTypeInfo);
            timerDataSerializer =
                    org.apache.flink.streaming.api.utils.PythonTypeUtils
                            .TypeInfoToSerializerConverter
                            .typeInfoSerializerConverter(timerDataTypeInfo);
            timerHandler = new TimerHandler();
        }

        // on_time column from the table arg's on_time_column (-1 when unbound); the worker's
        // TimeContext.time() reads it.
        onTimeColumn = extractOnTimeColumn(ptfSpec);
        if (onTimeColumn >= 0) {
            onTimeFieldGetter =
                    RowData.createFieldGetter(inputRowType.getTypeAt(onTimeColumn), onTimeColumn);
        }

        reusableInputRow = new GenericRowData(PTF_INPUT_FIELD_COUNT);
        rowDataWrapper = new StreamRecordRowDataWrappingCollector(output);

        bais = new ByteArrayInputStreamWithPos();
        baisWrapper = new DataInputViewStreamWrapper(bais);
        baos = new ByteArrayOutputStreamWithPos();
        baosWrapper = new DataOutputViewStreamWrapper(baos);

        super.open();
    }

    // ------------------------------------------------------------------------
    // OneInputStreamOperator
    // ------------------------------------------------------------------------

    @Override
    public void processElement(StreamRecord<RowData> element) throws Exception {
        final RowData userRow = element.getValue();
        reusableInputRow.setRowKind(RowKind.INSERT);
        reusableInputRow.setField(PTF_INPUT_ARG_ID, 0);
        reusableInputRow.setField(PTF_INPUT_ROW_KIND, encodeRowKind(userRow.getRowKind()));
        reusableInputRow.setField(PTF_INPUT_EVENT_TIME, extractEventTimeMs(element, userRow));
        reusableInputRow.setField(
                PTF_INPUT_WATERMARK,
                internalTimerService != null
                        ? internalTimerService.currentWatermark()
                        : Long.MIN_VALUE);
        // Project the partition key from the user row and forward it explicitly:
        // the Python worker's state proxies set_current_key() from this field, so
        // a null here would leave the worker's keyed-state backend with no key.
        reusableInputRow.setField(PTF_INPUT_PARTITION_KEY, projectPartitionKey(userRow));
        reusableInputRow.setField(PTF_INPUT_USER_ROW, userRow);

        ptfInputRowSerializer.serialize((RowData) reusableInputRow, baosWrapper);
        pythonFunctionRunner.process(baos.toByteArray());
        baos.reset();

        elementCount++;
        checkInvokeFinishBundleByCount();
        emitResults();
    }

    // ------------------------------------------------------------------------
    // Triggerable
    // ------------------------------------------------------------------------

    @Override
    public void onEventTime(InternalTimer<RowData, String> timer) throws Exception {
        forwardTimerFire(timer, /* processingTime */ false);
    }

    @Override
    public void onProcessingTime(InternalTimer<RowData, String> timer) throws Exception {
        forwardTimerFire(timer, /* processingTime */ true);
    }

    private void forwardTimerFire(
            InternalTimer<RowData, String> timer, boolean processingTime)
            throws Exception {
        if (timerHandler == null
                || timerDataSerializer == null
                || partitionKeyConverter == null) {
            // Non-keyed PTF — timers aren't supported. The user-facing API rejects
            // register_on_time on ROW_SEMANTIC PTFs, so reaching here would be a bug.
            throw new IllegalStateException(
                    "Timer fired on a non-keyed PTF; this should not happen.");
        }

        // The namespace is the timer name (empty for unnamed timers). Encode using the same
        // length-prefixed UTF-8 wire format the registration path consumes, so the bytes
        // round-trip Java -> Python -> ``OnTimerContext.current_timer()``.
        final String timerName = timer.getNamespace();
        final byte[] encodedNamespace;
        if (timerName == null || timerName.isEmpty()) {
            encodedNamespace = null;
        } else {
            // 4-byte big-endian length + UTF-8 bytes. Mirrors
            // Utf8LengthPrefixedStringSerializer.serialize.
            final byte[] utf8 = timerName.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            encodedNamespace = new byte[4 + utf8.length];
            encodedNamespace[0] = (byte) ((utf8.length >>> 24) & 0xff);
            encodedNamespace[1] = (byte) ((utf8.length >>> 16) & 0xff);
            encodedNamespace[2] = (byte) ((utf8.length >>> 8) & 0xff);
            encodedNamespace[3] = (byte) (utf8.length & 0xff);
            System.arraycopy(utf8, 0, encodedNamespace, 4, utf8.length);
        }

        // Convert RowData partition key -> Row (legacy timer wire format).
        final Row externalKey = partitionKeyConverter.toExternal(timer.getKey());

        final Row timerData =
                timerHandler.buildTimerData(
                        processingTime ? TimeDomain.PROCESSING_TIME : TimeDomain.EVENT_TIME,
                        internalTimerService != null
                                ? internalTimerService.currentWatermark()
                                : Long.MIN_VALUE,
                        timer.getTimestamp(),
                        externalKey,
                        encodedNamespace);
        timerDataSerializer.serialize(timerData, baosWrapper);
        pythonFunctionRunner.processTimer(baos.toByteArray());
        baos.reset();

        elementCount++;
        checkInvokeFinishBundleByCount();
        emitResults();
    }

    // ------------------------------------------------------------------------
    // Beam runner construction
    // ------------------------------------------------------------------------

    @Override
    public PythonEnv getPythonEnv() {
        return new PythonEnv(PythonEnv.ExecType.PROCESS);
    }

    @Override
    public PythonFunctionRunner createPythonFunctionRunner() throws Exception {
        @SuppressWarnings({"unchecked", "rawtypes"})
        final org.apache.flink.runtime.state.KeyedStateBackend<RowData> rowDataKeyedBackend =
                isKeyed
                        ? (org.apache.flink.runtime.state.KeyedStateBackend)
                                getKeyedStateBackend()
                        : null;
        @SuppressWarnings("rawtypes")
        final org.apache.flink.streaming.api.operators.python.process.timer.TimerRegistration
                timerRegistration =
                        isKeyed
                                ? new PtfTimerRegistration(
                                        rowDataKeyedBackend,
                                        (InternalTimerService) internalTimerService,
                                        this,
                                        Utf8LengthPrefixedStringSerializer.INSTANCE,
                                        timerDataSerializer,
                                        partitionKeyConverter,
                                        namedTimers,
                                        unnamedTimers)
                                : null;
        final FlinkFnApi.CoderInfoDescriptor timerCoderDescriptor =
                isKeyed
                        ? TimerUtils.createTimerDataCoderInfoDescriptorProto(timerDataTypeInfo)
                        : null;
        return new BeamProcessTablePythonFunctionRunner(
                getContainingTask().getEnvironment(),
                getRuntimeContext().getTaskInfo().getTaskName(),
                createPythonEnvironmentManager(),
                ptfSpec,
                getFlinkMetricContainer(),
                isKeyed ? getKeyedStateBackend() : null,
                partitionKeySerializer,
                /* namespaceSerializer */ isKeyed
                        ? Utf8LengthPrefixedStringSerializer.INSTANCE
                        : null,
                timerRegistration,
                getContainingTask().getEnvironment().getMemoryManager(),
                getOperatorConfig()
                        .getManagedMemoryFractionOperatorUseCaseOfSlot(
                                ManagedMemoryUseCase.PYTHON,
                                getContainingTask().getJobConfiguration(),
                                getContainingTask()
                                        .getEnvironment()
                                        .getTaskManagerInfo()
                                        .getConfiguration(),
                                getContainingTask()
                                        .getEnvironment()
                                        .getUserCodeClassLoader()
                                        .asClassLoader()),
                createInputCoderInfoDescriptor(),
                createOutputCoderInfoDescriptor(),
                timerCoderDescriptor);
    }

    private FlinkFnApi.CoderInfoDescriptor createInputCoderInfoDescriptor() {
        return createFlattenRowTypeCoderInfoDescriptorProto(
                ptfInputRowType, FlinkFnApi.CoderInfoDescriptor.Mode.MULTIPLE, false);
    }

    private FlinkFnApi.CoderInfoDescriptor createOutputCoderInfoDescriptor() {
        // ChangelogFunction PTFs (emits_changelog != INSERT_ONLY) need the Row-typed coder so the
        // worker preserves the user-set RowKind per emission; the flatten variant always encodes
        // RowKind=0, silently downgrading +U/-U/-D to +I.
        if (ptfSpec.getEmitsChangelog()
                != FlinkFnApi.UserDefinedProcessTableFunction.ChangelogMode.INSERT_ONLY) {
            return createRowTypeCoderInfoDescriptorProto(
                    outputRowType, FlinkFnApi.CoderInfoDescriptor.Mode.MULTIPLE, true);
        }
        return createFlattenRowTypeCoderInfoDescriptorProto(
                outputRowType, FlinkFnApi.CoderInfoDescriptor.Mode.MULTIPLE, true);
    }

    // ------------------------------------------------------------------------
    // Result emission — drains buffered Python outputs
    // ------------------------------------------------------------------------

    @Override
    public void emitResult(Tuple3<String, byte[], Integer> resultTuple) throws IOException {
        final byte[] rawResult = resultTuple.f1;
        final int length = resultTuple.f2;
        // MULTIPLE + separated_with_end_message: skip the 0x00 end-of-element sentinel.
        if (length == 1 && rawResult[0] == 0x00) {
            return;
        }
        bais.setBuffer(rawResult, 0, length);
        final RowData userRow = outputRowSerializer.deserialize(baisWrapper);
        rowDataWrapper.collect(userRow);
    }

    // ------------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------------

    /**
     * Compute the event-time millis for this element. Prefers the StreamRecord timestamp (the
     * canonical Flink path when an upstream WatermarkAssigner has set it). Falls back to reading
     * the user row's on_time column directly — matters for sources/plans that don't propagate
     * the row time to the StreamRecord (e.g. the datagen probe with WATERMARK FOR ts).
     */
    private long extractEventTimeMs(StreamRecord<RowData> element, RowData userRow) {
        if (element.hasTimestamp()) {
            return element.getTimestamp();
        }
        if (onTimeFieldGetter != null) {
            final Object raw = onTimeFieldGetter.getFieldOrNull(userRow);
            if (raw instanceof org.apache.flink.table.data.TimestampData) {
                return ((org.apache.flink.table.data.TimestampData) raw).getMillisecond();
            }
            if (raw instanceof Long) {
                return (Long) raw;
            }
        }
        return -1L;
    }

    /** Find the on_time_column on the SET_SEMANTIC table arg; -1 when absent. */
    private static int extractOnTimeColumn(FlinkFnApi.UserDefinedProcessTableFunction spec) {
        for (FlinkFnApi.UserDefinedProcessTableFunction.ArgumentSpec arg : spec.getArgumentsList()) {
            if (arg.hasTable() && arg.getTable().getOnTimeColumn() >= 0) {
                return arg.getTable().getOnTimeColumn();
            }
        }
        return -1;
    }

    /**
     * Project the partition-by columns out of {@code userRow} into a fresh {@link GenericRowData}
     * that the Python worker uses to {@code set_current_key()} on its keyed-state backend.
     * Returns {@code null} for ROW_SEMANTIC PTFs (no keying).
     */
    private @Nullable RowData projectPartitionKey(RowData userRow) {
        if (partitionKeyFieldGetters == null) {
            return null;
        }
        final GenericRowData out = new GenericRowData(partitionKeyFieldGetters.length);
        for (int i = 0; i < partitionKeyFieldGetters.length; i++) {
            out.setField(i, partitionKeyFieldGetters[i].getFieldOrNull(userRow));
        }
        return out;
    }

    /**
     * Extract the partition-by column indices from the SET_SEMANTIC table arg in the PTF spec;
     * returns an empty array when no arg is SET_SEMANTIC.
     */
    private static int[] extractPartitionColumns(FlinkFnApi.UserDefinedProcessTableFunction spec) {
        for (FlinkFnApi.UserDefinedProcessTableFunction.ArgumentSpec arg : spec.getArgumentsList()) {
            if (arg.hasTable()
                    && arg.getTable().getSemantics()
                            == FlinkFnApi.UserDefinedProcessTableFunction.TableSemantics
                                    .SET_SEMANTIC) {
                final java.util.List<Integer> cols = arg.getTable().getPartitionByColumnsList();
                final int[] out = new int[cols.size()];
                for (int i = 0; i < cols.size(); i++) {
                    out[i] = cols.get(i);
                }
                return out;
            }
        }
        return new int[0];
    }

    private static byte encodeRowKind(RowKind kind) {
        switch (kind) {
            case INSERT:
                return 0;
            case UPDATE_BEFORE:
                return 1;
            case UPDATE_AFTER:
                return 2;
            case DELETE:
                return 3;
            default:
                throw new IllegalStateException("Unknown RowKind: " + kind);
        }
    }
}
