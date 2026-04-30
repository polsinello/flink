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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
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
import org.apache.flink.python.env.PythonDependencyInfo;
import org.apache.flink.python.env.process.ProcessPythonEnvironmentManager;
import org.apache.flink.python.metric.process.FlinkMetricContainer;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.operators.AbstractInput;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorV2;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.MultipleInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.operators.python.process.timer.TimerHandler;
import org.apache.flink.streaming.api.operators.python.process.timer.TimerRegistration;
import org.apache.flink.streaming.api.operators.python.process.timer.TimerUtils;
import org.apache.flink.streaming.api.runners.python.beam.BeamPythonFunctionRunner;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.RowRowConverter;
import org.apache.flink.table.functions.python.PythonEnv;
import org.apache.flink.table.runtime.operators.python.utils.StreamRecordRowDataWrappingCollector;
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
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.python.PythonOptions.MAX_BUNDLE_SIZE;
import static org.apache.flink.python.PythonOptions.MAX_BUNDLE_TIME_MILLS;
import static org.apache.flink.python.PythonOptions.PYTHON_METRIC_ENABLED;
import static org.apache.flink.python.PythonOptions.PYTHON_SYSTEMENV_ENABLED;
import static org.apache.flink.python.util.ProtoUtils.createFlattenRowTypeCoderInfoDescriptorProto;
import static org.apache.flink.python.util.ProtoUtils.createRowTypeCoderInfoDescriptorProto;

/**
 * Multi-input runtime operator for a Python {@link
 * org.apache.flink.table.functions.ProcessTableFunction}. Handles PTFs with two or more table
 * arguments via the merge pattern: N {@code Input<RowData>} instances pack their elements into a
 * {@code PtfInputRow} carrying the {@code arg_id} (0..N-1) and forward through one shared Beam
 * runner; the Python worker demuxes by {@code arg_id}.
 *
 * <p>Heterogeneous inputs (different {@link RowType} per input) use a widened wire-format: the
 * operator computes a union {@code RowType} (concatenation of every input's fields with
 * disambiguating {@code argN_} prefixes) and projects each element into its widened slots, leaving
 * other inputs' slots {@code NULL}. The worker projects back to the per-arg shape before invoking
 * {@code eval}. Homogeneous inputs (all RowTypes equal) skip the widening.
 *
 * <p>Sibling of the single-input {@link PythonProcessTableOperator}. This class extends V2 because
 * {@link MultipleInputStreamOperator} mandates it; the runner-orchestration boilerplate is mirrored
 * from {@code AbstractExternalPythonFunctionOperator}, which extends the V1 base and so cannot be
 * reused here.
 */
@Internal
public class PythonMultiInputProcessTableOperator extends AbstractStreamOperatorV2<RowData>
        implements MultipleInputStreamOperator<RowData>, Triggerable<RowData, String> {

    private static final long serialVersionUID = 1L;

    /** Wire-format field count for {@code PtfInputRow}; mirror of the Python side. */
    private static final int PTF_INPUT_FIELD_COUNT = 6;

    private static final int PTF_INPUT_ARG_ID = 0;
    private static final int PTF_INPUT_ROW_KIND = 1;
    private static final int PTF_INPUT_EVENT_TIME = 2;
    private static final int PTF_INPUT_WATERMARK = 3;
    private static final int PTF_INPUT_PARTITION_KEY = 4;
    private static final int PTF_INPUT_USER_ROW = 5;

    // ------------------------------------------------------------------------
    // Configuration (set by constructor)
    // ------------------------------------------------------------------------

    private final Configuration config;
    private final FlinkFnApi.UserDefinedProcessTableFunction ptfSpec;

    /** One row type per Java input. May differ across inputs (heterogeneous case). */
    private final RowType[] inputRowTypes;

    private final RowType outputRowType;

    /**
     * Per-input partition column indices. Length matches {@link #inputRowTypes}; an empty array at
     * index {@code i} means input {@code i} is ROW_SEMANTIC (no partition by).
     */
    private final int[][] partitionColumnsPerInput;

    /**
     * Shared partition-key shape derived from the first SET_SEMANTIC input. The planner enforces
     * co-partitioning (matching key types across SET_SEMANTIC inputs); ROW_SEMANTIC inputs use the
     * same shape on the wire with {@code partition_key=null}.
     */
    private final RowType partitionKeyRowType;

    /** True if any input is SET_SEMANTIC (has non-empty partitionColumnsPerInput[i]). */
    private final boolean isKeyed;

    private final int numInputs;

    /**
     * Containing {@link StreamTask}, captured at construction because {@link
     * AbstractStreamOperatorV2} exposes no runtime accessor; used for Beam-runner construction and
     * environment lookups in {@link #open()} / {@link #createPythonFunctionRunner()}.
     */
    private transient StreamTask<?, ?> containingTask;

    // ------------------------------------------------------------------------
    // Bundle / runner orchestration (mirror of AbstractExternalPythonFunctionOperator)
    // ------------------------------------------------------------------------

    private transient PythonFunctionRunner pythonFunctionRunner;
    private transient ExecutorService flushThreadPool;

    private transient boolean systemEnvEnabled;
    private transient int maxBundleSize;
    private transient int elementCount;
    private transient long maxBundleTimeMills;
    private transient long lastFinishBundleTime;
    private transient ScheduledFuture<?> checkFinishBundleTimer;
    private transient Runnable bundleFinishedCallback;

    // ------------------------------------------------------------------------
    // PTF wire format (built in open(); shared across inputs)
    // ------------------------------------------------------------------------

    private transient RowType ptfInputRowType;
    private transient TypeSerializer<RowData> ptfInputRowSerializer;
    private transient TypeSerializer<RowData> outputRowSerializer;
    @Nullable private transient TypeSerializer<RowData> partitionKeySerializer;

    /** Per-input partition-key field getters; null entry for ROW_SEMANTIC inputs. */
    @Nullable private transient RowData.FieldGetter[][] partitionKeyFieldGettersPerInput;

    /** Per-input on_time field getter; null entry when input has no bound on_time column. */
    @Nullable private transient RowData.FieldGetter[] onTimeFieldGettersPerInput;

    // ---- Widened-user-row scaffolding (heterogeneous multi-input) ---- //

    /**
     * True when input row types differ across inputs. When set, {@link #widenedUserRowType}
     * carries the union shape, and {@link #processInputElement} fills only the slots owned by
     * the active arg (others are NULL).
     */
    private transient boolean isHeterogeneous;

    /** When heterogeneous: union RowType. Otherwise: same as {@code inputRowTypes[0]}. */
    private transient RowType widenedUserRowType;

    /** Per-input field offsets into the widened RowType. Length == numInputs. */
    private transient int[] argFieldOffsets;

    /** Per-input field counts (== inputRowTypes[i].getFieldCount()). Length == numInputs. */
    private transient int[] argFieldCounts;

    /**
     * Per-input field getters into the (per-input) input row, indexed
     * {@code [inputIdx][localFieldIdx]}. Used to copy values into the widened slot positions.
     * Only populated when {@link #isHeterogeneous}.
     */
    @Nullable private transient RowData.FieldGetter[][] perInputFieldGetters;

    /**
     * Reusable widened user-row buffer. Allocated once at open() time and reused per element.
     * Only populated when {@link #isHeterogeneous}.
     */
    @Nullable private transient GenericRowData reusableWidenedUserRow;

    /** Per-input on_time column index, -1 when absent. Cached for the TimestampData unwrap. */
    private transient int[] onTimeColumnPerInput;

    private transient ByteArrayInputStreamWithPos bais;
    private transient DataInputViewStreamWrapper baisWrapper;
    private transient ByteArrayOutputStreamWithPos baos;
    private transient DataOutputViewStreamWrapper baosWrapper;

    private transient StreamRecordRowDataWrappingCollector rowDataWrapper;
    private transient GenericRowData reusableInputRow;

    // ----- timer-side-channel scaffolding (only populated when isKeyed) ----- //

    @Nullable private transient InternalTimerService<String> internalTimerService;
    @Nullable private transient RowTypeInfo partitionKeyRowTypeInfo;
    @Nullable private transient TypeSerializer<Row> timerDataSerializer;
    @Nullable private transient TypeInformation<Row> timerDataTypeInfo;
    @Nullable private transient TimerHandler timerHandler;
    @Nullable private transient RowRowConverter partitionKeyConverter;
    @Nullable private transient
            org.apache.flink.api.common.state.MapState<String, Long> namedTimers;
    @Nullable private transient
            org.apache.flink.api.common.state.ListState<Long> unnamedTimers;

    // ------------------------------------------------------------------------

    public PythonMultiInputProcessTableOperator(
            StreamOperatorParameters<RowData> parameters,
            Configuration config,
            FlinkFnApi.UserDefinedProcessTableFunction ptfSpec,
            RowType[] inputRowTypes,
            RowType outputRowType,
            int[][] partitionColumnsPerInput,
            RowType partitionKeyRowType,
            boolean isKeyed) {
        super(parameters, inputRowTypes.length);
        this.containingTask = parameters.getContainingTask();
        this.config = Preconditions.checkNotNull(config);
        this.ptfSpec = Preconditions.checkNotNull(ptfSpec);
        this.inputRowTypes = Preconditions.checkNotNull(inputRowTypes);
        Preconditions.checkArgument(inputRowTypes.length >= 2,
                "PythonMultiInputProcessTableOperator requires at least 2 inputs; got %s",
                inputRowTypes.length);
        this.outputRowType = Preconditions.checkNotNull(outputRowType);
        this.partitionColumnsPerInput = Preconditions.checkNotNull(partitionColumnsPerInput);
        Preconditions.checkArgument(
                partitionColumnsPerInput.length == inputRowTypes.length,
                "partitionColumnsPerInput length %s != inputRowTypes length %s",
                partitionColumnsPerInput.length,
                inputRowTypes.length);
        this.partitionKeyRowType = partitionKeyRowType;
        this.isKeyed = isKeyed;
        this.numInputs = inputRowTypes.length;
    }

    // ------------------------------------------------------------------------
    // Lifecycle
    // ------------------------------------------------------------------------

    @Override
    public void open() throws Exception {
        // ---- Bundle / runner orchestration init (mirror of AbstractPythonFunctionOperator.open) -- //

        this.systemEnvEnabled = config.get(PYTHON_SYSTEMENV_ENABLED);
        this.maxBundleSize = config.get(MAX_BUNDLE_SIZE);
        if (this.maxBundleSize <= 0) {
            this.maxBundleSize = MAX_BUNDLE_SIZE.defaultValue();
        }
        this.maxBundleTimeMills = config.get(MAX_BUNDLE_TIME_MILLS);
        if (this.maxBundleTimeMills <= 0L) {
            this.maxBundleTimeMills = MAX_BUNDLE_TIME_MILLS.defaultValue();
        }
        this.elementCount = 0;
        this.lastFinishBundleTime = getProcessingTimeService().getCurrentProcessingTime();
        long bundleCheckPeriod = Math.max(this.maxBundleTimeMills, 1);
        this.checkFinishBundleTimer =
                getProcessingTimeService()
                        .scheduleAtFixedRate(
                                timestamp -> checkInvokeFinishBundleByTime(),
                                bundleCheckPeriod,
                                bundleCheckPeriod);

        // ---- PTF wire format build (shared across inputs) ---- //

        // Per-input partition-key field getters. ROW_SEMANTIC inputs (empty partition cols) get
        // a null entry; processInputElement projects null partition_key for them.
        partitionKeyFieldGettersPerInput = new RowData.FieldGetter[numInputs][];
        for (int i = 0; i < numInputs; i++) {
            final int[] cols = partitionColumnsPerInput[i];
            if (cols.length == 0) {
                partitionKeyFieldGettersPerInput[i] = null;
                continue;
            }
            final RowData.FieldGetter[] getters = new RowData.FieldGetter[cols.length];
            for (int j = 0; j < cols.length; j++) {
                getters[j] = RowData.createFieldGetter(inputRowTypes[i].getTypeAt(cols[j]), cols[j]);
            }
            partitionKeyFieldGettersPerInput[i] = getters;
        }

        if (isKeyed) {
            partitionKeySerializer = PythonTypeUtils.toInternalSerializer(partitionKeyRowType);
        }

        // ---- Widened user-row computation ---- //
        // Heterogeneous => concatenate all input fields with arg-prefixed names into one
        // widened RowType; processInputElement fills only the active arg's slots, NULL elsewhere.
        // Homogeneous (all inputRowTypes equal) => widened == inputRowTypes[0] verbatim.
        argFieldOffsets = new int[numInputs];
        argFieldCounts = new int[numInputs];
        for (int i = 0; i < numInputs; i++) {
            argFieldCounts[i] = inputRowTypes[i].getFieldCount();
        }
        boolean allEqual = true;
        for (int i = 1; i < numInputs; i++) {
            if (!inputRowTypes[i].equals(inputRowTypes[0])) {
                allEqual = false;
                break;
            }
        }
        isHeterogeneous = !allEqual;
        if (isHeterogeneous) {
            int totalFields = 0;
            final java.util.List<LogicalType> widenedTypes = new java.util.ArrayList<>();
            final java.util.List<String> widenedNames = new java.util.ArrayList<>();
            perInputFieldGetters = new RowData.FieldGetter[numInputs][];
            for (int i = 0; i < numInputs; i++) {
                argFieldOffsets[i] = totalFields;
                final RowType rt = inputRowTypes[i];
                final RowData.FieldGetter[] getters = new RowData.FieldGetter[rt.getFieldCount()];
                for (int j = 0; j < rt.getFieldCount(); j++) {
                    // Force-nullable copy: non-active arg slots are NULL on the wire.
                    widenedTypes.add(rt.getTypeAt(j).copy(true));
                    widenedNames.add("arg" + i + "_" + rt.getFieldNames().get(j));
                    getters[j] = RowData.createFieldGetter(rt.getTypeAt(j), j);
                    totalFields++;
                }
                perInputFieldGetters[i] = getters;
            }
            widenedUserRowType =
                    RowType.of(
                            widenedTypes.toArray(new LogicalType[0]),
                            widenedNames.toArray(new String[0]));
            reusableWidenedUserRow = new GenericRowData(totalFields);
        } else {
            for (int i = 0; i < numInputs; i++) {
                argFieldOffsets[i] = 0;
            }
            widenedUserRowType = inputRowTypes[0];
        }

        // Wire-format ptfInputRowType: field 4 (partition_key) carries the projected partition-key
        // shape, not the full user-row shape. Field 5 (user_row) uses the widened RowType.
        final LogicalType partitionKeyField =
                isKeyed ? partitionKeyRowType.copy(true) : inputRowTypes[0].copy(true);

        ptfInputRowType =
                RowType.of(
                        new LogicalType[] {
                            new IntType(false), // arg_id
                            new TinyIntType(false), // row_kind
                            new BigIntType(false), // event_time_ms
                            new BigIntType(false), // watermark_ms
                            partitionKeyField, // partition_key
                            widenedUserRowType.copy(false) // user_row (widened or shared)
                        },
                        new String[] {
                            "arg_id", "row_kind", "event_time", "watermark", "partition_key",
                            "user_row"
                        });
        ptfInputRowSerializer = PythonTypeUtils.toInternalSerializer(ptfInputRowType);
        outputRowSerializer = PythonTypeUtils.toInternalSerializer(outputRowType);

        // ---- Timer side channel (only for keyed PTFs) ---- //

        if (isKeyed) {
            internalTimerService =
                    getInternalTimerService(
                            "ptf-user-timers",
                            Utf8LengthPrefixedStringSerializer.INSTANCE,
                            this);
            // V2 multi-input ops have no keyed runtime context, so the named/unnamed timer
            // bookkeeping goes through getOrCreateKeyedState with VoidNamespaceSerializer (matching
            // what the V1 KeyedStateStore does for plain getMapState/getListState).
            namedTimers = getOrCreateKeyedState(
                    VoidNamespaceSerializer.INSTANCE,
                    new org.apache.flink.api.common.state.MapStateDescriptor<>(
                            "__pyflink_ptf_named_timers__",
                            org.apache.flink.api.common.typeutils.base.StringSerializer.INSTANCE,
                            org.apache.flink.api.common.typeutils.base.LongSerializer.INSTANCE));
            unnamedTimers = getOrCreateKeyedState(
                    VoidNamespaceSerializer.INSTANCE,
                    new org.apache.flink.api.common.state.ListStateDescriptor<>(
                            "__pyflink_ptf_unnamed_timers__",
                            org.apache.flink.api.common.typeutils.base.LongSerializer.INSTANCE));

            final DataType partitionKeyDataType =
                    TypeConversions.fromLogicalToDataType(partitionKeyRowType);
            partitionKeyConverter = RowRowConverter.create(partitionKeyDataType);
            partitionKeyConverter.open(
                    containingTask.getEnvironment().getUserCodeClassLoader().asClassLoader());
            partitionKeyRowTypeInfo =
                    (RowTypeInfo) TypeConversions.fromDataTypeToLegacyInfo(partitionKeyDataType);
            timerDataTypeInfo = TimerUtils.createTimerDataTypeInfo(partitionKeyRowTypeInfo);
            timerDataSerializer =
                    org.apache.flink.streaming.api.utils.PythonTypeUtils
                            .TypeInfoToSerializerConverter
                            .typeInfoSerializerConverter(timerDataTypeInfo);
            timerHandler = new TimerHandler();
        }

        // ---- on_time column extraction (per input) ---- //

        onTimeColumnPerInput = new int[numInputs];
        onTimeFieldGettersPerInput = new RowData.FieldGetter[numInputs];
        for (int i = 0; i < numInputs; i++) {
            onTimeColumnPerInput[i] = extractOnTimeColumnForInput(ptfSpec, i);
            if (onTimeColumnPerInput[i] >= 0) {
                onTimeFieldGettersPerInput[i] =
                        RowData.createFieldGetter(
                                inputRowTypes[i].getTypeAt(onTimeColumnPerInput[i]),
                                onTimeColumnPerInput[i]);
            }
        }

        // ---- Buffers + collector ---- //

        reusableInputRow = new GenericRowData(PTF_INPUT_FIELD_COUNT);
        rowDataWrapper = new StreamRecordRowDataWrappingCollector(output);

        bais = new ByteArrayInputStreamWithPos();
        baisWrapper = new DataInputViewStreamWrapper(bais);
        baos = new ByteArrayOutputStreamWithPos();
        baosWrapper = new DataOutputViewStreamWrapper(baos);

        // ---- Beam runner (must come after all wire-format/timer state is set) ---- //

        this.pythonFunctionRunner = createPythonFunctionRunner();
        this.pythonFunctionRunner.open(config);
        this.flushThreadPool = Executors.newSingleThreadExecutor();

        super.open();
    }

    @Override
    public void finish() throws Exception {
        try {
            invokeFinishBundle();
        } finally {
            super.finish();
        }
    }

    @Override
    public void close() throws Exception {
        try {
            if (checkFinishBundleTimer != null) {
                checkFinishBundleTimer.cancel(true);
                checkFinishBundleTimer = null;
            }
            if (pythonFunctionRunner != null) {
                pythonFunctionRunner.close();
                pythonFunctionRunner = null;
            }
            if (flushThreadPool != null) {
                flushThreadPool.shutdown();
                flushThreadPool = null;
            }
        } finally {
            super.close();
        }
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        try {
            invokeFinishBundle();
        } finally {
            super.prepareSnapshotPreBarrier(checkpointId);
        }
    }

    // ------------------------------------------------------------------------
    // MultipleInputStreamOperator
    // ------------------------------------------------------------------------

    @Override
    @SuppressWarnings("rawtypes")
    public List<Input> getInputs() {
        final List<Input> inputs = new ArrayList<>(numInputs);
        for (int i = 0; i < numInputs; i++) {
            // AbstractInput inputId is 1-indexed; argId stays 0-indexed for wire format.
            inputs.add(new PtfInput(this, i + 1, i));
        }
        return inputs;
    }

    /**
     * One {@link Input} per Java table-arg input. {@link AbstractInput} pulls the matching
     * KeySelector from the operator's config (set by {@code KeyedMultipleInputTransformation
     * .addInput}), so {@code stateKeySelector} is wired transparently.
     */
    private static final class PtfInput extends AbstractInput<RowData, RowData> {
        private final PythonMultiInputProcessTableOperator owner;
        private final int argId;

        PtfInput(PythonMultiInputProcessTableOperator owner, int inputId, int argId) {
            super(owner, inputId);
            this.owner = owner;
            this.argId = argId;
        }

        @Override
        public void processElement(StreamRecord<RowData> element) throws Exception {
            owner.processInputElement(argId, element);
        }
    }

    // ------------------------------------------------------------------------
    // Per-input element processing — packs the wire row, ships through the runner
    // ------------------------------------------------------------------------

    private void processInputElement(int argId, StreamRecord<RowData> element) throws Exception {
        final RowData userRow = element.getValue();
        reusableInputRow.setRowKind(RowKind.INSERT);
        reusableInputRow.setField(PTF_INPUT_ARG_ID, argId);
        reusableInputRow.setField(PTF_INPUT_ROW_KIND, encodeRowKind(userRow.getRowKind()));
        reusableInputRow.setField(PTF_INPUT_EVENT_TIME, extractEventTimeMs(argId, element, userRow));
        reusableInputRow.setField(
                PTF_INPUT_WATERMARK,
                internalTimerService != null
                        ? internalTimerService.currentWatermark()
                        : Long.MIN_VALUE);
        reusableInputRow.setField(PTF_INPUT_PARTITION_KEY, projectPartitionKey(argId, userRow));
        reusableInputRow.setField(PTF_INPUT_USER_ROW, projectIntoWidened(argId, userRow));

        ptfInputRowSerializer.serialize((RowData) reusableInputRow, baosWrapper);
        pythonFunctionRunner.process(baos.toByteArray());
        baos.reset();

        elementCount++;
        checkInvokeFinishBundleByCount();
        emitResults();
    }

    // The V2 framework dispatches per-input watermarks, advances the InternalTimerService (firing
    // the Triggerable callbacks below) and emits downstream. Unlike the V1 single-input path, this
    // operator does not finish the bundle before forwarding a watermark; that hook does not fit V2's
    // per-input dispatch and the resulting ordering is bounded by Beam's timer-fire ordering.

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
            throw new IllegalStateException(
                    "Timer fired on a non-keyed PTF; this should not happen.");
        }

        // Encode the timer name as length-prefixed UTF-8 (mirror of
        // Utf8LengthPrefixedStringSerializer.serialize). null/empty -> unnamed.
        final String timerName = timer.getNamespace();
        final byte[] encodedNamespace;
        if (timerName == null || timerName.isEmpty()) {
            encodedNamespace = null;
        } else {
            final byte[] utf8 = timerName.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            encodedNamespace = new byte[4 + utf8.length];
            encodedNamespace[0] = (byte) ((utf8.length >>> 24) & 0xff);
            encodedNamespace[1] = (byte) ((utf8.length >>> 16) & 0xff);
            encodedNamespace[2] = (byte) ((utf8.length >>> 8) & 0xff);
            encodedNamespace[3] = (byte) (utf8.length & 0xff);
            System.arraycopy(utf8, 0, encodedNamespace, 4, utf8.length);
        }

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
    // Beam-runner construction (mirror of PythonProcessTableOperator)
    // ------------------------------------------------------------------------

    public PythonEnv getPythonEnv() {
        return new PythonEnv(PythonEnv.ExecType.PROCESS);
    }

    public PythonFunctionRunner createPythonFunctionRunner() throws Exception {
        @SuppressWarnings({"unchecked", "rawtypes"})
        final KeyedStateBackend<RowData> rowDataKeyedBackend =
                isKeyed ? (KeyedStateBackend) getKeyedStateBackend() : null;
        @SuppressWarnings("rawtypes")
        final TimerRegistration timerRegistration =
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
                containingTask.getEnvironment(),
                getRuntimeContext().getTaskInfo().getTaskName(),
                createPythonEnvironmentManager(),
                ptfSpec,
                getFlinkMetricContainer(),
                isKeyed ? getKeyedStateBackend() : null,
                partitionKeySerializer,
                isKeyed ? Utf8LengthPrefixedStringSerializer.INSTANCE : null,
                timerRegistration,
                containingTask.getEnvironment().getMemoryManager(),
                getOperatorConfig()
                        .getManagedMemoryFractionOperatorUseCaseOfSlot(
                                ManagedMemoryUseCase.PYTHON,
                                containingTask.getJobConfiguration(),
                                containingTask
                                        .getEnvironment()
                                        .getTaskManagerInfo()
                                        .getConfiguration(),
                                containingTask
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
        // ChangelogFunction PTFs (emits_changelog != INSERT_ONLY) need the Row-typed coder so
        // the worker preserves the user-set RowKind per emission; the flatten variant always
        // encodes RowKind=0, silently downgrading +U/-U/-D to +I.
        if (ptfSpec.getEmitsChangelog()
                != FlinkFnApi.UserDefinedProcessTableFunction.ChangelogMode.INSERT_ONLY) {
            return createRowTypeCoderInfoDescriptorProto(
                    outputRowType, FlinkFnApi.CoderInfoDescriptor.Mode.MULTIPLE, true);
        }
        return createFlattenRowTypeCoderInfoDescriptorProto(
                outputRowType, FlinkFnApi.CoderInfoDescriptor.Mode.MULTIPLE, true);
    }

    private ProcessPythonEnvironmentManager createPythonEnvironmentManager() {
        final PythonDependencyInfo dependencyInfo =
                PythonDependencyInfo.create(config, getRuntimeContext().getDistributedCache());
        final PythonEnv pythonEnv = getPythonEnv();
        if (pythonEnv.getExecType() == PythonEnv.ExecType.PROCESS) {
            return new ProcessPythonEnvironmentManager(
                    dependencyInfo,
                    containingTask.getEnvironment().getTaskManagerInfo().getTmpDirectories(),
                    systemEnvEnabled ? new HashMap<>(System.getenv()) : new HashMap<>(),
                    getRuntimeContext().getJobInfo().getJobId());
        }
        throw new UnsupportedOperationException(
                "Execution type '" + pythonEnv.getExecType() + "' is not supported.");
    }

    private FlinkMetricContainer getFlinkMetricContainer() {
        return this.config.get(PYTHON_METRIC_ENABLED)
                ? new FlinkMetricContainer(getRuntimeContext().getMetricGroup())
                : null;
    }

    // ------------------------------------------------------------------------
    // Bundle orchestration (mirror of AbstractExternalPythonFunctionOperator)
    // ------------------------------------------------------------------------

    private void checkInvokeFinishBundleByCount() throws Exception {
        if (elementCount >= maxBundleSize) {
            invokeFinishBundle();
        }
    }

    private void checkInvokeFinishBundleByTime() throws Exception {
        long now = getProcessingTimeService().getCurrentProcessingTime();
        if (now - lastFinishBundleTime >= maxBundleTimeMills) {
            invokeFinishBundle();
        }
    }

    private void invokeFinishBundle() throws Exception {
        if (elementCount > 0) {
            AtomicBoolean flushThreadFinish = new AtomicBoolean(false);
            AtomicReference<Throwable> exceptionReference = new AtomicReference<>();
            flushThreadPool.submit(
                    () -> {
                        try {
                            pythonFunctionRunner.flush();
                        } catch (Throwable e) {
                            exceptionReference.set(e);
                        } finally {
                            flushThreadFinish.set(true);
                            ((BeamPythonFunctionRunner) pythonFunctionRunner)
                                    .notifyNoMoreResults();
                        }
                    });
            Tuple3<String, byte[], Integer> resultTuple;
            while (!flushThreadFinish.get()) {
                resultTuple = pythonFunctionRunner.takeResult();
                if (resultTuple.f2 != 0) {
                    emitResult(resultTuple);
                    emitResults();
                }
            }
            emitResults();
            Throwable flushThreadThrowable = exceptionReference.get();
            if (flushThreadThrowable != null) {
                throw new RuntimeException(
                        "Error while waiting for BeamPythonFunctionRunner flush",
                        flushThreadThrowable);
            }
            elementCount = 0;
            lastFinishBundleTime = getProcessingTimeService().getCurrentProcessingTime();
            if (bundleFinishedCallback != null) {
                bundleFinishedCallback.run();
                bundleFinishedCallback = null;
            }
        }
    }

    private void emitResults() throws Exception {
        Tuple3<String, byte[], Integer> resultTuple;
        while ((resultTuple = pythonFunctionRunner.pollResult()) != null && resultTuple.f2 != 0) {
            emitResult(resultTuple);
        }
    }

    public void emitResult(Tuple3<String, byte[], Integer> resultTuple) throws IOException {
        final byte[] rawResult = resultTuple.f1;
        final int length = resultTuple.f2;
        // MULTIPLE + separated_with_end_message: skip the 0x00 sentinel.
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

    private long extractEventTimeMs(int argId, StreamRecord<RowData> element, RowData userRow) {
        if (element.hasTimestamp()) {
            return element.getTimestamp();
        }
        final RowData.FieldGetter onTimeGetter =
                onTimeFieldGettersPerInput != null
                        ? onTimeFieldGettersPerInput[argId]
                        : null;
        if (onTimeGetter != null) {
            final Object raw = onTimeGetter.getFieldOrNull(userRow);
            if (raw instanceof org.apache.flink.table.data.TimestampData) {
                return ((org.apache.flink.table.data.TimestampData) raw).getMillisecond();
            }
            if (raw instanceof Long) {
                return (Long) raw;
            }
        }
        return -1L;
    }

    /**
     * Find the on_time_column on the table arg matching {@code argId} (0-indexed), or {@code -1}
     * when the arg has no bound on_time.
     */
    private static int extractOnTimeColumnForInput(
            FlinkFnApi.UserDefinedProcessTableFunction spec, int argId) {
        int tableArgIdx = 0;
        for (FlinkFnApi.UserDefinedProcessTableFunction.ArgumentSpec arg : spec.getArgumentsList()) {
            if (arg.hasTable()) {
                if (tableArgIdx == argId) {
                    return arg.getTable().getOnTimeColumn();
                }
                tableArgIdx++;
            }
        }
        return -1;
    }

    /**
     * Project the per-input element into the widened user_row shape. In the homogeneous case
     * (all input row types equal) this returns the input verbatim. In the heterogeneous case it
     * fills the widened buffer's active-arg slots from the input row's fields and leaves the
     * other inputs' slots NULL.
     */
    private RowData projectIntoWidened(int argId, RowData userRow) {
        if (!isHeterogeneous) {
            return userRow;
        }
        final GenericRowData widened = reusableWidenedUserRow;
        // Reset every slot to null (cheap — total field count is small).
        for (int i = 0; i < widened.getArity(); i++) {
            widened.setField(i, null);
        }
        final int offset = argFieldOffsets[argId];
        final RowData.FieldGetter[] getters = perInputFieldGetters[argId];
        for (int j = 0; j < getters.length; j++) {
            widened.setField(offset + j, getters[j].getFieldOrNull(userRow));
        }
        return widened;
    }

    @Nullable
    private RowData projectPartitionKey(int argId, RowData userRow) {
        final RowData.FieldGetter[] getters = partitionKeyFieldGettersPerInput[argId];
        if (getters == null) {
            return null;
        }
        final GenericRowData out = new GenericRowData(getters.length);
        for (int i = 0; i < getters.length; i++) {
            out.setField(i, getters[i].getFieldOrNull(userRow));
        }
        return out;
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
