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

package org.apache.flink.table.planner.plan.nodes.exec.stream;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.transformations.KeyedMultipleInputTransformation;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.python.PythonProcessTableFunction;
import org.apache.flink.table.planner.calcite.RexTableArgCall;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.utils.CommonPythonUtil;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.inference.SystemTypeInference;
import org.apache.flink.table.types.logical.RowType;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlCastFunction;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * {@code StreamExecNode} for a Python {@link
 * org.apache.flink.table.functions.ProcessTableFunction}.
 *
 * <p>Sibling of {@link StreamExecProcessTableFunction}. Routes to the Python operator stack via
 * reflection (avoids a hard {@code flink-table-planner} → {@code flink-python} module dependency).
 */
public class StreamExecPythonProcessTableFunction extends ExecNodeBase<RowData>
        implements StreamExecNode<RowData>, SingleTransformationTranslator<RowData> {

    public static final String PROCESS_TRANSFORMATION = "python-process";

    private final @Nullable String uid;
    private final RexCall invocation;
    private final List<ChangelogMode> inputChangelogModes;
    private final ChangelogMode outputChangelogMode;

    public StreamExecPythonProcessTableFunction(
            ReadableConfig tableConfig,
            List<InputProperty> inputProperties,
            RowType outputType,
            String description,
            @Nullable String uid,
            RexCall invocation,
            List<ChangelogMode> inputChangelogModes,
            ChangelogMode outputChangelogMode) {
        super(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(StreamExecPythonProcessTableFunction.class),
                ExecNodeContext.newPersistedConfig(
                        StreamExecPythonProcessTableFunction.class, tableConfig),
                inputProperties,
                outputType,
                description);
        this.uid = uid;
        this.invocation = invocation;
        this.inputChangelogModes = inputChangelogModes;
        this.outputChangelogMode = outputChangelogMode;
    }

    public RexCall getInvocation() {
        return invocation;
    }

    public @Nullable String getUid() {
        return uid;
    }

    public List<ChangelogMode> getInputChangelogModes() {
        return inputChangelogModes;
    }

    public ChangelogMode getOutputChangelogMode() {
        return outputChangelogMode;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        final List<ExecEdge> inputEdges = getInputEdges();
        if (inputEdges.isEmpty()) {
            throw new TableException(
                    "Python PTF requires at least one table argument; got 0 inputs.");
        }

        final PythonProcessTableFunction pythonFn = extractPythonProcessTableFunction(invocation);
        final boolean isKeyed = pythonFn.hasSetSemanticTableArg();
        final Configuration pythonConfig =
                CommonPythonUtil.extractPythonConfiguration(
                        planner.getTableConfig(), planner.getFlinkContext().getClassLoader());

        // Bake any scalar-arg RexLiterals into the spec proto so the Python worker can
        // recover their values at runtime. The proto bytes shipped on PythonProcessTableFunction
        // were written before the SQL invocation was bound to literal values; we now patch in
        // ScalarArgumentSpec.bound_value for each non-table operand. Per-arg ordering matches
        // declaration order — Calcite reorders named args during type inference.
        byte[] serializedSpec =
                bakeScalarArgValues(
                        pythonFn.getSerializedPtfSpec(),
                        invocation,
                        planner.getFlinkContext().getClassLoader());

        // Patch the row_type of any polymorphic table args (those declared without an explicit
        // row_type in @argument_hint) with the row type Calcite resolved from the SQL caller.
        // The Python wrapper writes a placeholder ROW with no fields for polymorphic args; we
        // detect that placeholder and fill the proto's Schema.FieldType from the input edge.
        // Once patched, the worker reads the proto exactly as it would for an explicitly-typed
        // arg.
        serializedSpec =
                patchTableArgRowTypes(
                        serializedSpec,
                        inputEdges,
                        invocation,
                        planner.getFlinkContext().getClassLoader());

        // Bake the planner-resolved uid (preferred over the wrapper's auto-derived value, so the
        // keyed state namespace matches the Java path's uid fan-out) and the worker state cache
        // size (the worker's RemoteKeyedStateBackend reads it from the proto, not from the Beam
        // pipeline options; an unset value leaves the cache disabled and drops cross-key state on
        // savepoint round-trips).
        serializedSpec =
                bakeSystemValues(
                        serializedSpec,
                        uid,
                        pythonConfig,
                        planner.getFlinkContext().getClassLoader());

        final Transformation<RowData> transformation;
        if (inputEdges.size() == 1) {
            transformation =
                    translateSingleInput(
                            planner,
                            config,
                            inputEdges.get(0),
                            pythonFn,
                            serializedSpec,
                            isKeyed,
                            pythonConfig);
        } else {
            transformation =
                    translateMultiInput(
                            planner,
                            config,
                            inputEdges,
                            pythonFn,
                            serializedSpec,
                            isKeyed,
                            pythonConfig);
        }

        if (CommonPythonUtil.isPythonWorkerUsingManagedMemory(
                pythonConfig, planner.getFlinkContext().getClassLoader())) {
            transformation.declareManagedMemoryUseCaseAtSlotScope(ManagedMemoryUseCase.PYTHON);
        }

        return transformation;
    }

    @SuppressWarnings("unchecked")
    private Transformation<RowData> translateSingleInput(
            PlannerBase planner,
            ExecNodeConfig config,
            ExecEdge inputEdge,
            PythonProcessTableFunction pythonFn,
            byte[] serializedSpec,
            boolean isKeyed,
            Configuration pythonConfig) {
        final Transformation<RowData> inputTransform =
                (Transformation<RowData>) inputEdge.translateToPlan(planner);

        final RowType inputRowType =
                ((InternalTypeInfo<RowData>) inputTransform.getOutputType()).toRowType();

        final OneInputStreamOperator<RowData, RowData> operator =
                createPythonProcessTableOperator(
                        pythonConfig,
                        serializedSpec,
                        inputRowType,
                        (RowType) getOutputType(),
                        isKeyed,
                        planner.getFlinkContext().getClassLoader());

        final OneInputTransformation<RowData, RowData> transformation =
                ExecNodeUtil.createOneInputTransformation(
                        inputTransform,
                        createTransformationMeta(PROCESS_TRANSFORMATION, config),
                        operator,
                        InternalTypeInfo.of(getOutputType()),
                        inputTransform.getParallelism(),
                        false);

        if (isKeyed) {
            final RowDataKeySelector keySelector =
                    buildPartitionKeySelector(
                            invocation,
                            (InternalTypeInfo<RowData>) inputTransform.getOutputType(),
                            planner.getFlinkContext().getClassLoader());
            transformation.setStateKeySelector(keySelector);
            transformation.setStateKeyType(keySelector.getProducedType());
        }
        return transformation;
    }

    /**
     * Multi-input dispatch path (table args >= 2). Builds a {@link
     * KeyedMultipleInputTransformation} where each input has its own {@link
     * RowDataKeySelector} (matching the per-input partition-by columns) and the operator is a
     * {@code PythonMultiInputProcessTableOperator} reflectively constructed via {@code
     * PythonMultiInputProcessTableOperatorFactory}.
     *
     * <p>Heterogeneous inputs (different RowTypes per input) are supported via the operator's
     * widened wire-format: it builds a union {@link RowType} (concatenation of all input row types
     * with disambiguating {@code argN_} field-name prefixes) and projects each input's row into the
     * widened slot positions, leaving non-active slots {@code NULL}. The Python worker projects
     * back to the per-arg shape before invoking {@code eval}.
     */
    @SuppressWarnings("unchecked")
    private Transformation<RowData> translateMultiInput(
            PlannerBase planner,
            ExecNodeConfig config,
            List<ExecEdge> inputEdges,
            PythonProcessTableFunction pythonFn,
            byte[] serializedSpec,
            boolean isKeyed,
            Configuration pythonConfig) {
        final ClassLoader classLoader = planner.getFlinkContext().getClassLoader();
        final int n = inputEdges.size();

        final List<Transformation<RowData>> inputTransforms = new ArrayList<>(n);
        final RowType[] inputRowTypes = new RowType[n];
        for (int i = 0; i < n; i++) {
            final Transformation<RowData> t =
                    (Transformation<RowData>) inputEdges.get(i).translateToPlan(planner);
            inputTransforms.add(t);
            inputRowTypes[i] = ((InternalTypeInfo<RowData>) t.getOutputType()).toRowType();
        }

        // Per-input partition columns from the RexTableArgCall operands; RexTableArgCall
        // .getInputIndex() maps each call to its planner input. ROW_SEMANTIC inputs stay empty.
        final int[][] partitionColumnsPerInput = new int[n][];
        for (int i = 0; i < n; i++) {
            partitionColumnsPerInput[i] = new int[0];
        }
        for (RexNode operand : invocation.getOperands()) {
            if (operand instanceof RexTableArgCall) {
                final RexTableArgCall call = (RexTableArgCall) operand;
                partitionColumnsPerInput[call.getInputIndex()] = call.getPartitionKeys();
            }
        }

        // Build per-input KeySelectors. SET_SEMANTIC inputs use a real RowDataKeySelector;
        // ROW_SEMANTIC inputs use an empty-partition selector (broadcast/SINGLETON distribution
        // already applied in the rule). The first SET_SEMANTIC input also gives us the
        // partitionKeyRowType for the operator and the stateKeyType for the transformation.
        final List<KeySelector<RowData, RowData>> keySelectors = new ArrayList<>(n);
        RowDataKeySelector firstSetSelector = null;
        for (int i = 0; i < n; i++) {
            final InternalTypeInfo<RowData> typeInfo =
                    (InternalTypeInfo<RowData>) inputTransforms.get(i).getOutputType();
            final RowDataKeySelector selector =
                    KeySelectorUtil.getRowDataSelector(
                            classLoader, partitionColumnsPerInput[i], typeInfo);
            keySelectors.add(selector);
            if (firstSetSelector == null && partitionColumnsPerInput[i].length > 0) {
                firstSetSelector = selector;
            }
        }
        if (isKeyed && firstSetSelector == null) {
            throw new TableException(
                    "Python PTF reports SET_SEMANTIC table args but no input carries a "
                            + "non-empty PARTITION BY. Planner state inconsistency.");
        }

        // partitionKeyRowType: the projection of the SET_SEMANTIC input's row type onto its
        // partition columns. Co-partitioning (planner-enforced) means the same shape applies for
        // every SET_SEMANTIC input.
        final RowType partitionKeyRowType =
                isKeyed
                        ? ((InternalTypeInfo<RowData>) firstSetSelector.getProducedType())
                                .toRowType()
                        : inputRowTypes[0];

        final StreamOperatorFactory<RowData> factory =
                createPythonMultiInputFactory(
                        pythonConfig,
                        serializedSpec,
                        inputRowTypes,
                        (RowType) getOutputType(),
                        partitionColumnsPerInput,
                        partitionKeyRowType,
                        isKeyed,
                        classLoader);

        final KeyedMultipleInputTransformation<RowData> transformation =
                ExecNodeUtil.createKeyedMultiInputTransformation(
                        inputTransforms,
                        keySelectors,
                        firstSetSelector != null
                                ? firstSetSelector.getProducedType()
                                : InternalTypeInfo.of(inputRowTypes[0]),
                        createTransformationMeta(PROCESS_TRANSFORMATION, config),
                        factory,
                        InternalTypeInfo.of(getOutputType()),
                        inputTransforms.get(0).getParallelism(),
                        false);
        transformation.setChainingStrategy(ChainingStrategy.HEAD_WITH_SOURCES);
        return transformation;
    }

    /**
     * Build a {@link RowDataKeySelector} from the {@code PARTITION BY} columns of the single
     * SET_SEMANTIC_TABLE arg in a single-input PTF invocation (the first {@link RexTableArgCall}).
     * Multi-input PTFs derive per-input selectors in {@link #translateMultiInput}.
     */
    private static RowDataKeySelector buildPartitionKeySelector(
            RexCall invocation,
            InternalTypeInfo<RowData> inputTypeInfo,
            ClassLoader classLoader) {
        for (RexNode operand : invocation.getOperands()) {
            if (operand instanceof RexTableArgCall) {
                final int[] partitionKeys = ((RexTableArgCall) operand).getPartitionKeys();
                return KeySelectorUtil.getRowDataSelector(
                        classLoader, partitionKeys, inputTypeInfo);
            }
        }
        throw new TableException(
                "No table argument found in Python PTF invocation; cannot derive partition key.");
    }

    /**
     * Walk the PTF invocation operands and rewrite the spec proto to embed any bound scalar
     * literals in {@code ScalarArgumentSpec.bound_value}. Operands match argument-declaration
     * order (Calcite reorders named args during type inference). Operands that aren't literals
     * (or that are table args) leave the corresponding {@code bound_value} empty — the worker
     * will see {@code None} and the user's default kicks in.
     *
     * <p>The wire format mirrors {@code Input.inputConstant}: byte 0 is the {@code j_type} tag
     * (0=basic types, 1=DATE, 2=TIME, 3=TIMESTAMP) and the rest is razorvine-Pickler payload, so
     * the same {@code _parse_constant_value} decoder used for plain UDFs handles PTF scalar args.
     */
    private static byte[] bakeScalarArgValues(
            byte[] originalSpec, RexCall invocation, ClassLoader classLoader) {
        try {
            final Class<?> protoClass =
                    Class.forName(
                            "org.apache.flink.fnexecution.v1.FlinkFnApi$UserDefinedProcessTableFunction",
                            true,
                            classLoader);
            final Object spec =
                    protoClass
                            .getMethod("parseFrom", byte[].class)
                            .invoke(null, (Object) originalSpec);
            final Method toBuilder = protoClass.getMethod("toBuilder");
            final Object specBuilder = toBuilder.invoke(spec);
            final Class<?> specBuilderClass = specBuilder.getClass();
            final Method getArgumentsCount = specBuilderClass.getMethod("getArgumentsCount");
            final int argsCount = (int) getArgumentsCount.invoke(specBuilder);

            // SystemTypeInference appends `on_time` and `uid` system args to every PTF
            // invocation (see SystemTypeInference.PROCESS_TABLE_FUNCTION_SYSTEM_ARGS). Strip
            // them so we only walk user-declared operands; their positional indices match the
            // proto's `arguments` list 1:1.
            final List<RexNode> rawOperands = invocation.getOperands();
            final int systemArgCount =
                    SystemTypeInference.PROCESS_TABLE_FUNCTION_SYSTEM_ARGS.size();
            if (rawOperands.size() < systemArgCount) {
                return originalSpec;
            }
            final List<RexNode> operands =
                    rawOperands.subList(0, rawOperands.size() - systemArgCount);
            if (operands.size() != argsCount) {
                // Defensive: declared-arg count and user operands should match after stripping
                // the system args. If they don't, leave the spec untouched and let the worker
                // fall back to None for SCALAR entries.
                return originalSpec;
            }

            final Method getArgumentsBuilder =
                    specBuilderClass.getMethod("getArgumentsBuilder", int.class);
            final Method buildSpec = specBuilderClass.getMethod("build");
            boolean modified = false;
            for (int i = 0; i < argsCount; i++) {
                RexNode operand = operands.get(i);
                // Unwrap a CAST(literal AS T) wrapper Calcite sometimes emits for typed literals.
                if (operand instanceof RexCall
                        && ((RexCall) operand).getOperator() instanceof SqlCastFunction
                        && ((RexCall) operand).getOperands().get(0) instanceof RexLiteral) {
                    operand = ((RexCall) operand).getOperands().get(0);
                }
                final Object argBuilder = getArgumentsBuilder.invoke(specBuilder, i);
                final Class<?> argBuilderClass = argBuilder.getClass();
                final boolean hasScalar =
                        (boolean) argBuilderClass.getMethod("hasScalar").invoke(argBuilder);
                if (!hasScalar) {
                    continue;
                }
                // Handles RexLiteral as well as ARRAY/ROW/MAP value constructors and
                // DESCRIPTOR column lists; returns null for operands that cannot be baked.
                final byte[] pickled =
                        CommonPythonUtil.convertRexNodeToPython(operand, classLoader);
                if (pickled == null) {
                    continue;
                }
                final Object scalarBuilder =
                        argBuilderClass.getMethod("getScalarBuilder").invoke(argBuilder);
                final Class<?> scalarBuilderClass = scalarBuilder.getClass();
                // flink-python relocates protobuf to its own shaded package; resolve ByteString
                // via the proto class's loader so it matches the FlinkFnApi builder's copy.
                final Class<?> byteStringClass =
                        Class.forName(
                                "org.apache.flink.api.python.shaded.com.google.protobuf.ByteString",
                                true,
                                protoClass.getClassLoader());
                final Method copyFrom = byteStringClass.getMethod("copyFrom", byte[].class);
                final Object byteStr = copyFrom.invoke(null, (Object) pickled);
                scalarBuilderClass
                        .getMethod("setBoundValue", byteStringClass)
                        .invoke(scalarBuilder, byteStr);
                modified = true;
            }
            if (!modified) {
                return originalSpec;
            }
            final Object built = buildSpec.invoke(specBuilder);
            return (byte[]) built.getClass().getMethod("toByteArray").invoke(built);
        } catch (ReflectiveOperationException | RuntimeException e) {
            throw new TableException(
                    "Failed to bake scalar argument values into the Python PTF spec.", e);
        }
    }

    /**
     * Set the planner-resolved {@code uid} and the worker {@code state_cache_size} on the spec
     * proto. The SQL-derived uid (when present) takes precedence over the Python wrapper's
     * auto-derived value so keyed-state namespacing follows the Java path's uid fan-out (same uid
     * -> shared state; different uid -> isolated state). The cache size is read from {@code
     * PythonOptions.STATE_CACHE_SIZE}; the worker's RemoteKeyedStateBackend consults the proto
     * field, so leaving it at 0 disables the cache and loses cross-key state across savepoints.
     */
    private static byte[] bakeSystemValues(
            byte[] originalSpec, String uid, Configuration pythonConfig, ClassLoader classLoader) {
        try {
            final Class<?> protoClass =
                    Class.forName(
                            "org.apache.flink.fnexecution.v1.FlinkFnApi$UserDefinedProcessTableFunction",
                            true,
                            classLoader);
            final Object spec =
                    protoClass
                            .getMethod("parseFrom", byte[].class)
                            .invoke(null, (Object) originalSpec);
            final Object specBuilder = protoClass.getMethod("toBuilder").invoke(spec);
            final Class<?> specBuilderClass = specBuilder.getClass();

            if (uid != null && !uid.isEmpty()) {
                specBuilderClass.getMethod("setUid", String.class).invoke(specBuilder, uid);
            }

            final int stateCacheSize =
                    CommonPythonUtil.getStateCacheSize(pythonConfig, classLoader);
            specBuilderClass
                    .getMethod("setStateCacheSize", int.class)
                    .invoke(specBuilder, stateCacheSize);

            final Object built = specBuilderClass.getMethod("build").invoke(specBuilder);
            return (byte[]) built.getClass().getMethod("toByteArray").invoke(built);
        } catch (ReflectiveOperationException | RuntimeException e) {
            throw new TableException(
                    "Failed to bake uid / state cache size into the Python PTF spec.", e);
        }
    }

    /**
     * Replace placeholder row_type entries on the spec proto's table-arg slots with the row
     * types Calcite resolved from the SQL caller. A polymorphic arg is detected by an empty
     * {@code row_schema.fields} list. Each affected arg's {@code row_type} is rebuilt from the
     * matching input edge's {@link RowType} via reflection over {@code PythonTypeUtils
     * .toProtoType} (loaded from the flink-python module's classloader to avoid a hard module
     * dependency).
     *
     * <p>Also resolves placeholder partition-by indices: a polymorphic arg has empty
     * {@code partition_by_columns} on the proto because the Python wrapper had no row_type to
     * resolve against. We pull them from the {@link RexTableArgCall} and write them in.
     */
    @SuppressWarnings("unchecked")
    private static byte[] patchTableArgRowTypes(
            byte[] originalSpec,
            List<ExecEdge> inputEdges,
            RexCall invocation,
            ClassLoader classLoader) {
        try {
            final Class<?> protoClass =
                    Class.forName(
                            "org.apache.flink.fnexecution.v1.FlinkFnApi$UserDefinedProcessTableFunction",
                            true,
                            classLoader);
            final Object spec =
                    protoClass.getMethod("parseFrom", byte[].class)
                            .invoke(null, (Object) originalSpec);
            final Object specBuilder = protoClass.getMethod("toBuilder").invoke(spec);
            final Class<?> specBuilderClass = specBuilder.getClass();
            final int argsCount =
                    (int) specBuilderClass.getMethod("getArgumentsCount").invoke(specBuilder);

            // Map each PTF input edge to its declared-arg index by walking the
            // RexTableArgCall operands. RexTableArgCall.getInputIndex() identifies which
            // input edge feeds the arg; the operand position identifies the declared arg slot.
            final RowType[] inputRowTypes = new RowType[inputEdges.size()];
            for (int i = 0; i < inputEdges.size(); i++) {
                inputRowTypes[i] = (RowType) inputEdges.get(i).getOutputType();
            }
            final List<RexNode> operands = invocation.getOperands();

            // Look up PythonTypeUtils.toProtoType reflectively. Using
            // protoClass.getClassLoader() ensures we resolve to the same flink-python module
            // copy that owns FlinkFnApi.
            final Class<?> typeUtilsClass =
                    Class.forName(
                            "org.apache.flink.table.runtime.typeutils.PythonTypeUtils",
                            true,
                            protoClass.getClassLoader());
            final Method toProtoType =
                    typeUtilsClass.getMethod(
                            "toProtoType",
                            org.apache.flink.table.types.logical.LogicalType.class);

            boolean modified = false;
            for (int argPos = 0; argPos < operands.size() && argPos < argsCount; argPos++) {
                final RexNode operand = operands.get(argPos);
                if (!(operand instanceof RexTableArgCall)) {
                    continue;
                }
                final RexTableArgCall tableArgCall = (RexTableArgCall) operand;
                final RowType resolvedRowType = inputRowTypes[tableArgCall.getInputIndex()];

                final Object argBuilder =
                        specBuilderClass
                                .getMethod("getArgumentsBuilder", int.class)
                                .invoke(specBuilder, argPos);
                final Class<?> argBuilderClass = argBuilder.getClass();
                final boolean hasTable =
                        (boolean) argBuilderClass.getMethod("hasTable").invoke(argBuilder);
                if (!hasTable) {
                    continue;
                }
                final Object tableBuilder =
                        argBuilderClass.getMethod("getTableBuilder").invoke(argBuilder);
                final Class<?> tableBuilderClass = tableBuilder.getClass();

                // Detect placeholder: row_type.row_schema.fields is empty.
                final Object rowTypeMsg =
                        tableBuilderClass.getMethod("getRowType").invoke(tableBuilder);
                final Object rowSchema =
                        rowTypeMsg.getClass().getMethod("getRowSchema").invoke(rowTypeMsg);
                final int rowSchemaFieldCount =
                        (int) rowSchema.getClass().getMethod("getFieldsCount").invoke(rowSchema);
                if (rowSchemaFieldCount > 0) {
                    continue; // Already-typed arg — leave untouched.
                }

                // Build the resolved Schema.FieldType from the input edge's RowType.
                final Object resolvedFieldType = toProtoType.invoke(null, resolvedRowType);
                tableBuilderClass
                        .getMethod("setRowType", resolvedFieldType.getClass())
                        .invoke(tableBuilder, resolvedFieldType);

                // Resolve partition_by_columns from the RexTableArgCall when absent.
                final int existingPartitions =
                        (int) tableBuilderClass
                                .getMethod("getPartitionByColumnsCount")
                                .invoke(tableBuilder);
                if (existingPartitions == 0) {
                    final int[] partitionKeys = tableArgCall.getPartitionKeys();
                    if (partitionKeys != null && partitionKeys.length > 0) {
                        final Method addPartition =
                                tableBuilderClass.getMethod(
                                        "addPartitionByColumns", int.class);
                        for (int idx : partitionKeys) {
                            addPartition.invoke(tableBuilder, idx);
                        }
                    }
                }
                modified = true;
            }
            if (!modified) {
                return originalSpec;
            }
            final Object built =
                    specBuilderClass.getMethod("build").invoke(specBuilder);
            return (byte[]) built.getClass().getMethod("toByteArray").invoke(built);
        } catch (ReflectiveOperationException | RuntimeException e) {
            throw new TableException(
                    "Failed to patch polymorphic table-arg row types into the Python PTF spec.",
                    e);
        }
    }

    /** Pull the PythonProcessTableFunction wrapper out of the planner's RexCall. */
    private static PythonProcessTableFunction extractPythonProcessTableFunction(RexCall call) {
        final FunctionDefinition definition = ShortcutUtils.unwrapFunctionDefinition(call);
        if (!(definition instanceof PythonProcessTableFunction)) {
            throw new TableException(
                    "Expected a PythonProcessTableFunction in the call, got "
                            + (definition == null ? "null" : definition.getClass().getName()));
        }
        return (PythonProcessTableFunction) definition;
    }

    /**
     * Reflectively instantiate {@code PythonProcessTableOperator} from {@code flink-python},
     * avoiding a hard planner -> runtime module dependency. {@code FlinkFnApi} lives only in {@code
     * flink-python}, so the proto is parsed via the user-code classloader here.
     */
    private static OneInputStreamOperator<RowData, RowData> createPythonProcessTableOperator(
            Configuration pythonConfig,
            byte[] serializedPtfSpec,
            RowType inputRowType,
            RowType outputRowType,
            boolean isKeyed,
            ClassLoader classLoader) {
        try {
            final Class<?> protoClass =
                    Class.forName(
                            "org.apache.flink.fnexecution.v1.FlinkFnApi$UserDefinedProcessTableFunction",
                            true,
                            classLoader);
            final Object protoSpec =
                    protoClass
                            .getMethod("parseFrom", byte[].class)
                            .invoke(null, (Object) serializedPtfSpec);

            final Class<?> clazz =
                    Class.forName(
                            "org.apache.flink.table.runtime.operators.python.processtable."
                                    + "PythonProcessTableOperator",
                            true,
                            classLoader);
            final Constructor<?> ctor =
                    clazz.getDeclaredConstructor(
                            Configuration.class,
                            protoClass,
                            RowType.class,
                            RowType.class,
                            boolean.class);
            @SuppressWarnings("unchecked")
            final OneInputStreamOperator<RowData, RowData> instance =
                    (OneInputStreamOperator<RowData, RowData>)
                            ctor.newInstance(
                                    pythonConfig, protoSpec, inputRowType, outputRowType, isKeyed);
            return instance;
        } catch (ReflectiveOperationException e) {
            throw new TableException(
                    "Failed to instantiate PythonProcessTableOperator. "
                            + "Ensure flink-python is on the planner classpath.",
                    e);
        }
    }

    /**
     * Reflectively construct {@code PythonMultiInputProcessTableOperatorFactory} from {@code
     * flink-python}. Same module-decoupling pattern as {@link #createPythonProcessTableOperator},
     * but for the multi-input V2 path which requires a {@link StreamOperatorFactory} rather than
     * a directly-constructed operator instance.
     */
    @SuppressWarnings("unchecked")
    private static StreamOperatorFactory<RowData> createPythonMultiInputFactory(
            Configuration pythonConfig,
            byte[] serializedPtfSpec,
            RowType[] inputRowTypes,
            RowType outputRowType,
            int[][] partitionColumnsPerInput,
            RowType partitionKeyRowType,
            boolean isKeyed,
            ClassLoader classLoader) {
        try {
            final Class<?> clazz =
                    Class.forName(
                            "org.apache.flink.table.runtime.operators.python.processtable."
                                    + "PythonMultiInputProcessTableOperatorFactory",
                            true,
                            classLoader);
            final Constructor<?> ctor =
                    clazz.getDeclaredConstructor(
                            Configuration.class,
                            byte[].class,
                            RowType[].class,
                            RowType.class,
                            int[][].class,
                            RowType.class,
                            boolean.class);
            return (StreamOperatorFactory<RowData>)
                    ctor.newInstance(
                            pythonConfig,
                            serializedPtfSpec,
                            inputRowTypes,
                            outputRowType,
                            partitionColumnsPerInput,
                            partitionKeyRowType,
                            isKeyed);
        } catch (ReflectiveOperationException e) {
            throw new TableException(
                    "Failed to instantiate PythonMultiInputProcessTableOperatorFactory. "
                            + "Ensure flink-python is on the planner classpath.",
                    e);
        }
    }
}
