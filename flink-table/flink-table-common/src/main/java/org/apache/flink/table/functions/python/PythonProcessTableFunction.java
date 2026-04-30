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

package org.apache.flink.table.functions.python;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.functions.ChangelogFunction;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.StaticArgument;
import org.apache.flink.table.types.inference.StaticArgumentTrait;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The Java-side wrapper of a user-defined Python {@link ProcessTableFunction} (PTF), executed in
 * process mode.
 *
 * <p>The actual PTF logic lives in a Python class registered via {@code pyflink.table.ptf(...)}.
 * Python ships the serialized {@code UserDefinedProcessTableFunction} protobuf payload (the {@code
 * serializedPtfSpec} field) plus a parallel set of serializable lists that describe the eval
 * signature ({@link #argNames}, {@link #argDataTypes}, {@link #argIsTable}, {@link #argIsOptional},
 * {@link #argTraitNames}). These lists are sufficient to rebuild the non-serializable {@link
 * StaticArgument} instances at planning time without decoding the proto Java-side — the proto bytes
 * are intentionally opaque to this class.
 *
 * <p>This separation matches the existing {@link PythonScalarFunction} pattern: the wrapper class
 * lives in {@code flink-table-common} for planner integration; the proto-aware code lives only in
 * the Python wrapper, which has direct access to the regenerated {@code FlinkFnApi}. All fields on
 * this class are serializable so {@code ClosureCleaner} can validate the function during {@code
 * create_temporary_function}.
 */
@Internal
public class PythonProcessTableFunction extends ProcessTableFunction<Object>
        implements PythonFunction, ChangelogFunction {

    private static final long serialVersionUID = 1L;

    /** Logical name of the registered function (used in {@link #toString} for plan output). */
    private final String name;

    /**
     * Serialized {@code UserDefinedProcessTableFunction} protobuf carrying the full PTF spec
     * (pickled user instance + args + state slots + uid + runtime context). Opaque to this class —
     * decoded by the Beam runner at runtime in {@code flink-python}.
     */
    private final byte[] serializedPtfSpec;

    /**
     * Output row data type emitted by {@link ProcessTableFunction#collect(Object)}. The {@link
     * #getTypeInference(DataTypeFactory)} method exposes this to the planner.
     */
    private final DataType outputType;

    // Serializable arg metadata, mirroring the eval signature in declaration order.
    private final List<String> argNames;
    private final List<DataType> argDataTypes;
    private final List<Boolean> argIsTable;
    private final List<Boolean> argIsOptional;

    /** Per-arg list of {@link StaticArgumentTrait} enum names. Each inner list is the trait set. */
    private final List<List<String>> argTraitNames;

    private final PythonFunctionKind pythonFunctionKind;
    private final boolean deterministic;
    private final PythonEnv pythonEnv;

    /**
     * Declared output changelog mode name from Python's {@code ChangelogMode} enum:
     * "INSERT_ONLY" / "ALL_RETRACT" / "UPSERT" / "ALL_CHANGES". Drives {@link
     * #getChangelogMode(ChangelogContext)} so the planner can satisfy a non-INSERT-only
     * {@code ModifyKindSetTrait} for PTFs that the user marked as emitting changes.
     */
    private final String emitsChangelogModeName;

    /**
     * Optional declarative {@link ChangelogContext} negotiation table. Maps the downstream's
     * required ChangelogMode (one of "INSERT_ONLY" / "ALL_RETRACT" / "UPSERT" / "ALL_CHANGES",
     * matching {@link #emitsChangelogModeName}'s naming) to the output mode the PTF will emit
     * in that scenario. When set (non-empty), {@link #getChangelogMode(ChangelogContext)}
     * looks up by the required mode's name and returns the mapped value; if no entry matches,
     * falls back to {@link #emitsChangelogModeName}. Set via the
     * {@link #setOutputChangelogForRequiredMap(Map)} setter from the Python wrapper at
     * registration time.
     */
    private Map<String, String> outputChangelogForRequiredMap = Collections.emptyMap();

    public PythonProcessTableFunction(
            String name,
            byte[] serializedPtfSpec,
            DataType outputType,
            List<String> argNames,
            List<DataType> argDataTypes,
            List<Boolean> argIsTable,
            List<Boolean> argIsOptional,
            List<List<String>> argTraitNames,
            PythonFunctionKind pythonFunctionKind,
            boolean deterministic,
            PythonEnv pythonEnv,
            String emitsChangelogModeName) {
        this.name = name;
        this.serializedPtfSpec = serializedPtfSpec;
        this.outputType = outputType;
        this.argNames = nullSafe(argNames);
        this.argDataTypes = nullSafe(argDataTypes);
        this.argIsTable = nullSafe(argIsTable);
        this.argIsOptional = nullSafe(argIsOptional);
        this.argTraitNames = nullSafe(argTraitNames);
        this.pythonFunctionKind = pythonFunctionKind;
        this.deterministic = deterministic;
        this.pythonEnv = pythonEnv;
        this.emitsChangelogModeName =
                emitsChangelogModeName == null ? "INSERT_ONLY" : emitsChangelogModeName;
    }

    private static <T> List<T> nullSafe(List<T> list) {
        return list == null ? Collections.emptyList() : new ArrayList<>(list);
    }

    // ------------------------------------------------------------------------
    // Placeholder eval / onTimer — never invoked at runtime
    // ------------------------------------------------------------------------

    public void eval(Object... args) {
        throw new UnsupportedOperationException(
                "PythonProcessTableFunction.eval should not be invoked directly; "
                        + "execution happens in the Python worker via PythonProcessTableOperator.");
    }

    public void onTimer(Object onTimerCtx, Object... state) {
        throw new UnsupportedOperationException(
                "PythonProcessTableFunction.onTimer should not be invoked directly; "
                        + "execution happens in the Python worker via PythonProcessTableOperator.");
    }

    // ------------------------------------------------------------------------
    // PythonFunction contract
    // ------------------------------------------------------------------------

    @Override
    public byte[] getSerializedPythonFunction() {
        return serializedPtfSpec;
    }

    @Override
    public PythonEnv getPythonEnv() {
        return pythonEnv;
    }

    @Override
    public PythonFunctionKind getPythonFunctionKind() {
        return pythonFunctionKind;
    }

    @Override
    public boolean takesRowAsInput() {
        return true;
    }

    @Override
    public boolean isDeterministic() {
        return deterministic;
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        final TypeInference.Builder builder = TypeInference.newBuilder();
        if (!argNames.isEmpty()) {
            builder.staticArguments(buildStaticArguments());
        }
        return builder.outputTypeStrategy(TypeStrategies.explicit(outputType)).build();
    }

    private List<StaticArgument> buildStaticArguments() {
        final List<StaticArgument> result = new ArrayList<>(argNames.size());
        for (int i = 0; i < argNames.size(); i++) {
            final String name = argNames.get(i);
            final DataType dt = argDataTypes.get(i);
            final boolean isTable = argIsTable.get(i);
            final boolean optional = argIsOptional.get(i);
            if (isTable) {
                final EnumSet<StaticArgumentTrait> traits =
                        EnumSet.noneOf(StaticArgumentTrait.class);
                for (String t : argTraitNames.get(i)) {
                    traits.add(StaticArgumentTrait.valueOf(t));
                }
                if (dt == null) {
                    // Polymorphic Row table arg — caller's row type is bound at
                    // SQL invocation time. {@link StaticArgument#table(String,
                    // Class, boolean, EnumSet)} is the polymorphic form; the
                    // conversion class must be a {@link Row} subtype (PTF
                    // worker side reads the wire as Row anyway).
                    result.add(
                            StaticArgument.table(
                                    name,
                                    org.apache.flink.types.Row.class,
                                    optional,
                                    traits));
                } else {
                    result.add(StaticArgument.table(name, dt, optional, traits));
                }
            } else {
                result.add(StaticArgument.scalar(name, dt, optional));
            }
        }
        return result;
    }

    public byte[] getSerializedPtfSpec() {
        return serializedPtfSpec;
    }

    /**
     * Setter invoked from the Python wrapper at registration time. Captures the user's
     * declarative ChangelogContext negotiation table. See
     * {@link #outputChangelogForRequiredMap} for the contract.
     */
    public void setOutputChangelogForRequiredMap(Map<String, String> table) {
        this.outputChangelogForRequiredMap =
                table == null ? Collections.emptyMap() : new HashMap<>(table);
    }

    public DataType getOutputType() {
        return outputType;
    }

    /** True if any declared argument is a SET_SEMANTIC_TABLE — drives keyed-state usage. */
    public boolean hasSetSemanticTableArg() {
        for (int i = 0; i < argIsTable.size(); i++) {
            if (argIsTable.get(i) && argTraitNames.get(i).contains("SET_SEMANTIC_TABLE")) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String toString() {
        return name;
    }

    // ------------------------------------------------------------------------
    // ChangelogFunction
    // ------------------------------------------------------------------------

    /**
     * Returns the declared output {@link ChangelogMode}. Mirrors the four PTF changelog modes
     * exposed via Python's {@code ChangelogMode} enum:
     *
     * <ul>
     *   <li>{@code INSERT_ONLY} → {@link ChangelogMode#insertOnly()} (default).
     *   <li>{@code ALL_RETRACT} / {@code ALL_CHANGES} → {@link ChangelogMode#all()} (full set:
     *       INSERT, UPDATE_BEFORE, UPDATE_AFTER, DELETE).
     *   <li>{@code UPSERT} → {@link ChangelogMode#upsert()} (INSERT, UPDATE_AFTER, DELETE with
     *       key-only deletes).
     * </ul>
     *
     * <p>The {@link ChangelogContext} is currently unused — Python PTFs declare a fixed output
     * mode rather than negotiating with input/required modes. A follow-up may expose the context
     * to the Python side via the wire protocol.
     */
    @Override
    public ChangelogMode getChangelogMode(ChangelogContext changelogContext) {
        // Step 1: Resolve the effective output-mode name. If the user supplied a
        // declarative negotiation table and the surrounding ChangelogContext provides
        // a required mode, look it up; otherwise fall back to the static declaration.
        String effectiveModeName = emitsChangelogModeName;
        if (!outputChangelogForRequiredMap.isEmpty()
                && changelogContext != null
                && changelogContext.getRequiredChangelogMode() != null) {
            final String requiredKey = nameOfChangelogMode(
                    changelogContext.getRequiredChangelogMode());
            final String mapped = outputChangelogForRequiredMap.get(requiredKey);
            if (mapped != null) {
                effectiveModeName = mapped;
            }
        }

        // Step 2: Translate the resolved name into a Flink ChangelogMode.
        switch (effectiveModeName) {
            case "ALL_CHANGES":
            case "ALL_RETRACT":
                return ChangelogMode.all();
            case "UPSERT":
                // Mirror the surrounding context's keyOnlyDeletes preference so the
                // sink/caller (whose PRIMARY KEY drives the required mode) can
                // negotiate full-row vs. key-only deletes with the PTF. Falls back
                // to full-row deletes when no required mode is available.
                if (changelogContext != null
                        && changelogContext.getRequiredChangelogMode() != null) {
                    return ChangelogMode.upsert(
                            changelogContext
                                    .getRequiredChangelogMode()
                                    .keyOnlyDeletes());
                }
                return ChangelogMode.upsert();
            case "INSERT_ONLY":
            default:
                return ChangelogMode.insertOnly();
        }
    }

    /**
     * Best-fit translation of a Flink {@link ChangelogMode} into the Python
     * {@code ChangelogMode} enum-name space used by {@link #emitsChangelogModeName} and
     * {@link #outputChangelogForRequiredMap}. The mapping mirrors the four canonical Java
     * shapes Python exposes:
     *
     * <ul>
     *   <li>{@code insertOnly()} -> {@code "INSERT_ONLY"}.
     *   <li>Has UPDATE_AFTER + DELETE without UPDATE_BEFORE -> {@code "UPSERT"}.
     *   <li>{@code all()} (every kind) -> {@code "ALL_CHANGES"}.
     *   <li>Anything else with retractions -> {@code "ALL_RETRACT"}.
     * </ul>
     */
    private static String nameOfChangelogMode(ChangelogMode mode) {
        if (mode.equals(ChangelogMode.insertOnly())) {
            return "INSERT_ONLY";
        }
        if (mode.equals(ChangelogMode.all())) {
            return "ALL_CHANGES";
        }
        // UPSERT requires UPDATE_AFTER without UPDATE_BEFORE. A mode that merely
        // lacks UPDATE_BEFORE (e.g. {INSERT, DELETE} or delete-only) is not an
        // upsert mode and must not borrow the UPSERT lookup key.
        final boolean hasUpdateAfter =
                mode.contains(org.apache.flink.types.RowKind.UPDATE_AFTER);
        final boolean hasUpdateBefore =
                mode.contains(org.apache.flink.types.RowKind.UPDATE_BEFORE);
        if (hasUpdateAfter && !hasUpdateBefore) {
            return "UPSERT";
        }
        // Any remaining retraction shape (has UPDATE_BEFORE, or DELETE without
        // updates) maps to ALL_RETRACT; otherwise it is effectively insert-only.
        final boolean hasRetractions =
                hasUpdateBefore
                        || hasUpdateAfter
                        || mode.contains(org.apache.flink.types.RowKind.DELETE);
        return hasRetractions ? "ALL_RETRACT" : "INSERT_ONLY";
    }
}
