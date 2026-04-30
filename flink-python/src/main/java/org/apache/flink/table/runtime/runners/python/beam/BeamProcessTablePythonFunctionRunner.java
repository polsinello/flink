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

package org.apache.flink.table.runtime.runners.python.beam;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.python.env.process.ProcessPythonEnvironmentManager;
import org.apache.flink.python.metric.process.FlinkMetricContainer;
import org.apache.flink.python.util.ProtoUtils;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.streaming.api.operators.python.process.timer.TimerRegistration;
import org.apache.flink.streaming.api.runners.python.beam.BeamPythonFunctionRunner;
import org.apache.flink.util.Preconditions;

import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.BeamUrns;
import org.apache.beam.runners.core.construction.graph.TimerReference;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.python.Constants.INPUT_COLLECTION_ID;
import static org.apache.flink.python.Constants.MAIN_INPUT_NAME;
import static org.apache.flink.python.Constants.MAIN_OUTPUT_NAME;
import static org.apache.flink.python.Constants.OUTPUT_COLLECTION_ID;
import static org.apache.flink.python.Constants.TIMER_ID;
import static org.apache.flink.python.Constants.TRANSFORM_ID;
import static org.apache.flink.python.Constants.WRAPPER_TIMER_CODER_ID;

/**
 * Beam-based Python function runner for Process Table Functions in process mode.
 *
 * <p>Differences from {@link BeamTablePythonFunctionRunner}:
 *
 * <ul>
 *   <li>Wraps the function spec in a {@code ParDoPayload} with a {@link RunnerApi.TimerFamilySpec}
 *       so the worker can register/fire timers via the timer side channel. Mirrors the design of
 *       {@code BeamDataStreamPythonFunctionRunner} for KeyedProcessFunction.
 *   <li>Multi-input table arguments are handled via the <em>merge pattern</em> (see design doc
 *       §2.2): each Java {@code Input<RowData>} packs its element into a {@code PtfInputRow} with
 *       the argument id, and forwards through the single Beam input PCollection. We do <em>not</em>
 *       use Beam's native multi-input PTransform semantics, matching the precedent set by PyFlink's
 *       {@code CoProcessFunction}.
 * </ul>
 *
 * <p>The runner is keyed-state aware (PTF set-semantic table args partition by user-defined keys);
 * ROW_SEMANTIC_TABLE-only PTFs may instantiate it without a keyed state backend, mirroring the
 * {@link BeamTablePythonFunctionRunner#stateless} factory pattern.
 *
 * <p>See {@code docs/proposals/pyflink-ptf-process-mode-design.md} (in the {@code
 * feature/pyflink-ptf-process-mode} branch) §2.4 for the timer transport design and §2.2 for the
 * multi-input merge pattern.
 */
@Internal
public class BeamProcessTablePythonFunctionRunner extends BeamPythonFunctionRunner {

    /** URN for the PTF transform. Matches PROCESS_TABLE_FUNCTION_URN in the Python worker. */
    public static final String PROCESS_TABLE_FUNCTION_URN =
            "flink:transform:process_table_function:v1";

    private final FlinkFnApi.UserDefinedProcessTableFunction ptfSpec;

    @Nullable private final FlinkFnApi.CoderInfoDescriptor timerCoderDescriptor;

    public BeamProcessTablePythonFunctionRunner(
            Environment environment,
            String taskName,
            ProcessPythonEnvironmentManager environmentManager,
            FlinkFnApi.UserDefinedProcessTableFunction ptfSpec,
            @Nullable FlinkMetricContainer flinkMetricContainer,
            @Nullable KeyedStateBackend<?> keyedStateBackend,
            @Nullable TypeSerializer<?> keySerializer,
            @Nullable TypeSerializer<?> namespaceSerializer,
            @Nullable TimerRegistration timerRegistration,
            MemoryManager memoryManager,
            double managedMemoryFraction,
            FlinkFnApi.CoderInfoDescriptor inputCoderDescriptor,
            FlinkFnApi.CoderInfoDescriptor outputCoderDescriptor,
            @Nullable FlinkFnApi.CoderInfoDescriptor timerCoderDescriptor) {
        super(
                environment,
                taskName,
                environmentManager,
                flinkMetricContainer,
                keyedStateBackend,
                /* operatorStateBackend */ null,
                keySerializer,
                namespaceSerializer,
                timerRegistration,
                memoryManager,
                managedMemoryFraction,
                inputCoderDescriptor,
                outputCoderDescriptor,
                Collections.emptyMap());
        this.ptfSpec = Preconditions.checkNotNull(ptfSpec);
        this.timerCoderDescriptor = timerCoderDescriptor;
    }

    @Override
    protected void buildTransforms(RunnerApi.Components.Builder componentsBuilder) {
        // Wrap the PTF spec in a ParDoPayload to enable timer family attachment.
        // The Beam protocol requires timer support to live on a ParDo primitive
        // transform — same approach as BeamDataStreamPythonFunctionRunner.
        final RunnerApi.ParDoPayload.Builder payloadBuilder =
                RunnerApi.ParDoPayload.newBuilder()
                        .setDoFn(
                                RunnerApi.FunctionSpec.newBuilder()
                                        .setUrn(PROCESS_TABLE_FUNCTION_URN)
                                        .setPayload(
                                                org.apache.beam.vendor.grpc.v1p60p1.com.google
                                                        .protobuf.ByteString.copyFrom(
                                                        ptfSpec.toByteArray()))
                                        .build());

        if (timerCoderDescriptor != null) {
            payloadBuilder.putTimerFamilySpecs(
                    TIMER_ID,
                    RunnerApi.TimerFamilySpec.newBuilder()
                            // EVENT_TIME is the canonical PTF time domain; the
                            // worker carries an explicit time-domain field in
                            // PtfTimerFire to support processing-time fires too.
                            .setTimeDomain(RunnerApi.TimeDomain.Enum.EVENT_TIME)
                            .setTimerFamilyCoderId(WRAPPER_TIMER_CODER_ID)
                            .build());
        }

        componentsBuilder.putTransforms(
                TRANSFORM_ID,
                RunnerApi.PTransform.newBuilder()
                        .setUniqueName(TRANSFORM_ID)
                        .setSpec(
                                RunnerApi.FunctionSpec.newBuilder()
                                        .setUrn(
                                                BeamUrns.getUrn(
                                                        RunnerApi.StandardPTransforms.Primitives
                                                                .PAR_DO))
                                        .setPayload(payloadBuilder.build().toByteString())
                                        .build())
                        .putInputs(MAIN_INPUT_NAME, INPUT_COLLECTION_ID)
                        .putOutputs(MAIN_OUTPUT_NAME, OUTPUT_COLLECTION_ID)
                        .build());
    }

    @Override
    protected List<TimerReference> getTimers(RunnerApi.Components components) {
        if (timerCoderDescriptor == null) {
            return Collections.emptyList();
        }
        RunnerApi.ExecutableStagePayload.TimerId timerId =
                RunnerApi.ExecutableStagePayload.TimerId.newBuilder()
                        .setTransformId(TRANSFORM_ID)
                        .setLocalName(TIMER_ID)
                        .build();
        return Collections.singletonList(TimerReference.fromTimerId(timerId, components));
    }

    @Override
    protected Optional<RunnerApi.Coder> getOptionalTimerCoderProto() {
        if (timerCoderDescriptor == null) {
            return Optional.empty();
        }
        return Optional.of(ProtoUtils.createCoderProto(timerCoderDescriptor));
    }
}
