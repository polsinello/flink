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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * {@link org.apache.flink.streaming.api.operators.StreamOperatorFactory} for {@link
 * PythonMultiInputProcessTableOperator}.
 *
 * <p>Factory pattern is mandated by {@link
 * org.apache.flink.streaming.api.transformations.KeyedMultipleInputTransformation}. Single-input
 * Python PTFs continue to use direct reflective instantiation in {@code
 * StreamExecPythonProcessTableFunction} (V1 path); only the multi-input path needs a factory
 * because V2's {@code MultipleInputStreamOperator} construction is parameter-driven.
 *
 * <p>Stores the PTF spec as serialized bytes rather than the proto object. The proto class lives
 * in {@code flink-python} and isn't visible to the planner module's classloader at construction
 * time; deferring deserialization to {@code createStreamOperator} keeps both modules clean.
 */
@Internal
public class PythonMultiInputProcessTableOperatorFactory
        extends AbstractStreamOperatorFactory<RowData> {

    private static final long serialVersionUID = 1L;

    private final Configuration pythonConfig;
    private final byte[] serializedPtfSpec;
    private final RowType[] inputRowTypes;
    private final RowType outputRowType;
    private final int[][] partitionColumnsPerInput;
    private final RowType partitionKeyRowType;
    private final boolean isKeyed;

    public PythonMultiInputProcessTableOperatorFactory(
            Configuration pythonConfig,
            byte[] serializedPtfSpec,
            RowType[] inputRowTypes,
            RowType outputRowType,
            int[][] partitionColumnsPerInput,
            RowType partitionKeyRowType,
            boolean isKeyed) {
        this.pythonConfig = pythonConfig;
        this.serializedPtfSpec = serializedPtfSpec;
        this.inputRowTypes = inputRowTypes;
        this.outputRowType = outputRowType;
        this.partitionColumnsPerInput = partitionColumnsPerInput;
        this.partitionKeyRowType = partitionKeyRowType;
        this.isKeyed = isKeyed;
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public <T extends StreamOperator<RowData>> T createStreamOperator(
            StreamOperatorParameters<RowData> parameters) {
        try {
            final FlinkFnApi.UserDefinedProcessTableFunction ptfSpec =
                    FlinkFnApi.UserDefinedProcessTableFunction.parseFrom(serializedPtfSpec);
            final PythonMultiInputProcessTableOperator operator =
                    new PythonMultiInputProcessTableOperator(
                            parameters,
                            pythonConfig,
                            ptfSpec,
                            inputRowTypes,
                            outputRowType,
                            partitionColumnsPerInput,
                            partitionKeyRowType,
                            isKeyed);
            return (T) operator;
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(
                    "Failed to deserialize PTF spec at operator construction time. The serialized "
                            + "bytes may be corrupted or use a proto schema incompatible with "
                            + "FlinkFnApi.UserDefinedProcessTableFunction.",
                    e);
        }
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        // Resolve via the user-code classloader so the factory stays portable across job
        // restarts (mirrors ProcessTableOperatorFactory in the Java path).
        try {
            return (Class<? extends StreamOperator>)
                    Class.forName(
                            "org.apache.flink.table.runtime.operators.python.processtable."
                                    + "PythonMultiInputProcessTableOperator",
                            true,
                            classLoader);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(
                    "PythonMultiInputProcessTableOperator class not found on the user-code "
                            + "classloader; ensure flink-python is on the runtime classpath.",
                    e);
        }
    }
}
