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
import org.apache.flink.table.functions.python.PythonAggregateFunctionInfo;
import org.apache.flink.table.runtime.dataview.DataViewSpec;
import org.apache.flink.table.types.logical.RowType;

/**
 * The embedded Python AggregateFunction operator for group aggregation. Runs Python aggregate
 * functions in-process via pemja.
 */
@Internal
public class EmbeddedPythonStreamGroupAggregateOperator
        extends AbstractEmbeddedStreamGroupAggregateOperator {

    private static final long serialVersionUID = 1L;

    /** True if the count(*) agg is inserted by the planner. */
    private final boolean countStarInserted;

    public EmbeddedPythonStreamGroupAggregateOperator(
            Configuration config,
            RowType inputType,
            RowType outputType,
            PythonAggregateFunctionInfo[] aggregateFunctions,
            DataViewSpec[][] dataViewSpecs,
            int[] grouping,
            int indexOfCountStar,
            boolean countStarInserted,
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
                generateUpdateBefore,
                minRetentionTime,
                maxRetentionTime);
        this.countStarInserted = countStarInserted;
    }

    @Override
    protected String getPythonFactoryFunctionName() {
        return "create_group_aggregate_operation_from_proto";
    }

    @Override
    protected FlinkFnApi.UserDefinedAggregateFunctions getUserDefinedFunctionsProto() {
        FlinkFnApi.UserDefinedAggregateFunctions.Builder builder =
                super.getUserDefinedFunctionsProto().toBuilder();
        builder.setCountStarInserted(countStarInserted);
        return builder.build();
    }
}
