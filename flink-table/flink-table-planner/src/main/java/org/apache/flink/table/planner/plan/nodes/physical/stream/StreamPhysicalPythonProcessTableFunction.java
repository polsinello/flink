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

package org.apache.flink.table.planner.plan.nodes.physical.stream;

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecPythonProcessTableFunction;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalTableFunctionScan;
import org.apache.flink.table.planner.plan.utils.ChangelogPlanUtils;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;

import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTableConfig;

/**
 * Stream physical node for a Python {@link org.apache.flink.table.functions.ProcessTableFunction}.
 *
 * <p>Subclasses {@link StreamPhysicalProcessTableFunction} to inherit constructor-time validations
 * and shared metadata, and overrides {@link #translateToExecNode()} to produce a {@link
 * StreamExecPythonProcessTableFunction} (routing the call into the Python operator stack).
 *
 * <p>Keeps its own references to the scan and row type because the parent's are private; this lets
 * {@link #copy} reconstruct the node.
 */
public class StreamPhysicalPythonProcessTableFunction extends StreamPhysicalProcessTableFunction {

    private final FlinkLogicalTableFunctionScan scan;
    private final RelDataType pythonRowType;

    public StreamPhysicalPythonProcessTableFunction(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            List<RelNode> inputs,
            FlinkLogicalTableFunctionScan scan,
            RelDataType rowType) {
        super(cluster, traitSet, inputs, scan, rowType);
        this.scan = scan;
        this.pythonRowType = rowType;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new StreamPhysicalPythonProcessTableFunction(
                getCluster(), traitSet, inputs, scan, pythonRowType);
    }

    @Override
    public ExecNode<?> translateToExecNode() {
        // Recompute changelog modes — same logic as the Java exec node path.
        final List<ChangelogMode> inputChangelogModes =
                getInputs().stream()
                        .map(StreamPhysicalRel.class::cast)
                        .map(ChangelogPlanUtils::getChangelogMode)
                        .map(JavaScalaConversionUtil::toJava)
                        .map(optional -> optional.orElseThrow(IllegalStateException::new))
                        .collect(Collectors.toList());
        final ChangelogMode outputChangelogMode =
                JavaScalaConversionUtil.toJava(ChangelogPlanUtils.getChangelogMode(this))
                        .orElseThrow(IllegalStateException::new);
        final RexCall call = getCall();

        return new StreamExecPythonProcessTableFunction(
                unwrapTableConfig(this),
                getInputs().stream().map(i -> InputProperty.DEFAULT).collect(Collectors.toList()),
                FlinkTypeFactory.toLogicalRowType(pythonRowType),
                getRelDetailedDescription(),
                getUid(),
                call,
                inputChangelogModes,
                outputChangelogMode);
    }
}
