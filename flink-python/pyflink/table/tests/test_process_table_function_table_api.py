################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
"""
Upstream-style pytest for Process Table Functions (PTF), Table API form.

Exercises the new PyFlink PTF Table API surface against the same core scenarios
as test_process_table_function.py (SQL):

  - ``t_env.from_call(fn, table.as_argument(name), lit(v).as_argument(name))``
  - ``table.process(fn, lit(v))`` / ``table.process(fn, lit(v).as_argument(name))``
  - ``table.partition_by(col(...)).process(fn, ...)``
  - ``t_env.from_call(fn, a.partition_by(col).as_argument("in1"),
                          b.partition_by(col).as_argument("in2"))``

Results are materialized via ``TableResult.collect`` and compared against the
exact rows ProcessTableFunctionTestPrograms.java expects.
"""
from pyflink.common import Row
from pyflink.table import DataTypes
from pyflink.table.expressions import col, lit
from pyflink.table.process_table_function import (
    ProcessTableFunction, ptf, argument_hint, state_hint,
    ArgumentTrait, StateKind,
)
from pyflink.table.table_environment import StreamTableEnvironment
from pyflink.testing.test_case_utils import PyFlinkStreamTableTestCase

# Reuse the same PTF definitions and rendering helpers as the SQL suite.
from pyflink.table.tests.test_process_table_function import (
    T_ROW, OUT, SCORE, _obj_str,
    ScalarArgsFunction, RowSemanticTableFunction, SetSemanticTableFunction,
    PojoStateFunction, ContextFunction, MultiInputFunction,
)


class ProcessTableFunctionTableApiTests(PyFlinkStreamTableTestCase):

    def setUp(self):
        self.t_env = StreamTableEnvironment.create(self.env)
        self.t_env.get_config().set("python.fn-execution.bundle.size", "1")
        # PTFs run only in process execution mode.
        self.t_env.get_config().set("python.execution-mode", "process")

    def _basic(self):
        return self.t_env.sql_query(
            "SELECT * FROM (VALUES ('Bob', 12), ('Alice', 42)) AS T(name, score)")

    def _multi(self):
        return self.t_env.sql_query(
            "SELECT * FROM (VALUES ('Bob', 12), ('Alice', 42), ('Bob', 99), "
            "('Bob', 100), ('Alice', 400)) AS T(name, score)")

    def _city(self):
        return self.t_env.sql_query(
            "SELECT * FROM (VALUES ('Bob', 'London'), ('Alice', 'Berlin'), "
            "('Charly', 'Paris')) AS T(name, city)")

    def _out_col(self, table):
        names = table.get_schema().get_field_names()
        oi = names.index("out")
        return sorted(str(r[oi]) for r in table.execute().collect())

    def _keyed_out(self, table):
        names = table.get_schema().get_field_names()
        oi = names.index("out")
        ki = 0 if names[0] != "out" else 1
        return sorted("%s | %s" % (r[ki], str(r[oi]))
                      for r in table.execute().collect())

    # ----- scalar args: from_call (named) ------------------------------- #

    def test_scalar_args_from_call(self):
        f = ptf(ScalarArgsFunction(), output_type=OUT)
        res = self.t_env.from_call(f, lit(42).as_argument("i"),
                                   lit(True).as_argument("b"))
        self.assertEqual(self._out_col(res), ["{42, true}"])

    # ----- row-semantic table: from_call + inline + inline-named -------- #

    def test_row_semantic_from_call(self):
        f = ptf(RowSemanticTableFunction(), output_type=OUT)
        res = self.t_env.from_call(f, self._basic().as_argument("r"),
                                   lit(1).as_argument("i"))
        self.assertEqual(self._out_col(res),
                         sorted(["{+I[Bob, 12], 1}", "{+I[Alice, 42], 1}"]))

    def test_row_semantic_inline_positional(self):
        f = ptf(RowSemanticTableFunction(), output_type=OUT)
        res = self._basic().process(f, lit(1))
        self.assertEqual(self._out_col(res),
                         sorted(["{+I[Bob, 12], 1}", "{+I[Alice, 42], 1}"]))

    def test_row_semantic_inline_named(self):
        f = ptf(RowSemanticTableFunction(), output_type=OUT)
        res = self._basic().process(f, lit(1).as_argument("i"))
        self.assertEqual(self._out_col(res),
                         sorted(["{+I[Bob, 12], 1}", "{+I[Alice, 42], 1}"]))

    # ----- set-semantic table: from_call + partition_by.process --------- #

    def test_set_semantic_from_call(self):
        f = ptf(SetSemanticTableFunction(), output_type=OUT)
        res = self.t_env.from_call(
            f, self._basic().partition_by(col("name")).as_argument("r"),
            lit(1).as_argument("i"))
        self.assertEqual(self._keyed_out(res),
                         sorted(["Bob | {+I[Bob, 12], 1}",
                                 "Alice | {+I[Alice, 42], 1}"]))

    def test_set_semantic_inline(self):
        f = ptf(SetSemanticTableFunction(), output_type=OUT)
        res = self._basic().partition_by(col("name")).process(f, lit(1))
        self.assertEqual(self._keyed_out(res),
                         sorted(["Bob | {+I[Bob, 12], 1}",
                                 "Alice | {+I[Alice, 42], 1}"]))

    # ----- stateful set-semantic via Table API -------------------------- #

    def test_pojo_state(self):
        f = ptf(PojoStateFunction(), output_type=OUT)
        res = self._multi().partition_by(col("name")).process(f)
        self.assertEqual(self._keyed_out(res), sorted([
            "Bob | {Score(s='null', i=null), +I[Bob, 12]}",
            "Alice | {Score(s='null', i=null), +I[Alice, 42]}",
            "Bob | {Score(s='Bob', i=0), +I[Bob, 99]}",
            "Bob | {Score(s='Bob', i=1), +I[Bob, 100]}",
            "Alice | {Score(s='null', i=0), +I[Alice, 400]}",
        ]))

    # ----- context introspection via Table API -------------------------- #

    def test_context(self):
        f = ptf(ContextFunction(), output_type=OUT)
        res = self.t_env.from_call(
            f, self._basic().partition_by(col("name")).as_argument("r"),
            lit("param").as_argument("s"))
        self.assertEqual(self._keyed_out(res), sorted([
            "Bob | {+I[Bob, 12], param, [0], [INSERT], "
            "ROW<`name` VARCHAR(5) NOT NULL, `score` INT NOT NULL>}",
            "Alice | {+I[Alice, 42], param, [0], [INSERT], "
            "ROW<`name` VARCHAR(5) NOT NULL, `score` INT NOT NULL>}",
        ]))

    # ----- multi-input via from_call with two table args ---------------- #

    def test_multi_input(self):
        f = ptf(MultiInputFunction(), output_type=OUT)
        res = self.t_env.from_call(
            f,
            self._multi().partition_by(col("name")).as_argument("in1"),
            self._city().partition_by(col("name")).as_argument("in2"))
        names = res.get_schema().get_field_names()
        oi = names.index("out")
        key_cols = [i for i in range(len(names)) if i != oi][:2]
        rows = list(res.execute().collect())
        actual = sorted("%s | %s | %s" % (r[key_cols[0]], r[key_cols[1]], str(r[oi]))
                        for r in rows)
        self.assertEqual(actual, sorted([
            "Bob | Bob | {+I[Bob, 12], null}",
            "Bob | Bob | {null, +I[Bob, London]}",
            "Alice | Alice | {+I[Alice, 42], null}",
            "Alice | Alice | {null, +I[Alice, Berlin]}",
            "Bob | Bob | {+I[Bob, 99], null}",
            "Charly | Charly | {null, +I[Charly, Paris]}",
            "Bob | Bob | {+I[Bob, 100], null}",
            "Alice | Alice | {+I[Alice, 400], null}",
        ]))


if __name__ == "__main__":
    import unittest
    unittest.main()
