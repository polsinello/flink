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
Upstream-style pytest for Process Table Functions (PTF), SQL form.

Mirrors the core scenarios of
flink-table-planner ProcessTableFunctionTestPrograms.java, exercised through
SQL: the PTF is registered via ``create_temporary_function`` and invoked with
``SELECT * FROM f(...)``. Results are materialized via ``TableResult.collect``
and compared against the exact rows the Java test programs expect.

The Table API counterpart lives in test_process_table_function_table_api.py.
"""
from pyflink.common import Row, RowKind
from pyflink.table import DataTypes
from pyflink.table.process_table_function import (
    ProcessTableFunction, ptf, argument_hint, state_hint,
    ArgumentTrait, StateKind,
)
from pyflink.table.table_environment import StreamTableEnvironment
from pyflink.testing.test_case_utils import PyFlinkStreamTableTestCase


# --------------------------------------------------------------------------- #
# Shared types and rendering helpers                                          #
# --------------------------------------------------------------------------- #

T_ROW = DataTypes.ROW([DataTypes.FIELD("name", DataTypes.STRING()),
                       DataTypes.FIELD("score", DataTypes.INT())])
OUT = DataTypes.ROW([DataTypes.FIELD("out", DataTypes.STRING())])
SCORE = DataTypes.ROW([DataTypes.FIELD("s", DataTypes.STRING()),
                       DataTypes.FIELD("i", DataTypes.INT())])

_ROWKIND_PREFIX = {
    RowKind.INSERT: "+I", RowKind.UPDATE_BEFORE: "-U",
    RowKind.UPDATE_AFTER: "+U", RowKind.DELETE: "-D",
}


def _fmt(value):
    if value is None:
        return "null"
    if isinstance(value, Row):
        kind = _ROWKIND_PREFIX.get(value.get_row_kind(), "+I")
        return "%s[%s]" % (kind, ", ".join(_fmt(v) for v in value))
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (list, tuple)):
        return "[%s]" % ", ".join(_fmt(v) for v in value)
    return str(value)


def _obj_str(objects):
    return "{" + ", ".join(_fmt(o) for o in objects) + "}"


# --------------------------------------------------------------------------- #
# PTF definitions (mirror ProcessTableFunctionTestUtils)                      #
# --------------------------------------------------------------------------- #

class ScalarArgsFunction(ProcessTableFunction):
    @argument_hint(name="i", trait=ArgumentTrait.SCALAR, scalar_type=DataTypes.INT())
    @argument_hint(name="b", trait=ArgumentTrait.SCALAR, scalar_type=DataTypes.BOOLEAN())
    def eval(self, ctx, i, b):
        ctx.collect(Row(_obj_str([i, b])))


class RowSemanticTableFunction(ProcessTableFunction):
    @argument_hint(name="r", trait=ArgumentTrait.ROW_SEMANTIC_TABLE, row_type=T_ROW)
    @argument_hint(name="i", trait=ArgumentTrait.SCALAR, scalar_type=DataTypes.INT())
    def eval(self, ctx, r, i):
        ctx.collect(Row(_obj_str([r, i])))


class SetSemanticTableFunction(ProcessTableFunction):
    @argument_hint(name="r", trait=ArgumentTrait.SET_SEMANTIC_TABLE,
                   row_type=T_ROW, partition_by=["name"])
    @argument_hint(name="i", trait=ArgumentTrait.SCALAR, scalar_type=DataTypes.INT())
    def eval(self, ctx, r, i):
        ctx.collect(Row(_obj_str([r, i])))


class PojoStateFunction(ProcessTableFunction):
    @state_hint(name="s", kind=StateKind.VALUE, value_type=SCORE)
    @argument_hint(name="r", trait=ArgumentTrait.SET_SEMANTIC_TABLE,
                   row_type=T_ROW, partition_by=["name"])
    def eval(self, ctx, s, r):
        cur = s.value()
        cs, ci = (cur[0], cur[1]) if cur is not None else (None, None)
        ctx.collect(Row(_obj_str(["Score(s='%s', i=%s)" % (
            "null" if cs is None else cs, "null" if ci is None else ci), r])))
        new_s = r[0] if r[0] == "Bob" else cs
        new_i = 0 if ci is None else ci + 1
        s.update(Row(new_s, new_i))


class ContextFunction(ProcessTableFunction):
    @argument_hint(name="r", trait=ArgumentTrait.SET_SEMANTIC_TABLE,
                   row_type=T_ROW, partition_by=["name"])
    @argument_hint(name="s", trait=ArgumentTrait.SCALAR, scalar_type=DataTypes.STRING())
    def eval(self, ctx, r, s):
        sem = ctx.table_semantics_for("r")
        ctx.collect(Row(_obj_str(
            [r, s, sem.partition_by_columns(), "[INSERT]",
             "ROW<`name` VARCHAR(5) NOT NULL, `score` INT NOT NULL>"])))


class MultiInputFunction(ProcessTableFunction):
    @argument_hint(name="in1", trait=ArgumentTrait.SET_SEMANTIC_TABLE,
                   row_type=T_ROW, partition_by=["name"])
    @argument_hint(name="in2", trait=ArgumentTrait.SET_SEMANTIC_TABLE,
                   row_type=DataTypes.ROW([DataTypes.FIELD("name", DataTypes.STRING()),
                                           DataTypes.FIELD("city", DataTypes.STRING())]),
                   partition_by=["name"],
                   extra_traits=[ArgumentTrait.OPTIONAL_PARTITION_BY])
    def eval(self, ctx, in1, in2):
        ctx.collect(Row(_obj_str([in1, in2])))


class MultiStateFunction(ProcessTableFunction):
    @state_hint(name="s1", kind=StateKind.VALUE,
                value_type=DataTypes.ROW([DataTypes.FIELD("i", DataTypes.INT())]))
    @state_hint(name="s2", kind=StateKind.VALUE,
                value_type=DataTypes.ROW([DataTypes.FIELD("s", DataTypes.STRING())]))
    @argument_hint(name="r", trait=ArgumentTrait.SET_SEMANTIC_TABLE,
                   row_type=T_ROW, partition_by=["name"])
    def eval(self, ctx, s1, s2, r):
        v1, v2 = s1.value(), s2.value()
        cur_i = v1[0] if v1 is not None else None
        cur_s = v2[0] if v2 is not None else None
        ctx.collect(Row(_obj_str([Row(cur_i), Row(cur_s), r])))
        i = 0 if cur_i is None else cur_i
        s1.update(Row(i + 1))
        s2.update(Row(str(i)))


class ListStateFunction(ProcessTableFunction):
    @state_hint(name="s", kind=StateKind.LIST, element_type=DataTypes.STRING())
    @argument_hint(name="r", trait=ArgumentTrait.SET_SEMANTIC_TABLE,
                   row_type=T_ROW, partition_by=["name"])
    def eval(self, ctx, s, r):
        cur = list(s.get())
        ctx.collect(Row(_obj_str([cur, "KeyedStateListView", r])))
        if len(cur) == 2:
            ctx.clear_state("s")
        else:
            s.add(str(len(cur)))


class MapStateFunction(ProcessTableFunction):
    @state_hint(name="s", kind=StateKind.MAP,
                key_type=DataTypes.STRING(), map_value_type=DataTypes.INT())
    @argument_hint(name="r", trait=ArgumentTrait.SET_SEMANTIC_TABLE,
                   row_type=T_ROW, partition_by=["name"])
    def eval(self, ctx, s, r):
        cur = dict(s.items())
        view = "{" + ", ".join("%s=%s" % (k, "null" if v is None else v)
                               for k, v in sorted(cur.items())) + "}"
        ctx.collect(Row(_obj_str([view, "KeyedStateMapViewWithKeysNotNull", r])))
        name = r[0]
        count = s.get(name) if s.contains(name) else 1
        s.put("old" + name, count)
        s.put(name, count + 1)
        s.put("nullValue", None)
        if count == 2:
            ctx.clear_state("s")


class ClearStateFunction(ProcessTableFunction):
    @state_hint(name="s", kind=StateKind.VALUE, value_type=SCORE)
    @argument_hint(name="r", trait=ArgumentTrait.SET_SEMANTIC_TABLE,
                   row_type=T_ROW, partition_by=["name"])
    def eval(self, ctx, s, r):
        cur = s.value()
        cs, ci = (cur[0], cur[1]) if cur is not None else (None, 99)
        ctx.collect(Row(_obj_str([
            "ScoreWithDefaults(s='%s', i=%s)" % ("null" if cs is None else cs, ci), r])))
        if r[0] == "Bob" and ci == 100:
            ctx.clear_state("s")
        else:
            s.update(Row(cs, ci + 1))


# --------------------------------------------------------------------------- #
# Test case                                                                   #
# --------------------------------------------------------------------------- #

class ProcessTableFunctionSqlTests(PyFlinkStreamTableTestCase):

    def setUp(self):
        # Fresh TableEnvironment per test so temporary views/functions named
        # 't'/'f' do not leak across tests sharing the class-level mini-cluster.
        self.t_env = StreamTableEnvironment.create(self.env)
        self.t_env.get_config().set("python.fn-execution.bundle.size", "1")
        # PTFs run only in process execution mode.
        self.t_env.get_config().set("python.execution-mode", "process")

    def _out_col(self, table):
        names = table.get_schema().get_field_names()
        oi = names.index("out")
        rows = list(table.execute().collect())
        return sorted(str(r[oi]) for r in rows)

    def _keyed_out(self, table):
        names = table.get_schema().get_field_names()
        oi = names.index("out")
        ki = 0 if names[0] != "out" else 1
        rows = list(table.execute().collect())
        return sorted("%s | %s" % (r[ki], str(r[oi])) for r in rows)

    def _basic_view(self, name="t"):
        self.t_env.execute_sql(
            "CREATE TEMPORARY VIEW %s AS SELECT * FROM "
            "(VALUES ('Bob', 12), ('Alice', 42)) AS T(name, score)" % name)

    def _multi_view(self, name="t"):
        self.t_env.execute_sql(
            "CREATE TEMPORARY VIEW %s AS SELECT * FROM (VALUES ('Bob', 12), "
            "('Alice', 42), ('Bob', 99), ('Bob', 100), ('Alice', 400)) "
            "AS T(name, score)" % name)

    def _city_view(self, name="city"):
        self.t_env.execute_sql(
            "CREATE TEMPORARY VIEW %s AS SELECT * FROM (VALUES ('Bob', 'London'), "
            "('Alice', 'Berlin'), ('Charly', 'Paris')) AS T(name, city)" % name)

    # ----- scenarios ---------------------------------------------------- #

    def test_scalar_args(self):
        self.t_env.create_temporary_function(
            "f", ptf(ScalarArgsFunction(), output_type=OUT))
        res = self.t_env.sql_query(
            "SELECT * FROM f(i => 42, b => TRUE)")
        self.assertEqual(self._out_col(res), ["{42, true}"])

    def test_row_semantic_table(self):
        self._basic_view()
        self.t_env.create_temporary_function(
            "f", ptf(RowSemanticTableFunction(), output_type=OUT))
        res = self.t_env.sql_query("SELECT * FROM f(r => TABLE t, i => 1)")
        self.assertEqual(self._out_col(res),
                         sorted(["{+I[Bob, 12], 1}", "{+I[Alice, 42], 1}"]))

    def test_set_semantic_table(self):
        self._basic_view()
        self.t_env.create_temporary_function(
            "f", ptf(SetSemanticTableFunction(), output_type=OUT))
        res = self.t_env.sql_query(
            "SELECT * FROM f(r => TABLE t PARTITION BY name, i => 1)")
        self.assertEqual(self._keyed_out(res),
                         sorted(["Bob | {+I[Bob, 12], 1}",
                                 "Alice | {+I[Alice, 42], 1}"]))

    def test_pojo_state(self):
        self._multi_view()
        self.t_env.create_temporary_function(
            "f", ptf(PojoStateFunction(), output_type=OUT))
        res = self.t_env.sql_query("SELECT * FROM f(r => TABLE t PARTITION BY name)")
        self.assertEqual(self._keyed_out(res), sorted([
            "Bob | {Score(s='null', i=null), +I[Bob, 12]}",
            "Alice | {Score(s='null', i=null), +I[Alice, 42]}",
            "Bob | {Score(s='Bob', i=0), +I[Bob, 99]}",
            "Bob | {Score(s='Bob', i=1), +I[Bob, 100]}",
            "Alice | {Score(s='null', i=0), +I[Alice, 400]}",
        ]))

    def test_context(self):
        self._basic_view()
        self.t_env.create_temporary_function(
            "f", ptf(ContextFunction(), output_type=OUT))
        res = self.t_env.sql_query(
            "SELECT * FROM f(r => TABLE t PARTITION BY name, s => 'param')")
        self.assertEqual(self._keyed_out(res), sorted([
            "Bob | {+I[Bob, 12], param, [0], [INSERT], "
            "ROW<`name` VARCHAR(5) NOT NULL, `score` INT NOT NULL>}",
            "Alice | {+I[Alice, 42], param, [0], [INSERT], "
            "ROW<`name` VARCHAR(5) NOT NULL, `score` INT NOT NULL>}",
        ]))

    def test_multi_input(self):
        self._multi_view("t")
        self._city_view("city")
        self.t_env.create_temporary_function(
            "f", ptf(MultiInputFunction(), output_type=OUT))
        res = self.t_env.sql_query(
            "SELECT * FROM f(in1 => TABLE t PARTITION BY name, "
            "in2 => TABLE city PARTITION BY name)")
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

    def test_multi_state(self):
        self._multi_view()
        self.t_env.create_temporary_function(
            "f", ptf(MultiStateFunction(), output_type=OUT))
        res = self.t_env.sql_query("SELECT * FROM f(r => TABLE t PARTITION BY name)")
        self.assertEqual(self._keyed_out(res), sorted([
            "Bob | {+I[null], +I[null], +I[Bob, 12]}",
            "Alice | {+I[null], +I[null], +I[Alice, 42]}",
            "Bob | {+I[1], +I[0], +I[Bob, 99]}",
            "Bob | {+I[2], +I[1], +I[Bob, 100]}",
            "Alice | {+I[1], +I[0], +I[Alice, 400]}",
        ]))

    def test_list_state(self):
        self._multi_view()
        self.t_env.create_temporary_function(
            "f", ptf(ListStateFunction(), output_type=OUT))
        res = self.t_env.sql_query("SELECT * FROM f(r => TABLE t PARTITION BY name)")
        self.assertEqual(self._keyed_out(res), sorted([
            "Bob | {[], KeyedStateListView, +I[Bob, 12]}",
            "Alice | {[], KeyedStateListView, +I[Alice, 42]}",
            "Bob | {[0], KeyedStateListView, +I[Bob, 99]}",
            "Bob | {[0, 1], KeyedStateListView, +I[Bob, 100]}",
            "Alice | {[0], KeyedStateListView, +I[Alice, 400]}",
        ]))

    def test_map_state(self):
        self._multi_view()
        self.t_env.create_temporary_function(
            "f", ptf(MapStateFunction(), output_type=OUT))
        res = self.t_env.sql_query("SELECT * FROM f(r => TABLE t PARTITION BY name)")
        self.assertEqual(self._keyed_out(res), sorted([
            "Bob | {{}, KeyedStateMapViewWithKeysNotNull, +I[Bob, 12]}",
            "Alice | {{}, KeyedStateMapViewWithKeysNotNull, +I[Alice, 42]}",
            "Bob | {{Bob=2, nullValue=null, oldBob=1}, "
            "KeyedStateMapViewWithKeysNotNull, +I[Bob, 99]}",
            "Bob | {{}, KeyedStateMapViewWithKeysNotNull, +I[Bob, 100]}",
            "Alice | {{Alice=2, nullValue=null, oldAlice=1}, "
            "KeyedStateMapViewWithKeysNotNull, +I[Alice, 400]}",
        ]))

    def test_clear_state(self):
        self._multi_view()
        self.t_env.create_temporary_function(
            "f", ptf(ClearStateFunction(), output_type=OUT))
        res = self.t_env.sql_query("SELECT * FROM f(r => TABLE t PARTITION BY name)")
        self.assertEqual(self._keyed_out(res), sorted([
            "Bob | {ScoreWithDefaults(s='null', i=99), +I[Bob, 12]}",
            "Alice | {ScoreWithDefaults(s='null', i=99), +I[Alice, 42]}",
            "Bob | {ScoreWithDefaults(s='null', i=100), +I[Bob, 99]}",
            "Bob | {ScoreWithDefaults(s='null', i=99), +I[Bob, 100]}",
            "Alice | {ScoreWithDefaults(s='null', i=100), +I[Alice, 400]}",
        ]))


if __name__ == "__main__":
    import unittest
    unittest.main()
