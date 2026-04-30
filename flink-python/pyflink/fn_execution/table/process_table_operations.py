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
Worker-side dispatch for Process Table Functions in process mode.

Receives wrapper rows (PtfInputRow) from the Beam runner, demuxes them by
``argument_id`` to populate ``eval``'s table arguments, binds the per-call
:class:`Context` and state proxies, invokes user code, and drains ``collect``
output. Also handles timer-fire side-channel events that route into
``on_timer``.
"""
from __future__ import annotations

from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

import cloudpickle

from pyflink.fn_execution.datastream.operations import Operation
from pyflink.fn_execution.metrics.process.metric_impl import GenericMetricGroup
from pyflink.fn_execution.table.process_table_state import (
    PtfStateSlotRegistry, build_state_spec_handles,
)
from pyflink.fn_execution.table.process_table_time import (
    _ContextImpl, _OnTimerContextImpl, _TimeContextImpl,
    build_table_semantics_views,
)


# PtfInputRow := (arg_id, row_kind, event_time_ms, watermark_ms,
#                 partition_key, user_row)
_PTF_INPUT_ARG_ID = 0
_PTF_INPUT_ROW_KIND = 1
_PTF_INPUT_EVENT_TIME = 2
_PTF_INPUT_WATERMARK = 3
_PTF_INPUT_PARTITION_KEY = 4
_PTF_INPUT_USER_ROW = 5

# PtfTimerFire is the 5-field Row built by Java's TimerHandler.buildTimerData:
# (timer_type_byte, watermark_ms, timestamp_ms, key_row, encoded_namespace).
_PTF_TIMER_DOMAIN = 0       # byte: 0=event_time, 1=processing_time
_PTF_TIMER_WATERMARK = 1    # long
_PTF_TIMER_TIMESTAMP = 2    # long
_PTF_TIMER_KEY = 3          # Row (partition key)
_PTF_TIMER_NAMESPACE = 4    # bytes (encoded namespace; null for VoidNamespace)


# Wire bytes for PtfTimerRegistration operand types (mirror of the Java enum).
# CLEAR_ALL_TIMERS is added on top of the standard TimerRegistration ops to
# drain all pending timers for the current partition.
_TIMER_OP_BYTES = {
    'REGISTER_EVENT_TIMER': 0,
    'REGISTER_PROC_TIMER': 1,
    'DELETE_EVENT_TIMER': 2,
    'DELETE_PROC_TIMER': 3,
    'CLEAR_ALL_TIMERS': 4,
}


class ProcessTableFunctionOperation(Operation):
    """
    Worker-side operator that runs a Python ``ProcessTableFunction``.

    Multi-input demultiplexing: every input element carries an ``arg_id``
    identifying which table argument it came from. The operation builds a
    positional argument tuple where exactly one position is the user row and
    the rest are ``None`` — matching the Java PTF dispatch convention.
    """

    def __init__(self, serialized_fn, keyed_state_backend):
        self._spec = serialized_fn
        self._keyed_state_backend = keyed_state_backend

        # Metric group setup.
        if serialized_fn.metric_enabled:
            self._base_metric_group = GenericMetricGroup(None, None)
        else:
            self._base_metric_group = None

        self._job_parameters = {p.key: p.value
                                for p in serialized_fn.job_parameters}

        self._user_func = cloudpickle.loads(serialized_fn.payload)
        self._uid = serialized_fn.uid
        self._has_on_timer = serialized_fn.has_on_timer

        # Argument layout: table args (positional) vs. scalar constants (bound
        # at registration time, not flowing through the data plane).
        self._arg_layout: List[_ArgLayoutEntry] = _build_arg_layout(
            serialized_fn.arguments)

        self._state_handles = build_state_spec_handles(
            serialized_fn.state_slots)

        # Runtime objects, created in open().
        self._state_registry: Optional[PtfStateSlotRegistry] = None
        self._context: Optional[_ContextImpl] = None
        self._on_timer_context: Optional[_OnTimerContextImpl] = None
        self._time_context: Optional[_TimeContextImpl] = None
        self._output_emitter: Optional[Callable[[Any], None]] = None
        # Timer-set side channel, installed by the add_timer_info runner hook.
        self._timer_coder_impl = None
        self._timer_output_stream = None
        # Partition key of the currently-dispatched element; consulted by the
        # timer-set emitter to populate the wire row.
        self._current_partition_key: Optional[List[Any]] = None

    def open(self):
        """Bundle-level setup; builds the Context family and runs user open()."""
        time_type = _resolve_time_type(self._spec.arguments)

        self._time_context = _TimeContextImpl(time_type, self._emit_timer)
        emit_timer = self._emit_timer

        # Each table arg carries its row type as a Schema.FieldType; convert it
        # back to a DataType so user code can read table_semantics_for().data_type().
        from pyflink.fn_execution.coders import LengthPrefixBaseCoder
        row_types_by_name: Dict[str, Any] = {}
        for arg in self._spec.arguments:
            if arg.WhichOneof('kind') != 'table':
                continue
            row_types_by_name[arg.name] = LengthPrefixBaseCoder._to_data_type(
                arg.table.row_type)
        table_semantics = build_table_semantics_views(
            self._spec.arguments, row_types_by_name)

        self._state_registry = PtfStateSlotRegistry(
            uid=self._uid or "py_ptf_default",
            state_specs=self._state_handles,
            keyed_state_backend=self._keyed_state_backend,
        )

        # Calcite expands the PTF output schema to [partition_key columns (one
        # group per SET_SEMANTIC input with PARTITION BY), user output columns,
        # rowtime column (if any table arg has on_time bound)]. Co-partitioning
        # makes the per-input partition keys equal, so the worker emits the same
        # value once per SET_SEMANTIC input.
        from pyflink.fn_execution.flink_fn_execution_pb2 import (
            UserDefinedProcessTableFunction as _Pb,
        )
        self._n_partition_repeats = sum(
            1 for arg in self._spec.arguments
            if arg.WhichOneof('kind') == 'table'
            and arg.table.semantics == _Pb.SET_SEMANTIC
            and len(arg.table.partition_by_columns) > 0
        )
        has_on_time = any(
            arg.WhichOneof('kind') == 'table'
            and arg.table.on_time_column >= 0
            for arg in self._spec.arguments
        )

        # Heterogeneous multi-input: when the Java operator detects different
        # RowTypes across inputs, user_row arrives in a widened wire-format
        # (all inputs' fields concatenated; inactive slots NULL). The per-arg
        # row_type lets the worker project back to the active arg's shape before
        # eval. Homogeneous / single-input PTFs leave _arg_field_offsets empty.
        self._is_widened_user_row = False
        self._arg_field_offsets: List[int] = []
        self._arg_field_lengths: List[int] = []
        self._arg_field_names: List[List[str]] = []
        table_args = [a for a in self._spec.arguments
                      if a.WhichOneof('kind') == 'table']
        if len(table_args) >= 2:
            first = table_args[0].table.row_type
            heterogeneous = any(
                a.table.row_type.SerializeToString() != first.SerializeToString()
                for a in table_args[1:]
            )
            if heterogeneous:
                self._is_widened_user_row = True
                offset = 0
                for a in table_args:
                    fields = list(a.table.row_type.row_schema.fields)
                    self._arg_field_offsets.append(offset)
                    self._arg_field_lengths.append(len(fields))
                    self._arg_field_names.append([f.name for f in fields])
                    offset += len(fields)

        # PASS_COLUMNS_THROUGH: when set on a table arg, Calcite expands the
        # PTF output schema to [arg_columns..., user_output...]. The worker
        # therefore needs to prepend the *full* input row to each emitted row
        # rather than the partition-key projection. We resolve the prepend
        # path once at open() and stash the column indices on the operation.
        # PASS_COLUMNS_THROUGH is mutually exclusive with timers + SET_SEMANTIC
        # partitioning (see PTF docs limitations).
        self._pass_through_columns: Optional[List[int]] = None
        for arg in self._spec.arguments:
            if arg.WhichOneof('kind') != 'table':
                continue
            if list(arg.table.pass_through_columns):
                self._pass_through_columns = list(arg.table.pass_through_columns)
                break

        # ChangelogFunction PTFs emit Row objects to preserve user-set RowKind
        # through the wire; INSERT_ONLY PTFs emit plain lists (FlattenRowCoder
        # fast path). The proto's emits_changelog enum drives this — 0 is
        # INSERT_ONLY.
        emits_changelog_value = int(self._spec.emits_changelog)
        emits_changelog_is_insert_only = emits_changelog_value == 0
        self._context = _ContextImpl(
            time_context=self._time_context,
            table_semantics=table_semantics,
            state_registry=self._state_registry,
            timer_emitter=emit_timer,
            has_on_time=has_on_time,
            emit_as_row=not emits_changelog_is_insert_only,
            n_partition_repeats=self._n_partition_repeats,
        )
        self._on_timer_context = _OnTimerContextImpl(self._context)

        from pyflink.table.udf import FunctionContext
        function_context = FunctionContext(
            self._base_metric_group, self._job_parameters)
        self._user_func.open(function_context)

    def close(self):
        if self._user_func is not None:
            self._user_func.close()

    def finish(self):
        """Flush pending state, commit to the Java backend, push metrics.

        proxy._flush() only writes to Beam's in-memory _added_elements buffer;
        the writes reach the Java keyed-state backend (and thus checkpoints /
        savepoints) only on commit(). Combined with the per-key-switch commit
        in PtfStateSlotRegistry.set_current_key, this captures the current key's
        last write at bundle finish.
        """
        if self._state_registry is not None:
            self._state_registry.flush()
        if self._keyed_state_backend is not None:
            self._keyed_state_backend.commit()
        self._update_metric_gauges(self._base_metric_group)

    def add_timer_info(self, timer_info) -> None:
        """
        Beam-runner hook invoked once per bundle to install the timer
        side-channel codec and output stream. Without it, the user's
        register_on_time(...) calls are dropped.
        """
        self._timer_coder_impl = timer_info.timer_coder_impl
        self._timer_output_stream = timer_info.output_stream

    def _emit_timer(self, op: str, name: str, ts_ms: int) -> None:
        """
        Encode a timer-set/delete request and push it onto the Beam timer side
        channel. Wire format (matches Java TimerRegistration.setTimer):
        Row(operand_byte, watermark=-1, timestamp_ms, key_row, encoded_namespace).
        """
        if self._timer_coder_impl is None or self._timer_output_stream is None:
            # Bundle not wired yet; open() runs first in normal operation.
            return
        if self._current_partition_key is None:
            raise RuntimeError(
                "PTF timer registered with no current partition key set; "
                "register_on_time may only be called from eval()/on_timer "
                "for SET_SEMANTIC PTFs.")
        op_byte = _TIMER_OP_BYTES.get(op)
        if op_byte is None:
            raise ValueError(f"Unknown PTF timer op: {op!r}")
        from apache_beam.transforms import userstate
        from apache_beam.transforms.window import GlobalWindow
        from pyflink.common import Row

        # Always emit the 4-byte big-endian length prefix — even for unnamed
        # timers (length 0) — so the Java Utf8LengthPrefixedStringSerializer can
        # readInt without an EOFException on an empty buffer.
        utf8 = name.encode('utf-8') if name else b''
        encoded_namespace = len(utf8).to_bytes(4, 'big', signed=False) + utf8
        key_row = Row(*self._current_partition_key)
        timer_data = Row(op_byte, -1, ts_ms, key_row, encoded_namespace)

        timer = userstate.Timer(
            user_key=timer_data,
            dynamic_timer_tag='',
            windows=(GlobalWindow(),),
            clear_bit=True,
            fire_timestamp=None,
            hold_timestamp=None,
            paneinfo=None)
        self._timer_coder_impl.encode_to_stream(
            timer, self._timer_output_stream, True)
        # Force-flush so the Java side sees the timer-set request immediately
        # rather than at bundle close.
        self._timer_coder_impl._key_coder_impl._value_coder._output_stream.maybe_flush()

    def process_element(self, value):
        """
        Dispatch a single PtfInputRow to ``eval``. Returns an iterable of
        emitted output rows (Beam reads it per-yield).
        """
        arg_id = value[_PTF_INPUT_ARG_ID]
        row_kind = value[_PTF_INPUT_ROW_KIND]
        event_time_ms = value[_PTF_INPUT_EVENT_TIME]
        watermark_ms = value[_PTF_INPUT_WATERMARK]
        partition_key = value[_PTF_INPUT_PARTITION_KEY]
        user_row = value[_PTF_INPUT_USER_ROW]

        if partition_key is not None:
            # The wire delivers partition_key as a Row; downstream expects a
            # list/tuple.
            if not isinstance(partition_key, (list, tuple)):
                partition_key = list(partition_key)
            self._state_registry.set_current_key(partition_key)
            # Stash for the timer-set emitter (register_on_time needs it).
            self._current_partition_key = list(partition_key)

        # Heterogeneous multi-input: project the widened user_row back to the
        # active arg's declared shape, rebuilding a Row with the user's original
        # field names so attribute / index access works as on a native row.
        if self._is_widened_user_row and user_row is not None:
            from pyflink.common.types import Row as _PyRow
            offset = self._arg_field_offsets[arg_id]
            length = self._arg_field_lengths[arg_id]
            names = self._arg_field_names[arg_id]
            values = list(user_row)[offset:offset + length]
            projected = _PyRow(*values)
            projected._fields = list(names)
            if hasattr(user_row, 'get_row_kind') and hasattr(projected, 'set_row_kind'):
                projected.set_row_kind(user_row.get_row_kind())
            user_row = projected

        if hasattr(user_row, 'set_row_kind'):
            user_row.set_row_kind(_decode_row_kind(row_kind))

        # PASS_COLUMNS_THROUGH routes the full input row through the same
        # _bind_call prepend slot as the SET_SEMANTIC partition key (they are
        # mutually exclusive at the planner level). Calcite emits pass-through
        # columns once, vs. partition keys once per SET_SEMANTIC input.
        prepend_for_collect: Any = partition_key
        prepend_repeat = self._n_partition_repeats
        if self._pass_through_columns is not None and user_row is not None:
            prepend_for_collect = [user_row[i] for i in self._pass_through_columns]
            prepend_repeat = 1

        self._context._bind_call(
            event_time_ms, watermark_ms, prepend_for_collect, prepend_repeat)

        call_args = self._build_eval_args(arg_id, user_row)
        state_proxies = self._state_registry.bind()
        self._user_func.eval(self._context, *state_proxies, *call_args)

        self._state_registry.flush()
        return self._context._drain_collected()

    def process_timer(self, timer_data):
        """Dispatch a timer fire to ``on_timer``.

        timer_data is the 5-field Row built by Java's TimerHandler.buildTimerData:
        (timer_type_byte, watermark_ms, timestamp_ms, key_row, encoded_namespace).
        """
        if not self._has_on_timer:
            return ()
        ts_ms = timer_data[_PTF_TIMER_TIMESTAMP]
        watermark_ms = timer_data[_PTF_TIMER_WATERMARK]
        key = timer_data[_PTF_TIMER_KEY]
        # The timer name is length-prefixed UTF-8; unnamed timers arrive empty,
        # decoded to None for current_timer().
        ns_bytes = timer_data[_PTF_TIMER_NAMESPACE]
        timer_name = None
        if ns_bytes and len(ns_bytes) >= 4:
            ns_len = int.from_bytes(ns_bytes[:4], 'big', signed=False)
            if ns_len > 0:
                timer_name = ns_bytes[4:4 + ns_len].decode('utf-8')

        partition_key_list: Optional[List[Any]] = None
        if key is not None:
            if not isinstance(key, (list, tuple)):
                key = list(key)
            partition_key_list = list(key)
            self._state_registry.set_current_key(partition_key_list)
            self._current_partition_key = partition_key_list

        self._on_timer_context._bind_fire(
            timer_name, ts_ms, watermark_ms, partition_key_list)
        state_proxies = self._state_registry.bind()
        self._user_func.on_timer(self._on_timer_context, *state_proxies)

        self._state_registry.flush()
        return self._context._drain_collected()

    def _build_eval_args(self, active_arg_id: int, user_row: Any) -> List[Any]:
        """
        Positional argument list for ``eval``: scalar args first (in
        declaration order), then table args, all-None except ``active_arg_id``.
        """
        result: List[Any] = []
        for entry in self._arg_layout:
            if entry.kind == 'SCALAR':
                result.append(entry.value)
            elif entry.kind == 'TABLE':
                if entry.position == active_arg_id:
                    result.append(user_row)
                else:
                    result.append(None)
            else:
                raise ValueError(f"Unknown arg kind: {entry.kind}")
        return result

    def _update_metric_gauges(self, base_metric_group):
        if base_metric_group is None:
            return
        for name, fn in base_metric_group._flink_gauge.items():
            base_metric_group._beam_gauge[name].set(fn())
        for sub in base_metric_group._sub_groups:
            self._update_metric_gauges(sub)


class _ArgLayoutEntry:
    __slots__ = ('name', 'kind', 'position', 'value')

    def __init__(self, name: str, kind: str, position: int, value: Any = None):
        self.name = name
        # kind in {"SCALAR", "TABLE"}.
        self.kind = kind
        # For TABLE: position == runner arg_id. For SCALAR: -1.
        self.position = position
        # For SCALAR: bound literal baked in by the planner, else None.
        self.value = value


def _decode_bound_scalar(raw: bytes) -> Any:
    """
    Decode the wire format produced by ``CommonPythonUtil.convertLiteralToPython``:
    byte[0] is the j_type tag (0=basic, 1=DATE, 2=TIME, 3=TIMESTAMP) and bytes[1:]
    are razorvine-Pickler payload. Mirrors ``_parse_constant_value`` (which is
    used for plain-UDF inputConstant) but returns just the value.
    """
    import datetime
    from pyflink.serializers import PickleSerializer
    if not raw:
        return None
    j_type = raw[0]
    pickled = PickleSerializer().loads(raw[1:])
    if j_type == 0:
        return pickled
    if j_type == 1:
        return datetime.date(year=1970, month=1, day=1) + datetime.timedelta(days=pickled)
    if j_type == 2:
        seconds, ms = divmod(pickled, 1000)
        minutes, seconds = divmod(seconds, 60)
        hours, minutes = divmod(minutes, 60)
        return datetime.time(hours, minutes, seconds, ms * 1000)
    if j_type == 3:
        return (datetime.datetime(year=1970, month=1, day=1)
                + datetime.timedelta(milliseconds=pickled))
    raise ValueError(f"Unknown PTF scalar j_type {j_type}")


def _build_arg_layout(pb_arguments) -> List[_ArgLayoutEntry]:
    """
    Walk the proto args and assign a stable arg_id to each table arg.
    Scalar args get position=-1; their bound value (if any) is decoded from
    ``arg.scalar.bound_value`` and stashed on ``entry.value``.

    The arg_id assigned here MUST match what the Java side emits in
    PtfInputRow's argument_id field.
    """
    entries: List[_ArgLayoutEntry] = []
    table_pos = 0
    for arg in pb_arguments:
        kind_oneof = arg.WhichOneof('kind')
        if kind_oneof == 'table':
            entries.append(_ArgLayoutEntry(arg.name, 'TABLE', table_pos))
            table_pos += 1
        elif kind_oneof == 'scalar':
            value = _decode_bound_scalar(arg.scalar.bound_value)
            entries.append(_ArgLayoutEntry(arg.name, 'SCALAR', -1, value))
        else:
            raise ValueError(
                f"Argument '{arg.name}' has no kind set (neither scalar nor "
                f"table); proto is malformed."
            )
    return entries


def _resolve_time_type(pb_arguments) -> int:
    """
    Find the bound on_time TimeType across all table args, if any.

    Java PTF semantics: at most one table arg has a bound on_time descriptor.
    Returns the proto enum int (NO_TIME if none).
    """
    from pyflink.fn_execution.flink_fn_execution_pb2 import (
        UserDefinedProcessTableFunction as Pb,
    )
    for arg in pb_arguments:
        if arg.WhichOneof('kind') == 'table' and arg.table.on_time_column >= 0:
            return arg.table.time_type
    return Pb.NO_TIME


def _decode_row_kind(byte_value: int):
    """Map the wire RowKind byte to ``pyflink.common.RowKind``."""
    from pyflink.common import RowKind
    return {
        0: RowKind.INSERT,
        1: RowKind.UPDATE_BEFORE,
        2: RowKind.UPDATE_AFTER,
        3: RowKind.DELETE,
    }.get(byte_value, RowKind.INSERT)
