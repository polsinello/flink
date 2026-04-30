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
TimeContext / OnTimerContext / Context concrete implementations for the PTF
process-mode runtime. They bind the in-band watermark/event-time from the
wrapper Row, the Beam timer side channel for register/delete/clearAll, and the
user-facing time-type conversion (Instant / LocalDateTime / Long).
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable, Generic, List, Optional, TypeVar, Union

from pyflink.table.process_table_function import (
    Context, OnTimerContext, TimeContext, TableSemanticsView, PTFChangelogMode,
)
from pyflink.table.types import DataType, RowType

T = TypeVar('T')


# Mirror of UserDefinedProcessTableFunction.TimeType proto enum.
_TIME_TYPE_NO_TIME = 0
_TIME_TYPE_INSTANT = 1
_TIME_TYPE_LOCAL_DATE_TIME = 2
_TIME_TYPE_LONG_MILLIS = 3

# datetime only spans years 1..9999, but Flink delivers Long.MAX_VALUE
# (Watermark.MAX_WATERMARK) at end-of-stream / when sources go idle. Clamp
# anything past the representable range to datetime.max instead of crashing.
_MAX_REPRESENTABLE_MS = 253402300799000  # 9999-12-31T23:59:59 UTC


def _ms_to_user(time_type: int, ms: int) -> Any:
    """Convert epoch-millis to the user's chosen time type."""
    if ms < 0:
        return None
    if time_type == _TIME_TYPE_LONG_MILLIS:
        return ms
    if time_type == _TIME_TYPE_LOCAL_DATE_TIME:
        if ms >= _MAX_REPRESENTABLE_MS:
            return datetime.max
        return datetime.utcfromtimestamp(ms / 1000.0)
    if time_type == _TIME_TYPE_INSTANT:
        # PyFlink does not currently ship a java.time.Instant analog; use
        # an aware UTC datetime as the closest stdlib match.
        if ms >= _MAX_REPRESENTABLE_MS:
            return datetime.max.replace(tzinfo=timezone.utc)
        return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
    if time_type == _TIME_TYPE_NO_TIME:
        return None
    raise ValueError(f"Unknown PTF TimeType: {time_type}")


def _user_to_ms(time_type: int, user_value: Any) -> int:
    """Convert a user-supplied timestamp to epoch-millis (long)."""
    if isinstance(user_value, int):
        return user_value
    if isinstance(user_value, datetime):
        if user_value.tzinfo is None:
            # Treat naive datetimes as UTC, matching the convention chosen
            # for LOCAL_DATE_TIME above.
            return int(user_value.replace(tzinfo=timezone.utc).timestamp() * 1000)
        return int(user_value.timestamp() * 1000)
    raise TypeError(
        f"PTF TimeContext expected long/int millis or datetime, got "
        f"{type(user_value).__name__}"
    )


class _TableSemanticsView(TableSemanticsView):
    """
    Concrete TableSemanticsView for a single table argument. Built once per PTF
    instance from the proto and reused across calls.
    """

    __slots__ = ('_partition_columns', '_time_column', '_data_type',
                 '_changelog_mode')

    def __init__(self,
                 partition_columns: List[int],
                 time_column: int,
                 data_type: DataType,
                 changelog_mode: PTFChangelogMode):
        self._partition_columns = partition_columns
        self._time_column = time_column
        self._data_type = data_type
        self._changelog_mode = changelog_mode

    def partition_by_columns(self) -> List[int]:
        return list(self._partition_columns)

    def time_column(self) -> int:
        return self._time_column

    def data_type(self) -> DataType:
        return self._data_type

    def changelog_mode(self) -> PTFChangelogMode:
        return self._changelog_mode


class _TimeContextImpl(TimeContext, Generic[T]):
    """
    Bound TimeContext for a single eval/onTimer call. The owning Context
    updates ``_event_time_ms`` / ``_watermark_ms`` per call; conversions happen
    on demand. Timer set/delete operations route through ``_timer_emitter``.
    """

    __slots__ = ('_time_type', '_event_time_ms', '_watermark_ms',
                 '_timer_emitter')

    def __init__(self,
                 time_type: int,
                 timer_emitter: Callable[[str, str, int], None]):
        self._time_type = time_type
        self._event_time_ms: int = -1
        self._watermark_ms: int = -1
        # _timer_emitter(op_type, name, ts_ms) enqueues a timer op on the wire.
        self._timer_emitter = timer_emitter

    def _set_event_time_ms(self, ms: int) -> None:
        self._event_time_ms = ms

    def _set_watermark_ms(self, ms: int) -> None:
        self._watermark_ms = ms

    # ----- TimeContext API ----- #

    def time(self) -> T:
        return _ms_to_user(self._time_type, self._event_time_ms)

    def current_watermark(self) -> T:
        return _ms_to_user(self._time_type, self._watermark_ms)

    def register_on_time(self,
                         name_or_timestamp: Union[str, T],
                         timestamp: Optional[T] = None) -> None:
        if timestamp is None:
            # Single-arg form: unnamed timer.
            ts_ms = _user_to_ms(self._time_type, name_or_timestamp)
            self._timer_emitter('REGISTER_EVENT_TIMER', '', ts_ms)
        else:
            # Two-arg form: named timer.
            if not isinstance(name_or_timestamp, str):
                raise TypeError(
                    "register_on_time(name, ts): first arg must be a "
                    "non-empty string name."
                )
            if not name_or_timestamp:
                raise ValueError(
                    "register_on_time(name, ts): name must be non-empty. "
                    "Use the single-argument form for unnamed timers."
                )
            ts_ms = _user_to_ms(self._time_type, timestamp)
            self._timer_emitter('REGISTER_EVENT_TIMER',
                                name_or_timestamp, ts_ms)

    def register_on_proc_time(self,
                              name_or_timestamp: Union[str, int],
                              timestamp: Optional[int] = None) -> None:
        """
        Register a processing-time timer. Mirrors ``register_on_time`` but
        ticks on wall-clock advances rather than watermark progress.

        Single-arg form: unnamed timer at ``name_or_timestamp`` (epoch-millis).
        Two-arg form: named timer with ``name_or_timestamp`` as the name.
        """
        if timestamp is None:
            ts_ms = int(name_or_timestamp)
            self._timer_emitter('REGISTER_PROC_TIMER', '', ts_ms)
        else:
            if not isinstance(name_or_timestamp, str) or not name_or_timestamp:
                raise ValueError(
                    "register_on_proc_time(name, ts): name must be non-empty.")
            self._timer_emitter('REGISTER_PROC_TIMER',
                                name_or_timestamp, int(timestamp))

    def clear_timer(self, name_or_timestamp: Union[str, T]) -> None:
        if isinstance(name_or_timestamp, str):
            if not name_or_timestamp:
                raise ValueError("clear_timer(name): name must be non-empty.")
            # Sentinel timestamp -1 — Java side resolves the actual ts from the
            # named-timer MapState before deletion.
            self._timer_emitter('DELETE_EVENT_TIMER', name_or_timestamp, -1)
        else:
            # Unnamed timer deleted by its firing time (Java clearTimer(T)).
            ts_ms = _user_to_ms(self._time_type, name_or_timestamp)
            self._timer_emitter('DELETE_EVENT_TIMER', '', ts_ms)

    def clear_all_timers(self) -> None:
        self._timer_emitter('CLEAR_ALL_TIMERS', '', -1)

    def _with_time_type(self, time_type: int) -> '_TimeContextImpl':
        """Return a view of this TimeContext re-typed to ``time_type``.

        Shares the live event-time / watermark pointers with this instance
        (per-call updates flow through both), so a user-requested explicit
        time class observes the same timestamps converted differently.
        """
        if time_type == self._time_type:
            return self
        view = _TimeContextImpl(time_type, self._timer_emitter)
        view._event_time_ms = self._event_time_ms
        view._watermark_ms = self._watermark_ms
        return view


def _time_type_for_class(time_class: Any) -> int:
    """Map a user-supplied Python time class to the proto TimeType enum."""
    if time_class is int:
        return _TIME_TYPE_LONG_MILLIS
    if time_class is datetime:
        return _TIME_TYPE_LOCAL_DATE_TIME
    raise TypeError(
        f"time_context(time_class): unsupported time class {time_class!r}; "
        f"use int (epoch millis) or datetime.datetime."
    )


class _ContextImpl(Context):
    """
    Concrete Context handed to ``ProcessTableFunction.eval`` calls.

    Lifecycle:
      - Constructed once per PTF operator instance.
      - ``_bind_call(...)`` is invoked before each ``eval`` call to refresh
        time/state pointers.
      - ``collect`` accumulates output rows into ``_collected``; the operator
        drains them at the end of each element.
    """

    def __init__(self,
                 time_context: _TimeContextImpl,
                 table_semantics: dict,
                 state_registry,
                 timer_emitter: Callable[[str, str, int], None],
                 has_on_time: bool = True,
                 emit_as_row: bool = False,
                 n_partition_repeats: int = 1):
        self._time_context = time_context
        # Maps argument name -> _TableSemanticsView.
        self._table_semantics = table_semantics
        self._state_registry = state_registry
        self._timer_emitter = timer_emitter
        self._collected: List[Any] = []
        # Per-call output-row context, refreshed by _bind_call.
        self._current_partition_key: Any = None
        self._current_event_time_ms: Any = None
        # How many times to repeat the partition_key prefix in the output row:
        # Calcite duplicates the partition columns once per SET_SEMANTIC input,
        # and 1 for PASS_COLUMNS_THROUGH.
        self._current_prepend_repeat: int = 1
        self._n_partition_repeats = max(1, n_partition_repeats)
        # Whether the planner appended an on_time column to the output schema
        # (iff at least one table arg has on_time bound).
        self._has_on_time = has_on_time
        # When True, collect emits pyflink.common.Row objects so the Java
        # RowCoder preserves the user-set RowKind (ChangelogFunction PTFs);
        # otherwise plain lists for the FlattenRowCoder fast path.
        self._emit_as_row = emit_as_row

    def _bind_call(self, event_time_ms: int, watermark_ms: int,
                   partition_key: Any = None,
                   prepend_repeat: int = 1) -> None:
        self._time_context._set_event_time_ms(event_time_ms)
        self._time_context._set_watermark_ms(watermark_ms)
        self._current_partition_key = partition_key
        self._current_event_time_ms = event_time_ms
        self._current_prepend_repeat = max(1, prepend_repeat)
        self._collected.clear()

    def _drain_collected(self) -> List[Any]:
        out, self._collected = self._collected, []
        return out

    def collect(self, row: Any) -> None:
        # Normalise the user value to a list of fields, capturing the RowKind
        # from a pyflink.common.Row (defaults to INSERT for tuple/list/scalar).
        from pyflink.common import Row as _PyflinkRow
        from pyflink.common.types import RowKind as _RowKind
        emitted_kind = _RowKind.INSERT
        if isinstance(row, _PyflinkRow):
            try:
                emitted_kind = row.get_row_kind()
            except AttributeError:
                pass
            row = list(row)
        elif isinstance(row, tuple):
            row = list(row)
        elif not isinstance(row, list):
            row = [row]
        # An INSERT_ONLY PTF (emit_as_row False) may only produce INSERT rows;
        # reject other kinds the way the Java runtime does.
        if emitted_kind != _RowKind.INSERT and not self._emit_as_row:
            raise RuntimeError(
                "Invalid row kind received: %s. Expected produced changelog "
                "mode: [INSERT]" % emitted_kind.name)
        # Output schema is [partition_key × prepend_repeat, user_output,
        # on_time?]. Calcite duplicates the partition columns once per
        # SET_SEMANTIC input; co-partitioning makes them equal, so emit the
        # same value per repeat.
        out_fields: List[Any] = []
        if self._current_partition_key is not None:
            for _ in range(self._current_prepend_repeat):
                out_fields.extend(self._current_partition_key)
        out_fields.extend(row)
        if self._has_on_time:
            ms = self._current_event_time_ms
            out_fields.append(
                None if ms is None else _ms_to_user(_TIME_TYPE_INSTANT, ms))
        if self._emit_as_row:
            out_row = _PyflinkRow(*out_fields)
            out_row.set_row_kind(emitted_kind)
            self._collected.append(out_row)
        else:
            self._collected.append(out_fields)

    def time_context(self, time_class: Any = None) -> TimeContext:
        if time_class is None:
            return self._time_context
        return self._time_context._with_time_type(_time_type_for_class(time_class))

    def table_semantics_for(self, argument_name: str) -> TableSemanticsView:
        ts = self._table_semantics.get(argument_name)
        if ts is None:
            raise KeyError(
                f"No table argument named '{argument_name}'. Available: "
                f"{list(self._table_semantics)}"
            )
        return ts

    def clear_state(self, state_name: str) -> None:
        self._state_registry.clear_state(state_name)

    def clear_all_state(self) -> None:
        self._state_registry.clear_all_state()

    def clear_all_timers(self) -> None:
        self._timer_emitter('CLEAR_ALL_TIMERS', '', -1)


class _OnTimerContextImpl(OnTimerContext):
    """OnTimerContext used during ``on_timer`` callbacks."""

    def __init__(self, base_context: _ContextImpl):
        self._base = base_context
        self._current_timer: Optional[str] = None

    def _bind_fire(self, name: Optional[str],
                   event_time_ms: int, watermark_ms: int,
                   partition_key: Any = None) -> None:
        self._current_timer = name
        # Forward the partition key so collect() can prepend partition columns.
        # The on_timer path is SET_SEMANTIC-only, so reuse n_partition_repeats.
        self._base._bind_call(
            event_time_ms, watermark_ms, partition_key,
            self._base._n_partition_repeats)

    def current_timer(self) -> Optional[str]:
        return self._current_timer

    def collect(self, row: Any) -> None:
        self._base.collect(row)

    def time_context(self, time_class: Any = None) -> TimeContext:
        return self._base.time_context(time_class)

    def table_semantics_for(self, argument_name: str) -> TableSemanticsView:
        return self._base.table_semantics_for(argument_name)

    def clear_state(self, state_name: str) -> None:
        self._base.clear_state(state_name)

    def clear_all_state(self) -> None:
        self._base.clear_all_state()

    def clear_all_timers(self) -> None:
        self._base.clear_all_timers()


def build_table_semantics_views(pb_arguments,
                                row_types_by_name: dict) -> dict:
    """
    Build a ``{argument_name: TableSemanticsView}`` map from the PTF proto's
    arguments list and a pre-resolved map of argument name to Python
    ``RowType`` (which lives outside the proto).
    """
    from pyflink.fn_execution.flink_fn_execution_pb2 import (
        UserDefinedProcessTableFunction as Pb,
    )
    changelog_map = {
        Pb.INSERT_ONLY: PTFChangelogMode.INSERT_ONLY,
        Pb.ALL_RETRACT: PTFChangelogMode.ALL_RETRACT,
        Pb.UPSERT: PTFChangelogMode.UPSERT,
        Pb.ALL_CHANGES: PTFChangelogMode.ALL_CHANGES,
    }
    out = {}
    for arg in pb_arguments:
        if arg.WhichOneof('kind') != 'table':
            continue
        out[arg.name] = _TableSemanticsView(
            partition_columns=list(arg.table.partition_by_columns),
            time_column=arg.table.on_time_column,
            data_type=row_types_by_name.get(arg.name),
            changelog_mode=changelog_map[arg.table.input_changelog],
        )
    return out
