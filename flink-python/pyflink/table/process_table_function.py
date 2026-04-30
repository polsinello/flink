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
Public API for Python Process Table Functions (PTFs) — process mode.

A Process Table Function is a stateful, timer-aware table function that takes one
or more table-typed arguments plus optional scalar arguments. PTFs are the most
powerful function kind in Flink SQL/Table API: they support row- and set-semantic
table inputs, named state slots with TTL, named/unnamed timers with onTimer
callbacks, changelog I/O, and TableSemantics introspection.

See the design document at docs/proposals/pyflink-ptf-process-mode-design.md
and the Java reference at
https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/table/functions/ptfs/.

Minimal example::

    class GreetingPtf(ProcessTableFunction):

        @argument_hint(name="input",
                       trait=ArgumentTrait.ROW_SEMANTIC_TABLE,
                       row_type=DataTypes.ROW([DataTypes.FIELD("name", DataTypes.STRING())]))
        def eval(self, ctx, input):
            ctx.collect(Row("Hello " + input["name"] + "!"))

    f = ptf(GreetingPtf(), output_type=DataTypes.ROW([DataTypes.FIELD("greeting", DataTypes.STRING())]))
    table_env.create_temporary_function("greet", f)

A more complete example with state and named timers::

    class CountAndAlert(ProcessTableFunction):

        @argument_hint(name="input",
                       trait=ArgumentTrait.SET_SEMANTIC_TABLE,
                       row_type=...,
                       on_time="ts",
                       partition_by=["customer_id"])
        @state_hint(name="memory",
                    kind=StateKind.VALUE,
                    value_type=DataTypes.BIGINT(),
                    ttl="1 hour")
        def eval(self, ctx, memory, input):
            count = (memory.value() or 0) + 1
            memory.update(count)
            if count >= 100:
                ctx.time_context().register_on_time("alert",
                                                    ctx.time_context().time().plus(60_000))

        def on_timer(self, ctx, memory):
            ctx.collect(Row("alert", memory.value()))
"""
from __future__ import annotations

import abc
import enum
import functools
from datetime import datetime
from typing import (
    Any, Callable, Dict, Generic, Iterable, List, Optional, Tuple, Type,
    TypeVar, Union,
)

from pyflink.table.data_view import ListView, MapView
from pyflink.table.types import DataType
from pyflink.table.udf import UserDefinedFunction, FunctionContext
from pyflink.util.api_stability_decorators import PublicEvolving, Internal

__all__ = [
    'ProcessTableFunction',
    'Context', 'OnTimerContext', 'TimeContext', 'TableSemanticsView',
    'ListView', 'MapView',
    'ArgumentTrait', 'StateKind', 'PTFChangelogMode',
    'argument_hint', 'state_hint',
    'ptf',
]

# Type variables for typed views.
T = TypeVar('T')
K = TypeVar('K')
V = TypeVar('V')

# Sentinel attribute used to attach decorator metadata to the eval method.
_PTF_EVAL_META_ATTR = '__pyflink_ptf_eval_meta__'


# ============================================================================
#  Enums mirroring Java org.apache.flink.table.functions.ProcessTableFunction
# ============================================================================

@PublicEvolving()
class ArgumentTrait(enum.Enum):
    """
    Marks how a single argument to a ProcessTableFunction's ``eval`` method is
    interpreted by the runtime. Mirrors
    ``org.apache.flink.table.annotation.ArgumentTrait``.
    """
    SCALAR = 'SCALAR'
    """A plain scalar argument (default for non-table args)."""

    ROW_SEMANTIC_TABLE = 'ROW_SEMANTIC_TABLE'
    """Each input row processed independently; framework distributes rows freely."""

    SET_SEMANTIC_TABLE = 'SET_SEMANTIC_TABLE'
    """Rows correlated by partition key; all rows for a key co-located on one task."""

    OPTIONAL_PARTITION_BY = 'OPTIONAL_PARTITION_BY'
    """For SET_SEMANTIC_TABLE: partitioning is optional (defaults to single partition)."""

    REQUIRE_ON_TIME = 'REQUIRE_ON_TIME'
    """The on_time descriptor must be supplied at the call site."""

    PASS_COLUMNS_THROUGH = 'PASS_COLUMNS_THROUGH'
    """Output rows include all input columns prepended to the PTF's emitted columns."""

    SUPPORT_UPDATES = 'SUPPORT_UPDATES'
    """The PTF accepts updating (changelog) input rows on this table arg."""

    REQUIRE_UPDATE_BEFORE = 'REQUIRE_UPDATE_BEFORE'
    """Force the input changelog into retract mode (+I, -U, +U, -D)."""

    REQUIRE_FULL_DELETE = 'REQUIRE_FULL_DELETE'
    """Require -D rows to carry full row content (not just keys)."""


@PublicEvolving()
class StateKind(enum.Enum):
    """The kind of a declared @state_hint slot."""
    VALUE = 'VALUE'
    """Single-value state. Reads/writes a structured Python object as one unit."""

    LIST = 'LIST'
    """List view over keyed ListState; lazy iteration; efficient append."""

    MAP = 'MAP'
    """Map view over keyed MapState; lazy per-key access."""


@PublicEvolving()
class PTFChangelogMode(enum.Enum):
    """Changelog modes for PTF input or output. Mirrors RowKind constraints."""
    INSERT_ONLY = 'INSERT_ONLY'
    """Only +I rows."""

    ALL_RETRACT = 'ALL_RETRACT'
    """+I, -U, +U, -D — full retraction-style changelog."""

    UPSERT = 'UPSERT'
    """+I, +U, -D — no update-before."""

    ALL_CHANGES = 'ALL_CHANGES'
    """No restriction; any RowKind."""


# ============================================================================
#  ABCs for runtime objects passed to user code
# ============================================================================

@PublicEvolving()
class TableSemanticsView(abc.ABC):
    """
    Snapshot of a single table argument's resolved semantics, available via
    ``Context.table_semantics_for(name)``. Mirrors
    ``org.apache.flink.table.functions.TableSemantics``.
    """

    @abc.abstractmethod
    def partition_by_columns(self) -> List[int]:
        """The resolved partition-by column indices into the table arg's row type."""

    @abc.abstractmethod
    def time_column(self) -> int:
        """Index of the bound on_time column, or ``-1`` if not bound."""

    @abc.abstractmethod
    def data_type(self) -> DataType:
        """The data type of the table argument (a row type)."""

    @abc.abstractmethod
    def changelog_mode(self) -> PTFChangelogMode:
        """The resolved changelog mode of the table argument."""


@PublicEvolving()
class TimeContext(abc.ABC, Generic[T]):
    """
    Time services bound to a table argument's on_time descriptor. Mirrors
    ``org.apache.flink.table.functions.ProcessTableFunction.TimeContext``.

    The type parameter ``T`` is the time type chosen at PTF registration:
    ``Instant`` (long ms epoch wrapped), ``datetime.datetime``, or ``int`` (long ms).
    """

    @abc.abstractmethod
    def time(self) -> T:
        """Timestamp of the row currently being processed, or the firing timer."""

    @abc.abstractmethod
    def current_watermark(self) -> T:
        """The operator's current input watermark."""

    @abc.abstractmethod
    def register_on_time(self,
                         name_or_timestamp: Union[str, T],
                         timestamp: Optional[T] = None) -> None:
        """
        Register an event-time timer. Two forms::

            register_on_time(timestamp)          # unnamed timer
            register_on_time("my-timer", timestamp)  # named timer (replaceable)

        Named timers are replaced if registered again with the same name; only
        the latest registration fires.
        """

    @abc.abstractmethod
    def clear_timer(self, name_or_timestamp: Union[str, T]) -> None:
        """
        Delete a registered timer. Two forms (mirrors Java
        ``clearTimer(String)`` / ``clearTimer(T)``)::

            clear_timer("my-timer")   # delete the named timer
            clear_timer(timestamp)    # delete the unnamed timer at this time

        No-op if no matching timer is registered.
        """

    @abc.abstractmethod
    def clear_all_timers(self) -> None:
        """Delete all timers — both named and unnamed — for the current key."""


@PublicEvolving()
class Context(abc.ABC):
    """
    Per-call runtime context for a Process Table Function. Available as the
    first argument to ``eval`` after ``self`` whenever the user signature
    declares it.

    Mirrors ``org.apache.flink.table.functions.ProcessTableFunction.Context``.
    """

    @abc.abstractmethod
    def collect(self, row: Any) -> None:
        """Emit one output row. May be called any number of times per ``eval``."""

    @abc.abstractmethod
    def time_context(self, time_class: Optional[Type] = None) -> TimeContext:
        """
        The time services for the bound on_time table argument.

        By default the time type ``T`` is auto-inferred from the on_time
        column's DataType. Pass ``time_class`` to select it explicitly (mirrors
        Java ``timeContext(Class<T>)``): ``int`` for epoch millis (Long),
        ``datetime.datetime`` for LocalDateTime/Instant.
        """

    @abc.abstractmethod
    def table_semantics_for(self, argument_name: str) -> TableSemanticsView:
        """Resolved semantics for the named table argument."""

    @abc.abstractmethod
    def clear_state(self, state_name: str) -> None:
        """
        Clear a single declared state slot for the current key. Mirrors Java's
        ``Context#clearState(stateName)``.

        :param state_name: Name of the state slot, as declared via
            ``@state_hint(name=...)``.
        :raises KeyError: If ``state_name`` does not match a declared slot.
        """

    @abc.abstractmethod
    def clear_all_state(self) -> None:
        """Clear every declared state slot for the current key."""

    @abc.abstractmethod
    def clear_all_timers(self) -> None:
        """Clear every registered timer (named and unnamed) for the current key."""

    def clear_all(self) -> None:
        """
        Clear every state slot and every timer for the current key. Mirrors
        Java's ``Context.clearAll`` — equivalent to calling ``clear_all_state``
        followed by ``clear_all_timers``.
        """
        self.clear_all_state()
        self.clear_all_timers()


@PublicEvolving()
class OnTimerContext(Context, abc.ABC):
    """
    Context for the ``on_timer`` callback. Adds ``current_timer()`` for
    identifying which named timer fired.
    """

    @abc.abstractmethod
    def current_timer(self) -> Optional[str]:
        """The name of the firing named timer, or ``None`` for an unnamed timer."""


# NOTE: ListView and MapView for PTF state slots are the same user-facing
# classes used in UDAF accumulators (``pyflink.table.data_view``). At PTF
# runtime, the worker substitutes state-backed implementations with the same
# duck-typed interface. The user types and methods stay identical whether
# the view is in-memory (UDAF context) or state-backed (PTF context).


# ============================================================================
#  ProcessTableFunction base class
# ============================================================================

@PublicEvolving()
class ProcessTableFunction(UserDefinedFunction):
    """
    Base class for Python Process Table Functions.

    Subclasses must implement ``eval``. ``eval``'s signature carries the
    full PTF declaration via ``@argument_hint`` and ``@state_hint`` decorators
    on the method itself::

        class MyPtf(ProcessTableFunction):

            @argument_hint(name="input",
                           trait=ArgumentTrait.SET_SEMANTIC_TABLE,
                           row_type=DataTypes.ROW([...]),
                           partition_by=["k"])
            @state_hint(name="memory",
                        kind=StateKind.VALUE,
                        value_type=DataTypes.BIGINT(),
                        ttl="1 hour")
            def eval(self, ctx, memory, input):
                ...

    Optional ``on_timer`` callback fires when registered timers trigger::

            def on_timer(self, ctx, memory):
                ...

    The class is registered via the module-level :func:`ptf` factory and
    installed as a temporary function via
    :meth:`pyflink.table.TableEnvironment.create_temporary_function`.
    """

    @abc.abstractmethod
    def eval(self, *args, **kwargs):
        """
        The PTF's per-row dispatch entry point. Receives:

        - Optionally, a :class:`Context` as the first argument after ``self``
          if the signature declares it (positionally).
        - A bound :class:`ValueStateProxy` / :class:`ListView` / :class:`MapView`
          for each ``@state_hint``-declared slot, in declaration order.
        - One non-null table-argument row plus all-None placeholders for the
          other table args (multi-table PTFs are dispatched per-input).
        - Scalar arguments resolved to their constant values.

        ``eval`` does not return a value. To emit output rows, call
        ``ctx.collect(row)`` any number of times.
        """

    def on_timer(self, ctx: 'OnTimerContext', *state) -> None:
        """
        Called when a registered timer fires. Default is a no-op.

        Receives the :class:`OnTimerContext` followed by all ``@state_hint``
        slots in declaration order. No table-argument rows are passed.
        """


# ============================================================================
#  Decorators for declaring arguments and state slots on ``eval``
# ============================================================================

@Internal()
class _StateHintMeta:
    """Internal: parsed @state_hint decorator payload."""

    __slots__ = ('name', 'kind', 'value_type', 'element_type',
                 'key_type', 'map_value_type', 'ttl')

    def __init__(self,
                 name: str,
                 kind: StateKind,
                 value_type: Optional[DataType] = None,
                 element_type: Optional[DataType] = None,
                 key_type: Optional[DataType] = None,
                 map_value_type: Optional[DataType] = None,
                 ttl: Optional[str] = None):
        self.name = name
        self.kind = kind
        self.value_type = value_type
        self.element_type = element_type
        self.key_type = key_type
        self.map_value_type = map_value_type
        self.ttl = ttl


@Internal()
class _ArgumentHintMeta:
    """Internal: parsed @argument_hint decorator payload."""

    __slots__ = ('name', 'trait', 'extra_traits', 'scalar_type', 'row_type',
                 'partition_by', 'on_time', 'pass_through_columns',
                 'optional', 'input_changelog')

    def __init__(self,
                 name: str,
                 trait: ArgumentTrait,
                 extra_traits: Optional[List[ArgumentTrait]] = None,
                 scalar_type: Optional[DataType] = None,
                 row_type: Optional[DataType] = None,
                 partition_by: Optional[List[str]] = None,
                 on_time: Optional[str] = None,
                 pass_through_columns: Optional[List[str]] = None,
                 optional: bool = False,
                 input_changelog: PTFChangelogMode = PTFChangelogMode.INSERT_ONLY):
        self.name = name
        self.trait = trait
        self.extra_traits = extra_traits or []
        self.scalar_type = scalar_type
        self.row_type = row_type
        self.partition_by = partition_by or []
        self.on_time = on_time
        self.pass_through_columns = pass_through_columns or []
        self.optional = optional
        self.input_changelog = input_changelog


@Internal()
class _EvalMetadata:
    """
    Internal aggregator attached to an ``eval`` method by the decorators.
    Decorators stack outermost-first, so we collect into a list and reverse
    once the chain is complete.
    """

    __slots__ = ('argument_hints', 'state_hints')

    def __init__(self):
        self.argument_hints: List[_ArgumentHintMeta] = []
        self.state_hints: List[_StateHintMeta] = []


def _get_or_init_eval_meta(func) -> _EvalMetadata:
    """Return (creating if absent) the metadata attached to ``func``."""
    meta = getattr(func, _PTF_EVAL_META_ATTR, None)
    if meta is None:
        meta = _EvalMetadata()
        setattr(func, _PTF_EVAL_META_ATTR, meta)
    return meta


@PublicEvolving()
def argument_hint(*,
                  name: str,
                  trait: ArgumentTrait = ArgumentTrait.SCALAR,
                  extra_traits: Optional[List[ArgumentTrait]] = None,
                  scalar_type: Optional[DataType] = None,
                  row_type: Optional[DataType] = None,
                  partition_by: Optional[List[str]] = None,
                  on_time: Optional[str] = None,
                  pass_through_columns: Optional[List[str]] = None,
                  optional: bool = False,
                  input_changelog: PTFChangelogMode = PTFChangelogMode.INSERT_ONLY):
    """
    Decorator declaring an argument to the PTF's ``eval`` method.

    Stack one ``@argument_hint`` per logical argument (in any order; declaration
    order is recovered from the decorator chain).

    :param name: Argument name (used for SQL named-arg invocation).
    :param trait: ROW_SEMANTIC_TABLE, SET_SEMANTIC_TABLE, or SCALAR.
    :param extra_traits: List of additional traits (PASS_COLUMNS_THROUGH,
        SUPPORT_UPDATES, OPTIONAL_PARTITION_BY, REQUIRE_ON_TIME, etc.).
    :param scalar_type: Required for SCALAR args. The DataType of the scalar.
    :param row_type: Required for table args. The row DataType of the table.
    :param partition_by: For SET_SEMANTIC_TABLE: column names from row_type to
        partition by.
    :param on_time: Column name from row_type that carries the event-time
        timestamp.
    :param pass_through_columns: Subset of row_type columns to pass through
        unchanged to the output.
    :param optional: If True, the argument is optional in the call.
    :param input_changelog: For SUPPORT_UPDATES: the input changelog mode.
    """
    meta = _ArgumentHintMeta(
        name=name, trait=trait, extra_traits=extra_traits,
        scalar_type=scalar_type, row_type=row_type,
        partition_by=partition_by, on_time=on_time,
        pass_through_columns=pass_through_columns, optional=optional,
        input_changelog=input_changelog,
    )

    def _wrap(func):
        eval_meta = _get_or_init_eval_meta(func)
        # Decorator-chain order: outermost runs LAST. We want declaration order
        # (top-to-bottom in source); so the bottom-most decorator runs first
        # and gets prepended.
        eval_meta.argument_hints.insert(0, meta)
        return func

    return _wrap


@PublicEvolving()
def state_hint(*,
               name: str,
               kind: StateKind,
               value_type: Optional[DataType] = None,
               element_type: Optional[DataType] = None,
               key_type: Optional[DataType] = None,
               map_value_type: Optional[DataType] = None,
               ttl: Optional[str] = None):
    """
    Decorator declaring a state slot used by ``eval`` and ``on_timer``.

    Stack one ``@state_hint`` per slot. The user's eval method receives one
    state object per slot, in declaration order, before the table-argument
    parameters.

    :param name: Slot name. Must be unique within the PTF.
    :param kind: One of ``StateKind.VALUE``, ``LIST``, ``MAP``.
    :param value_type: Required for ``StateKind.VALUE``.
    :param element_type: Required for ``StateKind.LIST`` (the element type).
    :param key_type: Required for ``StateKind.MAP``.
    :param map_value_type: Required for ``StateKind.MAP``.
    :param ttl: Optional duration string (e.g. ``"1 hour"``, ``"3 days"``,
        ``"45 min"``) — same syntax as Java ``@StateHint(ttl=...)``.
    """
    meta = _StateHintMeta(
        name=name, kind=kind, value_type=value_type,
        element_type=element_type, key_type=key_type,
        map_value_type=map_value_type, ttl=ttl,
    )

    def _wrap(func):
        eval_meta = _get_or_init_eval_meta(func)
        eval_meta.state_hints.insert(0, meta)
        return func

    return _wrap


# ============================================================================
#  Registration factory
# ============================================================================

@PublicEvolving()
def ptf(func: ProcessTableFunction,
        *,
        output_type: DataType,
        emits_changelog: PTFChangelogMode = PTFChangelogMode.INSERT_ONLY,
        output_changelog_for_required: Optional[
            Dict[PTFChangelogMode, PTFChangelogMode]] = None,
        uid: Optional[str] = None,
        deterministic: bool = True,
        name: Optional[str] = None) -> 'UserDefinedProcessTableFunctionWrapper':
    """
    Register a Process Table Function instance for use in Table API / SQL.

    :param func: An instance of a ``ProcessTableFunction`` subclass.
    :param output_type: The DataType of emitted output rows.
    :param emits_changelog: Output changelog mode (default INSERT_ONLY). Used as the
        fallback when ``output_changelog_for_required`` is not supplied or has no
        entry for the resolved required mode.
    :param output_changelog_for_required: Optional declarative ChangelogContext
        negotiation table. Maps the downstream's required PTFChangelogMode to the
        PTFChangelogMode the PTF will emit. Mirrors Java's
        ``ChangelogFunction.getChangelogMode(ChangelogContext)`` for the common
        case where the output mode depends on what the sink/caller requires.
        Example: ``{PTFChangelogMode.INSERT_ONLY: PTFChangelogMode.INSERT_ONLY,
        PTFChangelogMode.ALL_CHANGES: PTFChangelogMode.ALL_CHANGES}``.
    :param uid: System-arg uid for state-namespace disambiguation when the same
        PTF is registered under multiple names. Auto-derived if omitted.
    :param deterministic: Whether the PTF's emissions are deterministic.
    :param name: Optional registered name.
    :returns: A wrapper installable via
        ``table_env.create_temporary_function(name, ...)``.
    """
    if not isinstance(func, ProcessTableFunction):
        raise TypeError(
            f"ptf() requires a ProcessTableFunction instance, got "
            f"{type(func).__name__}"
        )
    if output_changelog_for_required is not None:
        for k, v in output_changelog_for_required.items():
            if not isinstance(k, PTFChangelogMode) or not isinstance(v, PTFChangelogMode):
                raise TypeError(
                    "output_changelog_for_required must map PTFChangelogMode -> "
                    f"PTFChangelogMode; got {type(k).__name__} -> "
                    f"{type(v).__name__}"
                )
    return UserDefinedProcessTableFunctionWrapper(
        func=func,
        output_type=output_type,
        emits_changelog=emits_changelog,
        output_changelog_for_required=output_changelog_for_required,
        uid=uid,
        deterministic=deterministic,
        name=name,
    )


# ============================================================================
#  Wrapper that bridges Python instance → Java PythonProcessTableFunction
# ============================================================================

@Internal()
class UserDefinedProcessTableFunctionWrapper:
    """
    Bridge between a Python ``ProcessTableFunction`` instance and the Java
    side. Inspects the user's ``eval`` decorator metadata, extracts arg/state
    specs, and (when ``_java_user_defined_function`` is invoked) constructs the
    Java ``PythonProcessTableFunction`` wrapper carrying the pickled payload
    and full proto spec.

    Not part of the public API; created via :func:`ptf`.
    """

    def __init__(self,
                 func: ProcessTableFunction,
                 output_type: DataType,
                 emits_changelog: PTFChangelogMode,
                 output_changelog_for_required: Optional[
                     Dict[PTFChangelogMode, PTFChangelogMode]],
                 uid: Optional[str],
                 deterministic: bool,
                 name: Optional[str]):
        self._func = func
        self._output_type = output_type
        self._emits_changelog = emits_changelog
        self._output_changelog_for_required = output_changelog_for_required
        self._uid = uid
        self._deterministic = deterministic
        self._name = name or type(func).__name__

        eval_method = type(func).eval
        meta: Optional[_EvalMetadata] = getattr(
            eval_method, _PTF_EVAL_META_ATTR, None)
        if meta is None or not meta.argument_hints:
            raise ValueError(
                f"ProcessTableFunction {type(func).__name__}.eval has no "
                f"@argument_hint declarations. At least one table or scalar "
                f"argument must be declared."
            )
        self._argument_hints: List[_ArgumentHintMeta] = list(meta.argument_hints)
        self._state_hints: List[_StateHintMeta] = list(meta.state_hints)
        # Validate uniqueness of arg names and state slot names.
        self._validate_metadata()

    # ------------------------------------------------------------------ #
    # Validation                                                          #
    # ------------------------------------------------------------------ #

    def _validate_metadata(self) -> None:
        seen_arg_names = set()
        for arg in self._argument_hints:
            if arg.name in seen_arg_names:
                raise ValueError(
                    f"Duplicate argument name '{arg.name}' in "
                    f"{type(self._func).__name__}.eval"
                )
            seen_arg_names.add(arg.name)
            self._validate_argument_hint(arg)
        seen_state_names = set()
        for st in self._state_hints:
            if st.name in seen_state_names:
                raise ValueError(
                    f"Duplicate state slot '{st.name}' in "
                    f"{type(self._func).__name__}.eval"
                )
            seen_state_names.add(st.name)
            self._validate_state_hint(st)

    def _validate_argument_hint(self, arg: _ArgumentHintMeta) -> None:
        if arg.trait == ArgumentTrait.SCALAR:
            if arg.scalar_type is None:
                raise ValueError(
                    f"Argument '{arg.name}' has trait SCALAR but no scalar_type."
                )
            if arg.row_type is not None:
                raise ValueError(
                    f"Argument '{arg.name}' is SCALAR; row_type must be None."
                )
        else:  # ROW_SEMANTIC_TABLE or SET_SEMANTIC_TABLE
            # row_type is optional → polymorphic Row table arg. The planner
            # resolves the actual row type from the SQL caller and patches the
            # spec proto at translate time. Mirrors Java's
            # ``@ArgumentHint(value = SET_SEMANTIC_TABLE) Row t`` form (no
            # ``@DataTypeHint``).
            if arg.scalar_type is not None:
                raise ValueError(
                    f"Argument '{arg.name}' is a table argument; "
                    f"scalar_type must be None."
                )
            if arg.trait == ArgumentTrait.SET_SEMANTIC_TABLE:
                if not arg.partition_by and \
                        ArgumentTrait.OPTIONAL_PARTITION_BY not in arg.extra_traits:
                    raise ValueError(
                        f"SET_SEMANTIC_TABLE argument '{arg.name}' must declare "
                        f"partition_by, or include OPTIONAL_PARTITION_BY in "
                        f"extra_traits."
                    )

    def _validate_state_hint(self, st: _StateHintMeta) -> None:
        if st.kind == StateKind.VALUE:
            if st.value_type is None:
                raise ValueError(
                    f"State slot '{st.name}' kind=VALUE requires value_type."
                )
        elif st.kind == StateKind.LIST:
            if st.element_type is None:
                raise ValueError(
                    f"State slot '{st.name}' kind=LIST requires element_type."
                )
        elif st.kind == StateKind.MAP:
            if st.key_type is None or st.map_value_type is None:
                raise ValueError(
                    f"State slot '{st.name}' kind=MAP requires both "
                    f"key_type and map_value_type."
                )

    # ------------------------------------------------------------------ #
    # Accessors used by the registration path                             #
    # ------------------------------------------------------------------ #

    _inline_counter = 0

    def _register_inline(self, t_env) -> str:
        """
        Register this PTF under a generated temporary system-function name so it
        can be invoked inline through the Table API (``table.process(ptf(...), ...)``).
        Idempotent per table environment.
        """
        import weakref
        names = self.__dict__.get('_inline_names')
        if names is None:
            # WeakKeyDictionary keyed on the env object: entries die with the
            # env, avoiding the id()-reuse-after-GC hazard of an id-keyed dict.
            names = weakref.WeakKeyDictionary()
            self.__dict__['_inline_names'] = names
        if t_env not in names:
            UserDefinedProcessTableFunctionWrapper._inline_counter += 1
            name = "_pyflink_ptf_%s_%d" % (
                self._name, UserDefinedProcessTableFunctionWrapper._inline_counter)
            t_env.create_temporary_system_function(name, self)
            names[t_env] = name
        return names[t_env]

    def _serialized_payload(self) -> bytes:
        """Pickle the user instance for shipment to the worker."""
        import cloudpickle
        return cloudpickle.dumps(self._func)

    def _build_proto(self) -> bytes:
        """
        Build the FlinkFnApi.UserDefinedProcessTableFunction proto and return
        its serialized form. Called by the Java wrapper at registration time.
        """
        # Lazy import: pb2 is generated at build time.
        from pyflink.fn_execution import flink_fn_execution_pb2 as pb2
        # Lazy import: type-mapping helpers live in Python coders.
        from pyflink.fn_execution.flink_fn_execution_pb2 import (
            UserDefinedProcessTableFunction as Pb,
            StateDescriptor as PbStateDescriptor,
        )

        pb = Pb()
        pb.payload = self._serialized_payload()
        pb.uid = self._uid or self._derive_uid()
        pb.emits_changelog = _changelog_to_pb(pb, self._emits_changelog)
        pb.has_on_timer = self._has_user_on_timer()
        # Cache + metric flags filled in by the Java wrapper from PythonOptions.
        pb.metric_enabled = False
        pb.profile_enabled = False

        for arg in self._argument_hints:
            pb_arg = pb.arguments.add()
            pb_arg.name = arg.name
            pb_arg.optional = arg.optional
            if arg.trait == ArgumentTrait.SCALAR:
                _build_scalar_arg(pb_arg.scalar, arg)
            else:
                _build_table_arg(pb, pb_arg.table, arg)

        for st in self._state_hints:
            pb_st = pb.state_slots.add()
            _build_state_slot(pb, pb_st, st)

        return pb.SerializeToString()

    def _has_user_on_timer(self) -> bool:
        cls = type(self._func)
        own = cls.__dict__.get('on_timer')
        # If the subclass did not override the no-op default, has_on_timer = False.
        return own is not None and own is not ProcessTableFunction.on_timer

    def _derive_uid(self) -> str:
        """Deterministic uid derivation when none was supplied."""
        return f"py_ptf_{type(self._func).__name__}"

    def _java_user_defined_function(self):
        """
        Construct the Java ``PythonProcessTableFunction`` wrapper through the
        gateway. The eval signature is shipped as parallel serializable lists
        (names, data types, table-flag, optional-flag, trait names) so the
        Java side can rebuild ``StaticArgument`` instances at planning time
        without decoding the proto.
        """
        from pyflink.java_gateway import get_gateway
        from pyflink.table.types import _to_java_data_type
        from pyflink.table.udf import _get_python_env

        gateway = get_gateway()
        j_python_function_kind = (
            gateway.jvm.org.apache.flink.table.functions.python
            .PythonFunctionKind.GENERAL
        )
        j_output_type = _to_java_data_type(self._output_type)
        j_python_env = _get_python_env()

        j_arg_names = gateway.jvm.java.util.ArrayList()
        j_arg_types = gateway.jvm.java.util.ArrayList()
        j_arg_is_table = gateway.jvm.java.util.ArrayList()
        j_arg_is_optional = gateway.jvm.java.util.ArrayList()
        j_arg_trait_names = gateway.jvm.java.util.ArrayList()

        for arg in self._argument_hints:
            j_arg_names.add(arg.name)
            j_arg_is_optional.add(arg.optional)
            if arg.trait == ArgumentTrait.SCALAR:
                j_arg_types.add(_to_java_data_type(arg.scalar_type))
                j_arg_is_table.add(False)
                j_arg_trait_names.add(gateway.jvm.java.util.ArrayList())
            else:
                # arg.row_type may be None for polymorphic table args. Pass a
                # null DataType through to Java; ``PythonProcessTableFunction``
                # uses {@link StaticArgument#table(String, Class, boolean,
                # EnumSet)} (the polymorphic form) when the entry is null.
                j_arg_types.add(
                    _to_java_data_type(arg.row_type)
                    if arg.row_type is not None else None
                )
                j_arg_is_table.add(True)
                j_per_arg_traits = gateway.jvm.java.util.ArrayList()
                if arg.trait == ArgumentTrait.SET_SEMANTIC_TABLE:
                    j_per_arg_traits.add("SET_SEMANTIC_TABLE")
                else:
                    j_per_arg_traits.add("ROW_SEMANTIC_TABLE")
                _add_trait_names(j_per_arg_traits, arg)
                j_arg_trait_names.add(j_per_arg_traits)

        j_ptf_class = (
            gateway.jvm.org.apache.flink.table.functions.python
            .PythonProcessTableFunction
        )
        j_ptf = j_ptf_class(
            self._name,
            bytearray(self._build_proto()),
            j_output_type,
            j_arg_names,
            j_arg_types,
            j_arg_is_table,
            j_arg_is_optional,
            j_arg_trait_names,
            j_python_function_kind,
            self._deterministic,
            j_python_env,
            self._emits_changelog.value,
        )
        # Forward declarative ChangelogContext negotiation table to the Java
        # wrapper. Java side reads this in getChangelogMode(ChangelogContext)
        # to choose an output mode based on the downstream's required mode.
        if self._output_changelog_for_required:
            j_map = gateway.jvm.java.util.HashMap()
            for required_mode, output_mode in (
                    self._output_changelog_for_required.items()):
                j_map.put(required_mode.value, output_mode.value)
            j_ptf.setOutputChangelogForRequiredMap(j_map)
        return j_ptf


# ============================================================================
#  Internal builders for the proto from declared metadata
# ============================================================================

@Internal()
def _add_trait_names(j_trait_names, arg: '_ArgumentHintMeta') -> None:
    """Append Java ``StaticArgumentTrait`` enum names for declared extra traits."""
    extras = arg.extra_traits or []
    if ArgumentTrait.OPTIONAL_PARTITION_BY in extras:
        j_trait_names.add("OPTIONAL_PARTITION_BY")
    if ArgumentTrait.REQUIRE_ON_TIME in extras:
        j_trait_names.add("REQUIRE_ON_TIME")
    if ArgumentTrait.PASS_COLUMNS_THROUGH in extras:
        j_trait_names.add("PASS_COLUMNS_THROUGH")
    if ArgumentTrait.SUPPORT_UPDATES in extras:
        j_trait_names.add("SUPPORT_UPDATES")
    if ArgumentTrait.REQUIRE_UPDATE_BEFORE in extras:
        j_trait_names.add("REQUIRE_UPDATE_BEFORE")
    if ArgumentTrait.REQUIRE_FULL_DELETE in extras:
        j_trait_names.add("REQUIRE_FULL_DELETE")


@Internal()
def _build_scalar_arg(pb_scalar, arg: _ArgumentHintMeta) -> None:
    _fill_field_type(pb_scalar.type, arg.scalar_type)


@Internal()
def _build_table_arg(pb_root, pb_table, arg: _ArgumentHintMeta) -> None:
    from pyflink.fn_execution.flink_fn_execution_pb2 import (
        UserDefinedProcessTableFunction as Pb,
        Schema as PbSchema,
    )
    if arg.row_type is not None:
        _fill_field_type(pb_table.row_type, arg.row_type)
    else:
        # Polymorphic table arg: leave the row_type as the proto3 default
        # (type_name=ROW with empty row_schema). The Java planner detects this
        # placeholder at translate time and patches the field with the row
        # type Calcite resolved from the SQL caller. Worker code reads the
        # patched proto exactly as it would for an explicit row_type.
        pb_table.row_type.type_name = PbSchema.TypeName.ROW
    if arg.trait == ArgumentTrait.ROW_SEMANTIC_TABLE:
        pb_table.semantics = Pb.ROW_SEMANTIC
    elif arg.trait == ArgumentTrait.SET_SEMANTIC_TABLE:
        pb_table.semantics = Pb.SET_SEMANTIC
    else:
        raise ValueError(f"Argument {arg.name}: unexpected table trait "
                         f"{arg.trait}")
    extras = arg.extra_traits or []
    # Column-index resolution: requires row_type to know the schema. For
    # polymorphic args (no row_type), resolution is deferred to the Java
    # planner which has the resolved schema from the SQL caller.
    if arg.row_type is not None:
        pb_table.partition_by_columns.extend(
            _resolve_column_indices(arg.row_type, arg.partition_by))
        if arg.pass_through_columns:
            pb_table.pass_through_columns.extend(
                _resolve_column_indices(arg.row_type, arg.pass_through_columns))
        elif ArgumentTrait.PASS_COLUMNS_THROUGH in extras:
            # PASS_COLUMNS_THROUGH means "all input columns" — Calcite's
            # SystemTypeInference appends every input column to the output
            # schema. Mirror that on the wire so the worker prepends all
            # input fields to each emitted row. (Java PTFs don't take an
            # explicit subset; the trait is all-or-nothing.)
            from pyflink.table.types import RowType as _RowType
            if isinstance(arg.row_type, _RowType):
                pb_table.pass_through_columns.extend(
                    range(len(arg.row_type.fields)))
        if arg.on_time is None:
            pb_table.on_time_column = -1
            pb_table.time_type = Pb.NO_TIME
        else:
            pb_table.on_time_column = _resolve_column_index(arg.row_type, arg.on_time)
            pb_table.time_type = _infer_time_type(pb_root, arg.row_type, arg.on_time)
    else:
        # Polymorphic: no row_type to index into. Default to no on_time and
        # let the planner-side translate update row_type before the worker
        # reads partition_by_columns / on_time_column.
        pb_table.on_time_column = -1
        pb_table.time_type = Pb.NO_TIME
    # Trait flags.
    pb_table.require_on_time = ArgumentTrait.REQUIRE_ON_TIME in extras
    pb_table.optional_partition_by = ArgumentTrait.OPTIONAL_PARTITION_BY in extras
    pb_table.supports_updates = ArgumentTrait.SUPPORT_UPDATES in extras
    pb_table.require_update_before = ArgumentTrait.REQUIRE_UPDATE_BEFORE in extras
    pb_table.require_full_delete = ArgumentTrait.REQUIRE_FULL_DELETE in extras
    pb_table.input_changelog = _changelog_to_pb(pb_root, arg.input_changelog)


@Internal()
def _build_state_slot(pb_root, pb_slot, st: _StateHintMeta) -> None:
    from pyflink.fn_execution.flink_fn_execution_pb2 import (
        UserDefinedProcessTableFunction as Pb,
    )
    pb_slot.name = st.name
    if st.kind == StateKind.VALUE:
        pb_slot.kind = Pb.VALUE_STATE
        _fill_field_type(pb_slot.value_type, st.value_type)
    elif st.kind == StateKind.LIST:
        pb_slot.kind = Pb.LIST_STATE
        _fill_field_type(pb_slot.element_type, st.element_type)
    elif st.kind == StateKind.MAP:
        pb_slot.kind = Pb.MAP_STATE
        _fill_field_type(pb_slot.map_type.key_type, st.key_type)
        _fill_field_type(pb_slot.map_type.value_type, st.map_value_type)
    else:
        raise ValueError(f"State slot {st.name}: unknown StateKind {st.kind}")
    # Parse TTL string into the StateTTLConfig proto.
    if st.ttl is not None:
        _fill_state_ttl_config(pb_slot.ttl_config, st.ttl)


@Internal()
def _changelog_to_pb(pb_root, mode: PTFChangelogMode) -> int:
    from pyflink.fn_execution.flink_fn_execution_pb2 import (
        UserDefinedProcessTableFunction as Pb,
    )
    return {
        PTFChangelogMode.INSERT_ONLY: Pb.INSERT_ONLY,
        PTFChangelogMode.ALL_RETRACT: Pb.ALL_RETRACT,
        PTFChangelogMode.UPSERT: Pb.UPSERT,
        PTFChangelogMode.ALL_CHANGES: Pb.ALL_CHANGES,
    }[mode]


@Internal()
def _resolve_column_indices(row_type: DataType,
                            column_names: List[str]) -> List[int]:
    if not column_names:
        return []
    return [_resolve_column_index(row_type, n) for n in column_names]


@Internal()
def _resolve_column_index(row_type: DataType, column_name: str) -> int:
    from pyflink.table.types import RowType
    if not isinstance(row_type, RowType):
        raise ValueError(
            f"Cannot resolve column '{column_name}': declared row_type is "
            f"not a RowType (got {type(row_type).__name__})"
        )
    for i, field in enumerate(row_type.fields):
        if field.name == column_name:
            return i
    available = [f.name for f in row_type.fields]
    raise ValueError(
        f"Column '{column_name}' not found in row_type. Available: {available}"
    )


@Internal()
def _infer_time_type(pb_root, row_type: DataType, on_time_col: str) -> int:
    """
    Resolve the Java-side time type for the on_time column based on the row
    type's column ``DataType``. Mirrors ``Context.timeContext(Class<T>)``
    selection in Java PTF.

    Mapping:
      - ``TIMESTAMP_LTZ``  → ``INSTANT`` (Python: tz-aware UTC datetime)
      - ``TIMESTAMP``      → ``LOCAL_DATE_TIME`` (Python: naive datetime)
      - ``BIGINT``         → ``LONG_MILLIS`` (Python: int)
      - anything else      → ``LONG_MILLIS`` (best-effort fallback)
    """
    from pyflink.fn_execution.flink_fn_execution_pb2 import (
        UserDefinedProcessTableFunction as Pb,
    )
    from pyflink.table.types import (
        RowType as _RowType,
        LocalZonedTimestampType,
        TimestampType,
        BigIntType,
    )
    if not isinstance(row_type, _RowType):
        return Pb.LONG_MILLIS
    for f in row_type.fields:
        if f.name == on_time_col:
            dt = f.data_type
            if isinstance(dt, LocalZonedTimestampType):
                return Pb.INSTANT
            if isinstance(dt, TimestampType):
                return Pb.LOCAL_DATE_TIME
            if isinstance(dt, BigIntType):
                return Pb.LONG_MILLIS
            break
    return Pb.LONG_MILLIS


@Internal()
def _fill_field_type(pb_field_type, dt: DataType) -> None:
    """
    Fill a ``Schema.FieldType`` proto from a Python ``DataType``. The inverse
    of ``pyflink.fn_execution.coders.LengthPrefixBaseCoder._to_data_type`` —
    every reader case has a corresponding writer case here.

    Recurses into nested ROW / ARRAY / MAP types.
    """
    from pyflink.fn_execution import flink_fn_execution_pb2 as pb2
    from pyflink.table.types import (
        TinyIntType, SmallIntType, IntType, BigIntType, BooleanType,
        FloatType, DoubleType, CharType, VarCharType, BinaryType,
        VarBinaryType, DecimalType, DateType, TimeType, TimestampType,
        LocalZonedTimestampType, ZonedTimestampType, ArrayType, RowType,
        MapType, MultisetType, NullType,
    )

    pb_field_type.nullable = bool(getattr(dt, '_nullable', True))
    Pb = pb2.Schema

    if isinstance(dt, TinyIntType):
        pb_field_type.type_name = Pb.TINYINT
    elif isinstance(dt, SmallIntType):
        pb_field_type.type_name = Pb.SMALLINT
    elif isinstance(dt, IntType):
        pb_field_type.type_name = Pb.INT
    elif isinstance(dt, BigIntType):
        pb_field_type.type_name = Pb.BIGINT
    elif isinstance(dt, BooleanType):
        pb_field_type.type_name = Pb.BOOLEAN
    elif isinstance(dt, FloatType):
        pb_field_type.type_name = Pb.FLOAT
    elif isinstance(dt, DoubleType):
        pb_field_type.type_name = Pb.DOUBLE
    elif isinstance(dt, CharType):
        pb_field_type.type_name = Pb.CHAR
        pb_field_type.char_info.length = dt.length
    elif isinstance(dt, VarCharType):
        pb_field_type.type_name = Pb.VARCHAR
        pb_field_type.var_char_info.length = dt.length
    elif isinstance(dt, BinaryType):
        pb_field_type.type_name = Pb.BINARY
        pb_field_type.binary_info.length = dt.length
    elif isinstance(dt, VarBinaryType):
        pb_field_type.type_name = Pb.VARBINARY
        pb_field_type.var_binary_info.length = dt.length
    elif isinstance(dt, DecimalType):
        pb_field_type.type_name = Pb.DECIMAL
        pb_field_type.decimal_info.precision = dt.precision
        pb_field_type.decimal_info.scale = dt.scale
    elif isinstance(dt, DateType):
        pb_field_type.type_name = Pb.DATE
    elif isinstance(dt, TimeType):
        pb_field_type.type_name = Pb.TIME
        pb_field_type.time_info.precision = dt.precision
    elif isinstance(dt, TimestampType):
        pb_field_type.type_name = Pb.TIMESTAMP
        pb_field_type.timestamp_info.precision = dt.precision
    elif isinstance(dt, LocalZonedTimestampType):
        pb_field_type.type_name = Pb.LOCAL_ZONED_TIMESTAMP
        pb_field_type.local_zoned_timestamp_info.precision = dt.precision
    elif isinstance(dt, ArrayType):
        pb_field_type.type_name = Pb.BASIC_ARRAY
        _fill_field_type(pb_field_type.collection_element_type, dt.element_type)
    elif isinstance(dt, RowType):
        pb_field_type.type_name = Pb.TypeName.ROW
        for f in dt.fields:
            pb_field = pb_field_type.row_schema.fields.add()
            pb_field.name = f.name
            if getattr(f, 'description', None):
                pb_field.description = f.description
            _fill_field_type(pb_field.type, f.data_type)
    elif isinstance(dt, MapType):
        pb_field_type.type_name = Pb.TypeName.MAP
        _fill_field_type(pb_field_type.map_info.key_type, dt.key_type)
        _fill_field_type(pb_field_type.map_info.value_type, dt.value_type)
    elif isinstance(dt, MultisetType):
        # MULTISET reuses the collection_element_type slot like BASIC_ARRAY;
        # the proto has no dedicated multiset element field.
        pb_field_type.type_name = Pb.TypeName.MULTISET
        _fill_field_type(pb_field_type.collection_element_type, dt.element_type)
    elif isinstance(dt, ZonedTimestampType):
        pb_field_type.type_name = Pb.TypeName.ZONED_TIMESTAMP
        pb_field_type.zoned_timestamp_info.precision = dt.precision
    elif isinstance(dt, NullType):
        pb_field_type.type_name = Pb.TypeName.NULL
    else:
        # RAW / INTERVAL (DayTime, YearMonth) / STRUCTURED have no
        # Schema.TypeName proto entry, so they cannot be carried on this wire
        # format without a proto/pb2 change (out of scope here). They round-trip
        # via the PickleCoder fallback in process_table_state instead.
        raise NotImplementedError(
            f"PTF FieldType writer: DataType {type(dt).__name__} has no "
            f"Schema.FieldType proto representation. Supported on-wire types: "
            f"primitives, DECIMAL, CHAR/VARCHAR, BINARY/VARBINARY, DATE/TIME, "
            f"TIMESTAMP/LOCAL_ZONED_TIMESTAMP/ZONED_TIMESTAMP, ARRAY, MULTISET, "
            f"MAP, ROW, NULL."
        )


@Internal()
def _fill_state_ttl_config(pb_ttl_config, ttl_string: str) -> None:
    """
    Parse a duration string like ``"1 hour"``, ``"3 days"``, ``"45 min"`` and
    fill the StateTTLConfig proto.
    """
    ms = _parse_duration_to_millis(ttl_string)
    from pyflink.fn_execution.flink_fn_execution_pb2 import StateDescriptor as Pb
    pb_ttl_config.update_type = Pb.StateTTLConfig.OnCreateAndWrite
    pb_ttl_config.state_visibility = Pb.StateTTLConfig.NeverReturnExpired
    pb_ttl_config.ttl_time_characteristic = Pb.StateTTLConfig.ProcessingTime
    pb_ttl_config.ttl = ms
    # Cleanup strategy defaults — full-state-scan + RocksDB compaction filter.
    cs = pb_ttl_config.cleanup_strategies
    cs.is_cleanup_in_background = True


@Internal()
def _parse_duration_to_millis(s: str) -> int:
    """
    Best-effort parse of a duration string into milliseconds.

    Supported forms (case-insensitive)::

        500 ms, 500 milliseconds
        30 s, 30 sec, 30 second(s)
        45 min, 45 minute(s)
        1 hour, 3 hours
        2 day, 2 days

    Always requires a space between the count and the unit; "10s" is rejected.
    """
    s = s.strip().lower()
    parts = s.split()
    if len(parts) != 2:
        raise ValueError(
            f"Cannot parse TTL '{s}'. Expected format like '1 hour', "
            f"'30 min', '500 ms'."
        )
    try:
        n = int(parts[0])
    except ValueError:
        raise ValueError(f"Cannot parse TTL count: '{parts[0]}'")

    unit = parts[1]
    # Singular table; we tolerate trailing 's' for plurals except for "ms"
    # (whose 's' is part of the unit name itself, not a plural).
    multipliers = {
        'ms': 1,
        'millisecond': 1,
        'milliseconds': 1,
        's': 1000,
        'sec': 1000,
        'secs': 1000,
        'second': 1000,
        'seconds': 1000,
        'min': 60 * 1000,
        'mins': 60 * 1000,
        'minute': 60 * 1000,
        'minutes': 60 * 1000,
        'hour': 3600 * 1000,
        'hours': 3600 * 1000,
        'day': 86400 * 1000,
        'days': 86400 * 1000,
    }
    if unit not in multipliers:
        raise ValueError(f"Unknown TTL unit: '{unit}'")
    return n * multipliers[unit]
