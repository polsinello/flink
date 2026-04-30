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
State-slot proxies for the PTF process-mode runtime.

Each user-declared @state_hint slot is realized at the worker via one of three
proxies: :class:`PtfValueState` (single-element keyed ListState),
:class:`PtfListView` (state-backed ``pyflink.table.ListView``), and
:class:`PtfMapView` (state-backed ``pyflink.table.MapView``).

The on-wire state name is ``"ptf:" + uid + ":" + slot_name`` to disambiguate
state across multiple PTF calls in the same query (matching the Java PTF
``uid`` system arg semantics).

The underlying Beam-driven keyed-state machinery is reused from the existing
UDAF DataView path (``pyflink.fn_execution.state_impl``).
"""
from __future__ import annotations

from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple, TypeVar

from pyflink.fn_execution.coders import PickleCoder, from_proto
from pyflink.table.data_view import ListView, MapView

T = TypeVar('T')
K = TypeVar('K')
V = TypeVar('V')


_STATE_NAME_PREFIX = "ptf"


def encode_state_name(uid: str, slot_name: str) -> str:
    """
    Encode the on-the-wire state name for a PTF state slot.

    The Beam ``BeamKeyedStateStore`` further prepends ``PYTHON_STATE_PREFIX``
    when constructing the Flink ``StateDescriptor``.
    """
    if not uid:
        raise ValueError("PTF uid must be non-empty for state slot encoding.")
    if ":" in slot_name:
        raise ValueError(
            f"PTF slot name '{slot_name}' must not contain ':' "
            f"(used as state-name separator)."
        )
    return f"{_STATE_NAME_PREFIX}:{uid}:{slot_name}"


class PtfValueState:
    """
    Single-value state slot, backed by a keyed ListState holding at most one
    element. Read returns the most recently written value, or ``None`` if never
    written.
    """

    __slots__ = ('_remote_list_state', '_dirty', '_value', '_loaded')

    def __init__(self, remote_list_state):
        # Loaded lazily; value() triggers the read on first access.
        self._remote_list_state = remote_list_state
        self._dirty = False
        self._value: Any = None
        self._loaded = False

    def value(self) -> Any:
        """Return the current value, or ``None`` if not set."""
        if not self._loaded:
            iterator = iter(self._remote_list_state.get())
            try:
                self._value = next(iterator)
            except StopIteration:
                self._value = None
            self._loaded = True
        return self._value

    def update(self, new_value: Any) -> None:
        """Overwrite the slot with ``new_value`` (may be ``None`` to clear)."""
        self._value = new_value
        self._loaded = True
        self._dirty = True

    def clear(self) -> None:
        """Remove any stored value."""
        self._value = None
        self._loaded = True
        self._dirty = True

    def _flush(self) -> None:
        """Write any pending update back to the backing state."""
        if not self._dirty:
            return
        self._remote_list_state.clear()
        if self._value is not None:
            self._remote_list_state.add(self._value)
        self._dirty = False


class PtfListView(ListView):
    """
    State-backed ListView proxy, duck-typed identical to
    ``pyflink.table.data_view.ListView``. Reads are streaming; ``add`` appends
    without re-reading.
    """

    def __init__(self, remote_list_state):
        # Skip super().__init__() (it sets _list = []); every accessor defers
        # to state instead.
        self._remote_list_state = remote_list_state

    def add(self, value: T) -> None:
        self._remote_list_state.add(value)

    def add_all(self, values: List[T]) -> None:
        self._remote_list_state.add_all(values)

    def get(self) -> Iterable[T]:
        return self._remote_list_state.get()

    def clear(self) -> None:
        self._remote_list_state.clear()

    def __iter__(self) -> Iterator[T]:
        return iter(self.get())


class PtfMapView(MapView):
    """
    State-backed MapView proxy, duck-typed identical to
    ``pyflink.table.data_view.MapView``. Per-key access is individual; bulk
    iteration walks the MapState lazily.
    """

    def __init__(self, remote_map_state):
        self._remote_map_state = remote_map_state

    def get(self, key: K) -> Optional[V]:
        return self._remote_map_state.get(key)

    def put(self, key: K, value: V) -> None:
        self._remote_map_state.put(key, value)

    def put_all(self, mapping: Dict[K, V]) -> None:
        self._remote_map_state.put_all(mapping)

    def remove(self, key: K) -> None:
        self._remote_map_state.remove(key)

    def contains(self, key: K) -> bool:
        return self._remote_map_state.contains(key)

    def items(self) -> Iterable[Tuple[K, V]]:
        return self._remote_map_state.items()

    def keys(self) -> Iterable[K]:
        return self._remote_map_state.keys()

    def values(self) -> Iterable[V]:
        return self._remote_map_state.values()

    def is_empty(self) -> bool:
        return self._remote_map_state.is_empty()

    def clear(self) -> None:
        self._remote_map_state.clear()


class PtfStateSlotRegistry:
    """
    Manages the state proxies for a single PTF instance. Created once per
    operator; ``set_current_key`` is called per input element to reset the
    per-key view of state. State is acquired lazily — ``bind()`` returns
    proxies, but no Beam state RPC is issued until a slot is read or written.
    """

    def __init__(self,
                 uid: str,
                 state_specs: List['_StateSpecHandle'],
                 keyed_state_backend):
        self._uid = uid
        self._specs = state_specs
        self._keyed_state_backend = keyed_state_backend
        self._proxies: Dict[str, Any] = {}
        # Underlying runtime-state objects keyed by slot name, reused across
        # keys (per-key behavior is handled by the keyed backend).
        self._remote_states: Dict[str, Any] = {}

    def set_current_key(self, key: Any) -> None:
        """Switch the keyed-state backend to ``key`` and reset proxy caches.

        PTF's keyed_state_backend is constructed with state_cache_size=0, so
        its ``set_current_key`` does NOT cache the outgoing key's in-memory
        state. We must commit the previous key's pending writes to the Java
        backend BEFORE switching, or they are dropped and lost across
        savepoint round-trips (only the last-active key would survive).
        """
        # Flush proxy updates into the underlying state buffer, then commit
        # each state object to the Java backend — both still keyed off the OLD
        # key — so the writes survive the key switch.
        for proxy in self._proxies.values():
            if isinstance(proxy, PtfValueState):
                proxy._flush()
        for state_obj in self._keyed_state_backend._all_states.values():
            internal_state = state_obj._internal_state
            if internal_state is not None:
                internal_state.commit()
                if hasattr(internal_state, '_cleared'):
                    internal_state._cleared = False
                if hasattr(internal_state, '_added_elements'):
                    internal_state._added_elements = []
        self._keyed_state_backend.set_current_key(key)
        # Reset value-state lazy-load flags so the next value() re-reads from
        # the new key's backing state.
        for proxy in self._proxies.values():
            if isinstance(proxy, PtfValueState):
                proxy._loaded = False
                proxy._dirty = False
                proxy._value = None

    def bind(self) -> List[Any]:
        """
        Return one proxy per declared slot, in declaration order.

        Proxies are created lazily and cached for the lifetime of the
        registry. The returned list is the same object set the user's
        ``eval`` / ``on_timer`` receives.
        """
        result: List[Any] = []
        for spec in self._specs:
            proxy = self._proxies.get(spec.name)
            if proxy is None:
                proxy = self._build_proxy(spec)
                self._proxies[spec.name] = proxy
            result.append(proxy)
        return result

    def flush(self) -> None:
        """Write back any pending state mutations (for ValueState proxies)."""
        for proxy in self._proxies.values():
            if isinstance(proxy, PtfValueState):
                proxy._flush()

    def clear_all_state(self) -> None:
        """Clear every slot for the current key."""
        for proxy in self._proxies.values():
            proxy.clear()
        # Force flush so the wire-level state is also cleared.
        self.flush()

    def clear_state(self, slot_name: str) -> None:
        """
        Clear a single named state slot for the current key. Lazily binds the
        slot's proxy if it has not been touched yet (so a cold ``clear_state``
        still wipes the on-the-wire backing store).
        """
        # Validate the slot name against declared specs.
        spec_match = next((s for s in self._specs if s.name == slot_name), None)
        if spec_match is None:
            available = [s.name for s in self._specs]
            raise KeyError(
                f"PTF state slot '{slot_name}' is not declared. "
                f"Available: {available}"
            )
        proxy = self._proxies.get(slot_name)
        if proxy is None:
            proxy = self._build_proxy(spec_match)
            self._proxies[slot_name] = proxy
        proxy.clear()
        # Flush only this slot's value-state pending writes.
        if isinstance(proxy, PtfValueState):
            proxy._flush()

    def _build_proxy(self, spec: '_StateSpecHandle'):
        encoded_name = encode_state_name(self._uid, spec.name)
        if spec.kind == 'VALUE':
            remote = self._get_or_create_list_state(encoded_name, spec)
            return PtfValueState(remote)
        elif spec.kind == 'LIST':
            remote = self._get_or_create_list_state(encoded_name, spec)
            return PtfListView(remote)
        elif spec.kind == 'MAP':
            remote = self._get_or_create_map_state(encoded_name, spec)
            return PtfMapView(remote)
        else:
            raise ValueError(f"Unknown state kind for slot '{spec.name}': "
                             f"{spec.kind}")

    def _get_or_create_list_state(self, encoded_name: str,
                                  spec: '_StateSpecHandle'):
        cached = self._remote_states.get(encoded_name)
        if cached is not None:
            return cached
        # VALUE state is backed by a single-element list; its element coder is
        # the declared value_type. LIST state's element coder is element_type.
        element_type = spec.value_type if spec.kind == 'VALUE' \
            else spec.element_type
        coder = _coder_for(element_type)
        remote = self._keyed_state_backend.get_list_state(
            encoded_name, coder, spec.ttl_config)
        self._remote_states[encoded_name] = remote
        return remote

    def _get_or_create_map_state(self, encoded_name: str,
                                 spec: '_StateSpecHandle'):
        cached = self._remote_states.get(encoded_name)
        if cached is not None:
            return cached
        key_coder = _coder_for(spec.map_key_type)
        value_coder = _coder_for(spec.map_value_type)
        remote = self._keyed_state_backend.get_map_state(
            encoded_name, key_coder, value_coder, spec.ttl_config)
        self._remote_states[encoded_name] = remote
        return remote


def _coder_for(field_type):
    """Build a typed coder from a ``Schema.FieldType`` proto.

    Falls back to :class:`PickleCoder` when the type is absent or has no
    dedicated coder (e.g. MULTISET / RAW / interval types), preserving the
    original pickle round-trip for those.
    """
    if field_type is None:
        return PickleCoder()
    try:
        return from_proto(field_type)
    except (ValueError, KeyError):
        return PickleCoder()


class _StateSpecHandle:
    """
    Decoded view of a single ``StateSpec`` proto entry, materialized as a
    Python object the registry can consume without re-parsing the proto.

    The ``*_type`` fields are the raw ``Schema.FieldType`` protos used to derive
    typed state coders; they are ``None`` for the slots they don't apply to.
    """

    __slots__ = ('name', 'kind', 'ttl_config', 'value_type', 'element_type',
                 'map_key_type', 'map_value_type')

    def __init__(self, name: str, kind: str, ttl_config,
                 value_type=None, element_type=None,
                 map_key_type=None, map_value_type=None):
        self.name = name
        # ``kind`` is one of "VALUE", "LIST", "MAP".
        self.kind = kind
        # ``ttl_config`` is a Flink-side StateTtlConfig or None.
        self.ttl_config = ttl_config
        self.value_type = value_type
        self.element_type = element_type
        self.map_key_type = map_key_type
        self.map_value_type = map_value_type


def build_state_spec_handles(pb_state_slots) -> List[_StateSpecHandle]:
    """
    Translate a repeated ``StateSpec`` proto field into a list of handles.

    The kind enum from the proto (``VALUE_STATE`` / ``LIST_STATE`` /
    ``MAP_STATE``) is normalized to ``"VALUE"`` / ``"LIST"`` / ``"MAP"``.
    """
    from pyflink.datastream.state import StateTtlConfig
    from pyflink.fn_execution.flink_fn_execution_pb2 import (
        UserDefinedProcessTableFunction as Pb,
    )
    kind_map = {
        Pb.VALUE_STATE: "VALUE",
        Pb.LIST_STATE: "LIST",
        Pb.MAP_STATE: "MAP",
    }
    handles: List[_StateSpecHandle] = []
    for slot in pb_state_slots:
        # Convert the proto StateTTLConfig to the user-facing StateTtlConfig
        # object expected by RemoteKeyedStateBackend.enable_time_to_live; the
        # backend calls .get_state_visibility() / .get_update_type() which
        # only exist on the latter.
        ttl = (StateTtlConfig._from_proto(slot.ttl_config)
               if slot.HasField('ttl_config') else None)
        which = slot.WhichOneof('type_info')
        handles.append(_StateSpecHandle(
            name=slot.name,
            kind=kind_map[slot.kind],
            ttl_config=ttl,
            value_type=slot.value_type if which == 'value_type' else None,
            element_type=slot.element_type if which == 'element_type' else None,
            map_key_type=slot.map_type.key_type if which == 'map_type' else None,
            map_value_type=(slot.map_type.value_type
                            if which == 'map_type' else None),
        ))
    return handles
