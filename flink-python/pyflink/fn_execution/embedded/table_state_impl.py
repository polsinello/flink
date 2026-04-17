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
Embedded (thread-mode) keyed state backend adapter for Table API aggregate operations.

Provides a RemoteKeyedStateBackend-compatible interface over a Java KeyedStateBackend
accessed via pemja. This lets GroupAggFunction, GroupTableAggFunction, and
GroupWindowAggFunction work unchanged in embedded mode by providing the same state API
they expect from the process-mode RemoteKeyedStateBackend.

State is stored as byte[] in Java, with Python coders handling serialization/deserialization.

Namespaces:
  - Group / table aggregates leave the state in VoidNamespace (single namespace per key).
  - Window aggregates call set_current_namespace(window) on each state handle before
    accessing it to pick the right per-window state slot. The adapter converts the
    Python window (TimeWindow / CountWindow) into a Java window object via pemja and
    re-acquires the state through KeyedStateBackend.getPartitionedState, which internally
    calls setCurrentNamespace on the backing InternalKvState.
"""

from pemja import findClass

from pyflink.datastream import TimeWindow, CountWindow

# Java state descriptor classes
JValueStateDescriptor = findClass('org.apache.flink.api.common.state.ValueStateDescriptor')
JListStateDescriptor = findClass('org.apache.flink.api.common.state.ListStateDescriptor')
JMapStateDescriptor = findClass('org.apache.flink.api.common.state.MapStateDescriptor')

# Java type info for byte[]
JPrimitiveArrayTypeInfo = findClass(
    'org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo')
BYTE_ARRAY_TYPE_INFO = JPrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO

# Java namespace classes for partitioned state access
JVoidNamespace = findClass('org.apache.flink.runtime.state.VoidNamespace')
JVoidNamespaceSerializer = findClass('org.apache.flink.runtime.state.VoidNamespaceSerializer')
JVoidNamespace_INSTANCE = JVoidNamespace.INSTANCE
JVoidNamespaceSerializer_INSTANCE = JVoidNamespaceSerializer.INSTANCE

# Java RowData classes for key conversion
JGenericRowData = findClass('org.apache.flink.table.data.GenericRowData')
JBinaryStringData = findClass('org.apache.flink.table.data.binary.BinaryStringData')

# Java window classes - used to translate Python TimeWindow / CountWindow objects into
# the Java objects that KeyedStateBackend.getPartitionedState expects as the namespace.
JTimeWindow = findClass('org.apache.flink.table.runtime.operators.window.TimeWindow')
JCountWindow = findClass('org.apache.flink.table.runtime.operators.window.CountWindow')


def _encode(coder_impl, value):
    """Encode a Python value to bytes using a FieldCoderImpl.

    Uses the high-level .encode() API which internally picks the right stream
    implementation (Cython or pure-Python). Avoid encode_to_stream directly —
    the Cython PickleCoderImpl rejects pure-Python OutputStream instances.
    """
    return coder_impl.encode(value)


def _decode(coder_impl, data):
    """Decode bytes to a Python value using a FieldCoderImpl."""
    return coder_impl.decode(data)


def _to_java_key(key):
    """
    Convert a Python key (list of field values) to a Java GenericRowData.

    The key comes from GroupAggFunction as a Python list like [1, "alice"].
    We must convert it to a GenericRowData with proper internal Flink types:
    - str -> BinaryStringData.fromString(val)
    - int, float, bool, None -> pass through (pemja handles conversion)
    """
    arity = len(key)
    j_row = JGenericRowData(arity)
    for i in range(arity):
        val = key[i]
        if isinstance(val, str):
            j_row.setField(i, JBinaryStringData.fromString(val))
        else:
            # int, float, bool, None, and other types are passed through;
            # pemja handles the Java conversion automatically.
            j_row.setField(i, val)
    return j_row


def _py_to_java_window(py_window):
    """Translate a Python TimeWindow / CountWindow into its Java counterpart."""
    if isinstance(py_window, TimeWindow):
        return JTimeWindow(py_window.start, py_window.end)
    if isinstance(py_window, CountWindow):
        return JCountWindow(py_window.id)
    raise ValueError(
        "Unsupported namespace type for embedded state backend: %s" % type(py_window))


class _BaseStateHandle(object):
    """Common scaffolding for namespace-aware state handles.

    The Java state handle is re-acquired lazily every time the namespace changes so
    that KeyedStateBackend.getPartitionedState can call setCurrentNamespace on the
    underlying InternalKvState. When no namespace is explicitly set the handle stays
    bound to VoidNamespace (the group/table aggregate case).
    """

    def __init__(self, j_backend, j_descriptor, j_namespace_serializer):
        self._j_backend = j_backend
        self._j_descriptor = j_descriptor
        self._j_namespace_serializer = j_namespace_serializer
        # Python-side namespace (for equality checks). None means VoidNamespace.
        self._current_py_namespace = None
        # Java state handle; invalidated on namespace change.
        self._j_state = None

    def set_current_namespace(self, py_namespace):
        if py_namespace == self._current_py_namespace:
            return
        self._current_py_namespace = py_namespace
        self._j_state = None

    def _get_j_state(self):
        if self._j_state is not None:
            return self._j_state
        if self._current_py_namespace is None:
            self._j_state = self._j_backend.getPartitionedState(
                JVoidNamespace_INSTANCE,
                JVoidNamespaceSerializer_INSTANCE,
                self._j_descriptor)
        else:
            if self._j_namespace_serializer is None:
                raise RuntimeError(
                    "set_current_namespace was called on a state handle that was not "
                    "configured with a namespace serializer; initialize the state backend "
                    "with a window namespace serializer to use windowed state.")
            j_window = _py_to_java_window(self._current_py_namespace)
            self._j_state = self._j_backend.getPartitionedState(
                j_window, self._j_namespace_serializer, self._j_descriptor)
        return self._j_state


class _ValueStateHandle(_BaseStateHandle):
    """Value state handle backed by Java ValueState<byte[]> plus a Python coder."""

    def __init__(self, j_backend, j_descriptor, coder_impl, j_namespace_serializer):
        super().__init__(j_backend, j_descriptor, j_namespace_serializer)
        self._coder_impl = coder_impl

    def value(self):
        data = self._get_j_state().value()
        if data is None:
            return None
        return _decode(self._coder_impl, bytes(data))

    def update(self, val):
        if val is None:
            self._get_j_state().clear()
        else:
            data = _encode(self._coder_impl, val)
            self._get_j_state().update(data)

    def clear(self):
        self._get_j_state().clear()


class _ListStateHandle(_BaseStateHandle):
    """List state handle backed by Java ListState<byte[]> plus a Python element coder."""

    def __init__(self, j_backend, j_descriptor, coder_impl, j_namespace_serializer):
        super().__init__(j_backend, j_descriptor, j_namespace_serializer)
        self._coder_impl = coder_impl

    def get(self):
        j_iterable = self._get_j_state().get()
        if j_iterable is None:
            return
        for item in j_iterable:
            yield _decode(self._coder_impl, bytes(item))

    def add(self, value):
        data = _encode(self._coder_impl, value)
        self._get_j_state().add(data)

    def add_all(self, values):
        for value in values:
            self.add(value)

    def update(self, values):
        self.clear()
        self.add_all(values)

    def clear(self):
        self._get_j_state().clear()


class _MapStateHandle(_BaseStateHandle):
    """Map state handle backed by Java MapState<byte[], byte[]> plus Python key/value coders."""

    def __init__(self, j_backend, j_descriptor, key_coder_impl, value_coder_impl,
                 j_namespace_serializer):
        super().__init__(j_backend, j_descriptor, j_namespace_serializer)
        self._key_coder_impl = key_coder_impl
        self._value_coder_impl = value_coder_impl

    def get(self, key):
        encoded_key = _encode(self._key_coder_impl, key)
        data = self._get_j_state().get(encoded_key)
        if data is None:
            return None
        return _decode(self._value_coder_impl, bytes(data))

    def put(self, key, value):
        encoded_key = _encode(self._key_coder_impl, key)
        encoded_value = _encode(self._value_coder_impl, value)
        self._get_j_state().put(encoded_key, encoded_value)

    def put_all(self, dict_value):
        for k, v in dict_value.items():
            self.put(k, v)

    def remove(self, key):
        encoded_key = _encode(self._key_coder_impl, key)
        self._get_j_state().remove(encoded_key)

    def contains(self, key):
        encoded_key = _encode(self._key_coder_impl, key)
        return self._get_j_state().contains(encoded_key)

    def items(self):
        entries = self._get_j_state().entries()
        if entries is None:
            return
        for entry in entries:
            k = _decode(self._key_coder_impl, bytes(entry.getKey()))
            v = _decode(self._value_coder_impl, bytes(entry.getValue()))
            yield (k, v)

    def keys(self):
        j_keys = self._get_j_state().keys()
        if j_keys is None:
            return
        for k in j_keys:
            yield _decode(self._key_coder_impl, bytes(k))

    def values(self):
        j_values = self._get_j_state().values()
        if j_values is None:
            return
        for v in j_values:
            yield _decode(self._value_coder_impl, bytes(v))

    def is_empty(self):
        return self._get_j_state().isEmpty()

    def clear(self):
        self._get_j_state().clear()


class EmbeddedTableKeyedStateBackend(object):
    """
    A keyed state backend adapter for Table API aggregate operations in embedded (thread) mode.

    Provides the same interface as RemoteKeyedStateBackend (from state_impl.py) so that
    GroupAggFunction, GroupTableAggFunction, and GroupWindowAggFunction can work unchanged.

    State is stored as byte[] in the Java KeyedStateBackend. Python FieldCoder objects
    handle serialization/deserialization between Python values and byte arrays.

    State handles are cached by (type, name) tuple to avoid recreating Java state descriptors
    on every call. This is important because GroupAggFunction calls get_value_state("accumulators",
    coder) on every key in finish_bundle().

    Optional ``j_window_namespace_serializer`` enables per-window state access for window
    aggregates. When set, state handles returned by the factory methods below support
    ``set_current_namespace(py_window)``; without it the handles stay VoidNamespace-bound
    (the group/table aggregate case).
    """

    def __init__(self, j_keyed_state_backend, j_window_namespace_serializer=None,
                 j_key_row_serializer=None):
        self._j_backend = j_keyed_state_backend
        self._j_window_namespace_serializer = j_window_namespace_serializer
        # Java RowDataSerializer for the group-by key row type. When provided, we
        # call `.toBinaryRow(generic_row)` before setCurrentKey so that the key
        # stored in the state backend has the same hashCode as the BinaryRowData
        # that came through the keyed shuffle. Without this, GenericRowData's
        # Arrays.hashCode disagrees with BinaryRowData's binary-buffer hash and
        # the KeyedStateBackend's key-group check fails with
        # `Key group X is not in KeyGroupRange{startKeyGroup=Y, endKeyGroup=Z}`.
        self._j_key_row_serializer = j_key_row_serializer
        self._current_key = None
        # Cache state handles by (state_type, name) to avoid recreating Java descriptors.
        # The Java state handle is bound to the backend and automatically reflects the
        # current key, so we only need one handle per (type, name).
        self._state_cache = {}

    @property
    def current_key(self):
        return self._current_key

    def get_current_key(self):
        return self._current_key

    def set_current_key(self, key):
        """
        Set the current key for subsequent state operations.

        The key is a Python list of field values (e.g., [1, "alice"]).
        Converts to a Java GenericRowData, then to BinaryRowData via the
        RowDataSerializer when available (for correct hashCode matching the
        upstream shuffle), and calls j_backend.setCurrentKey().
        """
        self._current_key = key
        j_key = _to_java_key(key)
        if self._j_key_row_serializer is not None:
            j_key = self._j_key_row_serializer.toBinaryRow(j_key)
        self._j_backend.setCurrentKey(j_key)

    def get_value_state(self, name, value_coder, ttl_config=None):
        """
        Get or create a value state handle.

        Args:
            name: State name (e.g., "accumulators")
            value_coder: A FieldCoder with .get_impl() returning a coder impl
                         that has .encode(value) and .decode(bytes) methods
            ttl_config: Optional TTL config (currently unused in embedded mode,
                        accepted for interface compatibility)

        Returns:
            A _ValueStateHandle with .value(), .update(val), .clear(),
            and .set_current_namespace(ns) for windowed access.
        """
        cache_key = ('value', name)
        if cache_key in self._state_cache:
            return self._state_cache[cache_key]

        j_descriptor = JValueStateDescriptor(name, BYTE_ARRAY_TYPE_INFO)
        coder_impl = value_coder.get_impl()
        handle = _ValueStateHandle(
            self._j_backend, j_descriptor, coder_impl,
            self._j_window_namespace_serializer)
        self._state_cache[cache_key] = handle
        return handle

    def get_list_state(self, name, element_coder, ttl_config=None):
        """
        Get or create a list state handle.

        Args:
            name: State name
            element_coder: A FieldCoder for individual list elements
            ttl_config: Optional TTL config (accepted for interface compatibility)

        Returns:
            A _ListStateHandle with .get(), .add(v), .add_all(values),
            .update(values), .clear(), and .set_current_namespace(ns).
        """
        cache_key = ('list', name)
        if cache_key in self._state_cache:
            return self._state_cache[cache_key]

        j_descriptor = JListStateDescriptor(name, BYTE_ARRAY_TYPE_INFO)
        coder_impl = element_coder.get_impl()
        handle = _ListStateHandle(
            self._j_backend, j_descriptor, coder_impl,
            self._j_window_namespace_serializer)
        self._state_cache[cache_key] = handle
        return handle

    def get_map_state(self, name, map_key_coder, map_value_coder, ttl_config=None):
        """
        Get or create a map state handle.

        Args:
            name: State name
            map_key_coder: A FieldCoder for map keys
            map_value_coder: A FieldCoder for map values
            ttl_config: Optional TTL config (accepted for interface compatibility)

        Returns:
            A _MapStateHandle with .get(key), .put(key, value), .put_all(dict),
            .remove(key), .contains(key), .items(), .keys(), .values(),
            .is_empty(), .clear(), and .set_current_namespace(ns).
        """
        cache_key = ('map', name)
        if cache_key in self._state_cache:
            return self._state_cache[cache_key]

        j_descriptor = JMapStateDescriptor(name, BYTE_ARRAY_TYPE_INFO, BYTE_ARRAY_TYPE_INFO)
        key_coder_impl = map_key_coder.get_impl()
        value_coder_impl = map_value_coder.get_impl()
        handle = _MapStateHandle(
            self._j_backend, j_descriptor, key_coder_impl, value_coder_impl,
            self._j_window_namespace_serializer)
        self._state_cache[cache_key] = handle
        return handle

    def clear_cached_iterators(self):
        """No-op in embedded mode — there are no gRPC iterator caches to clear."""
        pass

    def commit(self):
        """No-op in embedded mode — writes go directly to the Java state backend."""
        pass
