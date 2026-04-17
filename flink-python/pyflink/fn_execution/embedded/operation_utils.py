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
from pyflink.fn_execution.embedded.operations import (OneInputFunctionOperation,
                                                      TwoInputFunctionOperation)

from pyflink.fn_execution.embedded.converters import from_type_info_proto, from_schema_proto


def pare_user_defined_data_stream_function_proto(proto):
    from pyflink.fn_execution import flink_fn_execution_pb2
    serialized_fn = flink_fn_execution_pb2.UserDefinedDataStreamFunction()
    serialized_fn.ParseFromString(proto)
    return serialized_fn


def parse_coder_proto(proto):
    from pyflink.fn_execution import flink_fn_execution_pb2

    coder = flink_fn_execution_pb2.CoderInfoDescriptor()
    coder.ParseFromString(proto)
    return coder


def parse_function_proto(proto):
    from pyflink.fn_execution import flink_fn_execution_pb2
    serialized_fn = flink_fn_execution_pb2.UserDefinedFunctions()
    serialized_fn.ParseFromString(proto)
    return serialized_fn


def create_scalar_operation_from_proto(proto,
                                       input_coder_info,
                                       output_coder_into,
                                       one_arg_optimization=False,
                                       one_result_optimization=False):
    from pyflink.fn_execution.table.operations import ScalarFunctionOperation

    serialized_fn = parse_function_proto(proto)

    input_data_converter = (
        from_schema_proto(
            parse_coder_proto(input_coder_info).flatten_row_type.schema,
            one_arg_optimization))

    output_data_converter = (
        from_schema_proto(
            parse_coder_proto(output_coder_into).flatten_row_type.schema,
            one_result_optimization))

    scalar_operation = ScalarFunctionOperation(
        serialized_fn, one_arg_optimization, one_result_optimization)

    process_element_func = scalar_operation.process_element

    def process_element(value):
        actual_value = input_data_converter.to_internal(value)
        result = process_element_func(actual_value)
        return output_data_converter.to_external(result)

    scalar_operation.process_element = process_element
    return scalar_operation


def create_table_operation_from_proto(proto, input_coder_info, output_coder_into):
    from pyflink.fn_execution.table.operations import TableFunctionOperation

    serialized_fn = parse_function_proto(proto)

    input_data_converter = (
        from_schema_proto(parse_coder_proto(input_coder_info).flatten_row_type.schema))
    output_data_converter = (
        from_schema_proto(parse_coder_proto(output_coder_into).flatten_row_type.schema))

    table_operation = TableFunctionOperation(serialized_fn)

    process_element_func = table_operation.process_element

    def process_element(value):
        actual_value = input_data_converter.to_internal(value)
        results = process_element_func(actual_value)
        for result in results:
            yield output_data_converter.to_external(result)

    table_operation.process_element = process_element

    return table_operation


def create_one_input_user_defined_data_stream_function_from_protos(
        function_infos, input_coder_info, output_coder_info, runtime_context,
        function_context, timer_context, side_output_context, job_parameters, keyed_state_backend,
        operator_state_backend):
    serialized_fns = [pare_user_defined_data_stream_function_proto(proto)
                      for proto in function_infos]
    input_data_converter = (
        from_type_info_proto(parse_coder_proto(input_coder_info).raw_type.type_info))
    output_data_converter = (
        from_type_info_proto(parse_coder_proto(output_coder_info).raw_type.type_info))

    function_operation = OneInputFunctionOperation(
        serialized_fns,
        input_data_converter,
        output_data_converter,
        runtime_context,
        function_context,
        timer_context,
        side_output_context,
        job_parameters,
        keyed_state_backend,
        operator_state_backend)

    return function_operation


def create_two_input_user_defined_data_stream_function_from_protos(
        function_infos, input_coder_info1, input_coder_info2, output_coder_info, runtime_context,
        function_context, timer_context, side_output_context, job_parameters, keyed_state_backend,
        operator_state_backend):
    serialized_fns = [pare_user_defined_data_stream_function_proto(proto)
                      for proto in function_infos]

    input_data_converter1 = (
        from_type_info_proto(parse_coder_proto(input_coder_info1).raw_type.type_info))

    input_data_converter2 = (
        from_type_info_proto(parse_coder_proto(input_coder_info2).raw_type.type_info))

    output_data_converter = (
        from_type_info_proto(parse_coder_proto(output_coder_info).raw_type.type_info))

    function_operation = TwoInputFunctionOperation(
        serialized_fns,
        input_data_converter1,
        input_data_converter2,
        output_data_converter,
        runtime_context,
        function_context,
        timer_context,
        side_output_context,
        job_parameters,
        keyed_state_backend,
        operator_state_backend)

    return function_operation


# ---------------------------------------------------------------------------
# Aggregate operation factories (group-by aggregation in embedded/thread mode)
# ---------------------------------------------------------------------------

def parse_aggregate_function_proto(proto):
    """Parse bytes into a UserDefinedAggregateFunctions protobuf message."""
    from pyflink.fn_execution import flink_fn_execution_pb2
    serialized_fn = flink_fn_execution_pb2.UserDefinedAggregateFunctions()
    serialized_fn.ParseFromString(proto)
    return serialized_fn


def _wrap_aggregate_operation(operation, input_field_converters=None,
                              output_field_converters=None):
    """
    Wrap an aggregate operation's process_element, finish_bundle, and on_timer
    methods for embedded (thread-mode) type conversion.

    Java (via pemja) sends/receives tuples (Object[]), while the Python aggregate
    operations work with pyflink.table.Row objects carrying RowKind metadata.

    Input conversion (process_element):
        Java sends Object[] -> Python tuple: (row_kind_byte, field0, field1, ...)
        Wrapped to: Row(field0, field1, ...) with RowKind set from byte.

    When ``input_field_converters`` is provided, each field value is routed through
    the matching DataConverter's ``to_internal`` before being fed to the aggregate
    op — this is how nested ROW fields become ``pyflink.table.Row`` with named
    attributes (the shape user UDAF ``accumulate`` expects), how pemja
    ``PyJObject`` temporal values become native Python ``datetime``, etc.
    ``RowDataConverter`` in particular handles the ``[row_kind, [fields]]`` shape
    that Java's ``PythonTypeUtils.RowDataConverter.toExternalImpl`` emits for
    nested rows and recursively builds a Row with field names.

    Output conversion (finish_bundle):
        Python yields Row with RowKind set.
        Wrapped to yield: (row_kind_byte, field0, field1, ...)

    Timer handling (on_timer):
        Java sends Object[] -> Python tuple of key field values.
        Wrapped to: Row(key_field0, key_field1, ...)
    """
    from pyflink.table import Row
    from pyflink.common import RowKind
    from pyflink import fn_execution

    group_agg = operation.group_agg_function

    # When the Cython fast path is enabled (the default), `group_agg_function`
    # expects `InternalRow` (cdef class) instead of pyflink.table.Row. Mirror the
    # conversion `StreamGroupAggregateOperation.process_element_or_timer` does in
    # process mode: build a Row, then wrap via `InternalRow.from_row(row)`.
    if fn_execution.PYFLINK_CYTHON_ENABLED:
        from pyflink.fn_execution.coder_impl_fast import InternalRow

        # Build InternalRow directly (skip the Row intermediary) — InternalRow.from_row
        # reads ``row._fields`` which is only populated when Row is constructed with
        # kwargs. pemja gives us positional values so no field names are known.
        # InternalRowKind is a Cython cdef enum (not exported) so we just pass the raw
        # int; Cython casts it at the boundary.
        def _to_op_row(values, row_kind_value):
            return InternalRow(list(values), row_kind_value)
    else:
        def _to_op_row(values, row_kind_value):
            row = Row(*values)
            row.set_row_kind(RowKind(row_kind_value))
            return row

    if input_field_converters is not None:
        def _convert_fields(values):
            return [c.to_internal(v) for c, v in zip(input_field_converters, values)]
    else:
        def _convert_fields(values):
            return values

    if output_field_converters is not None:
        def _convert_out_fields(values):
            return [c.to_external(v) for c, v in zip(output_field_converters, values)]
    else:
        def _convert_out_fields(values):
            return list(values)

    # Wrap process_element: convert incoming tuple to Row/InternalRow with RowKind.
    # In embedded mode Java calls process_element directly (not through the
    # process_element_or_timer dispatcher used in process mode), so we call
    # group_agg_function.process_element directly.
    def process_element(value):
        group_agg.process_element(_to_op_row(_convert_fields(value[1:]), value[0]))

    operation.process_element = process_element

    # Wrap finish_bundle: convert outgoing Row/InternalRow to tuple. With the
    # Cython fast path enabled the aggregate op yields InternalRow (cdef class
    # with readonly ``values`` / ``row_kind`` attributes); without Cython it
    # yields pyflink.table.Row.
    original_finish_bundle = operation.finish_bundle

    if fn_execution.PYFLINK_CYTHON_ENABLED:
        def finish_bundle():
            for result in original_finish_bundle():
                yield (result.row_kind,) + tuple(_convert_out_fields(result.values))
    else:
        def finish_bundle():
            for result in original_finish_bundle():
                yield (result.get_row_kind().value,) + tuple(_convert_out_fields(result._values))

    # Java calls "finish_bundle_func" to get a callable that returns a generator
    operation.finish_bundle_func = finish_bundle

    # Wrap on_timer: convert incoming key tuple to Row/InternalRow (the
    # aggregate op's on_timer reads the values list, so the Cython cdef Row
    # path uses InternalRow too).
    if fn_execution.PYFLINK_CYTHON_ENABLED:
        def on_timer(key_values):
            # Timer key arrives without a row-kind — default to INSERT (0).
            group_agg.on_timer(InternalRow(list(key_values), 0))
    else:
        def on_timer(key_values):
            group_agg.on_timer(Row(*key_values))

    operation.on_timer = on_timer

    return operation


def _build_input_field_converters(input_type_proto):
    """Parse an input Schema proto and build per-field DataConverters for aggregate
    operators. Returns None if ``input_type_proto`` is None/empty so callers that
    haven't wired this through still work (identity pass-through)."""
    if not input_type_proto:
        return None
    from pyflink.fn_execution.embedded.converters import from_field_type_proto
    from pyflink.fn_execution import flink_fn_execution_pb2
    schema = flink_fn_execution_pb2.Schema()
    schema.ParseFromString(input_type_proto)
    return [from_field_type_proto(f.type) for f in schema.fields]


def create_group_aggregate_operation_from_proto(proto, keyed_state_backend, key_row_type_info,
                                                j_key_row_serializer=None,
                                                input_type_proto=None,
                                                output_type_proto=None):
    """
    Create a StreamGroupAggregateOperation for embedded (thread) mode.

    Called from Java's openPythonInterpreter() via pemja:
        interpreter.exec("operation = create_group_aggregate_operation_from_proto(...)")

    Args:
        proto: Serialized UserDefinedAggregateFunctions protobuf bytes.
        keyed_state_backend: Java KeyedStateBackend instance (accessed via pemja).
        key_row_type_info: String[] of key field type names (currently unused;
                           key conversion is handled by EmbeddedTableKeyedStateBackend).
        j_key_row_serializer: Java RowDataSerializer for the key RowType. When provided,
                              set_current_key converts the reconstructed GenericRowData
                              to a BinaryRowData via ``.toBinaryRow`` so its hashCode
                              matches the upstream shuffle's BinaryRowData hash (the
                              KeyedStateBackend's key-group check is strict).
        input_type_proto: Serialized Schema proto of the input RowType. When provided,
                          per-field converters are applied to incoming values so nested
                          ROW fields become pyflink.table.Row with named attributes
                          (match user UDAF expectation) and temporal PyJObjects are
                          unwrapped to native Python datetime/date/time.

    Returns:
        A StreamGroupAggregateOperation with wrapped process_element, finish_bundle_func,
        and on_timer methods for pemja type conversion.
    """
    from pyflink.fn_execution.table.operations import StreamGroupAggregateOperation
    from pyflink.fn_execution.embedded.table_state_impl import EmbeddedTableKeyedStateBackend

    serialized_fn = parse_aggregate_function_proto(proto)
    embedded_state_backend = EmbeddedTableKeyedStateBackend(
        keyed_state_backend, j_key_row_serializer=j_key_row_serializer)
    operation = StreamGroupAggregateOperation(serialized_fn, embedded_state_backend)
    return _wrap_aggregate_operation(
        operation,
        input_field_converters=_build_input_field_converters(input_type_proto),
        output_field_converters=_build_input_field_converters(output_type_proto))


def create_group_table_aggregate_operation_from_proto(proto, keyed_state_backend,
                                                      key_row_type_info,
                                                      j_key_row_serializer=None,
                                                      input_type_proto=None,
                                                      output_type_proto=None):
    """
    Create a StreamGroupTableAggregateOperation for embedded (thread) mode.

    See :func:`create_group_aggregate_operation_from_proto` for the
    ``j_key_row_serializer`` and ``input_type_proto`` arguments — they're used
    identically here (BinaryRowData key hashing + recursive nested-row field
    conversion for user UDAF attribute access).
    """
    from pyflink.fn_execution.table.operations import StreamGroupTableAggregateOperation
    from pyflink.fn_execution.embedded.table_state_impl import EmbeddedTableKeyedStateBackend

    serialized_fn = parse_aggregate_function_proto(proto)
    embedded_state_backend = EmbeddedTableKeyedStateBackend(
        keyed_state_backend, j_key_row_serializer=j_key_row_serializer)
    operation = StreamGroupTableAggregateOperation(serialized_fn, embedded_state_backend)
    return _wrap_aggregate_operation(
        operation,
        input_field_converters=_build_input_field_converters(input_type_proto),
        output_field_converters=_build_input_field_converters(output_type_proto))


def create_group_window_aggregate_operation_from_proto(
        proto, keyed_state_backend, key_row_type_info,
        input_type_proto, j_window_namespace_serializer,
        j_key_row_serializer=None):
    """
    Create a StreamGroupWindowAggregateOperation for embedded (thread) mode.

    Called from Java's openPythonInterpreter() via pemja:
        interpreter.exec("operation = create_group_window_aggregate_operation_from_proto(...)")

    Window aggregates use a different flow than group/table aggregates:
      - Input carries a tagged record: (record_type, row_tuple, timestamp, watermark, timer_tuple)
        where record_type=0 (NORMAL_RECORD) or 1 (TRIGGER_TIMER).
      - process_element is a generator that yields output records of two shapes:
          (NORMAL_RECORD, row_kind_byte, field_values_tuple, None)  -> emit row downstream
          (TRIGGER_TIMER, 0, None, (timer_op, key_tuple, timestamp, encoded_ns_bytes))
              -> Java registers/deletes a window timer on the keyed state backend.
      - No separate finish_bundle: results are emitted inline with each call.

    Args:
        proto: Serialized UserDefinedAggregateFunctions protobuf bytes (includes GroupWindow).
        keyed_state_backend: Java KeyedStateBackend instance (accessed via pemja).
        key_row_type_info: String[] of key field type names (interface parity; unused).
        input_type_proto: Serialized Schema protobuf of the input RowType. Used to build
            field-level converters so temporal types (TIMESTAMP_LTZ rowtime -> datetime)
            arrive as the Python objects GroupWindowAggFunction expects.
        j_window_namespace_serializer: Java TypeSerializer for the window namespace
            (TimeWindow.Serializer / CountWindow.Serializer). Required so per-window
            state access goes through getPartitionedState with the correct namespace.

    Returns:
        A StreamGroupWindowAggregateOperation whose process_element has been wrapped
        for pemja type marshalling.
    """
    from pyflink.fn_execution.table.operations import StreamGroupWindowAggregateOperation
    from pyflink.fn_execution.embedded.table_state_impl import EmbeddedTableKeyedStateBackend
    from pyflink.fn_execution.embedded.converters import from_schema_proto
    from pyflink.fn_execution.coder_impl_slow import TimeWindowCoderImpl, CountWindowCoderImpl
    from pyflink.fn_execution import flink_fn_execution_pb2
    from pyflink.table import Row
    from pyflink.common import RowKind

    NORMAL_RECORD = 0
    TRIGGER_TIMER = 1

    serialized_fn = parse_aggregate_function_proto(proto)

    embedded_state_backend = EmbeddedTableKeyedStateBackend(
        keyed_state_backend, j_window_namespace_serializer,
        j_key_row_serializer=j_key_row_serializer)
    # StreamGroupWindowAggregateOperation.create_process_function reads
    # keyed_state_backend._namespace_coder_impl during __init__ - must set it before
    # constructing the operation. Time vs count window dictates encoding: time windows
    # serialize (start, end) as two int64s; count windows serialize a single int64 id.
    if serialized_fn.group_window.is_time_window:
        embedded_state_backend._namespace_coder_impl = TimeWindowCoderImpl()
    else:
        embedded_state_backend._namespace_coder_impl = CountWindowCoderImpl()

    # Build a per-field converter for the input row. Non-temporal types fall through
    # via IdentityDataConverter; TIMESTAMP / TIMESTAMP_LTZ / DATE / TIME get converted
    # from the java.time.* PyJObjects pemja hands over into native Python datetime/date/time.
    input_schema = flink_fn_execution_pb2.Schema()
    input_schema.ParseFromString(input_type_proto)
    input_row_converter = from_schema_proto(input_schema)
    # Field names are needed on every input Row because StreamGroupWindowAggregateOperation
    # wraps it as InternalRow.from_row(row) under Cython, which dereferences row._fields.
    input_field_names = [f.name for f in input_schema.fields]

    operation = StreamGroupWindowAggregateOperation(serialized_fn, embedded_state_backend)
    process_element_or_timer = operation.process_element_or_timer

    def process_element(java_args):
        # java_args: (record_type, row_tuple_or_None, timestamp_or_None,
        #             watermark, timer_tuple_or_None)
        record_type = java_args[0]

        if record_type == NORMAL_RECORD:
            # row_tuple: (row_kind_byte, field0, field1, ...)
            row_tuple = java_args[1]
            converted_fields = input_row_converter.to_internal(row_tuple[1:])
            input_row = Row(*converted_fields)
            input_row.set_field_names(input_field_names)
            input_row.set_row_kind(RowKind(row_tuple[0]))
            input_data = (record_type, input_row, None, java_args[3], None)
        else:
            # timer_tuple: (timer_type_byte, key_tuple, encoded_namespace_bytes)
            timer_tuple = java_args[4]
            timer_row = Row(timer_tuple[0], Row(*timer_tuple[1]), timer_tuple[2])
            input_data = (record_type, None, java_args[2], java_args[3], timer_row)

        for result in process_element_or_timer(input_data):
            # result shape: [record_type, result_row_or_None, timer_row_or_None]
            result_type = result[0]
            if result_type == NORMAL_RECORD:
                result_row = result[1]
                # result_row may be either pyflink.common.Row (slow path) or the
                # Cython-defined InternalRow (fast path). The two have different APIs:
                # Row exposes get_row_kind()/._values, InternalRow exposes row_kind/values.
                if hasattr(result_row, '_values'):
                    row_kind_value = result_row.get_row_kind().value
                    fields = tuple(result_row._values)
                else:
                    row_kind_value = result_row.row_kind
                    fields = tuple(result_row.values)
                yield (NORMAL_RECORD, row_kind_value, fields, None)
            else:
                # timer_row._values = [timer_op_int, key_row, timestamp_int, encoded_ns_bytes]
                timer_data = result[2]
                timer_op = timer_data._values[0]
                key_row = timer_data._values[1]
                timer_ts = timer_data._values[2]
                encoded_ns = timer_data._values[3]
                yield (TRIGGER_TIMER,
                       0,
                       None,
                       (timer_op, tuple(key_row._values), timer_ts, encoded_ns))

    operation.process_element = process_element
    return operation
