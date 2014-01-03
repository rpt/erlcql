%% Copyright (c) 2013 Krzysztof Rutka
%%
%% Permission is hereby granted, free of charge, to any person obtaining a
%% copy of this software and associated documentation files (the "Software"),
%% to deal in the Software without restriction, including without limitation
%% the rights to use, copy, modify, merge, publish, distribute, sublicense,
%% and/or sell copies of the Software, and to permit persons to whom the
%% Software is furnished to do so, subject to the following conditions:
%%
%% The above copyright notice and this permission notice shall be included in
%% all copies or substantial portions of the Software.
%%
%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
%% FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
%% IN THE SOFTWARE.

%% @doc Native protocol decoder/parser.
%% @author Krzysztof Rutka <krzysztof.rutka@gmail.com>
-module(erlcql_decode).

%% API
-export([new_parser/0,
         parse/3]).

-include("erlcql.hrl").

-define(BYTE, 1/big-signed-integer-unit:8).
-define(BYTE2, 2/big-signed-integer-unit:8).
-define(STRING(Length), Length/bytes).

%%-----------------------------------------------------------------------------
%% API functions
%%-----------------------------------------------------------------------------

%% @doc Returns a new parser.
-spec new_parser() -> parser().
new_parser() ->
    #parser{}.

%% @doc Parses given data using a parser.
-spec parse(binary(), parser(), compression()) ->
          {ok, Responses :: [{Stream :: integer(),
                              Response :: response()}],
           NewParser :: parser()} |
          {error, Reason :: term()}.
parse(Data, #parser{buffer = Buffer} = Parser, Compression) ->
    NewBuffer = <<Buffer/binary, Data/binary>>,
    NewParser = Parser#parser{buffer = NewBuffer},
    run_parser(NewParser, [], Compression).

%%-----------------------------------------------------------------------------
%% Parser functions
%%-----------------------------------------------------------------------------

-spec run_parser(parser(), [{integer(), response()}], compression()) ->
          {ok, Responses :: [{Stream :: integer(),
                              Response :: response()}],
           NewParser :: parser()} |
          {error, Reason :: term()}.
run_parser(#parser{buffer = Buffer} = Parser, Responses, Compression) ->
    case decode(Buffer, Compression) of
        {ok, Stream, Response, Leftovers} ->
            NewParser = Parser#parser{buffer = Leftovers},
            run_parser(NewParser,
                       [{Stream, Response} | Responses], Compression);
        {error, binary_too_small} ->
            {ok, lists:reverse(Responses), Parser};
        {error, Other} ->
            {error, Other}
    end.

%%-----------------------------------------------------------------------------
%% Decode functions
%%-----------------------------------------------------------------------------

-spec decode(binary(), compression()) ->
          {ok, Stream :: integer(), Response :: response(), Rest :: binary()} |
          {error, Reason :: term()}.
decode(<<?RESPONSE:1, ?VERSION:7, _Flags:7, Decompress:1,
         Stream:?BYTE, Opcode:8, Length:32, Data:Length/binary,
         Rest/binary>>, Compression) ->
    Data2 = maybe_decompress(Decompress, Compression, Data),
    Response = case opcode(Opcode) of
                   error ->
                       error2(Data2);
                   ready ->
                       ready(Data2);
                   authenticate ->
                       authenticate(Data2);
                   supported ->
                       supported(Data2);
                   result ->
                       result(Data2);
                   event ->
                       event(Data2)
               end,
    {ok, Stream, Response, Rest};
decode(<<_Other:32, Length:32, _Data:Length/binary,
         _Rest/binary>>, _Compression) ->
    {error, bad_header};
decode(_Other, _Compression) ->
    {error, binary_too_small}.

-spec maybe_decompress(0 | 1, compression(), binary()) -> binary().
maybe_decompress(0, _Compression, Data) ->
    Data;
maybe_decompress(1, snappy, Data) ->
    {ok, DecompressedData} = snappy:decompress(Data),
    DecompressedData;
maybe_decompress(1, lz4, <<Size:32, Data/binary>>) ->
    {ok, UnpackedData} = lz4:uncompress(Data, Size),
    UnpackedData.

-spec opcode(integer()) -> response_opcode().
opcode(16#00) -> error;
opcode(16#02) -> ready;
opcode(16#03) -> authenticate;
opcode(16#06) -> supported;
opcode(16#08) -> result;
opcode(16#0c) -> event.

%% Error ----------------------------------------------------------------------

-spec error2(binary()) -> cql_error().
error2(<<ErrorCode:?INT, Data/binary>>) ->
    Error = case error_code(ErrorCode) of
                unavailable_exception ->
                    unavailable_exception(Data);
                write_timeout ->
                    write_timeout(Data);
                read_timeout ->
                    read_timeout(Data);
                already_exists ->
                    already_exists(Data);
                Other ->
                    other_error(Other, Data)
            end,
    {Code, Message, Extra} = Error,
    ?ERROR("Error message from Cassandra: ~s \"~s\"", [Code, Message]),
    {error, {Code, Message, Extra}}.

-spec error_code(integer()) -> error_code().
error_code(16#0000) -> server_error;
error_code(16#000A) -> protocol_error;
error_code(16#0100) -> bad_credentials;
error_code(16#1000) -> unavailable_exception;
error_code(16#1001) -> overloaded;
error_code(16#1002) -> is_bootstapping;
error_code(16#1003) -> truncate_error;
error_code(16#1100) -> write_timeout;
error_code(16#1200) -> read_timeout;
error_code(16#2000) -> syntax_error;
error_code(16#2100) -> unauthorized;
error_code(16#2200) -> invalid;
error_code(16#2300) -> config_error;
error_code(16#2400) -> already_exists;
error_code(16#2500) -> unprepared.

-spec unavailable_exception(binary()) ->
          {unavailable_exception, Message :: bitstring(),
           {Consistency :: consistency(),
            Required :: integer(), Alive :: integer()}}.
unavailable_exception(<<Length:?SHORT, Message:Length/binary,
                        Consistency:?SHORT, Required:?INT, Alive:?INT>>) ->
    {unavailable_exception, Message, {consistency(Consistency),
                                      Required, Alive}}.

-spec write_timeout(binary()) ->
          {write_timeout, Message :: bitstring(),
           {Consistency :: consistency(), Received :: integer(),
            Blockfor :: integer(), WriteType :: bitstring()}}.
write_timeout(<<Length:?SHORT, Message:Length/binary,
                Consistency:?SHORT, Received:?INT, Blockfor:?INT,
                Length2:?SHORT, WriteType:Length2/binary>>) ->
    {write_timeout, Message, {consistency(Consistency),
                              Received, Blockfor, WriteType}}.

-spec read_timeout(binary()) ->
          {read_timeout, Message :: bitstring(),
           {Consistency :: consistency(), Received :: integer(),
            Blockfor :: integer(), DataPresent :: boolean()}}.
read_timeout(<<Length:?SHORT, Message:Length/binary, Consistency:?SHORT,
               Received:?INT, Blockfor:?INT, PresentByte:8>>) ->
    Present = PresentByte == 0,
    {read_timeout, Message, {consistency(Consistency),
                             Received, Blockfor, Present}}.

-spec already_exists(binary()) ->
          {already_exists, Message :: bitstring(),
           {Keyspace :: bitstring(), Table :: bitstring()}}.
already_exists(<<Length:?SHORT, Message:Length/binary,
                 Length2:?SHORT, Keyspace:Length2/binary,
                 Length3:?SHORT, Table:Length3/binary>>) ->
    {already_exists, Message, {Keyspace, Table}}.

-spec other_error(error_code(), binary()) ->
          {Code :: error_code(), Message :: bitstring(), undefined}.
other_error(ErrorCode, <<Length:?SHORT, Message:Length/binary>>) ->
    {ErrorCode, Message, undefined}.

-spec consistency(integer()) -> consistency().
consistency(0) -> any;
consistency(1) -> one;
consistency(2) -> two;
consistency(3) -> three;
consistency(4) -> quorum;
consistency(5) -> all;
consistency(6) -> local_quorum;
consistency(7) -> each_quorum.

%% Ready ----------------------------------------------------------------------

-spec ready(binary()) -> ready.
ready(<<>>) ->
    ready.

%% Authenticate ---------------------------------------------------------------

-spec authenticate(binary()) -> authenticate().
authenticate(<<Length:?SHORT, AuthClass:?STRING(Length)>>) ->
    {authenticate, AuthClass}.

%% Supported ------------------------------------------------------------------

-spec supported(binary()) -> supported().
supported(<<N:?SHORT, Data/binary>>) ->
    Supported = supported_map(N, Data, []),
    {ok, Supported}.

-spec supported_map(integer(), binary(), [{binary(), [binary()]}]) ->
          Map :: [{binary(), [binary()]}].
supported_map(0, <<>>, Map) ->
    lists:reverse(Map);
supported_map(N, <<Length:?SHORT, Key:Length/binary,
                   M:?SHORT, Data/binary>>, Map) ->
    {Values, Rest} = supported_list(M, Data, []),
    supported_map(N - 1, Rest, [{Key, Values} | Map]).

-spec supported_list(integer(), binary(), [binary()]) ->
          {List :: [binary()], Rest :: binary()}.
supported_list(0, Rest, Values) ->
    {lists:reverse(Values), Rest};
supported_list(N, <<Length:?SHORT, Value:Length/binary,
                    Rest/binary>>, Values) ->
    supported_list(N - 1, Rest, [Value | Values]).

%% Result ---------------------------------------------------------------------

-spec result(binary()) -> result().
result(<<Kind:?INT, Data/binary>>) ->
    case result_kind(Kind) of
        void ->
            void(Data);
        rows ->
            rows(Data);
        set_keyspace ->
            set_keyspace(Data);
        prepared ->
            prepared(Data);
        schema_change ->
            schema_change(Data)
    end.

-spec result_kind(integer()) -> result_kind().
result_kind(16#0001) -> void;
result_kind(16#0002) -> rows;
result_kind(16#0003) -> set_keyspace;
result_kind(16#0004) -> prepared;
result_kind(16#0005) -> schema_change.

%% Result: Void

-spec void(binary()) -> void().
void(<<>>) ->
    {ok, void}.

%% Result: Rows

-spec rows(binary()) -> rows().
rows(Data) ->
    {ColumnCount, ColumnSpecs, RowData} = metadata(Data),
    <<RowCount:?INT, RowContent/binary>> = RowData,
    {_, ColumnTypes} = lists:unzip(ColumnSpecs),
    Rows = rows(RowCount, ColumnCount, ColumnTypes, RowContent, []),
    {ok, {Rows, ColumnSpecs}}.

-spec metadata(binary()) -> {integer(), column_specs(), Rest :: binary()}.
metadata(<<_:31, 0:1, ColumnCount:?INT, ColumnData/binary>>) ->
    {ColumnSpecs, Rest} = column_specs(false, ColumnCount, ColumnData, []),
    {ColumnCount, ColumnSpecs, Rest};
metadata(<<_:31, 1:1, ColumnCount:?INT,
           Length:?SHORT, _Keyspace:?STRING(Length),
           Length2:?SHORT, _Table:?STRING(Length2), ColumnData/binary>>) ->
    {ColumnSpecs, Rest} = column_specs(true, ColumnCount, ColumnData, []),
    {ColumnCount, ColumnSpecs, Rest}.

-spec column_specs(boolean(), integer(), binary(), column_specs()) ->
          {column_specs(), Rest :: binary()}.
column_specs(_Global, 0, Rest, ColumnSpecs) ->
    {lists:reverse(ColumnSpecs), Rest};
column_specs(true, N, <<Length:?SHORT, Name:?STRING(Length), TypeData/binary>>,
             ColumnSpecs) ->
    {Type, Rest} = option(TypeData),
    column_specs(true, N - 1, Rest, [{Name, Type} | ColumnSpecs]);
column_specs(false, N, <<Length:?SHORT, _Keyspace:?STRING(Length),
                         Length2:?SHORT, _Table:?STRING(Length2),
                         Length3:?SHORT, Name:?STRING(Length3), TypeData/binary>>,
             ColumnSpecs) ->
    {Type, Rest} = option(TypeData),
    column_specs(false, N - 1, Rest, [{Name, Type} | ColumnSpecs]).

-spec option(binary()) -> {option(), Rest :: binary()}.
option(<<Id:?SHORT, Data/binary>>) ->
    case option_id(Id) of
        custom ->
            custom_option(Data);
        list ->
            {Type, Rest} = option(Data),
            {{list, Type}, Rest};
        map ->
            {KeyType, Rest} = option(Data),
            {ValueType, Rest2} = option(Rest),
            {{map, KeyType, ValueType}, Rest2};
        set ->
            {Type, Rest} = option(Data),
            {{set, Type}, Rest};
        OptionId ->
            {OptionId, Data}
    end.

-spec custom_option(binary()) -> {option(), Rest :: binary()}.
custom_option(<<Length:?SHORT, Value:?STRING(Length), Rest/binary>>) ->
    {{custom, Value}, Rest}.

-spec option_id(integer()) -> option_id().
option_id(16#0000) -> custom;
option_id(16#0001) -> ascii;
option_id(16#0002) -> bigint;
option_id(16#0003) -> blob;
option_id(16#0004) -> boolean;
option_id(16#0005) -> counter;
option_id(16#0006) -> decimal;
option_id(16#0007) -> double;
option_id(16#0008) -> float;
option_id(16#0009) -> int;
option_id(16#000a) -> test;
option_id(16#000b) -> timestamp;
option_id(16#000c) -> uuid;
option_id(16#000d) -> varchar;
option_id(16#000e) -> varint;
option_id(16#000f) -> timeuuid;
option_id(16#0010) -> inet;
option_id(16#0020) -> list;
option_id(16#0021) -> map;
option_id(16#0022) -> set.

-spec rows(integer(), integer(), [option()], binary(), [[binary()]]) ->
          Rows :: [[binary()]].
rows(0, _N, _Types, <<>>, Rows) ->
    lists:reverse(Rows);
rows(M, N, Types, Data, Rows) ->
    {Values, Rest} = row_values(N, Types, Data, []),
    rows(M - 1, N, Types, Rest, [Values | Rows]).

-spec row_values(integer(), [option()], binary(), [binary()]) ->
          {Values :: [binary()], Rest :: binary()}.
row_values(0, [], Rest, Values) ->
    {lists:reverse(Values), Rest};
row_values(N, [_ | Types], <<-1:?INT, Rest/binary>>, Values) ->
    row_values(N - 1, Types, Rest, [null | Values]);
row_values(N, [Type | Types], <<Length:?INT, Value:Length/binary,
                                Rest/binary>>, Values) ->
    Value2 = convert_value(Type, Value),
    row_values(N - 1, Types, Rest, [Value2 | Values]).

-spec convert_value(option(), binary()) -> term().
convert_value(bigint, <<Int:64/signed>>) ->
    Int;
convert_value(boolean, <<_:7, Int:1>>) ->
    Int == 1;
convert_value(counter, Value) ->
    convert_value(bigint, Value);
convert_value(decimal, <<Scale:32, Binary/binary>>) ->
    Size = byte_size(Binary) * 8,
    <<Value:Size/signed>> = Binary,
    Value * math:pow(10, -Scale);
convert_value(double, <<Float:64/float>>) ->
    Float;
convert_value(float, <<Float:32/float>>) ->
    Float;
convert_value(inet, <<A:?BYTE, B:?BYTE, C:?BYTE, D:?BYTE>>) ->
    {A, B, C, D};
convert_value(inet, <<A:?BYTE2, B:?BYTE2, C:?BYTE2, D:?BYTE2,
                      E:?BYTE2, F:?BYTE2, G:?BYTE2, H:?BYTE2>>) ->
    {A, B, C, D, E, F, G, H};
convert_value(int, <<Int:32/signed>>) ->
    Int;
convert_value(timestamp, Value) ->
    convert_value(bigint, Value);
convert_value(timeuuid, Value) ->
    convert_value(uuid, Value);
convert_value(uuid, <<_:128>> = Uuid) ->
    list_to_binary(uuid:uuid_to_string(Uuid));
convert_value(varint, Value) ->
    binary:decode_unsigned(Value);
convert_value({list, Type}, <<N:?SHORT, Data/binary>>) ->
    convert_list(N, Type, Data, []);
convert_value({set, Type}, Value) ->
    convert_value({list, Type}, Value);
convert_value({map, KeyType, ValueType}, <<N:?SHORT, Data/binary>>) ->
    convert_map(N, {KeyType, ValueType}, Data, []);
convert_value(_Other, Value) ->
    Value.

-spec convert_list(integer(), option_id(), binary(), [term()]) -> [term()].
convert_list(0, _Type, <<>>, Values) ->
    lists:reverse(Values);
convert_list(N, Type, <<Size:?SHORT, Value:Size/binary,
                        Rest/binary>>, Values) ->
    Value2 = convert_value(Type, Value),
    convert_list(N - 1, Type, Rest, [Value2 | Values]).

-spec convert_map(integer(), {option_id(), option_id()}, binary(),
                  [{term(), term()}]) -> [{term(), term()}].
convert_map(0, _Types, <<>>, Values) ->
    lists:reverse(Values);
convert_map(N, {KeyType, ValueType} = Types,
            <<KeySize:?SHORT, Key:KeySize/binary, ValueSize:?SHORT,
              Value:ValueSize/binary, Rest/binary>>, Values) ->
    Key2 = convert_value(KeyType, Key),
    Value2 = convert_value(ValueType, Value),
    convert_map(N - 1, Types, Rest, [{Key2, Value2} | Values]).

%% Result: Set keyspace

-spec set_keyspace(binary()) -> set_keyspace().
set_keyspace(<<Length:?SHORT, Keyspace:Length/binary>>) ->
    {ok, Keyspace}.

%% Result: Prepared

-spec prepared(binary()) -> prepared().
prepared(<<Length:?SHORT, QueryId:Length/binary, _Metadata/binary>>) ->
    %% {ColumnCount, ColumnSpecs, <<>>} = metadata(Metadata),
    {ok, QueryId}.

%% Result: Schema change

-spec schema_change(binary()) -> schema_change().
schema_change(<<Length:?SHORT, Type:Length/binary,
                Length2:?SHORT, _Keyspace:Length2/binary,
                Length3:?SHORT, _Table:Length3/binary>>) ->
    {ok, schema_change_type(Type)}.

%% Event ----------------------------------------------------------------------

-spec event(binary()) -> event_res().
event(<<Length:?SHORT, Type:?STRING(Length), Data/binary>>) ->
    Event = case event_type(Type) of
                topology_change ->
                    topology_change_event(Data);
                status_change ->
                    status_change_event(Data);
                schema_change ->
                    schema_change_event(Data)
            end,
    {event, Event}.

-spec event_type(bitstring()) -> event_type().
event_type(<<"TOPOLOGY_CHANGE">>) -> topology_change;
event_type(<<"STATUS_CHANGE">>) -> status_change;
event_type(<<"SCHEMA_CHANGE">>) -> schema_change.

-spec topology_change_event(binary()) ->
          {topology_change, Type :: atom(), Inet :: inet()}.
topology_change_event(<<Length:?SHORT, Type:?STRING(Length), Data/binary>>) ->
    {topology_change, topology_change_type(Type), inet(Data)}.

-spec topology_change_type(bitstring()) -> atom().
topology_change_type(<<"NEW_NODE">>) -> new_node;
topology_change_type(<<"REMOVED_NODE">>) -> removed_node.

-spec status_change_event(binary()) ->
          {status_change, Type :: atom(), Inet :: inet()}.
status_change_event(<<Length:?SHORT, Type:?STRING(Length), Data/binary>>) ->
    {status_change, status_change_type(Type), inet(Data)}.

-spec status_change_type(bitstring()) -> atom().
status_change_type(<<"UP">>) -> up;
status_change_type(<<"DOWN">>) -> down.

-spec schema_change_event(binary()) ->
          {schema_change, Type :: atom(),
           {Keyspace :: bitstring(), Table :: bitstring()}}.
schema_change_event(<<Length:?SHORT, Type:?STRING(Length),
                      Length2:?SHORT, Keyspace:?STRING(Length2),
                      Length3:?SHORT, Table:?STRING(Length3)>>) ->
    {schema_change, schema_change_type(Type), {Keyspace, Table}}.

-spec schema_change_type(bitstring()) -> atom().
schema_change_type(<<"CREATED">>) -> created;
schema_change_type(<<"UPDATED">>) -> updated;
schema_change_type(<<"DROPPED">>) -> dropped.

-spec inet(binary()) -> inet().
inet(<<Size:?BYTE, Data/binary>>) ->
    inet(Size, Data).

-spec inet(4 | 16, binary()) -> inet().
inet(4, <<A:?BYTE, B:?BYTE, C:?BYTE, D:?BYTE, Port:?INT>>) ->
    {{A, B, C, D}, Port};
inet(16, <<A:?BYTE2, B:?BYTE2, C:?BYTE2, D:?BYTE2,
           E:?BYTE2, F:?BYTE2, G:?BYTE2, H:?BYTE2, Port:?INT>>) ->
    {{A, B, C, D, E, F, G, H}, Port}.

