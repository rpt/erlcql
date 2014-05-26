%% Copyright (c) 2013-2014 Krzysztof Rutka
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

%% @doc Native protocol request encoding.
%% @author Krzysztof Rutka <krzysztof.rutka@gmail.com>
-module(erlcql_encode).

-export([frame/4]).
-export([startup/3]).
-export([credentials/2]).
-export([options/1]).
-export(['query'/3]).
-export([prepare/2]).
-export([execute/4]).
-export([register/2]).
-export([auth_response/2]).

-include("erlcql.hrl").

%% API function ---------------------------------------------------------------

%% @doc Encodes the entire request frame.
-spec frame(version(), request(), tuple(), integer()) -> Frame :: iolist().
frame(V, {Opcode, Payload}, {Compression, _}, Stream) ->
    OpcodeByte = opcode(Opcode),
    {CompressionBit, Payload2} = maybe_compress(Compression, Payload),
    Length = int(iolist_size(Payload2)),
    [<<?REQUEST:1, V:7, 0:7, CompressionBit:1>>,
     Stream, OpcodeByte, Length, Payload2].

%% @doc Encodes the startup request message body.
-spec startup(version(), compression(), bitstring()) -> {startup, iolist()}.
startup(_V, false, CQLVersion) ->
    {startup, string_map([{<<"CQL_VERSION">>, CQLVersion}])};
startup(_V, Compression, CQLVersion) ->
    {startup, string_map([{<<"CQL_VERSION">>, CQLVersion},
                          {<<"COMPRESSION">>, compression(Compression)}])}.

%% @doc Encodes the credentials request message body.
-spec credentials(version(), [{K :: bitstring(), V :: bitstring()}]) ->
          {credentials, iolist()}.
credentials(1, Informations) ->
    {credentials, string_map(Informations)}.

%% @doc Encodes the options request message body.
-spec options(version()) -> {options, iolist()}.
options(_V) ->
    {options, []}.

%% @doc Encodes the query request message body.
-spec 'query'(version(), iodata(), consistency()) -> {'query', iolist()}.
'query'(1, QueryString, Consistency) ->
    {'query', [long_string(QueryString), short(consistency(Consistency))]};
'query'(2, QueryString, Consistency) ->
    %% TODO: Implement support for query flags
    %%       native_protocol_v2.spec#L292
    Flags = 0,
    {'query', [long_string(QueryString),
               short(consistency(Consistency)), Flags]}.

%% @doc Encodes the prepare request message body.
-spec prepare(version(), iodata()) -> {prepare, iolist()}.
prepare(_V, QueryString) ->
    {prepare, [long_string(QueryString)]}.

%% @doc Encodes the execute request message body.
-spec execute(version(), binary(), values(), consistency()) ->
          {execute, iolist()}.
execute(1, QueryId, Values, Consistency) ->
    BinaryValues = erlcql_convert:to_binary(Values),
    {execute, [short_bytes(QueryId), BinaryValues,
               short(consistency(Consistency))]};
execute(2, QueryId, Values, Consistency) ->
    %% TODO: Implement support for query flags
    %%       native_protocol_v2.spec#L292
    %% TODO: Merge with regular query
    BinaryValues = erlcql_convert:to_binary(Values),
    Flags = 1,
    {execute, [short_bytes(QueryId), short(consistency(Consistency)),
               Flags, BinaryValues]}.

%% @doc Encodes the register request message body.
-spec register(version(), [event_type()]) -> {register, iolist()}.
register(_V, Events) ->
    {register, event_list(Events)}.

-spec auth_response(version(), binary()) -> {auth_response, iolist()}.
auth_response(2, Token) ->
    {auth_response, [bytes(Token)]}.

%% Encode functions -----------------------------------------------------------

-spec int(integer()) -> binary().
int(X) ->
    <<X:?INT>>.

-spec short(integer()) -> binary().
short(X) ->
    <<X:?SHORT>>.

-spec string2(bitstring()) -> iolist().
string2(String) ->
    Length = iolist_size(String),
    [short(Length), String].

-spec long_string(iolist()) -> iolist().
long_string(String) ->
    Length = iolist_size(String),
    [int(Length), String].

-spec bytes(binary()) -> iolist().
bytes(Bytes) ->
    Length = iolist_size(Bytes),
    [int(Length), Bytes].

-spec short_bytes(binary()) -> iolist().
short_bytes(Bytes) ->
    Length = iolist_size(Bytes),
    [short(Length), Bytes].

-spec string_map([{K :: bitstring(), V :: bitstring()}]) -> iolist().
string_map(KeyValues) ->
    N = length(KeyValues),
    [short(N) | [[string2(Key), string2(Value)]
                 || {Key, Value} <- KeyValues]].

-spec opcode(atom()) -> integer().
opcode(startup)       -> 16#01;
opcode(credentials)   -> 16#04;
opcode(options)       -> 16#05;
opcode('query')       -> 16#07;
opcode(prepare)       -> 16#09;
opcode(execute)       -> 16#0a;
opcode(register)      -> 16#0b;
opcode(auth_response) -> 16#0f.

-spec consistency(consistency()) -> integer().
consistency(any)          -> 16#00;
consistency(one)          -> 16#01;
consistency(two)          -> 16#02;
consistency(three)        -> 16#03;
consistency(quorum)       -> 16#04;
consistency(all)          -> 16#05;
consistency(local_quorum) -> 16#06;
consistency(each_quorum)  -> 16#07;
consistency(serial)       -> 16#08;
consistency(local_serial) -> 16#09;
consistency(local_one)    -> 16#0a.

-spec event(event_type()) -> bitstring().
event(topology_change) -> <<"TOPOLOGY_CHANGE">>;
event(status_change)   -> <<"STATUS_CHANGE">>;
event(schema_change)   -> <<"SCHEMA_CHANGE">>.

-spec event_list([event_type()]) -> iolist().
event_list(Events) ->
    N = length(Events),
    [short(N) | [string2(event(Event)) || Event <- Events]].

%% @doc Compresses the payload if compression is enabled.
-spec maybe_compress(compression(), iolist()) -> {0 | 1, iolist()}.
maybe_compress(false, Payload) ->
    {0, Payload};
maybe_compress(_Any, []) ->
    {0, <<>>};
maybe_compress(snappy, Payload) ->
    {ok, CompressedPayload} = snappy:compress(Payload),
    {1, CompressedPayload};
maybe_compress(lz4, Payload) ->
    PayloadBinary = iolist_to_binary(Payload),
    Size = byte_size(PayloadBinary),
    {ok, CompressedPayload} = lz4:compress(PayloadBinary),
    {1, <<Size:32, CompressedPayload/binary>>}.

-spec compression(compression()) -> bitstring().
compression(snappy) -> <<"snappy">>;
compression(lz4)    -> <<"lz4">>.
