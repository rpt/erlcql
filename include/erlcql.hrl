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

%% @author Krzysztof Rutka <krzysztof.rutka@gmail.com>

%% Directions
-define(REQUEST, 0).
-define(RESPONSE, 1).

%% Versions
-define(VERSION, 1).
-define(CQL_VERSION, <<"3.0.0">>).

%% Encode/decode types
-define(INT, 4/big-signed-integer-unit:8).
-define(SHORT, 2/big-unsigned-integer-unit:8).
-define(BYTE, 1/big-signed-integer-unit:8).
-define(BYTE2, 2/big-signed-integer-unit:8).
-define(LENGTH, 4/big-unsigned-integer-unit:8).
-define(STRING(Length), Length/bytes).

%% Parser
-record(parser, {
          buffer = <<>> :: binary()
         }).
-type parser() :: #parser{}.

%%-----------------------------------------------------------------------------
%% Types
%%-----------------------------------------------------------------------------

%% Responses

-type cql_error() :: {error, {Code :: error_code(),
                              Message :: bitstring(),
                              Extra :: term()}}.
-type authenticate() :: {authenticate, AuthClass :: bitstring()}.
-type supported() :: {ok, [{Name :: bitstring(), [Value :: bitstring()]}]}.
-type rows() :: {rows,
                 Metadata :: {ColumnCount :: integer(),
                              Keyspace :: bitstring(),
                              Table :: bitstring(),
                              Specs :: [{Name :: bitstring(),
                                         Type :: term()}]},
                 Rows :: {RowCount :: integer(),
                          RowData :: [[bitstring() | binary()]]}}.
-type set_keyspace() :: {ok, Keyspace :: bitstring()}.
-type prepared() :: {ok, PreparedQueryId :: binary()}.
-type schema_change() :: {ok, {created | updated | dropped,
                               Keyspace :: bitstring(),
                               Table :: bitstring()}}.
-type result() :: void
                | rows()
                | set_keyspace()
                | prepared()
                | schema_change().
-type event() :: {event, {Kind :: atom(), Type :: atom(), Extra :: term()}}.

-type response() :: cql_error()
                  | ready
                  | authenticate()
                  | supported()
                  | result()
                  | event().

%% Enumerations

-type response_opcode() :: error
                         | ready
                         | authenticate
                         | supported
                         | result
                         | execute
                         | event.

-type consistency() :: any
                     | one
                     | two
                     | three
                     | quorum
                     | all
                     | local_quorum
                     | each_quorum.

-type event_type() :: topology_change
                    | status_change
                    | schema_change.

-type error_code() :: server_error
                    | protocol_error
                    | bad_credentials
                    | unavailable_exception
                    | overloaded
                    | is_bootstrapping
                    | truncate_error
                    | write_timeout
                    | read_timeout
                    | syntax_error
                    | unauthorized
                    | invalid
                    | config_error
                    | already_exists
                    | unprepared.

-type result_kind() :: void
                     | rows
                     | set_keyspace
                     | prepared
                     | schema_change.

-type option_id() :: custom
                   | ascii
                   | bigint
                   | blob
                   | boolean
                   | counter
                   | decimal
                   | double
                   | float
                   | int
                   | timestamp
                   | uuid
                   | varchar
                   | varint
                   | timeuuid
                   | inet
                   | list
                   | map
                   | set.

%% Others

-type inet() :: {inet:ip_address(), inet:port_number()}.

%%-----------------------------------------------------------------------------
%% Logging macros
%%-----------------------------------------------------------------------------

-ifdef(LOGS).
-ifdef(LAGER).
-compile({parse_transform, lager_transform}).
-define(EMERGENCY(Format, Data), lager:emergency(Format, Data)).
-define(ALERT(Format, Data), lager:alert(Format, Data)).
-define(CRITICAL(Format, Data), lager:critical(Format, Data)).
-define(ERROR(Format, Data), lager:error(Format, Data)).
-define(WARNING(Format, Data), lager:warning(Format, Data)).
-define(NOTICE(Format, Data), lager:notice(Format, Data)).
-define(INFO(Format, Data), lager:info(Format, Data)).
-define(DEBUG(Format, Data), lager:debug(Format, Data)).
-else.
-define(ERROR(Format, Data), error_logger:error_msg(Format ++ "~n", Data)).
-define(EMERGENCY(Format, Data), ?ERROR(Format, Data)).
-define(ALERT(Format, Data), ?ERROR(Format, Data)).
-define(CRITICAL(Format, Data), ?ERROR(Format, Data)).
-define(WARNING(Format, Data), error_logger:warning_msg(Format ++ "~n", Data)).
-define(INFO(Format, Data), error_logger:info_msg(Format ++ "~n", Data)).
-define(NOTICE(Format, Data), ?INFO(Format, Data)).
-define(DEBUG(Format, Data), ?INFO(Format, Data)).
-endif.
-define(EMERGENCY(Format), ?EMERGENCY(Format, [])).
-define(ALERT(Format), ?ALERT(Format, [])).
-define(CRITICAL(Format), ?CRITICAL(Format, [])).
-define(ERROR(Format), ?ERROR(Format, [])).
-define(WARNING(Format), ?WARNING(Format, [])).
-define(NOTICE(Format), ?NOTICE(Format, [])).
-define(INFO(Format), ?INFO(Format, [])).
-define(DEBUG(Format), ?DEBUG(Format, [])).
-else.
-define(ERROR(_Format, _Data), ok).
-define(ERROR(_Format), ok).
-define(EMERGENCY(_Format, _Data), ok).
-define(EMERGENCY(_Format), ok).
-define(ALERT(_Format, _Data), ok).
-define(ALERT(_Format), ok).
-define(CRITICAL(_Format, _Data), ok).
-define(CRITICAL(_Format), ok).
-define(WARNING(_Format, _Data), ok).
-define(WARNING(_Format), ok).
-define(INFO(_Format, _Data), ok).
-define(INFO(_Format), ok).
-define(NOTICE(_Format, _Data), ok).
-define(NOTICE(_Format), ok).
-define(DEBUG(_Format, _Data), ok).
-define(DEBUG(_Format), ok).
-endif.

