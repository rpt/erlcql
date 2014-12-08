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

-module(erlcql_client).
-behaviour(gen_fsm).

-export([start_link/1]).
-export(['query'/3]).
-export([execute/4]).
-export([batch/3]).
-export([prepare/2]).
-export([prepare/3]).
-export([options/1]).
-export([register/2]).
-export([async_query/3]).
-export([async_execute/4]).
-export([await/1]).
-export([await/2]).
-export([get_env_opt/2]).

-export([init/1]).
-export([handle_event/3]).
-export([handle_sync_event/4]).
-export([handle_info/3]).
-export([code_change/4]).
-export([terminate/3]).
-export([startup/2]).
-export([startup/3]).
-export([ready/2]).
-export([ready/3]).

-include("erlcql.hrl").
-include("erlcql_metrics.hrl").


-record(state, {
          async_ets :: ets(),
          auto_reconnect :: boolean(),
          backoff :: backoff(),
          cql_version :: bitstring(),
          credentials :: {bitstring(), bitstring()},
          database :: {Host :: string(), Port :: inet:port_number()},
          events :: [event()],
          event_fun :: event_fun(),
          flags :: {atom(), boolean()},
          keepalive = false :: boolean(),
          keyspace :: bitstring(),
          parent :: pid(),
          parser :: parser(),
          prepare :: [{Name :: atom(), Query :: iodata()}],
          prepared_ets :: ets(),
          socket :: undefined | socket(),
          streams = lists:seq(1, 127) :: [integer()],
          version :: version()
         }).
-type state() :: #state{}.

-define(TCP_OPTS, [binary, {active, false}, {packet, raw}]).
-define(ASYNC_ETS_NAME, erlcql_async).
-define(ASYNC_ETS_OPTS, [set, private,
                         {write_concurrency, true},
                         {read_concurrency, true}]).
-define(PREPARED_ETS_NAME, erlcql_prepared).
-define(PREPARED_ETS_OPTS, [set, private,
                            {read_concurrency, true}]).

-record(backoff, {
          start :: pos_integer(),
          max :: pos_integer() | infinity,
          current :: pos_integer()
         }).
-type backoff() :: #backoff{}.

start_link(Opts) ->
    Opts2 = [{parent, self()} | Opts],
    EventFun = event_fun(get_env_opt(event_handler, Opts)),
    Opts3 = [{event_fun, EventFun} | Opts2],
    gen_fsm:start_link(?MODULE, proplists:unfold(Opts3), []).

-spec 'query'(pid(), iodata(), consistency()) ->
          result() | {error, Reason :: term()}.
'query'(Pid, QueryString, Params) ->
    async_call(Pid, {'query', QueryString, Params}).

-spec prepare(pid(), iodata()) -> prepared() | {error, Reason :: term()}.
prepare(Pid, QueryString) ->
    async_call(Pid, {prepare, QueryString}).

-spec prepare(pid(), iodata(), atom()) -> ok | {error, Reason :: term()}.
prepare(Pid, QueryString, Name) ->
    async_call(Pid, {prepare, QueryString, Name}).

-spec execute(pid(), erlcql:uuid() | atom(), values(), consistency()) ->
          result() | {error, Reason :: term()}.
execute(Pid, QueryId, Values, Consistency) ->
    async_call(Pid, {execute, QueryId, Values, Consistency}).

-spec batch(pid(), [{atom(), values()}], proplist()) ->
          result() | {error, Reason :: term()}.
batch(Pid, Queries, Params) ->
    async_call(Pid, {batch, Queries, Params}).

-spec options(pid()) -> supported() | {error, Reason :: term()}.
options(Pid) ->
    async_call(Pid, options).

-spec register(pid(), [event_type()]) -> ready | {error, Reason :: term()}.
register(Pid, Events) ->
    async_call(Pid, {register, Events}).

-spec async_query(pid(), iodata(), consistency()) ->
          {ok, QueryRef :: erlcql:query_ref()} | {error, Reason :: term()}.
async_query(Pid, QueryString, Params) ->
    cast(Pid, {'query', QueryString, Params}).

-spec async_execute(pid(), erlcql:uuid() | atom(), values(), consistency()) ->
          {ok, QueryRef :: erlcql:query_ref()} | {error, Reason :: term()}.
async_execute(Pid, QueryId, Values, Consistency) ->
    cast(Pid, {execute, QueryId, Values, Consistency}).

-spec await(erlcql:query_ref()) -> response() | {error, Reason :: term()}.
await({ok, QueryRef}) ->
    Timeout = get_env(default_timeout),
    do_await(QueryRef, Timeout);
await({error, _Reason} = Error) ->
    Error;
await(QueryRef) ->
    Timeout = get_env(default_timeout),
    do_await(QueryRef, Timeout).

-spec await(erlcql:query_ref(), integer()) ->
          response() | {error, Reason :: term()}.
await({ok, QueryRef}, Timeout) ->
    do_await(QueryRef, Timeout);
await({error, _Reason} = Error, _Timeout) ->
    Error;
await(QueryRef, Timeout) ->
    do_await(QueryRef, Timeout).

init(Opts) ->
    State = init_state(Opts),
    case State#state.auto_reconnect of
        false ->
            try_connect(State);
        true ->
            _Ref = gen_fsm:send_event_after(0, reconnect),
            {ok, startup, State}
    end.

try_connect(#state{database = {Host, Port},
                   keepalive = Keepalive} = State) ->
    ?DEBUG("Trying to connect to ~p:~p", [Host, Port]),
    case gen_tcp:connect(Host, Port, [{keepalive, Keepalive} | ?TCP_OPTS]) of
        {ok, Socket} ->
            ?INFO("Connected to ~p:~p", [Host, Port]),
            State2 = State#state{socket = Socket},
            case init_connection(State2) of
                {ok, State3} ->
                    ?DEBUG("Connection init successful"),
                    ok = inet:setopts(Socket, [{active, once}]),
                    {ok, ready, State3};
                {error, Reason} = Error ->
                    ?ERROR("Connection init failed: ~s", [Reason]),
                    ok = quintana:notify_histogram(?CONNECTION_FAILURE_METRIC, 1),
                    {stop, Error}
            end;
        {error, Reason} = Error ->
            ?ERROR("Cannot connect to ~p:~p: ~s", [Host, Port, Reason]),
            {stop, Error}
    end.

-spec init_state(proplist()) -> state().
init_state(Opts) ->
    AsyncETS = ets:new(?ASYNC_ETS_NAME, ?ASYNC_ETS_OPTS),
    AutoReconnect = get_env_opt(auto_reconnect, Opts),
    ReconnectStart = get_env_opt(reconnect_start, Opts),
    ReconnectMax = get_env_opt(reconnect_max, Opts),
    Backoff = backoff_init(ReconnectStart, ReconnectMax),
    CQLVersion = get_env_opt(cql_version, Opts),
    Username = get_env_opt(username, Opts),
    Password = get_env_opt(password, Opts),
    Credentials = {Username, Password},
    Host = get_env_opt(host, Opts),
    Port = get_env_opt(port, Opts),
    Database = {Host, Port},
    EventFun = get_internal_opt(event_fun, Opts),
    Events = get_env_opt(register, Opts),
    Compression = get_env_opt(compression, Opts),
    Tracing = get_env_opt(tracing, Opts),
    Flags = {Compression, Tracing},
    Keepalive = get_env_opt(keepalive, Opts),
    Keyspace = get_env_opt(use, Opts),
    Parent = get_internal_opt(parent, Opts),
    Prepare = get_env_opt(prepare, Opts),
    PreparedETS = maybe_create_prepared_ets(Opts),
    Version = get_env_opt(version, Opts),
    Parser = erlcql_decode:new_parser(Version),
    #state{async_ets = AsyncETS,
           auto_reconnect = AutoReconnect,
           backoff = Backoff,
           cql_version = CQLVersion,
           credentials = Credentials,
           database = Database,
           event_fun = EventFun,
           events = Events,
           flags = Flags,
           keepalive = Keepalive,
           keyspace = Keyspace,
           parent = Parent,
           parser = Parser,
           prepare = Prepare,
           prepared_ets = PreparedETS,
           version = Version}.

startup(reconnect, #state{backoff = Backoff,
                          database = {Host, Port},
                          keepalive = Keepalive} = State) ->
    ?DEBUG("Trying to connect to ~p:~p", [Host, Port]),
    case gen_tcp:connect(Host, Port, [{keepalive, Keepalive} | ?TCP_OPTS]) of
        {ok, Socket} ->
            ?INFO("Connected to ~p:~p", [Host, Port]),
            Backoff2 = backoff_succeed(Backoff),
            State2 = State#state{socket = Socket,
                                 backoff = Backoff2},
            case init_connection(State2) of
                {ok, State3} ->
                    ?DEBUG("Connection init successful"),
                    ok = inet:setopts(Socket, [{active, once}]),
                    {next_state, ready, State3};
                {error, Reason} ->
                    ?ERROR("Connection init failed: ~s", [Reason]),
                    try_again(State)
            end;
        {error, Reason} ->
            ?WARNING("Cannot connect to ~p:~p: ~s", [Host, Port, Reason]),
            try_again(State)
    end;
startup(Event, State) ->
    ?ERROR("Bad event (startup): ~p", [Event]),
    ok = quintana:notify_histogram(?CONNECTION_STARTUP_FAIL_METRIC, 1),
    {stop, {bad_event, Event}, State}.

startup({_Ref, {'query', Query, _}}, _From, State) ->
    not_ready('query', Query, State);
startup({_Ref, {prepare, Query}}, _From, State) ->
    not_ready(prepare, Query, State);
startup({_Ref, {prepare, Query, _}}, _From, State) ->
    not_ready(prepare, Query, State);
startup({_Ref, {execute, Query, _, _}}, _From, State) ->
    not_ready(execute, Query, State);
startup({_Ref, {batch, _, _}}, _From, State) ->
    not_ready(batch, State);
startup({_Ref, options}, _From, State) ->
    not_ready(options, State);
startup({_Ref, {register, _}}, _From, State) ->
    not_ready(register, State);
startup(Event, _From, State) ->
    ?ERROR("Bad event (startup/sync): ~p", [Event]),
    {stop, {bad_event, Event}, State}.

not_ready(Request, State) ->
    ?DEBUG("Connection not ready for '~s'", [Request]),
    {reply, {error, not_ready}, startup, State}.

not_ready(Request, Info, State) ->
    ?DEBUG("Connection not ready for '~s': ~p", [Request, Info]),
    {reply, {error, not_ready}, startup, State}.

ready(Event, State) ->
    ?ERROR("Bad event (ready): ~p", [Event]),
    {stop, {bad_event, Event}, State}.

ready({_Ref, _}, _From, #state{streams = []} = State) ->
    ?CRITICAL("Too many requests!"),
    ok = quintana:notify_histogram(?CONNECTION_STREAMS_EXHAUSTED_METRIC, 1),
    {reply, {error, too_many_requests}, ready, State};
ready({Ref, {'query', QueryString, Params}}, {From, _},
      #state{version = Version} = State) ->
    Query = erlcql_encode:'query'(Version, QueryString, Params),
    send(Query, {Ref, From}, State);
ready({Ref, {prepare, Query}}, {From, _},
      #state{version = Version} = State) ->
    Prepare = erlcql_encode:prepare(Version, Query),
    send(Prepare, {Ref, From}, State);
ready({Ref, {prepare, Query, Name}}, {From, _},
      #state{prepared_ets = PreparedETS,
             version = Version} = State) ->
    Prepare = erlcql_encode:prepare(Version, Query),
    Fun = fun({ok, {QueryId, RequestMetadata, _ResultMetadata}} = Response) ->
                  Types = proplists:get_value(types, RequestMetadata),
                  Entry = {Name, Query, QueryId, Types},
                  true = ets:insert(PreparedETS, Entry),
                  Response;
             ({error, Reason}) ->
                  {error, {Name, Query, Reason}}
          end,
    send(Prepare, {Ref, From, Fun}, State);
ready({Ref, {execute, QueryId, Values, Consistency}}, {From, _},
      #state{version = Version} = State) when is_binary(QueryId) ->
    Execute = erlcql_encode:execute(Version, QueryId, Values, Consistency),
    send(Execute, {Ref, From}, State);
ready({Ref, {execute, Name, Values, Consistency}}, {From, _},
      #state{prepared_ets = PreparedETS,
             version = Version} = State) when is_atom(Name) ->
    case ets:lookup(PreparedETS, Name) of
        [{Name, Query, QueryId, undefined}] ->
            ?DEBUG("Executing ~s: ~s, ~p", [Name, Query, Values]),
            Execute = erlcql_encode:execute(Version, QueryId,
                                            Values, Consistency),
            send(Execute, {Ref, From}, State);
        [{Name, Query, QueryId, Types}] ->
            TypedValues = lists:zip(Types, Values),
            ?DEBUG("Executing ~s: ~s, ~p", [Name, Query, Values]),
            Execute = erlcql_encode:execute(Version, QueryId,
                                            TypedValues, Consistency),
            send(Execute, {Ref, From}, State);
        [] ->
            ?DEBUG("Execute failed, invalid query name: ~s", [Name]),
            {reply, {error, invalid_query_name}, ready, State}
    end;
ready({Ref, {batch, Queries, Params}}, {From, _},
       #state{prepared_ets = PreparedETS,
              version = Version} = State) ->
    case expand_prepared(PreparedETS, Queries, []) of
        {error, Error} ->
            {reply, {error, Error}, ready, State};
        Queries2 ->
            Batch = erlcql_encode:batch(Version, Queries2, Params),
            send(Batch, {Ref, From}, State)
    end;
ready({Ref, options}, {From, _}, #state{version = Version} = State) ->
    Options = erlcql_encode:options(Version),
    send(Options, {Ref, From}, State);
ready({Ref, {register, Events}}, {From, _},
      #state{version = Version} = State) ->
    Register = erlcql_encode:register(Version, Events),
    send(Register, {Ref, From}, State);
ready(Event, _From, State) ->
    ?ERROR("Bad event (ready/sync): ~p", [Event]),
    {stop, {bad_event, Event}, State}.

handle_event(Event, StateName, State) ->
    ?ERROR("Bad event (~s/handle_event): ~p", [StateName, Event]),
    {stop, {bad_event, Event}, State}.

handle_sync_event(Event, _From, StateName, State) ->
    ?ERROR("Bad event(~s/handle_sync_event): ~p", [StateName, Event]),
    Reason = {bad_event, Event},
    {stop, Reason, {error, Reason}, State}.

handle_info({tcp, Socket, Data}, ready, #state{socket = Socket} = State) ->
    ok = inet:setopts(Socket, [{active, once}]),
    parse_response(Data, State);
handle_info({tcp_closed, Socket}, _StateName,
            #state{socket = Socket, auto_reconnect = false} = State) ->
    ?ERROR("TCP socket ~p closed", [Socket]),
    ok = quintana:notify_histogram(?CONNECTION_SOCKET_CLOSED, 1),
    {stop, tcp_closed, State};
handle_info({tcp_closed, Socket}, _StateName,
            #state{socket = Socket, auto_reconnect = true} = State) ->
    ?WARNING("TCP socket ~p closed", [Socket]),
    ok = quintana:notify_histogram(?CONNECTION_SOCKET_CLOSED, 1),
    try_again(State);
handle_info({tcp_error, Socket, Reason}, _StateName,
            #state{socket = Socket, auto_reconnect = false} = State) ->
    ?ERROR("TCP socket ~p error: ~p", [Socket, Reason]),
    ok = quintana:notify_histogram(?CONNECTION_SOCKET_ERROR, 1),
    {stop, {tcp_error, Reason}, State};
handle_info({tcp_error, Socket, Reason}, _StateName,
            #state{socket = Socket, auto_reconnect = true} = State) ->
    ?WARNING("TCP socket ~p error: ~p", [Socket, Reason]),
    ok = quintana:notify_histogram(?CONNECTION_SOCKET_ERROR, 1),
    try_again(State);
handle_info(Info, StateName, State) ->
    ?ERROR("Bad info (~s/handle_info): ~p", [StateName, Info]),
    {stop, {bad_info, Info}, State}.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

terminate(_Reason, _StateName, #state{socket = Socket}) ->
    close_socket(Socket).

-spec expand_prepared(ets(), [{atom(), values()}], [{binary(), values()}]) ->
          [{binary(), values()}] | {error, invalid_query_name}.
expand_prepared(_, [], Acc) ->
    lists:reverse(Acc);
expand_prepared(ETS, [{Name, Values} | Qs], Acc) when is_atom(Name) ->
    case ets:lookup(ETS, Name) of
        [{Name, _Query, QueryId, undefined}] ->
            expand_prepared(ETS, Qs, [{QueryId, Values} | Acc]);
        [{Name, _Query, QueryId, Types}] ->
            TypedValues = lists:zip(Types, Values),
            expand_prepared(ETS, Qs, [{QueryId, TypedValues} | Acc]);
        [] ->
            {error, invalid_query_name}
    end.

-spec maybe_create_prepared_ets(proplist()) -> ets().
maybe_create_prepared_ets(Opts) ->
    New = fun() -> ets:new(?PREPARED_ETS_NAME, ?PREPARED_ETS_OPTS) end,
    get_opt(prepared_statements_ets_tid, Opts, New).

-spec event_fun(event_fun() | pid()) -> event_fun().
event_fun(Fun) when is_function(Fun) ->
    Fun;
event_fun(Pid) when is_pid(Pid) ->
    fun(Event) -> Pid ! Event end.

-spec init_connection(state()) -> ok | {error, Reason :: term()}.
init_connection(State) ->
    Funs = [fun send_options/1,
            fun send_startup/1,
            fun register_to_events/1,
            fun use_keyspace/1,
            fun prepare_queries/1],
    apply_funs(Funs, State).

-spec apply_funs([function()], state()) ->
          {ok, state()} | {error, Reason :: term()}.
apply_funs([], State) -> {ok, State};
apply_funs([Fun | Rest], State) ->
    case Fun(State) of
        {ok, State2} ->
            apply_funs(Rest, State2);
        {error, {Reason, _, _}} ->
            {error, Reason};
        {error, _Reason} = Error ->
            Error
    end.

-spec send_options(state()) -> {ok, state()} | {error, Reason :: term()}.
send_options(#state{cql_version = undefined,
                    version = Version} = State) ->
    Options = erlcql_encode:options(Version),
    ok = send_request(Options, 0, State),
    case wait_for_response(State) of
        {ok, Supported} ->
            CQLVersion = hd(get_opt(<<"CQL_VERSION">>, Supported)),
            State2 = State#state{cql_version = CQLVersion},
            {ok, State2};
        {error, _Reason} = Error ->
            Error
    end;
send_options(State) ->
    {ok, State}.

-spec send_startup(state()) -> {ok, state()} | {error, Reason :: term()}.
send_startup(#state{flags = {Compression, Tracing},
                    cql_version = CQLVersion,
                    version = Version} = State) ->
    Startup = erlcql_encode:startup(Version, Compression, CQLVersion),
    ok = send_request(Startup, 0, State#state{flags = {false, Tracing}}),
    wait_for_ready(State).

-spec wait_for_ready(state()) -> {ok, state()} | {error, term()}.
wait_for_ready(State) ->
    case wait_for_response(State) of
        ready ->
            {ok, State};
        {authenticate, AuthClass} ->
            case try_auth(AuthClass, State) of
                ok ->
                    wait_for_ready(State);
                {error, _} = Error ->
                    Error
            end;
        {error, _} = Error ->
            Error
    end.

-spec try_auth(bitstring(), state()) -> ok | {error, term()}.
try_auth(<<"org.apache.cassandra.auth.PasswordAuthenticator">>,
         #state{credentials = {Username, Password},
                version = Version} = State) ->
    Map = [{<<"username">>, Username},
           {<<"password">>, Password}],
    Credentials = erlcql_encode:credentials(Version, Map),
    ok = send_request(Credentials, 0, State);
try_auth(Other, _State) ->
    {error, {unknown_auth_class, Other}}.

-spec register_to_events(state()) -> {ok, state()} | {error, Reason :: term()}.
register_to_events(#state{events = undefined} = State) ->
    {ok, State};
register_to_events(#state{events = []} = State) ->
    {ok, State};
register_to_events(#state{events = Events,
                          version = Version} = State) ->
    Register = erlcql_encode:register(Version, Events),
    ok = send_request(Register, 0, State),
    case wait_for_response(State) of
        ready ->
            {ok, State};
        {error, _Reason} = Error ->
            Error
    end.

-spec use_keyspace(state()) -> {ok, state()} | {error, Reason :: term()}.
use_keyspace(#state{keyspace = undefined} = State) ->
    {ok, State};
use_keyspace(#state{keyspace = Keyspace,
                    version = Version} = State) ->
    Use = erlcql_encode:'query'(Version, [<<"USE ">>, Keyspace],
                                [{consistency, any}]),
    ok = send_request(Use, 0, State),
    case wait_for_response(State) of
        {ok, Keyspace} ->
            {ok, State};
        {error, _Reason} = Error ->
            Error
    end.

-spec prepare_queries(state()) -> {ok, state()} | {error, Reason :: term()}.
prepare_queries(#state{prepare = undefined} = State) ->
    {ok, State};
prepare_queries(#state{prepare = Queries} = State) ->
    prepare_queries(Queries, State).

-spec prepare_queries([{atom(), iodata()}], state()) ->
          {ok, state()} | {error, Reason :: term()}.
prepare_queries([], State) ->
    {ok, State};
prepare_queries([{Name, Query} | Rest],
                #state{prepared_ets = PreparedETS,
                       version = Version} = State) ->
    Prepare = erlcql_encode:prepare(Version, Query),
    ok = send_request(Prepare, 0, State),
    case wait_for_response(State) of
        {ok, {QueryId, RequestMetadata, _}} ->
            Types = proplists:get_value(types, RequestMetadata),
            true = ets:insert(PreparedETS, {Name, Query, QueryId, Types}),
            prepare_queries(Rest, State);
        {error, _Reason} = Error ->
            Error
    end.

-spec send_request(request(), integer(), state()) -> ok.
send_request(Body, Stream, #state{socket = Socket,
                                  flags = Flags,
                                  version = Version}) ->
    Frame = erlcql_encode:frame(Version, Body, Flags, Stream),
    ok = gen_tcp:send(Socket, Frame).

-spec wait_for_response(state()) ->
          ready() | authenticate() | response() | {error, term()}.
wait_for_response(#state{socket = Socket} = State) ->
    case gen_tcp:recv(Socket, 8, 5000) of
        {ok, Header} ->
            wait_for_body(Header, State);
        {error, _Reason} = Error ->
            Error
    end.

-spec wait_for_body(binary(), state()) ->
          ready() | authenticate() | response() | {error, term()}.
wait_for_body(<<_:32, 0:32>> = Header, State) ->
    decode_response(Header, State);
wait_for_body(<<_:32, Length:32>> = Header,
              #state{socket = Socket} = State) ->
    case gen_tcp:recv(Socket, Length, 5000) of
        {ok, Body} ->
            decode_response(<<Header/binary, Body/binary>>, State);
        {error, _Reason} = Error ->
            Error
    end.

-spec decode_response(binary(), state()) ->
          ready() | authenticate() | response() | {error, term()}.
decode_response(Binary, #state{version = Version,
                               flags = {Compression, _}}) ->
    case erlcql_decode:decode(Version, Binary, Compression) of
        {ok, 0, Response, <<>>} ->
            Response;
        {error, _} = Error ->
            Error
    end.

-spec try_again(state()) -> {next_state, startup, state()}.
try_again(#state{socket = Socket,
                 backoff = Backoff} = State) ->
    ok = close_socket(Socket),
    {Timeout, Backoff2} = backoff_fail(Backoff),
    ?DEBUG("Backing off for ~pms", [Timeout]),
    State2 = State#state{socket = undefined,
                         backoff = Backoff2},
    _Ref = gen_fsm:send_event_after(Timeout, reconnect),
    {next_state, startup, State2}.

-spec close_socket(undefined | socket()) -> ok.
close_socket(undefined) -> ok;
close_socket(Socket) ->
    ok = gen_tcp:close(Socket).

-spec async_call(pid(), tuple() | atom()) ->
          response() | {error, Reason :: term()}.
async_call(Pid, Request) ->
    case cast(Pid, Request) of
        {ok, QueryRef} ->
            await(QueryRef);
        {error, _Reason} = Error ->
            Error
    end.

-spec cast(pid(), tuple() | atom()) ->
          {ok, QueryRef :: erlcql:query_ref()} | {error, Reason :: term()}.
cast(Pid, Request) ->
    Ref = make_ref(),
    case gen_fsm:sync_send_event(Pid, {Ref, Request}) of
        {ok, Stream} ->
            {ok, {Ref, Pid, Stream}};
        {error, _Reason} = Error ->
            Error
    end.

-spec do_await(erlcql:query_ref(), integer()) ->
          response() | {error, Reason :: term()}.
do_await({Ref, _Pid, _Stream}, Timeout) ->
    receive
        {Ref, Response} ->
            Response
    after Timeout ->
            ok = quintana:notify_histogram(?CONNECTION_QUERY_TIMEOUT, 1),
            {error, timeout}
    end.

-spec send(request(), Info, state()) ->
          {reply, {ok, Stream :: integer()}, ready, NewState :: state()} when
      Info :: {reference(), pid()} | {reference(), pid(), fun()}.
send(Body, Info, #state{streams = [Stream | Streams],
                        async_ets = AsyncETS} = State) ->
    ok = send_request(Body, Stream, State),
    true = ets:insert(AsyncETS, {Stream, Info}),
    {reply, {ok, Stream}, ready, State#state{streams = Streams}}.

parse_response(Data, #state{parser = Parser,
                            flags = {Compression, _}} = State) ->
    case erlcql_decode:parse(Data, Parser, Compression) of
        {ok, Responses, Parser2} ->
            State2 = handle_responses(Responses, State),
            {next_state, ready, State2#state{parser = Parser2}, hibernate};
        {error, Reason} ->
            ?ERROR("Parsing response failed: ~p", [Reason]),
            ok = quintana:notify_histogram(?CONNECTION_PARSE_ERROR, 1),
            {stop, Reason, State}
    end.

handle_responses([], State) -> State;
handle_responses(Responses, State) ->
    lists:foldl(fun handle_response/2, State, Responses).

handle_response({-1, {event, Event}},
                #state{event_fun = EventFun} = State) ->
    ?DEBUG("Received an event: ~p", [Event]),
    EventFun(Event),
    State;
handle_response({Stream, Response}, #state{async_ets = AsyncETS} = State) ->
    case ets:lookup(AsyncETS, Stream) of
        [{Stream, {Ref, Pid}}] ->
            send_response(Stream, {Ref, Response}, Pid, State);
        [{Stream, {Ref, Pid, Fun}}] ->
            Response2 = Fun(Response),
            send_response(Stream, {Ref, Response2}, Pid, State);
        [] ->
            ?WARNING("Unexpected response (~p): ~p", [Stream, Response]),
            ok = quintana:notify_histogram(?CONNECTION_UNEXPECTED_RESPONSE, 1),
            State
    end.

-spec send_response(integer(), {erlcql:query_ref(), erlcql:response()},
                    pid(), state()) -> state().
send_response(Stream, Response, Pid, State=#state{async_ets = AsyncETS, streams = Streams}) ->
    Pid ! Response,
    true = ets:delete(AsyncETS, Stream),
    State#state{streams = [Stream | Streams]}.

-spec get_env_opt(term(), proplist()) -> Value :: term().
get_env_opt(Opt, Opts) ->
    case lists:keyfind(Opt, 1, Opts) of
        {Opt, Value} ->
            Value;
        false ->
            get_env(Opt)
    end.

get_internal_opt(Opt, Opts) ->
    {Opt, Value} = lists:keyfind(Opt, 1, Opts),
    Value.

-spec get_opt(term(), proplist()) -> Value :: term().
get_opt(Opt, Opts) ->
    get_opt(Opt, Opts, undefined).

-spec get_opt(term(), proplist(), term() | function()) -> Value :: term().
get_opt(Opt, Opts, Default) when is_function(Default) ->
    case lists:keyfind(Opt, 1, Opts) of
        {Opt, Value} ->
            Value;
        false ->
            Default()
    end;
get_opt(Opt, Opts, Default) ->
    case lists:keyfind(Opt, 1, Opts) of
        {Opt, Value} ->
            Value;
        false ->
            Default
    end.

-spec get_env(term()) -> Value :: term().
get_env(Opt) ->
    case application:get_env(?APP, Opt) of
        {ok, Val} ->
            Val;
        undefined ->
            erlcql:default(Opt)
    end.

-spec backoff_init(Start, Max) -> backoff() when
    Start :: pos_integer(),
    Max :: pos_integer() | infinity.
backoff_init(Start, Max) ->
    #backoff{start = Start,
             max = Max,
             current = Start}.

-spec backoff_fail(backoff()) -> {pos_integer(), backoff()}.
backoff_fail(#backoff{current = Current, max = infinity} = Backoff) ->
    {Current, Backoff#backoff{current = Current bsl 1}};
backoff_fail(#backoff{current = Current, max = Max} = Backoff) ->
    {Current, Backoff#backoff{current = min(Current bsl 1, Max)}}.

-spec backoff_succeed(backoff()) -> backoff().
backoff_succeed(#backoff{start = Start} = Backoff) ->
    Backoff#backoff{current = Start}.
