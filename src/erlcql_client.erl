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

%% @doc Native protocol CQL client module.
%% @author Krzysztof Rutka <krzysztof.rutka@gmail.com>
-module(erlcql_client).
-behaviour(gen_fsm).

%% API
-export([start_link/1]).
-export(['query'/3, async_query/3,
         execute/4, async_execute/4]).
-export([prepare/2, prepare/3,
         options/1,
         register/2]).
-export([await/1,
         await/2]).

%% gen_fsm callbacks
-export([init/1,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         code_change/4,
         terminate/3]).
-export([startup/2,
         startup/3,
         ready/2,
         ready/3]).

-include("erlcql.hrl").

-record(state, {
          async_ets :: ets(),
          credentials :: {bitstring(), bitstring()},
          event_fun :: event_fun(),
          flags :: {atom(), boolean()},
          parent :: pid(),
          parser :: parser(),
          prepared_ets :: ets(),
          socket :: socket(),
          streams = lists:seq(1, 127) :: [integer()]
         }).
-type state() :: #state{}.

-define(TCP_OPTS, [binary, {active, once}]).
-define(ASYNC_ETS_NAME, erlcql_async).
-define(ASYNC_ETS_OPTS, [set, private,
                         {write_concurrency, true},
                         {read_concurrency, true}]).
-define(PREPARED_ETS_NAME, erlcql_prepared).
-define(PREPARED_ETS_OPTS, [set, private,
                            {read_concurrency, true}]).
-define(TIMEOUT, timer:seconds(5)).

%%-----------------------------------------------------------------------------
%% API
%%-----------------------------------------------------------------------------

start_link(Opts) ->
    Opts2 = [{parent, self()} | Opts],
    EventFun = event_fun(get_env_opt(event_handler, Opts)),
    Opts3 = [{event_fun, EventFun} | Opts2],
    case gen_fsm:start_link(?MODULE, proplists:unfold(Opts3), []) of
        {ok, Pid} ->
            case wait_until_ready(Pid, Opts) of
                ready ->
                    {ok, Pid};
                {error, _Reason} = Error ->
                    Error
            end;
        {error, _Reason} = Error ->
            Error
    end.

-spec 'query'(pid(), iodata(), consistency()) ->
          result() | {error, Reason :: term()}.
'query'(Pid, QueryString, Consistency) ->
    async_call(Pid, {'query', QueryString, Consistency}).

-spec async_query(pid(), iodata(), consistency()) ->
          {ok, QueryRef :: erlcql:query_ref()} | {error, Reason :: term()}.
async_query(Pid, QueryString, Consistency) ->
    cast(Pid, {'query', QueryString, Consistency}).

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

-spec async_execute(pid(), binary(), values(), consistency()) ->
          {ok, QueryRef :: erlcql:query_ref()} | {error, Reason :: term()}.
async_execute(Pid, QueryId, Values, Consistency) ->
    cast(Pid, {execute, QueryId, Values, Consistency}).

-spec options(pid()) -> supported() | {error, Reason :: term()}.
options(Pid) ->
    async_call(Pid, options).

-spec register(pid(), [event_type()]) -> ready | {error, Reason :: term()}.
register(Pid, Events) ->
    async_call(Pid, {register, Events}).

-spec await(erlcql:query_ref()) -> response() | {error, Reason :: term()}.
await({ok, QueryRef}) ->
    do_await(QueryRef, ?TIMEOUT);
await({error, _Reason} = Error) ->
    Error;
await(QueryRef) ->
    do_await(QueryRef, ?TIMEOUT).

-spec await(erlcql:query_ref(), integer()) ->
          response() | {error, Reason :: term()}.
await({ok, QueryRef}, Timeout) ->
    do_await(QueryRef, Timeout);
await({error, _Reason} = Error, _Timeout) ->
    Error;
await(QueryRef, Timeout) ->
    do_await(QueryRef, Timeout).

%%-----------------------------------------------------------------------------
%% gen_fsm callbacks
%%-----------------------------------------------------------------------------

init(Opts) ->
    Host = get_env_opt(host, Opts),
    Port = get_env_opt(port, Opts),
    case gen_tcp:connect(Host, Port, ?TCP_OPTS) of
        {ok, Socket} ->
            State = init_state(Opts, Socket),
            ok = send_startup(State, Opts),
            {ok, startup, State};
        {error, Reason} ->
            ?ERROR("Cannot connect to Cassandra: ~s", [Reason]),
            {stop, Reason}
    end.

-spec init_state(Opts, Socket) -> State when
      Opts :: proplist(),
      Socket :: socket(),
      State :: state().
init_state(Opts, Socket) ->
    AsyncETS = ets:new(?ASYNC_ETS_NAME, ?ASYNC_ETS_OPTS),
    Username = get_env_opt(username, Opts),
    Password = get_env_opt(password, Opts),
    Credentials = {Username, Password},
    EventFun = get_opt(event_fun, Opts),
    Compression = get_env_opt(compression, Opts),
    Tracing = get_env_opt(tracing, Opts),
    Flags = {Compression, Tracing},
    Parent = get_opt(parent, Opts),
    Parser = erlcql_decode:new_parser(),
    PreparedETS = maybe_create_prepared_ets(Opts),
    #state{async_ets = AsyncETS,
           credentials = Credentials,
           event_fun = EventFun,
           flags = Flags,
           parent = Parent,
           parser = Parser,
           prepared_ets = PreparedETS,
           socket = Socket}.

-spec send_startup(State, Opts) -> ok when
      State :: state(),
      Opts :: proplist().
send_startup(#state{flags = {Compression, Tracing},
                    socket = Socket}, Opts) ->
    CQLVersion = get_env_opt(cql_version, Opts),
    Startup = erlcql_encode:startup(Compression, CQLVersion),
    Frame = erlcql_encode:frame(Startup, {false, Tracing}, 0),
    gen_tcp:send(Socket, Frame).

handle_event({timeout, Stream}, StateName,
             #state{async_ets = AsyncETS,
                    streams = Streams} = State) ->
    true = ets:delete(AsyncETS, Stream),
    {next_state, StateName, State#state{streams = [Stream | Streams]}};
handle_event(ready, ready, #state{parent = Parent} = State) ->
    Parent ! ready,
    {next_state, ready, State};
handle_event(Event, _StateName, State) ->
    {stop, {bad_event, Event}, State}.

handle_sync_event(Event, _From, _StateName, State) ->
    Reason = {bad_event, Event},
    {stop, Reason, {error, Reason}, State}.

startup(Event, State) ->
    {stop, {bad_event, Event}, State}.

startup({_Ref, {'query', _, _}}, _From, State) ->
    {reply, {error, not_ready}, startup, State};
startup({_Ref, {prepare, _}}, _From, State) ->
    {reply, {error, not_ready}, startup, State};
startup({_Ref, {prepare, _, _}}, _From, State) ->
    {reply, {error, not_ready}, startup, State};
startup({_Ref, {execute, _, _, _}}, _From, State) ->
    {reply, {error, not_ready}, startup, State};
startup({_Ref, options}, _From, State) ->
    {reply, {error, not_ready}, startup, State};
startup({_Ref, {register, _}}, _From, State) ->
    {reply, {error, not_ready}, startup, State};
startup(Event, _From, State) ->
    {stop, {bad_event, Event}, State}.

ready(Event, State) ->
    {stop, {bad_event, Event}, State}.

ready({_Ref, _}, _From, #state{streams = []} = State) ->
    ?CRITICAL("Too many requests to Cassandra!"),
    {reply, {error, too_many_requests}, ready, State};
ready({Ref, {'query', QueryString, Consistency}}, {From, _}, State) ->
    Query = erlcql_encode:'query'(QueryString, Consistency),
    send(Query, {Ref, From}, State);
ready({Ref, {prepare, QueryString}}, {From, _}, State) ->
    Prepare = erlcql_encode:prepare(QueryString),
    send(Prepare, {Ref, From}, State);
ready({Ref, {prepare, QueryString, Name}}, {From, _},
      #state{prepared_ets = PreparedETS} = State) ->
    Prepare = erlcql_encode:prepare(QueryString),
    Fun = fun({ok, QueryId} = Response) ->
                  true = ets:insert(PreparedETS, {Name, QueryId}),
                  Response;
             ({error, _} = Response) ->
                  Response
          end,
    send(Prepare, {Ref, From, Fun}, State);
ready({Ref, {execute, QueryId, Values, Consistency}},
      {From, _}, State) when is_binary(QueryId) ->
    Execute = erlcql_encode:execute(QueryId, Values, Consistency),
    send(Execute, {Ref, From}, State);
ready({Ref, {execute, QueryName, Values, Consistency}}, {From, _},
      #state{prepared_ets = PreparedETS} = State) when is_atom(QueryName) ->
    case ets:lookup(PreparedETS, QueryName) of
        [{QueryName, QueryId}] ->
            Execute = erlcql_encode:execute(QueryId, Values, Consistency),
            send(Execute, {Ref, From}, State);
        [] ->
            {reply, {error, invalid_query_name}, ready, State}
    end;
ready({Ref, options}, {From, _}, State) ->
    Options = erlcql_encode:options(),
    send(Options, {Ref, From}, State);
ready({Ref, {register, Events}}, {From, _}, State) ->
    Register = erlcql_encode:register(Events),
    send(Register, {Ref, From}, State);
ready(Event, _From, State) ->
    {stop, {bad_event, Event}, State}.

handle_info({tcp, Socket, Data}, ready, #state{socket = Socket} = State) ->
    ok = inet:setopts(Socket, [{active, once}]),
    parse_response(Data, State);
handle_info({tcp, Socket, Data}, startup, #state{socket = Socket} = State) ->
    ok = inet:setopts(Socket, [{active, once}]),
    parse_ready(Data, State);
handle_info({tcp_closed, Socket}, _StateName,
            #state{socket = Socket} = State) ->
    {stop, tcp_closed, State};
handle_info({tcp_error, Socket, Reason}, _StateName,
            #state{socket = Socket} = State) ->
    {stop, {tcp_error, Reason}, State};
handle_info(Info, _StateName, State) ->
    {stop, {bad_info, Info}, State}.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

terminate(_Reason, _StateName, #state{socket = Socket}) when is_port(Socket) ->
    gen_tcp:close(Socket);
terminate(_Reason, _StateName, _State) ->
    ok.

%%-----------------------------------------------------------------------------
%% Internal functions
%%-----------------------------------------------------------------------------

-spec maybe_create_prepared_ets(proplist()) -> ets().
maybe_create_prepared_ets(Opts) ->
   case get_opt(prepared_statements_ets_tid, Opts) of
       undefined ->
           ets:new(?PREPARED_ETS_NAME, ?PREPARED_ETS_OPTS);
       Tid ->
           Tid
   end.

-spec event_fun(event_fun() | pid()) -> event_fun().
event_fun(Fun) when is_function(Fun) ->
    Fun;
event_fun(Pid) when is_pid(Pid) ->
    fun(Event) -> Pid ! Event end.

-spec wait_until_ready(pid(), proplist()) -> ready | {error, atom()}.
wait_until_ready(Pid, Opts) ->
    receive
        ready ->
            init_env(Pid, Opts)
    after
        ?TIMEOUT ->
            {error, timeout}
    end.

-spec init_env(Pid, Opts) -> ok | {error, Reason} when
      Pid :: pid(),
      Opts :: proplist(),
      Reason :: atom().
init_env(Pid, Opts) ->
    UseFun = fun(Keyspace) -> 'query'(Pid, [<<"USE ">>, Keyspace], any) end,
    PrepareFun = fun(Queries) -> apply_prepare(Pid, Queries) end,
    RegisterFun = fun(Events) -> ?MODULE:register(Pid, Events) end,
    apply_funs([{use, UseFun, bad_keyspace},
                {prepare, PrepareFun, prepare_failed},
                {register, RegisterFun, register_failed}], Opts).

-spec apply_prepare(Pid, Queries) -> {ok, void} | {error, Reason} when
      Pid :: pid(),
      Queries :: [Query],
      Query :: {Name :: atom(), QueryString :: bitstring()},
      Reason :: atom().
apply_prepare(_Pid, []) ->
    {ok, void};
apply_prepare(Pid, [{Name, Query} | Queries]) ->
    case prepare(Pid, Query, Name) of
        {ok, _} ->
            apply_prepare(Pid, Queries);
        {error, _Reason} ->
            {error, prepare_failed}
    end.

-spec apply_funs(Funs, Opts) -> ok | {error, Reason} when
      Funs :: [{Opt :: atom(), Fun, Reason}],
      Fun :: fun(() -> Response),
      Response :: erlcql:response(),
      Opts :: proplist(),
      Reason :: atom().
apply_funs([], _Opts) ->
    ready;
apply_funs([{Key, Fun, Error} | Rest], Opts) ->
    Value = get_opt(Key, Opts),
    case Value of
        undefined ->
            apply_funs(Rest, Opts);
        Value ->
            case Fun(Value) of
                {ok, _} ->
                    apply_funs(Rest, Opts);
                ready ->
                    apply_funs(Rest, Opts);
                {error, _Reason} ->
                    {error, Error}
            end
    end.

-spec async_call(pid(), tuple() | atom()) -> response() |
                                             {error, Reason :: term()}.
async_call(Pid, Request) ->
    case cast(Pid, Request) of
        {ok, QueryRef} ->
            await(QueryRef);
        {error, _Reason} = Error ->
            Error
    end.

-spec cast(pid(), tuple() | atom()) -> {ok, QueryRef :: erlcql:query_ref()} |
                                       {error, Reason :: term()}.
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
do_await({Ref, Pid, Stream}, Timeout) ->
    receive
        {Ref, Response} ->
            Response
    after Timeout ->
            gen_fsm:send_all_state_event(Pid, {timeout, Stream}),
            {error, timeout}
    end.

-spec send(request(), Info, state()) ->
          {reply, {ok, Stream :: integer()}, ready, NewState :: state()} when
      Info :: {reference(), pid()} | {reference(), pid(), fun()}.
send(Message, Info, #state{socket = Socket,
                           flags = Flags,
                           streams = [Stream | Streams],
                           async_ets = AsyncETS} = State) ->
    Frame = erlcql_encode:frame(Message, Flags, Stream),
    ok = gen_tcp:send(Socket, Frame),
    true = ets:insert(AsyncETS, {Stream, Info}),
    {reply, {ok, Stream}, ready, State#state{streams = Streams}}.

parse_ready(Data, #state{parser = Parser,
                         flags = {Compression, _},
                         streams = Streams} = State) ->
    case erlcql_decode:parse(Data, Parser, Compression) of
        {ok, [], NewParser} ->
            {next_state, startup, State#state{parser = NewParser}};
        {ok, [{0, ready}], NewParser} ->
            send_ready(),
            {next_state, ready, State#state{parser = NewParser,
                                            streams = [0 | Streams]}};
        {ok, [{0, {authenticate, AuthClass}}], NewParser} ->
            try_auth(AuthClass, State#state{parser = NewParser});
        {ok, Other, _NewParser} ->
            ?ERROR("Received this instead of ready/authenticate: ~p", [Other]),
            {stop, {bad_response, Other}, State};
        {error, Reason} ->
            {stop, Reason, State}
    end.

-spec send_ready() -> any().
send_ready() ->
    gen_fsm:send_all_state_event(self(), ready).

try_auth(<<"org.apache.cassandra.auth.PasswordAuthenticator">>,
         #state{socket = Socket,
                credentials = {Username, Password},
                flags = Flags} = State) ->
    Map = [{<<"username">>, Username},
           {<<"password">>, Password}],
    Credentials = erlcql_encode:credentials(Map),
    Frame = erlcql_encode:frame(Credentials, Flags, 0),
    ok = gen_tcp:send(Socket, Frame),

    {next_state, startup, State};
try_auth(Other, State) ->
    {stop, {unknown_auth_class, Other}, State}.

parse_response(Data, #state{parser = Parser,
                            flags = {Compression, _}} = State) ->
    case erlcql_decode:parse(Data, Parser, Compression) of
        {ok, Responses, NewParser} ->
            NewState = handle_responses(Responses, State),
            {next_state, ready, NewState#state{parser = NewParser}};
        {error, Reason} ->
            {stop, Reason, State}
    end.

handle_responses(Responses, State) ->
    lists:foldl(fun handle_response/2, State, Responses).

handle_response({-1, {event, Event}},
                #state{event_fun = EventFun} = State) ->
    ?INFO("Received an event from Cassandra: ~p", [Event]),
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
            State
    end.

-spec send_response(integer(), {erlcql:query_ref(), erlcql:response()},
                    pid(), state()) -> state().
send_response(Stream, Response, Pid, #state{async_ets = AsyncETS,
                                            streams = Streams} = State) ->
    true = ets:delete(AsyncETS, Stream),
    Pid ! Response,
    State#state{streams = [Stream | Streams]}.

%%-----------------------------------------------------------------------------
%% Helper functions
%%-----------------------------------------------------------------------------

-spec get_env_opt(atom(), proplist()) -> Value :: term().
get_env_opt(Opt, Opts) ->
    case lists:keyfind(Opt, 1, Opts) of
        {Opt, Value} ->
            Value;
        false ->
            get_env(Opt)
    end.

-spec get_opt(atom(), proplist()) -> Value :: term().
get_opt(Opt, Opts) ->
    get_opt(Opt, Opts, undefined).

-spec get_opt(atom(), proplist(), term()) -> Value :: term().
get_opt(Opt, Opts, Default) ->
    case lists:keyfind(Opt, 1, Opts) of
        {Opt, Value} ->
            Value;
        false ->
            Default
    end.

-spec get_env(atom()) -> Value :: term().
get_env(Opt) ->
    case application:get_env(?APP, Opt) of
        {ok, Val} ->
            Val;
        undefined ->
            erlcql:default(Opt)
    end.
