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

%% @doc Native protocol CQL client module.
%% @author Krzysztof Rutka <krzysztof.rutka@gmail.com>
-module(erlcql_client).
-behaviour(gen_fsm).

%% API
-export([start_link/1]).
-export(['query'/3,
         execute/4]).
-export([prepare/2,
         options/1,
         register/2]).

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
          socket :: port(),
          async_ets :: integer(),
          credentials :: {bitstring(), bitstring()},
          flags :: {atom(), boolean()},
          streams = lists:seq(1, 127) :: [integer()],
          parser :: parser()
         }).

-define(TCP_OPTS, [binary, {active, once}]).
-define(ETS_NAME, erlcql_async).
-define(ETS_OPTS, [set, private,
                   {write_concurrency, true},
                   {read_concurrency, true}]).
-define(TIMEOUT, timer:seconds(5)).

%%-----------------------------------------------------------------------------
%% API
%%-----------------------------------------------------------------------------

-spec start_link(proplists:proplist()) ->
          {ok, pid()} | ignore | {error, Reason :: term()}.
start_link(Opts) ->
    gen_fsm:start_link(?MODULE, proplists:unfold(Opts), []).

-spec 'query'(pid(), bitstring(), consistency()) ->
          result() | {error, Reason :: term()}.
'query'(Pid, QueryString, Consistency) ->
    async_call(Pid, {'query', QueryString, Consistency}).

-spec prepare(pid(), bitstring()) -> prepared() | {error, Reason :: term()}.
prepare(Pid, QueryString) ->
    async_call(Pid, {prepare, QueryString}).

-spec execute(pid(), binary(), [binary()], consistency()) ->
          result() | {error, Reason :: term()}.
execute(Pid, QueryId, Values, Consistency) ->
    async_call(Pid, {execute, QueryId, Values, Consistency}).

-spec options(pid()) -> supported() | {error, Reason :: term()}.
options(Pid) ->
    async_call(Pid, options).

-spec register(pid(), [event_type()]) -> ready | {error, Reason :: term()}.
register(Pid, Events) ->
    async_call(Pid, {register, Events}).

%%-----------------------------------------------------------------------------
%% gen_fsm callbacks
%%-----------------------------------------------------------------------------

init(Opts) ->
    Host = get_opt(host, Opts),
    Port = get_opt(port, Opts),
    case gen_tcp:connect(Host, Port, ?TCP_OPTS) of
        {ok, Socket} ->
            AsyncETS = ets:new(?ETS_NAME, ?ETS_OPTS),
            Compression = get_opt(compression, Opts),
            Tracing = get_opt(tracing, Opts),
            Flags = {Compression, Tracing},
            CQLVersion = get_opt(cql_version, Opts),
            Username = get_opt(username, Opts),
            Password = get_opt(password, Opts),
            Credentials = {Username, Password},
            Parser = erlcql_decode:new_parser(),

            Startup = erlcql_encode:startup(Compression, CQLVersion),
            Frame = erlcql_encode:frame(Startup, {false, Tracing}, 0),
            ok = gen_tcp:send(Socket, Frame),

            {ok, startup, #state{socket = Socket,
                                 flags = Flags,
                                 credentials = Credentials,
                                 async_ets = AsyncETS,
                                 parser = Parser}};
        {error, Reason} ->
            ?ERROR("Cannot connect to Cassandra: ~s", [Reason]),
            {stop, Reason}
    end.

handle_event({timeout, Stream}, StateName,
             #state{async_ets = AsyncETS,
                    streams = Streams} = State) ->
    true = ets:delete(AsyncETS, Stream),
    {next_state, StateName, State#state{streams = [Stream | Streams]}};
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
    send(Query, Ref, From, State);
ready({Ref, {prepare, QueryString}}, {From, _}, State) ->
    Prepare = erlcql_encode:prepare(QueryString),
    send(Prepare, Ref, From, State);
ready({Ref, {execute, QueryId, Values, Consistency}}, {From, _}, State) ->
    Execute = erlcql_encode:execute(QueryId, Values, Consistency),
    send(Execute, Ref, From, State);
ready({Ref, options}, {From, _}, State) ->
    Options = erlcql_encode:options(),
    send(Options, Ref, From, State);
ready({Ref, {register, Events}}, {From, _}, State) ->
    Register = erlcql_encode:register(Events),
    send(Register, Ref, From, State);
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

-spec async_call(pid(), tuple() | atom()) -> response() |
                                             {error, Reason :: term()}.
async_call(Pid, Request) ->
    Ref = make_ref(),
    case gen_fsm:sync_send_event(Pid, {Ref, Request}) of
        {ok, Stream} ->
            receive
                {Ref, Response} ->
                    Response
            after ?TIMEOUT ->
                    gen_fsm:send_all_state_event(Pid, {timeout, Stream}),
                    {error, timeout}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

-spec send(request(), reference(), pid(), #state{}) ->
          {reply, {ok, Stream :: integer()}, ready, NewState :: #state{}}.
send(Message, Ref, From, #state{socket = Socket,
                                flags = Flags,
                                streams = [Stream | Streams],
                                async_ets = AsyncETS} = State) ->
    Frame = erlcql_encode:frame(Message, Flags, Stream),
    ok = gen_tcp:send(Socket, Frame),
    true = ets:insert(AsyncETS, {Stream, {Ref, From}}),
    {reply, {ok, Stream}, ready, State#state{streams = Streams}}.

parse_ready(Data, #state{parser = Parser,
                         flags = {Compression, _},
                         streams = Streams} = State) ->
    case erlcql_decode:parse(Data, Parser, Compression) of
        {ok, [], NewParser} ->
            {next_state, startup, State#state{parser = NewParser}};
        {ok, [{0, ready}], NewParser} ->
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

handle_response({-1, {event, _} = Event}, State) ->
    ?INFO("Received an event from Cassandra: ~p", [Event]),
    State;
handle_response({Stream, Response}, #state{async_ets = AsyncETS,
                                           streams = Streams} = State) ->
    case ets:lookup(AsyncETS, Stream) of
        [{Stream, {Ref, Pid}}] ->
            true = ets:delete(AsyncETS, Stream),
            Pid ! {Ref, Response},
            State#state{streams = [Stream | Streams]};
        [] ->
            ?WARNING("Unexpected response (~p): ~p", [Stream, Response]),
            State;
        _Else ->
            ?CRITICAL("This should NOT happen: more then one async stream!")
    end.

%%-----------------------------------------------------------------------------
%% Helper functions
%%-----------------------------------------------------------------------------

-spec get_opt(atom(), proplists:proplist()) -> Value :: term().
get_opt(Opt, Opts) ->
    case lists:keyfind(Opt, 1, Opts) of
        {Opt, Value} ->
            Value;
        false ->
            application:get_env(?APP, Opt, erlcql:default(Opt))
    end.
