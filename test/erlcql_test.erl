-module(erlcql_test).

-export([create_keyspace/0,
         keyspace_exists/1,
         drop_keyspace/1]).
-export([gen_table_name/0]).
-export([start_client/1,
         single_query/1,
         'query'/2,
         execute/3,
         stop_client/1]).

-define(CQL_VERSION, <<"3.0.0">>).
-define(OPTS, [{cql_version, ?CQL_VERSION}]).
-define(CONSISTENCY, quorum).

-spec create_keyspace() -> Keyspace :: bitstring().
create_keyspace() ->
    _ = random:seed(now()),
    N = integer_to_list(random:uniform(1000000000)),
    Keyspace = [<<"erlcql_tests_">>, N],
    Query = [<<"CREATE KEYSPACE IF NOT EXISTS ">>, Keyspace,
             <<" WITH replication = {",
               "'class': 'SimpleStrategy', ",
               "'replication_factor': 1}">>],
    single_query(Query),
    iolist_to_binary(Keyspace).

-spec keyspace_exists(bitstring()) -> boolean().
keyspace_exists(Keyspace) ->
    Query = <<"SELECT keyspace_name FROM system.schema_keyspaces">>,
    {Keyspaces, _} = single_query(Query),
    lists:member([Keyspace], Keyspaces).

-spec drop_keyspace(bitstring()) -> ok.
drop_keyspace(Keyspace) ->
    Query = [<<"DROP KEYSPACE IF EXISTS ">>, Keyspace],
    single_query(Query),
    ok.

-spec gen_table_name() -> Table :: bitstring().
gen_table_name() ->
    _ = random:seed(now()),
    N = integer_to_list(random:uniform(1000000000)),
    iolist_to_binary([<<"table_">>, N]).

-spec start_client(bitstring() | proplists:proplist()) -> Pid :: pid().
start_client(Opts) when is_list(Opts) ->
    {ok, Pid} = erlcql_client:start_link(Opts ++ ?OPTS),
    Pid;
start_client(Keyspace) ->
    Opts = [{use, Keyspace} | ?OPTS],
    {ok, Pid} = erlcql_client:start_link(Opts),
    Pid.

-spec single_query(iodata()) -> {ok, any()} | {error, any()}.
single_query(Query) ->
    {ok, Pid} = erlcql_client:start_link(?OPTS),
    {ok, Response} = erlcql_client:'query'(Pid, Query, ?CONSISTENCY),
    stop_client(Pid),
    Response.

-spec 'query'(pid(), iodata()) -> any().
'query'(Pid, Query) ->
    {ok, Response} = erlcql_client:'query'(Pid, Query, ?CONSISTENCY),
    Response.

-spec execute(pid(), atom(), [any()]) -> any().
execute(Pid, Name, Values) ->
    {ok, Response} = erlcql_client:execute(Pid, Name, Values, ?CONSISTENCY),
    Response.

-spec stop_client(pid()) -> ok.
stop_client(Pid) ->
    true = unlink(Pid),
    true = exit(Pid, kill),
    ok.
