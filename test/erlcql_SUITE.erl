-module(erlcql_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").

-define(HOST, "localhost").
-define(OPTS, []).
-define(KEYSPACE, <<"erlcql_tests">>).

-define(CREATE_KEYSPACE,
        <<"CREATE KEYSPACE ", ?KEYSPACE/binary, " WITH replication = ",
          "{'class': 'SimpleStrategy', 'replication_factor': 1}">>).
-define(DROP_KEYSPACE, <<"DROP KEYSPACE ", ?KEYSPACE/binary>>).
-define(USE_KEYSPACE, <<"USE ", ?KEYSPACE/binary>>).
-define(CREATE_TABLE, <<"CREATE TABLE t (k int PRIMARY KEY, v text)">>).
-define(DROP_TABLE, <<"DROP TABLE t">>).

%% Fixtures -------------------------------------------------------------------

suite() ->
    [{ct_hooks, [cth_surefire]}, {timetrap, {seconds, 120}}].

init_per_suite(Config) ->
    {ok, Pid} = erlcql_client:start_link(?HOST, ?OPTS),
    unlink(Pid),
    q(Pid, ?DROP_KEYSPACE),
    [{pid, Pid} | Config].

end_per_suite(Config) ->
    Pid = get_pid(Config),
    exit(Pid, kill).

init_per_group(data_tests, Config) ->
    Pid = get_pid(Config),
    q(Pid, ?CREATE_KEYSPACE),
    q(Pid, ?USE_KEYSPACE),
    q(Pid, ?CREATE_TABLE),
    Config;
init_per_group(_GroupName, Config) ->
    Config.

end_per_group(data_tests, Config) ->
    Pid = get_pid(Config),
    q(Pid, ?DROP_KEYSPACE);
end_per_group(_GroupName, _Config) ->
    ok.

init_per_testcase(drop_keyspace, Config) ->
    Pid = get_pid(Config),
    q(Pid, ?CREATE_KEYSPACE),
    Config;
init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(create_keyspace, Config) ->
    Pid = get_pid(Config),
    q(Pid, ?DROP_KEYSPACE);
end_per_testcase(_TestCase, _Config) ->
    ok.

groups() ->
    [{keyspace_tests, [],
      [create_keyspace,
       drop_keyspace]},
     {data_tests, [],
      [insert]}].

all() ->
    [{group, keyspace_tests},
     {group, data_tests}].

%% Tests ----------------------------------------------------------------------

create_keyspace(Config) ->
    Pid = get_pid(Config),
    {ok, created} = q(Pid, ?CREATE_KEYSPACE),
    Keyspaces = get_keyspaces(Pid),
    true == lists:member([?KEYSPACE], Keyspaces).

drop_keyspace(Config) ->
    Pid = get_pid(Config),
    {ok, dropped} = q(Pid, ?DROP_KEYSPACE),
    Keyspaces = get_keyspaces(Pid),
    false == lists:member([?KEYSPACE], Keyspaces).

insert(Config) ->
    Pid = get_pid(Config),
    {ok, void} = q(Pid, <<"INSERT INTO t (k, v) VALUES (1, 'one')">>),
    Rows = q(Pid, <<"SELECT * FROM t">>),
    {ok, {[[<<0, 0, 0, 1>>, <<"one">>]],
          [{<<"k">>, int}, {<<"v">>, varchar}]}} = Rows.

%% Helpers --------------------------------------------------------------------

get_pid(Config) ->
    proplists:get_value(pid, Config).

q(Pid, String) ->
    erlcql_client:query(Pid, String, one).

get_keyspaces(Pid) ->
    Check = <<"SELECT keyspace_name FROM system.schema_keyspaces">>,
    {ok, {Keyspaces, _}} = q(Pid, Check),
    Keyspaces.
