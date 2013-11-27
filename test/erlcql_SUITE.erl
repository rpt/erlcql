-module(erlcql_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").

-define(KEYSPACE, <<"erlcql_tests">>).

-define(CREATE_KEYSPACE,
        [<<"CREATE KEYSPACE erlcql_tests WITH replication = ",
           "{'class': 'SimpleStrategy', 'replication_factor': 1}">>]).
-define(DROP_KEYSPACE, <<"DROP KEYSPACE erlcql_tests">>).
-define(USE_KEYSPACE, <<"USE erlcql_tests">>).
-define(CREATE_TABLE, <<"CREATE TABLE t (k int PRIMARY KEY, v text)">>).
-define(DROP_TABLE, <<"DROP TABLE t">>).

-import(erlcql, [q/2, q/3]).

%% Fixtures -------------------------------------------------------------------

suite() ->
    [{ct_hooks, [cth_surefire]},
     {timetrap, {seconds, 60}}].

init_per_suite(Config) ->
    {ok, Pid} = erlcql:start_link(),
    unlink(Pid),
    q(Pid, ?DROP_KEYSPACE),
    [{pid, Pid} | Config].

end_per_suite(Config) ->
    Pid = get_pid(Config),
    exit(Pid, kill).

init_per_group(data_tests, Config) ->
    Pid = get_pid(Config),
    q(Pid, ?CREATE_KEYSPACE),
    {ok, ?KEYSPACE} = q(Pid, ?USE_KEYSPACE),
    q(Pid, ?CREATE_TABLE),
    Config;
init_per_group(_GroupName, Config) ->
    Config.

end_per_group(data_tests, Config) ->
    Pid = get_pid(Config),
    {ok, dropped} = q(Pid, ?DROP_KEYSPACE);
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
    {ok, dropped} = q(Pid, ?DROP_KEYSPACE);
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
    Rows = q(Pid, <<"SELECT * FROM t">>, one),
    {ok, {[[<<0, 0, 0, 1>>, <<"one">>]],
          [{<<"k">>, int}, {<<"v">>, varchar}]}} = Rows.

%% Helpers --------------------------------------------------------------------

get_pid(Config) ->
    proplists:get_value(pid, Config).

get_keyspaces(Pid) ->
    Check = <<"SELECT keyspace_name FROM system.schema_keyspaces">>,
    {ok, {Keyspaces, _}} = q(Pid, Check, one),
    Keyspaces.
