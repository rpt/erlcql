-module(erlcql_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").

-define(KEYSPACE, <<"erlcql_tests">>).

-define(CREATE_KEYSPACE, <<"CREATE KEYSPACE IF NOT EXISTS erlcql_tests ",
                           "WITH replication = {'class': 'SimpleStrategy', ",
                           "'replication_factor': 1}">>).
-define(DROP_KEYSPACE, <<"DROP KEYSPACE IF EXISTS erlcql_tests">>).
-define(USE_KEYSPACE, <<"USE erlcql_tests">>).
-define(CREATE_TABLE, <<"CREATE TABLE IF NOT EXISTS t ",
                        "(k int PRIMARY KEY, v text)">>).
-define(DROP_TABLE, <<"DROP TABLE IF EXISTS t">>).

-import(erlcql, [q/2, q/3]).

%% Fixtures -------------------------------------------------------------------

init_per_suite(Config) ->
    {ok, Pid} = erlcql:start_link(),
    unlink(Pid),
    q(Pid, ?DROP_KEYSPACE),
    [{pid, Pid} | Config].

end_per_suite(Config) ->
    Pid = get_pid(Config),
    q(Pid, ?DROP_KEYSPACE),
    exit(Pid, kill).

init_per_group(types, Config) ->
    Pid = get_pid(Config),
    q(Pid, ?CREATE_KEYSPACE),
    Config;
init_per_group(data_manipulation, Config) ->
    Pid = get_pid(Config),
    q(Pid, ?CREATE_KEYSPACE),
    q(Pid, ?USE_KEYSPACE),
    q(Pid, ?CREATE_TABLE),
    Config;
init_per_group(_GroupName, Config) ->
    Config.

end_per_group(types, Config) ->
    Pid = get_pid(Config),
    q(Pid, ?DROP_KEYSPACE);
end_per_group(data_manipulation, Config) ->
    Pid = get_pid(Config),
    q(Pid, ?DROP_KEYSPACE);
end_per_group(_GroupName, _Config) ->
    ok.

init_per_testcase(create_keyspace, Config) ->
    Pid = get_pid(Config),
    q(Pid, ?DROP_KEYSPACE),
    Config;
init_per_testcase(drop_keyspace, Config) ->
    Pid = get_pid(Config),
    q(Pid, ?CREATE_KEYSPACE),
    Config;
init_per_testcase(TestCase, Config) ->
    clean_type_table(TestCase, Config),
    Config.

end_per_testcase(create_keyspace, Config) ->
    Pid = get_pid(Config),
    q(Pid, ?DROP_KEYSPACE);
end_per_testcase(TestCase, Config) ->
    clean_type_table(TestCase, Config),
    ok.

groups() ->
    [{keyspaces, [],
      [create_keyspace,
       drop_keyspace]},
     {types, [],
      [{native, [],
        [ascii,
         bigint,
         blob,
         boolean,
         counter,
         decimal,
         double,
         float,
         inet,
         int,
         text,
         timestamp,
         timeuuid,
         uuid,
         varchar,
         varint]},
       {collections, [],
        [list_of_ints,
         list_of_varints,
         set_of_floats,
         map_of_strings_to_stings]}]},
     {data_manipulation, [],
      [insert]}].

all() ->
    [{group, keyspaces},
     {group, types},
     {group, data_manipulation}].

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

%% Type tests

ascii(Config) ->
    Pid = get_pid(Config),
    check_type(Pid, ascii, <<"'string'">>, <<"string">>).

bigint(Config) ->
    Pid = get_pid(Config),
    check_type(Pid, bigint, <<"12345678">>, 12345678).

boolean(Config) ->
    Pid = get_pid(Config),
    check_type(Pid, boolean, <<"true">>, true).

blob(Config) ->
    Pid = get_pid(Config),
    check_type(Pid, blob, <<"0x65726C63716C">>, <<"erlcql">>).

counter(Config) ->
    Pid = get_pid(Config),
    create_type_table(Pid, counter),
    q(Pid, <<"UPDATE erlcql_tests.t SET v = v + 1 WHERE k = 'key'">>, one),
    q(Pid, <<"UPDATE erlcql_tests.t SET v = v + 2 WHERE k = 'key'">>, one),
    q(Pid, <<"UPDATE erlcql_tests.t SET v = v + 3 WHERE k = 'key'">>, one),
    6 = get_value(Pid, counter).

decimal(Config) ->
    Pid = get_pid(Config),
    check_type(Pid, decimal, <<"1234.5678">>, 1234.5678).

double(Config) ->
    Pid = get_pid(Config),
    check_type(Pid, double, <<"1.2345678">>, 1.2345678).

float(Config) ->
    Pid = get_pid(Config),
    check_type(Pid, float, <<"0.15625">>, 0.15625).

inet(Config) ->
    Pid = get_pid(Config),
    check_type(Pid, inet, <<"'10.0.2.12'">>, {10, 0, 2, 12}).

int(Config) ->
    Pid = get_pid(Config),
    check_type(Pid, int, <<"1234">>, 1234).

text(Config) ->
    Pid = get_pid(Config),
    check_type(Pid, text, <<"'string'">>, <<"string">>).

timestamp(Config) ->
    Pid = get_pid(Config),
    check_type(Pid, timestamp, <<"1385553738674">>, 1385553738674).

timeuuid(Config) ->
    Uuid1 = <<"4600fa40-5756-11e3-949a-0800200c9a66">>,
    Pid = get_pid(Config),
    check_type(Pid, timeuuid, Uuid1, Uuid1).

uuid(Config) ->
    Uuid4 = <<"591f0d7e-be9e-48d0-8742-0c096937a902">>,
    Pid = get_pid(Config),
    check_type(Pid, uuid, Uuid4, Uuid4).

varchar(Config) ->
    Pid = get_pid(Config),
    check_type(Pid, varchar, <<"'string'">>, <<"string">>).

varint(Config) ->
    Pid = get_pid(Config),
    check_type(Pid, varint, <<"32800">>, 32800).

list_of_ints(Config) ->
    Pid = get_pid(Config),
    check_type(Pid, <<"list<int>">>, <<"[1, 2, 3]">>, [1, 2, 3]).

list_of_varints(Config) ->
    Pid = get_pid(Config),
    check_type(Pid, <<"list<varint>">>, <<"[1, 2, 3]">>, [1, 2, 3]).

set_of_floats(Config) ->
    Pid = get_pid(Config),
    check_type(Pid, <<"set<double>">>, <<"{0.1, 1.2, 2.3}">>, [0.1, 1.2, 2.3]).

map_of_strings_to_stings(Config) ->
    Pid = get_pid(Config),
    check_type(Pid, <<"map<varchar, boolean>">>,
               <<"{'Poland': true, 'USA': false}">>,
               [{<<"Poland">>, true}, {<<"USA">>, false}]).

%% Data tests

insert(Config) ->
    Pid = get_pid(Config),
    {ok, void} = q(Pid, <<"INSERT INTO t (k, v) VALUES (1, 'one')">>),
    Rows = q(Pid, <<"SELECT * FROM t">>, one),
    {ok, {[[1, <<"one">>]], [{<<"k">>, int}, {<<"v">>, varchar}]}} = Rows.

%% Helpers --------------------------------------------------------------------

get_pid(Config) ->
    proplists:get_value(pid, Config).

get_keyspaces(Pid) ->
    Check = <<"SELECT keyspace_name FROM system.schema_keyspaces">>,
    {ok, {Keyspaces, _}} = q(Pid, Check, one),
    Keyspaces.

check_type(Pid, Type, Value, Expected) ->
    create_type_table(Pid, Type),
    insert_type(Pid, Value),
    Expected = get_value(Pid, Type).

create_type_table(Pid, Type) when is_atom(Type) ->
    TypeBin = atom_to_binary(Type, utf8),
    create_type_table(Pid, TypeBin);
create_type_table(Pid, Type) ->
    {ok, created} = q(Pid, [<<"CREATE TABLE IF NOT EXISTS erlcql_tests.t ",
                              "(k varchar PRIMARY KEY, v ">>, Type, <<")">>]).

insert_type(Pid, Value) ->
    {ok, void} = q(Pid, [<<"INSERT INTO erlcql_tests.t ",
                           "(k, v) VALUES ('key', ">>, Value, <<")">>]).

get_value(Pid, text) ->
    get_value(Pid, varchar);
get_value(Pid, <<"list<int>">>) ->
    get_value(Pid, {list, int});
get_value(Pid, <<"list<varint>">>) ->
    get_value(Pid, {list, varint});
get_value(Pid, <<"set<double>">>) ->
    get_value(Pid, {set, double});
get_value(Pid, <<"map<varchar, boolean>">>) ->
    get_value(Pid, {map, varchar, boolean});
get_value(Pid, Type) ->
    Res = q(Pid, <<"SELECT v FROM erlcql_tests.t">>, one),
    {ok, {[[Value]], [{<<"v">>, Type}]}} = Res,
    Value.

clean_type_table(TestCase, Config) ->
    {_, [], Tests} = lists:keyfind(types, 1, groups()),
    {_, [], Native} = lists:keyfind(native, 1, Tests),
    {_, [], Collections} = lists:keyfind(collections, 1, Tests),
    Types = Native ++ Collections,
    case lists:member(TestCase, Types) of
        true ->
            Pid = get_pid(Config),
            q(Pid, <<"DROP TABLE IF EXISTS erlcql_tests.t">>);
        false ->
            ok
    end.
