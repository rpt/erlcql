-module(erlcql_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("proper/include/proper.hrl").

-define(OPTS, [{cql_version, <<"3.0.0">>}]).
-define(KEYSPACE, <<"erlcql_tests">>).

-define(CREATE_KEYSPACE, <<"CREATE KEYSPACE IF NOT EXISTS erlcql_tests ",
                           "WITH replication = {'class': 'SimpleStrategy', ",
                           "'replication_factor': 1}">>).
-define(DROP_KEYSPACE, <<"DROP KEYSPACE IF EXISTS erlcql_tests">>).
-define(USE_KEYSPACE, <<"USE erlcql_tests">>).
-define(CREATE_TABLE, <<"CREATE TABLE IF NOT EXISTS t ",
                        "(k int PRIMARY KEY, v text)">>).
-define(DROP_TABLE, <<"DROP TABLE IF EXISTS t">>).

-define(PROPTEST(A), true = proper:quickcheck(A())).
-define(PROPTEST(A, Args), true = proper:quickcheck(A(Args), {numtests, 1000})).

-import(erlcql, [q/2, q/3]).

%% Fixtures -------------------------------------------------------------------

init_per_suite(Config) ->
    {ok, Pid} = erlcql:start_link("localhost", ?OPTS),
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
init_per_group(client, Config) ->
    Pid = get_pid(Config),
    q(Pid, ?CREATE_KEYSPACE),
    Config;
init_per_group(_GroupName, Config) ->
    Config.

end_per_group(types, Config) ->
    Pid = get_pid(Config),
    q(Pid, ?DROP_KEYSPACE);
end_per_group(data_manipulation, Config) ->
    Pid = get_pid(Config),
    q(Pid, ?DROP_KEYSPACE);
end_per_group(client, Config) ->
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
         map_of_strings_to_boolean]}]},
     {data_manipulation, [],
      [insert]},
     {client, [],
      [start_without_use,
       start_with_use]}].

all() ->
    [{group, keyspaces},
     {group, types},
     {group, data_manipulation},
     {group, client}].

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
    create_type_table(Pid, ascii),
    ?PROPTEST(prop_ascii, Pid).

prop_ascii(Pid) ->
    ?FORALL(S, ascii_string(),
            begin
                E = escape_string(S),
                B2 = [$', E, $'],
                insert_type(Pid, B2),
                list_to_binary(S) == get_value(Pid, ascii)
            end).

bigint(Config) ->
    Pid = get_pid(Config),
    create_type_table(Pid, bigint),
    ?PROPTEST(prop_bigint, Pid).

prop_bigint(Pid) ->
    ?FORALL(I, union([integer(), integer(-(1 bsl 63), (1 bsl 63) - 1)]),
            begin
                S = integer_to_binary(I),
                insert_type(Pid, S),
                I == get_value(Pid, bigint)
            end).

boolean(Config) ->
    Pid = get_pid(Config),
    check_type(Pid, boolean, <<"true">>, true),
    check_type(Pid, boolean, <<"false">>, false).

blob(Config) ->
    Pid = get_pid(Config),
    create_type_table(Pid, blob),
    ?PROPTEST(prop_blob, Pid).

prop_blob(Pid) ->
    ?FORALL(B, binary(),
            begin
                Blob = cql_blob(B),
                insert_type(Pid, Blob),
                B == get_value(Pid, blob)
            end).

counter(Config) ->
    Pid = get_pid(Config),
    create_type_table(Pid, counter),
    q(Pid, <<"UPDATE erlcql_tests.t SET v = v + 1 WHERE k = 'key'">>, one),
    q(Pid, <<"UPDATE erlcql_tests.t SET v = v + 2 WHERE k = 'key'">>, one),
    q(Pid, <<"UPDATE erlcql_tests.t SET v = v + 3 WHERE k = 'key'">>, one),
    6 = get_value(Pid, counter).

decimal(Config) ->
    Pid = get_pid(Config),
    create_type_table(Pid, decimal),
    ?PROPTEST(prop_decimal, Pid).

prop_decimal(Pid) ->
    ?FORALL(F, float(),
            begin
                S = float_to_binary(F),
                insert_type(Pid, S),
                V = get_value(Pid, decimal),
                %% NB: This comparison is tuned for these tests only
                true == (abs(F - V) < 1.0e-12)
            end).

double(Config) ->
    Pid = get_pid(Config),
    create_type_table(Pid, double),
    ?PROPTEST(prop_double, Pid).

prop_double(Pid) ->
    ?FORALL(F, float(),
            begin
                S = float_to_binary(F),
                insert_type(Pid, S),
                V = get_value(Pid, double),
                %% NB: This comparison is tuned for these tests only
                true == (abs(F - V) < 1.0e-12)
            end).

float(Config) ->
    Pid = get_pid(Config),
    create_type_table(Pid, float),
    ?PROPTEST(prop_float, Pid).

prop_float(Pid) ->
    ?FORALL(F, float(),
            begin
                S = float_to_binary(F),
                insert_type(Pid, S),
                V = get_value(Pid, float),
                %% NB: This comparison is tuned for these tests only
                true == (abs(F - V) < 1.0e-4)
            end).

inet(Config) ->
    Pid = get_pid(Config),
    create_type_table(Pid, inet),
    ?PROPTEST(prop_inet, Pid).

prop_inet(Pid) ->
    ?FORALL(A, ip_address(),
            begin
                S = [$', inet:ntoa(A), $'],
                insert_type(Pid, S),
                A =:= get_value(Pid, inet)
            end).

int(Config) ->
    Pid = get_pid(Config),
    create_type_table(Pid, int),
    ?PROPTEST(prop_int, Pid).

prop_int(Pid) ->
    ?FORALL(I, union([integer(), integer(-(1 bsl 31), (1 bsl 31) - 1)]),
            begin
                S = integer_to_binary(I),
                insert_type(Pid, S),
                I == get_value(Pid, int)
            end).

text(Config) ->
    Pid = get_pid(Config),
    create_type_table(Pid, text),
    ?PROPTEST(prop_text, Pid).

prop_text(Pid) ->
    ?FORALL(B, utf8_bin(),
            begin
                Text = [$', B, $'],
                insert_type(Pid, Text),
                B == get_value(Pid, text)
            end).

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
    create_type_table(Pid, varchar),
    ?PROPTEST(prop_varchar, Pid).

prop_varchar(Pid) ->
    ?FORALL(B, utf8_bin(),
            begin
                Varchar = [$', B, $'],
                insert_type(Pid, Varchar),
                B == get_value(Pid, varchar)
            end).

varint(Config) ->
    Pid = get_pid(Config),
    create_type_table(Pid, varint),
    ?PROPTEST(prop_varint, Pid).

prop_varint(Pid) ->
    ?FORALL(I, union([integer(), integer(-(1 bsl 63), (1 bsl 63) - 1)]),
            begin
                S = integer_to_binary(I),
                insert_type(Pid, S),
                I == get_value(Pid, varint)
            end).

list_of_ints(Config) ->
    Pid = get_pid(Config),
    create_type_table(Pid, <<"list<int>">>),
    ?PROPTEST(prop_list_of_ints, Pid).

prop_list_of_ints(Pid) ->
    ?FORALL(L, list(integer()),
            begin
                SL = [integer_to_list(X) || X <- L],
                V = iolist_to_binary([$[, string:join(SL, ", "), $]]),
                insert_type(Pid, V),
                L == get_value(Pid, <<"list<int>">>)
            end).

list_of_varints(Config) ->
    Pid = get_pid(Config),
    create_type_table(Pid, <<"list<varint>">>),
    ?PROPTEST(prop_list_of_varints, Pid).

prop_list_of_varints(Pid) ->
    ?FORALL(L, list(integer()),
            begin
                SL = [integer_to_list(X) || X <- L],
                V = iolist_to_binary([$[, string:join(SL, ", "), $]]),
                insert_type(Pid, V),
                L == get_value(Pid, <<"list<varint>">>)
            end).

set_of_floats(Config) ->
    Pid = get_pid(Config),
    check_type(Pid, <<"set<double>">>, <<"{0.1, 1.2, 2.3}">>, [0.1, 1.2, 2.3]).

map_of_strings_to_boolean(Config) ->
    Pid = get_pid(Config),
    create_type_table(Pid, <<"map<varchar, boolean>">>),
    ?PROPTEST(prop_map_of_strings_to_boolean, Pid).

b2l(false) ->
    "false";
b2l(true) ->
    "true".

prop_map_of_strings_to_boolean(Pid) ->
    ?FORALL(L, list({utf8_bin(), boolean()}),
            begin
                ML = [[$', S, "': ", b2l(B)] || {S, B} <- L],
                V = iolist_to_binary([${, string:join(ML, ", "), $}]),
                ct:log("L: ~p", [L]),
                insert_type(Pid, V),
                Z = get_value(Pid, <<"map<varchar, boolean>">>),
                ct:log("Z: ~p", [Z]),
                [] == Z -- L
            end).

insert(Config) ->
    Pid = get_pid(Config),
    {ok, void} = q(Pid, <<"INSERT INTO t (k, v) VALUES (1, 'one')">>),
    Rows = q(Pid, <<"SELECT * FROM t">>, one),
    {ok, {[[1, <<"one">>]], [{<<"k">>, int}, {<<"v">>, varchar}]}} = Rows.

%% Client tests

start_without_use(_Config) ->
    {ok, Pid} = erlcql:start_link("localhost", ?OPTS),
    unlink(Pid),
    Msg = <<"no keyspace has been specified">>,
    {error, {invalid, Msg, _}} = q(Pid, ?CREATE_TABLE),
    exit(Pid, kill).

start_with_use(_Config) ->
    Opts = [{use, ?KEYSPACE} | ?OPTS],
    {ok, Pid} = erlcql:start_link("localhost", Opts),
    unlink(Pid),
    {ok, _} = q(Pid, ?CREATE_TABLE),
    exit(Pid, kill).

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

ascii_char() ->
    ?LET(C, integer(0, 127), C).

ascii_string() ->
    ?LET(L, list(ascii_char()), L).

cql_blob(B) ->
    B2 = bstr:hexencode(B),
    <<"0x", B2/binary>>.

ip_address() ->
    {integer(0, 255), integer(0, 255), integer(0, 255), integer(0, 255)}.

unicode_char() ->
    ?SUCHTHAT(C, char(), C < 16#D800 orelse C > 16#DFFF).

utf8_bin() ->
    ?LET(S, list(unicode_char()),
         begin
             S2 = escape_string(S),
             unicode:characters_to_binary(S2, unicode, utf8)
         end).

escape_string(S) ->
    R = lists:foldl(fun($', Acc) ->
                            [$', $' | Acc];
                       (X, Acc) ->
                            [X | Acc]
                    end,
                    [],
                    S),
    lists:reverse(R).

cql_int_list(L) ->
    SL = [integer_to_list(X) || X <- L],
    [$[, SL, $]].
