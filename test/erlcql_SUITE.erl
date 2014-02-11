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
                S = list_to_binary(integer_to_list(I)),
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
    ?PROPTEST(prop_counter, Pid).


prop_counter(Pid) ->
    ?FORALL(L, non_empty(list(integer())),
            begin
                [q(Pid, ["UPDATE erlcql_tests.t SET v = v + ", integer_to_list(I), " WHERE k = 'key'"], one) ||
                    I <- L],
                S = lists:sum(L),
                V = get_value(Pid, counter),
                q(Pid, ["UPDATE erlcql_tests.t SET v = v - ", integer_to_list(V), " WHERE k = 'key'"], one),
                S =:= V
            end).

decimal(Config) ->
    Pid = get_pid(Config),
    create_type_table(Pid, decimal),
    ?PROPTEST(prop_decimal, Pid).

prop_decimal(Pid) ->
    ?FORALL(F, float(),
            begin
                S = list_to_binary(float_to_list(F)),
                insert_type(Pid, S),
                V = get_value(Pid, decimal),
                compare_floats(F, V, 1.0e-12)
            end).

double(Config) ->
    Pid = get_pid(Config),
    create_type_table(Pid, double),
    ?PROPTEST(prop_double, Pid).

prop_double(Pid) ->
    ?FORALL(F, float(),
            begin
                S = list_to_binary(float_to_list(F)),
                insert_type(Pid, S),
                V = get_value(Pid, double),
                compare_floats(F, V, 1.0e-12)
            end).

float(Config) ->
    Pid = get_pid(Config),
    create_type_table(Pid, float),
    ?PROPTEST(prop_float, Pid).

prop_float(Pid) ->
    ?FORALL(F, float(),
            begin
                S = list_to_binary(float_to_list(F)),
                insert_type(Pid, S),
                V = get_value(Pid, float),
                compare_floats(F, V, 1.0e-7)
            end).

inet(Config) ->
    Pid = get_pid(Config),
    create_type_table(Pid, inet),
    ?PROPTEST(prop_inet, Pid).

prop_inet(Pid) ->
    ?FORALL(A, ip_address(),
            begin
                S = [$', ntoa(A), $'],
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
                S = list_to_binary(integer_to_list(I)),
                insert_type(Pid, S),
                I == get_value(Pid, int)
            end).

text(Config) ->
    Pid = get_pid(Config),
    create_type_table(Pid, text),
    ?PROPTEST(prop_text, Pid).

prop_text(Pid) ->
    ?FORALL(L, unicode_list(),
            begin
                E = escape_utf8_bin(L),
                B = utf8_bin(L),
                Text = [$', E, $'],
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
    ?FORALL(L, unicode_list(),
            begin
                E = escape_utf8_bin(L),
                B = utf8_bin(L),
                Varchar = [$', E, $'],
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
                S = list_to_binary(integer_to_list(I)),
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
    ?FORALL(L, list({unicode_list(), boolean()}),
            begin
                L2 = [{utf8_bin(U), B} || {U, B} <- L],
                ML = [[$', escape_utf8_bin(U), "': ", b2l(B)] || {U, B} <- L],
                V = iolist_to_binary([${, string:join(ML, ", "), $}]),
                insert_type(Pid, V),
                Z = get_value(Pid, <<"map<varchar, boolean>">>),
                [] == Z -- L2
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
    B2 = hexencode(B),
    <<"0x", B2/binary>>.

ip_address() ->
    {integer(0, 255), integer(0, 255), integer(0, 255), integer(0, 255)}.

unicode_char() ->
    ?SUCHTHAT(C, char(), C < 16#D800 orelse C > 16#DFFF).

utf8_bin(S) ->
    unicode:characters_to_binary(S, unicode, utf8).

escape_utf8_bin(S) ->
    S2 = escape_string(S),
    unicode:characters_to_binary(S2, unicode, utf8).

unicode_list() ->
    ?LET(S, list(unicode_char()), S).

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

-spec hexencode(binary()) -> binary().
hexencode(Str) ->
    hexencode(Str, <<>>).

hexencode(<<Hi:4, Lo:4, Tail/binary>>, Acc) ->
    HiChar = integer_to_hex_char(Hi, lower),
    LoChar = integer_to_hex_char(Lo, lower),
    hexencode(Tail, <<Acc/binary, HiChar, LoChar>>);
hexencode(<<>>, Acc) ->
    Acc.

integer_to_hex_char(N) when N >= 0 ->
    if
        N =< 9 ->
            $0 + N;
        N =< 15 ->
            $A - 10 + N;
        true ->
            erlang:error(badarg)
    end.

integer_to_hex_char(N, lower) when N >= 0 ->
    if
        N =< 9 ->
            $0 + N;
        N =< 15 ->
            $a - 10 + N;
        true ->
            erlang:error(badarg)
    end;
integer_to_hex_char(N, upper) ->
    integer_to_hex_char(N).

ntoa({A,B,C,D}) ->
    integer_to_list(A) ++ "." ++ integer_to_list(B) ++ "." ++
        integer_to_list(C) ++ "." ++ integer_to_list(D);
ntoa({0,0,0,0,0,0,0,0}) -> "::";
ntoa({0,0,0,0,0,0,0,1}) -> "::1";
ntoa({0,0,0,0,0,0,A,B}) -> "::" ++ dig_to_dec(A) ++ "." ++ dig_to_dec(B);
ntoa({0,0,0,0,0,16#ffff,A,B}) ->
    "::FFFF:" ++ dig_to_dec(A) ++ "." ++ dig_to_dec(B);
ntoa({_,_,_,_,_,_,_,_}=T) ->
    ntoa(tuple_to_list(T), []);
ntoa(_) ->
    {error, einval}.

ntoa([], R) ->
    ntoa_done(R);
ntoa([0,0|T], R) ->
    ntoa(T, R, 2);
ntoa([D|T], R) ->
    ntoa(T, [D|R]).

ntoa([], R, _) ->
    ntoa_done(R, []);
ntoa([0|T], R, N) ->
    ntoa(T, R, N+1);
ntoa([D|T], R, N) ->
    ntoa(T, R, N, [D]).

ntoa([], R1, _N1, R2) ->
    ntoa_done(R1, R2);
ntoa([0,0|T], R1, N1, R2) ->
    ntoa(T, R1, N1, R2, 2);
ntoa([D|T], R1, N1, R2) ->
    ntoa(T, R1, N1, [D|R2]).

ntoa(T, R1, N1, R2, N2) when N2 > N1 ->
    ntoa(T, R2++dup(N1, 0, R1), N2);
ntoa([], R1, _N1, R2, N2) ->
    ntoa_done(R1, dup(N2, 0, R2));
ntoa([0|T], R1, N1, R2, N2) ->
    ntoa(T, R1, N1, R2, N2+1);
ntoa([D|T], R1, N1, R2, N2) ->
    ntoa(T, R1, N1, [D|dup(N2, 0, R2)]).

ntoa_done(R1, R2) ->
    lists:append(
      separate(":", lists:map(fun dig_to_hex/1, lists:reverse(R1)))++
      ["::"|separate(":", lists:map(fun dig_to_hex/1, lists:reverse(R2)))]).

ntoa_done(R) ->
    lists:append(separate(":", lists:map(fun dig_to_hex/1, lists:reverse(R)))).

separate(_E, []) ->
    [];
separate(E, [_|_]=L) ->
    separate(E, L, []).

separate(E, [H|[_|_]=T], R) ->
    separate(E, T, [E,H|R]);
separate(_E, [H], R) ->
    lists:reverse(R, [H]).

dup(0, _, L) ->
    L;
dup(N, E, L) when is_integer(N), N >= 1 ->
    dup(N-1, E, [E|L]).

dig_to_dec(0) -> "0.0";
dig_to_dec(X) ->
    integer_to_list((X bsr 8) band 16#ff) ++ "." ++
        integer_to_list(X band 16#ff).

dig_to_hex(X) ->
    erlang:integer_to_list(X, 16).

compare_floats(A, B, E) ->
    AA = abs(A),
    AB = abs(B),
    Diff = abs(A - B),
    if
        A == 0.0 orelse B == 0.0 ->
            true;
        true ->
            (Diff / (AA + AB)) < E
    end.
