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

%% @doc API module.
%% @author Krzysztof Rutka <krzysztof.rutka@gmail.com>
-module(erlcql).

%% API
-export([start_link/0,
         start_link/1,
         start_link/2]).
-export([q/2, query/2,
         q/3, query/3,
         e/3, execute/3,
         e/4, execute/4]).

-include("erlcql.hrl").

-define(DEFAULT_HOST, "localhost").
-define(DEFAULT_CONSISTENCY, one).

%%-----------------------------------------------------------------------------
%% API functions
%%-----------------------------------------------------------------------------

-spec start_link() -> {ok, pid()} | ignore | {error, Reason :: term()}.
start_link() ->
    start_link(?DEFAULT_HOST).

-spec start_link(string()) -> {ok, pid()} | ignore | {error, Reason :: term()}.
start_link(Host) ->
    start_link(Host, []).

-spec start_link(string(), proplists:proplist()) ->
          {ok, pid()} | ignore | {error, Reason :: term()}.
start_link(Host, Opts) ->
    erlcql_client:start_link(Host, Opts).

-spec q(pid(), bitstring()) -> result() | {error, Reason :: term()}.
q(Pid, Query) ->
    query(Pid, Query, ?DEFAULT_CONSISTENCY).

-spec query(pid(), bitstring()) -> result() | {error, Reason :: term()}.
query(Pid, Query) ->
    query(Pid, Query, ?DEFAULT_CONSISTENCY).

-spec q(pid(), bitstring(), consistency()) ->
          result() | {error, Reason :: term()}.
q(Pid, Query, Consistency) ->
    query(Pid, Query, Consistency).

-spec query(pid(), bitstring(), consistency()) ->
          result() | {error, Reason :: term()}.
query(Pid, Query, Consistency) ->
    erlcql_client:query(Pid, Query, Consistency).

-spec e(pid(), binary(), [binary()]) ->
          result() | {error, Reason :: term()}.
e(Pid, QueryId, Values) ->
    execute(Pid, QueryId, Values, ?DEFAULT_CONSISTENCY).

-spec execute(pid(), binary(), [binary()]) ->
          result() | {error, Reason :: term()}.
execute(Pid, QueryId, Values) ->
    execute(Pid, QueryId, Values, ?DEFAULT_CONSISTENCY).

-spec e(pid(), binary(), [binary()], consistency()) ->
          result() | {error, Reason :: term()}.
e(Pid, QueryId, Values, Consistency) ->
    execute(Pid, QueryId, Values, Consistency).

-spec execute(pid(), binary(), [binary()], consistency()) ->
          result() | {error, Reason :: term()}.
execute(Pid, QueryId, Values, Consistency) ->
    erlcql_client:execute(Pid, QueryId, Values, Consistency).
