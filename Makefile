.PHONY: all compile get-deps dialyze wait test console clean

APPS = dialyzer.apps
PLT = apps.plt
TIMEOUT = 15

all: get-deps compile

compile:
	@ rebar compile

get-deps:
	@ rebar get-deps

dialyze: compile $(PLT)
	@ echo "==> (dialyze)"
	@ dialyzer --plt $(PLT) ebin \
	  -Wunmatched_returns \
	  -Wno_undefined_callbacks

$(PLT): dialyzer.apps
	@ echo "==> (dialyze)"
	@ printf "Building $(PLT) file..."
	@- dialyzer -q --build_plt --output_plt $(PLT) \
	   --apps $(shell cat $(APPS))
	@ echo " done"

wait:
	@ ./wait $(TIMEOUT) || (echo "Cassandra down"; exit 1)

test: compile wait
	@ rebar skip_deps=true ct

console: compile
	@ erl -pa ebin deps/*/ebin

clean:
	@ rebar clean
