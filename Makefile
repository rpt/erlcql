.PHONY: all deps compile dialyze wait test console clean

APPS = dialyzer.apps
PLT = apps.plt
TIMEOUT = 15

all: deps compile

deps:
	@ rebar get-deps

compile:
	@ rebar compile

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
	@ ./scripts/wait.sh $(TIMEOUT) || (echo "Cassandra down"; exit 1)

test: compile wait
	@ rebar skip_deps=true ct

console: compile
	@ erl -pa ebin deps/*/ebin

clean:
	@ rebar clean
