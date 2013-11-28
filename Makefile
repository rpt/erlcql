.PHONY: all compile get-deps dialyze test console clean

APPS = dialyzer.apps
PLT = apps.plt

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

test: compile
	@ rebar skip_deps=true ct

console: compile
	@ erl -pa ebin deps/*/ebin

clean:
	@ rebar clean
