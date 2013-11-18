.PHONY: all compile get-deps dialyze test console clean

APPS = erts kernel stdlib
PLT = apps.plt

all: get-deps compile

compile:
	@ rebar compile

get-deps:
	@ rebar get-deps

dialyze: $(PLT)
	@ echo "==> (dialyze)"
	@ dialyzer --plt $(PLT) ebin -Wunmatched_returns -Wno_undefined_callbacks

$(PLT):
	@ echo "==> (dialyze)"
	@ printf "Building $(PLT) file..."
	@- dialyzer -q --build_plt --output_plt $(PLT) --apps $(APPS) \
	   deps/snappy/ebin
	@ echo " done"

test: compile
	@ rebar skip_deps=true ct

console:
	@ erl -pa ebin deps/*/ebin

clean:
	@ rebar clean
