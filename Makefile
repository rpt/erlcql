APPS = erts kernel stdlib
PLT = apps.plt

compile: get-deps
	@ rebar compile

get-deps:
	@ rebar get-deps

dialyze: compile $(PLT)
	@ echo "==> (dialyzer)"
	@ dialyzer --plt $(PLT) ebin -Wunmatched_returns

$(PLT):
	@ echo "==> (dialyzer)"
	@ printf "Building $(PLT) file..."
	@- dialyzer -q --build_plt --output_plt $(PLT) --apps $(APPS) \
	   deps/snappy/ebin
	@ echo " done"

clean:
	@ rebar clean

.PHONY: compile get-deps dialyzer clean
