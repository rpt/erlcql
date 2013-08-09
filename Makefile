APPS = erts kernel stdlib

compile:
	@ rebar compile

dialyzer: build.plt compile
	dialyzer --plt $< ebin

build.plt:
	dialyzer -q --build_plt --apps $(APPS) --output_plt $@

clean:
	@ rebar clean

.PHONY: compile dialyzer clean

