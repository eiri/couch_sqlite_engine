.DEFAULT_GOAL := help

COUCHDIR := $(PWD)/couchdb
REBAR := $(COUCHDIR)/bin/rebar

.PHONY: help
help: ## this help message
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

couchdb:
	@git clone https://github.com/apache/couchdb.git

couchdb/bin/couchjs: couchdb
	@cd $(COUCHDIR); ./configure --dev; make

.PHONY: compile-couch
compile-couch: couchdb/bin/couchjs

couchdb/tmp/etc/default_eunit.ini:
	@cd $(COUCHDIR); $(REBAR) setup_eunit

.PHONY: setup-eunit
setup-eunit: couchdb/tmp/etc/default_eunit.ini

deps/couch:
	@mkdir -p $(PWD)/deps; cd $(PWD)/deps; ln -s ../couchdb/src/couch

.PHONY: compile
compile: compile-couch deps/couch ## compile the engine
	@$(REBAR) compile

.PHONY: test
test: export BUILDDIR = $(COUCHDIR)
test: export ERL_AFLAGS = "-config $(COUCHDIR)/rel/files/eunit.config"
test: compile setup-eunit ## run eunit tests
	@$(REBAR) eunit skip_deps=true