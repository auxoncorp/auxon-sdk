ifeq ($(OS),Windows_NT)
    TEST_INGEST := test-ingest.exe
    TEST_MUTATOR_HTTP := test-mutator-http.exe
    TEST_MUTATOR := test-mutator.exe
else
    TEST_INGEST_STATIC := test-ingest-static
    TEST_INGEST := test-ingest
    TEST_MUTATOR_HTTP := test-mutator-http
    TEST_MUTATOR := test-mutator
endif

TEST_INGEST_SRC := ingest.c
TEST_MUTATOR_HTTP_SRC := http_mutator.c
TEST_MUTATOR_SRC := mutator.c

RUN_ID ?= 1
AUTH_TOKEN_HEX ?=

INCLS = -I../target/release-with-debug/include

CC := gcc
CCFLAGS = -ansi -Wall -Wextra -Werror -Wconversion -Wsign-conversion -UNDEBUG \
		  -DRUN_ID=$(RUN_ID) -DAUTH_TOKEN_HEX=\"$(AUTH_TOKEN_HEX)\"
CCFLAGS_ASAN = $(CCFLAGS) \
               -O1 -g3 -fsanitize=address -fno-omit-frame-pointer -fno-common -fno-optimize-sibling-calls
LDFLAGS = "-Wl,-rpath,$(PWD)/../target/release-with-debug/"

DEPS = -lrt -lssl -lcrypto -lm
DYN_LIBS = -L../target/release-with-debug -lmodality $(DEPS)
DYN_LIBS_ASAN = $(DYN_LIBS) -lasan $(DEPS)
STATIC_LIBS = ../target/release-with-debug/libmodality.a -lpthread -ldl $(DEPS)

.PHONY: all clean capi test

all: capi $(TEST_INGEST_STATIC) $(TEST_INGEST) $(TEST_MUTATOR_HTTP) $(TEST_MUTATOR)

capi:
	@cd ../ && MODALITY_SDK_CAPI_OUT_DIR="$(PWD)/../target/release-with-debug" cargo build --profile=release-with-debug

test: all
ifeq ($(OS),Windows_NT)
	./$(TEST_INGEST)
	./$(TEST_MUTATOR_HTTP)
	./$(TEST_MUTATOR)
else
	cd ../target/release-with-debug && ldconfig -Nn .
	LD_LIBRARY_PATH=../target/release-with-debug ASAN_OPTIONS=abort_on_error=1:disable_coredump=0:unmap_shadow_on_exit=1:symbolize=1 ASAN_SYMBOLIZER_PATH=$(shell which llvm-symbolizer) ./$(TEST_INGEST)
endif
	./$(TEST_INGEST_STATIC)
	LD_LIBRARY_PATH=../target/release-with-debug ./$(TEST_MUTATOR_HTTP)
	LD_LIBRARY_PATH=../target/release-with-debug ASAN_OPTIONS=abort_on_error=1:disable_coredump=0:unmap_shadow_on_exit=1:symbolize=1 ASAN_SYMBOLIZER_PATH=$(shell which llvm-symbolizer) ./$(TEST_MUTATOR)

$(TEST_INGEST_STATIC): capi
	$(CC) $(CCFLAGS) $(INCLS) -o $@ $(TEST_INGEST_SRC) $(STATIC_LIBS)

$(TEST_INGEST): capi
	$(CC) $(CCFLAGS_ASAN) $(LDFLAGS) $(INCLS) -o $@ $(TEST_INGEST_SRC) $(DYN_LIBS_ASAN)

$(TEST_MUTATOR_HTTP): capi
	$(CC) $(CCFLAGS_ASAN) $(LDFLAGS) $(INCLS) -o $@ $(TEST_MUTATOR_HTTP_SRC) $(DYN_LIBS_ASAN)

$(TEST_MUTATOR): capi
	$(CC) $(CCFLAGS_ASAN) $(LDFLAGS) $(INCLS) -o $@ $(TEST_MUTATOR_SRC) $(DYN_LIBS_ASAN)

clean:
	-rm -f $(TEST_INGEST_STATIC) $(TEST_INGEST) $(TEST_MUTATOR_HTTP) $(TEST_MUTATOR)
