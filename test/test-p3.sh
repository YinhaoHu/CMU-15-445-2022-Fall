#!/bin/bash


function test() {
    option=$1 
    # OPTION should be this format: xx. e.g. 01, 12
    if [ "${#option}" -ne 2 ]; then 
        echo "error: test parameter should be a 2-digits number. Enter '$0 help' for more information"
        exit 1
    fi
    format
    test_case=../test/sql/p3.*$option*.slt
    make -j8 sqllogictest 
    if [ $? -eq 0 ]; then
        ./bin/bustub-sqllogictest $test_case --verbose 
        echo "------------------------------------"
        echo "$(printf "*** Detail for test case %s are shown above." $test_case)"
    fi   
} 

function shell(){
    make -j8 shell
    ./bin/bustub-shell
}

function help(){
    echo "=====Project #3 testing script usage.====="
    echo "(1) $0 test <test_case> : run the specific test case, the parameter MUST be two digits(e.g. $0 test 01)"
    echo "  or q# for leaderboard test(e.g. $0 test q1)"
    echo "(2) $0 list : show all the test cases"
    echo "(3) $0 help : show help information"
    echo "(4) $0 shell: compile and run the bustub shell"
}

function list(){
    echo "=== All test cases are listed below ==="
    ls ../test/sql/* | grep p3 --color=never
}

function full-test() {
    for ((i=1;i<10;i++)); do
        test 0$i 
    done
    for ((i=10;i<=16;i++)); do
        test $i 
    done
}

function format() {    
    cd ..
    clang-format -i \
        src/include/execution/executors/seq_scan_executor.h\
        src/execution/seq_scan_executor.cpp\
        src/include/execution/executors/insert_executor.h\
        src/execution/insert_executor.cpp\
        src/include/execution/executors/delete_executor.h\
        src/execution/delete_executor.cpp\
        src/include/execution/executors/index_scan_executor.h\
        src/execution/index_scan_executor.cpp\
        src/include/execution/sort_executor.h\
        src/execution/sort_executor.cpp\
        src/include/execution/limit_executor.h\
        src/execution/limit_executor.cpp\
        src/include/execution/topn_executor.h\
        src/execution/topn_executor.cpp\
        src/optimizer/sort_limit_as_topn.cpp\
        src/include/execution/aggregation_executor.h\
        src/execution/aggregation_executor.cpp\
        src/include/execution/nested_loop_join_executor.h\
        src/execution/nested_loop_join_executor.cpp\
        src/include/execution/nested_index_join_executor.h\
        src/execution/nested_index_join_executor.cpp
    cd build
    echo "Formatted the source files for project 3."
}

case "$1" in
    "help") 
        help
     ;;
    "list")
        list
        ;;
    "shell")
        shell
        ;;
    "format")
        format
        ;;
    "test")
        test $2
        ;;
    "full-test")
        full-test
        ;;
    *) 
        help
        ;;
esac