#!/bin/bash

OPTION=$1
TREE_DOT_NAME=t
function test_1() {
    format
    make -j8 b_plus_tree_insert_test
    if [ $? -eq 0 ]; then
        ./test/b_plus_tree_insert_test
    fi
}

function test_2() {
    format
    make -j8 b_plus_tree_delete_test
    if [ $? -eq 0 ]; then
        ./test/b_plus_tree_delete_test
    fi
}

function test_3() {
    format
    make -j8 b_plus_tree_concurrent_test
    if [ $? -eq 0 ]; then
        ./test/b_plus_tree_concurrent_test
    fi 
}

function test_4() {
    format
    make -j8 b_plus_tree_contention_test
    if [ $? -eq 0 ]; then
        ./test/b_plus_tree_contention_test
    fi 
}


function format() {
    clang-format -i \
        ../src/include/storage/page/b_plus_tree_page.h \
        ../src/storage/page/b_plus_tree_page.cpp \
        ../src/include/storage/page/b_plus_tree_internal_page.h \
        ../src/storage/page/b_plus_tree_internal_page.cpp \
        ../src/include/storage/page/b_plus_tree_leaf_page.h \
        ../src/storage/page/b_plus_tree_leaf_page.cpp \
        ../src/include/storage/index/b_plus_tree.h \
        ../src/storage/index/b_plus_tree.cpp \
        ../src/include/storage/index/index_iterator.h \
        ../src/storage/index/index_iterator.cpp

    echo "Formatted the source files for project 2."
}


function profiler(){
    profiler_file=b_plus_tree_profiler.data
    perf record -o $profiler_file -g ./test/b_plus_tree_concurrent_test 
    perf report -i $profiler_file
}

function help(){
    echo "=====Project #2 testing script usage.====="
    echo "1) $0 <test_case_num(1~4)>: test the specific case. Like, $0 0"
    echo "2) $0 format: format the files for this project"
    echo "3) $0 make-print: generate the B+ tree with the given input"
    echo "4) $0 print: display the generated B+ tree in a picture form"
    echo "5) $0 profiler: generate the profiler for the conccurent test(change the profiler function as needed)"
    echo "6) $0 see-profiler: see the result of the previous profiler result"
    echo "7) $0 all: test all the test cases together."
}

if [ "$1" = "help" ]; then
    echo "usage: $0 <task number | all>"
fi

case "$1" in
"1")
    test_1
    ;;
"2")
    test_2
    ;;
"3")
    test_3
    ;;
"4")
    test_4
    ;;
"format")
    format
    ;;
"make-print")
    make b_plus_tree_printer -j8
    echo "### Generating file name should be $TREE_DOT_NAME"
    ./bin/b_plus_tree_printer
    ;;
"print")
    dot -Tpng -O $TREE_DOT_NAME
    eog $TREE_DOT_NAME.png
    ;;
"profiler")
    profiler
    ;;
"see-profiler") 
    profiler_file=b_plus_tree_profiler.data
    perf report -i $profiler_file
    ;; 
"all")
    test_1
    test_2
    test_3
    test_4
    ;;
*)
    help
    exit 1
    ;;
esac
