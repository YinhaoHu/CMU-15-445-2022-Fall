#!/bin/bash

OPTION=$1

function test_1() {
    format
    make -j8 extendible_hash_table_test;
    if [ $? -eq 0 ]; then
        ./test/extendible_hash_table_test;
    fi
}

function test_2() {
    format
    make -j8 lru_k_replacer_test;
    if [ $? -eq 0 ]; then
        ./test/lru_k_replacer_test;
    fi
}

function test_3() {
    format
    make -j8 buffer_pool_manager_instance_test;
    if [ $? -eq 0 ]; then
        ./test/buffer_pool_manager_instance_test;
    fi 
}

function format() {
    clang-format -i \
    ../src/include/container/hash/extendible_hash_table.h \
    ../src/container/hash/extendible_hash_table.cpp \
    ../src/include/buffer/lru_k_replacer.h \
    ../src/buffer/lru_k_replacer.cpp \
    ../src/include/buffer/buffer_pool_manager_instance.h \
    ../src/buffer/buffer_pool_manager_instance.cpp 

    echo "Formatted the source files for project 1."
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
    "format")
    format
    ;;
    "all") 
    test_1
    test_2
    test_3
    ;;
    *)
    echo "unexpected input"
    exit 1
    ;;
esac