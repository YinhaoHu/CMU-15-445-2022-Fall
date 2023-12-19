function test() {
    case $1 in
    1)
        make -j8 lock_manager_test
        ./test/lock_manager_test
        ;;
    2)
        make -j8 deadlock_detection_test
        ./test/deadlock_detection_test
        ;;
    3)
        make -j8 transaction_test
        ./test/transaction_test
        ;;
    4)
        make -j8 terrier-bench
        ./bin/bustub-terrier-bench --duration 30000
        ;;
    *)
        echo "test case <$1> is not supported."
        ;;
    esac
    exit 0
}

function format() {    
    cd ..
    clang-format -i \
        src/include/concurrency/transaction.h\
        src/concurrency/transaction.h
    cd build
    echo "Formatted the source files for project 4."
}

function help() {
    echo "***** Test script for project #4 Concurrency Control *****"
    echo "$0 help : show help information"
    echo "$0 format : format the files to be commited"
    echo "$0 test <case>: test the specific case and the case range is [1,3]"
}

case "$1" in
    "help") 
        help
     ;;
    "format")
        format
        ;;
    "test")
        test $2
        ;;
    *) 
        help
        ;;
esac