#!/bin/bash

OUT_FILE="test.out"

./test-p2.sh 3 > $OUT_FILE

if [ $? -eq 0 ]; then
    cat $OUT_FILE | grep OK
    rm $OUT_FILE
else 
    echo "### Some tests are not passed, see $OUT_FILE for more information"
fi

