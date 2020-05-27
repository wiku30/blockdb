#!/bin/bash

# This script starts a series of tests.

cd ../
if [ ! -f "config.json" ]; then
  	echo "!! Error: config.json not found."
	exit -1
fi

echo "Killall go before the tests!"
killall go

echo "Tests start..."

I=0
for filename in example_test/test*.go; do
	echo "Running " $filename
	TESTS[$I]=$filename
	go run $filename
	RESULT[$I]=$?
	I=$[I + 1]
done

# Define colors for summary.
BGre='\e[1;32m';
BRed='\e[1;31m';
NoColor='\033[0m';

echo "==== Test Summary ===="
TOTAL=$I
SUM=0
N=$[TOTAL-1]
for I in `seq 0 $N`; do
	filename=${TESTS[$I]}
	code=${RESULT[$I]}
	printf "$filename :"
	if [ "$code" -eq "0" ]; then
		printf "${BGre} PASS ${NoColor} \n"
		SUM=$[SUM + 1]
	else
		printf "${BRed} FAIL ${NoColor} \n"
	fi
done
echo "Overall: $SUM / $TOTAL "
