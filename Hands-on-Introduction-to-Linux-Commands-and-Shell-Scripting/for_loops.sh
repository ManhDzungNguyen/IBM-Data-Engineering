#!/bin/bash

my_array=(1 2 3)
for item in ${my_array[@]}; do
    echo $item
done

echo "-----"

my_array=(4 5 6)
for i in ${!my_array[@]}; do
    echo ${my_array[$i]}
done

echo "-----"

N=6
for (( i=0; i<=$N; i++ )) ; do
    echo $i
done

echo "-----"

#!/usr/bin/env bash
# initialize array, count, and sum
my_array=(1 2 3)
count=0
sum=0
for i in ${!my_array[@]}; do
    # print the ith array element
    echo ${my_array[$i]}
    # increment the count by one
    count=$(($count+1))
    # add the current value of the array to the sum
    sum=$(($sum+${my_array[$i]}))
done
echo $count
echo $sum