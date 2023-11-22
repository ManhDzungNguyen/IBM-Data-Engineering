my_array=(1 2 "three" "four" 5)
my_array+=("six")
my_array+=(7)

# print the first item of the array:
echo ${my_array[0]}

# print the third item of the array:
echo ${my_array[2]}

# print all array elements:
echo ${my_array[@]}