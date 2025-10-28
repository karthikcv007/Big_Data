-- Load the data; assuming space as delimiter
data = LOAD '/user/hadoop/new_input/New_input.txt' USING PigStorage(' ') AS (name:chararray, age:int);

-- Sort the data by age in ascending order
sorted_data = ORDER data BY age ASC;

-- Store the result in HDFS
STORE sorted_data INTO '/user/hadoop/output' USING PigStorage(' ');


