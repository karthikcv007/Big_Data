-- Load the dataset (space-separated fields)
weather_data = LOAD '/user/hadoop/weather/weather.txt' 
                USING PigStorage(' ') 
                AS (year:int, month:int, day:int, temperature:int);

-- Group data by year
grouped_data = GROUP weather_data BY year;

-- Find maximum temperature for each year
max_temp = FOREACH grouped_data GENERATE 
                group AS year,
                MAX(weather_data.temperature) AS max_temperature;

-- (Optional) remove old output if exists
rmf /user/hadoop/output/max_temp_by_year;

-- Store the output in HDFS
STORE max_temp INTO '/user/hadoop/output/max_temp_by_year' USING PigStorage(',');




