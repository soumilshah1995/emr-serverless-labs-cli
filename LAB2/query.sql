SET hive.cli.print.header=true;
SET hive.query.name=ExtremeWeather;

SET hive.tez.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET mapreduce.input.fileinputformat.split.maxsize=16777216;

CREATE TABLE  gold_total_rides_amount AS
SELECT
    year,
    month, COUNT(*) AS total_rides,
    SUM(total_amount) AS total_amount_earned
    FROM nyctaxi
    GROUP BY
    year, month;

