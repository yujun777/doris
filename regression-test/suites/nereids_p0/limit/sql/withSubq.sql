-- database: presto; groups: limit; tables: nation
SELECT /*+SET_VAR(parallel_pipeline_task_num=2) */
COUNT(*) FROM (SELECT * FROM tpch_tiny_nation LIMIT 10) t1
