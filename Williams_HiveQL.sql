-- Databricks notebook source
-- MAGIC %md Question 1 

-- COMMAND ----------

SELECT count(id)
FROM clinicaltrial_2021
group by id 
having count(id) >1 


-- COMMAND ----------

SELECT count(id)
FROM clinicaltrial_2021

-- COMMAND ----------

-- MAGIC %md Question 2 

-- COMMAND ----------

SELECT TYPE, COUNT(TYPE)
FROM clinicaltrial_2021
GROUP BY TYPE
ORDER BY COUNT(TYPE) DESC

-- COMMAND ----------

-- MAGIC %md Question 3

-- COMMAND ----------

CREATE temp VIEW explode_conditions1
AS select explode(split(conditions, ',')) as exploded
from clinicaltrial_2021

-- COMMAND ----------

select exploded, count(exploded) as count from explode_conditions1 group by exploded 
order by count desc limit 5 

-- COMMAND ----------

-- MAGIC %md Question 4

-- COMMAND ----------

CREATE temp VIEW mesh_code
AS select term, left(tree, 3) as code from mesh

-- COMMAND ----------

select count(*) from mesh_code

-- COMMAND ----------

CREATE TEMP VIEW mesh_code_2 AS select term, collect_list(code) as list_code
from mesh_code
group by term


-- COMMAND ----------

select * from mesh_code_2

-- COMMAND ----------

SELECT * FROM explode_conditions1

-- COMMAND ----------

CREATE TEMP VIEW explode_condition_3
as select exploded, count(exploded) as count from explode_conditions1 group by exploded order by count(exploded) desc 

-- COMMAND ----------

CREATE TEMP VIEW joined 
as select count, list_code
from (select e.exploded, e.count, c.term, c.list_code
FROM explode_condition_3 e JOIN mesh_code_2 c ON e.exploded=c.term
WHERE e.exploded is not null)

-- COMMAND ----------

SELECT * FROM joined

-- COMMAND ----------

CREATE TEMP VIEW joined_exploded
as select count, explode(list_code) from joined 

-- COMMAND ----------

select col, sum(count) from joined_exploded group by col order by sum(count) desc limit 5

-- COMMAND ----------

-- MAGIC %md Question 5 

-- COMMAND ----------

CREATE TEMP VIEW sponsor_count 
as select count(sponsor) as count, sponsor
FROM clinicaltrial_2021
group by sponsor
order by count(sponsor) desc


-- COMMAND ----------

select count, sponsor from sponsor_count 
LEFT ANTI JOIN pharma on sponsor_count.sponsor=pharma.Parent_Company
limit 10

-- COMMAND ----------

-- MAGIC %md Question 6 

-- COMMAND ----------

CREATE TEMP VIEW completed 
as select split(completion, ' ')[0] as month, split(completion, ' ')[1] as year, status from clinicaltrial_2021
where status = "Completed"  


-- COMMAND ----------

-- MAGIC %python 
-- MAGIC dbutils.fs.rm('dbfs:/user/hive/warehouse/months_index', True)

-- COMMAND ----------

drop table if exists months_index

-- COMMAND ----------

create table months_index(
month string,
index int)


-- COMMAND ----------

INSERT INTO TABLE months_index
  VALUES ("Jan", 1), ("Feb", 2), ("Mar",3), ("Apr",4), ("May",5), ("Jun",6), 
          ("Jul",7), ("Aug",8), ("Sep",9), ("Oct",10), ("Nov",11), ("Dec",12);

-- COMMAND ----------

select c.month, count(c.month) as count_month from completed c
join months_index m on m.month=c.month
where year = 2021
group by c.month, index
order by index

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC df = sqlContext.sql('select c.month, count(c.month) as count_month from completed c join months_index m on m.month=c.month where year = 2021 group by c.month, index order by index')

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC 
-- MAGIC new_pandas = df.toPandas()
-- MAGIC first_column = new_pandas['month'].tolist()
-- MAGIC second_column = new_pandas['count_month'].tolist()
-- MAGIC print(first_column,second_column)

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC import matplotlib.pyplot as plt
-- MAGIC %matplotlib inline
-- MAGIC plt.bar(first_column, second_column)
-- MAGIC plt.xlabel("months")
-- MAGIC plt.ylabel("count")
-- MAGIC plt.title('Number of completed studies by Month')
