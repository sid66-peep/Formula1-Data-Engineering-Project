-- Databricks notebook source
select team_name,
       count(1) AS total_races,
       sum(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points
from f1_presentation.calculated_race_results
group by team_name 
having count(1) >= 100
order by avg_points DESC;

-- COMMAND ----------

select team_name,
       count(1) AS total_races,
       sum(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points
from f1_presentation.calculated_race_results
where race_year between 2011 and 2020
group by team_name 
having count(1) >= 100
order by avg_points DESC;

-- COMMAND ----------

select team_name,
       count(1) AS total_races,
       sum(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points
from f1_presentation.calculated_race_results
where race_year between 2001 and 2010
group by team_name 
having count(1) >= 100
order by avg_points DESC;

-- COMMAND ----------
