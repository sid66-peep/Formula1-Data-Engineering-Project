-- Databricks notebook source
-- MAGIC %python
-- MAGIC html = """<h1 style="color:Black;text-align:center;font-family:Ariel">Report on Dominant Formula 1 Drivers & Teams </h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_dominant_drivers
AS
select driver_name,
       count(1) AS total_races,
       sum(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points,
       rank() over(order by AVG(calculated_points) DESC) AS driver_rank
from f1_presentation.calculated_race_results
group by driver_name 
having count(1) >= 50
order by avg_points DESC;

-- COMMAND ----------

select race_year,
       driver_name,
       count(1) AS total_races,
       sum(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points
from f1_presentation.calculated_race_results
where driver_name IN (select driver_name from v_dominant_drivers where driver_rank <=10)
group by race_year, driver_name 
order by race_year, avg_points DESC;

-- COMMAND ----------

select race_year,
       driver_name,
       count(1) AS total_races,
       sum(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points
from f1_presentation.calculated_race_results
where driver_name IN (select driver_name from v_dominant_drivers where driver_rank <=10)
group by race_year, driver_name 
order by race_year, avg_points DESC;

-- COMMAND ----------

select race_year,
       driver_name,
       count(1) AS total_races,
       sum(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points
from f1_presentation.calculated_race_results
where driver_name IN (select driver_name from v_dominant_drivers where driver_rank <=10)
group by race_year, driver_name 
order by race_year, avg_points DESC;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_dominant_teams
AS
select team_name,
       count(1) AS total_races,
       sum(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points,
       rank() over(order by AVG(calculated_points) DESC) AS team_rank
from f1_presentation.calculated_race_results
group by team_name 
having count(1) >= 100
order by avg_points DESC;

-- COMMAND ----------

select race_year,
       team_name,
       count(1) AS total_races,
       sum(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points
from f1_presentation.calculated_race_results
where team_name IN (select team_name from v_dominant_teams where team_rank <=5)
group by race_year, team_name 
order by race_year, avg_points DESC;

-- COMMAND ----------

select race_year,
       driver_name,
       count(1) AS total_races,
       sum(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points
from f1_presentation.calculated_race_results
where driver_name IN (select driver_name from v_dominant_drivers where driver_rank <=10) and race_year between 2011 and 2020 
group by race_year, driver_name 
order by race_year, avg_points DESC;

-- COMMAND ----------

select race_year,
       team_name,
       count(1) AS total_races,
       sum(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points
from f1_presentation.calculated_race_results
where team_name IN (select team_name from v_dominant_teams where team_rank <=5) and race_year between 2011 and 2020
group by race_year, team_name 
order by race_year, avg_points DESC;
