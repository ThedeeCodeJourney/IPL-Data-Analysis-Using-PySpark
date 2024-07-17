# Databricks notebook source
#importing Necessary Libraries
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, BooleanType, DateType, DecimalType
from pyspark.sql.functions import col, sum, avg, when
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC __Loading Data__

# COMMAND ----------

print("Files in the `dbfs:/FileStore/shared_uploads/deeptidongare@gmail.com` directory:")
files_in_filestore_tables = dbutils.fs.ls("dbfs:/FileStore/shared_uploads/deeptidongare@gmail.com")
for f in files_in_filestore_tables:
    print(f)

# COMMAND ----------

dbutils.fs.ls("dbfs:/FileStore/shared_uploads/deeptidongare@gmail.com/Ball_by_Ball2.csv")

# COMMAND ----------

ball_schema = StructType([
    StructField("match_id", IntegerType(), True),
    StructField("over_id", IntegerType(), True),
    StructField("ball_id", IntegerType(), True),
    StructField("innings_no", IntegerType(), True),
    StructField("team_batting", StringType(), True),
    StructField("team_bowling", StringType(), True),
    StructField("striker_batting_position", IntegerType(), True),
    StructField("extra_type", StringType(), True),
    StructField("runs_scored", IntegerType(), True),
    StructField("extra_runs", IntegerType(), True),
    StructField("wides", IntegerType(), True),
    StructField("legbyes", IntegerType(), True),
    StructField("byes", IntegerType(), True),
    StructField("noballs", IntegerType(), True),
    StructField("penalty", IntegerType(), True),
    StructField("bowler_extras", IntegerType(), True),
    StructField("out_type", StringType(), True),
    StructField("caught", BooleanType(), True),
    StructField("bowled", BooleanType(), True),
    StructField("run_out", BooleanType(), True),
    StructField("lbw", BooleanType(), True),
    StructField("retired_hurt", BooleanType(), True),
    StructField("stumped", BooleanType(), True),
    StructField("caught_and_bowled", BooleanType(), True),
    StructField("hit_wicket", BooleanType(), True),
    StructField("obstructingfeild", BooleanType(), True),
    StructField("bowler_wicket", BooleanType(), True),
    StructField("match_date", DateType(), True),
    StructField("season", IntegerType(), True),
    StructField("striker", IntegerType(), True),
    StructField("non_striker", IntegerType(), True),
    StructField("bowler", IntegerType(), True),
    StructField("player_out", IntegerType(), True),
    StructField("fielders", IntegerType(), True),
    StructField("striker_match_sk", IntegerType(), True),
    StructField("strikersk", IntegerType(), True),
    StructField("nonstriker_match_sk", IntegerType(), True),
    StructField("nonstriker_sk", IntegerType(), True),
    StructField("fielder_match_sk", IntegerType(), True),
    StructField("fielder_sk", IntegerType(), True),
    StructField("bowler_match_sk", IntegerType(), True),
    StructField("bowler_sk", IntegerType(), True),
    StructField("playerout_match_sk", IntegerType(), True),
    StructField("battingteam_sk", IntegerType(), True),
    StructField("bowlingteam_sk", IntegerType(), True),
    StructField("keeper_catch", BooleanType(), True),
    StructField("player_out_sk", IntegerType(), True),
    StructField("matchdatesk", DateType(), True)
])

# COMMAND ----------

df_ball = spark.read\
    .schema(ball_schema)\
        .option("header","True")\
            .csv("dbfs:/FileStore/shared_uploads/deeptidongare@gmail.com/Ball_by_Ball2.csv")

df_ball.printSchema()
display(df_ball)

# COMMAND ----------

df_match_schema= StructType([
    StructField("match_sk", IntegerType(), True),
    StructField("match_id", IntegerType(), True),
    StructField("team1", StringType(), True),
    StructField("team2", StringType(), True),
    StructField("match_date", DateType(), True),
    StructField("season_year", IntegerType(), True),
    StructField("venue_name", StringType(), True),
    StructField("city_name", StringType(), True),
    StructField("country_name", StringType(), True),
    StructField("toss_winner", StringType(), True),
    StructField("match_winner", StringType(), True),
    StructField("toss_name", StringType(), True),
    StructField("win_type", StringType(), True),
    StructField("outcome_type", StringType(), True),
    StructField("manofmach", StringType(), True),
    StructField("win_margin", IntegerType(), True),
    StructField("country_id", IntegerType(), True)
])

# COMMAND ----------

df_match = spark.read\
    .schema(df_match_schema)\
        .option("header","True")\
            .csv("/FileStore/tables/Match.csv")

display(df_match)

# COMMAND ----------

schema_player = StructType([
    StructField("player_sk", IntegerType(), True),
    StructField("player_id", IntegerType(), True),
    StructField("player_name", StringType(), True),
    StructField("dob", DateType(), True),
    StructField("batting_hand", StringType(), True),
    StructField("bowling_skill", StringType(), True),
    StructField("country_name", StringType(), True)
])

# COMMAND ----------

df_player = spark.read\
    .schema(schema_player)\
        .option("header","True")\
            .csv("/FileStore/tables/Player.csv")

#display(df_player)
df_player.printSchema()

# COMMAND ----------

display(df_player)

# COMMAND ----------

display(df_player)

# COMMAND ----------

schema_player_match = StructType([
    StructField("player_match_sk", IntegerType(), True),
    StructField("playermatch_key", DecimalType(10, 2), True),  # Adjust precision and scale as necessary
    StructField("match_id", IntegerType(), True),
    StructField("player_id", IntegerType(), True),
    StructField("player_name", StringType(), True),
    StructField("dob", DateType(), True),
    StructField("batting_hand", StringType(), True),
    StructField("bowling_skill", StringType(), True),
    StructField("country_name", StringType(), True),
    StructField("role_desc", StringType(), True),
    StructField("player_team", StringType(), True),
    StructField("opposit_team", StringType(), True),
    StructField("season_year", IntegerType(), True),  # PySpark does not have a YearType, so IntegerType is used
    StructField("is_manofthematch", BooleanType(), True),
    StructField("age_as_on_match", IntegerType(), True),
    StructField("isplayers_team_won", BooleanType(), True),
    StructField("batting_status", StringType(), True),
    StructField("bowling_status", StringType(), True),
    StructField("player_captain", StringType(), True),
    StructField("opposit_captain", StringType(), True),
    StructField("player_keeper", StringType(), True),
    StructField("opposit_keeper", StringType(), True)
])

# COMMAND ----------

df_player_match = spark.read\
    .schema(schema_player_match)\
        .option("header","True")\
            .csv("/FileStore/tables/Player_match.csv")

#display(df_player_match)
df_player_match.printSchema()

# COMMAND ----------

display(df_player_match)

# COMMAND ----------

schema_team = StructType([
    StructField("team_sk", IntegerType(), True),
    StructField("team_id", IntegerType(), True),
    StructField("team_name", StringType(), True)
])

# COMMAND ----------

df_team = spark.read\
    .schema(schema_team)\
        .option("header","True")\
            .csv("/FileStore/tables/Team.csv")

display(df_team)

# COMMAND ----------

# MAGIC %md
# MAGIC __Data Cleaning and Filtering__

# COMMAND ----------

#filtering out wides and noballs for valid deliveries

df_ball = df_ball.filter((col("wides")==0)&(col("noballs")==0))
display(df_ball)

# COMMAND ----------

df_ball_filtered = df_ball.filter((col("wides") == 0) & (col("noballs") == 0))
filtered_count = df_ball_filtered.count()
print(f"Number of rows after filtering: {filtered_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC __Analysis__

# COMMAND ----------

# MAGIC %md
# MAGIC _1. Total and Average Runs_

# COMMAND ----------

#Count Total and average runs scored in each match and innings
total_avg_runs = df_ball.groupBy("match_id","innings_no").agg(
    sum("runs_scored").alias("total_runs"),
    avg("runs_scored").alias("avg_runs")
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC _2. Running Total of Runs Each Over_

# COMMAND ----------

# Window Function: Calculate running total of runs in each match for each over
windowSpec = Window.partitionBy("match_id","innings_no").orderBy("over_id")

df_ball = df_ball.withColumn(
    "running_total_runs",
    sum("runs_scored").over(windowSpec)
)

# COMMAND ----------

# MAGIC %md
# MAGIC _3. High Impact Balls_

# COMMAND ----------

#conditional Column:Flag for High impact ball (either wicket or 6 runs and extra balls) 
df_ball = df_ball.withColumn(
    "High_impact_ball",
    when((col("runs_scored")+ col("extra_runs") > 6) | (col("bowler_wicket")==True), True).otherwise(False)
)
display(df_ball)

# COMMAND ----------

# MAGIC %md
# MAGIC _4. Categorizing High Impact Wins_

# COMMAND ----------

#high margin wins categorize win margins into high, low and medium
df_match = df_match.withColumn(
    "High_margin_wins",
    when(col('win_margin') >= 100, 'High')\
    .when((col("win_margin") >= 50) & (col("win_margin") <100), "Medium")\
        .otherwise("low")
)


display(df_match1)

# COMMAND ----------

# MAGIC %md 
# MAGIC _5. Impact of Toss_

# COMMAND ----------

# analyse the impact of toss
df_match =df_match.withColumn(
    "toss_match_winner",
    when(col("toss_winner")== col("match_winner"), "True")\
        .otherwise("False")
)

df_match1 =df_match.select("match_id", "team1","team2","season_year","venue_name","city_name","toss_winner","match_winner",
                           "toss_name","win_type","outcome_type","manofmach","win_margin","High_margin_wins","toss_match_winner")

display(df_match1)

# COMMAND ----------

from pyspark.sql.functions import lower, regexp_replace

# COMMAND ----------

# MAGIC %md 
# MAGIC __Player Analysis__

# COMMAND ----------

#Normalize and clean players name in player table

df_player = df_player.drop("dob")
#df_player =df_player.withColumn("player_name", lower(regexp_replace("player_name", "[^a-zA-Z0-9 ]", " " )))


#categorizing players based on batting hand
df_player =df_player.withColumn(
    "battling_hand_style",
    when(col("batting_hand").contains("Right"),"Right Handed").otherwise("Left Handed")
)
display(df_player)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC __SQL Views and Queries__

# COMMAND ----------

# MAGIC %md
# MAGIC _1. Creating SQL views_

# COMMAND ----------


df_ball.createOrReplaceTempView("ball")
df_match.createOrReplaceTempView("match")
df_player_match.createOrReplaceTempView("player_match")
df_team.createOrReplaceTempView("team")
df_player.createOrReplaceTempView("player")


# COMMAND ----------

# MAGIC %md
# MAGIC _2. Top Scoring Batsman of the Season_

# COMMAND ----------

# top scoring batsman of the season
top_scoring_batsman_per_season=spark.sql('''
select p.player_name,
m.season_year,
sum(b.runs_scored) as total_runs
from ball b 
JOIN match m on b.match_id =m.match_id
JOIN player_match pm on pm.match_id = m.match_id
JOIN player p on p.player_id = pm.player_id
group by p.player_name, m.season_year
order by m.season_year, total_runs 
                                        
''')

top_scoring_batsman_per_season.show()


# COMMAND ----------

# MAGIC %md
# MAGIC _3. Impact of Toss on Match Outcome_

# COMMAND ----------

toss_match_impact=spark.sql("""
select m.match_id, m.toss_winner, m.match_winner, m.toss_name, m.outcome_type,
CASE when toss_winner == match_winner Then "Won" else "Lost" end as match_outcome
from match m
""")
toss_match_impact.show()

# COMMAND ----------

# MAGIC %md
# MAGIC _4. Average runs on wins_

# COMMAND ----------

average_runs_in_wins = spark.sql("""
SELECT p.player_name, AVG(b.runs_scored) AS avg_runs_in_wins, COUNT(*) AS innings_played
FROM ball b
JOIN player_match pm ON b.match_id = pm.match_id AND b.striker = pm.player_id
JOIN player p ON pm.player_id = p.player_id
JOIN match m ON pm.match_id = m.match_id
WHERE m.match_winner = pm.player_team
GROUP BY p.player_name
ORDER BY avg_runs_in_wins ASC
""")
average_runs_in_wins.show()


# COMMAND ----------

# MAGIC %md
# MAGIC _5. Economical Bowlers in Powerplay_

# COMMAND ----------


economical_bowlers_powerplay = spark.sql("""
SELECT 
p.player_name, 
AVG(b.runs_scored) AS avg_runs_per_ball, 
COUNT(b.bowler_wicket) AS total_wickets
FROM ball b
JOIN player_match pm ON b.match_id = pm.match_id AND b.bowler = pm.player_id
JOIN player p ON pm.player_id = p.player_id
WHERE b.over_id <= 6
GROUP BY p.player_name
HAVING COUNT(*) >= 1
ORDER BY avg_runs_per_ball, total_wickets DESC
""")
economical_bowlers_powerplay.show()


# COMMAND ----------

# MAGIC %md
# MAGIC __Visualization__

# COMMAND ----------

import matplotlib.pyplot as plt

# COMMAND ----------


# Assuming 'economical_bowlers_powerplay' is already executed and available as a Spark DataFrame
economical_bowlers_pd = economical_bowlers_powerplay.toPandas()

# Visualizing using Matplotlib
plt.figure(figsize=(12, 8))
# Limiting to top 10 for clarity in the plot
top_economical_bowlers = economical_bowlers_pd.nsmallest(10, 'avg_runs_per_ball')
plt.bar(top_economical_bowlers['player_name'], top_economical_bowlers['avg_runs_per_ball'], color='skyblue')
plt.xlabel('Bowler Name')
plt.ylabel('Average Runs per Ball')
plt.title('Most Economical Bowlers in Powerplay Overs (Top 10)')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# COMMAND ----------

import seaborn as sns

# COMMAND ----------

toss_match_impact_pd = toss_match_impact.toPandas()
plt.figure(figsize=(10,6))
sns.countplot(x="toss_winner", hue="match_outcome", data=toss_match_impact_pd)
plt.title("Impact of winning Toss on match outcome")
plt.xlabel("toss_winner")
plt.ylabel("Number of matches")
plt.legend(title="Match Outcome")
plt.xticks(rotation=45)
plt.show()

# COMMAND ----------


average_runs_pd = average_runs_in_wins.toPandas()

# Using seaborn to plot average runs in winning matches
plt.figure(figsize=(12, 8))
top_scorers = average_runs_pd.nlargest(10, 'avg_runs_in_wins')
sns.barplot(x='player_name', y='avg_runs_in_wins', data=top_scorers)
plt.title('Average Runs Scored by Batsmen in Winning Matches (Top 10 Scorers)')
plt.xlabel('Player Name')
plt.ylabel('Average Runs in Wins')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()


# COMMAND ----------


