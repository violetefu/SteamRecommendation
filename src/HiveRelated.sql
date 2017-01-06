```sh
hadoop fs -mkdir ./Game_Recommendation
hadoop fs -put ./steam_app_info.csv ./Game_Recommendation
hadoop fs -put ./game_recommended.csv ./Game_Recommendation

hive
```
/************************************/
/*** Create tbl_steam_app in Hive ***/
/************************************/
```sql
USE wq_db;

CREATE TABLE IF NOT EXISTS tbl_steam_app_old(
    steam_appid INT,
    name VARCHAR(500),
    type VARCHAR(15),
    initial_price FLOAT,
    release_date VARCHAR(20),
    score INT,
    recommendation INT,
    windows INT,
    mac INT,
    linux INT,
    header_image VARCHAR(100)
)
COMMENT "This is the steam apps' information"
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
ESCAPED BY '"'
STORED AS TEXTFILE tblproperties("skip.header.line.count"="1");

LOAD DATA INPATH './Game_Recommendation/steam_app_info.csv' OVERWRITE INTO TABLE tbl_steam_app_old;
```
/****************************************/
/*** Convert INT(0/1) To BOOLEAN Type ***/
/****************************************/
```sql
SELECT *
FROM tbl_steam_app_old
LIMIT 10;

CREATE TABLE IF NOT EXISTS tbl_steam_app(
    steam_appid INT,
    name VARCHAR(500),
    type VARCHAR(15),
    initial_price FLOAT,
    release_date VARCHAR(20),
    score INT,
    recommendation INT,
    windows BOOLEAN,
    mac BOOLEAN,
    linux BOOLEAN,
    header_image VARCHAR(100)
)
COMMENT "This is the steam apps' information"
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
ESCAPED BY '"'
STORED AS TEXTFILE;

INSERT INTO tbl_steam_app
SELECT steam_appid, name, type, initial_price, release_date, score, recommendation, CAST(windows AS BOOLEAN), CAST(mac AS BOOLEAN), CAST(linux AS BOOLEAN), header_image
FROM tbl_steam_app_old;

SELECT *
FROM tbl_steam_app
LIMIT 10;

SELECT count(*)
FROM tbl_steam_app;
```

/********************************************/
/*** Create tbl_recommended_games in Hive ***/
/********************************************/
```sql
CREATE TABLE IF NOT EXISTS tbl_recommended_games(
    user_id BIGINT,
    g0 INT,
    g1 INT,
    g2 INT,
    g3 INT,
    g4 INT,
    g5 INT,
    g6 INT,
    g7 INT,
    g8 INT,
    g9 INT
)
COMMENT "This is the recommended games for each user"
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
ESCAPED BY '"'
STORED AS TEXTFILE tblproperties("skip.header.line.count"="1");

LOAD DATA INPATH './Game_Recommendation/game_recommended.csv' OVERWRITE INTO TABLE tbl_recommended_games;

SELECT count(*)
FROM tbl_recommended_games
WHERE g0 IS NULL;