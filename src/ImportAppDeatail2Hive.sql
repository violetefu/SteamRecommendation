```sh
hadoop fs -mkdir ./Game_Recommendation
hadoop fs -put ./steam_app_info.csv ./Game_Recommendation

hive
```

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
