# Operations

1. [Create Tables](#create-tables)
2. [Load Data](#load-data)
   1. [external tables](#external-tables)
   2. [exterbak table + static partition](#exterbak-table--static-partition)
   3. [internal tables](#internal-tables)

## Create Tables
```sql
create external table booking_csv(
    transaction_no int,
    user_id int,
    booking_date_id int,
    junk_booking_id int
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
location 'hdfs:///user/liu_osama/database/booking'
TBLPROPERTIES ("skip.header.line.count"="1");

create table booking_orc(
    transaction_no int,
    user_id int,
    booking_date_id int
)
PARTITIONED BY (junk_booking_id INT)
CLUSTERED BY (booking_date_id) INTO 10 buckets
STORED AS ORC;

--------------------------------------------

create external table chatting_csv(
    duration int,
    helpful int,
    experience int,
    response_time int,
    user_id int,
    starting_time_id int,
    junk_chat_id int,
    employee_id int,
    chat_date_id int,
    ending_time_id int
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
location 'hdfs:///user/liu_osama/database/chatting'
TBLPROPERTIES ("skip.header.line.count"="1");

create table chatting_orc(
    user_id int,
    rate STRUCT<helpful: int, experience: int>,
    duration int,
    response_time int,
    junk_chat_id int,
    chat_date_id int,
    starting_time_id int,
    ending_time_id int
)
PARTITIONED BY (employee_id INT)
CLUSTERED BY (chat_date_id) INTO 5 buckets
STORED AS ORC;

-----------------------------------------

create external table date_csv(
    date_id int,
    dt date,
    year int,
    month string,
    month_no int,
    day_of_week string,
    day_of_week_no int
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
location 'hdfs:///user/liu_osama/database/date'
TBLPROPERTIES ("skip.header.line.count"="1");

create table date_par(
    date_id int,
    dt date,
    year int,
    month string,
    month_no int,
    day_of_week string,
    day_of_week_no int
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS PARQUET;

------------------------------------------

-- External table
create external table employee_ext(
    employee_id int,
    profile struct<name_and_surname:string,sex:char(1),pesel_bk:string>,
    age_category string,
    is_current boolean
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
COLLECTION ITEMS TERMINATED BY '|'
location 'hdfs:///user/liu_osama/database/employee'
TBLPROPERTIES ("skip.header.line.count"="1");

-------------------------------------------

-- External table
create external table junk_booking_ext(
    Junk_booking_ID int,
    channel char(3),
    is_successful boolean,
    is_employee_needed boolean
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
location 'hdfs:///user/liu_osama/database/junk_booking'
TBLPROPERTIES ("skip.header.line.count"="1");

-------------------------------------------

-- External table
create external table junk_chat_ext(
    Junk_chat_ID int,
    is_call_service_needed boolean,
    is_problem_solved boolean
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
location 'hdfs:///user/liu_osama/database/junk_chat'
TBLPROPERTIES ("skip.header.line.count"="1");

--------------------------------------------

-- External table
create external table promo_code_ext(
    Promo_code_ID int,
    category_discount struct<category:string, dicount:int, conditions:array<string>>,
    is_used boolean
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
COLLECTION ITEMS TERMINATED BY '|'
location 'hdfs:///user/liu_osama/database/promo_code'
TBLPROPERTIES ("skip.header.line.count"="1");

--------------------------------------------

-- external + static partition
create external table signing_in_ext(
    discount int,
    sign_in_date_id int,
    promo_code_id int,
    user_id int
)
PARTITIONED BY (year INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
COLLECTION ITEMS TERMINATED BY '|'
location 'hdfs:///user/liu_osama/database/signing_in';

--------------------------------------------

create external table time_csv(
    time_id int,
    hours int,
    minutes int,
    seconds int,
    time_of_day string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
location 'hdfs:///user/liu_osama/database/time'
TBLPROPERTIES ("skip.header.line.count"="1");

create table time_par(
    time_id int,
    hours int,
    minutes int,
    seconds int,
    time_of_day string
)
STORED AS PARQUET;

--------------------------------------------

create external table user_csv(
    user_id int,
    profile struct<name_and_surname:string,sex:char(1),pesel_bk:string>,
    age_category string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
COLLECTION ITEMS TERMINATED BY '|'
location 'hdfs:///user/liu_osama/database/user'
TBLPROPERTIES ("skip.header.line.count"="1");

create external table user_par(
    user_id int,
    profile struct<name_and_surname:string,sex:char(1),pesel_bk:string>
)
PARTITIONED BY (age_category string)
CLUSTERED BY (user_id) INTO 5 buckets
STORED AS parquet;
```

## Load Data
### external tables
```bash
# external table
hdfs dfs -copyFromLocal ./* /user/liu_osama/database;
```

### exterbak table + static partition
```sql
-- exterbak table + static partition
ALTER TABLE signing_in_ext ADD PARTITION (year='2015') LOCATION '/user/liu_osama/database/signing_in/2015';
ALTER TABLE signing_in_ext ADD PARTITION (year='2016') LOCATION '/user/liu_osama/database/signing_in/2016';
ALTER TABLE signing_in_ext ADD PARTITION (year='2017') LOCATION '/user/liu_osama/database/signing_in/2017';
ALTER TABLE signing_in_ext ADD PARTITION (year='2018') LOCATION '/user/liu_osama/database/signing_in/2018';
ALTER TABLE signing_in_ext ADD PARTITION (year='2019') LOCATION '/user/liu_osama/database/signing_in/2019';
ALTER TABLE signing_in_ext ADD PARTITION (year='2020') LOCATION '/user/liu_osama/database/signing_in/2020';
ALTER TABLE signing_in_ext ADD PARTITION (year='2021') LOCATION '/user/liu_osama/database/signing_in/2021';
```

### internal tables
```sql
-- internal table
INSERT INTO TABLE booking_orc partition(junk_booking_id)
SELECT * FROM booking_csv;

INSERT INTO TABLE chatting_orc partition(employee_id)
SELECT user_id,named_struct("helpful",helpful,"experience",experience),duration,response_time,junk_chat_id,chat_date_id,starting_time_id,ending_time_id,employee_id FROM chatting_csv;

INSERT INTO TABLE date_par
SELECT * FROM date_csv;

INSERT INTO TABLE time_par
SELECT * FROM time_csv;

INSERT INTO TABLE user_par partition(age_category)
SELECT * FROM user_csv;
```