-- ============================================================
-- STAYSPHERE HOTEL CHAIN OPERATIONS DATA PLATFORM
-- Snowflake Complete Backend SQL
-- Medallion Architecture: RAW -> VALIDATED -> CURATED
-- ============================================================

-- ============================================================
-- SECTION 0: DATABASE & SCHEMA SETUP
-- ============================================================

CREATE DATABASE IF NOT EXISTS STAYSPHERE;

CREATE SCHEMA IF NOT EXISTS STAYSPHERE.RAW;
CREATE SCHEMA IF NOT EXISTS STAYSPHERE.VALIDATED;
CREATE SCHEMA IF NOT EXISTS STAYSPHERE.CURATED;
CREATE SCHEMA IF NOT EXISTS STAYSPHERE.AUDIT;

USE DATABASE STAYSPHERE;

-- ============================================================
-- SECTION 1: AWS S3 STORAGE INTEGRATION + STAGE SETUP
-- ============================================================

-- Step 1: Creating Storage Integration (run as ACCOUNTADMIN)
--  run DESC INTEGRATION S3_INT and share the STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID with  AWS admin to update the IAM trust policy.

USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE STORAGE INTEGRATION S3_INT
  TYPE                      = EXTERNAL_STAGE
  STORAGE_PROVIDER          = S3
  ENABLED                   = TRUE
  STORAGE_AWS_ROLE_ARN      = 'arn:aws:iam::390844744820:role/hotel_chain_role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://hotel-chain-account-bucket/');

-- Retrieve IAM values to configure AWS trust policy
DESC INTEGRATION S3_INT;

-- Step 2: Grant integration usage to sysadmin / data engineer role
GRANT USAGE ON INTEGRATION S3_INT TO ROLE SYSADMIN;

USE DATABASE STAYSPHERE;
USE SCHEMA STAYSPHERE.RAW;

-- Step 3: Shared CSV file format
CREATE OR REPLACE FILE FORMAT STAYSPHERE.RAW.CSV_FORMAT
  TYPE              = 'CSV'
  FIELD_DELIMITER   = ','
  SKIP_HEADER       = 1
  TRIM_SPACE        = TRUE
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  NULL_IF           = ('NULL', 'null', '');

-- Step 4: External stage pointing to S3 bucket root
CREATE OR REPLACE STAGE STAYSPHERE.RAW.HOTEL_S3_STAGE
  URL               = 's3://hotel-chain-account-bucket/'
  STORAGE_INTEGRATION = S3_INT
  FILE_FORMAT       = STAYSPHERE.RAW.CSV_FORMAT;

-- Verifying stage & list files
LIST @STAYSPHERE.RAW.HOTEL_S3_STAGE;

-- ============================================================
-- SECTION 2: RAW LAYER TABLES (All 5 Domains)
-- ============================================================

-- 2.1 RAW Guests
CREATE OR REPLACE TABLE STAYSPHERE.RAW.GUESTS (
    guest_id          VARCHAR,
    name              VARCHAR,
    dob               VARCHAR,
    gender            VARCHAR,
    email             VARCHAR,
    phone             VARCHAR,
    address           VARCHAR,
    city              VARCHAR,
    country           VARCHAR,
    loyalty_tier      VARCHAR,
    registration_date VARCHAR
);
 
-- 2.2 Reservations  (9 columns = CSV columns)
CREATE OR REPLACE TABLE STAYSPHERE.RAW.RESERVATIONS (
    reservation_id    VARCHAR,
    guest_id          VARCHAR,
    room_id           VARCHAR,
    check_in_date     VARCHAR,
    check_out_date    VARCHAR,
    booking_channel   VARCHAR,
    booking_time      VARCHAR,
    cancellation_time VARCHAR,
    status            VARCHAR
);
 
-- 2.3 Rooms  (8 columns = CSV columns)
CREATE OR REPLACE TABLE STAYSPHERE.RAW.ROOMS (
    room_id    VARCHAR,
    hotel_id   VARCHAR,
    room_type  VARCHAR,
    floor      VARCHAR,
    capacity   VARCHAR,
    amenities  VARCHAR,
    status     VARCHAR,
    base_price VARCHAR
);
 
-- 2.4 Housekeeping  (9 columns = CSV columns)
CREATE OR REPLACE TABLE STAYSPHERE.RAW.HOUSEKEEPING (
    task_id             VARCHAR,
    room_id             VARCHAR,
    task_type           VARCHAR,
    assigned_staff      VARCHAR,
    scheduled_time      VARCHAR,
    start_time          VARCHAR,
    end_time            VARCHAR,
    issue_detected_flag VARCHAR,
    status              VARCHAR
);
 
-- 2.5 Billing  (9 columns = CSV columns)
CREATE OR REPLACE TABLE STAYSPHERE.RAW.BILLING (
    bill_id        VARCHAR,
    reservation_id VARCHAR,
    guest_id       VARCHAR,
    total_amount   VARCHAR,
    taxes          VARCHAR,
    discounts      VARCHAR,
    payment_mode   VARCHAR,
    payment_time   VARCHAR,
    is_flagged     VARCHAR
);
 
-- ============================================================
-- SECTION 3: SNOWPIPE AUTO INGESTION
-- Plain COPY INTO — no column mapping needed because
-- table columns now match CSV columns exactly
-- ============================================================
 
CREATE OR REPLACE PIPE STAYSPHERE.RAW.GUESTS_PIPE
  AUTO_INGEST = TRUE
AS
COPY INTO STAYSPHERE.RAW.GUESTS
FROM @STAYSPHERE.RAW.HOTEL_S3_STAGE/guests/
FILE_FORMAT = (FORMAT_NAME = STAYSPHERE.RAW.CSV_FORMAT);
 
CREATE OR REPLACE PIPE STAYSPHERE.RAW.RESERVATIONS_PIPE
  AUTO_INGEST = TRUE
AS
COPY INTO STAYSPHERE.RAW.RESERVATIONS
FROM @STAYSPHERE.RAW.HOTEL_S3_STAGE/reservations/
FILE_FORMAT = (FORMAT_NAME = STAYSPHERE.RAW.CSV_FORMAT);
 
CREATE OR REPLACE PIPE STAYSPHERE.RAW.ROOMS_PIPE
  AUTO_INGEST = TRUE
AS
COPY INTO STAYSPHERE.RAW.ROOMS
FROM @STAYSPHERE.RAW.HOTEL_S3_STAGE/rooms/
FILE_FORMAT = (FORMAT_NAME = STAYSPHERE.RAW.CSV_FORMAT);
 
CREATE OR REPLACE PIPE STAYSPHERE.RAW.HOUSEKEEPING_PIPE
  AUTO_INGEST = TRUE
AS
COPY INTO STAYSPHERE.RAW.HOUSEKEEPING
FROM @STAYSPHERE.RAW.HOTEL_S3_STAGE/housekeeping_maintenance/
FILE_FORMAT = (FORMAT_NAME = STAYSPHERE.RAW.CSV_FORMAT);
 
CREATE OR REPLACE PIPE STAYSPHERE.RAW.BILLING_PIPE
  AUTO_INGEST = TRUE
AS
COPY INTO STAYSPHERE.RAW.BILLING
FROM @STAYSPHERE.RAW.HOTEL_S3_STAGE/billing_payments/
FILE_FORMAT = (FORMAT_NAME = STAYSPHERE.RAW.CSV_FORMAT);
 
-- ============================================================
-- Get SQS ARN → configure S3 Event Notifications in AWS
-- ============================================================
SHOW PIPES IN SCHEMA STAYSPHERE.RAW;
 
-- Refresh pipes to backfill files already sitting in S3
ALTER PIPE STAYSPHERE.RAW.GUESTS_PIPE       REFRESH;
ALTER PIPE STAYSPHERE.RAW.RESERVATIONS_PIPE REFRESH;
ALTER PIPE STAYSPHERE.RAW.ROOMS_PIPE        REFRESH;
ALTER PIPE STAYSPHERE.RAW.HOUSEKEEPING_PIPE REFRESH;
ALTER PIPE STAYSPHERE.RAW.BILLING_PIPE      REFRESH;
 

-- View copy history per pipe
SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME    => 'STAYSPHERE.RAW.rooms',
    START_TIME    => DATEADD(HOURS, -24, CURRENT_TIMESTAMP())
));


-- See raw content of rooms fil

-- Then refresh the rooms pipe
ALTER PIPE STAYSPHERE.RAW.ROOMS_PIPE REFRESH;
-- Verify loaded data
SELECT * FROM STAYSPHERE.RAW.GUESTS;
SELECT * FROM STAYSPHERE.RAW.RESERVATIONS;
SELECT * FROM STAYSPHERE.RAW.ROOMS;
SELECT * FROM STAYSPHERE.RAW.HOUSEKEEPING;
SELECT * FROM STAYSPHERE.RAW.BILLING;

 
-- ============================================================
-- SECTION 4: AUDIT TABLES
-- ============================================================
 
CREATE OR REPLACE TABLE STAYSPHERE.AUDIT.LOAD_LOG (
    log_id       NUMBER AUTOINCREMENT PRIMARY KEY,
    domain       VARCHAR,
    total_rows   NUMBER,
    passed_rows  NUMBER,
    failed_rows  NUMBER,
    error_detail VARCHAR,
    batch_ts     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
 
CREATE OR REPLACE TABLE STAYSPHERE.AUDIT.DQ_EXCEPTIONS (
    exception_id NUMBER AUTOINCREMENT PRIMARY KEY,
    domain       VARCHAR,
    natural_key  VARCHAR,
    error_type   VARCHAR,  -- DEDUP | REF_INTEGRITY | TEMPORAL | VALUE
    error_detail VARCHAR,
    logged_at    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
 
-- ============================================================
-- SECTION 5: VALIDATED LAYER
-- Type cast + dedup on natural keys + DQ filters
-- Based on ACTUAL CSV values:
--   status      → 'Confirmed','Cancelled' (mixed case) → INITCAP
--   task_type   → 'cleaning','maintenance' (lowercase) → UPPER
--   is_flagged  → 'Y'/'N'                             → BOOLEAN
-- ============================================================
 
-- GUESTS: dedup on guest_id, valid email only
CREATE OR REPLACE TABLE STAYSPHERE.VALIDATED.GUESTS AS
SELECT
    guest_id,
    name,
    TRY_TO_DATE(dob, 'YYYY-MM-DD')              AS dob,
    gender,
    email,
    phone,
    address,
    city,
    country,
    loyalty_tier,
    TRY_TO_DATE(registration_date, 'YYYY-MM-DD') AS registration_date
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY guest_id ORDER BY guest_id) AS rn
    FROM STAYSPHERE.RAW.GUESTS
    WHERE guest_id IS NOT NULL AND TRIM(guest_id) != ''
) WHERE rn = 1
  AND email LIKE '%@%.%';
 
-- RESERVATIONS: dedup, checkout > checkin, no strict status filter
CREATE OR REPLACE TABLE STAYSPHERE.VALIDATED.RESERVATIONS AS
SELECT
    reservation_id,
    guest_id,
    room_id,
    TRY_TO_DATE(check_in_date,  'YYYY-MM-DD')  AS check_in_date,
    TRY_TO_DATE(check_out_date, 'YYYY-MM-DD')  AS check_out_date,
    booking_channel,
    TRY_TO_TIMESTAMP(booking_time)              AS booking_time,
    TRY_TO_TIMESTAMP(cancellation_time)         AS cancellation_time,
    INITCAP(TRIM(status))                       AS status  -- confirmed/CONFIRMED → Confirmed
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY reservation_id ORDER BY reservation_id) AS rn
    FROM STAYSPHERE.RAW.RESERVATIONS
    WHERE reservation_id IS NOT NULL AND TRIM(reservation_id) != ''
) WHERE rn = 1
  AND TRY_TO_DATE(check_in_date,  'YYYY-MM-DD') IS NOT NULL
  AND TRY_TO_DATE(check_out_date, 'YYYY-MM-DD') IS NOT NULL
  AND TRY_TO_DATE(check_out_date, 'YYYY-MM-DD') > TRY_TO_DATE(check_in_date, 'YYYY-MM-DD');
 
-- ROOMS: dedup, capacity > 0, price >= 0
CREATE OR REPLACE TABLE STAYSPHERE.VALIDATED.ROOMS AS
SELECT
    room_id,
    hotel_id,
    room_type,
    TRY_TO_NUMBER(floor)              AS floor,
    TRY_TO_NUMBER(capacity)           AS capacity,
    amenities,
    INITCAP(TRIM(status))             AS status,
    TRY_TO_DECIMAL(base_price, 10, 2) AS base_price
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY room_id ORDER BY room_id) AS rn
    FROM STAYSPHERE.RAW.ROOMS
    WHERE room_id IS NOT NULL AND TRIM(room_id) != ''
) WHERE rn = 1
  AND TRY_TO_NUMBER(capacity) > 0
  AND TRY_TO_DECIMAL(base_price, 10, 2) >= 0;
 
-- HOUSEKEEPING: dedup, task_type UPPER (cleaning→CLEANING), Y/N→BOOLEAN
CREATE OR REPLACE TABLE STAYSPHERE.VALIDATED.HOUSEKEEPING AS
SELECT
    task_id,
    room_id,
    UPPER(TRIM(task_type))           AS task_type,  -- cleaning → CLEANING
    assigned_staff,
    TRY_TO_TIMESTAMP(scheduled_time) AS scheduled_time,
    TRY_TO_TIMESTAMP(start_time)     AS start_time,
    TRY_TO_TIMESTAMP(end_time)       AS end_time,
    CASE WHEN UPPER(TRIM(issue_detected_flag)) = 'Y' THEN TRUE ELSE FALSE END AS issue_detected_flag,
    INITCAP(TRIM(status))            AS status
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY task_id ORDER BY task_id) AS rn
    FROM STAYSPHERE.RAW.HOUSEKEEPING
    WHERE task_id IS NOT NULL AND TRIM(task_id) != ''
) WHERE rn = 1
  AND (TRY_TO_TIMESTAMP(end_time) IS NULL
    OR TRY_TO_TIMESTAMP(end_time) >= TRY_TO_TIMESTAMP(start_time));
 
-- BILLING: dedup, amounts >= 0, Y/N → BOOLEAN
CREATE OR REPLACE TABLE STAYSPHERE.VALIDATED.BILLING AS
SELECT
    bill_id,
    reservation_id,
    guest_id,
    TRY_TO_DECIMAL(total_amount, 12, 2) AS total_amount,
    TRY_TO_DECIMAL(taxes,        10, 2) AS taxes,
    TRY_TO_DECIMAL(discounts,    10, 2) AS discounts,
    payment_mode,
    TRY_TO_TIMESTAMP(payment_time)      AS payment_time,
    CASE WHEN UPPER(TRIM(is_flagged)) = 'Y' THEN TRUE ELSE FALSE END AS is_flagged
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY bill_id ORDER BY bill_id) AS rn
    FROM STAYSPHERE.RAW.BILLING
    WHERE bill_id IS NOT NULL AND TRIM(bill_id) != ''
) WHERE rn = 1
  AND TRY_TO_DECIMAL(total_amount, 12, 2) >= 0
  AND TRY_TO_DECIMAL(taxes,        10, 2) >= 0
  AND TRY_TO_DECIMAL(discounts,    10, 2) >= 0;
 
-- Log referential integrity exceptions
INSERT INTO STAYSPHERE.AUDIT.DQ_EXCEPTIONS (domain, natural_key, error_type, error_detail)
SELECT 'RESERVATIONS', reservation_id, 'REF_INTEGRITY', 'guest_id not in GUESTS'
FROM STAYSPHERE.VALIDATED.RESERVATIONS
WHERE guest_id NOT IN (SELECT guest_id FROM STAYSPHERE.VALIDATED.GUESTS);
 
INSERT INTO STAYSPHERE.AUDIT.DQ_EXCEPTIONS (domain, natural_key, error_type, error_detail)
SELECT 'HOUSEKEEPING', task_id, 'REF_INTEGRITY', 'room_id not in ROOMS'
FROM STAYSPHERE.VALIDATED.HOUSEKEEPING
WHERE room_id NOT IN (SELECT room_id FROM STAYSPHERE.VALIDATED.ROOMS);
 
SELECT * FROM STAYSPHERE.VALIDATED.GUESTS;
SELECT * FROM STAYSPHERE.VALIDATED.RESERVATIONS;
SELECT * FROM STAYSPHERE.VALIDATED.ROOMS;
SELECT * FROM STAYSPHERE.VALIDATED.HOUSEKEEPING;
SELECT * FROM STAYSPHERE.VALIDATED.BILLING;
 
-- ============================================================
-- SECTION 6: CURATED LAYER — KIMBALL DIMENSIONAL MODEL
-- ============================================================
 
-- DIM_HOTEL: distinct hotels from rooms
CREATE OR REPLACE TABLE STAYSPHERE.CURATED.DIM_HOTEL AS
SELECT DISTINCT hotel_id, 'Unknown' AS hotel_name, 'Unknown' AS region
FROM STAYSPHERE.VALIDATED.ROOMS;
 
-- DIM_GUEST: SCD-2 tracks loyalty_tier & contact changes
CREATE OR REPLACE TABLE STAYSPHERE.CURATED.DIM_GUEST (
    guest_sk          NUMBER AUTOINCREMENT PRIMARY KEY,
    guest_id          VARCHAR,
    name              VARCHAR,
    dob               DATE,
    gender            VARCHAR,
    email             VARCHAR,
    phone             VARCHAR,
    city              VARCHAR,
    country           VARCHAR,
    loyalty_tier      VARCHAR,
    registration_date DATE,
    effective_from    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    effective_to      TIMESTAMP_NTZ,
    is_current        BOOLEAN DEFAULT TRUE
);
 
INSERT INTO STAYSPHERE.CURATED.DIM_GUEST
    (guest_id,name,dob,gender,email,phone,city,country,loyalty_tier,registration_date)
SELECT guest_id,name,dob,gender,email,phone,city,country,loyalty_tier,registration_date
FROM STAYSPHERE.VALIDATED.GUESTS;
 
-- DIM_ROOM: SCD-2 tracks base_price & status changes
CREATE OR REPLACE TABLE STAYSPHERE.CURATED.DIM_ROOM (
    room_sk        NUMBER AUTOINCREMENT PRIMARY KEY,
    room_id        VARCHAR,
    hotel_id       VARCHAR,
    room_type      VARCHAR,
    floor          NUMBER,
    capacity       NUMBER,
    amenities      VARCHAR,
    status         VARCHAR,
    base_price     DECIMAL(10,2),
    effective_from TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    effective_to   TIMESTAMP_NTZ,
    is_current     BOOLEAN DEFAULT TRUE
);
 
INSERT INTO STAYSPHERE.CURATED.DIM_ROOM
    (room_id,hotel_id,room_type,floor,capacity,amenities,status,base_price)
SELECT room_id,hotel_id,room_type,floor,capacity,amenities,status,base_price
FROM STAYSPHERE.VALIDATED.ROOMS;
 
-- DIM_DATE: 10 years of dates for joining on date_sk (YYYYMMDD integer)
CREATE OR REPLACE TABLE STAYSPHERE.CURATED.DIM_DATE AS
WITH dates AS (
    SELECT DATEADD(DAY, SEQ4(), '2020-01-01'::DATE) AS dt
    FROM TABLE(GENERATOR(ROWCOUNT => 3650))
)
SELECT
    TO_NUMBER(TO_CHAR(dt,'YYYYMMDD')) AS date_sk,
    dt          AS full_date,
    YEAR(dt)    AS year,
    MONTH(dt)   AS month,
    DAY(dt)     AS day,
    QUARTER(dt) AS quarter,
    DAYNAME(dt) AS day_name,
    CASE WHEN DAYOFWEEK(dt) IN (1,7) THEN TRUE ELSE FALSE END AS is_weekend
FROM dates;
 
-- FACT_RESERVATION
CREATE OR REPLACE TABLE STAYSPHERE.CURATED.FACT_RESERVATION AS
SELECT
    r.reservation_id,
    g.guest_sk,
    rm.room_sk,
    TO_NUMBER(TO_CHAR(r.check_in_date,  'YYYYMMDD')) AS check_in_date_sk,
    TO_NUMBER(TO_CHAR(r.check_out_date, 'YYYYMMDD')) AS check_out_date_sk,
    r.booking_channel,
    r.booking_time,
    r.cancellation_time,
    r.status,
    DATEDIFF('day', r.check_in_date, r.check_out_date)        AS length_of_stay,
    CASE WHEN r.status = 'Cancelled' THEN TRUE ELSE FALSE END  AS is_cancelled,
    CASE WHEN r.status = 'No_Show'   THEN TRUE ELSE FALSE END  AS is_no_show
FROM STAYSPHERE.VALIDATED.RESERVATIONS r
LEFT JOIN STAYSPHERE.CURATED.DIM_GUEST g  ON g.guest_id = r.guest_id AND g.is_current = TRUE
LEFT JOIN STAYSPHERE.CURATED.DIM_ROOM  rm ON rm.room_id = r.room_id  AND rm.is_current = TRUE;
 
-- FACT_HOUSEKEEPING
CREATE OR REPLACE TABLE STAYSPHERE.CURATED.FACT_HOUSEKEEPING AS
SELECT
    h.task_id,
    rm.room_sk,
    h.task_type,
    h.assigned_staff,
    h.scheduled_time,
    h.start_time,
    h.end_time,
    DATEDIFF('minute', h.start_time, h.end_time) AS duration_minutes,
    h.issue_detected_flag,
    h.status
FROM STAYSPHERE.VALIDATED.HOUSEKEEPING h
LEFT JOIN STAYSPHERE.CURATED.DIM_ROOM rm ON rm.room_id = h.room_id AND rm.is_current = TRUE;
 
-- FACT_BILLING
CREATE OR REPLACE TABLE STAYSPHERE.CURATED.FACT_BILLING AS
SELECT
    b.bill_id,
    b.reservation_id,
    g.guest_sk,
    b.total_amount,
    b.taxes,
    b.discounts,
    b.total_amount - b.discounts AS net_revenue,
    b.payment_mode,
    b.payment_time,
    b.is_flagged
FROM STAYSPHERE.VALIDATED.BILLING b
LEFT JOIN STAYSPHERE.CURATED.DIM_GUEST g ON g.guest_id = b.guest_id AND g.is_current = TRUE;
 
SELECT * FROM STAYSPHERE.CURATED.DIM_HOTEL;
SELECT * FROM STAYSPHERE.CURATED.DIM_GUEST;
SELECT * FROM STAYSPHERE.CURATED.DIM_ROOM;
SELECT * FROM STAYSPHERE.CURATED.FACT_RESERVATION;
SELECT * FROM STAYSPHERE.CURATED.FACT_HOUSEKEEPING;
SELECT * FROM STAYSPHERE.CURATED.FACT_BILLING;
 
-- ============================================================
-- SECTION 7: STREAMS + INCREMENTAL MERGE TASK
-- ============================================================
 
CREATE OR REPLACE STREAM STAYSPHERE.RAW.STREAM_GUESTS       ON TABLE STAYSPHERE.RAW.GUESTS;
CREATE OR REPLACE STREAM STAYSPHERE.RAW.STREAM_RESERVATIONS ON TABLE STAYSPHERE.RAW.RESERVATIONS;
CREATE OR REPLACE STREAM STAYSPHERE.RAW.STREAM_ROOMS        ON TABLE STAYSPHERE.RAW.ROOMS;
CREATE OR REPLACE STREAM STAYSPHERE.RAW.STREAM_HOUSEKEEPING ON TABLE STAYSPHERE.RAW.HOUSEKEEPING;
CREATE OR REPLACE STREAM STAYSPHERE.RAW.STREAM_BILLING      ON TABLE STAYSPHERE.RAW.BILLING;
 
-- Fires every hour when any stream has new rows
CREATE OR REPLACE TASK STAYSPHERE.RAW.TASK_INCREMENTAL_MERGE
  WAREHOUSE = COMPUTE_WH
  SCHEDULE  = 'USING CRON 0 * * * * UTC'
WHEN
  SYSTEM$STREAM_HAS_DATA('STAYSPHERE.RAW.STREAM_GUESTS')
  OR SYSTEM$STREAM_HAS_DATA('STAYSPHERE.RAW.STREAM_RESERVATIONS')
  OR SYSTEM$STREAM_HAS_DATA('STAYSPHERE.RAW.STREAM_ROOMS')
  OR SYSTEM$STREAM_HAS_DATA('STAYSPHERE.RAW.STREAM_HOUSEKEEPING')
  OR SYSTEM$STREAM_HAS_DATA('STAYSPHERE.RAW.STREAM_BILLING')
AS
BEGIN
  MERGE INTO STAYSPHERE.VALIDATED.GUESTS tgt
  USING (
    SELECT guest_id, name, TRY_TO_DATE(dob,'YYYY-MM-DD') AS dob,
           gender, email, phone, address, city, country, loyalty_tier,
           TRY_TO_DATE(registration_date,'YYYY-MM-DD') AS registration_date
    FROM STAYSPHERE.RAW.STREAM_GUESTS
    WHERE METADATA$ACTION = 'INSERT' AND guest_id IS NOT NULL AND email LIKE '%@%.%'
  ) src ON tgt.guest_id = src.guest_id
  WHEN MATCHED     THEN UPDATE SET tgt.loyalty_tier = src.loyalty_tier, tgt.email = src.email
  WHEN NOT MATCHED THEN INSERT (guest_id,name,dob,gender,email,phone,address,city,country,loyalty_tier,registration_date)
                        VALUES (src.guest_id,src.name,src.dob,src.gender,src.email,src.phone,src.address,src.city,src.country,src.loyalty_tier,src.registration_date);
 
  MERGE INTO STAYSPHERE.VALIDATED.RESERVATIONS tgt
  USING (
    SELECT reservation_id, guest_id, room_id,
           TRY_TO_DATE(check_in_date,'YYYY-MM-DD')  AS check_in_date,
           TRY_TO_DATE(check_out_date,'YYYY-MM-DD') AS check_out_date,
           booking_channel, TRY_TO_TIMESTAMP(booking_time) AS booking_time,
           TRY_TO_TIMESTAMP(cancellation_time) AS cancellation_time,
           INITCAP(TRIM(status)) AS status
    FROM STAYSPHERE.RAW.STREAM_RESERVATIONS
    WHERE METADATA$ACTION = 'INSERT' AND reservation_id IS NOT NULL
      AND TRY_TO_DATE(check_out_date,'YYYY-MM-DD') > TRY_TO_DATE(check_in_date,'YYYY-MM-DD')
  ) src ON tgt.reservation_id = src.reservation_id
  WHEN MATCHED     THEN UPDATE SET tgt.status = src.status, tgt.cancellation_time = src.cancellation_time
  WHEN NOT MATCHED THEN INSERT (reservation_id,guest_id,room_id,check_in_date,check_out_date,booking_channel,booking_time,cancellation_time,status)
                        VALUES (src.reservation_id,src.guest_id,src.room_id,src.check_in_date,src.check_out_date,src.booking_channel,src.booking_time,src.cancellation_time,src.status);
 
  MERGE INTO STAYSPHERE.VALIDATED.ROOMS tgt
  USING (
    SELECT room_id, hotel_id, room_type, TRY_TO_NUMBER(floor) AS floor,
           TRY_TO_NUMBER(capacity) AS capacity, amenities,
           INITCAP(TRIM(status)) AS status, TRY_TO_DECIMAL(base_price,10,2) AS base_price
    FROM STAYSPHERE.RAW.STREAM_ROOMS
    WHERE METADATA$ACTION = 'INSERT' AND room_id IS NOT NULL AND TRY_TO_NUMBER(capacity) > 0
  ) src ON tgt.room_id = src.room_id
  WHEN MATCHED     THEN UPDATE SET tgt.status = src.status, tgt.base_price = src.base_price
  WHEN NOT MATCHED THEN INSERT (room_id,hotel_id,room_type,floor,capacity,amenities,status,base_price)
                        VALUES (src.room_id,src.hotel_id,src.room_type,src.floor,src.capacity,src.amenities,src.status,src.base_price);
 
  MERGE INTO STAYSPHERE.VALIDATED.HOUSEKEEPING tgt
  USING (
    SELECT task_id, room_id, UPPER(TRIM(task_type)) AS task_type, assigned_staff,
           TRY_TO_TIMESTAMP(scheduled_time) AS scheduled_time,
           TRY_TO_TIMESTAMP(start_time) AS start_time, TRY_TO_TIMESTAMP(end_time) AS end_time,
           CASE WHEN UPPER(TRIM(issue_detected_flag)) = 'Y' THEN TRUE ELSE FALSE END AS issue_detected_flag,
           INITCAP(TRIM(status)) AS status
    FROM STAYSPHERE.RAW.STREAM_HOUSEKEEPING
    WHERE METADATA$ACTION = 'INSERT' AND task_id IS NOT NULL
  ) src ON tgt.task_id = src.task_id
  WHEN MATCHED     THEN UPDATE SET tgt.status = src.status, tgt.end_time = src.end_time
  WHEN NOT MATCHED THEN INSERT (task_id,room_id,task_type,assigned_staff,scheduled_time,start_time,end_time,issue_detected_flag,status)
                        VALUES (src.task_id,src.room_id,src.task_type,src.assigned_staff,src.scheduled_time,src.start_time,src.end_time,src.issue_detected_flag,src.status);
 
  MERGE INTO STAYSPHERE.VALIDATED.BILLING tgt
  USING (
    SELECT bill_id, reservation_id, guest_id,
           TRY_TO_DECIMAL(total_amount,12,2) AS total_amount,
           TRY_TO_DECIMAL(taxes,10,2) AS taxes, TRY_TO_DECIMAL(discounts,10,2) AS discounts,
           payment_mode, TRY_TO_TIMESTAMP(payment_time) AS payment_time,
           CASE WHEN UPPER(TRIM(is_flagged)) = 'Y' THEN TRUE ELSE FALSE END AS is_flagged
    FROM STAYSPHERE.RAW.STREAM_BILLING
    WHERE METADATA$ACTION = 'INSERT' AND bill_id IS NOT NULL
      AND TRY_TO_DECIMAL(total_amount,12,2) >= 0
  ) src ON tgt.bill_id = src.bill_id
  WHEN MATCHED     THEN UPDATE SET tgt.is_flagged = src.is_flagged
  WHEN NOT MATCHED THEN INSERT (bill_id,reservation_id,guest_id,total_amount,taxes,discounts,payment_mode,payment_time,is_flagged)
                        VALUES (src.bill_id,src.reservation_id,src.guest_id,src.total_amount,src.taxes,src.discounts,src.payment_mode,src.payment_time,src.is_flagged);
END;
 
ALTER TASK STAYSPHERE.RAW.TASK_INCREMENTAL_MERGE RESUME;
 
-- ============================================================
-- SECTION 8: ANOMALY DETECTION STORED PROCEDURES
-- ============================================================
 
CREATE OR REPLACE TABLE STAYSPHERE.CURATED.ANOMALY_LOG (
    anomaly_id  NUMBER AUTOINCREMENT PRIMARY KEY,
    rule_name   VARCHAR,
    entity_id   VARCHAR,
    details     VARCHAR,
    detected_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
 
-- Rule 1: Double booking — same room, overlapping dates, not cancelled
CREATE OR REPLACE PROCEDURE STAYSPHERE.CURATED.SP_DOUBLE_BOOKING()
RETURNS VARCHAR LANGUAGE SQL AS
$$
BEGIN
  INSERT INTO STAYSPHERE.CURATED.ANOMALY_LOG (rule_name, entity_id, details)
  SELECT DISTINCT 'DOUBLE_BOOKING', r1.reservation_id,
    'Overlaps with ' || r2.reservation_id || ' on room_sk ' || r1.room_sk
  FROM STAYSPHERE.CURATED.FACT_RESERVATION r1
  JOIN STAYSPHERE.CURATED.FACT_RESERVATION r2
    ON  r1.room_sk = r2.room_sk AND r1.reservation_id != r2.reservation_id
    AND r1.is_cancelled = FALSE AND r2.is_cancelled = FALSE
    AND r1.check_in_date_sk  < r2.check_out_date_sk
    AND r1.check_out_date_sk > r2.check_in_date_sk;
  RETURN 'Done';
END;
$$;
 
-- Rule 2: HK SLA breach — cleaning task took more than 120 minutes
CREATE OR REPLACE PROCEDURE STAYSPHERE.CURATED.SP_HK_SLA_BREACH()
RETURNS VARCHAR LANGUAGE SQL AS
$$
BEGIN
  INSERT INTO STAYSPHERE.CURATED.ANOMALY_LOG (rule_name, entity_id, details)
  SELECT 'HK_SLA_BREACH', task_id,
    'Cleaning took ' || duration_minutes || ' mins (SLA: 120 mins)'
  FROM STAYSPHERE.CURATED.FACT_HOUSEKEEPING
  WHERE task_type = 'CLEANING' AND status = 'Completed' AND duration_minutes > 120;
  RETURN 'Done';
END;
$$;
 
-- Rule 3: Maintenance overdue — issue flagged but no follow-up within 24h
CREATE OR REPLACE PROCEDURE STAYSPHERE.CURATED.SP_MAINTENANCE_OVERDUE()
RETURNS VARCHAR LANGUAGE SQL AS
$$
BEGIN
  INSERT INTO STAYSPHERE.CURATED.ANOMALY_LOG (rule_name, entity_id, details)
  SELECT 'MAINTENANCE_OVERDUE', task_id,
    'Issue unresolved — overdue by ' || DATEDIFF('hour', end_time, CURRENT_TIMESTAMP()) || 'h'
  FROM STAYSPHERE.CURATED.FACT_HOUSEKEEPING
  WHERE issue_detected_flag = TRUE AND status = 'Completed'
    AND DATEDIFF('hour', end_time, CURRENT_TIMESTAMP()) > 24;
  RETURN 'Done';
END;
$$;
 
-- Rule 4: Billing mismatch — billed != (base_price × nights + taxes - discounts) by > $10
CREATE OR REPLACE PROCEDURE STAYSPHERE.CURATED.SP_BILLING_MISMATCH()
RETURNS VARCHAR LANGUAGE SQL AS
$$
BEGIN
  INSERT INTO STAYSPHERE.CURATED.ANOMALY_LOG (rule_name, entity_id, details)
  SELECT 'BILLING_MISMATCH', fb.bill_id,
    'Billed $' || fb.total_amount || ' vs expected $' ||
    ROUND(dr.base_price * fr.length_of_stay + fb.taxes - fb.discounts, 2)
  FROM STAYSPHERE.CURATED.FACT_BILLING fb
  JOIN STAYSPHERE.CURATED.FACT_RESERVATION fr ON fr.reservation_id = fb.reservation_id
  JOIN STAYSPHERE.CURATED.DIM_ROOM         dr ON dr.room_sk = fr.room_sk AND dr.is_current = TRUE
  WHERE ABS(fb.total_amount - (dr.base_price * fr.length_of_stay + fb.taxes - fb.discounts)) > 10;
  RETURN 'Done';
END;
$$;
 
-- Rule 5: Suspicious cancellation — same guest cancels > 3 times within 24h
CREATE OR REPLACE PROCEDURE STAYSPHERE.CURATED.SP_SUSPICIOUS_CANCELLATION()
RETURNS VARCHAR LANGUAGE SQL AS
$$
BEGIN
  INSERT INTO STAYSPHERE.CURATED.ANOMALY_LOG (rule_name, entity_id, details)
  SELECT 'SUSPICIOUS_CANCELLATION', guest_sk::VARCHAR,
    cnt || ' cancellations within 24h'
  FROM (
    SELECT guest_sk, COUNT(*) AS cnt,
           DATEDIFF('hour', MIN(cancellation_time), MAX(cancellation_time)) AS hr_span
    FROM STAYSPHERE.CURATED.FACT_RESERVATION
    WHERE is_cancelled = TRUE AND cancellation_time IS NOT NULL
    GROUP BY guest_sk HAVING COUNT(*) > 3 AND hr_span <= 24
  );
  RETURN 'Done';
END;
$$;
 
-- Run all anomaly checks
CALL STAYSPHERE.CURATED.SP_DOUBLE_BOOKING();
CALL STAYSPHERE.CURATED.SP_HK_SLA_BREACH();
CALL STAYSPHERE.CURATED.SP_MAINTENANCE_OVERDUE();
CALL STAYSPHERE.CURATED.SP_BILLING_MISMATCH();
CALL STAYSPHERE.CURATED.SP_SUSPICIOUS_CANCELLATION();
 
SELECT * FROM STAYSPHERE.CURATED.ANOMALY_LOG;
 

-- ============================================================
-- SECTION 9: KPI VIEWS  (5 required KPIs from the use case)
-- ============================================================
 
-- KPI 1: Room Occupancy Rate = booked rooms / total rooms x 100
CREATE OR REPLACE VIEW STAYSPHERE.CURATED.VW_KPI_OCCUPANCY AS
SELECT hotel_id, year, month, booked_rooms, total_rooms,
    ROUND(booked_rooms / NULLIF(total_rooms,0) * 100, 2) AS occupancy_rate_pct
FROM (
    SELECT dr.hotel_id, dd.year, dd.month,
        COUNT(DISTINCT CASE WHEN fr.status = 'Confirmed' THEN fr.reservation_id END) AS booked_rooms,
        COUNT(DISTINCT dr.room_sk) AS total_rooms
    FROM STAYSPHERE.CURATED.FACT_RESERVATION fr
    JOIN STAYSPHERE.CURATED.DIM_ROOM dr ON dr.room_sk = fr.room_sk AND dr.is_current = TRUE
    JOIN STAYSPHERE.CURATED.DIM_DATE dd ON dd.date_sk = fr.check_in_date_sk
    GROUP BY dr.hotel_id, dd.year, dd.month
);
 
-- KPI 2: Booking Conversion = confirmed / total reservations x 100
CREATE OR REPLACE VIEW STAYSPHERE.CURATED.VW_KPI_CONVERSION AS
SELECT hotel_id, year, month, total_reservations, confirmed, cancellations, no_shows,
    ROUND(confirmed / NULLIF(total_reservations,0) * 100, 2) AS conversion_pct
FROM (
    SELECT dr.hotel_id, dd.year, dd.month,
        COUNT(*) AS total_reservations,
        SUM(CASE WHEN fr.status = 'Confirmed' THEN 1 ELSE 0 END) AS confirmed,
        SUM(CASE WHEN fr.is_cancelled = TRUE  THEN 1 ELSE 0 END) AS cancellations,
        SUM(CASE WHEN fr.is_no_show   = TRUE  THEN 1 ELSE 0 END) AS no_shows
    FROM STAYSPHERE.CURATED.FACT_RESERVATION fr
    JOIN STAYSPHERE.CURATED.DIM_ROOM dr ON dr.room_sk = fr.room_sk AND dr.is_current = TRUE
    JOIN STAYSPHERE.CURATED.DIM_DATE dd ON dd.date_sk = fr.check_in_date_sk
    GROUP BY dr.hotel_id, dd.year, dd.month
);
 
-- KPI 3: HK SLA Compliance = tasks within 120 min / total tasks x 100
CREATE OR REPLACE VIEW STAYSPHERE.CURATED.VW_KPI_HK_SLA AS
SELECT hotel_id, task_type, year, month, total_tasks, within_sla,
    ROUND(within_sla / NULLIF(total_tasks,0) * 100, 2) AS sla_pct
FROM (
    SELECT dr.hotel_id, fh.task_type,
        YEAR(fh.scheduled_time)  AS year,
        MONTH(fh.scheduled_time) AS month,
        COUNT(*) AS total_tasks,
        SUM(CASE WHEN fh.duration_minutes <= 120 AND fh.status = 'Completed' THEN 1 ELSE 0 END) AS within_sla
    FROM STAYSPHERE.CURATED.FACT_HOUSEKEEPING fh
    JOIN STAYSPHERE.CURATED.DIM_ROOM dr ON dr.room_sk = fh.room_sk AND dr.is_current = TRUE
    GROUP BY dr.hotel_id, fh.task_type, YEAR(fh.scheduled_time), MONTH(fh.scheduled_time)
);
 
-- KPI 4: RevPAR = total net revenue / total available rooms
CREATE OR REPLACE VIEW STAYSPHERE.CURATED.VW_KPI_REVPAR AS
SELECT hotel_id, year, month, total_revenue, total_rooms,
    ROUND(total_revenue / NULLIF(total_rooms,0), 2) AS revpar
FROM (
    SELECT dr.hotel_id, dd.year, dd.month,
        SUM(fb.net_revenue)        AS total_revenue,
        COUNT(DISTINCT dr.room_sk) AS total_rooms
    FROM STAYSPHERE.CURATED.FACT_BILLING fb
    JOIN STAYSPHERE.CURATED.FACT_RESERVATION fr ON fr.reservation_id = fb.reservation_id
    JOIN STAYSPHERE.CURATED.DIM_ROOM         dr ON dr.room_sk = fr.room_sk AND dr.is_current = TRUE
    JOIN STAYSPHERE.CURATED.DIM_DATE         dd ON dd.date_sk = fr.check_in_date_sk
    GROUP BY dr.hotel_id, dd.year, dd.month
);
 
-- KPI 5: Billing Accuracy Index = 1 - (flagged bills / total bills)
CREATE OR REPLACE VIEW STAYSPHERE.CURATED.VW_KPI_BILLING_ACCURACY AS
SELECT year, month, total_bills, flagged_bills,
    ROUND(1 - flagged_bills / NULLIF(total_bills,0), 4) AS accuracy_index
FROM (
    SELECT YEAR(payment_time)  AS year,
           MONTH(payment_time) AS month,
           COUNT(*) AS total_bills,
           SUM(CASE WHEN is_flagged = TRUE THEN 1 ELSE 0 END) AS flagged_bills
    FROM STAYSPHERE.CURATED.FACT_BILLING
    WHERE payment_time IS NOT NULL
    GROUP BY YEAR(payment_time), MONTH(payment_time)
);
 
SELECT * FROM STAYSPHERE.CURATED.VW_KPI_OCCUPANCY;
SELECT * FROM STAYSPHERE.CURATED.VW_KPI_CONVERSION;
SELECT * FROM STAYSPHERE.CURATED.VW_KPI_HK_SLA;
SELECT * FROM STAYSPHERE.CURATED.VW_KPI_REVPAR;
SELECT * FROM STAYSPHERE.CURATED.VW_KPI_BILLING_ACCURACY;
 
-- ============================================================
-- SECTION 10: SECURITY — DYNAMIC DATA MASKING (PII)
-- Email and phone visible only to ACCOUNTADMIN
-- ============================================================
 
CREATE OR REPLACE MASKING POLICY STAYSPHERE.RAW.MASK_EMAIL
  AS (val VARCHAR) RETURNS VARCHAR ->
  CASE WHEN CURRENT_ROLE() = 'ACCOUNTADMIN' THEN val
       ELSE REGEXP_REPLACE(val,'(.{2}).+(@.+)','\\1***\\2') END;
 
CREATE OR REPLACE MASKING POLICY STAYSPHERE.RAW.MASK_PHONE
  AS (val VARCHAR) RETURNS VARCHAR ->
  CASE WHEN CURRENT_ROLE() = 'ACCOUNTADMIN' THEN val
       ELSE 'XXXXXX' || RIGHT(val,4) END;
 
ALTER TABLE STAYSPHERE.CURATED.DIM_GUEST MODIFY COLUMN email SET MASKING POLICY STAYSPHERE.RAW.MASK_EMAIL;
ALTER TABLE STAYSPHERE.CURATED.DIM_GUEST MODIFY COLUMN phone SET MASKING POLICY STAYSPHERE.RAW.MASK_PHONE;
 
-- ============================================================
-- SECTION 11: DAILY ANOMALY TASK (2am UTC every day)
-- ============================================================
 
CREATE OR REPLACE TASK STAYSPHERE.CURATED.TASK_DAILY_ANOMALY
  WAREHOUSE = COMPUTE_WH
  SCHEDULE  = 'USING CRON 0 2 * * * UTC'
AS
BEGIN
  CALL STAYSPHERE.CURATED.SP_DOUBLE_BOOKING();
  CALL STAYSPHERE.CURATED.SP_HK_SLA_BREACH();
  CALL STAYSPHERE.CURATED.SP_MAINTENANCE_OVERDUE();
  CALL STAYSPHERE.CURATED.SP_BILLING_MISMATCH();
  CALL STAYSPHERE.CURATED.SP_SUSPICIOUS_CANCELLATION();
END;
 
ALTER TASK STAYSPHERE.CURATED.TASK_DAILY_ANOMALY RESUME;
 
-- ============================================================
-- ROW COUNT CHECK — run after every full load to verify
-- ============================================================
SELECT 'RAW GUESTS'        AS layer, COUNT(*) AS cnt FROM STAYSPHERE.RAW.GUESTS
UNION ALL SELECT 'RAW RESERVATIONS'  , COUNT(*) FROM STAYSPHERE.RAW.RESERVATIONS
UNION ALL SELECT 'RAW ROOMS'         , COUNT(*) FROM STAYSPHERE.RAW.ROOMS
UNION ALL SELECT 'RAW HOUSEKEEPING'  , COUNT(*) FROM STAYSPHERE.RAW.HOUSEKEEPING
UNION ALL SELECT 'RAW BILLING'       , COUNT(*) FROM STAYSPHERE.RAW.BILLING
UNION ALL SELECT 'VAL GUESTS'        , COUNT(*) FROM STAYSPHERE.VALIDATED.GUESTS
UNION ALL SELECT 'VAL RESERVATIONS'  , COUNT(*) FROM STAYSPHERE.VALIDATED.RESERVATIONS
UNION ALL SELECT 'VAL ROOMS'         , COUNT(*) FROM STAYSPHERE.VALIDATED.ROOMS
UNION ALL SELECT 'VAL HOUSEKEEPING'  , COUNT(*) FROM STAYSPHERE.VALIDATED.HOUSEKEEPING
UNION ALL SELECT 'VAL BILLING'       , COUNT(*) FROM STAYSPHERE.VALIDATED.BILLING
UNION ALL SELECT 'FACT RESERVATION'  , COUNT(*) FROM STAYSPHERE.CURATED.FACT_RESERVATION
UNION ALL SELECT 'FACT HOUSEKEEPING' , COUNT(*) FROM STAYSPHERE.CURATED.FACT_HOUSEKEEPING
UNION ALL SELECT 'FACT BILLING'      , COUNT(*) FROM STAYSPHERE.CURATED.FACT_BILLING;
-- ============================================================
-- END
-- ============================================================