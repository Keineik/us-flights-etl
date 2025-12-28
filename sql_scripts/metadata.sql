-- =====================================================================
-- 1. DROP DATABASE NẾU CÓ
-- =====================================================================
USE tempdb;
GO
DECLARE @SQL AS NVARCHAR (1000);
IF EXISTS (SELECT 1
           FROM sys.databases
           WHERE [name] = N'FLIGHTS_Metadata')
    BEGIN
        SET @SQL = N'USE [FLIGHTS_Metadata];
                 ALTER DATABASE FLIGHTS_Metadata SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                 USE [tempdb];
                 DROP DATABASE FLIGHTS_Metadata;';
        EXECUTE (@SQL);
    END
GO

-- =====================================================================
-- 2. CREATE DATABASE VÀ TABLES
-- =====================================================================
CREATE DATABASE FLIGHTS_Metadata;
GO
USE FLIGHTS_Metadata;
GO

CREATE TABLE dbo.data_flow (
    Id				INT     		IDENTITY(1, 1) PRIMARY KEY,
    TableName		NVARCHAR(100)   NOT NULL,		
    LSET			DATETIME2		NULL,
	CET				DATETIME2		NULL
);
GO

USE FLIGHTS_METADATA;
GO

CREATE TABLE dbo.DQ_Rules (
    RuleKey             INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    
    -- Rule Details
    RuleName            NVARCHAR(100) NOT NULL,
    Description         NVARCHAR(500),
    IsActive            BIT DEFAULT 1,             -- Status: Active/Disabled
    
    -- Technical Mapping (Where to apply the rule)
    TargetTable         NVARCHAR(100) NOT NULL,    -- Table Key
    TargetColumn        NVARCHAR(100) NOT NULL,
    RuleExpression      NVARCHAR(MAX),             -- SQL or SSIS Expression logic
    
    -- Classification
    RuleType            NVARCHAR(20) NOT NULL,     -- 'Error' (Hard Stop) or 'Warning' (Info)
    RuleCategory        NVARCHAR(50) NOT NULL,     -- 'Incoming', 'Cross-Reference', 'Internal'
    
    -- Risk & Action
    RiskLevel           TINYINT CHECK (RiskLevel BETWEEN 1 AND 5), -- 1 (Low) to 5 (Critical)
    Action              NVARCHAR(20) NOT NULL,     -- 'Reject', 'Allow', 'Fix'
    
    -- Notification
    NotificationGroup   NVARCHAR(100),             -- e.g., 'DataStewards_Email_Group'
    
    -- Metadata
    CreatedDate         DATETIME2 DEFAULT GETDATE(),
    UpdatedDate         DATETIME2 DEFAULT GETDATE()
);
GO

--CREATE TABLE dbo.DQ_Logs (
--    LogKey              BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY, -- The Log Key
    
--    -- Foreign Keys
--    RuleKey             INT NOT NULL,              -- Link to DQ_Rules
--    ExecutionID         NVARCHAR(50),              -- Link to your ETL Batch/Execution ID
    
--    -- Event Details
--    DQTimestamp         DATETIME2 DEFAULT GETDATE(),
    
--    -- Action & Status Tracking
--    DQAction            NVARCHAR(20),              -- Action taken: 'Reject', 'Allow', 'Fix'
--    CorrectionStatus    NVARCHAR(20) DEFAULT 'Pending', -- 'Fixed', 'Not Fixed', 'Ignored'
    
--    -- Pointers to the Bad Data
--    TableName           NVARCHAR(100),             -- Table Key
--    RowBusinessKey      NVARCHAR(255),             -- Row Key (e.g., FlightDate + Airline + FlightNum)
--    AuditRecordID       BIGINT,                    -- Pointer to the specific row in DQ Audit Tables
    
--    CONSTRAINT FK_DQLog_Rule FOREIGN KEY (RuleKey) REFERENCES dbo.DQ_Rules(RuleKey)
--);
--GO

CREATE TABLE dbo.ETL_Execution_Log (
    -- 1. Identity Key (Used to link DQ Logs)
    LogID               BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    
    -- 2. SSIS System Metadata (Links back to SSISDB)
    ServerExecutionID   BIGINT,         -- System::ServerExecutionID (The ID in SSIS Catalog)
    PackageName         NVARCHAR(100),  -- System::PackageName
    MachineName         NVARCHAR(50),   -- System::MachineName
    UserName            NVARCHAR(50),   -- System::UserName
    
    -- 3. Timing
    StartTime           DATETIME2 DEFAULT GETDATE(),
    EndTime             DATETIME2 NULL,
    DurationSeconds     AS DATEDIFF(SECOND, StartTime, EndTime), -- Calculated Column
    
    -- 4. Status
    Status              NVARCHAR(20) DEFAULT 'Running', -- 'Running', 'Success', 'Failed'
    
    -- 5. Data Volume Metrics (Critical for checking data health)
    SourceRowCount      INT DEFAULT 0,
    InsertRowCount      INT DEFAULT 0,
    UpdateRowCount      INT DEFAULT 0,
    ErrorRowCount       INT DEFAULT 0,
    
    -- 6. Error Details (If failed)
    ErrorMessage        NVARCHAR(MAX)
);
GO

USE FLIGHTS_METADATA;
GO

-- =====================================================================
-- 2.1. AUDIT TABLE FOR AIRLINES
-- =====================================================================

CREATE TABLE dbo.DQ_Audit_STG_Airlines (
    -- MANDATORY AUDIT COLUMNS
    AuditRecordID       BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    DQLogID             BIGINT,             -- Links to the Log Event
    RuleID              INT,                -- Links to the Rule violated
    DQTimestamp         DATETIME2 DEFAULT GETDATE(),
    DQAction            NVARCHAR(50),       -- Reject, Allow, Fix
    DQStatus            NVARCHAR(50),       -- Fixed, Pending

    -- ORIGINAL DATA COLUMNS
    IATA_CODE           CHAR(2),
    AIRLINE             NVARCHAR(100)
);
GO

-- =====================================================================
-- 2.2. AUDIT TABLE FOR AIRPORTS
-- =====================================================================

CREATE TABLE dbo.DQ_Audit_STG_Airports (
    -- MANDATORY AUDIT COLUMNS
    AuditRecordID       BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    DQLogID             BIGINT,
    RuleID              INT,
    DQTimestamp         DATETIME2 DEFAULT GETDATE(),
    DQAction            NVARCHAR(50),
    DQStatus            NVARCHAR(50),

    -- ORIGINAL DATA COLUMNS
    IATA_CODE           CHAR(10),
    AIRPORT             NVARCHAR(100),
    CITY                NVARCHAR(100),
    [STATE]             CHAR(2),
    COUNTRY             NVARCHAR(50),
    LATITUDE            DECIMAL(12,9),
    LONGTITUDE          DECIMAL(12,9)
);
GO

-- =====================================================================
-- 2.3. AUDIT TABLE FOR FLIGHTS
-- =====================================================================

USE FLIGHTS_METADATA;
GO

CREATE TABLE dbo.DQ_Audit_STG_Flights (
    -- =========================================================
    -- 1. AUDIT METADATA (Quản lý lỗi)
    -- =========================================================
    AuditRecordID       BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    DQLogID             BIGINT,             -- Link đến lần chạy (Execution Log)
    RuleID              INT,                -- Link đến Rule bị vi phạm
    DQTimestamp         DATETIME2 DEFAULT GETDATE(),
    DQAction            NVARCHAR(50),       -- Reject / Fix / Allow
    DQStatus            NVARCHAR(50),       -- Pending / Fixed
    
    -- Cột này để phân biệt nguồn dữ liệu (VD: 'STG_Flights_1')
    SourceTableName     NVARCHAR(50),       

    -- =========================================================
    -- 2. ORIGINAL DATA (Dữ liệu gốc bị lỗi)
    -- =========================================================
    
    [DATE]              DATE,
    AIRLINE             CHAR(2),
    FLIGHT_NUMBER       INT,
    TAIL_NUMBER         VARCHAR(10),
    ORIGIN_AIRPORT      CHAR(10),
    DESTINATION_AIRPORT CHAR(10),
    SCHEDULED_DEPARTURE CHAR(10),
    DEPARTURE_TIME      CHAR(10),
    DEPARTURE_DELAY     FLOAT,
    TAXI_OUT            FLOAT,
    WHEELS_OFF          CHAR(10),
    SCHEDULED_TIME      FLOAT,
    ELAPSED_TIME        FLOAT,
    AIR_TIME            FLOAT,
    DISTANCE            FLOAT,
    WHEELS_ON           CHAR(10),
    TAXI_IN             FLOAT,
    SCHEDULED_ARRIVAL   CHAR(10),
    ARRIVAL_TIME        CHAR(10),
    ARRIVAL_DELAY       FLOAT,
    DIVERTED            INT,
    CANCELLED           INT,
    CANCELLATION_REASON VARCHAR(1),
    AIR_SYSTEM_DELAY    FLOAT,
    SECURITY_DELAY      FLOAT,
    AIRLINE_DELAY       FLOAT,
    LATE_AIRCRAFT_DELAY FLOAT,
    WEATHER_DELAY       FLOAT,
    
    -- Metadata từ Source
    CREATED             DATETIME2,
    MODIFIED            DATETIME2,
    
    -- Ghi chú thêm về lỗi (nếu cần mở rộng sau này)
    ErrorMessage        NVARCHAR(MAX)
);
GO

--CREATE TABLE dbo.DQ_Audit_STG_Flights_1 (
--    -- MANDATORY AUDIT COLUMNS
--    AuditRecordID       BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
--    DQLogID             BIGINT,
--    RuleID              INT,
--    DQTimestamp         DATETIME2 DEFAULT GETDATE(),
--    DQAction            NVARCHAR(50),
--    DQStatus            NVARCHAR(50),

--    -- ORIGINAL DATA COLUMNS
--    [DATE]              DATE,
--    AIRLINE             CHAR(2),
--    FLIGHT_NUMBER       INT,
--    TAIL_NUMBER         VARCHAR(10),
--    ORIGIN_AIRPORT      CHAR(10),
--    DESTINATION_AIRPORT CHAR(10),
--    SCHEDULED_DEPARTURE CHAR(10),
--    DEPARTURE_TIME      CHAR(10),
--    DEPARTURE_DELAY     FLOAT,
--    TAXI_OUT            FLOAT,
--    WHEELS_OFF          CHAR(10),
--    SCHEDULED_TIME      FLOAT,
--    ELAPSED_TIME        FLOAT,
--    AIR_TIME            FLOAT,
--    DISTANCE            FLOAT,
--    WHEELS_ON           CHAR(10),
--    TAXI_IN             FLOAT,
--    SCHEDULED_ARRIVAL   CHAR(10),
--    ARRIVAL_TIME        CHAR(10),
--    ARRIVAL_DELAY       FLOAT,
--    DIVERTED            INT,
--    CANCELLED           INT,
--    CANCELLATION_REASON VARCHAR(1),
--    AIR_SYSTEM_DELAY    FLOAT,
--    SECURITY_DELAY      FLOAT,
--    AIRLINE_DELAY       FLOAT,
--    LATE_AIRCRAFT_DELAY FLOAT,
--    WEATHER_DELAY       FLOAT,
--    CREATED             DATETIME2,
--    MODIFIED            DATETIME2
--);
--GO

---- =====================================================================
---- 2.4. AUDIT TABLE FOR FLIGHTS_2
---- =====================================================================

--CREATE TABLE dbo.DQ_Audit_STG_Flights_2 (
--    -- MANDATORY AUDIT COLUMNS
--    AuditRecordID       BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
--    DQLogID             BIGINT,
--    RuleID              INT,
--    DQTimestamp         DATETIME2 DEFAULT GETDATE(),
--    DQAction            NVARCHAR(50),
--    DQStatus            NVARCHAR(50),

--    -- ORIGINAL DATA COLUMNS
--    [DATE]              DATE,
--    AIRLINE             CHAR(2),
--    FLIGHT_NUMBER       INT,
--    TAIL_NUMBER         VARCHAR(10),
--    ORIGIN_AIRPORT      CHAR(10),
--    DESTINATION_AIRPORT CHAR(10),
--    SCHEDULED_DEPARTURE CHAR(10),
--    DEPARTURE_TIME      CHAR(10),
--    DEPARTURE_DELAY     FLOAT,
--    TAXI_OUT            FLOAT,
--    WHEELS_OFF          CHAR(10),
--    SCHEDULED_TIME      FLOAT,
--    ELAPSED_TIME        FLOAT,
--    AIR_TIME            FLOAT,
--    DISTANCE            FLOAT,
--    WHEELS_ON           CHAR(10),
--    TAXI_IN             FLOAT,
--    SCHEDULED_ARRIVAL   CHAR(10),
--    ARRIVAL_TIME        CHAR(10),
--    ARRIVAL_DELAY       FLOAT,
--    DIVERTED            INT,
--    CANCELLED           INT,
--    CANCELLATION_REASON VARCHAR(1),
--    AIR_SYSTEM_DELAY    FLOAT,
--    SECURITY_DELAY      FLOAT,
--    AIRLINE_DELAY       FLOAT,
--    LATE_AIRCRAFT_DELAY FLOAT,
--    WEATHER_DELAY       FLOAT,
--    CREATED             DATETIME2,
--    MODIFIED            DATETIME2
--);
--GO

---- =====================================================================
---- 2.5. AUDIT TABLE FOR FLIGHTS_3
---- =====================================================================

--CREATE TABLE dbo.DQ_Audit_STG_Flights_3 (
--    -- MANDATORY AUDIT COLUMNS
--    AuditRecordID       BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
--    DQLogID             BIGINT,
--    RuleID              INT,
--    DQTimestamp         DATETIME2 DEFAULT GETDATE(),
--    DQAction            NVARCHAR(50),
--    DQStatus            NVARCHAR(50),

--    -- ORIGINAL DATA COLUMNS
--    [DATE]              DATE,
--    AIRLINE             CHAR(2),
--    FLIGHT_NUMBER       INT,
--    TAIL_NUMBER         VARCHAR(10),
--    ORIGIN_AIRPORT      CHAR(10),
--    DESTINATION_AIRPORT CHAR(10),
--    SCHEDULED_DEPARTURE CHAR(10),
--    DEPARTURE_TIME      CHAR(10),
--    DEPARTURE_DELAY     FLOAT,
--    TAXI_OUT            FLOAT,
--    WHEELS_OFF          CHAR(10),
--    SCHEDULED_TIME      FLOAT,
--    ELAPSED_TIME        FLOAT,
--    AIR_TIME            FLOAT,
--    DISTANCE            FLOAT,
--    WHEELS_ON           CHAR(10),
--    TAXI_IN             FLOAT,
--    SCHEDULED_ARRIVAL   CHAR(10),
--    ARRIVAL_TIME        CHAR(10),
--    ARRIVAL_DELAY       FLOAT,
--    DIVERTED            INT,
--    CANCELLED           INT,
--    CANCELLATION_REASON VARCHAR(1),
--    AIR_SYSTEM_DELAY    FLOAT,
--    SECURITY_DELAY      FLOAT,
--    AIRLINE_DELAY       FLOAT,
--    LATE_AIRCRAFT_DELAY FLOAT,
--    WEATHER_DELAY       FLOAT,
--    CREATED             DATETIME2,
--    MODIFIED            DATETIME2
--);
--GO

-- =====================================================================
-- 3. INSERT DỮ LIỆU KHỞI TẠO
-- =====================================================================
INSERT INTO dbo.data_flow (TableName, LSET, CET) VALUES
('Flights_1', '1900-10-01 00:00:00', NULL),
('Flights_2', '1900-10-01 00:00:00', NULL),
('Flights_3', '1900-10-01 00:00:00', NULL),
('NDS_Flights', '1900-10-01 00:00:00', NULL),
('NDS_Airlines', '1900-10-01 00:00:00', NULL),
('NDS_Airports', '1900-10-01 00:00:00', NULL);


INSERT INTO dbo.DQ_Rules (RuleName, Description, TargetTable, TargetColumn, RuleType, RuleCategory, RiskLevel, Action, RuleExpression, NotificationGroup) VALUES 
('Ref_Airline_Check', 'Airline code must exist in NDS.', 'STG_Flights_1', 'AIRLINE', 'Error', 'Cross-Reference', 5, 'Reject', 'AirlineSK IS NULL', 'ETL_Admins'), 
('Negative_Distance', 'Distance cannot be negative.', 'STG_Flights_1', 'DISTANCE', 'Warning', 'Incoming', 3, 'Fix', 'DISTANCE < 0', 'Data_Stewards'), 
('Missing_Dep_Delay', 'Departure Delay is NULL.', 'STG_Flights_1', 'DEPARTURE_DELAY', 'Warning', 'Incoming', 1, 'Allow', 'DEPARTURE_DELAY IS NULL', NULL);
GO

INSERT INTO dbo.DQ_Rules 
(RuleName, Description, TargetTable, TargetColumn, RuleExpression, RuleType, RiskLevel, Action)
VALUES 

-- =======================================================
-- BẢNG AIRLINES (3 Rules)
-- =======================================================

-- Rule 1: Sống còn - Khóa chính không được thiếu
('AL_Check_Missing_Key', 
 'AirlineNK (IATA Code) is mandatory. Cannot be NULL.', 
 'Airlines', 'AirlineNK', 
 'ISNULL([AirlineNK]) || TRIM([AirlineNK]) == ""', 
 'Error', 5, 'Reject'),

-- Rule 2: Định dạng - Mã hãng bay phải đúng 2 ký tự (VD: VN, AA)
('AL_Check_Format', 
 'Airline Code must be exactly 2 characters.', 
 'Airlines', 'AirlineNK', 
 'LEN(TRIM([AirlineNK])) != 2', 
 'Error', 4, 'Reject'),

-- Rule 3: Làm sạch - Thiếu tên thì gán "Unknown" để không mất dữ liệu
('AL_Fix_Missing_Name', 
 'If Name is missing, replace with "Unknown Airline".', 
 'Airlines', 'AirlineName', 
 'ISNULL([AirlineName]) || TRIM([AirlineName]) == ""', 
 'Warning', 2, 'Fix'),

-- =======================================================
-- BẢNG AIRPORTS (3 Rules)
-- =======================================================

-- Rule 4: Sống còn - Khóa sân bay không được thiếu
('AP_Check_Missing_Key', 
 'AirportNK (IATA Code) is mandatory. Cannot be NULL.', 
 'Airports', 'AirportNK', 
 'ISNULL([AirportNK]) || TRIM([AirportNK]) == ""', 
 'Error', 5, 'Reject'),

-- Rule 5: Định dạng - Mã sân bay phải đúng 3 ký tự (VD: SGN, HAN)
('AP_Check_Format', 
 'Airport Code must be exactly 3 characters.', 
 'Airports', 'AirportNK', 
 'LEN(TRIM([AirportNK])) != 3', 
 'Error', 4, 'Reject'),

-- Rule 6: Logic - Tọa độ sai (Vĩ độ quá 90 hoặc Kinh độ quá 180)
('AP_Check_Coordinates', 
 'Latitude/Longitude out of valid geographic range.', 
 'Airports', 'Latitude', 
 '[Latitude] < -90 || [Latitude] > 90 || [Longitude] < -180 || [Longitude] > 180', 
 'Error', 5, 'Reject');
GO
-- =====================================================================
-- 4. Data Quality Firewall
-- =====================================================================

CREATE OR ALTER PROCEDURE dbo.USP_Firewall_Flights_To_NDS
    @ExecutionID NVARCHAR(50) -- Passed from SSIS or Job Scheduler
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @LogID BIGINT;
    DECLARE @SourceSK INT;
    DECLARE @RuleID_Reject INT, @RuleID_Fix INT, @RuleID_Allow INT;

    -- Get Rule IDs for logging
    SELECT @RuleID_Reject = RuleKey FROM dbo.DQ_Rules WHERE RuleName = 'Ref_Airline_Check';
    SELECT @RuleID_Fix    = RuleKey FROM dbo.DQ_Rules WHERE RuleName = 'Negative_Distance';
    SELECT @RuleID_Allow  = RuleKey FROM dbo.DQ_Rules WHERE RuleName = 'Missing_Dep_Delay';

    -- Get SourceSK for "Source 1" (Assuming this proc processes Source 1)
    SELECT TOP 1 @SourceSK = SourceSK FROM [FLIGHTS_NDS].dbo.SourceSystem WHERE SourceName = 'Source 1';
    IF @SourceSK IS NULL SET @SourceSK = 1; -- Fallback

    PRINT '1. Loading Data into Working Set...';

    -- ====================================================================
    -- 1. PREPARE WORKING SET
    -- Import Stage data into Temp Table and add placeholder columns for SKs
    -- ====================================================================
    IF OBJECT_ID('tempdb..#WorkingSet') IS NOT NULL DROP TABLE #WorkingSet;

    SELECT 
        -- Staging Columns
        [DATE], AIRLINE, FLIGHT_NUMBER, TAIL_NUMBER, ORIGIN_AIRPORT, DESTINATION_AIRPORT, 
        SCHEDULED_DEPARTURE, DEPARTURE_TIME, DEPARTURE_DELAY, TAXI_OUT, WHEELS_OFF, 
        SCHEDULED_TIME, ELAPSED_TIME, AIR_TIME, DISTANCE, WHEELS_ON, TAXI_IN, 
        SCHEDULED_ARRIVAL, ARRIVAL_TIME, ARRIVAL_DELAY, DIVERTED, CANCELLED, 
        CANCELLATION_REASON, AIR_SYSTEM_DELAY, SECURITY_DELAY, AIRLINE_DELAY, 
        LATE_AIRCRAFT_DELAY, WEATHER_DELAY,
        
        -- Derived/Lookup Columns
        CAST(NULL AS INT) AS AirlineSK,
        CAST(NULL AS INT) AS OriginAirportSK,
        CAST(NULL AS INT) AS DestAirportSK
    INTO #WorkingSet
    FROM [FLIGHTS_Stage].dbo.STG_Flights_1;

    -- ====================================================================
    -- 2. PERFORM LOOKUPS (Cross-Reference Pre-Check)
    -- Map Natural Keys (NK) to Surrogate Keys (SK) from NDS
    -- ====================================================================
    UPDATE W
    SET 
        W.AirlineSK       = A.AirlineSK,
        W.OriginAirportSK = O.AirportSK,
        W.DestAirportSK   = D.AirportSK
    FROM #WorkingSet W
    LEFT JOIN [FLIGHTS_NDS].dbo.Airlines A ON W.AIRLINE = A.AirlineNK
    LEFT JOIN [FLIGHTS_NDS].dbo.Airports O ON W.ORIGIN_AIRPORT = O.AirportNK
    LEFT JOIN [FLIGHTS_NDS].dbo.Airports D ON W.DESTINATION_AIRPORT = D.AirportNK;

    PRINT '2. Applying DQ Rules...';

    -- ====================================================================
    -- RULE 1: REJECT (Invalid Airline)
    -- Logic: If AirlineSK IS NULL (Lookup failed), Reject the row.
    -- ====================================================================
    
    -- A. Identify Violations
    SELECT * INTO #Violations_Reject FROM #WorkingSet WHERE AirlineSK IS NULL;

    -- B. Audit (Record bad data)
    INSERT INTO dbo.DQ_Audit_STG_Flights_1 
    (
        DQLogID, RuleID, DQTimestamp, DQAction, DQStatus,
        [DATE], AIRLINE, FLIGHT_NUMBER, TAIL_NUMBER, ORIGIN_AIRPORT, DESTINATION_AIRPORT, 
        SCHEDULED_DEPARTURE, DEPARTURE_TIME, DEPARTURE_DELAY, TAXI_OUT, WHEELS_OFF, 
        SCHEDULED_TIME, ELAPSED_TIME, AIR_TIME, DISTANCE, WHEELS_ON, TAXI_IN, 
        SCHEDULED_ARRIVAL, ARRIVAL_TIME, ARRIVAL_DELAY, DIVERTED, CANCELLED, 
        CANCELLATION_REASON, AIR_SYSTEM_DELAY, SECURITY_DELAY, AIRLINE_DELAY, 
        LATE_AIRCRAFT_DELAY, WEATHER_DELAY
    )
    SELECT 
        NULL, @RuleID_Reject, GETDATE(), 'Reject', 'Not Fixed',
        [DATE], AIRLINE, FLIGHT_NUMBER, TAIL_NUMBER, ORIGIN_AIRPORT, DESTINATION_AIRPORT, 
        SCHEDULED_DEPARTURE, DEPARTURE_TIME, DEPARTURE_DELAY, TAXI_OUT, WHEELS_OFF, 
        SCHEDULED_TIME, ELAPSED_TIME, AIR_TIME, DISTANCE, WHEELS_ON, TAXI_IN, 
        SCHEDULED_ARRIVAL, ARRIVAL_TIME, ARRIVAL_DELAY, DIVERTED, CANCELLED, 
        CANCELLATION_REASON, AIR_SYSTEM_DELAY, SECURITY_DELAY, AIRLINE_DELAY, 
        LATE_AIRCRAFT_DELAY, WEATHER_DELAY
    FROM #Violations_Reject;

    -- C. Log Summary
    INSERT INTO dbo.DQ_Logs (RuleKey, ExecutionID, DQTimestamp, DQAction, CorrectionStatus, TableName, RowBusinessKey)
    SELECT @RuleID_Reject, @ExecutionID, GETDATE(), 'Reject', 'Not Fixed', 'STG_Flights_1', CONCAT([DATE], '|', AIRLINE, '|', FLIGHT_NUMBER)
    FROM #Violations_Reject;

    -- D. Enforce Firewall (Remove from Processing)
    DELETE FROM #WorkingSet WHERE AirlineSK IS NULL;
    
    DROP TABLE #Violations_Reject;

    -- ====================================================================
    -- RULE 2: FIX (Negative Distance)
    -- Logic: If Distance < 0, Audit original, then Update to ABS(Distance).
    -- ====================================================================
    
    -- A. Identify Violations
    SELECT * INTO #Violations_Fix FROM #WorkingSet WHERE DISTANCE < 0;

    -- B. Audit (Record Original Bad Data)
    INSERT INTO dbo.DQ_Audit_STG_Flights_1 
    (
        DQLogID, RuleID, DQTimestamp, DQAction, DQStatus,
        [DATE], AIRLINE, FLIGHT_NUMBER, TAIL_NUMBER, ORIGIN_AIRPORT, DESTINATION_AIRPORT, 
        DISTANCE -- Key column involved
        -- (Can map other columns if needed for context, omitting for brevity in Fix logic)
    )
    SELECT 
        NULL, @RuleID_Fix, GETDATE(), 'Fix', 'Fixed',
        [DATE], AIRLINE, FLIGHT_NUMBER, TAIL_NUMBER, ORIGIN_AIRPORT, DESTINATION_AIRPORT, 
        DISTANCE
    FROM #Violations_Fix;

    -- C. Log Summary
    INSERT INTO dbo.DQ_Logs (RuleKey, ExecutionID, DQTimestamp, DQAction, CorrectionStatus, TableName, RowBusinessKey)
    SELECT @RuleID_Fix, @ExecutionID, GETDATE(), 'Fix', 'Fixed', 'STG_Flights_1', CONCAT([DATE], '|', AIRLINE, '|', FLIGHT_NUMBER)
    FROM #Violations_Fix;

    -- D. Enforce Firewall (Apply Fix to WorkingSet)
    UPDATE #WorkingSet
    SET DISTANCE = ABS(DISTANCE)
    WHERE DISTANCE < 0;

    DROP TABLE #Violations_Fix;

    -- ====================================================================
    -- RULE 3: ALLOW (Missing Departure Delay)
    -- Logic: If Delay is NULL, Log it, but let it pass to NDS.
    -- ====================================================================

    -- A. Identify Violations
    SELECT * INTO #Violations_Allow FROM #WorkingSet WHERE DEPARTURE_DELAY IS NULL;

    -- B. Audit
    INSERT INTO dbo.DQ_Audit_STG_Flights_1 
    (
        DQLogID, RuleID, DQTimestamp, DQAction, DQStatus,
        [DATE], AIRLINE, FLIGHT_NUMBER, TAIL_NUMBER, DEPARTURE_DELAY
    )
    SELECT 
        NULL, @RuleID_Allow, GETDATE(), 'Allow', 'Not Fixed',
        [DATE], AIRLINE, FLIGHT_NUMBER, TAIL_NUMBER, DEPARTURE_DELAY
    FROM #Violations_Allow;

    -- C. Log Summary
    INSERT INTO dbo.DQ_Logs (RuleKey, ExecutionID, DQTimestamp, DQAction, CorrectionStatus, TableName, RowBusinessKey)
    SELECT @RuleID_Allow, @ExecutionID, GETDATE(), 'Allow', 'Not Fixed', 'STG_Flights_1', CONCAT([DATE], '|', AIRLINE, '|', FLIGHT_NUMBER)
    FROM #Violations_Allow;

    -- D. Enforce Firewall (No Action Needed on Data)
    DROP TABLE #Violations_Allow;

    PRINT '3. Inserting Clean Data into NDS...';

    -- ====================================================================
    -- 3. FINAL INSERT INTO NDS
    -- Map all columns from WorkingSet to NDS.Flights
    -- ====================================================================
    
    INSERT INTO [FLIGHTS_NDS].dbo.Flights
    (
        -- Keys
        AirlineSK, 
        OriginAirportSK, 
        DestAirportSK, 
        SourceSK,

        -- Identifiers
        FlightDate, 
        FlightNumber, 
        TailNumber, 

        -- Times
        ScheduledDeparture, 
        DepartureTime, 
        ScheduledArrival, 
        ArrivalTime, 
        WheelsOff, 
        WheelsOn, 

        -- Metrics
        DepartureDelay, 
        ArrivalDelay, 
        TaxiOut, 
        TaxiIn, 
        ScheduledTime, 
        ElapsedTime, 
        AirTime, 
        Distance, 

        -- Status
        Cancelled, 
        Diverted, 
        CancellationReason, 

        -- Delays
        AirSystemDelay, 
        SecurityDelay, 
        AirlineDelay, 
        LateAircraftDelay, 
        WeatherDelay
    )
    SELECT 
        -- Keys (Handled in Step 2, defaulting to -1 if Lookup failed/missing but wasn't rejected)
        ISNULL(AirlineSK, -1),
        ISNULL(OriginAirportSK, -1),
        ISNULL(DestAirportSK, -1),
        @SourceSK,

        -- Identifiers
        [DATE], 
        FLIGHT_NUMBER, 
        TAIL_NUMBER, 

        -- Times (Direct Map)
        SCHEDULED_DEPARTURE, 
        DEPARTURE_TIME, 
        SCHEDULED_ARRIVAL, 
        ARRIVAL_TIME, 
        WHEELS_OFF, 
        WHEELS_ON, 

        -- Metrics (Direct Map)
        DEPARTURE_DELAY, 
        ARRIVAL_DELAY, 
        TAXI_OUT, 
        TAXI_IN, 
        SCHEDULED_TIME, 
        ELAPSED_TIME, 
        AIR_TIME, 
        DISTANCE, -- Validated & Fixed value

        -- Status
        CANCELLED, 
        DIVERTED, 
        CANCELLATION_REASON, 

        -- Delays
        AIR_SYSTEM_DELAY, 
        SECURITY_DELAY, 
        AIRLINE_DELAY, 
        LATE_AIRCRAFT_DELAY, 
        WEATHER_DELAY
    FROM #WorkingSet;

    -- Cleanup
    DROP TABLE #WorkingSet;

    PRINT 'Firewall Process Successfully Completed.';
END;
GO



select * from DQ_Audit_STG_Flights;
select * from ETL_Execution_Log;