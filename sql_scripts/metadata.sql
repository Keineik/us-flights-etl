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


INSERT INTO dbo.DQ_Rules 
(RuleName, Description, TargetTable, TargetColumn, RuleType, RuleCategory, RiskLevel, Action, RuleExpression, NotificationGroup) 
VALUES 
-- 1. Check Reference (Lookup)
('Ref_Airline_Check', 
 'Airline code must exist in NDS.', 
 'STG_Flights_1', 'AIRLINE', 
 'Error', 'Cross-Reference', 5, 'Reject', 
 'AirlineSK IS NULL', 'ETL_Admins'),

-- 2. Fix Distance Âm
('Negative_Distance', 
 'Distance cannot be negative.', 
 'STG_Flights_1', 'DISTANCE', 
 'Warning', 'Incoming', 3, 'Fix', 
 'DISTANCE < 0', 'Data_Stewards'),

-- 3. Allow Delay Null
('Missing_Dep_Delay', 
 'Departure Delay is NULL.', 
 'STG_Flights_1', 'DEPARTURE_DELAY', 
 'Warning', 'Incoming', 1, 'Allow', 
 'DEPARTURE_DELAY IS NULL', NULL),

-- 4. Logic Thời gian (Air Time > Elapsed Time là vô lý)
('Logic_AirTime_Invalid', 
 'Air Time cannot be greater than Actual Elapsed Time.', 
 'STG_Flights_1', 'AIR_TIME', 
 'Error', 'Validity', 4, 'Reject', 
 '[AIR_TIME] > [ELAPSED_TIME]', 'Data_Stewards'),

-- 5. Chuyến bay Ma (Không Cancel mà lại không có giờ bay)
('Logic_Ghost_Flight', 
 'Flight is not Cancelled but Departure Time is missing.', 
 'STG_Flights_1', 'DEPARTURE_TIME', 
 'Error', 'Validity', 5, 'Reject', 
 '[CANCELLED] == 0 && ISNULL([DEPARTURE_TIME])', 'ETL_Admins');
GO

select * from DQ_Audit_STG_Flights;
select * from ETL_Execution_Log;