-- =====================================================================
-- 1. SETUP DATABASE
-- =====================================================================
USE tempdb;
GO

IF EXISTS (SELECT 1 FROM sys.databases WHERE [name] = N'FLIGHTS_DDS')
BEGIN
    ALTER DATABASE FLIGHTS_DDS SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE FLIGHTS_DDS;
END
GO

CREATE DATABASE FLIGHTS_DDS;
GO
USE FLIGHTS_DDS;
GO

-- =====================================================================
-- 2. CREATE DIMENSIONS
-- =====================================================================

-- 2.1 DIMENSION: DATE (Ngày tháng năm)
CREATE TABLE dbo.DimDate (
    DateSK				INT NOT NULL PRIMARY KEY, -- Format: YYYYMMDD
    [Date]              DATE NOT NULL UNIQUE,
    [Day]               TINYINT,
    [Week]              TINYINT,
    [Month]             TINYINT,
    [MonthName]         NVARCHAR(20),
    [Quarter]           TINYINT,
    [Year]              INT,
    DayOfWeekName       NVARCHAR(20),
    IsWeekend           BIT -- 1: Cuối tuần, 0: Trong tuần
);
GO

-- 2.3 DIMENSION: AIRLINES (Hãng bay - Có SCD Type 1)
CREATE TABLE dbo.DimAirlines (
    AirlineSK          INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    AirlineNK           CHAR(2) NOT NULL UNIQUE,
    AirlineName         NVARCHAR(100)
);
GO

-- 2.4 DIMENSION: AIRPORTS (Sân bay - Có SCD Type 2)
CREATE TABLE dbo.DimAirports (
    AirportSK          INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    AirportNK           CHAR(10) NOT NULL UNIQUE,
    AirportName         NVARCHAR(100),
    City                NVARCHAR(100),
    [State]             VARCHAR(2),
    Country             NVARCHAR(50),
    Latitude            DECIMAL(12,9),
    Longitude           DECIMAL(12,9),

    -- SCD Metadata
    ValidFrom			DATETIME2,
    ValidTo				DATETIME2
);
GO

-- 2.5 DIMENSION: CANCELLATION REASON (Lý do hủy)
CREATE TABLE dbo.DimCancelReason (
    CancelReasonSK     INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    Code               CHAR(1) NOT NULL UNIQUE,       -- A, B, C, D
    Description        NVARCHAR(100)
);
GO

-- =====================================================================
-- 3. CREATE FACT TABLE
-- =====================================================================

CREATE TABLE dbo.FactFlights (
    FactFlightSK        BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,

    -- Foreign Keys
    DateSK             INT NOT NULL,              -- Derived from NDS.FlightDate
    
    AirlineSK          INT NOT NULL DEFAULT -1,   -- Lookup từ NDS.AirlineNK
    OriginAirportSK    INT NOT NULL DEFAULT -1,   -- Lookup từ NDS.OriginAirportNK
    DestAirportSK      INT NOT NULL DEFAULT -1,   -- Lookup từ NDS.DestAirportNK
    CancelReasonSK	   INT NULL,				  -- Lookup từ NDS.CancellationReason

    -- Direct Mapping Columns (Map trực tiếp từ NDS -> DDS)
    -- Identifiers
    FlightNumber        INT,
    TailNumber          VARCHAR(20),
    
    ScheduledDeparture  TIME,
    DepartureTime       TIME,
    ScheduledArrival    TIME,
    ArrivalTime         TIME,

    -- Time Metrics (Float/Int values)
    DepartureDelay      FLOAT,
    ArrivalDelay        FLOAT,
    TaxiOut             FLOAT,
    TaxiIn              FLOAT,
    ScheduledTime       FLOAT,
    ElapsedTime         FLOAT,
    AirTime             FLOAT,
    Distance            FLOAT,

    -- Delay Breakdown
    AirSystemDelay      FLOAT,
    SecurityDelay       FLOAT,
    AirlineDelay        FLOAT,
    LateAircraftDelay   FLOAT,
    WeatherDelay        FLOAT,

    -- Status Flags
    Cancelled           INT,
    Diverted            INT,
   
    -- Derived Columns để tính toán thêm KPIs
    -- OTP Flags
    IsOnTime_PlusMinus5 INT DEFAULT 0, -- ArrivalDelay between -5 and 5
    IsOnTime_Standard   INT DEFAULT 0, -- ArrivalDelay < 15

    -- CONSTRAINTS
    CONSTRAINT FK_Fact_Date FOREIGN KEY (DateSK) REFERENCES dbo.DimDate(DateSK),
    CONSTRAINT FK_Fact_Airline FOREIGN KEY (AirlineSK) REFERENCES dbo.DimAirlines(AirlineSK),
    CONSTRAINT FK_Fact_Origin FOREIGN KEY (OriginAirportSK) REFERENCES dbo.DimAirports(AirportSK),
    CONSTRAINT FK_Fact_Dest FOREIGN KEY (DestAirportSK) REFERENCES dbo.DimAirports(AirportSK),
    CONSTRAINT FK_Fact_CancelReason FOREIGN KEY (CancelReasonSK) REFERENCES dbo.DimCancelReason(CancelReasonSK),
);
GO
