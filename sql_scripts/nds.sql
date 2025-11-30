-- =====================================================================
-- 1. SETUP DATABASE
-- =====================================================================
USE tempdb;
GO

IF EXISTS (SELECT 1 FROM sys.databases WHERE [name] = N'FLIGHTS_NDS')
BEGIN
    ALTER DATABASE FLIGHTS_NDS SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE FLIGHTS_NDS;
END
GO

CREATE DATABASE FLIGHTS_NDS;
GO
USE FLIGHTS_NDS;
GO

-- Table: SourceSystem
CREATE TABLE dbo.SourceSystem (
    SourceSK			INT IDENTITY(1,1) NOT NULL PRIMARY KEY, -- Surrogate Key
    SourceName			NVARCHAR(100),
    Description			NVARCHAR(100)
);
GO

INSERT INTO SourceSystem (SourceName) VALUES ('Source 1'), ('Source 2'), ('Source 3')

-- Table: Airlines
-- Stores unique airline information
CREATE TABLE dbo.Airlines (
    AirlineSK           INT IDENTITY(1,1) NOT NULL PRIMARY KEY, -- Surrogate Key
    AirlineNK           CHAR(2) NOT NULL,                   -- Natural Key
    AirlineName         NVARCHAR(100),
    SourceSK            INT, -- To track which file it came from

    -- Metadata
    CreatedDate         DATETIME2 DEFAULT GETDATE(),
    UpdatedDate         DATETIME2 DEFAULT GETDATE(),

    CONSTRAINT FK_Airlines_Source FOREIGN KEY (SourceSK) REFERENCES dbo.SourceSystem(SourceSK)
);
GO

-- Table: Airports
-- Stores unique airport details
CREATE TABLE dbo.Airports (
    AirportSK           INT IDENTITY(1,1) NOT NULL PRIMARY KEY, -- Surrogate Key
    AirportNK           CHAR(10) NOT NULL,                   -- Natural Key
    AirportName         NVARCHAR(100),
    City                NVARCHAR(100),
    [State]             VARCHAR(2),
    Country             NVARCHAR(50),
    Latitude            DECIMAL(12,9),
    Longitude           DECIMAL(12,9),
    SourceSK            INT, -- To track which file it came from

    -- Metadata
    CreatedDate         DATETIME2 DEFAULT GETDATE(),
    UpdatedDate         DATETIME2 DEFAULT GETDATE(),

    CONSTRAINT FK_Airports_Source FOREIGN KEY (SourceSK) REFERENCES dbo.SourceSystem(SourceSK)
);
GO

-- Table: Flights
-- Purpose: Consolidated flight data with relationships to Airlines and Airports
CREATE TABLE dbo.Flights (
    FlightSK            INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    
    -- Foreign Keys linking to NDS Reference Tables
    AirlineSK           INT NOT NULL, 
    OriginAirportSK     INT NOT NULL,
    DestAirportSK       INT NOT NULL,

	-- Natural Keys of Reference Tables
	Airline				CHAR(2) NOT NULL,
	OriginAirport		VARCHAR(10) NOT NULL,
	DestAirport			VARCHAR(10) NOT NULL,
    
    -- Flight Identification
    FlightDate          DATE NOT NULL,
    FlightNumber        INT,
    TailNumber          VARCHAR(20),
    
    -- Time Metrics (Stored as Time or Varchar(10) HHMM depending on preference)
    ScheduledDeparture  VARCHAR(10), 
    DepartureTime       VARCHAR(10),
    ScheduledArrival    VARCHAR(10),
    ArrivalTime         VARCHAR(10),
    
    -- Performance Metrics (Calculated fields for analysis)
    DepartureDelay      FLOAT,  -- Difference in minutes
    ArrivalDelay        FLOAT,  -- Difference in minutes
    TaxiOut             FLOAT,
    TaxiIn              FLOAT,
    WheelsOff           VARCHAR(10),
    WheelsOn            VARCHAR(10),
	ScheduledTime		FLOAT,
	ElapsedTime			FLOAT,
    AirTime             FLOAT,
    Distance            FLOAT,
    
    -- Status Flags
    Cancelled           INT,
    Diverted            INT,
    CancellationReason  CHAR(1), -- 'A', 'B', 'C', 'D'
    
    -- Delay Breakdown (in minutes)
    AirSystemDelay      FLOAT,
    SecurityDelay       FLOAT,
    AirlineDelay        FLOAT,
    LateAircraftDelay   FLOAT,
    WeatherDelay        FLOAT,
    
    -- Metadata
    CreatedDate         DATETIME2 DEFAULT GETDATE(),
    UpdatedDate         DATETIME2 DEFAULT GETDATE(),

    SourceSK            INT -- To track which file it came from

    -- CONSTRAINT: Relationships
    CONSTRAINT FK_Flights_Airlines FOREIGN KEY (AirlineSK) REFERENCES dbo.Airlines(AirlineSK),
    CONSTRAINT FK_Flights_Origin FOREIGN KEY (OriginAirportSK) REFERENCES dbo.Airports(AirportSK),
    CONSTRAINT FK_Flights_Dest FOREIGN KEY (DestAirportSK) REFERENCES dbo.Airports(AirportSK),
    CONSTRAINT FK_Flights_Source FOREIGN KEY (SourceSK) REFERENCES dbo.SourceSystem(SourceSK)
);
GO