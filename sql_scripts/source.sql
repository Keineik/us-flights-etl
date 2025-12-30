-- =====================================================================
-- 1. DROP DATABASE NẾU CÓ
-- =====================================================================
USE tempdb;
GO
DECLARE @SQL AS NVARCHAR (1000);
IF EXISTS (SELECT 1
           FROM sys.databases
           WHERE [name] = N'FLIGHTS_Source')
    BEGIN
        SET @SQL = N'USE [FLIGHTS_Source];
                 ALTER DATABASE FLIGHTS_Source SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                 USE [tempdb];
                 DROP DATABASE FLIGHTS_Source;';
        EXECUTE (@SQL);
    END

-- =====================================================================
-- 2. CREATE DATABASE VÀ TABLE
-- =====================================================================
CREATE DATABASE FLIGHTS_Source;
GO
USE FLIGHTS_Source;
GO

-- FLIGHTS_Source A: ERP Tables

-- Tour_A (Danh sách tour)

CREATE TABLE dbo.Airlines (
	IATA_CODE	CHAR(2)			PRIMARY KEY,
	AIRLINE		NVARCHAR(100)	UNIQUE NOT NULL
)

CREATE TABLE dbo.Airports (
	IATA_CODE	CHAR(10)			PRIMARY KEY,
	AIRPORT		NVARCHAR(100)	UNIQUE NOT NULL,
	CITY		NVARCHAR(100)	NULL,
	[STATE]		CHAR(2)			NULL,
	COUNTRY		NVARCHAR(50)	NULL,
	LATITUDE	DECIMAL(12,9)	NULL,
	LONGTITUDE	DECIMAL(12,9)	NULL
)

CREATE TABLE dbo.Flights_1 (
    [DATE]					DATE,
    AIRLINE					CHAR(2),
    FLIGHT_NUMBER			INT,
    TAIL_NUMBER				VARCHAR(10),
    ORIGIN_AIRPORT			CHAR(10),
    DESTINATION_AIRPORT		CHAR(10),
    SCHEDULED_DEPARTURE		CHAR(10),
    DEPARTURE_TIME			CHAR(10),
    DEPARTURE_DELAY			FLOAT,
    TAXI_OUT				FLOAT,
    WHEELS_OFF				CHAR(10),
    SCHEDULED_TIME			FLOAT,
    ELAPSED_TIME			FLOAT,
    AIR_TIME				FLOAT,
    DISTANCE				FLOAT,
    WHEELS_ON				CHAR(10),
    TAXI_IN					FLOAT,
    SCHEDULED_ARRIVAL		CHAR(10),
    ARRIVAL_TIME			CHAR(10),
    ARRIVAL_DELAY			FLOAT,
    DIVERTED				INT,
    CANCELLED				INT,
    CANCELLATION_REASON		VARCHAR(1),
    AIR_SYSTEM_DELAY		FLOAT,
    SECURITY_DELAY			FLOAT,
    AIRLINE_DELAY			FLOAT,
    LATE_AIRCRAFT_DELAY		FLOAT,
    WEATHER_DELAY			FLOAT,
	CREATED					DATETIME2,
	MODIFIED				DATETIME2
)

CREATE TABLE dbo.Flights_2 (
    [DATE]					DATE,
    AIRLINE					CHAR(2),
    FLIGHT_NUMBER			INT,
    TAIL_NUMBER				VARCHAR(10),
    ORIGIN_AIRPORT			CHAR(10),
    DESTINATION_AIRPORT		CHAR(10),
    SCHEDULED_DEPARTURE		CHAR(10),
    DEPARTURE_TIME			CHAR(10),
    DEPARTURE_DELAY			FLOAT,
    TAXI_OUT				FLOAT,
    WHEELS_OFF				CHAR(10),
    SCHEDULED_TIME			FLOAT,
    ELAPSED_TIME			FLOAT,
    AIR_TIME				FLOAT,
    DISTANCE				FLOAT,
    WHEELS_ON				CHAR(10),
    TAXI_IN					FLOAT,
    SCHEDULED_ARRIVAL		CHAR(10),
    ARRIVAL_TIME			CHAR(10),
    ARRIVAL_DELAY			FLOAT,
    DIVERTED				INT,
    CANCELLED				INT,
    CANCELLATION_REASON		VARCHAR(1),
    AIR_SYSTEM_DELAY		FLOAT,
    SECURITY_DELAY			FLOAT,
    AIRLINE_DELAY			FLOAT,
    LATE_AIRCRAFT_DELAY		FLOAT,
    WEATHER_DELAY			FLOAT,
	CREATED					DATETIME2,
	MODIFIED				DATETIME2
)

CREATE TABLE dbo.Flights_3 (
    [DATE]					DATE,
    AIRLINE					CHAR(2),
    FLIGHT_NUMBER			INT,
    TAIL_NUMBER				VARCHAR(10),
    ORIGIN_AIRPORT			CHAR(10),
    DESTINATION_AIRPORT		CHAR(10),
    SCHEDULED_DEPARTURE		CHAR(10),
    DEPARTURE_TIME			CHAR(10),
    DEPARTURE_DELAY			FLOAT,
    TAXI_OUT				FLOAT,
    WHEELS_OFF				CHAR(10),
    SCHEDULED_TIME			FLOAT,
    ELAPSED_TIME			FLOAT,
    AIR_TIME				FLOAT,
    DISTANCE				FLOAT,
    WHEELS_ON				CHAR(10),
    TAXI_IN					FLOAT,
    SCHEDULED_ARRIVAL		CHAR(10),
    ARRIVAL_TIME			CHAR(10),
    ARRIVAL_DELAY			FLOAT,
    DIVERTED				INT,
    CANCELLED				INT,
    CANCELLATION_REASON		VARCHAR(1),
    AIR_SYSTEM_DELAY		FLOAT,
    SECURITY_DELAY			FLOAT,
    AIRLINE_DELAY			FLOAT,
    LATE_AIRCRAFT_DELAY		FLOAT,
    WEATHER_DELAY			FLOAT,
	CREATED					DATETIME2,
	MODIFIED				DATETIME2
)

-- =====================================================================
-- 3. INSERT DỮ LIỆU MẪU
-- =====================================================================
BULK INSERT dbo.Airlines
FROM 'C:\Users\PC\source\repos\us-flights-etl\data\airlines.csv'
WITH
(
	FORMAT = 'CSV',
    FIRSTROW = 2,
    FIELDTERMINATOR = ',', 
    ROWTERMINATOR = '0x0a',
    TABLOCK,
	KEEPNULLS
)
GO

BULK INSERT dbo.Airports
FROM 'C:\Users\PC\source\repos\us-flights-etl\data\filtered_airpoirt.csv'
WITH
(
	FORMAT = 'CSV',
    FIRSTROW = 2,
    FIELDTERMINATOR = ',', 
    ROWTERMINATOR = '0x0a',
    TABLOCK,
	KEEPNULLS
)
GO

BULK INSERT dbo.Flights_1
FROM 'C:\Users\PC\source\repos\us-flights-etl\data\filtered_flights_1.csv'
WITH
(
	FORMAT = 'CSV',
	CODEPAGE = '65001', -- UTF-8
    FIRSTROW = 2,
    FIELDTERMINATOR = ',', 
    ROWTERMINATOR = '0x0a',
    TABLOCK,
	KEEPNULLS
)
GO

BULK INSERT dbo.Flights_2
FROM 'C:\Users\PC\source\repos\us-flights-etl\data\filtered_flights_2.csv'
WITH
(
	FORMAT = 'CSV',
	CODEPAGE = '65001', -- UTF-8
    FIRSTROW = 2,
    FIELDTERMINATOR = ',', 
    ROWTERMINATOR = '0x0a',
    TABLOCK,
	KEEPNULLS
)
GO

BULK INSERT dbo.Flights_3
FROM 'C:\Users\PC\source\repos\us-flights-etl\data\filtered_flights_3.csv'
WITH
(
	FORMAT = 'CSV',
	CODEPAGE = '65001', -- UTF-8
    FIRSTROW = 2,
    FIELDTERMINATOR = ',', 
    ROWTERMINATOR = '0x0a',
    TABLOCK,
	KEEPNULLS
)
GO