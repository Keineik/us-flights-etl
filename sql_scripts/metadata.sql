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


-- =====================================================================
-- 3. INSERT DỮ LIỆU KHỞI TẠO
-- =====================================================================
INSERT INTO dbo.data_flow (TableName, LSET, CET) VALUES
('Flights_1', '1900-10-01 00:00:00', NULL),
('Flights_2', '1900-10-01 00:00:00', NULL),
('Flights_3', '1900-10-01 00:00:00', NULL);