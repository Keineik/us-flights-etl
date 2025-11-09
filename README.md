## US Flights ETL Project

This project is an ETL pipeline for US flights data using SQL Server and SSIS.

### 1. Update File Paths in SQL Scripts

Before running the SQL scripts, update the file paths in `sql_scripts/source.sql` to match the location of your CSV data files (in the `data/` folder).

1. Open `sql_scripts/source.sql` in a text editor.
2. Find any references to file paths (e.g., `FROM 'C:\path\to\data\filtered_flights_1.csv'`).
3. Change them to the correct absolute path on your machine (e.g., `C:\Keineik\Projects\us-flights-etl\data\filtered_flights_1.csv`).

### 2. Run SQL Scripts to Set Up Databases

Execute the following scripts in order using SQL Server Management Studio (SSMS) or a similar tool:

1. `sql_scripts/metadata.sql`
2. `sql_scripts/source.sql`
3. `sql_scripts/stage.sql`

This will create the necessary databases, tables, and load the source data.

### 3. Configure SSIS Package Parameters

1. Open the SSIS project (`us-flights-etl/us-flights-etl.dtproj`) in SQL Server Data Tools (SSDT) or Visual Studio.
2. In the Solution Explorer, right-click the `Package.dtsx` or `Source_to_Stage.dtsx` package and select **Edit**.
3. Update the connection manager parameters to point to your SQL Server instance and database. You can do this by:
	- Editing the `.conmgr` files (e.g., `FLIGHTS_Source.conmgr`) directly, or
	- Using the **Parameters** tab in the SSIS package designer to set the correct values.

### 4. Run the ETL Process Using SSIS

You can run the ETL process in one of two ways:

#### Option 1: From Visual Studio/SSDT

1. Right-click the SSIS package (e.g., `Source_to_Stage.dtsx`) and select **Execute Package**.
2. Monitor the execution and ensure all steps complete successfully.

#### Option 2: Using the Deployed .ispac File

1. Deploy the `us-flights-etl/bin/Development/us-flights-etl.ispac` to your SSIS Catalog on SQL Server.
2. Run the package from SQL Server Management Studio (SSMS) or using the `dtexec` command-line tool.

Example command (update paths and package names as needed):

```powershell
dtexec /ISSERVER "\SSISDB\us-flights-etl\Source_to_Stage" /SERVER <YourServerName>
```

---

**Note:** Ensure you have the necessary permissions to create databases and run SSIS packages on your SQL Server instance.
