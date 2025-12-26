-- ================ Initialization Script ===================== --
/* This script initialice the data base called 'data_warehouse' after checking if it already
 exists. If the database exists, it's dropped and recreated. Aditionally, the script creates
 three schemas within the database: "bronze", "silver" and "gold"

 === WARNING ===
 If the database already exists this script WILL DROP the entire 'data_warehouse' database, so
 all data in the database will be permanently deleted. Proceed with caution and ensure you have
 proper backups before running this script.

*/

DROP DATABASE IF EXISTS data_warehouse;
CREATE DATABASE data_warehouse;

-- We connect with the new database
\c data_warehouse

-- We create the medallion layers
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;


-- Success mesaje
SELECT 'Everythin works fine!' as mensaje;
