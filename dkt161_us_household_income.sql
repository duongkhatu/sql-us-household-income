USE finalproject;

# Data Cleaning
## Adding the NOT NULL constraint to the `Place` column
SELECT State_Name, City, Place FROM us_household_income
WHERE Place IS NULL; -- can't populate this value so gotta drop the row
DELETE FROM us_household_income
WHERE Place IS NULL;
ALTER TABLE us_household_income MODIFY Place VARCHAR(36) NOT NULL;

## Remove duplicates
SELECT id, COUNT(*) AS duplicates
FROM us_household_income
GROUP BY id
HAVING COUNT(*) > 1;

DELETE FROM us_household_income
WHERE EXISTS (SELECT 1 FROM (SELECT id, row_id FROM us_household_income) AS u2
	WHERE us_household_income.id = u2.id AND us_household_income.row_id > u2.row_id);
    
-- There are records with identical data across all columns yet with different id and can't be filtered by the DELETE statement above
SELECT State_Code, State_Name, State_ab, County, City, Place, Type, Primary_, Zip_Code, Area_Code,
	ALand, AWater, Lat, Lon, COUNT(*)
FROM us_household_income
GROUP BY State_Code, State_Name, State_ab, County, City, Place, Type, Primary_, Zip_Code, Area_Code,
	ALand, AWater, Lat, Lon
HAVING COUNT(*) > 1;

DELETE FROM us_household_income
WHERE row_id IN (
	SELECT row_id
	FROM (
		SELECT row_id, CONCAT(State_Code, State_Name, State_ab, County, City, Place, Type, Primary_, Zip_Code,
			Area_Code, ALand, AWater, Lat, Lon),
		ROW_NUMBER() OVER(PARTITION BY CONCAT(State_Code, State_Name, State_ab, County, City, Place, Type,
			Primary_, Zip_Code, Area_Code, ALand, AWater, Lat, Lon)
            ORDER BY CONCAT(State_Code, State_Name, State_ab, County, City, Place, Type, Primary_, Zip_Code, 			Area_Code,	ALand, AWater, Lat, Lon)) AS row_num -- sorry that it looks really messy
		FROM us_household_income
		) AS row_table
	WHERE row_num > 1
	); -- 2270 rows out game

## Data inconsistency
UPDATE us_household_income
SET `Type` = 'CDP'
WHERE `Type` = 'CPD';
UPDATE us_household_income
SET `Type` = 'Borough'
WHERE `Type` = 'Boroughs';

SELECT DISTINCT State_Code, State_Name, State_ab
FROM us_household_income; -- Georgia - georia
UPDATE us_household_income
SET `State_Name` = 'Georgia'
WHERE `State_Name` = 'georia';

SELECT DISTINCT County
FROM us_household_income
WHERE County LIKE '%�%'; -- 2 rows need correcting

SELECT DISTINCT Place
FROM us_household_income
WHERE Place LIKE '%�%'; -- 6 rows need correcting

-- I'll create a table of font errors for easy future correction
CREATE TABLE font_errors_instances(
	in_column VARCHAR(10),
    before_correct VARCHAR(36),
    after_correct VARCHAR(36)
);
INSERT INTO font_errors_instances
VALUES ('County', 'Do�a Ana County', 'Dona Ana County'),
	('County', 'R�o Grande Municipio', 'Rio Grande Municipio'),
    ('Place', 'Boquer�n', 'Boqueron'),
    ('Place', 'El Mang�', 'El Mango'),
    ('Place', 'Fr�nquez', 'Franquez'),
    ('Place', 'Liborio Negr�n Torres', 'Liborio Negron Torres'),
    ('Place', 'Parcelas Pe�uelas', 'Parcelas Penuelas'),
    ('Place', 'R�o Lajas', 'Rio Lajas');

-- Time to update the records with fonr errors:
UPDATE us_household_income u
JOIN font_errors_instances fe ON u.County = fe.before_correct
SET u.County = fe.after_correct;
UPDATE us_household_income u
JOIN font_errors_instances fe ON u.Place = fe.before_correct
SET u.Place = fe.after_correct;
    
SELECT DISTINCT Primary_
FROM us_household_income
ORDER BY Primary_; -- no inconsistencies

SELECT DISTINCT Area_Code
FROM us_household_income
ORDER BY Area_Code; -- There is a 'M' area code?
SELECT row_id, State_Name, County, City, Place FROM us_household_income
WHERE Area_Code = 'M';
-- Texas, Anderson County, Pasadena, Elkhart. Only one record, seems to be wrong. Area code can only be numerical with 3 digits.
SELECT * FROM us_household_income
WHERE State_Name = 'Texas' AND City = 'Pasadena' AND Place = 'Elkhart';
-- There are 2 different area codes there. I don't know which one is right so I'll set it to NULL
ALTER TABLE us_household_income MODIFY Area_Code VARCHAR(3); -- remove the NOT NULL constraint
UPDATE us_household_income
SET Area_Code = NULL
WHERE Area_Code = 'M';

SELECT DISTINCT LENGTH(Zip_Code)
FROM us_household_income; -- Zip codes can only be five digits long
SELECT * FROM us_household_income
WHERE LENGTH(Zip_Code) IN (3, 4); -- These seem to be missing 1 to 2 zeros at the beginning. Googled the zip codes and confirmed it's the right city and state
ALTER TABLE us_household_income MODIFY Zip_Code VARCHAR(5); -- Gotta change it to string
UPDATE us_household_income
SET Zip_Code = CASE
 WHEN LENGTH(Zip_Code) = 3 THEN CONCAT('00', Zip_Code)
 WHEN LENGTH(Zip_Code) = 4 THEN CONCAT('0', Zip_Code)
 ELSE Zip_Code
 END;

SELECT DISTINCT LENGTH(Area_Code)
FROM us_household_income; -- This column is consistent

-- Lat ranges from -90 to 90 and Lon ranges from -180 to 180.
SELECT * FROM us_household_income
WHERE Lat < -90 OR Lat > 90
	OR Lon < -180 OR Lon > 180; -- Confirmed there are no lat or lon out of range

-- ALand = 0?
SELECT * FROM us_household_income
WHERE ALand = 0;
-- 62 rows. ALand cannot be zero though. Gotta change these to null.
ALTER TABLE us_household_income MODIFY ALand BIGINT; -- remove the NOT NULL constraint
UPDATE us_household_income
SET ALand = NULL WHERE ALand = 0;

# EDA tasks
## Task 0
DELIMITER $$
CREATE PROCEDURE clean_new_data()
BEGIN
	-- Remove duplicates
    DELETE FROM us_household_income
	WHERE row_id IN (
	SELECT row_id
	FROM (
		SELECT row_id, CONCAT(State_Code, State_Name, State_ab, County, City, Place, Type, Primary_, Zip_Code,
			Area_Code, ALand, AWater, Lat, Lon),
		ROW_NUMBER() OVER(PARTITION BY CONCAT(State_Code, State_Name, State_ab, County, City, Place, Type,
			Primary_, Zip_Code, Area_Code, ALand, AWater, Lat, Lon)
            ORDER BY CONCAT(State_Code, State_Name, State_ab, County, City, Place, Type, Primary_, Zip_Code, 			Area_Code,	ALand, AWater, Lat, Lon)) AS row_num
		FROM us_household_income
		) AS row_table
	WHERE row_num > 1
	);
	-- Correct font errors
    UPDATE us_household_income u
    JOIN font_errors_instances fe ON u.County = fe.before_correct
	SET u.County = fe.after_correct;
	UPDATE us_household_income u
    JOIN font_errors_instances fe ON u.Place = fe.before_correct
	SET u.Place = fe.after_correct;
    -- Change ALand = 0 to NULLs
    UPDATE us_household_income
	SET ALand = NULL WHERE ALand = 0;
END $$

CREATE EVENT clean_new_data_wkly
ON SCHEDULE EVERY 1 WEEK
STARTS (CURRENT_TIMESTAMP + INTERVAL 1 WEEK)
DO
BEGIN
	-- Call the clean data procedure
	CALL clean_new_data();
    -- Compress new zip codes and area codes
	UPDATE us_household_income
	SET Zip_Code = COMPRESS(Zip_Code)
	WHERE LENGTH(Zip_Code) <= 5;
    -- Compressed values will be longer than 5 characters (which is the length of a normal zip code). The same logic applies to area code.
	UPDATE us_household_income
	SET Area_Code = COMPRESS(Area_Code)
    WHERE LENGTH(Area_Code) <= 3;
END $$
DELIMITER ;

## Task 1: Summarizing Data by State
SELECT State_Name, State_ab,
	AVG(ALand) AS avg_aland,
    AVG(AWater) AS avg_awater
FROM us_household_income
GROUP BY State_Name, State_ab
ORDER BY avg_aland DESC;

## Task 2: Filtering Cities by Population Range
SELECT City, State_Name, County
FROM us_household_income
GROUP BY City, State_Name, County
HAVING SUM(ALand) BETWEEN 50000000 AND 100000000 -- have to sum because one city can have many records
ORDER BY City;

## Task 3: Counting Cities per State
SELECT State_Name, State_ab,
	COUNT(DISTINCT City) AS city_count
FROM us_household_income
GROUP BY State_Name, State_ab
ORDER BY city_count DESC;

## Task 4: Identifying Top 10 Counties with the Widest Water Area
SELECT County, State_Name,
	SUM(AWater) AS total_water_area
FROM us_household_income
GROUP BY County, State_Name
ORDER BY total_water_area DESC
LIMIT 10;

## Task 5: Finding Cities Near Specific Coordinates
SELECT City, State_Name, County, Lat, Lon
FROM us_household_income
WHERE Lat BETWEEN 30 AND 35
	AND Lon BETWEEN -90 AND -85
ORDER BY Lat, Lon;

## Task 6: Rank cities within each state based on their land area
SELECT City, State_Name, ALand,
	RANK() OVER(PARTITION BY State_Name ORDER BY ALand DESC) AS aland_rank
FROM us_household_income
ORDER BY State_Name, aland_rank;

## Task 7: Creating Aggregate Reports For Each State
SELECT State_Name, State_ab,
	SUM(ALand) AS total_land_area,
    SUM(AWater) AS total_water_area,
    COUNT(DISTINCT City) AS city_count
FROM us_household_income
GROUP BY State_Name, State_ab
ORDER BY total_land_area DESC;

## Task 8: Cities where the land area is above the average land area of all cities
-- The average of all cities is different from the average of all records in the table. I calculate it by dividing the total land area of all records by the count of cities. But there are cities with the same name in different states. I solve this problem by counting distinct cities within each state first, then sum them together:
SET @city_count = 0;
SELECT SUM(statewise_city_count)
INTO @city_count
FROM (
	SELECT State_Name, COUNT(DISTINCT City) AS statewise_city_count
	FROM us_household_income
    GROUP BY State_Name) AS subq;
    
SELECT City, State_Name, SUM(ALand) AS city_aland
FROM us_household_income
GROUP BY City, State_Name
HAVING SUM(ALand) > (SELECT SUM(ALand) FROM us_household_income)/@city_count
ORDER BY city_aland DESC;

## Task 9: Cities where the water area is greater than 50% of the land area
SELECT City, State_Name,
	SUM(ALand) AS total_land_area,
    SUM(AWater) AS total_water_area,
	ROUND(SUM(AWater) / SUM(ALand), 2) AS water_to_land_ratio
FROM us_household_income
GROUP BY City, State_Name
HAVING ROUND(SUM(AWater) / SUM(ALand), 2) > .50
ORDER BY water_to_land_ratio DESC;
-- There are 323 cities with a water-to-land ratio greater than 0.5

## Task 10: Stored procedure for state report
DROP PROCEDURE IF EXISTS state_report;
DELIMITER $$
CREATE PROCEDURE state_report(IN p_state_ab VARCHAR(2))
BEGIN
	-- General state report
	SELECT COUNT(DISTINCT City) AS City_Count,
		AVG(ALand) AS Average_Land_Area,
        AVG(AWater) AS Average_Water_Area
    FROM us_household_income
    WHERE State_ab = p_state_ab;
	-- Detailed report of cities in the corresponding state
    SELECT City, SUM(ALand) AS Land_Area, SUM(AWater) AS Water_Area
    FROM us_household_income
    WHERE State_ab = p_state_ab
    GROUP BY City
    ORDER BY City; -- Order by city alphabetically for it to look nice
END $$
DELIMITER ;
-- Test run
CALL state_report('NC');

## Task 11: Temp table - top 20 cities by land area
-- There are cities with the same name in different states, so for the result's accuracy, I'll concat city and state_ab in the temp table
DROP TEMPORARY TABLE IF EXISTS top20_aland_cities;
CREATE TEMPORARY TABLE top20_aland_cities
SELECT CONCAT(City, State_Ab) AS city_concat
FROM us_household_income
GROUP BY CONCAT(City, State_Ab)
ORDER BY SUM(ALand) DESC
LIMIT 20;

SELECT City, State_Name,
	SUM(ALand) AS land_area, SUM(AWater) AS water_area,
	ROUND(AVG(AWater),2) AS avg_awater
FROM us_household_income
WHERE CONCAT(City, State_Ab) IN (SELECT city_concat FROM top20_aland_cities)
GROUP BY City, State_Name;

## Task 12
-- Again, the overall average land area across all cities is calculated by dividing the total land area by the number of cities stored in variable @city_count.
SELECT State_Name, state_avg
FROM (
	SELECT State_Name, ROUND(SUM(ALand)/COUNT(DISTINCT City), 2) AS state_avg
    FROM us_household_income
    GROUP BY State_Name
    ) AS st_avg
WHERE state_avg > (SELECT SUM(ALand) FROM us_household_income)/@city_count;

## Task 13: Indexing vs. No indexing
-- Query 1: state + city
SELECT State_Name, County, City,
	AVG(ALand) AS avg_aland, AVG(AWater) AS avg_awater
FROM us_household_income
WHERE State_Name = 'California' AND City = 'Fresno'
GROUP BY State_Name, County, City; -- 0.078 sec
-- Query 2: state + county
SELECT State_Name, County, City,
	AVG(ALand) AS avg_aland, AVG(AWater) AS avg_awater
FROM us_household_income
WHERE State_Name = 'California' AND County = 'Los Angeles County'
GROUP BY State_Name, County, City; -- 0.063 sec
-- Query 3: state + county + city
SELECT State_Name, County, City,
	AVG(ALand) AS avg_aland, AVG(AWater) AS avg_awater
FROM us_household_income
WHERE State_Name = 'California' AND County = 'Los Angeles County' AND City = 'Los Angeles'
GROUP BY State_Name, County, City; -- 0.062 sec

-- Create composite indexes
CREATE INDEX idx_state_county
ON us_household_income(State_Name(8), County(10));

CREATE INDEX idx_state_city
ON us_household_income(State_Name(8), City(10));

CREATE INDEX idx_state_county_city
ON us_household_income(State_Name(8), County(10), City(10));

SHOW INDEXES FROM us_household_income;
-- Try the queries above again. I use EXPLAIN to retrieve query plan but do not write it explicitly here as it would be repetitive and long
SELECT State_Name, County, City,
	AVG(ALand) AS avg_aland, AVG(AWater) AS avg_awater
FROM us_household_income
WHERE State_Name = 'California' AND City = 'Fresno'
GROUP BY State_Name, County, City; -- Uses idx_state_city, 0.000 sec

SELECT State_Name, County, City,
	AVG(ALand) AS avg_aland, AVG(AWater) AS avg_awater
FROM us_household_income
WHERE State_Name = 'California' AND County = 'Los Angeles County'
GROUP BY State_Name, County, City; -- Uses idx_state_county, 0.000 sec

SELECT State_Name, County, City,
	AVG(ALand) AS avg_aland, AVG(AWater) AS avg_awater
FROM us_household_income
WHERE State_Name = 'California' AND County = 'Los Angeles County' AND City = 'Los Angeles'
GROUP BY State_Name, County, City; -- Uses idx_state_county_city, 0.000 sec
-- => Overall, the execution time decreases remarkably with indexing compared to without indexing. MySQL automatically chooses the index that fits each specific query best.

## Task 14: Recursive CTE
-- Warning: I haven't actually test run this. Maybe it's an infinite loop, maybe it takes more than 600 seconds to run (which I've set my maximum wait time to). Anyway I'm so done with it.
WITH RECURSIVE city_aggregate AS (
	SELECT State_Name, City, SUM(ALand) AS ALand
	FROM us_household_income
	GROUP BY State_Name, City
),
cumulative_aland AS (
	SELECT State_Name, City, ALand AS city_aland,
		ALand AS cumulative_aland
	FROM city_aggregate
	UNION ALL
	SELECT c.State_Name, c.City, c.ALand AS city_aland,
		ca.cumulative_aland + c.ALand AS cumulative_aland
    FROM city_aggregate c, cumulative_aland ca
	WHERE c.State_Name = ca.State_Name
		AND c.City > ca.City
)
SELECT State_Name, City, city_aland, cumulative_aland
FROM cumulative_aland
ORDER BY State_Name, City;

## Task 15: Find anomalies
WITH state_land_stats AS (
    SELECT State_Name,
		-- take the avg of all cities, not all records since the question is to identify cities
        SUM(ALand) / COUNT(DISTINCT City) AS avg_aland,
        STD(ALand) AS std_aland
    FROM us_household_income
    GROUP BY State_Name
),
city_z_scores AS (
    SELECT u.State_Name, u.City, SUM(u.ALand) AS ALand,
		sls.avg_aland, sls.std_aland,
        (SUM(u.ALand) - sls.avg_aland) / sls.std_aland AS z_score
    FROM us_household_income u
    JOIN state_land_stats sls ON u.State_Name = sls.State_Name
    GROUP BY u.State_Name, u.City
)
SELECT State_Name, City, ALand, ROUND(avg_aland,2) AS avg_aland, ROUND(z_score, 2) AS z_score
FROM city_z_scores
WHERE ROUND(ABS(z_score),2) > 3.00 -- Observations more than 3 standard deviations away from the mean is usually considered outliers
ORDER BY ABS(z_score) DESC;
-- => There are 470 cities with abnormally high land area. No city has abnormally small land area.

## Task 16: Stored Procedures for Complex Calculations
DROP PROCEDURE IF EXISTS predict_land_water;
DELIMITER $$
CREATE PROCEDURE predict_land_water(IN p_city VARCHAR(22), IN p_state_ab CHAR(2), IN p_years INT)
BEGIN
    -- Assume a constant growth rate of 1%/year for both land and water since there is no year or date data
    SELECT ROUND(SUM(ALand) * POWER(1 + .01, p_years),2) AS Predicted_Land_Area,
		ROUND(SUM(AWater) * POWER(1 + .01, p_years),2) AS Predicted_Water_Area
    FROM us_household_income
    WHERE City = p_city AND State_ab = p_state_ab
    GROUP BY State_ab, City;
END $$
DELIMITER ;

-- Test run
CALL predict_land_water('Los Angeles', 'CA', 2);
CALL predict_land_water('Springfield', 'IL', 3);
CALL predict_land_water('Tucson', 'AZ', 2);

## Task 17: Create trigger to update summary table
-- Create summary table
DROP TABLE IF EXISTS state_summary;
CREATE TABLE state_summary (
    State_Ab CHAR(2) NOT NULL PRIMARY KEY,
    State_Name VARCHAR(20) NOT NULL,
    Total_Land_Area BIGINT NOT NULL,
    Total_Water_Area BIGINT NOT NULL,
    Number_Of_Cities INT NOT NULL,
    Average_City_Land_Area DECIMAL(13,2) NOT NULL,
    Average_City_Water_Area DECIMAL(13,2) NOT NULL
);
INSERT INTO state_summary
SELECT State_ab, State_Name, SUM(ALand), SUM(AWater),
	COUNT(DISTINCT City), -- I added city count, avg aland and awater of all cities in that state
	ROUND(SUM(ALand) / COUNT(DISTINCT City), 2),
	ROUND(SUM(AWater) / COUNT(DISTINCT City), 2)
FROM us_household_income
GROUP BY State_ab, State_Name;

-- Create a procedure to call in the trigger
DROP PROCEDURE IF EXISTS update_state_summary;
DELIMITER $$
CREATE PROCEDURE update_state_summary(IN p_state_ab CHAR(2))
BEGIN
	DECLARE v_st_name VARCHAR(20);
    DECLARE v_aland BIGINT;
    DECLARE v_awater BIGINT;
    DECLARE v_city_count INT;
    DECLARE v_avg_city_aland DECIMAL(13,2);
    DECLARE v_avg_city_awater DECIMAL(13,2);
    -- Calculate the aggregated columns for the specified state
    SELECT State_Name, SUM(ALand), SUM(AWater), COUNT(DISTINCT City),
        ROUND(SUM(ALand) / COUNT(DISTINCT City), 2),
        ROUND(SUM(AWater) / COUNT(DISTINCT City), 2)
    INTO v_st_name, v_aland, v_awater, v_city_count, v_avg_city_aland, v_avg_city_awater
    FROM us_household_income
    WHERE State_ab = p_state_ab
    GROUP BY State_Name;
    -- Update or insert into the summary table (insert a new record in case the president wants a new state, maybe...)
    INSERT INTO state_summary (State_Ab, State_Name, Total_Land_Area, Total_Water_Area, Number_Of_Cities,
		Average_City_Land_Area, Average_City_Water_Area)
    VALUES (p_state_ab, v_st_name, v_aland, v_awater, v_city_count, v_avg_city_aland, v_avg_city_awater)
    ON DUPLICATE KEY UPDATE -- If key already exists then update
		State_Name = v_st_name, Total_Land_Area = v_aland,
        Total_Water_Area = v_awater, Number_Of_Cities = v_city_count,
        Average_City_Land_Area = v_avg_city_aland,
        Average_City_Water_Area = v_avg_city_awater;
END $$
-- Create trigger after insert
CREATE TRIGGER st_sum_aft_insert
AFTER INSERT ON us_household_income
FOR EACH ROW
BEGIN
    CALL update_state_summary(NEW.State_ab);
END $$
-- Create trigger after update
CREATE TRIGGER st_sum_aft_update
AFTER UPDATE ON us_household_income
FOR EACH ROW
BEGIN
    CALL update_state_summary(NEW.State_ab);
END $$
-- Create trigger after delete
CREATE TRIGGER st_sum_aft_delete
AFTER DELETE ON us_household_income
FOR EACH ROW
BEGIN
    CALL update_state_summary(OLD.State_ab);
END $$
DELIMITER ;

-- Test the trigger
SELECT * FROM state_summary; -- Note that Alabama has 250 cities

INSERT INTO us_household_income
VALUES (99999, 696969696, 1, 'Alabama', 'AL', 'Sweet Home Alabama County', 'Wakanda', 'Forever', 'Track', 'Track', 36265, '256', 10000000, 500000, 33.8405764, -86.6350183);
-- Note: if you rerun this after task 19 then use the insert statement below instead. Just get rid of the #s.
#INSERT INTO us_household_income
#VALUES (99999, 696969696, 1, 'Alabama', 'AL', 'Sweet Home Alabama County', 'Wakanda', 'Forever', 'Track', 'Track', 36265, '256', 10000000, 500000, 33.8405764, -86.6350183, ST_GeomFromText('POINT(1 1)'));

SELECT * FROM state_summary;
-- Boom, it now has 251 cities (250 + Wakanda). The other aggregated columns are also updated.

DELETE FROM us_household_income
WHERE County = 'Sweet Home Alabama County';
SELECT * FROM state_summary; -- Back to 250 cities again. Bye Wakanda.

## Task 18: Advanced Data Encryption and Security
-- I'll use COMPRESS() to encrypt the two columns
-- Alter data type of the two columns
ALTER TABLE us_household_income
MODIFY Zip_Code VARBINARY(20) NOT NULL,
MODIFY Area_Code VARBINARY(18) NOT NULL;
-- Encrypt
UPDATE us_household_income
SET Zip_Code = COMPRESS(Zip_Code),
Area_Code = COMPRESS(Area_Code);
-- Test to see if it worked
SELECT Zip_Code, Area_Code FROM us_household_income LIMIT 50; -- They have become blobs
-- How to decrypt this:
SELECT CONVERT(UNCOMPRESS(Zip_Code) USING utf8) AS Zip_Code,
	CONVERT(UNCOMPRESS(Area_Code) USING utf8) AS Area_Code
FROM us_household_income
LIMIT 50; -- They seems fine! Shh, only authorized users should know about this.

## Task 19: Geospatial Analysis
-- Create a spatial index
ALTER TABLE us_household_income ADD coordinates POINT;
UPDATE us_household_income SET coordinates = POINT(Lon, Lat);
ALTER TABLE us_household_income MODIFY coordinates POINT SRID 0 NOT NULL;
CREATE SPATIAL INDEX idx_coordinates ON us_household_income(coordinates);

-- Formula for calculating the distance between two coordinates (lat and lon in radian):
-- = acos(sin(lat1)*sin(lat2)+cos(lat1)*cos(lat2)*cos(lon2-lon1))*6371 (6371 is Earth radius in km)
DROP PROCEDURE IF EXISTS cities_within_radius;
DELIMITER $$
CREATE PROCEDURE cities_within_radius(IN p_lat DECIMAL(10,7),
	IN p_lon DECIMAL(12,7), IN p_radius_km DECIMAL(10,3))
BEGIN
    SELECT City, State_Name, County,
        ROUND(ACOS(SIN(RADIANS(Lat)) * SIN(RADIANS(p_lat)) + COS(RADIANS(Lat)) * COS(RADIANS(p_lat))
        * (COS(RADIANS(p_lon) - RADIANS(Lon)))) * 6371, 3) AS Distance
    FROM us_household_income
    WHERE
		ROUND(ACOS(SIN(RADIANS(Lat)) * SIN(RADIANS(p_lat)) + COS(RADIANS(Lat)) * COS(RADIANS(p_lat))
        * (COS(RADIANS(p_lon) - RADIANS(Lon)))) * 6371, 3) <= p_radius_km
	ORDER BY Distance;
END $$
DELIMITER ;

-- Test run
CALL cities_within_radius(33.1236547, -88.9053358, 50.500);

## Task 20: Analyzing Correlations
SELECT State_Name,
	SUM((ALand - (SELECT AVG(ALand) FROM us_household_income)) *
	(AWater - (SELECT AVG(AWater) FROM us_household_income))) /
	SQRT(NULLIF(SUM(POWER(ALand - (SELECT AVG(ALand) FROM us_household_income), 2)) *
	SUM(POWER(AWater - (SELECT AVG(AWater) FROM us_household_income), 2)), 0)) AS corr
FROM us_household_income
GROUP BY State_Name
ORDER BY corr DESC;
-- => Only Hawaii have a slightly negative correlation coefficient - this may be related to the fact that it is an archipelago. All the other states have a positive correlation coefficient between land area and water area. Some states with extremely high correlation (close to 1) are District of Columbia and Rhode Island. We can see that these are two of the smallest states in the USA.

## Task 21: Find hotspots
-- The question does not require the hotspots to be city so I take the average of all records
SET @avg_land_water = 0;
SET @std_land_water = 0;
SELECT AVG(ALand + AWater), STD(ALand + AWater)
INTO @avg_land_water, @std_land_water
FROM us_household_income;

WITH hotspot_z_scores AS (
    SELECT State_Name, City, ALand, AWater,
        ((ALand + AWater) - @avg_land_water) / @std_land_water AS z_score
    FROM us_household_income
)
SELECT State_Name, City, ALand, AWater, ROUND(z_score,2) AS z_score
FROM hotspot_z_scores
WHERE ROUND(ABS(z_score),2) > 3.00
ORDER BY ABS(z_score) DESC;
-- => Most of the top ranks belong to places in Alaska. The largest z_score is 77.45, indicating that the combination of ALand and AWater of this hotspot is 77.45 standard deviations away from the mean of all records.

## Task 22
DROP TABLE IF EXISTS allocate_resources;
CREATE TABLE allocate_resources(
	City_ID VARCHAR(24) PRIMARY KEY NOT NULL,
	City VARCHAR(22) NOT NULL, State_Name VARCHAR(20) NOT NULL,
    Land_Area BIGINT, Water_Area BIGINT,
    Allocated_Resources DECIMAL(25,2));

DROP PROCEDURE IF EXISTS allocate_resources;
DELIMITER $$
CREATE PROCEDURE allocate_resources(IN total_resources_dollars BIGINT, IN land_importance DECIMAL(4,3))
BEGIN
    -- I'll build a simple model that calculate each city's score by the weighted total of ALand and AWater
	-- General model: Score = land_importance * ALand + (1-land_importance) * AWater
    -- land_importance must be a decimal in range [0, 1] with up to 3 decimal digits
    CREATE TEMPORARY TABLE city_scores
	SELECT State_ab, City, State_Name,
		SUM(ALand) AS Land_Area, SUM(AWater) AS Water_Area,
		land_importance * SUM(ALand) + (1 - land_importance) * SUM(AWater) AS Score,
		ROUND(land_importance * SUM(ALand) + (1 - land_importance) * SUM(AWater) / (
			SELECT SUM(Score) FROM (
				SELECT land_importance * SUM(ALand) + (1 - land_importance) * SUM(AWater) AS Score
				FROM us_household_income GROUP BY City, State_Name) AS sub_city_scores
			) * total_resources_dollars, 2) AS Allocated_Resources
	FROM us_household_income
	GROUP BY State_ab, State_Name, City;
	-- CTEs can't be followed by an insert statement so I have to use a temp table. I have to do all the calculations within one temp table so it looks really complicated.
    
    TRUNCATE TABLE allocate_resources; -- truncate to delete all records before inserting new ones
    INSERT INTO allocate_resources(City_ID, City, State_Name, Land_Area, Water_Area, Allocated_Resources)
    SELECT CONCAT(State_ab, City), City, State_Name, Land_Area, Water_Area, Allocated_Resources
    FROM city_scores;
	-- Drop the temp table after use
	DROP TEMPORARY TABLE IF EXISTS city_scores;
	-- Return the results from the table
	SELECT * FROM allocate_resources;
END $$
DELIMITER ;

-- Test run
CALL allocate_resources(1600000000000, 0.5);

