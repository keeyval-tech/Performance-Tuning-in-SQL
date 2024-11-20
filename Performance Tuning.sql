CREATE TABLE staging_table1 (
    CRASH_DATE DATE,
    CRASH_TIME TIME,
    BOROUGH CHARACTER(100),
    ZIP_CODE CHARACTER(100),
    LATITUDE NUMERIC,
    LONGITUDE NUMERIC,
    LOC CHARACTER(100),
    ON_STREET_NAME CHARACTER(100),
    CROSS_STREET_NAME CHARACTER(100),
	OFF_STREET_NAME CHARACTER(100),
    NUMBER_OF_PERSONS_INJURED NUMERIC,
    NUMBER_OF_PERSONS_KILLED NUMERIC,
    NUMBER_OF_PEDESTRIANS_INJURED NUMERIC,
    NUMBER_OF_PEDESTRIANS_KILLED NUMERIC,
    NUMBER_OF_CYCLIST_INJURED NUMERIC,
    NUMBER_OF_CYCLIST_KILLED NUMERIC,
    NUMBER_OF_MOTORIST_INJURED NUMERIC,
    NUMBER_OF_MOTORIST_KILLED NUMERIC,
    CONTRIBUTING_FACTOR_VEHICLE_1 CHARACTER(500),
    CONTRIBUTING_FACTOR_VEHICLE_2 CHARACTER(500),
	CONTRIBUTING_FACTOR_VEHICLE_3 CHARACTER(500),
	COLLISION_ID NUMERIC PRIMARY KEY,
    VEHICLE_TYPE_CODE_1 CHARACTER(500),
    VEHICLE_TYPE_CODE_2 CHARACTER(500),
    VEHICLE_TYPE_CODE_3 CHARACTER(500)
);

select * from staging_table1

--- Vehicle Details 1Table

--done
CREATE TABLE Vehicle_1_Details (
    Vehicle_1_ID SERIAL PRIMARY KEY,
    Vehicle_Type_Code_1 VARCHAR(255),
    Contributing_Factor_Vehicle_1 VARCHAR(255)
) 

--done
INSERT INTO Vehicle_1_Details (Vehicle_Type_Code_1, Contributing_Factor_Vehicle_1)
SELECT DISTINCT Vehicle_Type_Code_1, Contributing_Factor_Vehicle_1
FROM staging_table1;

--done
ALTER TABLE STAGING_TABLE1 ADD COLUMN Vehicle_1_ID NUMERIC;

--done
UPDATE STAGING_TABLE1 
SET VEHICLE_1_ID = Vehicle_1_Details.VEHICLE_1_ID 
FROM Vehicle_1_Details
WHERE STAGING_TABLE1.Vehicle_Type_Code_1  = VEHICLE_1_DETAILS.VEHICLE_TYPE_CODE_1 
AND STAGING_TABLE1.Contributing_Factor_Vehicle_1 = VEHICLE_1_DETAILS.Contributing_Factor_Vehicle_1;

----
--done
CREATE TABLE Vehicle_2_Details (
    Vehicle_2_ID SERIAL PRIMARY KEY,
    Vehicle_Type_Code_2 VARCHAR(255),
    Contributing_Factor_Vehicle_2 VARCHAR(255)
) 

--done
INSERT INTO Vehicle_2_Details (Vehicle_Type_Code_2, Contributing_Factor_Vehicle_2)
SELECT DISTINCT Vehicle_Type_Code_2, Contributing_Factor_Vehicle_2
FROM staging_table1;

--done
ALTER TABLE STAGING_TABLE1 ADD COLUMN Vehicle_2_ID NUMERIC;

--done
UPDATE STAGING_TABLE1 
SET VEHICLE_2_ID = Vehicle_2_Details.VEHICLE_2_ID 
FROM Vehicle_2_Details
WHERE STAGING_TABLE1.Vehicle_Type_Code_2  = VEHICLE_2_DETAILS.VEHICLE_TYPE_CODE_2
AND STAGING_TABLE1.Contributing_Factor_Vehicle_2 = VEHICLE_2_DETAILS.Contributing_Factor_Vehicle_2;

---
--done
CREATE TABLE Vehicle_3_Details (
    Vehicle_3_ID SERIAL PRIMARY KEY,
    Vehicle_Type_Code_3 VARCHAR(255),
    Contributing_Factor_Vehicle_3 VARCHAR(255)
) 

--done
INSERT INTO Vehicle_3_Details (Vehicle_Type_Code_3, Contributing_Factor_Vehicle_3)
SELECT DISTINCT Vehicle_Type_Code_3, Contributing_Factor_Vehicle_3
FROM staging_table1;

--done
ALTER TABLE STAGING_TABLE1 ADD COLUMN Vehicle_3_ID NUMERIC;

--done
UPDATE STAGING_TABLE1 
SET VEHICLE_3_ID = Vehicle_3_Details.VEHICLE_3_ID 
FROM Vehicle_3_Details
WHERE STAGING_TABLE1.Vehicle_Type_Code_3  = VEHICLE_3_DETAILS.VEHICLE_TYPE_CODE_3
AND STAGING_TABLE1.Contributing_Factor_Vehicle_3 = VEHICLE_3_DETAILS.Contributing_Factor_Vehicle_3;
----

--done
CREATE TABLE Vehicle_Details (
    Vehicle_Details_ID SERIAL PRIMARY KEY,
	Vehicle_1_ID INT,
	Vehicle_2_ID INT,
	Vehicle_3_ID INT,
    FOREIGN KEY (Vehicle_1_ID) REFERENCES Vehicle_1_Details,
    FOREIGN KEY (Vehicle_2_ID) REFERENCES Vehicle_2_Details,
    FOREIGN KEY (Vehicle_3_ID) REFERENCES Vehicle_3_Details
) 

--alternative ERD 2 - tried union
--CREATE TABLE Vehicle_Details (
--    Vehicle_Details_ID SERIAL PRIMARY KEY,
--    Vehicle_Type_Code VARCHAR(255),
--    Contributing_Factor VARCHAR(255)
--);

--alternative ERD 2
--INSERT INTO Vehicle_Details (Vehicle_Type_Code, Contributing_Factor)
--SELECT 
--    Vehicle_Type_Code_1 AS Vehicle_Type_Code, 
--    Contributing_Factor_Vehicle_1 AS Contributing_Factor 
--FROM Vehicle_1_Details
--UNION
--SELECT 
--    Vehicle_Type_Code_2, 
--    Contributing_Factor_Vehicle_2 
--FROM Vehicle_2_Details
--UNION
--SELECT 
--    Vehicle_Type_Code_3, 
--    Contributing_Factor_Vehicle_3 
--FROM Vehicle_3_Details;


DROP TABLE Vehicle_Details
select * from vehicle_details

--done
ALTER TABLE STAGING_TABLE1 ADD COLUMN Vehicle_Details_ID NUMERIC;

--done
INSERT INTO Vehicle_Details (Vehicle_1_ID, Vehicle_2_ID, Vehicle_3_ID)
SELECT
  (SELECT v1.Vehicle_1_ID FROM Vehicle_1_Details v1 WHERE v1.Vehicle_1_ID = s1.Vehicle_1_ID AND v1.Vehicle_1_ID = s1.Vehicle_1_ID LIMIT 1),
  (SELECT v2.Vehicle_2_ID FROM Vehicle_2_Details v2 WHERE v2.Vehicle_2_ID = s1.Vehicle_2_ID AND v2.Vehicle_2_ID = s1.Vehicle_2_ID LIMIT 1),
  (SELECT v3.Vehicle_3_ID FROM Vehicle_3_Details v3 WHERE v3.Vehicle_3_ID = s1.Vehicle_3_ID AND v3.Vehicle_3_ID = s1.Vehicle_3_ID LIMIT 1)
FROM staging_table1 s1;

--done
UPDATE STAGING_TABLE1 
SET VEHICLE_DETAILS_ID = VEHICLE_DETAILS.VEHICLE_DETAILS_ID 
FROM VEHICLE_DETAILS
WHERE STAGING_TABLE1.Vehicle_1_ID  = Vehicle_Details.Vehicle_1_ID
AND STAGING_TABLE1.Vehicle_2_ID = Vehicle_Details.Vehicle_2_ID 
AND STAGING_TABLE1.Vehicle_3_ID = Vehicle_Details.Vehicle_3_ID;

--alternative ERD 2
UPDATE STAGING_TABLE1 
SET VEHICLE_DETAILS_ID = VEHICLE_DETAILS.VEHICLE_DETAILS_ID 
FROM VEHICLE_DETAILS
WHERE STAGING_TABLE1.Vehicle_Type_Code_1  = Vehicle_Details.Vehicle_Type_Code
AND STAGING_TABLE1.Contributing_Factor_Vehicle_1 = Vehicle_Details.Contributing_Factor
AND STAGING_TABLE1.Vehicle_Type_Code_2 = Vehicle_Details.Vehicle_Type_Code
AND STAGING_TABLE1.Contributing_Factor_Vehicle_2 = Vehicle_Details.Contributing_Factor
AND STAGING_TABLE1.Vehicle_Type_Code_3  = Vehicle_Details.Vehicle_Type_Code
AND STAGING_TABLE1.Contributing_Factor_Vehicle_3 = Vehicle_Details.Contributing_Factor ;

SELECT * FROM VEHICLE_DETAILS
DROP TABLE VEHICLE_DETAILS

-- Street Table

--done
CREATE TABLE Street (
    Street_ID SERIAL PRIMARY KEY,
    On_Street_Name VARCHAR(255),
    Cross_Street_Name VARCHAR(255),
    Off_Street_Name VARCHAR(255)
) 


--done
ALTER TABLE STAGING_TABLE1 ADD COLUMN Street_ID NUMERIC;

--done
INSERT INTO Street(On_Street_Name, Cross_Street_Name, Off_Street_Name)
SELECT DISTINCT On_Street_Name, Cross_Street_Name, Off_Street_Name
FROM STAGING_TABLE1

--done
UPDATE STAGING_TABLE1 
SET STREET_ID = STREET.STREET_ID
FROM STREET
WHERE STAGING_TABLE1.On_Street_Name  = Street.On_Street_Name
AND STAGING_TABLE1.Cross_Street_Name = Street.Cross_Street_Name
AND STAGING_TABLE1.Off_Street_Name = Street.Off_Street_Name;

select * from street
drop table street

-- Borough Table

--done
CREATE TABLE Borough (
    Borough_ID SERIAL PRIMARY KEY,
    Borough VARCHAR(50)
) 

--done
ALTER TABLE STAGING_TABLE1 ADD COLUMN Borough_ID NUMERIC;

--done
INSERT INTO Borough(Borough)
SELECT DISTINCT Borough
FROM STAGING_TABLE1

--done
UPDATE STAGING_TABLE1 
SET BOROUGH_ID = BOROUGH.BOROUGH_ID
FROM BOROUGH
WHERE STAGING_TABLE1.BOROUGH  = BOROUGH.BOROUGH;

select * from BOROUGH

-- ZIP Code

--done
CREATE TABLE ZIP_Code (
    ZIP_Code_ID SERIAL PRIMARY KEY,
    ZIP_Code VARCHAR(255)
) 

--done
ALTER TABLE STAGING_TABLE1 ADD COLUMN ZIP_Code_ID NUMERIC;

--done
INSERT INTO ZIP_Code(ZIP_Code)
SELECT DISTINCT ZIP_Code
FROM STAGING_TABLE1

--done
UPDATE STAGING_TABLE1 
SET ZIP_CODE_ID = ZIP_CODE.ZIP_CODE_ID
FROM ZIP_CODE
WHERE STAGING_TABLE1.ZIP_CODE  = ZIP_CODE.ZIP_CODE;

SELECT * FROM ZIP_CODE

-- Coordinates

--done
CREATE TABLE Coordinates (
    Coordinates_ID SERIAL PRIMARY KEY,
    Latitude NUMERIC,
    Longitude NUMERIC,
    Loc VARCHAR(30)
) 

--done
ALTER TABLE STAGING_TABLE1 ADD COLUMN Coordinates_ID NUMERIC;

--done
INSERT INTO Coordinates(Latitude,Longitude,Loc)
SELECT DISTINCT Latitude,Longitude,Loc
FROM STAGING_TABLE1

--done
UPDATE STAGING_TABLE1 
SET Coordinates_ID = Coordinates.Coordinates_ID
FROM Coordinates
WHERE STAGING_TABLE1.Latitude = Coordinates.Latitude
AND STAGING_TABLE1.Longitude = Coordinates.Longitude
AND STAGING_TABLE1.Loc = Coordinates.Loc;

SELECT * FROM Coordinates

-- Collision Details

--done
CREATE TABLE Collision_Details(
    Collision_Details_ID SERIAL PRIMARY KEY,
	Crash_Date DATE,
	Crash_Time Time,
	Coordinates_ID INT,
	Street_ID INT,
	Borough_ID INT,
	ZIP_CODE_ID INT,
	FOREIGN KEY (Coordinates_ID) REFERENCES Coordinates,
    FOREIGN KEY (Street_ID) REFERENCES Street,
    FOREIGN KEY (Borough_ID) REFERENCES Borough,
    FOREIGN KEY (ZIP_CODE_ID) REFERENCES ZIP_Code
)

--done
ALTER TABLE STAGING_TABLE1 ADD COLUMN Collision_Details_ID NUMERIC;

--done
INSERT INTO Collision_Details (Crash_Date, Crash_Time, Coordinates_ID, Street_ID, Borough_ID, ZIP_Code_ID)
SELECT
    s1.CRASH_DATE,
    s1.CRASH_TIME,
    s1.Coordinates_ID,
    st.Street_ID,
    s1.Borough_ID,
    s1.ZIP_Code_ID
FROM
    staging_table1 s1
    JOIN Coordinates co ON s1.coordinates_ID = co.Coordinates_ID
    JOIN Street st ON st.Street_ID = s1.Street_ID 
    JOIN Borough b ON b.Borough_ID = s1.BOROUGH_ID
    JOIN ZIP_Code z ON z.ZIP_Code_ID = s1.ZIP_CODE_ID;

--done
UPDATE STAGING_TABLE1 
SET Collision_Details_ID = Collision_Details.Collision_Details_ID
FROM Collision_Details
WHERE STAGING_TABLE1.Crash_Date = STAGING_TABLE1.Crash_Date
AND STAGING_TABLE1.Crash_Time = STAGING_TABLE1.Crash_Time
AND STAGING_TABLE1.Coordinates_ID = Collision_Details.Coordinates_ID
AND STAGING_TABLE1.Street_ID = Collision_Details.Street_ID
AND STAGING_TABLE1.Borough_ID = Collision_Details.Borough_ID
AND STAGING_TABLE1.ZIP_CODE_ID = Collision_Details.ZIP_CODE_ID;

select * from collision_details

-- Collision 

--done
CREATE TABLE Collision(
    Collision_ID NUMERIC PRIMARY KEY,
	Number_of_Persons_Injured INT,
	Number_of_Persons_Killed INT,
	Collision_Details_ID INT,
	Vehicle_Details_ID INT,
	FOREIGN KEY (Collision_Details_ID) REFERENCES Collision_Details,
    FOREIGN KEY (Vehicle_Details_ID) REFERENCES Vehicle_Details
)

-- ALTER TABLE STAGING_TABLE1 ADD COLUMN Collision_ID NUMERIC;
--Not needed, as Collision_ID already exists

--done
INSERT INTO Collision (
    Collision_ID,
    Number_of_Persons_Injured,
    Number_of_Persons_Killed,
    Collision_Details_ID,
    Vehicle_Details_ID
)
SELECT
    s1.COLLISION_ID,
    s1.NUMBER_OF_PERSONS_INJURED,
    s1.NUMBER_OF_PERSONS_KILLED,
    cd.Collision_Details_ID,
    vd.Vehicle_Details_ID
FROM 
    staging_table1 s1
LEFT JOIN Collision_Details cd ON cd.Crash_Date = s1.CRASH_DATE AND cd.Crash_Time = s1.CRASH_TIME
LEFT JOIN Vehicle_Details vd ON vd.Vehicle_1_ID = s1.Vehicle_1_ID AND vd.Vehicle_2_ID = s1.Vehicle_2_ID AND vd.Vehicle_3_ID = s1.Vehicle_3_ID
ON CONFLICT (Collision_ID) DO NOTHING;

select * from staging_table1


-- Vertically partitioned Tables

-- Pedestrian
--done
CREATE TABLE Pedestrian (
    Collision_ID INT PRIMARY KEY,
    Number_of_Pedestrians_Injured INT,
    Number_of_Pedestrians_Killed INT,
    FOREIGN KEY (Collision_ID) REFERENCES Collision
);

--done
INSERT INTO Pedestrian (Collision_ID, Number_of_Pedestrians_Injured, Number_of_Pedestrians_Killed)
SELECT
    COLLISION_ID,
    Number_of_Pedestrians_Injured,
    Number_of_Pedestrians_Killed
FROM staging_table1;

-- Cyclist
--done
CREATE TABLE Cyclist (
    Collision_ID INT PRIMARY KEY,
    Number_of_Cyclist_Injured INT,
    Number_of_Cyclist_Killed INT,
    FOREIGN KEY (Collision_ID) REFERENCES Collision
);

--done
INSERT INTO Cyclist (Collision_ID, Number_of_Cyclist_Injured, Number_of_Cyclist_Killed)
SELECT
    COLLISION_ID,
    Number_of_Cyclist_Injured,
    Number_of_Cyclist_Killed
FROM staging_table1;

-- Motorist
--done
CREATE TABLE Motorist (
    Collision_ID INT PRIMARY KEY,
    Number_of_Motorist_Injured INT,
    Number_of_Motorist_Killed INT,
    FOREIGN KEY (Collision_ID) REFERENCES Collision
);

--done
INSERT INTO Motorist (Collision_ID, Number_of_Motorist_Injured, Number_of_Motorist_Killed)
SELECT
    COLLISION_ID,
    Number_of_Motorist_Injured,
    Number_of_Motorist_Killed
FROM staging_table1;

--- To check if tables are populated correctly

SELECT
    c.Collision_ID,
    cd.Crash_Date,
    cd.Crash_Time,
    b.Borough,
    z.ZIP_Code,
    co.Latitude,
    co.Longitude,
    co.Loc,
    st.On_Street_Name,
    st.Cross_Street_Name,
    st.Off_Street_Name,
    c.Number_of_Persons_Injured,
    c.Number_of_Persons_Killed,
    p.Number_of_Pedestrians_Injured,
    p.Number_of_Pedestrians_Killed,
    cy.Number_of_Cyclist_Injured,
    cy.Number_of_Cyclist_Killed,
    m.Number_of_Motorist_Injured,
    m.Number_of_Motorist_Killed,
    v1.Vehicle_Type_Code_1,
    v1.Contributing_Factor_Vehicle_1,
    v2.Vehicle_Type_Code_2,
    v2.Contributing_Factor_Vehicle_2,
    v3.Vehicle_Type_Code_3,
    v3.Contributing_Factor_Vehicle_3
FROM
    Collision c
    JOIN Collision_Details cd ON c.Collision_Details_ID = cd.Collision_Details_ID
    JOIN Coordinates co ON cd.Coordinates_ID = co.Coordinates_ID
    JOIN Street st ON cd.Street_ID = st.Street_ID
    JOIN Borough b ON cd.Borough_ID = b.Borough_ID
    JOIN ZIP_Code z ON cd.ZIP_CODE_ID = z.ZIP_Code_ID
    JOIN Pedestrian p ON c.Collision_ID = p.Collision_ID
    JOIN Cyclist cy ON c.Collision_ID = cy.Collision_ID
    JOIN Motorist m ON c.Collision_ID = m.Collision_ID
    JOIN Vehicle_Details vd ON c.Vehicle_Details_ID = vd.Vehicle_Details_ID
	JOIN Vehicle_1_Details v1 ON vd.Vehicle_1_ID = v1.vehicle_1_ID
	JOIN Vehicle_2_Details v2 ON vd.Vehicle_2_ID = v2.vehicle_2_ID
	JOIN Vehicle_3_Details v3 ON vd.Vehicle_3_ID = v3.vehicle_3_ID
WHERE
    c.Collision_ID = 4455765;



-------------------
-- Trigger
-- To prevent deletion from Collision Table

CREATE OR REPLACE FUNCTION prevent_delete()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'Deletion from Collision table is not allowed';
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER no_delete_collision
BEFORE DELETE ON Collision
FOR EACH ROW
EXECUTE FUNCTION prevent_delete();

-- Stored Procedure
CREATE OR REPLACE PROCEDURE load_collision_details()
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO Collision (
        Collision_ID,
        Number_of_Persons_Injured,
        Number_of_Persons_Killed,
        Collision_Details_ID,
        Vehicle_Details_ID
    )
    VALUES (999999999, 6, 2, 9999999, 999999      
    );
END;
$$;

-- Stored Procedure




--Performace Tuning Queries

--Business Question 1: Assess the safety and risk factors associated with traffic collisions on various streets within different boroughs.

SELECT 
  Street.On_Street_Name, 
  Borough.Borough,
  COUNT(DISTINCT Collision.Collision_ID) AS Total_Collisions,
  SUM(Pedestrian.Number_of_Pedestrians_Injured) AS Total_Pedestrians_Injured,
  COUNT(DISTINCT Vehicle_1_Details.Vehicle_Type_Code_1) AS Distinct_First_Vehicle_Types,
  SUM(CASE WHEN Motorist.Number_of_Motorist_Killed > 0 THEN 1 ELSE 0 END) AS Motorist_Fatalities,
  SUM(CASE WHEN Collision_Details.Crash_Time BETWEEN '00:00:00' AND '06:00:00' THEN 1 ELSE 0 END) AS NightTime_Collisions,
  SUM(CASE WHEN Vehicle_1_Details.Contributing_Factor_Vehicle_1 LIKE '%Speeding%' THEN 1 ELSE 0 END) AS Speeding_Collisions
FROM 
  Street
JOIN Collision_Details ON Street.Street_ID = Collision_Details.Street_ID
JOIN Borough ON Collision_Details.Borough_ID = Borough.Borough_ID
JOIN Collision ON Collision_Details.Collision_Details_ID = Collision.Collision_Details_ID
JOIN Pedestrian ON Collision.Collision_ID = Pedestrian.Collision_ID
LEFT JOIN Motorist ON Collision.Collision_ID = Motorist.Collision_ID
JOIN Vehicle_Details ON Collision.Vehicle_Details_ID = Vehicle_Details.Vehicle_Details_ID
JOIN Vehicle_1_Details ON Vehicle_Details.Vehicle_1_ID = Vehicle_1_Details.Vehicle_1_ID
GROUP BY 
  Street.On_Street_Name, 
  Borough.Borough
ORDER BY 
  Total_Collisions DESC;

-- Tuned Query
--Changes Made: Indexing Number_of_Pedestrians_Injured, Vehicke_Type_Code_1, Number_of_Motorists_KilledCrash_Time and Crash_Date

CREATE INDEX index_vtc_1 ON Vehicle_Details (Vehicle_Type_Code_1);
CREATE INDEX index_p_injured ON Pedestrian (Number_of_Pedestrians_Injured);
CREATE INDEX index_crash_time ON Collision_Details (Crash_Time);
CREATE INDEX index_crash_date ON Collision_Details (Crash_Date);
CREATE INDEX index_m_fatalities ON Motorist (Number_of_Motorist_Killed);

-- Implementing CTE after indexing
-- After CTE, the run time fell from 5.806 seconds to 4.045 seconds

WITH CollisionA AS (
    SELECT 
        Street.Street_ID,
        Collision_Details.Borough_ID,
        COUNT(DISTINCT Collision.Collision_ID) AS Total_Collisions,
        SUM(Pedestrian.Number_of_Pedestrians_Injured) AS Total_Pedestrians_Injured,
        COUNT(DISTINCT Vehicle_1_Details.Vehicle_Type_Code_1) AS Distinct_First_Vehicle_Types,
        SUM(CASE WHEN Motorist.Number_of_Motorist_Killed > 0 THEN 1 ELSE 0 END) AS Motorist_Fatalities,
        SUM(CASE WHEN Collision_Details.Crash_Time BETWEEN '00:00:00' AND '06:00:00' THEN 1 ELSE 0 END) AS NightTime_Collisions,
        SUM(CASE WHEN Vehicle_1_Details.Contributing_Factor_Vehicle_1 LIKE '%Speeding%' THEN 1 ELSE 0 END) AS Speeding_Collisions
    FROM 
        Street
    JOIN Collision_Details ON Street.Street_ID = Collision_Details.Street_ID
    JOIN Collision ON Collision_Details.Collision_Details_ID = Collision.Collision_Details_ID
    JOIN Pedestrian ON Collision.Collision_ID = Pedestrian.Collision_ID
    LEFT JOIN Motorist ON Collision.Collision_ID = Motorist.Collision_ID
    JOIN Vehicle_Details ON Collision.Vehicle_Details_ID = Vehicle_Details.Vehicle_Details_ID
    JOIN Vehicle_1_Details ON Vehicle_Details.Vehicle_1_ID = Vehicle_1_Details.Vehicle_1_ID
    GROUP BY 
        Street.Street_ID,
        Collision_Details.Borough_ID
)
SELECT 
    Street.On_Street_Name,
    Borough.Borough,
    COUNT(CollisionA.Total_Collisions) AS Total_Collisions,
    SUM(CollisionA.Total_Pedestrians_Injured) AS Total_Pedestrians_Injured,
    SUM(CollisionA.Distinct_First_Vehicle_Types) AS Distinct_First_Vehicle_Types,
    SUM(CollisionA.Motorist_Fatalities) AS Motorist_Fatalities,
    SUM(CollisionA.NightTime_Collisions) AS NightTime_Collisions,
    SUM(CollisionA.Speeding_Collisions) AS Speeding_Collisions
FROM CollisionA
JOIN Street ON Street.Street_ID = CollisionA.Street_ID
JOIN Borough ON Borough.Borough_ID = CollisionA.Borough_ID
GROUP BY
    Street.On_Street_Name,
    Borough.Borough
ORDER BY 
    COUNT(CollisionA.Total_Collisions) DESC;


------ Business Query 2: What are the key factors contributing to collisions on each street in terms of total collisions, pedestrian injuries, 
-- motorist fatalities, and speeding-related incidents, along with insights into the most common first vehicle types involved?

SELECT 
  Street.On_Street_Name,
  Borough.Borough,
  ZIP_Code.ZIP_Code,
  EXTRACT(MONTH FROM Collision_Details.Crash_Date) AS Collision_Month,
  COUNT(DISTINCT Collision.Collision_ID) AS Num_Collisions,
  SUM(Pedestrian.Number_of_Pedestrians_Injured) AS Pedestrians_Injured,
  SUM(Cyclist.Number_of_Cyclist_Injured) AS Cyclists_Injured,
  SUM(Motorist.Number_of_Motorist_Injured) AS Motorists_Injured,
  Vehicle_1_Details.Vehicle_Type_Code_1
FROM  Street
JOIN Collision_Details ON Street.Street_ID = Collision_Details.Street_ID
JOIN Borough ON Collision_Details.Borough_ID = Borough.Borough_ID
JOIN ZIP_Code ON Collision_Details.ZIP_Code_ID = ZIP_Code.ZIP_Code_ID
LEFT JOIN Collision ON Collision_Details.Collision_Details_ID = Collision.Collision_Details_ID
LEFT JOIN Pedestrian ON Collision.Collision_ID = Pedestrian.Collision_ID
LEFT JOIN Cyclist ON Collision.Collision_ID = Cyclist.Collision_ID
LEFT JOIN Motorist ON Collision.Collision_ID = Motorist.Collision_ID
LEFT JOIN Vehicle_Details ON Collision.Vehicle_Details_ID = Vehicle_Details.Vehicle_Details_ID
LEFT JOIN Vehicle_1_Details ON Vehicle_Details.Vehicle_1_ID = Vehicle_1_Details.Vehicle_1_ID
WHERE Collision_Details.Crash_Date BETWEEN '2021-04-01' AND '2024-08-31'
      AND Collision_Details.Crash_Time BETWEEN '05:00:00' AND '19:00:00'
GROUP BY Street.On_Street_Name, Borough.Borough, ZIP_Code.ZIP_Code, EXTRACT(MONTH FROM Collision_Details.Crash_Date), Vehicle_1_Details.Vehicle_Type_Code_1
ORDER BY Num_Collisions DESC, Collision_Month;

  
-- AFTER TUNING.

-- Pedestrians_Injured, Vehicle_Type_Code_1 already indexed and hence when the code was run again after indexing in the previous query, we saw a significant fall of 1.2 seconds.

-- Indexing Cyclists_Injured and Motorists_Injured
CREATE INDEX index_c_injured ON Cyclist (Number_of_Cyclist_Injured);
CREATE INDEX index_m_injured ON Motorist (Number_of_Motorist_Injured);

-- The run time fell from 3.858 seconds to 1.611 seconds only by indexing.
-- We will further implement a few more methods to see if we can bring it down further.

WITH CollisionB AS (
    SELECT 
        Street.On_Street_Name,
        Borough.Borough,
        ZIP_Code.ZIP_Code,
        EXTRACT(MONTH FROM Collision_Details.Crash_Date) AS Collision_Month,
        COUNT(DISTINCT Collision.Collision_ID) AS Num_Collisions,
        SUM(Pedestrian.Number_of_Pedestrians_Injured) AS Pedestrians_Injured,
        SUM(Cyclist.Number_of_Cyclist_Injured) AS Cyclists_Injured,
        SUM(Motorist.Number_of_Motorist_Injured) AS Motorists_Injured,
        Vehicle_1_Details.Vehicle_Type_Code_1
    FROM Street
    JOIN Collision_Details ON Street.Street_ID = Collision_Details.Street_ID
    JOIN Borough ON Collision_Details.Borough_ID = Borough.Borough_ID
    JOIN ZIP_Code ON Collision_Details.ZIP_Code_ID = ZIP_Code.ZIP_Code_ID
    LEFT JOIN Collision ON Collision_Details.Collision_Details_ID = Collision.Collision_Details_ID
    LEFT JOIN Pedestrian ON Collision.Collision_ID = Pedestrian.Collision_ID
    LEFT JOIN Cyclist ON Collision.Collision_ID = Cyclist.Collision_ID
    LEFT JOIN Motorist ON Collision.Collision_ID = Motorist.Collision_ID
    LEFT JOIN Vehicle_Details ON Collision.Vehicle_Details_ID = Vehicle_Details.Vehicle_Details_ID
    LEFT JOIN Vehicle_1_Details ON Vehicle_Details.Vehicle_1_ID = Vehicle_1_Details.Vehicle_1_ID
    WHERE Collision_Details.Crash_Date BETWEEN '2021-04-01' AND '2024-08-31'
        AND Collision_Details.Crash_Time BETWEEN '05:00:00' AND '19:00:00'
    GROUP BY 
        Street.On_Street_Name, 
        Borough.Borough, 
        ZIP_Code.ZIP_Code, 
        EXTRACT(MONTH FROM Collision_Details.Crash_Date), 
        Vehicle_1_Details.Vehicle_Type_Code_1
)
SELECT 
    On_Street_Name,
    Borough,
    ZIP_Code,
    Collision_Month,
    Num_Collisions,
    Pedestrians_Injured,
    Cyclists_Injured,
    Motorists_Injured,
    Vehicle_Type_Code_1
FROM  CollisionB
ORDER BY 
    Num_Collisions DESC, 
    Collision_Month;



-- After implementing CTE, our query was completed in 1.181 seconds

---- Business Question 3: What is the statistical overview of collisions that occurred on various streets and boroughs, considering factors such as the total number 
-- of collisions, pedestrians injured, types of vehicles involved, fatalities among motorists, evening collisions, alcohol-related incidents.

-- Query cOMPLETE TiME - 7.166 SECONDS

SELECT 
    Street.On_Street_Name, 
    Borough.Borough,
    Vehicle_1_Details.Vehicle_Type_Code_1,
    COUNT(Collision.Collision_ID) AS Total_Collisions,
    SUM(Pedestrian.Number_of_Pedestrians_Injured) AS Total_Pedestrians_Injured,
    SUM(CASE WHEN Motorist.Number_of_Motorist_Killed > 0 THEN 1 ELSE 0 END) AS Motorist_Fatalities,
    SUM(CASE WHEN Collision_Details.Crash_Time BETWEEN '18:00:01' AND '23:59:59' THEN 1 ELSE 0 END) AS Evening_Collisions,
    SUM(CASE WHEN Vehicle_1_Details.Contributing_Factor_Vehicle_1 LIKE '%Alcohol%' THEN 1 ELSE 0 END) AS Alcohol_Related_Collisions
FROM 
    Street
JOIN Collision_Details ON Street.Street_ID = Collision_Details.Street_ID
JOIN Borough ON Collision_Details.Borough_ID = Borough.Borough_ID
JOIN Collision ON Collision_Details.Collision_Details_ID = Collision.Collision_Details_ID
JOIN Pedestrian ON Collision.Collision_ID = Pedestrian.Collision_ID
LEFT JOIN Motorist ON Collision.Collision_ID = Motorist.Collision_ID
JOIN Vehicle_Details ON Collision.Vehicle_Details_ID = Vehicle_Details.Vehicle_Details_ID
JOIN Vehicle_1_Details ON Vehicle_Details.Vehicle_1_ID = Vehicle_1_Details.Vehicle_1_ID
GROUP BY 
    Street.On_Street_Name, 
    Borough.Borough, 
    Vehicle_1_Details.Vehicle_Type_Code_1
ORDER BY 
    Total_Pedestrians_Injured DESC;

--- Indexing only Contributing_Factor_Vehicle_1, as all other attributes were indexed in the previous queries.
-- On executing the query after indexing, our query took 4.509 seconds to complete instead of the initial value of 7.166.

-- Using Window Function and introducing partition we get the query output in 3.725 seconds

CREATE INDEX index_CFV_1 ON Vehicle_Details (contributing_factor_vehicle_1);

WITH CollisionData AS (
    SELECT 
        Street.On_Street_Name, 
        Borough.Borough,
        Vehicle_1_Details.Vehicle_Type_Code_1,
        Collision.Collision_ID,
        Pedestrian.Number_of_Pedestrians_Injured,
        CASE WHEN Motorist.Number_of_Motorist_Killed > 0 THEN 1 ELSE 0 END AS Motorist_Fatalities,
        CASE WHEN Collision_Details.Crash_Time BETWEEN '18:00:01' AND '23:59:59' THEN 1 ELSE 0 END AS Evening_Collision,
        CASE WHEN Vehicle_1_Details.Contributing_Factor_Vehicle_1 LIKE '%Alcohol%' THEN 1 ELSE 0 END AS Alcohol_Related_Collision
    FROM Street
    JOIN Collision_Details ON Street.Street_ID = Collision_Details.Street_ID
    JOIN Borough ON Collision_Details.Borough_ID = Borough.Borough_ID
    JOIN Collision ON Collision_Details.Collision_Details_ID = Collision.Collision_Details_ID
    JOIN Pedestrian ON Collision.Collision_ID = Pedestrian.Collision_ID
    LEFT JOIN Motorist ON Collision.Collision_ID = Motorist.Collision_ID
    JOIN Vehicle_Details ON Collision.Vehicle_Details_ID = Vehicle_Details.Vehicle_Details_ID
    JOIN Vehicle_1_Details ON Vehicle_Details.Vehicle_1_ID = Vehicle_1_Details.Vehicle_1_ID
)
SELECT 
    On_Street_Name,
    Borough,
    Vehicle_Type_Code_1,
    COUNT(Collision_ID) AS Total_Collisions,
    SUM(Number_of_Pedestrians_Injured) AS Total_Pedestrians_Injured,
    SUM(Motorist_Fatalities) AS Total_Motorist_Fatalities,
    SUM(Evening_Collision) AS Total_Evening_Collisions,
    SUM(Alcohol_Related_Collision) AS Total_Alcohol_Related_Collisions
FROM CollisionData
GROUP BY 
    On_Street_Name,
    Borough,
    Vehicle_Type_Code_1
ORDER BY Total_Pedestrians_Injured DESC;


	
-- -- BUSINESS QUESTION 4: What is the distribution of collisions involving sedans across different streets and boroughs, considering the number of persons injured in 
--each collision?

-- This query takes 5.976 seconds to run.

SELECT
    Collision.COLLISION_ID,
    Street.On_Street_Name,
    Borough.Borough,
    Collision_Details.ZIP_CODE_ID,
    Vehicle_1_Details.Vehicle_Type_Code_1,
    Vehicle_2_Details.Vehicle_Type_Code_2,
    Vehicle_3_Details.Vehicle_Type_Code_3,
    Collision.Number_Of_Persons_Injured,
    COUNT(DISTINCT Collision.COLLISION_ID) AS Total_Collisions
FROM Vehicle_Details
JOIN Vehicle_1_Details ON Vehicle_Details.Vehicle_1_ID = Vehicle_1_Details.Vehicle_1_ID
JOIN Vehicle_2_Details ON Vehicle_Details.Vehicle_2_ID = Vehicle_2_Details.Vehicle_2_ID
JOIN Vehicle_3_Details ON Vehicle_Details.Vehicle_3_ID = Vehicle_3_Details.Vehicle_3_ID
JOIN Collision ON Vehicle_Details.Vehicle_Details_ID = Collision.Vehicle_Details_ID
JOIN Collision_Details ON Collision.Collision_Details_ID = Collision_Details.Collision_Details_ID
JOIN Street ON Collision_Details.Street_ID = Street.Street_ID
JOIN Borough ON Collision_Details.Borough_ID = Borough.Borough_ID
WHERE 
    Vehicle_1_Details.Vehicle_Type_Code_1 = 'Sedan' AND 
    Vehicle_2_Details.Vehicle_Type_Code_2 = 'Sedan' AND 
    Vehicle_3_Details.Vehicle_Type_Code_3 = 'Sedan'
GROUP BY Collision.COLLISION_ID,
    Street.On_Street_Name,
    Borough.Borough,
    Collision_Details.ZIP_CODE_ID,
    Vehicle_1_Details.Vehicle_Type_Code_1,
    Vehicle_2_Details.Vehicle_Type_Code_2,
    Vehicle_3_Details.Vehicle_Type_Code_3,
    Collision.Number_Of_Persons_Injured
ORDER BY COUNT(DISTINCT Collision.COLLISION_ID) DESC;

--AFTER TUNING
-- Indexing already done

WITH CollisionData AS (
    SELECT
        Collision.COLLISION_ID,
        Street.On_Street_Name,
        Borough.Borough,
        Collision_Details.ZIP_CODE_ID,
        Vehicle_1_Details.Vehicle_Type_Code_1,
        Vehicle_2_Details.Vehicle_Type_Code_2,
        Vehicle_3_Details.Vehicle_Type_Code_3,
        Collision.Number_Of_Persons_Injured,
        Vehicle_Details.Vehicle_Details_ID
    FROM Vehicle_Details
    JOIN Vehicle_1_Details ON Vehicle_Details.Vehicle_1_ID = Vehicle_1_Details.Vehicle_1_ID
    JOIN Vehicle_2_Details ON Vehicle_Details.Vehicle_2_ID = Vehicle_2_Details.Vehicle_2_ID
    JOIN Vehicle_3_Details ON Vehicle_Details.Vehicle_3_ID = Vehicle_3_Details.Vehicle_3_ID
    JOIN Collision ON Vehicle_Details.Vehicle_Details_ID = Collision.Vehicle_Details_ID
    JOIN Collision_Details ON Collision.Collision_Details_ID = Collision_Details.Collision_Details_ID
    JOIN Street ON Collision_Details.Street_ID = Street.Street_ID
    JOIN Borough ON Collision_Details.Borough_ID = Borough.Borough_ID
    WHERE 
        Vehicle_1_Details.Vehicle_Type_Code_1 = 'Sedan' AND 
        Vehicle_2_Details.Vehicle_Type_Code_2 = 'Sedan' AND 
        Vehicle_3_Details.Vehicle_Type_Code_3 = 'Sedan'
)
SELECT
    COLLISION_ID,
    On_Street_Name,
    Borough,
    ZIP_CODE_ID,
    Vehicle_Type_Code_1,
    Vehicle_Type_Code_2,
    Vehicle_Type_Code_3,
    Number_Of_Persons_Injured,
    COUNT(DISTINCT COLLISION_ID) AS Total_Collisions
FROM CollisionData
GROUP BY
    COLLISION_ID,
    On_Street_Name,
    Borough,
    ZIP_CODE_ID,
    Vehicle_Type_Code_1,
    Vehicle_Type_Code_2,
    Vehicle_Type_Code_3,
    Number_Of_Persons_Injured
ORDER BY Total_Collisions DESC;

-- Our query complete time dropped to 0.750.
