# US Household Income Analysis
## Data dictionary
The **US Household Income Dataset** contains detailed information on various geographic and demographic aspects of cities and counties across the United States. The dataset includes the following key columns:

- row_id: A unique identifier for each row in the dataset.
- id: Another identifier for the data entries.
- State_Code: Numerical code representing the state.
- State_Name: Full name of the state.
- State_ab: Abbreviation of the state name.
- County: Name of the county.
- City: Name of the city.
- Place: Specific place or area within the city or county.
- Type: Type of geographic area (e.g., track).
- Primary: Primary designation of the area.
- Zip_Code: Postal code for the area.
- Area_Code: Telephone area code.
- ALand: Land area in square meters.
- AWater: Water area in square meters.
- Lat: Latitude coordinate.
- Lon: Longitude coordinate.

## Project Requirements
### Data Cleaning

**Task 1: Data Consistency**:  
Check for and correct inconsistencies.

**Task 2: Removing Duplicates**:  
Identify and remove duplicate rows if any.

### EDA

**Task 0: Summarizing Data by State**  
Assume that you will have new data each week. Can you create store procedure and create event to active procedure every weeks to update and clean new data (From Cleaning Tasks)?

**Task 1: Summarizing Data by State**  
Can you provide a summary report that shows the average land area and average water area for each state? Please include the state name and abbreviation, and order the results by the average land area in descending order.

**Task 2: Filtering Cities by Population Range**  
We need a list of all cities where the land area is between 50,000,000 and 100,000,000 square meters. Include the city name, state name, and county in the results, and order the list alphabetically by city name.

**Task 3: Counting Cities per State**  
Can you generate a report that counts the number of cities in each state? The report should include the state name and abbreviation and be ordered by the number of cities in descending order.

**Task 4: Identifying Counties with Significant Water Area**  
Please identify the top 10 counties with the highest total water area. The report should include the county name, state name, and total water area, ordered by total water area in descending order.

**Task 5: Finding Cities Near Specific Coordinates**  
We are looking for a list of all cities within a specific latitude and longitude range (latitude between 30 and 35, longitude between -90 and -85). Include the city name, state name, county, and coordinates, and order the results by latitude and then by longitude.

**Task 6: Using Window Functions for Ranking**  
We need to rank cities within each state based on their land area. Please use a window function to assign ranks and include the city name, state name, land area, and rank in your results. The report should be ordered by state name and rank.

**Task 7: Creating Aggregate Reports**  
Can you generate a report showing the total land area and water area for each state, along with the number of cities in each state? Include the state name and abbreviation, and order the results by the total land area in descending order.

**Task 8: Subqueries for Detailed Analysis**  
Can you provide a list of all cities where the land area is above the average land area of all cities? Use a subquery to calculate the average land area. The report should include the city name, state name, and land area, ordered by land area in descending order.

**Task 9: Identifying Cities with High Water to Land Ratios**  
Can you identify cities where the water area is greater than 50% of the land area? Include the city name, state name, land area, water area, and the calculated water to land ratio, and order the results by the water to land ratio in descending order.

**Task 10: Dynamic SQL for Custom Reports**  
Can you create a stored procedure that accepts a state abbreviation as input and returns a detailed report for that state? The report should include the total number of cities, average land area, average water area, and a list of all cities with their respective land and water areas.

**Task 11: Creating and Using Temporary Tables**  
We need to create a temporary table that stores the top 20 cities by land area for further analysis. Use this temporary table to calculate the average water area of these top 20 cities, and include the city name, state name, land area, and water area in your final report.

**Task 12: Complex Multi-Level Subqueries**  
Can you list all states where the average land area of cities is greater than the overall average land area across all cities in the dataset? Use multiple subqueries to calculate the overall average land area and the state-wise average land areas. Include the state name and average land area in your results.

**Task 13: Optimizing Indexes for Query Performance**  
Can you analyze the impact of indexing on query performance? Create indexes on the columns State_Name, City, and County, and compare the execution time of a complex query before and after indexing. Provide insights into how indexing improved (or did not improve) the query performance, including execution times and query plans.

**Task 14: Recursive Common Table Expressions (CTEs)**  
Can you create a recursive CTE that calculates the cumulative land area for cities within each state, ordered by city name? Include the city name, state name, individual land area, and cumulative land area in your results.

**Task 15: Data Anomalies Detection**  
Can you detect anomalies in the dataset, such as cities with unusually high or low land areas compared to the state average? Use statistical methods like Z-score or standard deviation to identify these anomalies. Include the city name, state name, land area, state average land area, and anomaly score in your results, ordered by the anomaly score in descending order.

**Task 16: Stored Procedures for Complex Calculations**  
Can you write a stored procedure that performs complex calculations, such as predicting future land and water area based on historical trends? The stored procedure should accept parameters for the city and state and return predicted values. Test the stored procedure with different inputs and document the results.

**Task 17: Implementing Triggers for Data Integrity**  
Can you write a trigger that automatically updates a summary table whenever new data is inserted, updated, or deleted in the main dataset? The summary table should include aggregated information such as total land area and water area by state. Test the trigger to ensure it correctly updates the summary table in response to changes in the main dataset.

**Task 18: Advanced Data Encryption and Security**  
We need to implement data encryption and ensure secure access to sensitive information in the dataset. Use MySQL encryption functions to encrypt columns such as Zip_Code and Area_Code. Demonstrate how to decrypt the data for authorized users and discuss the implications for data security.

**Task 19: Geospatial Analysis**  
We need to identify cities that fall within a specified radius from a given point (latitude and longitude). Include calculations to determine the distance between the given point and each city. Return the city name, state name, county, and calculated distance.

**Task 20: Analyzing Correlations**  
Can you calculate the correlation between land area (ALand) and water area (AWater) for each state? Use statistical functions to determine the strength of the correlation. Include the state name and the correlation coefficient in your results.

**Task 21: Hotspot Detection**  
Can you identify “hotspots” where the combination of land area and water area significantly deviates from the norm? Use statistical methods such as Z-scores or clustering to detect these hotspots. Include city name, state name, land area, water area, and the deviation score in your report.

**Task 22: Resource Allocation Optimization**  
We need to optimize the allocation of resources (e.g., funding) based on the land and water area of each city. Create a model to distribute resources in a way that maximizes efficiency and equity. Include the city name, state name, land area, water area, and allocated resources in your results.
