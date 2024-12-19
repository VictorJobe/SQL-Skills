--All these queries are based on the AdventureWorks database
--My goal is to showcase my SQL skills with these queries.

--Query 1
--Write a query that displays the details of orders shipped on the date 3-12-2013.
--Add a row at the bottom of the table showing the total sum of all orders.

SELECT 
    CAST(SalesOrderID AS VARCHAR) AS SalesOrderID,
    OrderDate,
    ShipDate,
    Status,
    CustomerID,
    FORMAT(SUM(TotalDue), 'C', 'EN-US') AS TotalDue
FROM 
    Sales.SalesOrderHeader
WHERE 
    ShipDate = '2013-12-03'
GROUP BY 
    SalesOrderID, OrderDate, ShipDate, Status, CustomerID

UNION ALL

SELECT 
    'Total' AS SalesOrderID,
    NULL AS OrderDate,
    NULL AS ShipDate,
    NULL AS Status,
    NULL AS CustomerID,
    FORMAT(SUM(TotalDue), 'C', 'EN-US') AS TotalDue
FROM 
    Sales.SalesOrderHeader
WHERE 
    ShipDate = '2013-12-03'


--Query 2
--Write a query that displays the 5 products with the lowest prices for each category.
--Show the columns: Category ID, Category Name, and Product Price.

WITH RankedProducts AS (
    SELECT 
        p.ProductID,
        p.Name AS ProductName,
        pc.ProductCategoryID,
        pc.Name AS CategoryName,
        pp.ListPrice,
        ROW_NUMBER() OVER (PARTITION BY pc.ProductCategoryID ORDER BY pp.ListPrice ASC) AS RankInCategory
    FROM 
        Production.Product AS p
    INNER JOIN 
        Production.ProductSubcategory AS ps ON p.ProductSubcategoryID = ps.ProductSubcategoryID
    INNER JOIN 
        Production.ProductCategory AS pc ON ps.ProductCategoryID = pc.ProductCategoryID
    INNER JOIN 
        Production.ProductListPriceHistory AS pp ON p.ProductID = pp.ProductID
    WHERE 
        pp.ListPrice > 0  -- Remove products with no price
)
SELECT 
    ProductCategoryID AS CategoryID,
    CategoryName,
    ListPrice AS ProductPrice
FROM 
    RankedProducts
WHERE 
    RankInCategory <= 5
ORDER BY 
    CategoryID, ProductPrice;


--Query 3
--Perform a ranking only for the countries USA (US), Canada (CA), and France (FR).
--Display only the cities ranked in the top three places for each country.

WITH RankedCities AS (
    SELECT 
        sp.CountryRegionCode,
        a.City,
        ROW_NUMBER() OVER (PARTITION BY sp.CountryRegionCode ORDER BY a.City ASC) AS CityRank
    FROM 
        Person.Address AS a
    INNER JOIN 
        Person.StateProvince AS sp ON a.StateProvinceID = sp.StateProvinceID
    WHERE 
        sp.CountryRegionCode IN ('US', 'CA', 'FR') -- Filter for specific countries
)
SELECT 
    CountryRegionCode,
    City,
    CityRank
FROM 
    RankedCities
WHERE 
    CityRank <= 3 -- Get top 3 cities for each country
ORDER BY 
    CountryRegionCode, CityRank

--Query 4
--Write a query that divides the Products table into 5 groups, 
--with the sorting based on the Price column.

SELECT ProductNumber,ListPrice,NTILE(5)OVER(ORDER BY ListPrice) AS "NLT" 
FROM Production.Product
WHERE ListPrice !=0

--Query 5
--Write a query that displays for each salesperson their total sales for each year, 
--the sales target for that year, and the difference between 
--the sales and the sales target.
WITH Sales_CTE AS (
    SELECT 
        SalesPersonID,
        SUM(TotalDue) AS TotalSales,
        YEAR(OrderDate) AS SalesYear
    FROM Sales.SalesOrderHeader
    WHERE SalesPersonID IS NOT NULL
    GROUP BY SalesPersonID, YEAR(OrderDate)
),
Sales_Quota_CTE AS (
    SELECT 
        BusinessEntityID, 
        SUM(SalesQuota) AS SalesQuota,
        YEAR(QuotaDate) AS SalesQuotaYear
    FROM Sales.SalesPersonQuotaHistory
    GROUP BY BusinessEntityID, YEAR(QuotaDate)
)
SELECT 
    S.SalesPersonID, 
    P.FirstName + ' ' + P.LastName AS [Emp's Name], 
    S.SalesYear,
    FORMAT(S.TotalSales, 'C', 'en-us') AS TotalSales, 
    SQ.SalesQuotaYear,
    FORMAT(SQ.SalesQuota, 'C', 'en-us') AS SalesQuota,
    FORMAT(S.TotalSales - SQ.SalesQuota, 'C', 'en-us') AS Above_or_Below_Quota
FROM Sales_CTE S
JOIN Sales_Quota_CTE SQ 
    ON SQ.BusinessEntityID = S.SalesPersonID
    AND S.SalesYear = SQ.SalesQuotaYear
JOIN Person.Person P 
    ON P.BusinessEntityID = S.SalesPersonID
ORDER BY S.SalesPersonID, S.SalesYear;

--QUERY 6
--Write a query that displays for each rank in the company, 
--the number of male employees and the number of female employees.

SELECT OrganizationLevel,M AS "Male",F AS "Female"
FROM (
	SELECT OrganizationLevel,Gender
	FROM HumanResources.Employee) E
	PIVOT(COUNT(Gender)FOR Gender IN([M],[F]))PIV
