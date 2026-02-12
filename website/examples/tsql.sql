-- T-SQL (SQL Server) formatting examples
-- Exercises a broad range of T-SQL syntax for formatter validation

/* ===== DDL: Pre-check drop patterns and table creation ===== */

IF OBJECT_ID(N'dbo.AuditLog', N'U') IS NOT NULL DROP TABLE dbo.AuditLog;

IF OBJECT_ID(N'dbo.OrderLineItems', N'U') IS NOT NULL DROP TABLE dbo.OrderLineItems;

IF OBJECT_ID(N'dbo.Orders', N'U') IS NOT NULL DROP TABLE dbo.Orders;

IF OBJECT_ID(N'dbo.Products', N'U') IS NOT NULL DROP TABLE dbo.Products;

IF OBJECT_ID(N'dbo.Customers', N'U') IS NOT NULL DROP TABLE dbo.Customers;

IF OBJECT_ID(N'dbo.Employees', N'U') IS NOT NULL DROP TABLE dbo.Employees;

GO

CREATE TABLE dbo.Customers (CustomerId INT IDENTITY(1, 1) NOT NULL, ExternalRef UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID(), FirstName NVARCHAR(100) NOT NULL, LastName NVARCHAR(100) NOT NULL, Email NVARCHAR(256) NULL, Phone VARCHAR(20) NULL, CreditLimit MONEY NOT NULL DEFAULT 0, SmallBalance SMALLMONEY NULL, DateOfBirth DATE NULL, CreatedAt DATETIME2(3) NOT NULL DEFAULT SYSDATETIME(), ModifiedAt DATETIMEOFFSET NOT NULL DEFAULT SYSDATETIMEOFFSET(), RowVer ROWVERSION, Notes NVARCHAR(MAX) NULL, ProfilePhoto VARBINARY(MAX) NULL, IsActive BIT NOT NULL DEFAULT 1, LoyaltyTier TINYINT NOT NULL DEFAULT 0, FullName AS (FirstName + N' ' + LastName), FullNamePersisted AS (FirstName + N' ' + LastName) PERSISTED, CONSTRAINT PK_Customers PRIMARY KEY CLUSTERED (CustomerId), CONSTRAINT UQ_Customers_Email UNIQUE NONCLUSTERED (Email), CONSTRAINT UQ_Customers_ExternalRef UNIQUE NONCLUSTERED (ExternalRef), CONSTRAINT CK_Customers_CreditLimit CHECK (CreditLimit >= 0));
GO

CREATE TABLE dbo.Products (ProductId INT IDENTITY(1, 1) NOT NULL, Sku VARCHAR(50) NOT NULL, ProductName NVARCHAR(200) NOT NULL, Category NVARCHAR(100) NOT NULL, SubCategory NVARCHAR(100) NULL, UnitPrice MONEY NOT NULL, Weight DECIMAL(10, 2) NULL, IsDiscontinued BIT NOT NULL DEFAULT 0, LaunchDate DATE NULL, Metadata NVARCHAR(MAX) NULL, LegacyImage IMAGE NULL, PriceWithTax AS (UnitPrice * 1.08) PERSISTED, CONSTRAINT PK_Products PRIMARY KEY CLUSTERED (ProductId), CONSTRAINT UQ_Products_Sku UNIQUE NONCLUSTERED (Sku));
GO

CREATE TABLE dbo.Orders (OrderId INT IDENTITY(1000, 1) NOT NULL, CustomerId INT NOT NULL, OrderDate DATETIME2(0) NOT NULL DEFAULT SYSDATETIME(), ShipDate DATETIME2(0) NULL, Status NVARCHAR(20) NOT NULL DEFAULT N'Pending', TotalAmount MONEY NOT NULL DEFAULT 0, DiscountPct DECIMAL(5, 2) NULL, Notes NVARCHAR(MAX) NULL, CONSTRAINT PK_Orders PRIMARY KEY CLUSTERED (OrderId), CONSTRAINT FK_Orders_Customers FOREIGN KEY (CustomerId) REFERENCES dbo.Customers (CustomerId));
GO

CREATE TABLE dbo.OrderLineItems (LineItemId INT IDENTITY(1, 1) NOT NULL, OrderId INT NOT NULL, ProductId INT NOT NULL, Quantity SMALLINT NOT NULL DEFAULT 1, UnitPrice MONEY NOT NULL, LineTotal AS (Quantity * UnitPrice), CONSTRAINT PK_OrderLineItems PRIMARY KEY CLUSTERED (LineItemId), CONSTRAINT FK_LineItems_Orders FOREIGN KEY (OrderId) REFERENCES dbo.Orders (OrderId), CONSTRAINT FK_LineItems_Products FOREIGN KEY (ProductId) REFERENCES dbo.Products (ProductId));
GO

CREATE TABLE dbo.Employees (EmployeeId INT IDENTITY(1, 1) NOT NULL, ManagerId INT NULL, DepartmentName NVARCHAR(100) NOT NULL, FullName NVARCHAR(200) NOT NULL, HireDate DATE NOT NULL, Salary MONEY NOT NULL, Region NVARCHAR(50) NULL, CONSTRAINT PK_Employees PRIMARY KEY CLUSTERED (EmployeeId), CONSTRAINT FK_Employees_Manager FOREIGN KEY (ManagerId) REFERENCES dbo.Employees (EmployeeId));
GO

CREATE TABLE dbo.AuditLog (AuditId BIGINT IDENTITY(1, 1) NOT NULL, TableName NVARCHAR(128) NOT NULL, Operation NVARCHAR(10) NOT NULL, RecordId INT NOT NULL, ChangedBy NVARCHAR(128) NOT NULL DEFAULT SUSER_SNAME(), ChangedAt DATETIME2(3) NOT NULL DEFAULT SYSDATETIME(), OldValues NVARCHAR(MAX) NULL, NewValues NVARCHAR(MAX) NULL, CONSTRAINT PK_AuditLog PRIMARY KEY CLUSTERED (AuditId));
GO

/* ===== Indexes: clustered, nonclustered, filtered ===== */

CREATE NONCLUSTERED INDEX IX_Orders_CustomerId ON dbo.Orders (CustomerId) INCLUDE (OrderDate, TotalAmount);
GO

CREATE NONCLUSTERED INDEX IX_Orders_Status ON dbo.Orders (Status) INCLUDE (CustomerId, OrderDate) WHERE Status <> N'Cancelled';
GO

CREATE NONCLUSTERED INDEX IX_Products_Category ON dbo.Products (Category, SubCategory) INCLUDE (ProductName, UnitPrice) WHERE IsDiscontinued = 0;
GO

CREATE NONCLUSTERED INDEX IX_Employees_Dept_Region ON dbo.Employees (DepartmentName, Region) INCLUDE (FullName, Salary);
GO

/* ===== ALTER TABLE: add constraints after the fact ===== */

ALTER TABLE dbo.Orders ADD CONSTRAINT CK_Orders_Status CHECK (Status IN (N'Pending', N'Shipped', N'Delivered', N'Cancelled', N'Returned'));

ALTER TABLE dbo.OrderLineItems ADD CONSTRAINT CK_LineItems_Quantity CHECK (Quantity > 0);

ALTER TABLE dbo.Employees ADD CONSTRAINT CK_Employees_Salary CHECK (Salary >= 0);
GO

/* ===== Control flow, variables, session commands ===== */

DECLARE @Today DATE = GETDATE();

DECLARE @CutoffDate DATE;

SET @CutoffDate = DATEADD(MONTH, -6, @Today);

PRINT N'Cutoff date for stale orders: ' + CONVERT(NVARCHAR(10), @CutoffDate, 120);

IF EXISTS (SELECT 1 FROM dbo.Orders WHERE OrderDate < @CutoffDate AND Status = N'Pending')
BEGIN
    PRINT N'Found stale pending orders older than 6 months';
    UPDATE dbo.Orders SET Status = N'Cancelled' WHERE OrderDate < @CutoffDate AND Status = N'Pending';
    PRINT N'Stale orders cancelled: ' + CAST(@@ROWCOUNT AS NVARCHAR(10));
END;
GO

EXEC sp_rename N'dbo.Customers.Phone', N'PhoneNumber', N'COLUMN';
GO

DBCC CHECKIDENT (N'dbo.Orders', RESEED, 1000);
GO

/* ===== SELECT: TOP variants ===== */

SELECT TOP 10 c.CustomerId, c.FullNamePersisted, c.Email, c.CreditLimit FROM dbo.Customers c ORDER BY c.CreditLimit DESC;

SELECT TOP 5 PERCENT o.OrderId, o.CustomerId, o.TotalAmount, o.OrderDate FROM dbo.Orders o ORDER BY o.TotalAmount DESC;

SELECT TOP 10 WITH TIES e.FullName, e.Salary, e.DepartmentName FROM dbo.Employees e ORDER BY e.Salary DESC;

/* ===== Table hints ===== */

SELECT c.CustomerId, c.FirstName, c.LastName, c.Email FROM dbo.Customers c WITH (NOLOCK) WHERE c.IsActive = 1;

SELECT o.OrderId, o.TotalAmount FROM dbo.Orders o WITH (ROWLOCK) WHERE o.Status = N'Pending';

UPDATE dbo.Products WITH (UPDLOCK, HOLDLOCK) SET UnitPrice = UnitPrice * 1.05 WHERE Category = N'Electronics' AND IsDiscontinued = 0;

SELECT o.OrderId, o.TotalAmount, li.ProductId, li.Quantity FROM dbo.Orders o WITH (NOLOCK) INNER JOIN dbo.OrderLineItems li WITH (NOLOCK) ON o.OrderId = li.OrderId WHERE o.OrderDate >= '2024-01-01';

/* ===== CROSS APPLY and OUTER APPLY ===== */

SELECT c.CustomerId, c.FullNamePersisted, latest.OrderId, latest.OrderDate, latest.TotalAmount FROM dbo.Customers c OUTER APPLY (SELECT TOP 1 o.OrderId, o.OrderDate, o.TotalAmount FROM dbo.Orders o WHERE o.CustomerId = c.CustomerId ORDER BY o.OrderDate DESC) latest WHERE c.IsActive = 1;

SELECT c.CustomerId, c.FullNamePersisted, topItems.ProductId, topItems.Quantity, topItems.LineTotal FROM dbo.Customers c CROSS APPLY (SELECT TOP 3 li.ProductId, li.Quantity, li.LineTotal FROM dbo.Orders o INNER JOIN dbo.OrderLineItems li ON o.OrderId = li.OrderId WHERE o.CustomerId = c.CustomerId ORDER BY li.LineTotal DESC) topItems;

SELECT e.FullName, e.DepartmentName, subordinates.DirectReportCount FROM dbo.Employees e OUTER APPLY (SELECT COUNT(*) AS DirectReportCount FROM dbo.Employees sub WHERE sub.ManagerId = e.EmployeeId) subordinates;

/* ===== PIVOT and UNPIVOT ===== */

SELECT CustomerId, [Pending], [Shipped], [Delivered], [Cancelled] FROM (SELECT o.CustomerId, o.Status, o.TotalAmount FROM dbo.Orders o) src PIVOT (SUM(TotalAmount) FOR Status IN ([Pending], [Shipped], [Delivered], [Cancelled])) pvt ORDER BY CustomerId;

SELECT p.ProductId, p.ProductName, yr.OrderYear, yr.YearlyRevenue FROM dbo.Products p CROSS APPLY (SELECT YEAR(o.OrderDate) AS OrderYear, SUM(li.LineTotal) AS YearlyRevenue FROM dbo.OrderLineItems li INNER JOIN dbo.Orders o ON li.OrderId = o.OrderId WHERE li.ProductId = p.ProductId GROUP BY YEAR(o.OrderDate)) yr;

SELECT CustomerId, StatusLabel, StatusAmount FROM (SELECT CustomerId, [Pending], [Shipped], [Delivered] FROM (SELECT o.CustomerId, o.Status, o.TotalAmount FROM dbo.Orders o) src PIVOT (SUM(TotalAmount) FOR Status IN ([Pending], [Shipped], [Delivered])) pvt) p UNPIVOT (StatusAmount FOR StatusLabel IN ([Pending], [Shipped], [Delivered])) unpvt;

/* ===== MERGE: upsert and sync ===== */

MERGE dbo.Products AS tgt USING (SELECT 'WIDGET-001' AS Sku, N'Premium Widget' AS ProductName, N'Accessories' AS Category, 29.99 AS UnitPrice) AS src ON tgt.Sku = src.Sku WHEN MATCHED THEN UPDATE SET tgt.ProductName = src.ProductName, tgt.UnitPrice = src.UnitPrice WHEN NOT MATCHED BY TARGET THEN INSERT (Sku, ProductName, Category, UnitPrice) VALUES (src.Sku, src.ProductName, src.Category, src.UnitPrice) OUTPUT $action AS MergeAction, INSERTED.ProductId, INSERTED.Sku, INSERTED.ProductName;

MERGE dbo.Customers AS tgt USING (SELECT N'John' AS FirstName, N'Doe' AS LastName, N'john.doe@example.com' AS Email, 5000.00 AS CreditLimit) AS src ON tgt.Email = src.Email WHEN MATCHED THEN UPDATE SET tgt.CreditLimit = src.CreditLimit, tgt.ModifiedAt = SYSDATETIMEOFFSET() WHEN NOT MATCHED BY TARGET THEN INSERT (FirstName, LastName, Email, CreditLimit) VALUES (src.FirstName, src.LastName, src.Email, src.CreditLimit) WHEN NOT MATCHED BY SOURCE AND tgt.IsActive = 0 THEN DELETE OUTPUT $action, INSERTED.CustomerId, INSERTED.Email, DELETED.CustomerId AS OldCustomerId;

/* ===== TRY_CONVERT, TRY_CAST, CONVERT with style codes ===== */

SELECT TRY_CONVERT(INT, N'12345') AS SafeInt, TRY_CONVERT(INT, N'abc') AS FailedInt, TRY_CAST(N'2024-03-15' AS DATE) AS SafeDate, TRY_CAST(N'not-a-date' AS DATE) AS FailedDate, CONVERT(VARCHAR(10), GETDATE(), 120) AS IsoDate, CONVERT(VARCHAR(20), GETDATE(), 101) AS UsDate, CONVERT(VARCHAR(30), GETDATE(), 113) AS EuroDatetime, CONVERT(NVARCHAR(50), CAST(1234.56 AS MONEY), 1) AS FormattedMoney;

/* ===== IIF, CHOOSE, COALESCE, ISNULL ===== */

SELECT c.CustomerId, c.FullNamePersisted, IIF(c.CreditLimit > 10000, N'Premium', N'Standard') AS Tier, CHOOSE(c.LoyaltyTier + 1, N'Bronze', N'Silver', N'Gold', N'Platinum') AS LoyaltyLabel, COALESCE(c.Email, c.PhoneNumber, N'No contact info') AS PrimaryContact, ISNULL(c.Notes, N'') AS SafeNotes FROM dbo.Customers c;

/* ===== STRING_AGG ===== */

SELECT o.OrderId, STRING_AGG(p.ProductName, N', ') WITHIN GROUP (ORDER BY p.ProductName) AS ProductList, SUM(li.LineTotal) AS OrderTotal FROM dbo.Orders o INNER JOIN dbo.OrderLineItems li ON o.OrderId = li.OrderId INNER JOIN dbo.Products p ON li.ProductId = p.ProductId GROUP BY o.OrderId;

SELECT e.DepartmentName, STRING_AGG(e.FullName, N'; ') WITHIN GROUP (ORDER BY e.Salary DESC) AS EmployeesBySalary, COUNT(*) AS HeadCount, AVG(e.Salary) AS AvgSalary FROM dbo.Employees e GROUP BY e.DepartmentName;

/* ===== OFFSET / FETCH (pagination) ===== */

SELECT o.OrderId, o.CustomerId, o.OrderDate, o.TotalAmount, o.Status FROM dbo.Orders o ORDER BY o.OrderDate DESC OFFSET 0 ROWS FETCH NEXT 25 ROWS ONLY;

SELECT p.ProductId, p.Sku, p.ProductName, p.UnitPrice, p.Category FROM dbo.Products p WHERE p.IsDiscontinued = 0 ORDER BY p.Category, p.ProductName OFFSET 50 ROWS FETCH NEXT 25 ROWS ONLY;

/* ===== OPTION hints ===== */

SELECT c.CustomerId, c.FullNamePersisted, COUNT(o.OrderId) AS OrderCount, SUM(o.TotalAmount) AS TotalSpent FROM dbo.Customers c LEFT JOIN dbo.Orders o ON c.CustomerId = o.CustomerId GROUP BY c.CustomerId, c.FullNamePersisted OPTION (RECOMPILE);

SELECT p.Category, SUM(li.LineTotal) AS Revenue FROM dbo.Products p INNER JOIN dbo.OrderLineItems li ON p.ProductId = li.ProductId GROUP BY p.Category OPTION (MAXDOP 4);

/* ===== FOR JSON PATH and FOR XML PATH ===== */

SELECT c.CustomerId, c.FirstName, c.LastName, c.Email, c.CreditLimit FROM dbo.Customers c WHERE c.IsActive = 1 ORDER BY c.LastName FOR JSON PATH, ROOT(N'customers');

SELECT o.OrderId, o.OrderDate, o.TotalAmount, (SELECT li.ProductId, li.Quantity, li.UnitPrice, li.LineTotal FROM dbo.OrderLineItems li WHERE li.OrderId = o.OrderId FOR JSON PATH) AS LineItems FROM dbo.Orders o WHERE o.CustomerId = 1 FOR JSON PATH, INCLUDE_NULL_VALUES;

SELECT c.CustomerId AS [@id], c.FirstName AS [Name/First], c.LastName AS [Name/Last], c.Email AS [Contact/Email] FROM dbo.Customers c WHERE c.IsActive = 1 FOR XML PATH(N'Customer'), ROOT(N'Customers');

/* ===== OPENJSON and OPENROWSET ===== */

SELECT j.CustomerId, j.FirstName, j.LastName, j.Email FROM OPENJSON(N'[{"CustomerId":1,"FirstName":"Alice","LastName":"Smith","Email":"alice@example.com"},{"CustomerId":2,"FirstName":"Bob","LastName":"Jones","Email":"bob@example.com"}]') WITH (CustomerId INT '$.CustomerId', FirstName NVARCHAR(100) '$.FirstName', LastName NVARCHAR(100) '$.LastName', Email NVARCHAR(256) '$.Email') j;

SELECT j.[key] AS ArrayIndex, j.[value] AS RawJson, JSON_VALUE(j.[value], '$.name') AS ProductName, JSON_VALUE(j.[value], '$.price') AS Price FROM OPENJSON(N'[{"name":"Widget","price":9.99},{"name":"Gadget","price":19.99}]') j;

/* ===== Date functions ===== */

SELECT SYSDATETIME() AS CurrentDateTime2, GETDATE() AS CurrentDateTime, GETUTCDATE() AS CurrentUtcDateTime, DATEADD(DAY, -30, GETDATE()) AS ThirtyDaysAgo, DATEADD(YEAR, 1, SYSDATETIME()) AS OneYearFromNow, DATEDIFF(DAY, '2024-01-01', GETDATE()) AS DaysSinceNewYear, DATEDIFF(MONTH, c.CreatedAt, SYSDATETIME()) AS MonthsSinceCreated, EOMONTH(GETDATE()) AS EndOfMonth, EOMONTH(GETDATE(), 1) AS EndOfNextMonth FROM dbo.Customers c WHERE c.CustomerId = 1;

/* ===== String functions ===== */

SELECT c.CustomerId, CHARINDEX(N'@', c.Email) AS AtPosition, PATINDEX(N'%[0-9]%', c.LastName) AS FirstDigitPos, STUFF(c.Email, 1, CHARINDEX(N'@', c.Email), N'***@') AS MaskedEmail, LEFT(c.FirstName, 1) + N'. ' + c.LastName AS ShortName, REPLACE(UPPER(c.LastName), N' ', N'_') AS NormalizedLastName FROM dbo.Customers c WHERE c.Email IS NOT NULL;

/* ===== OBJECT_ID, SCOPE_IDENTITY, NEWID ===== */

SELECT OBJECT_ID(N'dbo.Customers', N'U') AS CustomersTableId, OBJECT_ID(N'dbo.PK_Customers', N'PK') AS PkConstraintId;

INSERT INTO dbo.Products (Sku, ProductName, Category, UnitPrice) VALUES (N'TEST-' + CAST(NEWID() AS VARCHAR(8)), N'Test Product', N'Testing', 0.01);

SELECT SCOPE_IDENTITY() AS NewProductId;

/* ===== EXISTS with correlated subqueries ===== */

SELECT c.CustomerId, c.FullNamePersisted, c.CreditLimit FROM dbo.Customers c WHERE EXISTS (SELECT 1 FROM dbo.Orders o WHERE o.CustomerId = c.CustomerId AND o.TotalAmount > 1000) AND NOT EXISTS (SELECT 1 FROM dbo.Orders o WHERE o.CustomerId = c.CustomerId AND o.Status = N'Cancelled');

SELECT p.ProductId, p.ProductName, p.Category FROM dbo.Products p WHERE NOT EXISTS (SELECT 1 FROM dbo.OrderLineItems li WHERE li.ProductId = p.ProductId) AND p.IsDiscontinued = 0;

/* ===== Window functions ===== */

SELECT e.EmployeeId, e.FullName, e.DepartmentName, e.Salary, ROW_NUMBER() OVER (PARTITION BY e.DepartmentName ORDER BY e.Salary DESC) AS DeptRank, RANK() OVER (ORDER BY e.Salary DESC) AS OverallRank, DENSE_RANK() OVER (ORDER BY e.Salary DESC) AS DenseOverallRank, NTILE(4) OVER (ORDER BY e.Salary DESC) AS SalaryQuartile FROM dbo.Employees e;

SELECT o.OrderId, o.CustomerId, o.OrderDate, o.TotalAmount, LAG(o.TotalAmount, 1, 0) OVER (PARTITION BY o.CustomerId ORDER BY o.OrderDate) AS PrevOrderAmount, LEAD(o.TotalAmount, 1) OVER (PARTITION BY o.CustomerId ORDER BY o.OrderDate) AS NextOrderAmount, FIRST_VALUE(o.TotalAmount) OVER (PARTITION BY o.CustomerId ORDER BY o.OrderDate ROWS UNBOUNDED PRECEDING) AS FirstOrderAmount, LAST_VALUE(o.TotalAmount) OVER (PARTITION BY o.CustomerId ORDER BY o.OrderDate ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS LastOrderAmount FROM dbo.Orders o;

-- Running total and moving average
SELECT o.OrderId, o.OrderDate, o.TotalAmount, SUM(o.TotalAmount) OVER (ORDER BY o.OrderDate ROWS UNBOUNDED PRECEDING) AS RunningTotal, AVG(o.TotalAmount) OVER (ORDER BY o.OrderDate ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS MovingAvg3, COUNT(o.OrderId) OVER (PARTITION BY YEAR(o.OrderDate), MONTH(o.OrderDate) ORDER BY o.OrderDate) AS MonthRunningCount FROM dbo.Orders o ORDER BY o.OrderDate;

/* ===== Real-world pattern: top-N per group via CROSS APPLY ===== */

SELECT d.DepartmentName, topEarners.FullName, topEarners.Salary, topEarners.Rnk FROM (SELECT DISTINCT DepartmentName FROM dbo.Employees) d CROSS APPLY (SELECT TOP 3 e.FullName, e.Salary, ROW_NUMBER() OVER (ORDER BY e.Salary DESC) AS Rnk FROM dbo.Employees e WHERE e.DepartmentName = d.DepartmentName ORDER BY e.Salary DESC) topEarners ORDER BY d.DepartmentName, topEarners.Rnk;

/* ===== Real-world pattern: latest record per entity via OUTER APPLY ===== */

SELECT c.CustomerId, c.FullNamePersisted, c.Email, latestOrder.OrderId, latestOrder.OrderDate, latestOrder.TotalAmount, latestOrder.Status FROM dbo.Customers c OUTER APPLY (SELECT TOP 1 o.OrderId, o.OrderDate, o.TotalAmount, o.Status FROM dbo.Orders o WHERE o.CustomerId = c.CustomerId ORDER BY o.OrderDate DESC) latestOrder WHERE c.IsActive = 1 ORDER BY c.LastName, c.FirstName;

/* ===== Real-world pattern: dynamic date filtering ===== */

DECLARE @ReportStart DATE = DATEADD(QUARTER, -1, DATEADD(QUARTER, DATEDIFF(QUARTER, 0, GETDATE()), 0));

DECLARE @ReportEnd DATE = DATEADD(DAY, -1, DATEADD(QUARTER, DATEDIFF(QUARTER, 0, GETDATE()), 0));

SELECT c.FullNamePersisted, COUNT(o.OrderId) AS OrderCount, SUM(o.TotalAmount) AS TotalSpent, MIN(o.OrderDate) AS FirstOrder, MAX(o.OrderDate) AS LastOrder, DATEDIFF(DAY, MIN(o.OrderDate), MAX(o.OrderDate)) AS SpanDays FROM dbo.Customers c INNER JOIN dbo.Orders o ON c.CustomerId = o.CustomerId WHERE o.OrderDate BETWEEN @ReportStart AND @ReportEnd GROUP BY c.FullNamePersisted HAVING SUM(o.TotalAmount) > 500 ORDER BY TotalSpent DESC;
GO

/* ===== Real-world pattern: conditional aggregation ===== */

SELECT e.DepartmentName, COUNT(*) AS HeadCount, SUM(IIF(e.Salary > 100000, 1, 0)) AS HighEarners, SUM(IIF(e.Salary <= 100000, 1, 0)) AS StandardEarners, AVG(e.Salary) AS AvgSalary, SUM(CASE WHEN e.Region = N'East' THEN e.Salary ELSE 0 END) AS EastPayroll, SUM(CASE WHEN e.Region = N'West' THEN e.Salary ELSE 0 END) AS WestPayroll, STRING_AGG(IIF(e.Salary > 100000, e.FullName, NULL), N', ') AS HighEarnerNames FROM dbo.Employees e GROUP BY e.DepartmentName;

/* ===== Real-world pattern: PIVOT for monthly crosstab ===== */

SELECT CustomerId, [1] AS Jan, [2] AS Feb, [3] AS Mar, [4] AS Apr, [5] AS May, [6] AS Jun, [7] AS Jul, [8] AS Aug, [9] AS Sep, [10] AS Oct, [11] AS Nov, [12] AS Dec FROM (SELECT o.CustomerId, MONTH(o.OrderDate) AS OrderMonth, o.TotalAmount FROM dbo.Orders o WHERE YEAR(o.OrderDate) = YEAR(GETDATE())) src PIVOT (SUM(TotalAmount) FOR OrderMonth IN ([1], [2], [3], [4], [5], [6], [7], [8], [9], [10], [11], [12])) pvt;

/* ===== Real-world pattern: MERGE for inventory sync ===== */

MERGE dbo.Products AS tgt USING (SELECT s.Sku, s.ProductName, s.Category, s.UnitPrice FROM OPENJSON(N'[{"Sku":"AAA-100","ProductName":"Alpha","Category":"Widgets","UnitPrice":15.00},{"Sku":"BBB-200","ProductName":"Bravo","Category":"Gadgets","UnitPrice":25.00}]') WITH (Sku VARCHAR(50), ProductName NVARCHAR(200), Category NVARCHAR(100), UnitPrice MONEY) s) AS src ON tgt.Sku = src.Sku WHEN MATCHED AND (tgt.ProductName <> src.ProductName OR tgt.UnitPrice <> src.UnitPrice) THEN UPDATE SET tgt.ProductName = src.ProductName, tgt.UnitPrice = src.UnitPrice WHEN NOT MATCHED BY TARGET THEN INSERT (Sku, ProductName, Category, UnitPrice) VALUES (src.Sku, src.ProductName, src.Category, src.UnitPrice) WHEN NOT MATCHED BY SOURCE AND tgt.Category IN (N'Widgets', N'Gadgets') THEN UPDATE SET tgt.IsDiscontinued = 1 OUTPUT $action, INSERTED.Sku, INSERTED.ProductName, DELETED.Sku AS OldSku;

/* ===== Complex query: multi-join with window functions and hints ===== */

SELECT c.FullNamePersisted, o.OrderId, o.OrderDate, p.ProductName, li.Quantity, li.LineTotal, SUM(li.LineTotal) OVER (PARTITION BY c.CustomerId ORDER BY o.OrderDate ROWS UNBOUNDED PRECEDING) AS CustomerRunningTotal, DENSE_RANK() OVER (PARTITION BY p.Category ORDER BY li.LineTotal DESC) AS CategoryItemRank FROM dbo.Customers c WITH (NOLOCK) INNER JOIN dbo.Orders o WITH (NOLOCK) ON c.CustomerId = o.CustomerId INNER JOIN dbo.OrderLineItems li WITH (NOLOCK) ON o.OrderId = li.OrderId INNER JOIN dbo.Products p WITH (NOLOCK) ON li.ProductId = p.ProductId WHERE o.Status IN (N'Shipped', N'Delivered') AND o.OrderDate >= DATEADD(YEAR, -1, GETDATE()) OPTION (RECOMPILE, MAXDOP 2);

/* ===== CTE with recursion: org chart traversal ===== */

;WITH OrgChart AS (SELECT e.EmployeeId, e.FullName, e.ManagerId, e.DepartmentName, e.Salary, 0 AS OrgLevel, CAST(e.FullName AS NVARCHAR(MAX)) AS OrgPath FROM dbo.Employees e WHERE e.ManagerId IS NULL UNION ALL SELECT e.EmployeeId, e.FullName, e.ManagerId, e.DepartmentName, e.Salary, oc.OrgLevel + 1, CAST(oc.OrgPath + N' > ' + e.FullName AS NVARCHAR(MAX)) FROM dbo.Employees e INNER JOIN OrgChart oc ON e.ManagerId = oc.EmployeeId) SELECT oc.EmployeeId, oc.FullName, oc.DepartmentName, oc.Salary, oc.OrgLevel, oc.OrgPath, COUNT(*) OVER () AS TotalEmployees FROM OrgChart oc ORDER BY oc.OrgPath OPTION (MAXRECURSION 50);

/* ===== Multiple CTEs feeding a final query ===== */

;WITH CustomerStats AS (SELECT o.CustomerId, COUNT(*) AS OrderCount, SUM(o.TotalAmount) AS TotalSpent, AVG(o.TotalAmount) AS AvgOrder, MIN(o.OrderDate) AS FirstOrder, MAX(o.OrderDate) AS LastOrder FROM dbo.Orders o WHERE o.Status <> N'Cancelled' GROUP BY o.CustomerId), CustomerTiers AS (SELECT cs.CustomerId, cs.OrderCount, cs.TotalSpent, cs.AvgOrder, DATEDIFF(DAY, cs.FirstOrder, cs.LastOrder) AS CustomerLifespanDays, CASE WHEN cs.TotalSpent > 10000 THEN N'VIP' WHEN cs.TotalSpent > 5000 THEN N'Gold' WHEN cs.TotalSpent > 1000 THEN N'Silver' ELSE N'Bronze' END AS SpendTier FROM CustomerStats cs) SELECT c.FullNamePersisted, c.Email, ct.OrderCount, ct.TotalSpent, ct.AvgOrder, ct.CustomerLifespanDays, ct.SpendTier, NTILE(10) OVER (ORDER BY ct.TotalSpent DESC) AS SpendDecile FROM dbo.Customers c INNER JOIN CustomerTiers ct ON c.CustomerId = ct.CustomerId ORDER BY ct.TotalSpent DESC;

/* ===== Subquery in SELECT, FROM, and WHERE ===== */

SELECT c.CustomerId, c.FullNamePersisted, (SELECT COUNT(*) FROM dbo.Orders o WHERE o.CustomerId = c.CustomerId) AS OrderCount, (SELECT TOP 1 o.TotalAmount FROM dbo.Orders o WHERE o.CustomerId = c.CustomerId ORDER BY o.OrderDate DESC) AS LatestOrderAmount FROM dbo.Customers c WHERE c.CustomerId IN (SELECT DISTINCT o.CustomerId FROM dbo.Orders o WHERE o.TotalAmount > (SELECT AVG(o2.TotalAmount) FROM dbo.Orders o2));

/* ===== CASE expression varieties ===== */

SELECT p.ProductId, p.ProductName, p.UnitPrice, CASE WHEN p.UnitPrice < 10 THEN N'Budget' WHEN p.UnitPrice BETWEEN 10 AND 50 THEN N'Mid-Range' WHEN p.UnitPrice BETWEEN 50 AND 200 THEN N'Premium' ELSE N'Luxury' END AS PriceBand, CASE p.Category WHEN N'Electronics' THEN N'Tech' WHEN N'Accessories' THEN N'Acc' WHEN N'Clothing' THEN N'Apparel' ELSE N'Other' END AS ShortCategory FROM dbo.Products p WHERE p.IsDiscontinued = 0;

/* ===== INSERT with OUTPUT ===== */

INSERT INTO dbo.Orders (CustomerId, OrderDate, Status, TotalAmount) OUTPUT INSERTED.OrderId, INSERTED.CustomerId, INSERTED.OrderDate, INSERTED.Status SELECT TOP 5 c.CustomerId, SYSDATETIME(), N'Pending', ABS(CHECKSUM(NEWID())) % 1000 + 100 FROM dbo.Customers c WHERE c.IsActive = 1 ORDER BY NEWID();

/* ===== DELETE with OUTPUT and JOIN ===== */

DELETE li OUTPUT DELETED.LineItemId, DELETED.OrderId, DELETED.ProductId, DELETED.Quantity FROM dbo.OrderLineItems li INNER JOIN dbo.Orders o ON li.OrderId = o.OrderId WHERE o.Status = N'Cancelled' AND o.OrderDate < DATEADD(YEAR, -2, GETDATE());

/* ===== UPDATE with FROM and JOIN ===== */

UPDATE o SET o.TotalAmount = orderTotals.ComputedTotal FROM dbo.Orders o INNER JOIN (SELECT li.OrderId, SUM(li.LineTotal) AS ComputedTotal FROM dbo.OrderLineItems li GROUP BY li.OrderId) orderTotals ON o.OrderId = orderTotals.OrderId WHERE o.TotalAmount <> orderTotals.ComputedTotal;

/* ===== UNION / UNION ALL / INTERSECT / EXCEPT ===== */

SELECT c.Email AS ContactEmail, N'Customer' AS SourceType FROM dbo.Customers c WHERE c.Email IS NOT NULL UNION ALL SELECT CAST(e.FullName + N'@company.com' AS NVARCHAR(256)), N'Employee' FROM dbo.Employees e UNION ALL SELECT N'support@company.com', N'System' EXCEPT SELECT c.Email, N'Customer' FROM dbo.Customers c WHERE c.IsActive = 0;

/* ===== FOR JSON with nesting ===== */

SELECT c.CustomerId, c.FirstName, c.LastName, c.Email, c.CreditLimit, (SELECT o.OrderId, o.OrderDate, o.TotalAmount, o.Status, (SELECT li.ProductId, li.Quantity, li.UnitPrice, li.LineTotal FROM dbo.OrderLineItems li WHERE li.OrderId = o.OrderId FOR JSON PATH) AS Items FROM dbo.Orders o WHERE o.CustomerId = c.CustomerId FOR JSON PATH) AS Orders FROM dbo.Customers c WHERE c.IsActive = 1 FOR JSON PATH, ROOT(N'report'), INCLUDE_NULL_VALUES;

/* ===== Conditional logic with IF and temp-table pattern ===== */

DECLARE @OutputMode NVARCHAR(10) = N'summary';

IF @OutputMode = N'summary'
BEGIN
    SELECT e.DepartmentName, COUNT(*) AS HeadCount, SUM(e.Salary) AS TotalPayroll, AVG(e.Salary) AS AvgSalary FROM dbo.Employees e GROUP BY e.DepartmentName ORDER BY TotalPayroll DESC;
END
ELSE
BEGIN
    SELECT e.EmployeeId, e.FullName, e.DepartmentName, e.Salary, e.HireDate, e.Region FROM dbo.Employees e ORDER BY e.DepartmentName, e.FullName;
END;
GO

/* ===== OPENROWSET example (commented out as it requires linked server config) ===== */
-- SELECT * FROM OPENROWSET('SQLNCLI', 'Server=RemoteServer;Trusted_Connection=yes;', 'SELECT ProductId, ProductName, UnitPrice FROM RemoteDb.dbo.Products');

/* ===== Cleanup ===== */

IF OBJECT_ID(N'dbo.AuditLog', N'U') IS NOT NULL DROP TABLE dbo.AuditLog;

IF OBJECT_ID(N'dbo.OrderLineItems', N'U') IS NOT NULL DROP TABLE dbo.OrderLineItems;

IF OBJECT_ID(N'dbo.Orders', N'U') IS NOT NULL DROP TABLE dbo.Orders;

IF OBJECT_ID(N'dbo.Products', N'U') IS NOT NULL DROP TABLE dbo.Products;

IF OBJECT_ID(N'dbo.Customers', N'U') IS NOT NULL DROP TABLE dbo.Customers;

IF OBJECT_ID(N'dbo.Employees', N'U') IS NOT NULL DROP TABLE dbo.Employees;
GO
