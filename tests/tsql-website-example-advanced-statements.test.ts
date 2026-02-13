import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';
import { parse } from '../src/parser';

describe('T-SQL website example advanced statements', () => {
  it('formats UNPIVOT applied to a derived table source', () => {
    const sql = `SELECT CustomerId, StatusLabel, StatusAmount FROM (SELECT CustomerId, [Pending], [Shipped], [Delivered] FROM (SELECT o.CustomerId, o.Status, o.TotalAmount FROM dbo.Orders o) src PIVOT (SUM(TotalAmount) FOR Status IN ([Pending], [Shipped], [Delivered])) pvt) p UNPIVOT (StatusAmount FOR StatusLabel IN ([Pending], [Shipped], [Delivered])) unpvt;`;
    expect(() => parse(sql, { dialect: 'tsql', recover: false })).not.toThrow();
    const out = formatSQL(sql, { dialect: 'tsql' });
    expect(out).toBe(
      [
        'SELECT CustomerId,',
        '       StatusLabel,',
        '       StatusAmount',
        '  FROM (SELECT CustomerId,',
        '               [Pending],',
        '               [Shipped],',
        '               [Delivered]',
        '          FROM (SELECT o.CustomerId,',
        '                       o.Status,',
        '                       o.TotalAmount',
        '                  FROM dbo.Orders AS o) AS src',
        '               PIVOT (SUM(TotalAmount) FOR Status IN ([Pending], [Shipped], [Delivered])) pvt) AS p',
        '       UNPIVOT (StatusAmount FOR StatusLabel IN ([Pending], [Shipped], [Delivered])) unpvt;',
        '',
      ].join('\n'),
    );
  });

  it('formats MERGE with OUTPUT', () => {
    const sql = `MERGE dbo.Products AS tgt USING (SELECT 'WIDGET-001' AS Sku, N'Premium Widget' AS ProductName, N'Accessories' AS Category, 29.99 AS UnitPrice) AS src ON tgt.Sku = src.Sku WHEN MATCHED THEN UPDATE SET tgt.ProductName = src.ProductName, tgt.UnitPrice = src.UnitPrice WHEN NOT MATCHED BY TARGET THEN INSERT (Sku, ProductName, Category, UnitPrice) VALUES (src.Sku, src.ProductName, src.Category, src.UnitPrice) OUTPUT $action AS MergeAction, INSERTED.ProductId, INSERTED.Sku, INSERTED.ProductName;`;
    expect(() => parse(sql, { dialect: 'tsql', recover: false })).not.toThrow();
    const out = formatSQL(sql, { dialect: 'tsql' });
    expect(out).toBe(
      [
        ' MERGE INTO dbo.Products AS tgt',
        ' USING (SELECT \'WIDGET-001\' AS Sku,',
        '               N\'Premium Widget\' AS ProductName,',
        '               N\'Accessories\' AS Category,',
        '               29.99 AS UnitPrice) AS src',
        '    ON tgt.Sku = src.Sku',
        '  WHEN MATCHED THEN',
             '       UPDATE',
        '          SET tgt.ProductName = src.ProductName,',
        '              tgt.UnitPrice = src.UnitPrice',
        '  WHEN NOT MATCHED BY TARGET THEN',
        '       INSERT (Sku, ProductName, Category, UnitPrice)',
        '       VALUES (src.Sku, src.ProductName, src.Category, src.UnitPrice)',
        'OUTPUT $action AS MergeAction, INSERTED.ProductId, INSERTED.Sku, INSERTED.ProductName;',
        '',
      ].join('\n'),
    );
  });

  it('formats MERGE with NOT MATCHED BY SOURCE delete and OUTPUT', () => {
    const sql = `MERGE dbo.Customers AS tgt USING (SELECT N'John' AS FirstName, N'Doe' AS LastName, N'john.doe@example.com' AS Email, 5000.00 AS CreditLimit) AS src ON tgt.Email = src.Email WHEN MATCHED THEN UPDATE SET tgt.CreditLimit = src.CreditLimit, tgt.ModifiedAt = SYSDATETIMEOFFSET() WHEN NOT MATCHED BY TARGET THEN INSERT (FirstName, LastName, Email, CreditLimit) VALUES (src.FirstName, src.LastName, src.Email, src.CreditLimit) WHEN NOT MATCHED BY SOURCE AND tgt.IsActive = 0 THEN DELETE OUTPUT $action, INSERTED.CustomerId, INSERTED.Email, DELETED.CustomerId AS OldCustomerId;`;
    expect(() => parse(sql, { dialect: 'tsql', recover: false })).not.toThrow();
    const out = formatSQL(sql, { dialect: 'tsql' });
    expect(out).toBe(
      [
        ' MERGE INTO dbo.Customers AS tgt',
        ' USING (SELECT N\'John\' AS FirstName,',
        '               N\'Doe\' AS LastName,',
        '               N\'john.doe@example.com\' AS Email,',
        '               5000.00 AS CreditLimit) AS src',
        '    ON tgt.Email = src.Email',
        '  WHEN MATCHED THEN',
        '       UPDATE',
        '          SET tgt.CreditLimit = src.CreditLimit,',
        '              tgt.ModifiedAt = SYSDATETIMEOFFSET()',
        '  WHEN NOT MATCHED BY TARGET THEN',
        '       INSERT (FirstName, LastName, Email, CreditLimit)',
        '       VALUES (src.FirstName, src.LastName, src.Email, src.CreditLimit)',
        '  WHEN NOT MATCHED BY SOURCE AND tgt.IsActive = 0 THEN',
        '       DELETE',
        'OUTPUT $action, INSERTED.CustomerId, INSERTED.Email, DELETED.CustomerId AS OldCustomerId;',
        '',
      ].join('\n'),
    );
  });

  it('formats SELECT ... FOR JSON PATH', () => {
    const sql = `SELECT c.CustomerId, c.FirstName, c.LastName, c.Email, c.CreditLimit FROM dbo.Customers c WHERE c.IsActive = 1 ORDER BY c.LastName FOR JSON PATH, ROOT(N'customers');`;
    expect(() => parse(sql, { dialect: 'tsql', recover: false })).not.toThrow();
    const out = formatSQL(sql, { dialect: 'tsql' });
    expect(out).toBe(
      [
        'SELECT c.CustomerId,',
        '       c.FirstName,',
        '       c.LastName,',
        '       c.Email,',
        '       c.CreditLimit',
        '  FROM dbo.Customers AS c',
        ' WHERE c.IsActive = 1',
        ' ORDER BY c.LastName',
        '   FOR JSON PATH, ROOT(N\'customers\');',
        '',
      ].join('\n'),
    );
  });

  it('formats nested FOR JSON PATH subqueries', () => {
    const sql = `SELECT o.OrderId, o.OrderDate, o.TotalAmount, (SELECT li.ProductId, li.Quantity, li.UnitPrice, li.LineTotal FROM dbo.OrderLineItems li WHERE li.OrderId = o.OrderId FOR JSON PATH) AS LineItems FROM dbo.Orders o WHERE o.CustomerId = 1 FOR JSON PATH, INCLUDE_NULL_VALUES;`;
    expect(() => parse(sql, { dialect: 'tsql', recover: false })).not.toThrow();
    const out = formatSQL(sql, { dialect: 'tsql' });
    expect(out).toBe(
      [
        'SELECT o.OrderId,',
        '       o.OrderDate,',
        '       o.TotalAmount,',
        '       (SELECT li.ProductId,',
        '               li.Quantity,',
        '               li.UnitPrice,',
        '               li.LineTotal',
        '          FROM dbo.OrderLineItems AS li',
        '         WHERE li.OrderId = o.OrderId',
        '           FOR JSON PATH) AS LineItems',
        '  FROM dbo.Orders AS o',
        ' WHERE o.CustomerId = 1',
        '   FOR JSON PATH, INCLUDE_NULL_VALUES;',
        '',
      ].join('\n'),
    );
  });

  it('formats SELECT ... FOR XML PATH', () => {
    const sql = `SELECT c.CustomerId AS [@id], c.FirstName AS [Name/First], c.LastName AS [Name/Last], c.Email AS [Contact/Email] FROM dbo.Customers c WHERE c.IsActive = 1 FOR XML PATH(N'Customer'), ROOT(N'Customers');`;
    expect(() => parse(sql, { dialect: 'tsql', recover: false })).not.toThrow();
    const out = formatSQL(sql, { dialect: 'tsql' });
    expect(out).toBe(
      [
        'SELECT c.CustomerId AS [@id],',
        '       c.FirstName AS [Name/First],',
        '       c.LastName AS [Name/Last],',
        '       c.Email AS [Contact/Email]',
        '  FROM dbo.Customers AS c',
        ' WHERE c.IsActive = 1',
        '   FOR XML PATH(N\'Customer\'), ROOT(N\'Customers\');',
        '',
      ].join('\n'),
    );
  });

  it('formats OPENJSON WITH schema clauses', () => {
    const sql = `SELECT j.CustomerId, j.FirstName, j.LastName, j.Email FROM OPENJSON(N'[{\"CustomerId\":1,\"FirstName\":\"Alice\",\"LastName\":\"Smith\",\"Email\":\"alice@example.com\"},{\"CustomerId\":2,\"FirstName\":\"Bob\",\"LastName\":\"Jones\",\"Email\":\"bob@example.com\"}]') WITH (CustomerId INT '$.CustomerId', FirstName NVARCHAR(100) '$.FirstName', LastName NVARCHAR(100) '$.LastName', Email NVARCHAR(256) '$.Email') j;`;
    expect(() => parse(sql, { dialect: 'tsql', recover: false })).not.toThrow();
    const out = formatSQL(sql, { dialect: 'tsql' });
    expect(out).toBe(
      [
        'SELECT j.CustomerId,',
        '       j.FirstName,',
        '       j.LastName,',
        '       j.Email',
        '  FROM OPENJSON(N\'[{\"CustomerId\":1,\"FirstName\":\"Alice\",\"LastName\":\"Smith\",\"Email\":\"alice@example.com\"},{\"CustomerId\":2,\"FirstName\":\"Bob\",\"LastName\":\"Jones\",\"Email\":\"bob@example.com\"}]\')',
        '       WITH (CustomerId int \'$.CustomerId\', FirstName nvarchar(100) \'$.FirstName\', LastName nvarchar(100) \'$.LastName\', Email nvarchar(256) \'$.Email\') AS j;',
        '',
      ].join('\n'),
    );
  });

  it('formats INSERT ... OUTPUT ... SELECT', () => {
    const sql = `INSERT INTO dbo.Orders (CustomerId, OrderDate, Status, TotalAmount) OUTPUT INSERTED.OrderId, INSERTED.CustomerId, INSERTED.OrderDate, INSERTED.Status SELECT TOP 5 c.CustomerId, SYSDATETIME(), N'Pending', ABS(CHECKSUM(NEWID())) % 1000 + 100 FROM dbo.Customers c WHERE c.IsActive = 1 ORDER BY NEWID();`;
    expect(() => parse(sql, { dialect: 'tsql', recover: false })).not.toThrow();
    const out = formatSQL(sql, { dialect: 'tsql' });
    expect(out).toBe(
      [
        'INSERT INTO dbo.Orders (CustomerId, OrderDate, Status, TotalAmount)',
        'OUTPUT INSERTED.OrderId, INSERTED.CustomerId, INSERTED.OrderDate, INSERTED.Status',
        'SELECT TOP 5 c.CustomerId,',
        '             SYSDATETIME(),',
        '             N\'Pending\',',
        '             ABS(CHECKSUM(NEWID())) % 1000 + 100',
        '  FROM dbo.Customers AS c',
        ' WHERE c.IsActive = 1',
        ' ORDER BY NEWID();',
        '',
      ].join('\n'),
    );
  });

  it('formats DELETE alias OUTPUT ... FROM ... JOIN', () => {
    const sql = `DELETE li OUTPUT DELETED.LineItemId, DELETED.OrderId, DELETED.ProductId, DELETED.Quantity FROM dbo.OrderLineItems li INNER JOIN dbo.Orders o ON li.OrderId = o.OrderId WHERE o.Status = N'Cancelled' AND o.OrderDate < DATEADD(year, -2, GETDATE());`;
    expect(() => parse(sql, { dialect: 'tsql', recover: false })).not.toThrow();
    const out = formatSQL(sql, { dialect: 'tsql' });
    expect(out).toBe(
      [
        'DELETE li',
        'OUTPUT DELETED.LineItemId, DELETED.OrderId, DELETED.ProductId, DELETED.Quantity',
        '  FROM dbo.OrderLineItems AS li',
        '       INNER JOIN dbo.Orders AS o',
        '       ON li.OrderId = o.OrderId',
        ' WHERE o.Status = N\'Cancelled\'',
        '   AND o.OrderDate < DATEADD(year, -2, GETDATE());',
        '',
      ].join('\n'),
    );
  });

  it('formats report-shaped FOR JSON nesting', () => {
    const sql = `SELECT c.CustomerId, c.FirstName, c.LastName, c.Email, c.CreditLimit, (SELECT o.OrderId, o.OrderDate, o.TotalAmount, o.Status, (SELECT li.ProductId, li.Quantity, li.UnitPrice, li.LineTotal FROM dbo.OrderLineItems li WHERE li.OrderId = o.OrderId FOR JSON PATH) AS Items FROM dbo.Orders o WHERE o.CustomerId = c.CustomerId FOR JSON PATH) AS Orders FROM dbo.Customers c WHERE c.IsActive = 1 FOR JSON PATH, ROOT(N'report'), INCLUDE_NULL_VALUES;`;
    expect(() => parse(sql, { dialect: 'tsql', recover: false })).not.toThrow();
    const out = formatSQL(sql, { dialect: 'tsql' });
    expect(out).toBe(
      [
        'SELECT c.CustomerId,',
        '       c.FirstName,',
        '       c.LastName,',
        '       c.Email,',
        '       c.CreditLimit,',
        '       (SELECT o.OrderId,',
        '               o.OrderDate,',
        '               o.TotalAmount,',
        '               o.Status,',
        '               (SELECT li.ProductId,',
        '                       li.Quantity,',
        '                       li.UnitPrice,',
        '                       li.LineTotal',
        '                  FROM dbo.OrderLineItems AS li',
        '                 WHERE li.OrderId = o.OrderId',
        '                   FOR JSON PATH) AS Items',
        '          FROM dbo.Orders AS o',
        '         WHERE o.CustomerId = c.CustomerId',
        '           FOR JSON PATH) AS Orders',
        '  FROM dbo.Customers AS c',
        ' WHERE c.IsActive = 1',
        '   FOR JSON PATH, ROOT(N\'report\'), INCLUDE_NULL_VALUES;',
        '',
      ].join('\n'),
    );
  });
});
