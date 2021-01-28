
use master  
GO

IF NOT EXISTS(select * from sys.databases where name = 'IceCream')
BEGIN
    CREATE DATABASE IceCream
END
GO

/*------------------------*/

use IceCream
GO

IF(OBJECT_ID('dbo.MakerFlavor','U') IS NULL)
BEGIN
    -- denormalized intersection table
    CREATE TABLE dbo.MakerFlavor(
        Maker   SYSNAME NOT NULL,
        Flavor  SYSNAME NOT NULL,     
        BaseFlavor  SYSNAME NOT NULL,              
        CONSTRAINT PK_MakerFlavor PRIMARY KEY CLUSTERED(Maker, Flavor)
    );
END
GO

/*------------------------*/

IF( (SELECT COUNT(*) FROM dbo.MakerFlavor) = 0)
BEGIN
    INSERT INTO dbo.MakerFlavor(Maker,Flavor,BaseFlavor) VALUES
        ('Haagen-Dazs','Cherry Vanilla','Vanilla'),
        ('Cold Stone Creamery','Coffee','Misc'),
        ('Cold Stone Creamery','Mint','Misc'),
        ('Baskin-Robbins','Vanilla','Vanilla'),
        ('Graeters','Cinnamon','Cinnamon'),
        ('Haagen-Dazs','Chocolate','Chocolate'),
        ('Marble Slab Creamery','Cinnamon','Cinnamon'),
        ('Cold Stone Creamery','French Vanilla','Vanilla'),        
        ('Haagen-Dazs','Pistachio','Misc'),      
        ('Marble Slab Creamery','Vanilla','Vanilla'),
        ('Haagen-Dazs','Chocolate Sea Salt','Chocolate'),
        ('Baskin-Robbins','Chocolate Almond','Chocolate')

    -- covering index to support query and aid software design
    CREATE NONCLUSTERED INDEX IX_MakerFlavor_Flavor
        ON dbo.MakerFlavor(Flavor)
        INCLUDE(Maker,BaseFlavor);           
END
GO

/*------------------------*/

SET STATISTICS IO ON

-- index scan over IX_MakerFlavor_Flavor
SELECT Maker, Flavor, BaseFlavor FROM dbo.MakerFlavor;

/*
    ORIGINAL QUERY
        valuable for checking correctness of software solution 
        simple execution plan, though, not helpful for s/w modeling
        Index Seek on IX_MakerFlavor_Flavor as covering index
        * three Seek Keys
        * Predicate on the disjunct (UNION)
        Scan count 3, logical reads 6
*/
SELECT  Maker, Flavor, BaseFlavor
FROM    dbo.MakerFlavor 
WHERE   (Maker LIKE '% Creamery' OR BaseFlavor = 'Vanilla')
        AND
        Flavor IN('Mint','Coffee','Vanilla');

/*
    REWRITE 1
        rewrte translating the two logical connectives to set operators
        this query version helps software design:
        * guides creation of Map structures to be used as indexes
        * is translatable to series of filter, flatMap and map functions
        Scan count 3, logical reads 18
*/
(SELECT Maker, Flavor, BaseFlavor
FROM    dbo.MakerFlavor
WHERE   Maker LIKE '% Creamery'
    UNION
SELECT  Maker, Flavor, BaseFlavor
FROM    dbo.MakerFlavor
WHERE   BaseFlavor = 'Vanilla')
    INTERSECT
SELECT  Maker, Flavor, BaseFlavor
FROM    dbo.MakerFlavor 
WHERE   Flavor IN('Mint','Coffee','Vanilla');

/*------------------------*/

/*
    TEST I: INTERSECT operator optimization
        Since Flavor --> BaseFlavor etc. as per article, constraining on 'one' side of 
        many-to-one produces too many rows, which will be culled later by the INTERSECT operator
        NOTE: this does not affect correctness - just sub-optimal for software design
        RESULT SET: 4 rows  
        NOTE: placing a covering index on BaseFlavor takes query plan from 
              index scan on Flavor index to index seek but size of result set
              is the problem
*/
/*
-- statements to experiment as desired
CREATE NONCLUSTERED INDEX IX_MakerFlavor_BaseFlavor
    ON dbo.MakerFlavor(BaseFlavor)
    INCLUDE (Maker,Flavor);

DROP INDEX IX_MakerFlavor_BaseFlavor ON dbo.MakerFlavor;   
*/
SELECT  Maker, Flavor, BaseFlavor
FROM    dbo.MakerFlavor
WHERE   BaseFlavor = 'Vanilla';

/*
    TEST II: INTERSECT operator optimization
        Restricting on 'many' side produces fewer rows to cull by INTERSECT
        RESULT SET: 2 rows
*/
SELECT  Maker, Flavor, BaseFlavor
FROM    dbo.MakerFlavor
WHERE   Flavor = 'Vanilla';

/*
    FINAL FORM
        lower half of UNION restricts on Flavor not base flavor
        RESULT: same result set
                execution plan differs only in Predicate: match on Flavor, 
                   not base flavor, equals 'Vanilla'  
*/
(SELECT Maker, Flavor, BaseFlavor
FROM    dbo.MakerFlavor
WHERE   Maker LIKE '% Creamery'
    UNION
SELECT  Maker, Flavor, BaseFlavor
FROM    dbo.MakerFlavor
WHERE   Flavor = 'Vanilla')
    INTERSECT
SELECT  Maker, Flavor, BaseFlavor
FROM    dbo.MakerFlavor 
WHERE   Flavor IN('Mint','Coffee','Vanilla');

/*------------------------*/

/*
    TEST :  a property of the sample data (see article section Testing: A Note on the Sample Data)
        since the set produced by the two union queries is a
        proper superset of that produced by the restriction on three flavor names,
        the latter query by itself (sans intersect) yields the correct data
        NOTE CAVEAT!
*/
SELECT  Maker, Flavor, BaseFlavor
FROM    dbo.MakerFlavor
WHERE   Maker LIKE '% Creamery'
    UNION
SELECT  Maker, Flavor, BaseFlavor
FROM    dbo.MakerFlavor
WHERE   Flavor = 'Vanilla';

SELECT  Maker, Flavor, BaseFlavor
FROM    dbo.MakerFlavor 
WHERE   Flavor IN('Mint','Coffee','Vanilla');

/*------------------------*/

/*
    TEST  update maker so union rowset no longer proper superset of 
          that produced by the restriction on three flavor names
          RESULT: one result row missing (Cold Stone Creamery,	Coffee,	Misc)
*/
/*
-- three rows returned not four
UPDATE  dbo.MakerFlavor
SET     Maker = 'Bayview Ice'
WHERE   Maker = 'Cold Stone Creamery'
        AND
        Flavor = 'Coffee';
-- put back missing ros
INSERT INTO dbo.MakerFlavor(Maker,Flavor,BaseFlavor) VALUES('Cold Stone Creamery','Coffee','Misc');        
*/
(SELECT  Maker, Flavor, BaseFlavor
FROM    dbo.MakerFlavor
WHERE   Maker LIKE '% Creamery'
    UNION
SELECT  Maker, Flavor, BaseFlavor
FROM    dbo.MakerFlavor
WHERE   Flavor = 'Vanilla')
    INTERSECT
SELECT  Maker, Flavor, BaseFlavor
FROM    dbo.MakerFlavor 
WHERE   Flavor IN('Mint','Coffee','Vanilla');        
