/*          DATA CLEANING PROJECT          */

/* STEPS
	0. create duplicate of raw/staging dataset - work off the staging data
    1. remove duplicates
    2. standardize data
    3. addresse null or blank values
    4. remove unncessary columns or rows (careful)
    5. 
*/

SELECT COUNT(*)
FROM layoffs_staging;

CREATE TABLE layoffs_staging
LIKE layoffs;

INSERT layoffs_staging
SELECT *
FROM layoffs;

/*          duplicates          */

	-- selecting multiple columns in the partion to create unique identifier, essentially so that we can get a unique row number
    -- then if there returns a "2" in row_num, then there are duplicates
    -- filter by row_num query (with CTE or subquery) and return row_num > 1

WITH duplicate_cte AS (  
	SELECT *,
		ROW_NUMBER() OVER(
			PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
	FROM layoffs_staging
)

SELECT *
FROM duplicate_cte
WHERE row_num > 1;


-- choose a few rows to check. not everything returned was a duplicate --
-- go back and partition by all columns --

SELECT *
FROM layoffs_staging
WHERE company = 'Casper';


-- since there isn't a unique identifier, here's a work around for deleting duplicate rows
-- create another duplicate table of staging table (right click on table > copy to clipboard > create statement
-- add an addition column, row_num (from layoffs_staging)

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- empty table 
SELECT *
FROM layoffs_staging2;

-- insert info
INSERT INTO layoffs_staging2
SELECT *,
		ROW_NUMBER() OVER(
			PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
	FROM layoffs_staging;

-- first select * from layoffs_staging2 then change to delete    
DELETE
FROM layoffs_staging2
WHERE row_num > 1;
    
SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

SELECT *
FROM layoffs_staging2;


/*          standardizing data          */

	-- find issues in your data then fix them
			 
-- company 
	-- had extra spaces
SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

-- industry
	-- blank and null
    -- crypto and crypto currency and cryptocurrency
SELECT DISTINCT(industry)
FROM layoffs_staging2
ORDER BY 1;

SELECT *
FROM
	layoffs_staging2
WHERE industry LIKE 'Crypto%';


UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- check location (all is well)
-- check country (one row has a period after country)

SELECT DISTINCT location
FROM layoffs_staging2
ORDER BY 1;

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;

SELECT DISTINCT country
FROM layoffs_staging2
WHERE country LIKE 'United States%';


UPDATE layoffs_staging2
SET country = 'United States'
WHERE country LIKE 'United States%';

	/* alternate way to fix the trailing period:
		SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
        FROM layoffs_staging2
        ORDER BY 1;
        
        UPDATE layoffs_staging2
		SET country = TRIM(TRAILING '.' FROM country)
		WHERE country LIKE 'United States%'
	*/


-- `date`
	-- `date` was imported as a text data type, need to change it to date data type
    -- first get it in the proper format of a date,
    -- then change the data type

SELECT `date`,
	STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

SELECT `date`
FROM layoffs_staging2
ORDER BY 1;

-- `date` to date data type
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;


/*          null and blank values          */

-- check total_laid_off AND percentage_laid_off together (if both are blank, it's not very helpful, maybe that means no layoffs)
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
	AND percentage_laid_off IS NULL
ORDER BY total_laid_off;

-- see if we can populate blank and null industry values
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
	OR industry = '';	-- blank cells

SELECT *
FROM layoffs_staging2
WHERE company =  'Airbnb';
	-- we can populate with 'Travel'
    
-- change blank cells to null cells
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- join table on itself to populate null industries
-- inner join to only return rows that are in both tables
-- t1 will contain the null industry cells
-- t2 will contain the non-null industry cells

SELECT 
	t1.company,
    t1.location,
    t1.industry,
    t2.industry
FROM layoffs_staging2 AS t1
	JOIN layoffs_staging2 AS t2
		ON t1.company = t2.company
        AND t1.location = t2.location
WHERE t1.industry IS NULL 
	AND t2.industry IS NOT NULL;

-- now populate the null values
UPDATE layoffs_staging2 AS t1
	JOIN layoffs_staging2 AS t2
		ON t1.company = t2.company
        AND t1.location = t2.location
SET t1.industry = t2.industry
WHERE t1.industry IS NULL 
	AND t2.industry IS NOT NULL;

-- check
-- we see that Bally's industry is still null
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL;

SELECT *
FROM layoffs_staging2
WHERE company LIKE "Bally%";	-- Bally's doesn't have another row to help populate


/*          deleting unnecessary          */

-- check total_laid_off AND percentage_laid_off together (if both are blank, it's not very helpful, maybe that means no layoffs) 
-- kinda iffy to delete these, but we won't be able to do much analysis with those nulls
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
	AND percentage_laid_off IS NULL;

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
	AND percentage_laid_off IS NULL;

SELECT COUNT(*)
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT *
FROM layoffs_staging2;