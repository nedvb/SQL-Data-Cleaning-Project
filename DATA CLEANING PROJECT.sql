-- Data cleaning project
-- 12/19/2024
-- Ned Van Breugel

SELECT *
FROM layoffs; -- raw data from file download

CREATE TABLE layoffs_staging
LIKE layoffs; -- Create a copy of the data to work with during cleaning

INSERT INTO layoffs_staging
SELECT * FROM layoffs; -- Insert the data into the copy of the table

SELECT *
FROM layoffs_staging;

-- Step 1: remove any duplicates ---------------------------------------------------------------------------------------------------------------------------

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
) -- Create a CTE that partitions each of the rows into groups based on the columns and iterates over all the rows to return row_num as an index
SELECT *
FROM duplicate_cte 
WHERE row_num > 1; -- If any of the rows contain exact duplicates, return them

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
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; -- Create a new copy of the table

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging; -- Insert into the new copy with the "unique row_num" index

DELETE
FROM layoffs_staging2
WHERE row_num > 1; -- Delete all the rows that are duplicates

SELECT *
FROM layoffs_staging2;

-- Step 2: standardize the data ---------------------------------------------------------------------------------------------------------------------------

SELECT * 
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company); -- Clean up the company film by standardizing empty spacing

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%'; -- Specific case where 'industry' contained different text values for values that should be consistent

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%'; -- Specific case where 'country' contained different text values for values that should be consistent

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y'); -- Set date field to a date format

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE; -- Update the data type in the table

SELECT * 
FROM layoffs_staging2;

-- Step 3: handle null values or blank values ---------------------------------------------------------------------------------------------------------------------------

UPDATE layoffs_staging2
SET industry = null
WHERE industry = ''; -- Change any fields where the industry was blank to null

UPDATE layoffs_staging2 AS t1
JOIN layoffs_staging2 as t2
	ON t1.company = t2.company
    AND t1.location = t2.location
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL; -- If there are multiple rows for the same company, and one of the rows has an industry and the other(s) are blank, use the populated value to populate the value of the one that was blank.

-- -- Step 4: remove any columns or rows---------------------------------------------------------------------------------------------------------------------------

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL; -- Delete any rows that have nulls for both 'total_laid_off' and 'percentage_laid_off'

SELECT *
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num; -- Drop the row_num index that was used in an earlier step 