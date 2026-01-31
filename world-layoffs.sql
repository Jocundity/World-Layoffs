CREATE TABLE layoffs (
	company VARCHAR(100), 
	location VARCHAR(100), 
	industry VARCHAR(100), 
	total_laid_off INTEGER,
	percentage_laid_off DECIMAL(3,2),
	layoff_date DATE,
	stage VARCHAR(100),
	country VARCHAR(100),
	funds_raised_millions INTEGER
);

SELECT * FROM layoffs;

/**** DATA CLEANING:

-- 1. Remove duplicates
-- 2. Standardise the data
-- 3. Handle Null values or blank values
-- 4. Remove any rows or columns that are unnecessary

****/

-- Create staging table to protect original from changes
CREATE TABLE layoffs_staging
(LIKE layoffs);

INSERT INTO layoffs_staging
SELECT * FROM layoffs;

-- 1. Remove duplicates:

-- Check for duplicates
WITH duplicate_cte AS (
	SELECT *,
	ROW_NUMBER() OVER(
		PARTITION BY company,
		location,
		industry,
		total_laid_off,
		percentage_laid_off,
		layoff_date,
		stage,
		country,
		funds_raised_millions
	) AS row_num
	FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

-- Create second table to remove duplicates from
CREATE TABLE layoffs_staging2
(
	company VARCHAR(100), 
	location VARCHAR(100), 
	industry VARCHAR(100), 
	total_laid_off INTEGER,
	percentage_laid_off DECIMAL(3,2),
	layoff_date DATE,
	stage VARCHAR(100),
	country VARCHAR(100),
	funds_raised_millions INTEGER,
	row_num INTEGER
);

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER( 
		PARTITION BY company,
		location,
		industry,
		total_laid_off,
		percentage_laid_off,
		layoff_date,
		stage,
		country,
		funds_raised_millions
	) AS row_num
	FROM layoffs_staging;

DELETE  
FROM layoffs_staging2
WHERE row_num > 1;

-- All duplicate rows are now removed
SELECT * 
FROM layoffs_staging2;

-- 2. Standardise the data:

-- Trim whitespace
UPDATE layoffs_staging2
SET company = TRIM(company),
	location = TRIM(location),
	industry = TRIM(industry),
	stage = TRIM(stage),
	country = TRIM(country);

-- Standardise industry names
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- Standardise country names
UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- 3. Handle Null or blank values

/* Populate null industry values 
if the value is known in another row
by self-joining table */
SELECT DISTINCT company, industry
FROM layoffs_staging2
WHERE industry IS NULL OR industry = '';

SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company 
	AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry = '')
	AND (t2.industry IS NOT NULL AND t2.industry != '');

UPDATE layoffs_staging2 t1
SET industry = t2.industry
FROM layoffs_staging2 t2
WHERE t1.company = t2.company 
	AND t1.location = t2.location
	AND (t1.industry IS NULL OR t1.industry = '')
	AND (t2.industry IS NOT NULL AND t2.industry != '');

-- 4. Remove any rows or columns that are unnecessary

-- Remove rows where total & percentage laid off are null
DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
	AND percentage_laid_off IS NULL;

-- Drop row_num column
ALTER TABLE layoffs_staging2
DROP column row_num;

SELECT * from layoffs_staging2;

-- Rename table to 'layoffs_clean' after cleaning is finished
ALTER table layoffs_staging2
RENAME TO layoffs_clean;

/**** Exploratory Data Analysis ****/
SELECT *
FROM layoffs_clean;

-- What is the highest amount of people that were laid off in one day?
SELECT MAX(total_laid_off)
FROM layoffs_clean;

-- Which companies went under? (Laid off 100% of workforce)
SELECT *
FROM layoffs_clean
WHERE percentage_laid_off = 1;

-- How many companies went under? 
SELECT COUNT(*)
FROM layoffs_clean
WHERE percentage_laid_off = 1;

-- Of companies that went under, how many employees worked there?
SELECT company, industry, total_laid_off
FROM layoffs_clean
WHERE percentage_laid_off = 1 
	AND total_laid_off IS NOT NULL 
ORDER BY total_laid_off DESC;

-- How much funding did these companies have?
SELECT company, industry, funds_raised_millions
FROM layoffs_clean
WHERE percentage_laid_off = 1 
	AND funds_raised_millions IS NOT NULL 
ORDER BY funds_raised_millions DESC;

-- Which companies laid off the most employees?
SELECT company, SUM(total_laid_off)
FROM layoffs_clean
WHERE total_laid_off IS NOT NULL 
GROUP BY company
ORDER BY SUM(total_laid_off) DESC;

-- Which industries got hit hardest the most?
SELECT industry, SUM(total_laid_off)
FROM layoffs_clean
WHERE total_laid_off IS NOT NULL 
GROUP BY industry
ORDER BY SUM(total_laid_off) DESC;

-- Which countries had the most layoffs?
SELECT country, SUM(total_laid_off)
FROM layoffs_clean
WHERE total_laid_off IS NOT NULL 
GROUP BY country
ORDER BY SUM(total_laid_off) DESC;

-- What is the date range of the data in this table?
SELECT MIN(layoff_date), MAX (layoff_date)
FROM layoffs_clean;

-- How did the layoffs progress? (Rolling total of layoffs by month)
WITH rolling_total AS (
	SELECT TO_CHAR(layoff_date, 'YYYY-MM') AS layoff_month, 
		SUM(total_laid_off) OVER(
			ORDER BY TO_CHAR(layoff_date, 'YYYY-MM')
			ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
		) AS rolling_total_layoffs
	FROM layoffs_clean
	WHERE TO_CHAR(layoff_date, 'YYYY-MM') IS NOT NULL
	ORDER BY layoff_month
)
SELECT layoff_month, 
	MAX(rolling_total_layoffs) AS rolling_total_by_month
FROM rolling_total
GROUP BY layoff_month;

-- Which companies laid off the most people per by year?
WITH ranked AS (
	SELECT company,
		EXTRACT(YEAR FROM layoff_date) AS year,
		SUM(total_laid_off),
		DENSE_RANK() OVER(
				PARTITION BY EXTRACT(YEAR FROM layoff_date)
				ORDER BY SUM(total_laid_off) DESC
			) AS rank_in_year
	FROM layoffs_clean
	WHERE EXTRACT(YEAR FROM layoff_date) IS NOT NULL
		AND total_laid_off IS NOT NULL
	GROUP BY company, EXTRACT(YEAR FROM layoff_date)
	ORDER BY year, rank_in_year
)
SELECT *
FROM ranked
WHERE rank_in_year <= 5; 

-- What is the average percentage of layoffs overall?
SELECT AVG(percentage_laid_off)
FROM layoffs_clean;

-- What is the average percentage of layoffs per industry?
SELECT industry, AVG(percentage_laid_off)
FROM layoffs_clean
GROUP BY industry
ORDER BY AVG(percentage_laid_off) DESC;

