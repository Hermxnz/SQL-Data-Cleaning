SELECT * 
FROM layoffs;
SELECT * 
FROM layoffs_no_dupes;
CREATE TABLE layoffs_staging like layoffs;
INSERT layoffs_staging select* FROM layoffs;




SELECT *, row_number() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;


WITH duplicate_cte AS 
(
SELECT *, row_number() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT * 
FROM duplicate_cte
WHERE row_num > 1;

SELECT * 
FROM layoffs_staging
WHERE company = 'Casper';



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

INSERT INTO layoffs_staging2
SELECT*,row_number() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

DELETE 
FROM layoffs_staging2
WHERE row_num> 1;


SELECT DISTINCT company
FROM layoffs_no_dupes;

UPDATE layoffs_no_dupes
SET company = TRIM(company);

SELECT DISTINCT industry
FROM layoffs_no_dupes;

SELECT *
FROM layoffs_no_dupes
WHERE industry LIKE "Crypto%";
;
UPDATE layoffs_no_dupes
SET industry = 'Crypto' 
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT location
FROM layoffs_no_dupes;

SELECT DISTINCT country
FROM layoffs_no_dupes;

SELECT *
FROM layoffs_no_dupes
WHERE country LIKE 'United States'
;

SELECT DISTINCT country
FROM layoffs_no_dupes;

UPDATE layoffs_no_dupes
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

SELECT `date`,
str_to_date(`date`,'%m/%d/%Y') 
FROM layoffs_no_dupes;


UPDATE layoffs_no_dupes
SET `date` = str_to_date(`date`,'%m/%d/%Y');

ALTER TABLE layoffs_no_dupes
MODIFY COLUMN `date` DATE;

-- NULL AND BLANK VALUES


SELECT *
FROM layoffs_no_dupes
WHERE industry IS NULL 
OR industry ="";

SELECT *
FROM layoffs_no_dupes
WHERE company = 'Airbnb';


SELECT * 
from layoffs_no_dupes t1
JOIN layoffs_no_dupes t2
	ON t1.company= t2.company
    AND t1.location = t2.location
    WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

UPDATE layoffs_no_dupes
SET industry = NULL 
WHERE industry = '';

UPDATE layoffs_no_dupes t1
JOIN layoffs_no_dupes t2
	ON t1.company= t2.company
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL)
AND t2.industry IS NOT NULL ;

SELECT * 
FROM layoffs_no_dupes
WHERE company LIKE 'Bally%';

select * 
FROM layoffs_no_dupes
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL
;

DELETE  
FROM layoffs_no_dupes
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL
;

SELECT * 
FROM layoffs_no_dupes;

ALTER TABLE layoffs_no_dupes
DROP column row_num;

