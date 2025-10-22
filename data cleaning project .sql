-- data cleaning project 
use world_layoffs;
SELECT * FROM world_layoffs.layoffs;

-- 1. check for duplicates and remove any
-- 2. standardize data and fix errors
-- 3. Look at null values and see what 
-- 4. remove any columns and rows that are not necessary - few ways

DROP TABLE IF EXISTS layoffs_staging;
-- first thing we want to do is create a staging table. This is the one we will work in and clean the data. We want a table with the raw data in case something happens
create table layoffs_staging
like world_layoffs.layoffs;

SELECT * FROM layoffs_staging;

insert layoffs_staging
select * from layoffs;

SELECT * 
FROM layoffs_staging;

-- no id for this table so more difficult in removing duplicates

SELECT *,
row_number() over(
partition by company,industry,total_laid_off,`date` ) as row_num
FROM layoffs_staging;

with duplicate_cte as
(SELECT *,
row_number() over(
partition by company,location,industry,total_laid_off,`date`,stage,funds_raised) as row_num
FROM layoffs_staging
)
select * from duplicate_cte
where row_num>1;

-- no duplicates exits currently so thats good 
-- 17:49:59	with duplicate_cte as (SELECT *, row_number() over( partition by company,industry,total_laid_off,`date` ) as row_num FROM layoffs_staging ) select * from duplicate_cte where row_num>1	0 row(s) returned	0.016 sec / 0.000036 sec



-- if duplicates exists we wanna delete the duplicate but not all of them we need to have one row 
 
/*

with duplicate_cte as
(SELECT *,
row_number() over(
partition by company,location,industry,total_laid_off,`date`,stage,funds_raised) as row_num
FROM layoffs_staging
)
delete 
from duplicate_cte
where row_num>1;

wont allow cuz you are tring to update
18:08:08	with duplicate_cte as (SELECT *, row_number() over( partition by company,location,industry,total_laid_off,`date`,stage,funds_raised) as row_num FROM layoffs_staging ) delete  from duplicate_cte where row_num>1	Error Code: 1288. The target table duplicate_cte of the DELETE is not updatable	0.0017 sec
so we create new table with row_num and delete all the rows with row_num>1 which deletes duplicates
*/


CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `total_laid_off` double DEFAULT NULL,
  `date` text,
  `percentage_laid_off` text,
  `industry` text,
  `source` text,
  `stage` text,
  `funds_raised` text,
  `country` text,
  `date_added` text,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT * 
FROM layoffs_staging2;

insert into layoffs_staging2
SELECT *,
row_number() over(
partition by company,location,industry,total_laid_off,`date`,stage,funds_raised) as row_num
FROM layoffs_staging;

SELECT * 
FROM layoffs_staging2 
where row_num>1;

    SET SQL_SAFE_UPDATES = 0;
delete
FROM layoffs_staging2 
where row_num>1;

SELECT * 
FROM layoffs_staging2 ;


-- standardize data and fix errors

select distinct company from layoffs_staging2 ;
select company, trim(company) from layoffs_staging2 ; 

update layoffs_staging2
set company = trim(company);

select 
distinct industry 
from layoffs_staging2 
order by 1; 

-- if the industry have same industry descibed as different each time it needs to be updated
SELECT * 
FROM layoffs_staging2
where industry like '%crypto%' ;

update layoffs_staging2
set industry='Crypto'
where industry like '%crypto%' ;

select 
distinct location 
from layoffs_staging2 
order by 1;



select 
distinct country 
from layoffs_staging2 
order by 1;


UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country);


 
/*
Column: date

Collation: utf8mb4_0900_ai_ci

Definition:
date
text
*/

select `date`,
str_to_date(`date`,'%m/%d/%Y')
from layoffs_staging2;

update layoffs_staging2
set `date`= str_to_date(`date`,'%m/%d/%Y');

select * from layoffs_staging2;
-- Column: date

-- Collation: utf8mb4_0900_ai_ci

-- Definition:
-- date
-- text
-- still a text so me modity date datatype 
alter table layoffs_staging2
modify column `date` DATE;

-- Column: date

-- Definition:
-- date
-- date

select * 
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;


select distinct industry 
from layoffs_staging2
where industry is null
or industry = '';



-- 4. remove any columns and rows we need to

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL;

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Delete Useless data we can't really use
DELETE FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * 
FROM world_layoffs.layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;


SELECT * 
FROM world_layoffs.layoffs_staging2;

