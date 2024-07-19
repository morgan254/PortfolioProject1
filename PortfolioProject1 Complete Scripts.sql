Select * 
From PortfolioProject1..CovidDeaths
order by 3,4

--Select * 
--From PortfolioProject1..CovidVaccinations

--Select location, date, total_cases, new_cases, total_deaths, population
--From PortfolioProject1..CovidDeaths
--order by 1,2

Select location, date, total_cases, total_deaths,
(CONVERT(float,total_deaths) / NULLIF(CONVERT(float,total_cases),0))*100 as deathpercentage
From PortfolioProject1..CovidDeaths
Where location like '%Kenya%'
order by 1,2

Select location, date, population, total_cases,
(CONVERT(float,total_cases) / NULLIF(CONVERT(float,population),0))*100 as percentpopulationinfected
From PortfolioProject1..CovidDeaths
Where location like '%Kenya%'
order by 1,2

Select location, population, MAX(total_cases) as highestinfectioncount,
(CONVERT (float, MAX (total_cases)) / NULLIF(CONVERT(float,population),0))*100 as percentpopulationinfected
From PortfolioProject1..CovidDeaths
Group by location, population
Order by percentpopulationinfected desc

Select location, MAX(cast (total_deaths as int)) as totaldeathcount
From PortfolioProject1..CovidDeaths
Where continent is not null
Group by location
Order by totaldeathcount desc

Select location, MAX(cast (total_deaths as int)) as totaldeathcount
From PortfolioProject1..CovidDeaths
Where continent is null
Group by location
Order by totaldeathcount desc

Select continent, MAX(cast (total_deaths as int)) as totaldeathcount
From PortfolioProject1..CovidDeaths
Where continent is not null
Group by continent
Order by totaldeathcount desc

Select date, SUM (cast (new_cases as int)), SUM (cast (new_deaths as int)),
SUM (cast (new_deaths as int)) / SUM (new_cases) as globaldeathpercentage
From PortfolioProject1..CovidDeaths
Where continent is null
Group by date
order by 1,2

Select new_cases, new_deaths
From PortfolioProject1..CovidDeaths

--SELECT 
--    date, 
--    SUM(CAST(new_cases AS int)) AS total_cases,
--    SUM(CAST(new_deaths AS int)) AS total_deaths,
--    CASE
--        WHEN SUM(CAST(new_cases AS int)) = 0 THEN NULL
--        ELSE SUM(CAST(new_deaths AS int)) / SUM(CAST(new_cases AS int))
--    END AS globaldeathpercentage
--FROM 
--    PortfolioProject1..CovidDeaths
--WHERE 
--    continent IS NULL
--GROUP BY 
--    date
--ORDER BY 
--    date;

Select *
From PortfolioProject1..CovidDeaths dea
Join PortfolioProject1..CovidVaccinations vac
	on dea.location = vac.location

SELECT *
From PortfolioProject1..CovidDeaths


--SELECT date, SUM(new_cases), SUM(cast(new_deaths as int)), SUM(cast(new_deaths as int))/SUM(new_cases) *100 as DeathPercentage
--FROM PortfolioProject1..CovidDeaths
--WHERE continent is not null
--GROUP BY date
--ORDER BY 1,2 

SELECT 
    date, 
    SUM(new_cases) AS TotalCases, 
    SUM(CAST(new_deaths AS int)) AS TotalDeaths,
    CASE 
        WHEN SUM(new_cases) = 0 THEN 0
        ELSE (SUM(CAST(new_deaths AS int)) / SUM(new_cases)) * 100 
    END AS DeathPercentage
FROM 
    PortfolioProject1..CovidDeaths
WHERE 
    continent IS NOT NULL
GROUP BY 
    date
ORDER BY 
    date, TotalCases;

SELECT new_deaths, new_cases
FROM PortfolioProject1..CovidDeaths

SELECT *
FROM PortfolioProject1.dbo.CovidDeaths dea
JOIN PortfolioProject1..CovidVaccinations vac
	ON dea.location = vac.location 
	and dea.date = vac.date

SELECT *
FROM PortfolioProject1..CovidVaccinations

--Looking at Total Population vs Vaccinations
SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations
, SUM (Cast(vac.new_vaccinations as bigint)) OVER (Partition by dea.location Order by dea.location, dea.date) as CumulativeTotalVaccinations
FROM PortfolioProject1..CovidDeaths dea
JOIN PortfolioProject1..CovidVaccinations vac
ON dea.location = vac.location
and dea.date = vac.date
WHERE dea.continent is not null
order by 2,3

-- Finding the percentage population vaccinated per country

-- USING CTE (Common Table Expression)
WITH PopvsVac (continent, location, date, population, new_vaccinations, CumulativeTotalVaccinations)
AS
(
SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations
, SUM (Cast(vac.new_vaccinations as bigint)) OVER (Partition by dea.location Order by dea.location, dea.date) as CumulativeTotalVaccinations
FROM PortfolioProject1..CovidDeaths dea
JOIN PortfolioProject1..CovidVaccinations vac
ON dea.location = vac.location
and dea.date = vac.date
WHERE dea.continent is not null
)
SELECT * , (CumulativeTotalVaccinations/population)*100 AS PercentPopulationVaccinated
FROM PopvsVac

--TEMP TABLE

DROP TABLE IF EXISTS #PercentPopulationVaccinated
CREATE TABLE #PercentPopulationVaccinated
(
continent nvarchar(255),
location nvarchar(255),
date datetime,
population numeric,
new_vaccinations numeric,
CumulativeTotalVaccinations numeric
)

INSERT INTO #PercentPopulationVaccinated
SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations
, SUM (CAST(vac.new_vaccinations AS bigint)) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.date) AS CumulativeTotalVaccinations
FROM PortfolioProject1..CovidDeaths dea
JOIN PortfolioProject1..CovidVaccinations vac
ON dea.location = vac.location
AND dea.date = vac.date
WHERE dea.continent is not null

SELECT *, (CumulativeTotalVaccinations/Population)*100 AS PercentPopulationVaccinated
From #PercentPopulationVaccinated

--(TEMP TABLE METHOD FULL SOLUTION 2)

-- Step 1: Create the temporary table
CREATE TABLE #PercentPopulationVaccinated
(
    continent NVARCHAR(255),
    location NVARCHAR(255),
    date DATETIME,
    population NUMERIC,
    new_vaccinations NUMERIC,
    CumulativeTotalVaccinations NUMERIC
);

-- Step 2: Insert data into the temporary table
INSERT INTO #PercentPopulationVaccinated
SELECT 
    dea.continent, 
    dea.location, 
    dea.date, 
    dea.population, 
    vac.new_vaccinations,
    SUM(CAST(vac.new_vaccinations AS BIGINT)) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.date) AS CumulativeTotalVaccinations
FROM 
    PortfolioProject1..CovidDeaths dea
JOIN 
    PortfolioProject1..CovidVaccinations vac
    ON dea.location = vac.location
    AND dea.date = vac.date
WHERE 
    dea.continent IS NOT NULL;

-- Step 3: Query the temporary table for the final result
SELECT 
    *,
    (CumulativeTotalVaccinations / population) * 100 AS PercentPopulationVaccinated
FROM 
    #PercentPopulationVaccinated;

-- Step 4: Drop the temporary table if no longer needed
DROP TABLE #PercentPopulationVaccinated;

--Total deaths per continent
Select continent, MAX(cast (total_deaths as int)) as totaldeathcount
From PortfolioProject1..CovidDeaths
Where continent is not null
Group by continent
Order by totaldeathcount desc

--Creating view for later visualization
DROP VIEW IF EXISTS PercentPopulationVaccinated
CREATE VIEW PercentPopulationVaccinatedNew AS
SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations
, SUM (CAST(vac.new_vaccinations AS bigint)) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.date) AS CumulativeTotalVaccinations
FROM PortfolioProject1..CovidDeaths dea
JOIN PortfolioProject1..CovidVaccinations vac
ON dea.location = vac.location
AND dea.date = vac.date
WHERE dea.continent is not null