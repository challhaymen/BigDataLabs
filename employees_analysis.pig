-- 1. Chargement des données d'employés
-- Structure: ID, Nom, Prenom, Sexe, Salaire, DepNo, Ville, Region
Employees = LOAD 'input/employees.txt' USING PigStorage(',') 
    AS (ID:int, Name:chararray, Gender:chararray, Salary:long, DepNo:int, City:chararray, Region:chararray);

-- 2. Chargement des données de départements
-- Structure: DepNo, DeptName
Departments = LOAD 'input/departements.txt' USING PigStorage(',') 
    AS (DepNo:int, DeptName:chararray);

-- 3. Jointure des deux ensembles de données sur le numéro de département (DepNo)
-- La jointure permet d'avoir toutes les informations dans une seule structure.
Joined_Data = JOIN Employees BY DepNo LEFT OUTER, Departments BY DepNo;

-- Salaire Moyen des Employés par Département
Avg_Salary_Per_Dept = GROUP Joined_Data BY DeptName;
Result_Avg_Salary = FOREACH Avg_Salary_Per_Dept 
    GENERATE group AS DeptName, AVG(Joined_Data.Salary) AS Avg_Salary;
DUMP Result_Avg_Salary;

-- Nombre d'Employés par Département
Count_Emp_Per_Dept = GROUP Joined_Data BY DeptName;
Result_Count_Emp = FOREACH Count_Emp_Per_Dept 
    GENERATE group AS DeptName, COUNT(Joined_Data) AS Employee_Count;
DUMP Result_Count_Emp;

-- Liste de tous les Employés avec leurs Départements
Result_Emp_Dept_List = FOREACH Joined_Data 
    GENERATE Name, Gender, Salary, City, DeptName;
DUMP Result_Emp_Dept_List;

-- Employés ayant un Salaire Supérieur à 60 000
High_Salary_Emp = FILTER Joined_Data BY Salary > 60000;
Result_High_Salary = FOREACH High_Salary_Emp 
    GENERATE Name, Salary, DeptName;
DUMP Result_High_Salary;

-- Département avec le Salaire le plus Élevé
-- Trie les données par salaire décroissant
Sorted_by_Salary = ORDER Joined_Data BY Salary DESC;
-- Prend le premier enregistrement
Highest_Paid_Dept = LIMIT Sorted_by_Salary 1;
Result_Highest_Paid = FOREACH Highest_Paid_Dept 
    GENERATE Name, Salary, DeptName;
DUMP Result_Highest_Paid;

-- Départements sans Employés
No_Employee_Dept = FILTER Joined_Data BY Employees::ID is null;
Result_No_Emp_Dept = FOREACH No_Employee_Dept 
    GENERATE Departments::DeptName AS Empty_Department;
DUMP Result_No_Emp_Dept;

-- Nombre Total d'Employés dans l'Entreprise
-- Groupe toutes les données en un seul groupe (ALL)
Total_Employees = GROUP Employees ALL;
-- Compte le nombre d'employés dans ce groupe unique
Result_Total_Count = FOREACH Total_Employees 
    GENERATE COUNT(Employees) AS Total_Employee_Count;
DUMP Result_Total_Count;

-- Employés de la Ville de Paris
Paris_Employees = FILTER Joined_Data BY City == 'Paris';
Result_Paris_Emp = FOREACH Paris_Employees 
    GENERATE Name, DeptName, Salary;
DUMP Result_Paris_Emp;

-- Salaire Total des Employés dans chaque Ville
Total_Salary_Per_City = GROUP Joined_Data BY City;
Result_Total_Salary_City = FOREACH Total_Salary_Per_City 
    GENERATE group AS City, SUM(Joined_Data.Salary) AS Total_Salary;
DUMP Result_Total_Salary_City;

-- Départements qui ont des Femmes Employées
-- Filtre pour obtenir uniquement les femmes
Female_Employees = FILTER Joined_Data BY Gender == 'Female';
-- Groupe par nom de département (pour éliminer les doublons de département)
Dept_With_Female_Emp = GROUP Female_Employees BY DeptName;
-- Projette le nom du département
Result_Female_Dept = FOREACH Dept_With_Female_Emp 
    GENERATE group AS DeptName;
DUMP Result_Female_Dept;
