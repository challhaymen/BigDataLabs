-- Chargement et définition du schéma pour les films (JSON imbriqué)
Movies = LOAD 'input/films.json' USING PigStorage()
    AS (full_record:map); 

-- Pour extraire les champs JSON :
-- Note : Nous utilisons des tuples pour les champs complexes (director, actors)
movies_flat = FOREACH Movies GENERATE 
    full_record#'_id' AS idFilm:chararray,
    full_record#'title' AS title:chararray,
    full_record#'year' AS year:int,
    full_record#'genre' AS genre:chararray,
    full_record#'country' AS country:chararray,
    full_record#'director' AS director_bag:bag{(director_id:chararray)},
    full_record#'actors' AS actors_bag:bag{actor:(actor_id:chararray, role:chararray)};

-- Préparation pour l'extraction de l'ID du directeur
Movies_Clean = FOREACH movies_flat GENERATE
    idFilm, title, year, genre, country, 
    (chararray)FLATTEN(director_bag) AS director_id,
    actors_bag;

-- Chargement et définition du schéma pour les artistes
Artists = LOAD 'input/artists.json' USING PigStorage()
    AS (full_record:map);

Artists_Clean = FOREACH Artists GENERATE
    full_record#'_id' AS idArtist:chararray,
    full_record#'last_name' AS last_name:chararray,
    full_record#'first_name' AS first_name:chararray,
    full_record#'birth_date' AS birth_date:chararray;

-- Création de Collections Spécifiques
-- Collection mUSA_annee (Films Américains par Année)
-- Filtrer les films américains
mUSA = FILTER Movies_Clean BY country == 'USA';

-- Regrouper par année
mUSA_annee = GROUP mUSA BY year;
DUMP mUSA_annee;

-- Collection mUSA_director (Films Américains par Réalisateur)
-- Regrouper par ID de réalisateur
mUSA_director = GROUP mUSA BY director_id;
DUMP mUSA_director;

-- Collection mUSA_acteurs (Triplets Film/Acteur/Rôle)
mUSA_acteurs = FOREACH mUSA GENERATE 
    idFilm,
    FLATTEN(actors_bag) AS (actor_id:chararray, role:chararray);
DUMP mUSA_acteurs;

-- Collection moviesActors (Film ID associé à la description complète de l'acteur)
moviesActors = JOIN mUSA_acteurs BY actor_id, Artists_Clean BY idArtist;

-- Projeter les champs souhaités après la jointure
moviesActors = FOREACH moviesActors GENERATE
    mUSA_acteurs::idFilm AS idFilm,
    mUSA_acteurs::role AS role,
    Artists_Clean::idArtist AS idActor,
    Artists_Clean::first_name,
    Artists_Clean::last_name,
    Artists_Clean::birth_date;
DUMP moviesActors;

-- Collection fullMovies (Film complet associé à tous les acteurs complets)
-- Joindre les films aux acteurs complets
Full_Join = JOIN mUSA BY idFilm, moviesActors BY idFilm;

-- Regrouper par film
fullMovies = GROUP Full_Join BY mUSA::idFilm;

-- Projeter les informations
fullMovies = FOREACH fullMovies GENERATE
    group AS idFilm,
    FLATTEN(Full_Join.mUSA) AS (idFilm_movie, title, year, genre, country, director_id, actors_bag), -- Le premier enregistrement du film est suffisant
    Full_Join.moviesActors AS full_actors_list; -- Ce bag contient tous les acteurs complets
DUMP fullMovies;

-- Collection ActeursRealisateurs
-- 1. Films où l'artiste est réalisateur
Directors_Films = FOREACH mUSA GENERATE 
    director_id AS idArtist,
    (idFilm, title, director_id) AS directed_film;

-- 2. Films où l'artiste est acteur
Actors_Films = FOREACH moviesActors GENERATE 
    idActor AS idArtist,
    (idFilm, title, idActor, role) AS acted_film;

-- 3. Utilisation de COGROUP pour fusionner les deux collections sur idArtist
ActeursRealisateurs = COGROUP Directors_Films BY idArtist FULL, Actors_Films BY idArtist;

-- 4. Nettoyage et projection du résultat
ActeursRealisateurs = FOREACH ActeursRealisateurs GENERATE
    group AS idArtist,
    Directors_Films.directed_film AS Directed_Films_List,
    Actors_Films.acted_film AS Acted_Films_List;
    
DUMP ActeursRealisateurs;

-- Enregistrement du résultat final
STORE ActeursRealisateurs INTO 'pigout/ActeursRealisateurs';
