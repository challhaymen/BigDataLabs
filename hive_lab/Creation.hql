-- Création de la table clients
CREATE TABLE clients (
    client_id INT,
    nom STRING,
    email STRING,
    telephone STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- Création de la table hotels
CREATE TABLE hotels (
    hotel_id INT,
    nom STRING,
    ville STRING,
    etoiles INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- Création de la table partitionnée reservations
-- date_debut n'est PAS dans la définition des colonnes internes.
CREATE TABLE reservations (
    reservation_id INT,
    client_id INT,
    hotel_id INT,
    date_fin DATE,
    prix_total DOUBLE
)
PARTITIONED BY (date_debut DATE)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- Création de la table temporaire (staging)
-- Cette table contient date_debut car elle reçoit le fichier CSV brut.
CREATE TABLE reservations_stage (
    reservation_id INT,
    client_id INT,
    hotel_id INT,
    date_debut DATE,
    date_fin DATE,
    prix_total DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;
