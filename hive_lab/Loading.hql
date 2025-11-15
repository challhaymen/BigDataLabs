-- Chargement des données (depuis le système de fichiers local)
LOAD DATA LOCAL INPATH '/shared_volume/clients.csv'
INTO TABLE clients;

LOAD DATA LOCAL INPATH '/shared_volume/hotels.csv'
INTO TABLE hotels;

-- Charger le CSV dans la table staging
LOAD DATA LOCAL INPATH '/shared_volume/reservations.csv'
INTO TABLE reservations_stage;


-- Insérer les données dans la table partitionnée
-- Attention à l'ordre des colonnes : la colonne de partition (date_debut) doit être en dernier.
INSERT INTO TABLE reservations
PARTITION (date_debut)
SELECT
    reservation_id,
    client_id,
    hotel_id,
    date_fin,
    prix_total,
    date_debut -- La colonne de partition
FROM reservations_stage;
