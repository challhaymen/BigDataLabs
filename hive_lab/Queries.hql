-- 5. Utilisation de requêtes simples --
-- Lister tous les clients --
SELECT * FROM clients;

-- Lister tous les hôtels à Paris --
SELECT hotel_id, nom, etoiles
FROM hotels_partitioned
WHERE ville = 'Paris';

-- Lister toutes les réservations avec les informations sur les hôtels et les clients --
SELECT r.reservation_id,
       c.nom AS client_nom,
       c.email AS client_email,
       h.nom AS hotel_nom,
       h.ville AS hotel_ville,
       h.etoiles AS hotel_etoiles,
       r.date_debut,
       r.date_fin,
       r.prix_total
FROM reservations_bucketed r
JOIN clients c
  ON r.client_id = c.client_id
JOIN hotels_partitioned h
  ON r.hotel_id = h.hotel_id;

-- Requêtes avec jointures --
-- Afficher le nombre de réservations par client --
SELECT c.nom AS client_nom,
       COUNT(r.reservation_id) AS nb_reservations
FROM clients c
LEFT JOIN reservations_bucketed r
  ON c.client_id = r.client_id
GROUP BY c.nom;

-- Afficher les Hôtels réservés par chaque client --
SELECT c.nom AS client_nom,
       h.nom AS hotel_nom
FROM clients c
JOIN reservations_bucketed r
  ON c.client_id = r.client_id
JOIN hotels_partitioned h
  ON r.hotel_id = h.hotel_id;

-- Afficher les noms des hôtels avec plus d’une réservation --
SELECT h.nom AS hotel_nom,
       COUNT(r.reservation_id) AS nb_reservations
FROM hotels_partitioned h
JOIN reservations_bucketed r
  ON h.hotel_id = r.hotel_id
GROUP BY h.nom
HAVING COUNT(r.reservation_id) > 1;

-- Afficher les noms des hôtels sans réservation --
SELECT h.nom AS hotel_nom
FROM hotels_partitioned h
LEFT JOIN reservations_bucketed r
  ON h.hotel_id = r.hotel_id
WHERE r.reservation_id IS NULL;

-- Requêtes imbriquées --
-- Clients ayant réservé un hôtel avec plus de 4 étoiles --
SELECT DISTINCT c.nom AS client_nom
FROM clients c
JOIN reservations_bucketed r
  ON c.client_id = r.client_id
JOIN hotels_partitioned h
  ON r.hotel_id = h.hotel_id
WHERE h.etoiles > 4;

-- Total des revenus générés par chaque hôtel --
SELECT h.nom AS hotel_nom,
       SUM(r.prix_total) AS total_revenus
FROM hotels_partitioned h
JOIN reservations_bucketed r
  ON h.hotel_id = r.hotel_id
GROUP BY h.nom;

-- Utilisation de fonctions d’agrégation avec partitions et buckets --
-- Revenus totaux par ville (table partitionnée) --
SELECT ville,
       SUM(r.prix_total) AS total_revenus
FROM hotels_partitioned h
JOIN reservations_bucketed r
  ON h.hotel_id = r.hotel_id
GROUP BY ville;

-- Nombre total de réservations par client (table bucketée) --
SELECT c.nom AS client_nom,
       COUNT(r.reservation_id) AS nb_reservations
FROM clients c
JOIN reservations_bucketed r
  ON c.client_id = r.client_id
GROUP BY c.nom;

-- Nettoyage et suppression des données --
DROP TABLE IF EXISTS reservations_bucketed;
DROP TABLE IF EXISTS reservations_stage;
DROP TABLE IF EXISTS reservations;

DROP TABLE IF EXISTS hotels_partitioned;
DROP TABLE IF EXISTS hotels;

DROP TABLE IF EXISTS clients;

DROP DATABASE IF EXISTS hotel_booking CASCADE;

