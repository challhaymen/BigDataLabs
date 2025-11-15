-- Top 20 des Aéroports par Volume Total de Vols
-- 1. Calculer les vols sortants (Departures)
Departures = GROUP Flights BY Origin;
Outgoing_Count = FOREACH Departures GENERATE
    group AS Airport,
    COUNT(Flights) AS Outgoing_Vols;

-- 2. Calculer les vols entrants (Arrivals)
Arrivals = GROUP Flights BY Dest;
Incoming_Count = FOREACH Arrivals GENERATE
    group AS Airport,
    COUNT(Flights) AS Incoming_Vols;

-- 3. Joindre les deux comptes sur le code d'aéroport (FULL OUTER pour inclure tous les aéroports)
Traffic = JOIN Outgoing_Count BY Airport FULL OUTER, Incoming_Count BY Airport;

-- 4. Calculer le volume total et nettoyer les valeurs NULL après la jointure
Final_Traffic = FOREACH Traffic GENERATE
    (Outgoing_Count::Airport IS NULL ? Incoming_Count::Airport : Outgoing_Count::Airport) AS Airport,
    (Outgoing_Count::Outgoing_Vols IS NULL ? 0 : Outgoing_Count::Outgoing_Vols) AS Outgoing_Vols,
    (Incoming_Count::Incoming_Vols IS NULL ? 0 : Incoming_Count::Incoming_Vols) AS Incoming_Vols,
    (Outgoing_Count::Outgoing_Vols IS NULL ? 0 : Outgoing_Count::Outgoing_Vols) + (Incoming_Count::Incoming_Vols IS NULL ? 0 : Incoming_Count::Incoming_Vols) AS Total_Vols;

-- 5. Trier et prendre le Top 20
Sorted_Traffic = ORDER Final_Traffic BY Total_Vols DESC;
Top20_Airports = LIMIT Sorted_Traffic 20;

-- 6. Joindre avec Airports pour obtenir le nom de la ville/aéroport
Final_Top20 = JOIN Top20_Airports BY Airport, Airports_Clean BY iata;
Final_Top20_Result = FOREACH Final_Top20 GENERATE 
    Top20_Airports::Airport, Top20_Airports::Total_Vols, Airports_Clean::airport, Airports_Clean::city;

DUMP Final_Top20_Result;

-- Popularité des Transporteurs
-- 1. Regrouper par Année et Transporteur (Carrier)
Carrier_Volume = GROUP Flights BY (Year, Carrier);

-- 2. Calculer le volume total (Count) et le volume logarithmique
Log_Volume = FOREACH Carrier_Volume GENERATE
    group AS (Year, Carrier),
    COUNT(Flights) AS Total_Vols,
    LOG10(COUNT(Flights)) AS Log10_Vols;

-- 3. Joindre avec Carriers pour le nom du transporteur
Log_Volume_Named = JOIN Log_Volume BY Carrier, Carriers_Clean BY Code;

-- Résultat du volume logarithmique
DUMP Log_Volume_Named;

-- Proportion de Vols Retardés
-- 1. Créer un champ binaire (1=Retardé, 0=À l'heure/En avance)
Delayed_Flights = FOREACH Flights GENERATE
    Year, Month, DayofMonth, DepTime,
    (ArrDelay > 15 OR DepDelay > 15 ? 1 : 0) AS IsDelayed:int;

-- 2. Regrouper par Année (Granularité minimale pour l'exemple)
Grouped_By_Year = GROUP Delayed_Flights BY Year;

-- 3. Calculer le nombre total et la somme des vols retardés
Delay_Ratio = FOREACH Grouped_By_Year GENERATE
    group AS Year,
    COUNT(Delayed_Flights) AS Total_Vols,
    SUM(Delayed_Flights.IsDelayed) AS Delayed_Count,
    (double)SUM(Delayed_Flights.IsDelayed) / COUNT(Delayed_Flights) AS Delay_Fraction;

DUMP Delay_Ratio;

-- Retards des Transporteurs
-- 1. Créer la structure de base (inclut IsDelayed)
Carrier_Delays = FOREACH Flights GENERATE
    Year, Month, DepTime, Carrier,
    (ArrDelay > 15 OR DepDelay > 15 ? 1 : 0) AS IsDelayed:int;

-- 2. Regrouper par Transporteur et Année (Granularité minimale)
Grouped_By_Carrier_Year = GROUP Carrier_Delays BY (Carrier, Year);

-- 3. Calculer la proportion de retards
Carrier_Delay_Ratio = FOREACH Grouped_By_Carrier_Year GENERATE
    group AS (Carrier, Year),
    COUNT(Carrier_Delays) AS Total_Vols,
    (double)SUM(Carrier_Delays.IsDelayed) / COUNT(Carrier_Delays) AS Carrier_Delay_Fraction;

-- 4. Joindre avec Carriers pour le nom
Carrier_Delay_Named = JOIN Carrier_Delay_Ratio BY Carrier, Carriers_Clean BY Code;

DUMP Carrier_Delay_Named;

-- Itinéraires les Plus Fréquentés
-- 1. Créer la paire non ordonnée (Origin, Dest)
Itineraries = FOREACH Flights GENERATE
    (Origin < Dest ? Origin : Dest) AS AirportA,
    (Origin < Dest ? Dest : Origin) AS AirportB;

-- 2. Regrouper par la paire non ordonnée
Grouped_Itineraries = GROUP Itineraries BY (AirportA, AirportB);

-- 3. Compter la fréquence
Frequency_Itineraries = FOREACH Grouped_Itineraries GENERATE
    group AS Itinerary,
    COUNT(Itineraries) AS Frequency;

-- 4. Trier par fréquence décroissante
Top_Itineraries = ORDER Frequency_Itineraries BY Frequency DESC;

DUMP Top_Itineraries;
