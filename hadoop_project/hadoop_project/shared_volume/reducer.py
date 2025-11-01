#!/usr/bin/env python
from operator import itemgetter
import sys

current_word = None
current_count = 0
word = None

# lecture ligne par ligne depuis l'entrée standard (STDIN)
for line in sys.stdin:
    line = line.strip()  # supprimer les espaces en début et fin
    # séparation des données par tabulation (comme dans mapper.py)
    word, count = line.split('\t', 1)

    # conversion du count en entier
    try:
        count = int(count)
    except ValueError:
        # ignorer les lignes où count n'est pas un nombre
        continue

    # Hadoop trie la sortie du mapper par clé avant de l'envoyer au reducer
    if current_word == word:
        current_count += count
    else:
        if current_word:
            # écrire le résultat sur STDOUT
            print('%s\t%s' % (current_word, current_count))
        current_count = count
        current_word = word

# sortie pour le dernier mot
if current_word == word:
    print('%s\t%s' % (current_word, current_count))

