#!/usr/bin/env python
import sys

# lire les lignes depuis STDIN
for line in sys.stdin:
    line = line.strip()        # supprimer les espaces en début et fin
    words = line.split()       # découper la ligne en mots
    for word in words:
        # afficher chaque mot avec une occurrence = 1
        print(f"{word}\t1")

