# Questionnaire DBA

*Mis à jour le 4 novembre 2026*

Le déploiement des connecteurs Debezium est bloqué tant que nous n'avons pas ces réponses. Merci de :

1. remplir l'inventaire des bases ;
2. remplir la colonne **Réponse DBA** ;
3. passer l'état de chaque question à **Répondu** (ou **Sans objet**).

Dès réception, nous fournirons pour chaque base la liste exacte des paramètres, du compte et des droits à mettre en place.

Rappel : Debezium tourne sur **mcq-dbz70** et **mcq-dbz71**. Il lit le journal de transactions de chaque base (LogMiner pour Oracle, réplication logique `pgoutput` pour PostgreSQL) et n'écrit pas dans les tables métier.

## Inventaire des bases à capturer

Une ligne par base. Ajoutez des lignes si besoin.

| Techno | Hôte et port | Base, service ou PDB | Version | HA, RAC, CDB | Schémas et tables à capturer | Snapshot initial (oui / non) | Volume WAL ou redo par jour | Contact DBA |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| PostgreSQL | | | | | | | | |
| PostgreSQL | | | | | | | | |
| Oracle | | | | | | | | |
| Oracle | | | | | | | | |

## Questions générales

| # | Question | Pourquoi nous en avons besoin | Réponse DBA | État |
| --- | --- | --- | --- | --- |
| G1 | La licence GoldenGate sera-t-elle conservée après la migration ? | Elle seule autorise le mode XStream ; sinon, ce sera LogMiner | | En attente |
| G2 | GoldenGate et Debezium tourneront-ils en parallèle pendant la transition ? À partir de quel point (date, SCN, LSN) Debezium doit-il démarrer ? | Fixer la position de départ, sans trou ni doublon côté consommateurs | | En attente |
| G3 | Les cibles sont-elles déjà chargées ? Pour quelles tables faut-il un snapshot initial ? | Le snapshot relit toute la table en `SELECT` : il faut en planifier la charge et la durée | | En attente |
| G4 | Quelle durée maximale d'interruption du flux faut-il pouvoir rattraper (en heures) ? | Dimensionne la rétention des WAL et des archives redo | | En attente |
| G5 | Y a-t-il des tables sans clé primaire parmi celles à capturer ? | La clé primaire sert de clé aux messages Kafka ; sans elle, il faut désigner des colonnes | | En attente |
| G6 | Y a-t-il des colonnes LOB (CLOB, BLOB, BYTEA), XMLTYPE, spatiales, ou des NUMBER sans précision ? | Options du connecteur à activer, taille des messages, format des décimaux | | En attente |
| G7 | À quelle fréquence la structure des tables change-t-elle (DDL), et qui l'annonce ? | Debezium suit les DDL, mais les consommateurs doivent être prévenus | | En attente |
| G8 | Quelles sont les plus grosses transactions (nombre de lignes, durée) : batchs de nuit, purges ? | En LogMiner, une transaction est gardée en mémoire jusqu'au COMMIT : cela dimensionne Kafka Connect | | En attente |
| G9 | Combien de lignes sont modifiées par jour, en moyenne et en pointe ? | Rétention des topics et taille du FS Kafka (70 Go aujourd'hui) | | En attente |
| G10 | Quels sont le jeu de caractères et le fuseau horaire de chaque base ? | Conversion correcte des textes et des dates | | En attente |
| G11 | Quelle est la politique d'expiration des mots de passe des comptes techniques ? Un profil sans expiration est-il possible ? | Un mot de passe expiré arrête le connecteur | | En attente |
| G12 | Un filtrage réseau existe-t-il entre mcq-dbz70/71 et les bases ? Sur quels ports écoutent-elles ? | Ouverture des flux | | En attente |
| G13 | Quelles fenêtres de maintenance et bascules HA sont prévues, et comment nous prévenir ? | Mettre les connecteurs en pause et poser un silence sur les alertes | | En attente |
| G14 | Qui surveille aujourd'hui les slots PostgreSQL et la rétention des archives Oracle ? | Un connecteur arrêté fait grossir les WAL : la supervision doit être partagée | | En attente |

## PostgreSQL

| # | Question | Pourquoi nous en avons besoin | Réponse DBA | État |
| --- | --- | --- | --- | --- |
| P1 | Version exacte (mineure comprise) ? | Fonctions disponibles : `max_slot_wal_keep_size` depuis la 13, synchronisation des slots depuis la 17 | | En attente |
| P2 | Haute disponibilité : Patroni, repmgr, autre ? Debezium doit-il se connecter par une VIP ou par un nom DNS qui suit le primaire ? | Sans gestion des slots, une bascule perd le slot et impose un nouveau snapshot | | En attente |
| P3 | Valeurs actuelles de `wal_level`, `max_replication_slots` et `max_wal_senders`, et combien sont déjà utilisés ? | Il faut `wal_level = logical` et un slot par connecteur | | En attente |
| P4 | Un redémarrage pour passer `wal_level` à `logical` est-il possible, et quand ? | Ce paramètre n'est pris en compte qu'au redémarrage | | En attente |
| P5 | Des slots ou publications de réplication logique existent-ils déjà ? | Cohabitation et nommage | | En attente |
| P6 | La publication sera-t-elle créée par les DBA (recommandé) ou par Debezium ? | Pour que Debezium la crée, son compte doit être propriétaire des tables | | En attente |
| P7 | `REPLICA IDENTITY FULL` est-il acceptable, et sur quelles tables ? | Nécessaire pour avoir l'image complète avant modification ; augmente le volume de WAL | | En attente |
| P8 | Des tables partitionnées sont-elles à capturer ? | Une option de publication est à activer pour publier sous le nom de la table mère | | En attente |
| P9 | Quelle valeur de `max_slot_wal_keep_size` est acceptable ? | Protège le disque de la base ; au-delà de cette valeur, le connecteur doit refaire un snapshot | | En attente |
| P10 | Quel espace disque est disponible pour `pg_wal` ? | Détermine la durée d'interruption du connecteur que la base peut supporter | | En attente |
| P11 | Pouvez-vous ajouter mcq-dbz70/71 dans `pg_hba.conf`, et avec quelle méthode (`scram-sha-256`) ? | Connexion du compte Debezium en mode réplication | | En attente |

## Oracle

| # | Question | Pourquoi nous en avons besoin | Réponse DBA | État |
| --- | --- | --- | --- | --- |
| O1 | Version et niveau de patch (ex. 19c et sa Release Update) ? | Compatibilité avec Debezium 3.1 et le pilote ojdbc11 21.15 | | En attente |
| O2 | Architecture CDB/PDB (noms du CDB et des PDB) ou non-CDB ? | En CDB, le compte Debezium est un compte commun `C##...` et le connecteur cible une PDB | | En attente |
| O3 | RAC (nombre de nœuds, adresse SCAN) ou instance unique ? | Configuration du connecteur, lecture des redo de tous les threads | | En attente |
| O4 | Data Guard : la capture sur le primaire est-elle acceptée, ou faut-il lire sur un standby (archives seules) ? | Arbitrage entre la charge sur le primaire et une latence plus élevée sur le standby | | En attente |
| O5 | La base est-elle déjà en `ARCHIVELOG` ? | Prérequis bloquant pour LogMiner | | En attente |
| O6 | Quel supplemental logging est en place (minimal, `ALL COLUMNS` par table) ? Il l'est probablement déjà pour GoldenGate | Il faut le minimal au niveau de la base et `ALL COLUMNS` sur chaque table capturée | | En attente |
| O7 | Taille et nombre de groupes de redo, nombre de bascules de log par heure en pointe ? | Des bascules trop fréquentes ralentissent LogMiner | | En attente |
| O8 | Destination, taille et rétention des archives (politique RMAN, FRA) ? | Les archives doivent rester disponibles pendant une interruption du connecteur | | En attente |
| O9 | Quel tablespace pour le compte Debezium ? | Il y crée une petite table technique `LOG_MINING_FLUSH` | | En attente |
| O10 | Qui crée le compte Debezium avec les droits LogMiner, et quand ? (la liste des droits sera fournie à réception de ce questionnaire) | Prérequis bloquant | | En attente |
| O11 | Valeurs de `NLS_CHARACTERSET` et `NLS_NCHAR_CHARACTERSET` ? | Encodage des textes dans les messages | | En attente |
| O12 | Quelle marge CPU et I/O reste disponible sur le serveur aux heures de pointe ? | Les sessions LogMiner s'exécutent sur le serveur Oracle | | En attente |
