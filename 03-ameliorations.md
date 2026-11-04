# Améliorations et optimisations futures

*Mis à jour le 4 novembre 2026*

Six chantiers sont prioritaires avant la mise en production :

1. la purge des logs ;
2. la supervision des slots PostgreSQL ;
3. l'envoi des alertes ;
4. les tests de résilience ;
5. le dimensionnement de la rétention ;
6. le retour à Kafka 3.9.2.

Le reste relève de la sécurité et du confort d'exploitation.

Hors de cette liste, deux étapes du projet restent à faire : le paramétrage des bases par les DBA (étape 5) et le déploiement des connecteurs Debezium (étape 6).

| Priorité | Amélioration | Pourquoi | Prérequis | Statut |
| --- | --- | --- | --- | --- |
| Haute | Purger les logs de `/var/log/kafka` (tâche planifiée ou `tmpfiles.d`, rétention 14 jours) | Le log4j fourni par Kafka crée un fichier par jour sans jamais les supprimer : le FS système finira par se remplir | aucun | À faire |
| Haute | Superviser les slots de réplication PostgreSQL (WAL retenus) et la rétention des archives Oracle | Un connecteur arrêté bloque la purge des WAL et peut remplir le disque de la base PostgreSQL. Côté Oracle, des archives purgées trop tôt imposent un nouveau snapshot | DBA ; un `postgres_exporter` n'est pas dans le bundle | À faire |
| Haute | Destinataire pour Alertmanager (SMTP interne ou webhook) | Les alertes sont visibles dans l'interface mais ne sont envoyées à personne | adresse du relais SMTP ou URL du webhook | À faire |
| Haute | Tests de résilience : arrêt d'un broker, du controller leader, d'un worker Connect | Vérifier la tolérance à la panne et le déclenchement des alertes avant la production | aucun | À faire |
| Haute | Fixer la rétention des topics par taille (`retention.bytes`) et agrandir le FS Kafka de 70 Go si besoin | Avec une réplication de 3, chaque broker porte une copie complète de tous les topics : environ 55 Go utiles au total | volume de modifications par jour | À faire |
| Haute | Revenir à Kafka 3.9.2 | Dernière version corrective de la branche 3.9 | redémarrage progressif, un nœud à la fois | À faire |
| Moyenne | Authentification Active Directory pour AKHQ et Grafana | Comptes nominatifs et traçabilité, à la place des comptes partagés `admin` / `lecteur` | serveur LDAP, compte de service, groupes AD | À faire |
| Moyenne | Passer Kafka en SASL_SSL | Les mots de passe SCRAM et les données circulent en clair sur le réseau | PKI et certificats par machine | À faire |
| Moyenne | HTTPS sur AKHQ, Grafana, Prometheus, Alertmanager et l'API REST Connect | Les interfaces sont ouvertes à tous les réseaux et l'authentification Basic circule en clair | PKI | À faire |
| Moyenne | Tableaux de bord Grafana : Node Exporter Full (1860), Kafka Exporter Overview (7589) et un tableau Kafka-CDC dédié | Grafana n'a encore aucun tableau de bord | fichiers JSON à ajouter au bundle ; connecteurs actifs pour la partie Debezium | À faire |
| Moyenne | Ajuster les seuils d'alerte (retard Debezium 5 min, lag consommateurs 100 000 messages) | Seuils provisoires, fixés sans connaître les volumes | quelques semaines de métriques | À faire |
| Moyenne | Valider le dimensionnement (heap 6 Go brokers, 8 Go Connect) | Le mode LogMiner garde les transactions longues en mémoire | volumes et transactions longues fournis par les DBA, tests de charge | À faire |
| Moyenne | Sauvegarder les configurations et `grafana.db` | Pouvoir reconstruire une machine sans repartir de zéro | outil de sauvegarde de l'exploitation | À faire |
| Moyenne | Runbook d'exploitation et procédure de rotation des mots de passe | Exploitation par d'autres équipes | aucun | À faire |
| Basse | Retirer les mots de passe des fichiers `.properties` (`FileConfigProvider`) | Limiter les secrets en clair dans les configurations | aucun | À faire |
| Basse | API REST Connect en lecture seule via nginx (option B) | Le compte REST unique a tous les droits | RPM nginx | À faire |
| Basse | Authentification sur les ports de métriques (9100, 7071, 7072) | Ces ports exposent l'état des machines sans contrôle d'accès | aucun | À faire |
| Basse | Évaluer un Schema Registry (Apicurio) et le format Avro | Le JSON sans schéma perd une partie des types, notamment les décimaux | besoin des consommateurs | À faire |
| Basse | Évaluer Oracle XStream à la place de LogMiner | Latence et charge plus faibles à très fort volume | licence GoldenGate conservée, Instant Client | À faire |
