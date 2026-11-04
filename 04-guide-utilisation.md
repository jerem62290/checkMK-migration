# Guide d'utilisation et d'administration

*Mis à jour le 4 novembre 2026*

Toute l'administration passe par la toolbox mcq-dbt70. Les interfaces web servent à consulter ; les commandes Kafka et l'API REST de Connect servent à agir. Les mots de passe sont dans le coffre.

## Accès

| Outil | URL | Comptes | Usage |
| --- | --- | --- | --- |
| AKHQ | http://mcq-dbt70.cgi.credit-cgi.fr:8080 | `admin` (administration), `lecteur` (consultation) | parcourir topics et messages, groupes de consommateurs, connecteurs, ACLs |
| Grafana | http://mcq-dbt70.cgi.credit-cgi.fr:3000 | `admin` | tableaux de bord, requêtes sur les métriques (Explore) |
| Prometheus | http://mcq-dbt70.cgi.credit-cgi.fr:9090 | `admin` | état des cibles, règles et alertes en cours |
| Alertmanager | http://mcq-dbt70.cgi.credit-cgi.fr:9093 | `admin` | alertes actives, silences pendant une maintenance |
| API REST Connect | http://mcq-dbz70.cgi.credit-cgi.fr:8083 (ou dbz71) | `connect-admin` | créer, mettre en pause, redémarrer, supprimer des connecteurs |
| Commandes Kafka | `ssh mcq-dbt70`, puis root | `/root/kafka-admin.properties` (compte `admin`) | topics, comptes, ACLs, groupes |

## Pour les DBA

### Ce qui est en place

- Deux serveurs, **mcq-dbz70 et mcq-dbz71**, hébergent Debezium 3.1.3. Ce sont les seules machines qui se connecteront à vos bases : ce sont elles qu'il faut autoriser (`pg_hba.conf`, listener Oracle, filtrage réseau éventuel).
- Un connecteur correspond à une base source et à un compte Debezium. Chaque connecteur tourne sur un seul des deux workers à la fois. Si ce worker tombe, l'autre reprend le connecteur à la dernière position enregistrée.
- Debezium **ne modifie pas vos données métier**. Il lit le journal de transactions. À la première mise en route, si on le demande, il fait aussi un snapshot des tables configurées, en lecture seule (`SELECT`).
- Seules les transactions **validées** sont publiées. Un ROLLBACK n'apparaît jamais dans Kafka.
- Oracle est lu en mode **LogMiner**, sans licence supplémentaire. XStream n'est pas utilisé.

### Ce que Debezium crée ou utilise sur vos bases

| Base | Élément | Impact à surveiller |
| --- | --- | --- |
| PostgreSQL | un **slot de réplication logique** par connecteur (plugin `pgoutput`, intégré) | tant que le connecteur est arrêté, le slot retient les WAL : le disque peut se remplir |
| PostgreSQL | une **publication** listant les tables capturées (créée par vous de préférence) | aucun |
| Oracle | des sessions **LogMiner** ouvertes en continu par le compte Debezium | consommation CPU et I/O sur le serveur Oracle |
| Oracle | une petite table `LOG_MINING_FLUSH` dans le schéma du compte Debezium | aucun |
| Oracle | lecture des **redo et archive logs** | les archives doivent rester disponibles au moins aussi longtemps que la plus longue interruption tolérée du connecteur |

### Ce dont nous aurons besoin (détail fourni à l'étape 5)

**PostgreSQL**

- `wal_level = logical` (nécessite un redémarrage), et `max_replication_slots` et `max_wal_senders` dimensionnés pour un slot par connecteur.
- Un compte avec l'attribut `REPLICATION` et le droit `SELECT` sur les tables capturées.
- La publication des tables à capturer.
- `REPLICA IDENTITY FULL` sur les tables dont les consommateurs ont besoin de l'image avant modification.
- Recommandé à partir de PostgreSQL 13 : `max_slot_wal_keep_size`, pour qu'un slot abandonné ne remplisse pas le disque.
- En cas de haute disponibilité (Patroni ou autre) : la manière dont les slots survivent à une bascule.

**Oracle**

- Base en `ARCHIVELOG`.
- Supplemental logging minimal au niveau de la base, et `ALL COLUMNS` sur chaque table capturée.
- Un compte Debezium avec les droits LogMiner. En architecture CDB/PDB, ce doit être un compte commun `C##...`.
- Des redo logs correctement dimensionnés, et une rétention des archives compatible avec une interruption du connecteur.

**Informations à nous transmettre** : versions, architecture (CDB/PDB, RAC, haute disponibilité), volume de redo ou de WAL par jour, transactions longues connues (batchs, purges), maintien ou non de la licence GoldenGate. Le détail est dans `05-questionnaire-dba.md`.

### Requêtes de contrôle côté base

PostgreSQL, retard et volume de WAL retenu par slot :

```sql
SELECT slot_name, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS wal_retenu
FROM pg_replication_slots WHERE slot_type = 'logical';
```

Oracle, mode d'archivage et supplemental logging :

```sql
SELECT log_mode, supplemental_log_data_min, supplemental_log_data_all FROM v$database;
```

### Voir les modifications arriver

Dans AKHQ (compte `lecteur`), ouvrez **Topics**, choisissez `cdc.<techno>.<source>.<schéma>.<table>`, puis **Data**. Chaque message correspond à une ligne modifiée.

| Champ du message | Contenu |
| --- | --- |
| `op` | `c` insertion, `u` mise à jour, `d` suppression, `r` lecture du snapshot initial |
| `before` | la ligne avant modification (complète seulement avec `REPLICA IDENTITY FULL` ou le supplemental logging `ALL COLUMNS`) |
| `after` | la ligne après modification (vide pour une suppression) |
| `source` | base, schéma, table, LSN ou SCN, horodatage de la transaction |
| `ts_ms` | heure de traitement par Debezium |

Après une suppression, Debezium publie aussi un message vide (*tombstone*) avec la même clé.

## Préparer une session d'administration (mcq-dbt70, en root)

```bash
export JAVA_HOME=/usr/lib/jvm/jre-21-openjdk K=/opt/kafka/current/bin BS=mcq-dbk70.cgi.credit-cgi.fr:9092
ADM="--bootstrap-server $BS --command-config /root/kafka-admin.properties"
read -rsp 'PW connect-admin : ' CA; echo; CON=http://mcq-dbz70.cgi.credit-cgi.fr:8083
```

Les exemples suivants utilisent `$K`, `$ADM`, `$CA` et `$CON`. Sous `sudo`, `/usr/local/bin` n'est pas dans le `PATH` : appelez `promtool` et `amtool` par leur chemin complet.

## Services

| Machine | Service | Logs |
| --- | --- | --- |
| mcq-dbk70/71/72 | `kafka` | `/var/log/kafka/server.log`, `controller.log` |
| mcq-dbz70/71 | `kafka-connect` | `/var/log/kafka/connect.log` |
| mcq-dbt70 | `akhq`, `prometheus`, `alertmanager`, `grafana-server`, `kafka_exporter`, `blackbox_exporter` | `journalctl -u <service>` ; Grafana : `/var/log/grafana` |
| toutes | `node_exporter` | `journalctl -u node_exporter` |

```bash
systemctl status kafka            # état
systemctl restart kafka           # redémarrage
journalctl -u akhq -n 50 --no-pager
```

Règles d'exploitation :

1. **Un broker à la fois.** Avant de passer au suivant, `$K/kafka-topics.sh $ADM --describe --under-replicated-partitions` ne doit rien afficher.
2. Arrêt complet : les workers Connect d'abord, puis les brokers. Redémarrage : les brokers d'abord (au moins 2 sur 3 pour le quorum), puis Connect.
3. Avant une maintenance, posez un **silence** dans Alertmanager (bouton **New Silence**).

## Kafka : état du cluster et topics

```bash
$K/kafka-metadata-quorum.sh $ADM describe --status          # leader, votants [1,2,3], retard des followers
$K/kafka-topics.sh $ADM --list
$K/kafka-topics.sh $ADM --describe --topic <topic>          # partitions, leaders, ISR
$K/kafka-topics.sh $ADM --describe --under-replicated-partitions
$K/kafka-configs.sh $ADM --alter --entity-type topics --entity-name <topic> --add-config retention.ms=259200000
```

La dernière commande passe la rétention d'un topic à 3 jours.

## Kafka : consommateurs

```bash
$K/kafka-consumer-groups.sh $ADM --list
$K/kafka-consumer-groups.sh $ADM --describe --group <groupe>     # colonne LAG = retard
$K/kafka-consumer-groups.sh $ADM --group <groupe> --topic <topic> \
  --reset-offsets --to-datetime 2026-11-04T08:00:00.000 --execute
```

Le repositionnement n'est possible que si toutes les instances du consommateur sont arrêtées. Remplacez `--execute` par `--dry-run` pour voir le résultat sans l'appliquer.

## Kafka : comptes et ACLs

Créer un compte pour une application consommatrice :

```bash
read -rsp 'PW app-<nom> : ' PW; echo
$K/kafka-configs.sh $ADM --alter --entity-type users --entity-name app-<nom> --add-config "SCRAM-SHA-512=[iterations=8192,password=$PW]"; unset PW
$K/kafka-acls.sh $ADM --add --allow-principal User:app-<nom> --operation Read --operation Describe --topic cdc.ora.FACT. --resource-pattern-type prefixed
$K/kafka-acls.sh $ADM --add --allow-principal User:app-<nom> --operation Read --group app-<nom> --resource-pattern-type prefixed
```

Le préfixe de topic (ici `cdc.ora.FACT.`) limite l'application aux tables dont elle a besoin. Ses groupes de consommateurs doivent commencer par `app-<nom>`.

Changer un mot de passe, lister et retirer des droits :

```bash
$K/kafka-configs.sh $ADM --alter --entity-type users --entity-name <compte> --add-config "SCRAM-SHA-512=[iterations=8192,password=...]"
$K/kafka-acls.sh $ADM --list --principal User:<compte>
$K/kafka-acls.sh $ADM --remove --allow-principal User:<compte> --operation Read --topic <topic>
```

Après le changement du mot de passe d'un compte utilisé par un service (`connect`, `akhq`, `kexporter`), mettez à jour le fichier de configuration du service et redémarrez-le.

Configuration à fournir aux équipes consommatrices :

```properties
bootstrap.servers=mcq-dbk70.cgi.credit-cgi.fr:9092,mcq-dbk71.cgi.credit-cgi.fr:9092,mcq-dbk72.cgi.credit-cgi.fr:9092
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="app-<nom>" password="<mot de passe>";
group.id=app-<nom>-<usage>
```

## Kafka Connect : connecteurs

```bash
curl -s -u "connect-admin:$CA" "$CON/connectors?expand=status"                 # liste et état
curl -s -u "connect-admin:$CA" "$CON/connectors/<nom>/status"
curl -s -u "connect-admin:$CA" -X PUT -H 'Content-Type: application/json' \
  --data @<nom>.json "$CON/connectors/<nom>/config"                           # créer ou modifier
curl -s -u "connect-admin:$CA" -X PUT  "$CON/connectors/<nom>/pause"
curl -s -u "connect-admin:$CA" -X PUT  "$CON/connectors/<nom>/resume"
curl -s -u "connect-admin:$CA" -X POST "$CON/connectors/<nom>/restart?includeTasks=true&onlyFailed=true"
curl -s -u "connect-admin:$CA" "$CON/connectors/<nom>/offsets"                  # position LSN ou SCN
curl -s -u "connect-admin:$CA" -X DELETE "$CON/connectors/<nom>"
curl -s -u "connect-admin:$CA" "$CON/connector-plugins"
```

N'importe quel worker répond : les requêtes sont transmises au leader du groupe. Une pause conserve la position de lecture. Supprimer un connecteur PostgreSQL ne supprime pas son slot côté base : prévenez les DBA.

## Supervision

```bash
/usr/local/bin/promtool check config /etc/prometheus/prometheus.yml && systemctl reload prometheus
/usr/local/bin/amtool check-config /etc/alertmanager/alertmanager.yml && systemctl reload alertmanager
sudo -u grafana grafana cli --homepath /usr/share/grafana --config /etc/grafana/grafana.ini \
  admin reset-admin-password <nouveau> cfg:default.paths.data=/inte/vq/DAT/data/AKH/grafana
```

Validez toujours une configuration avant de recharger le service. La dernière commande réinitialise le mot de passe admin de Grafana.

Les règles d'alerte sont dans `/etc/prometheus/rules/kafka-cdc.yml`.

Pour ajouter un compte web à Prometheus ou Alertmanager, générez l'empreinte avec `htpasswd -nBC 10 <compte>`, puis ajoutez la ligne dans le `web.yml` concerné. Le fichier est relu automatiquement.

## Diagnostic rapide

| Symptôme | Où regarder |
| --- | --- |
| Un service ne démarre pas | `journalctl -u <service> -n 50`, puis le fichier de log du service |
| Refus inexpliqué d'accès à un fichier | `ausearch -m avc -ts recent \| grep denied` (SELinux) |
| `TopicAuthorizationException` ou `GroupAuthorizationException` | ACLs du compte : `kafka-acls.sh --list --principal User:<compte>` |
| `SaslAuthenticationException` | mot de passe ou compte SCRAM : `kafka-configs.sh --describe --entity-type users` |
| Connecteur en `FAILED` | `.../connectors/<nom>/status` (champ `trace`), puis `/var/log/kafka/connect.log` |
| Retard Debezium qui grandit | Grafana ou Prometheus : `debezium_metrics_millisecondsbehindsource`, puis la charge sur la base |
| Lag d'un consommateur | AKHQ (**Consumer Groups**) ou `kafka-consumer-groups.sh --describe` |
| Cible Prometheus en `down` | Prometheus, **Status > Target health**, colonne **Error** |
