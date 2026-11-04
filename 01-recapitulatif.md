# Stack CDC Debezium / Kafka – Récapitulatif de l'installation

*Mis à jour le 4 novembre 2026*

La plateforme de capture de modifications (CDC) qui remplace Oracle GoldenGate est installée et supervisée sur 6 serveurs RHEL 8.10. Seuls les connecteurs Debezium restent à déployer, après le paramétrage des bases par les DBA.

Ce dossier comprend 5 documents :

1. `01-recapitulatif.md` : ce récapitulatif
2. `02-architecture.md` : l'architecture
3. `03-ameliorations.md` : les améliorations futures
4. `04-guide-utilisation.md` : le guide d'utilisation et d'administration
5. `05-questionnaire-dba.md` : les informations à obtenir des DBA

Aucun mot de passe ne figure dans ces documents. Ils sont dans le coffre, et seuls les fichiers qui les contiennent sont cités.

## Avancement

| Étape | Contenu | Machines | État |
| --- | --- | --- | --- |
| 0 | Recueil des besoins, choix techniques | toutes | Fait |
| 1 | Socle OS : comptes, Java 21, sysctl, SELinux, binaires Kafka | toutes | Fait |
| 2 | Cluster Kafka KRaft 3 nœuds | mcq-dbk70/71/72 | Fait |
| 3 | Comptes SCRAM, ACLs, topics internes Connect | toolbox → cluster | Fait |
| 4 | Kafka Connect distribué + plugins Debezium | mcq-dbz70/71 | Fait |
| 5 | Prérequis bases PostgreSQL / Oracle | DBA | En attente des DBA |
| 6 | Déploiement des connecteurs Debezium | dbz + toolbox | À faire |
| 7 | Temurin 25 + AKHQ 0.28.0 | mcq-dbt70 | Fait |
| 8 | Monitoring : exporters, Prometheus, Alertmanager, Grafana | toutes | Fait, 27 cibles up |
| 9 | Tests de résilience, tableaux de bord, runbook | toutes | À faire |

## Étape 0 – Décisions prises

- Kafka **3.9.1** (Scala 2.13) en KRaft, chaque nœud étant à la fois broker et controller, sous **Java 21** (RPM `java-21-openjdk-headless`, version 21.0.12.1).
- Sécurité : **SASL_PLAINTEXT + SCRAM-SHA-512** pour les clients et la réplication, **PLAIN** sur le listener controller, ACLs avec refus par défaut. Pas de PKI ni de TLS pour l'instant.
- Comptes de service dédiés. SELinux reste en `enforcing`. Pas de pare-feu local. Interfaces d'admin ouvertes à tous les réseaux, mais toutes authentifiées.
- Debezium 3.1.3.Final, avec Oracle en **LogMiner** par défaut. XStream exigerait la licence GoldenGate et l'Instant Client, absents du bundle.
- Messages en JSON sans schéma embarqué : le bundle ne contient pas de Schema Registry.
- Installation hors ligne : le bundle est copié dans `/opt/bundle/artifacts` sur chaque machine.

## Étape 1 – Socle OS (6 machines)

```bash
dnf install -y java-21-openjdk-headless policycoreutils-python-utils
```

Java 21 pour Kafka et Connect, et `semanage` pour gérer les contextes SELinux.

```bash
useradd -r -m -d /var/lib/kafka -s /sbin/nologin kafka
mkdir -p /etc/kafka /var/log/kafka /inte/vq/DAT/data/KFK/data
chown -R kafka:kafka /etc/kafka /var/log/kafka /inte/vq/DAT/data/KFK
```

Compte de service `kafka` sans shell, et répertoires de configuration, de logs et de données (brokers et workers Connect).

```bash
cat > /etc/sysctl.d/90-kafka.conf   # vm.swappiness=1, vm.max_map_count=262144, dirty ratios, somaxconn
sysctl --system
```

Réglages noyau des brokers : pas de swap, beaucoup de fichiers mappés, écritures disque lissées.

```bash
semanage fcontext -a -t var_lib_t '/inte/vq/DAT/data/KFK(/.*)?'
restorecon -Rv /inte/vq/DAT/data/KFK
```

`/inte` n'est pas un chemin standard : on lui donne le type SELinux des données applicatives.

```bash
mkdir -p /opt/kafka
tar -xzf /opt/bundle/artifacts/kafka/kafka_2.13-3.9.1.tgz -C /opt/kafka
ln -sfn /opt/kafka/kafka_2.13-3.9.1 /opt/kafka/current
semanage fcontext -a -t bin_t '/opt/kafka/kafka_2.13-3.9.1/bin(/.*)?'
restorecon -Rv /opt/kafka/kafka_2.13-3.9.1
chmod 755 /opt/kafka && chmod -R u=rwX,go=rX /opt/kafka/kafka_2.13-3.9.1
```

Binaires Kafka sur les brokers, les workers et la toolbox (pour les commandes d'admin). Le `chmod` corrige le `umask` restrictif des serveurs, qui empêchait le compte `kafka` d'exécuter les scripts.

## Étape 2 – Cluster KRaft (mcq-dbk70/71/72)

```bash
kafka-storage.sh random-uuid      # une seule fois : ID du cluster
openssl rand -base64 24           # mot de passe du compte admin
```

L'ID du cluster est commun aux 3 nœuds. Le mot de passe admin est rangé dans le coffre.

Contenu de `/etc/kafka/server.properties` (640 root:kafka) :
- `process.roles=broker,controller` et `node.id` 1, 2 ou 3 ;
- votants KRaft statiques sur le port 9093 ;
- listener `BROKER` sur 9092 en SCRAM-SHA-512, listener `CONTROLLER` sur 9093 en PLAIN ;
- `StandardAuthorizer` avec `super.users=User:admin` ;
- réplication 3 et `min.insync.replicas=2` ;
- création automatique de topics désactivée, rétention 7 jours.

```bash
sudo -u kafka kafka-storage.sh format -t <CLUSTER_ID> -c /etc/kafka/server.properties \
  --add-scram "SCRAM-SHA-512=[name=admin,password=...]"
```

Formate le stockage et crée le compte SCRAM `admin` dès le départ. Sans lui, les brokers ne pourraient pas s'authentifier entre eux.

```bash
systemctl enable --now kafka      # unit /etc/systemd/system/kafka.service, heap 6 Go
```

Service systemd sous le compte `kafka`, avec `LimitNOFILE=200000`.

```bash
kafka-metadata-quorum.sh --bootstrap-server ... --command-config /root/kafka-admin.properties describe --status
```

Vérifications faites depuis la toolbox : quorum `[1,2,3]` avec un leader, topic de test répliqué sur 3 brokers, client sans authentification refusé.

## Étape 3 – Comptes et ACLs (depuis mcq-dbt70)

```bash
kafka-configs.sh $ADM --alter --entity-type users --entity-name <compte> \
  --add-config "SCRAM-SHA-512=[iterations=8192,password=...]"
```

Création des comptes `connect`, `akhq` et `kexporter`.

```bash
kafka-topics.sh $ADM --create --topic connect-cdc-configs --partitions 1  --replication-factor 3 --config cleanup.policy=compact
kafka-topics.sh $ADM --create --topic connect-cdc-offsets --partitions 25 --replication-factor 3 --config cleanup.policy=compact
kafka-topics.sh $ADM --create --topic connect-cdc-status  --partitions 5  --replication-factor 3 --config cleanup.policy=compact
```

Topics internes de Kafka Connect, créés à la main pour garantir la réplication et la compaction.

```bash
kafka-acls.sh $ADM --add --allow-principal User:connect --operation Read --operation Write \
  --operation Create --operation Describe --operation DescribeConfigs \
  --topic <prefixe> --resource-pattern-type prefixed
```

Droits accordés :
- **`connect`** : droits préfixés sur les topics `connect-cdc-`, `cdc.` et `__debezium-heartbeat`, et sur les groupes `connect-cdc` et `cdc.`.
- **`akhq`** : lecture sur tout, puis droits d'administration. Des ACLs **DENY** protègent les topics internes, l'historique de schéma et les flux `cdc.*`.
- **`kexporter`** : description seule.

Tests validés : création autorisée sous `cdc.`, refusée hors préfixe, visibilité limitée aux topics autorisés.

## Étape 4 – Kafka Connect (mcq-dbz70/71)

```bash
cd /opt/kafka/plugins
tar -xzf .../debezium-connector-postgres-3.1.3.Final-plugin.tar.gz
tar -xzf .../debezium-connector-oracle-3.1.3.Final-plugin.tar.gz
cp .../{ojdbc11,xdb,xmlparserv2}-21.15.0.0.jar debezium-connector-oracle/
```

Installation des plugins Debezium. Les pilotes Oracle sont ajoutés au plugin Oracle, qui ne les fournit pas (licence).

Contenu de `/etc/kafka/connect-distributed.properties` (640 root:kafka) :
- groupe `connect-cdc` ;
- authentification SCRAM, répétée pour les clients `producer.`, `consumer.` et `admin.` ;
- convertisseurs JSON sans schéma ;
- `topic.creation.enable=true` ;
- API REST sur 8083, avec l'extension d'authentification Basic.

Les comptes REST `connect-admin` et `akhq-connect` sont déclarés dans `/etc/kafka/connect-rest-jaas.conf` et `/etc/kafka/connect-rest-users.properties`.

```bash
systemctl enable --now kafka-connect   # heap 8 Go
```

Vérifications : 401 sans identifiants, version 3.9.1 avec identifiants, plugins PostgreSQL et Oracle chargés, 2 membres dans le groupe `connect-cdc`.

## Étape 7 – AKHQ (mcq-dbt70)

```bash
tar -xzf .../OpenJDK25U-jre_x64_linux_hotspot_25.0.4.1_1.tar.gz -C /opt/java
ln -sfn /opt/java/jdk-25.0.4.1+1-jre /opt/java/temurin-25
semanage fcontext -a -t bin_t '/opt/java/jdk-25.0.4.1\+1-jre/bin(/.*)?'
```

Java 25 dédié à AKHQ, sans toucher au Java du système.

```bash
useradd -r -M -d /opt/akhq -s /sbin/nologin akhq
cp .../akhq-0.28.0-all.jar /opt/akhq/ && ln -sfn /opt/akhq/akhq-0.28.0-all.jar /opt/akhq/akhq.jar
```

Contenu de `/etc/akhq/application.yml` (640 root:akhq) :
- connexion SCRAM au cluster et lien vers Connect ;
- rôles `admin` et `reader` ;
- utilisateurs web `admin` et `lecteur`, mots de passe hachés en SHA-256 ;
- métriques sur le port 28081.

```bash
systemctl enable --now akhq
```

## Étape 8 – Monitoring

- **node_exporter** sur les 6 machines : binaire dans `/usr/local/bin`, compte `node_exporter`, port 9100, collecteur systemd.
- **Agent JMX exporter** 1.6.0, ajouté par drop-in systemd :
  - `kafka.service.d/jmx-exporter.conf` (port 7071) ;
  - `kafka-connect.service.d/jmx-exporter.conf` (port 7072, avec l'option JAAS de l'API REST).

  Les brokers ont été redémarrés un par un, sans aucune partition sous-répliquée entre deux redémarrages.
- **Sur la toolbox** :
  - binaires `prometheus`, `promtool`, `alertmanager`, `amtool`, `kafka_exporter` et `blackbox_exporter` dans `/usr/local/bin` ;
  - comptes `prometheus`, `alertmanager` et `exporter` ;
  - données dans `/inte/vq/DAT/data/AKH` (contexte `var_lib_t`).
- **kafka_exporter** et **blackbox_exporter** écoutent uniquement sur 127.0.0.1.
- **Alertmanager** et **Prometheus** sont protégés par un fichier `web.yml` (empreintes bcrypt générées par `htpasswd`, paquet `httpd-tools`). Prometheus garde 30 jours de données, avec un plafond de 20 Go, et charge 16 règles d'alerte.
- **Grafana** 13.2.0, installé par RPM :
  - données déplacées sur le FS AKH ;
  - appels Internet et préinstallation de plugins désactivés ;
  - source de données Prometheus déclarée par fichier.

## Incidents rencontrés et corrections

| Symptôme | Cause | Correction |
| --- | --- | --- |
| `permission denied` sur `kafka-storage.sh` | `umask` 027/077 : `/opt/kafka` créé en 750 | `chmod 755` + `go=rX` sur l'arborescence |
| Kafka `exit 1` au démarrage | Cause non consignée (à compléter) | Cluster opérationnel ensuite |
| `amtool` introuvable | `sudo` exclut `/usr/local/bin` du `PATH` | Appel par chemin complet |
| Grafana : `failed to get plugin` | Téléchargement de plugins préinstallés, impossible hors ligne | `GF_PLUGINS_PREINSTALL_DISABLED=true` |
| Cible Prometheus `9090/metrics` en 401 | Le mot de passe en clair ne correspondait pas à l'empreinte de `web.yml` | Fichier `.pw` et empreinte réalignés |
