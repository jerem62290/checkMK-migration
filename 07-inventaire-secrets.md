# Inventaire des mots de passe et procédures de changement

*Mis à jour le 4 novembre 2026*

Ce document liste chaque mot de passe ou secret de la plateforme : son compte, son rôle, les machines où il sert, les fichiers qui le contiennent et la procédure pour le changer. **Il ne contient aucune valeur** : elles sont dans le coffre.

Au total : 15 mots de passe côté plateforme, 1 clé de session (AKHQ), 2 comptes Oracle gérés par les DBA, et 1 compte par application consommatrice.

## Règles générales

- Générez les mots de passe avec `openssl rand -base64 24`. Évitez les caractères `#`, `&`, `\`, `'` et `"` : ils cassent les commandes `sed` et les lignes JAAS.
- Mettez à jour le coffre **avant** de changer le mot de passe sur les serveurs.
- Après chaque changement, vérifiez que toutes les cibles sont `up` dans Prometheus (**Status > Target health**) et qu'aucun connecteur n'est en `FAILED`.

Variables utilisées dans les commandes, à définir sur la toolbox mcq-dbt70 :

```bash
export JAVA_HOME=/usr/lib/jvm/jre-21-openjdk K=/opt/kafka/current/bin BS=mcq-dbk70.cgi.credit-cgi.fr:9092
ADM="--bootstrap-server $BS --command-config /root/kafka-admin.properties"
```

---

## Inventaire

### Comptes Kafka (SCRAM-SHA-512)

| # | Compte | Rôle | Machines | Fichiers qui le contiennent |
| --- | --- | --- | --- | --- |
| 1 | `admin` | Super-utilisateur Kafka : réplication entre brokers, quorum KRaft (PLAIN sur le port 9093), administration | dbk70/71/72, dbt70 | dbk70-72 : `/etc/kafka/server.properties` (2 lignes JAAS). dbt70 : `/root/kafka-admin.properties` |
| 2 | `connect` | Workers Connect et Debezium : écriture dans les topics `cdc.*` | dbz70/71 | dbz70/71 : `/etc/kafka/connect-distributed.properties` (4 lignes) et `/etc/kafka/secrets/kafka-connect.properties` |
| 3 | `akhq` | Accès d'AKHQ au cluster | dbt70 | `/etc/akhq/application.yml` |
| 4 | `kexporter` | kafka_exporter (lag des consommateurs) | dbt70 | `/etc/sysconfig/kafka_exporter` |
| 5 | `app-<nom>` | Un compte par application consommatrice | serveurs des applications | configuration de chaque application |

### API REST de Kafka Connect (authentification Basic)

| # | Compte | Rôle | Machines | Fichiers qui le contiennent |
| --- | --- | --- | --- | --- |
| 6 | `connect-admin` | Administration des connecteurs par `curl` | appels depuis dbt70 vers dbz70/71:8083 | dbz70/71 : `/etc/kafka/connect-rest-users.properties` |
| 7 | `akhq-connect` | Accès d'AKHQ à l'API REST | dbt70 → dbz70/71:8083 | dbz70/71 : `/etc/kafka/connect-rest-users.properties`. dbt70 : `/etc/akhq/application.yml` |

### Interfaces web de la toolbox (dbt70)

| # | Compte | Rôle | URL | Fichiers qui le contiennent |
| --- | --- | --- | --- | --- |
| 8 | `admin` (AKHQ) | Administration dans AKHQ | :8080 | empreinte SHA-256 dans `/etc/akhq/application.yml` |
| 9 | `lecteur` (AKHQ) | Consultation seule dans AKHQ | :8080 | empreinte SHA-256 dans `/etc/akhq/application.yml` |
| 10 | `admin` (Prometheus) | Interface web Prometheus, et collecte de ses propres métriques | :9090 | empreinte bcrypt dans `/etc/prometheus/web.yml` ; en clair dans `/etc/prometheus/secrets/prometheus-admin.pw` |
| 11 | `grafana` (Prometheus) | Source de données de Grafana | Grafana → :9090 | empreinte bcrypt dans `/etc/prometheus/web.yml` ; en clair dans `/etc/grafana/provisioning/datasources/prometheus.yml` |
| 12 | `admin` (Alertmanager) | Interface web Alertmanager (silences) | :9093 | empreinte bcrypt dans `/etc/alertmanager/web.yml` |
| 13 | `prometheus` (Alertmanager) | Envoi des alertes de Prometheus vers Alertmanager | Prometheus → :9093 | empreinte bcrypt dans `/etc/alertmanager/web.yml` ; en clair dans `/etc/prometheus/secrets/alertmanager.pw` |
| 14 | `admin` (Grafana) | Administration de Grafana | :3000 | base `/inte/vq/DAT/data/AKH/grafana/grafana.db` (haché) |
| 15 | *(clé de session)* | Signature des sessions web d'AKHQ (JWT) | dbt70 | `/etc/akhq/application.yml` |

### Comptes Oracle (gérés par les DBA)

| # | Compte | Rôle | Machines | Fichiers qui le contiennent |
| --- | --- | --- | --- | --- |
| 16 | `c##dbzuser` | Connexion de Debezium au serveur sortant XStream | dbz70/71 → base Oracle | dbz70/71 : `/etc/kafka/secrets/<flux>.properties`, par exemple `ora-fact.properties` |
| 17 | `c##dbzadmin` | Administration XStream (serveurs sortants) | DBA uniquement | aucun fichier côté Kafka |

Le compte Debezium PostgreSQL s'ajoutera à cette liste au premier flux PostgreSQL.

---

## Procédures de changement

### #1 – `admin` (Kafka)

C'est le changement le plus délicat. Ce mot de passe sert à la fois à l'authentification entre brokers (SCRAM) et au quorum KRaft (PLAIN). Pendant un redémarrage progressif, un nœud qui a le nouveau mot de passe ne peut pas s'authentifier auprès d'un nœud qui a encore l'ancien. Deux méthodes :

**Méthode A – fenêtre de maintenance (simple, coupure de quelques minutes)**

1. Mettez les connecteurs en pause et prévenez les consommateurs. Posez un silence dans Alertmanager.
2. Depuis la toolbox, changez le compte SCRAM :
   ```bash
   read -rsp 'Nouveau PW admin : ' PW; echo; $K/kafka-configs.sh $ADM --alter --entity-type users --entity-name admin --add-config "SCRAM-SHA-512=[iterations=8192,password=$PW]"
   ```
3. Sur **les 3 brokers**, remplacez le mot de passe dans les deux lignes JAAS de `/etc/kafka/server.properties` (`password="..."` et `user_admin="..."`) :
   ```bash
   read -rsp 'Nouveau PW admin : ' PW; echo; sed -i "s#password=\"[^\"]*\"#password=\"$PW\"#g; s#user_admin=\"[^\"]*\"#user_admin=\"$PW\"#" /etc/kafka/server.properties; unset PW
   ```
4. Arrêtez les 3 brokers, puis redémarrez-les à quelques secondes d'intervalle :
   ```bash
   systemctl restart kafka
   ```
5. Sur la toolbox, mettez à jour `/root/kafka-admin.properties` (ligne `sasl.jaas.config`), puis vérifiez le quorum :
   ```bash
   $K/kafka-metadata-quorum.sh $ADM describe --status
   ```
6. Relancez les connecteurs et retirez le silence.

**Méthode B – sans coupure, par un compte de transition**

Le principe : ajouter un second super-utilisateur, par exemple `kraft`, faire basculer les brokers sur ce compte un par un, puis retirer l'ancien. Il faut trois redémarrages progressifs :

1. Créer le compte SCRAM `kraft` et l'ajouter à `super.users`, ainsi que `user_kraft` dans la ligne PLAIN, sur les 3 brokers ;
2. Remplacer `username="admin"` par `kraft` dans les deux lignes JAAS ;
3. Retirer `admin`.

À préparer ensemble avant une première exécution.

### #2 – `connect` (Kafka)

Coupure de quelques secondes par worker. Les connecteurs reprennent à leur dernière position.

1. Sur **dbz70 et dbz71**, mettez à jour les fichiers **sans redémarrer** :
   ```bash
   read -rsp 'Nouveau PW connect : ' PW; echo; sed -i "s#password=\"[^\"]*\"#password=\"$PW\"#g" /etc/kafka/connect-distributed.properties; printf 'sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="connect" password="%s";\n' "$PW" > /etc/kafka/secrets/kafka-connect.properties; unset PW
   ```
2. Depuis la toolbox, changez le compte SCRAM :
   ```bash
   read -rsp 'Nouveau PW connect : ' PW; echo; $K/kafka-configs.sh $ADM --alter --entity-type users --entity-name connect --add-config "SCRAM-SHA-512=[iterations=8192,password=$PW]"; unset PW
   ```
3. Redémarrez **aussitôt** les workers, l'un après l'autre :
   ```bash
   systemctl restart kafka-connect
   ```
4. Vérifiez que tous les connecteurs sont en `RUNNING`.

### #3 – `akhq` (Kafka)

1. Changez le compte SCRAM `akhq`, comme au #2 étape 2, en remplaçant `connect` par `akhq`.
2. Sur dbt70, remplacez le mot de passe dans la ligne `sasl.jaas.config` de `/etc/akhq/application.yml`, puis redémarrez AKHQ :
   ```bash
   systemctl restart akhq
   ```

### #4 – `kexporter` (Kafka)

1. Changez le compte SCRAM `kexporter`, comme au #2 étape 2.
2. Sur dbt70 :
   ```bash
   read -rsp 'Nouveau PW kexporter : ' P; echo; printf 'KEXP_PW=%s\n' "$P" > /etc/sysconfig/kafka_exporter; unset P; systemctl restart kafka_exporter
   ```

### #5 – `app-<nom>` (Kafka)

1. Prévenez l'équipe de l'application.
2. Changez le compte SCRAM, comme au #2 étape 2.
3. L'équipe met à jour sa configuration et redémarre son application.

### #6 – `connect-admin` (API REST Connect)

Le fichier des comptes REST n'est lu qu'au démarrage du worker.

1. Sur dbz70 et dbz71, modifiez la ligne `connect-admin: ...` de `/etc/kafka/connect-rest-users.properties`.
2. Redémarrez les workers, l'un après l'autre :
   ```bash
   systemctl restart kafka-connect
   ```

### #7 – `akhq-connect` (API REST Connect)

1. Sur dbz70 et dbz71, modifiez la ligne `akhq-connect: ...` de `/etc/kafka/connect-rest-users.properties`, puis redémarrez les workers l'un après l'autre.
2. Sur dbt70, remplacez `basic-auth-password` dans `/etc/akhq/application.yml`, puis redémarrez AKHQ :
   ```bash
   systemctl restart akhq
   ```

### #8 et #9 – `admin` et `lecteur` (AKHQ)

1. Calculez l'empreinte du nouveau mot de passe :
   ```bash
   read -rsp 'Nouveau PW : ' P; echo; echo -n "$P" | sha256sum | cut -d' ' -f1; unset P
   ```
2. Remplacez l'empreinte de l'utilisateur concerné dans la section `basic-auth` de `/etc/akhq/application.yml`.
3. Redémarrez AKHQ :
   ```bash
   systemctl restart akhq
   ```

### #10 – `admin` (Prometheus)

Prometheus relit ces fichiers à chaque requête : aucun redémarrage n'est nécessaire. `htpasswd` demande le mot de passe deux fois.

```bash
H=$(htpasswd -nBC 10 admin | cut -d: -f2) && sed -i "s#^  admin: .*#  admin: $H#" /etc/prometheus/web.yml && unset H && /usr/local/bin/promtool check web-config /etc/prometheus/web.yml
```

```bash
read -rsp 'Même PW : ' P; echo; printf '%s' "$P" > /etc/prometheus/secrets/prometheus-admin.pw; unset P
```

### #11 – `grafana` (Prometheus)

1. Mettez à jour l'empreinte dans `/etc/prometheus/web.yml`, comme au #10, en remplaçant `admin` par `grafana`.
2. Remplacez `basicAuthPassword` dans `/etc/grafana/provisioning/datasources/prometheus.yml`, puis redémarrez Grafana, qui ne lit ce fichier qu'au démarrage :
   ```bash
   systemctl restart grafana-server
   ```

### #12 – `admin` (Alertmanager)

Alertmanager relit le fichier à chaque requête : aucun redémarrage n'est nécessaire.

```bash
H=$(htpasswd -nBC 10 admin | cut -d: -f2) && sed -i "s#^  admin: .*#  admin: $H#" /etc/alertmanager/web.yml && unset H
```

### #13 – `prometheus` (Alertmanager)

Les deux fichiers sont relus automatiquement : aucun redémarrage n'est nécessaire.

```bash
H=$(htpasswd -nBC 10 prometheus | cut -d: -f2) && sed -i "s#^  prometheus: .*#  prometheus: $H#" /etc/alertmanager/web.yml && unset H
```

```bash
read -rsp 'Même PW : ' P; echo; printf '%s' "$P" > /etc/prometheus/secrets/alertmanager.pw; unset P
```

Vérifiez ensuite que la cible `alertmanager` est `up` dans Prometheus.

### #14 – `admin` (Grafana)

Depuis l'interface : **Profile > Change password**. En cas d'oubli, réinitialisez-le sur dbt70 :

```bash
sudo -u grafana grafana cli --homepath /usr/share/grafana --config /etc/grafana/grafana.ini admin reset-admin-password <nouveau> cfg:default.paths.data=/inte/vq/DAT/data/AKH/grafana
```

### #15 – Clé de session AKHQ

1. Générez une nouvelle clé :
   ```bash
   openssl rand -hex 32
   ```
2. Remplacez la valeur `secret` (section `jwt`) dans `/etc/akhq/application.yml`, puis redémarrez AKHQ :
   ```bash
   systemctl restart akhq
   ```

Les utilisateurs devront se reconnecter.

### #16 – `c##dbzuser` (Oracle)

1. Sur dbz70 et dbz71, mettez à jour le fichier secret du flux, **avant** que les DBA changent le mot de passe :
   ```bash
   read -rsp 'Nouveau PW c##dbzuser : ' P; echo; printf 'password=%s\n' "$P" > /etc/kafka/secrets/ora-fact.properties; unset P
   ```
2. Les DBA changent le mot de passe :
   ```sql
   ALTER USER c##dbzuser IDENTIFIED BY "<nouveau>" CONTAINER=ALL;
   ```
3. Redémarrez le connecteur, qui relit le secret à son démarrage :
   ```bash
   curl -s -u "connect-admin:$CA" -X POST "http://mcq-dbz70.cgi.credit-cgi.fr:8083/connectors/ora-fact/restart?includeTasks=true"
   ```

Faites-le pour chaque flux qui utilise ce compte.

### #17 – `c##dbzadmin` (Oracle)

Changement réalisé par les DBA seuls, sans impact côté Kafka.

---

## Récapitulatif des impacts

| # | Secret | Redémarrage nécessaire | Coupure |
| --- | --- | --- | --- |
| 1 | `admin` Kafka | 3 brokers | quelques minutes (méthode A) ou aucune (méthode B) |
| 2 | `connect` | 2 workers | quelques secondes par worker |
| 3 | `akhq` | AKHQ | interface AKHQ |
| 4 | `kexporter` | kafka_exporter | métriques de lag |
| 5 | `app-<nom>` | application concernée | application concernée |
| 6, 7 | REST Connect | 2 workers (+ AKHQ pour #7) | quelques secondes par worker |
| 8, 9, 15 | AKHQ | AKHQ | interface AKHQ |
| 10, 12, 13 | Prometheus / Alertmanager | aucun | aucune |
| 11 | `grafana` | Grafana | interface Grafana |
| 14 | `admin` Grafana | aucun | aucune |
| 16 | `c##dbzuser` | connecteur concerné | quelques secondes |
| 17 | `c##dbzadmin` | aucun | aucune |
