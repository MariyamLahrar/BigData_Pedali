\# 🚲 Pedali — Plateforme Big Data pour l'analyse des trajets à vélo



\## 📌 Présentation du projet



\*\*Pedali\*\* est un projet académique réalisé dans le cadre du module \*\*Big Data 2\*\*.



Il consiste à concevoir et mettre en place une architecture Big Data permettant de collecter, traiter, stocker et analyser des données de trajets à vélo.



Le projet combine des traitements batch et streaming afin d'explorer les données historiques, de traiter les événements en temps réel et de produire des indicateurs analytiques.



\## 🎯 Objectifs



\- Mettre en place une infrastructure Big Data avec Docker.

\- Collecter des événements de trajets avec Apache Kafka.

\- Traiter les données distribuées avec Apache Spark.

\- Exploiter les API Spark RDD, Spark SQL et Structured Streaming.

\- Construire un Data Lake organisé en couches Bronze, Silver et Gold.

\- Réaliser des analyses temporelles et des agrégations.

\- Analyser les relations entre stations sous forme de graphes.

\- Produire des résultats et des visualisations.



\## 🛠️ Technologies utilisées



| Technologie | Utilisation |

|---|---|

| Python | Développement des traitements et scripts |

| Apache Spark 3.5.5 | Traitements distribués |

| PySpark | RDD, DataFrames, SQL et streaming |

| Apache Kafka 3.9.1 | Production et consommation d'événements |

| MinIO | Stockage objet compatible S3 |

| Delta Lake | Tables analytiques et historique des versions |

| Docker Compose | Orchestration des services |

| Jupyter Notebook | Exploration et analyse |

| GraphFrames | Analyse des graphes |

| GraphX / Scala | Traitements de graphes distribués |



🏗️ Architecture du projet



Le pipeline de traitement suit les étapes suivantes :



\*\*Sources de données → Kafka → Spark → MinIO → Analyses et résultats\*\*



Le Data Lake est organisé en trois couches :



\- \*\*Bronze :\*\* conservation des données brutes.

\- \*\*Silver :\*\* nettoyage, enrichissement et déduplication.

\- \*\*Gold :\*\* production d'indicateurs et de résultats analytiques.



&#x20;📂 Travaux réalisés



\### Partie 1 — Infrastructure Big Data



Mise en place des services Docker : Spark Master, Spark Workers, Kafka, MinIO et Jupyter.



\### Partie 2 — Traitement avec Spark RDD



Chargement et analyse des données historiques de trajets à vélo à l'aide de transformations et d'actions RDD.



\### Partie 3 — Analyse avec Spark SQL



Manipulation des DataFrames, exécution de requêtes analytiques et stockage des résultats au format Parquet.



\### Partie 4 — Streaming avec Kafka et Spark



Production d'événements Kafka et traitement des flux de trajets avec Spark Structured Streaming.



\### Partie 5 — Data Lake et analyses temporelles



Implémentation du pipeline Bronze/Silver/Gold, nettoyage des données, déduplication, watermark, fenêtres temporelles et tables Delta Lake.



\### Partie 6 — Analyse de graphes



Modélisation du réseau de stations, analyse des connexions avec GraphFrames et GraphX et visualisation des résultats.



&#x20;📁 Organisation du dépôt



```text

Pedali-BigData2/

├── data/                 # Données et ressources

├── notebooks/            # Notebooks Jupyter

├── scripts/              # Scripts Python et Scala

├── resultats/            # Résultats et visualisations

├── docker-compose.yml    # Configuration des services

├── .gitignore            # Exclusions Git

└── README.md             # Documentation du projet

```



&#x20;🚀 Démarrage



&#x20;  Prérequis



\- Docker Desktop

\- Docker Compose

\- Git

\- Un terminal PowerShell ou équivalent



&#x20;  Lancer l'infrastructure



Depuis la racine du projet :



```bash

docker compose up -d

```



\### Vérifier les services



```bash

docker compose ps

```



\### Arrêter les services



```bash

docker compose stop

```



Les traitements Spark et les producteurs Kafka sont disponibles dans le dossier `scripts/`.



\*\*Remarque :\*\* certaines configurations et données locales peuvent être nécessaires pour reproduire l'ensemble des traitements. Les identifiants doivent être configurés localement et ne doivent pas être publiés dans le dépôt.



\## 📊 Résultats



Le projet permet notamment :



\- L'analyse distribuée des trajets historiques.

\- Le traitement d'événements de trajets en streaming.

\- La structuration des données dans un Data Lake.

\- Le calcul d'indicateurs par ville et par période.

\- L'analyse des connexions entre stations.

\- La génération de visualisations analytiques.



👩‍💻 Réalisation



\*\*Projet académique — Big Data 2\*\*



Réalisé par :

\- Mariyam Lahrar

\- Youssra Akaili



&#x20;📚 Contexte académique



École Marocaine des Sciences de l'Ingénieur (EMSI)



Année universitaire : \*\*2026–2027\*\*

