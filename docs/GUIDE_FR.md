# Comprendre et relancer le projet, étape par étape

## 1. La question métier

L'analyste veut repérer les mouvements inhabituels des matières premières et vérifier si le pétrole apporte de l'information pour prévoir l'inflation. Une alerte décrit un mouvement de prix : elle n'en prouve ni la cause ni l'effet sur l'inflation.

## 2. Le parcours d'une observation

1. `sources.py` télécharge les fichiers officiels. Chaque fichier conserve son URL, sa date de téléchargement et son SHA256 dans un manifeste.
2. `contracts.py` transforme chaque ligne en événement : instrument, unité, type de prix, fréquence, date économique, date d'acquisition, révision et identité.
3. `producer.py` publie l'événement dans Kafka. La clé est l'instrument. Kafka conserve les événements pour permettre leur rejeu.
4. `streaming.py` lance deux requêtes Spark. La première conserve toutes les livraisons dans Delta et alimente PostgreSQL. La seconde calcule des statistiques par journée économique et applique un watermark de sept jours.
5. `store.py` distingue un message reçu d'une observation unique. Recevoir le même événement deux fois ne crée pas deux prix. Une même identité contenant deux valeurs différentes est rejetée.
6. `signals.py` calcule les variations et les scores avec les observations antérieures seulement.
7. `api.py` sert les résultats ; React présente l'historique, les alertes et la provenance.

L'identité économique n'est pas l'offset Kafka. Un nouvel offset peut contenir un doublon. Une révision doit avoir une nouvelle identité. Les modes REPLAY, LIVE et SYNTHETIC restent séparés.

## 3. Installation sous Windows

Ouvrir PowerShell dans le dossier du projet. Installer Docker Desktop et activer les conteneurs Linux, Python 3.11, Node 22+ et uv. Prévoir environ 6 Go de RAM disponibles pour les services et de l'espace pour les images Java/Spark.

```powershell
$env:UV_CACHE_DIR = (Join-Path (Get-Location) '.cache\uv')
uv sync --frozen
cd frontend
npm ci
npm run build
cd ..
docker compose up -d --build
.venv\Scripts\python.exe -m observatory.cli init
.venv\Scripts\python.exe -m observatory.cli fetch --start 2000-01-01
.venv\Scripts\python.exe -m observatory.cli replay
```

Le premier téléchargement des images prend plusieurs minutes. Le rejeu est asynchrone : attendre la consommation Spark. Ouvrir http://127.0.0.1:8080 et consulter http://127.0.0.1:8080/ops pour suivre l'ingestion. Le bouton de rafraîchissement actualise les données affichées.

Pour le développement de l'interface : `cd frontend`, puis `npm run dev`. Vite utilise le port 8082 et transmet les requêtes API vers 8080. Après une modification, reconstruire l'interface puis l'image API pour le rendu servi par Docker.

## 4. Lire le poste de travail

- **Market overview** : choisir un instrument, une période ; survoler le graphique pour lire une date. Les points cerclés indiquent les observations signalées.
- **Shock history** : filtrer l'instrument et la direction, exporter la sélection CSV.
- **Data sources** : vérifier la source, la fréquence, les unités et le sens des trois modes.
- **Research notebook** : consulter la comparaison macro, avec son niveau de preuve explicite.
- **Engineering console** : inspecter les rejets, les lots, le watermark, les tests de reprise et la charge. Ce n'est pas un indicateur de risque économique.

## 5. Comprendre le score

On calcule la différence de prix entre deux observations, puis on la compare aux différences précédentes. La médiane et la MAD limitent l'effet d'une ancienne valeur extrême. Le seuil ±4 sert à sélectionner des mouvements à examiner. Un score élevé peut provenir d'une donnée erronée : toujours lire la source.

Les fréquences ne sont pas interchangeables. Un score mensuel de l'or utilise un historique mensuel. Le WTI peut être négatif : utiliser automatiquement `log(prix_t / prix_t-1)` serait incorrect. Les variations en pourcentage autour de zéro demandent aussi une interprétation prudente.

## 6. Lancer les expériences

```powershell
.venv\Scripts\python.exe -m observatory.cli evaluate-detectors
.venv\Scripts\python.exe -m observatory.cli macro --origins 96
.venv\Scripts\python.exe -m observatory.cli verify
.venv\Scripts\python.exe -m observatory.cli reliability
.venv\Scripts\python.exe -m observatory.cli load-test --events 5000
.venv\Scripts\python.exe -m pytest
```

`reliability` arrête brutalement puis redémarre uniquement le service Spark de CE projet. Les tests utilisent des données artificielles isolées. Les rapports sont écrits dans `artifacts/`. `verify` s'exécute lorsque le rejeu est terminé et compare les valeurs du fichier à celles servies après Kafka/Spark.

Le backtest macro par défaut utilise les données révisées actuelles et des délais de publication supposés. Il reste exploratoire. Avec une clé personnelle FRED, définir `$env:FRED_API_KEY`, puis lancer `macro --strict`. La clé ne doit jamais être commise. Lire `EVALUATION.md` avant de commenter une performance.

## 7. Activer un flux de marché réel

Le rejeu n'est pas un flux de cotations temps réel. Après vérification de l'abonnement et des droits de diffusion du fournisseur :

```powershell
$env:TWELVE_DATA_API_KEY = '<votre-cle>'
$env:MARKET_DATA_LICENSE_ACK = 'true'
.venv\Scripts\python.exe -m observatory.cli live --symbols XAU/USD
```

Le flux live utilise des instruments distincts des historiques de référence. L'adaptateur est fourni mais son fonctionnement dépend des droits du compte ; aucune clé ni validation d'un abonnement réel n'est livrée.

## 8. Arrêter et reprendre

`docker compose stop` conserve les données. `docker compose up -d` reprend avec les checkpoints. Ne pas supprimer les volumes pour résoudre une erreur sans en comprendre les conséquences. Les autres projets Docker ne sont pas concernés. Kafka local est un seul broker ; les accès sont limités à localhost. Ce déploiement ne doit pas être exposé sur Internet.

## 9. Ce que tu dois pouvoir expliquer en entretien

Pourquoi une file d'événements plutôt qu'un simple script ? Comment distinguer doublon, correction et retard ? Pourquoi le watermark peut-il écarter un événement des statistiques sans l'effacer de l'audit ? Pourquoi un backtest avec des données révisées n'est-il pas une preuve de prévision en temps réel ? Pourquoi garder une référence simple même avec XGBoost ?

Les limites actuelles sont concrètes : un seul broker, Spark local, signaux pandas recalculés sur un historique borné, absence de SSO/TLS. Les tests produisent des preuves locales, pas une certification de disponibilité bancaire.
