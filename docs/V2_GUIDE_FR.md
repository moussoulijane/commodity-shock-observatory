# V2 — comprendre les changements et relancer les preuves

## Le besoin métier

Un analyste suivant une entreprise importatrice veut relier un mouvement de marché à une exposition. Trois profils **fictifs** représentent un importateur d'énergie, un industriel consommant du cuivre et un acheteur de blé. Les quantités mensuelles, bases et couvertures sont des hypothèses de démonstration. Les prix de référence viennent des historiques REPLAY, avec leurs dates et fréquences originales.

Le taux MAD/USD est **fourni comme hypothèse par l'utilisateur**. La valeur proposée de 10 n'est jamais présentée comme une cotation de marché. Un scénario de coût ne constitue ni une prévision d'inflation ni une estimation de pertes de crédit.

## Lire la décomposition des coûts

Pour une quantité q, un benchmark P en USD et un change F en MAD/USD, le coût de référence vaut q × (P + base fixe) × F. Le scénario distingue l'effet matière première, l'effet change et leur interaction. Sans base ni couverture, une hausse de prix de 10 % et une hausse du dollar de 3 % donnent **13,3 %** de hausse du coût, et non 13 %.

Les couvertures sont idéalisées : couverture matière sur une fraction des quantités et couverture change sur une fraction du **notionnel USD de référence**. Les dollars supplémentaires dus à une hausse du benchmark restent exposés au nouveau change. Ainsi, avec P=100, F=10, q=1, aucune couverture matière et une couverture intégrale du notionnel USD initial, +10 % de prix et +10 % de change donnent un coût net de **1 110 MAD**, et non 1 100. Primes, échéances, collatéral, fiscalité et risque de contrepartie ne sont pas modélisés. Une couverture peut augmenter le coût net lorsque les marchés baissent.

Le filtre de date limite la **date économique** des références ; il ne transforme pas les fichiers révisés actuels en historique réellement connu à cette date. L'écran précise cette limite et montre la date d'acquisition du snapshot.

## Ce qui change dans le moteur

1. Kafka conserve les contrats V1 et les trois topics de modes ; leur signification ne change pas.
2. Une requête d'ingestion sans watermark conserve tous les messages dans une Bronze Delta V2 et alimente le registre PostgreSQL par COPY et opérations SQL en lots.
3. La Silver Delta est typée et conserve les révisions. Les tables et checkpoints V1 restent disponibles séparément.
4. Les événements modifiés déterminent des voisinages bornés : 62 observations précédentes et suivantes, fusionnés lorsqu'ils se recouvrent. Une correction ancienne ne recharge pas tout l'historique jusqu'à aujourd'hui.
5. Spark calcule exactement les médianes/MAD et les signaux dans ces contextes, puis les partitions écrivent des UPSERT idempotents. Il calcule aussi des barres canoniques OHLC : minute pour les cotations, jour pour les séries quotidiennes, mois pour les benchmarks mensuels. Ce ne sont pas des volumes négociés.
6. Un verrou d'écriture tenu pendant le traitement et un numéro de génération empêchent un ancien travail de remplacer une correction plus récente. Les périodes à recalculer sont durables, afin de reprendre après une écriture partielle entre systèmes.
7. Les fenêtres de télémétrie ont une requête et un checkpoint par mode. Le flux SYNTHETIC ne peut plus avancer le watermark REPLAY. Une arrivée trop tardive peut encore sortir des statistiques bornées de son propre mode ; l'audit et les barres canoniques restent reconstruisibles.

Le pont Python de validation/transfert reste borné à 2 000 offsets par déclenchement. Ce déploiement utilise un driver local[2] et deux partitions de calcul ; il ne démontre pas une capacité de cluster.

## Démarrage et migration

Les commandes suivantes s'exécutent depuis le dossier du projet, avec Docker Desktop actif :

```powershell
uv sync --frozen
cd frontend
npm ci
npm run build
cd ..
docker compose up -d --build
.venv\Scripts\python.exe -m observatory.cli init
```

Pour un nouveau dossier sans historiques : lancer `fetch`, puis `replay`, comme dans le guide initial. Pour une installation V1, les nouveaux checkpoints lisent les messages encore présents dans Kafka. Laisser l'ingestion finir avant d'évaluer. Si la rétention Kafka a supprimé des messages, republier l'archive complète avec `replay`; ne pas effacer les volumes pour contourner une erreur.

Ouvrir http://127.0.0.1:8080 pour les scénarios et http://127.0.0.1:8080/ops pour les preuves techniques. Les évaluations et diagnostics restent séparés de l'utilisation quotidienne.

## Reproduire les validations

```powershell
.venv\Scripts\python.exe -m pytest -q
$env:CSO_DB_TESTS = '1'
.venv\Scripts\python.exe -m pytest tests/test_incremental_store.py -q
Remove-Item Env:CSO_DB_TESTS
docker compose run --rm --no-deps spark python -m observatory.v2_checks
.venv\Scripts\python.exe -m observatory.cli verify
.venv\Scripts\python.exe -m observatory.cli reliability
.venv\Scripts\python.exe -m observatory.cli commit-recovery
.venv\Scripts\python.exe -m observatory.cli load-test --events 5000
.venv\Scripts\python.exe -m observatory.cli sustained-load --seconds 120 --rate 50
```

Les tests PostgreSQL utilisent un schéma temporaire distinct et le retirent après le test. Les exercices de reprise interrompent **uniquement le service Spark de ce projet**. `commit-recovery` provoque volontairement une erreur après l'écriture des observations, puis vérifie la reconstruction des signaux après redémarrage. Les tests de charge publient exclusivement dans SYNTHETIC. Ne pas lancer plusieurs exercices simultanément.

Les latences V2 sont observées par requêtes SELECT concurrentes à la publication. Elles incluent l'incertitude du polling. La mesure V1 utilisait un autre protocole : ses chiffres ne permettent pas d'annoncer directement un facteur d'accélération.

## Alertes et recherche macro

```powershell
.venv\Scripts\python.exe -m observatory.cli evaluate-detectors
.venv\Scripts\python.exe -m observatory.cli macro --origins 96
```

Le protocole d'alertes compare plusieurs scores sur quatre régimes, avec des graines de calibration/test disjointes. Le même budget de 1 % est imposé **en calibration** ; les taux de fausses alertes peuvent diverger ensuite. La règle opérationnelle |z| ≥4 n'est pas remplacée automatiquement par un gagnant synthétique.

Le premier appel `macro` télécharge le panneau public révisé. Ensuite, ajouter `--panel data/macro-revised.csv` permet de refaire l'expérience hors ligne ; ce fichier de données est ignoré par Git et n'est pas livré dans un nouveau clone.

La recherche macro utilise les dernières révisions connues à chaque origine lorsque les événements de millésime sont disponibles. Les cibles de première publication utilisent le dénominateur connu lors de cette publication. Les bandes sont calibrées sur les erreurs antérieures seulement, avec une couverture empirique mesurée. Une bande nominale à 90 % n'est pas une garantie de couverture.

Le panneau CSV révisé reste **exploratoire**. Un import CSV générique ne peut pas être estampillé « strict » par un simple argument de ligne de commande. Le chemin ALFRED strict exige une clé personnelle et des métadonnées d'acquisition vérifiées par le programme ; sa couverture historique doit encore être auditée. Voir les protocoles spécialisés [macro](MACRO_V2.md), [détecteurs](DETECTORS_V2.md) et [expositions](EXPOSURE_V2.md).
