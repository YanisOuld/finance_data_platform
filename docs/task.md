# First Task

## CLOUD 
### TODO
Apprendre sur S3
- Apprendre la logique
- Intégrer S3 dans l'application

## BRONZE LAYER
### TODO
Finir le Bronze Layer
- Faire l'extraction de tout nos endpoints ou API
  - yahoo finance for price, dividends, splits 
  - SEC EDGAR API pour les fondamentaux
  - Fred pour la macros et le fx 
  - OpenFIGI API pour le Mapping
- Les stocker dans un S3

## SILVER LAYER
### TODO
- Commencer à apprendre les methodes de normalisation
- Normaliser toutes les types de datas
  - Price
  - Financial
  - FX
  - Macros
  - 
- Les stocker dans un format de tableaux dans un S3 

## GOLD LAYER
### TODO
- Commencer à indentifier les metrics pour le projet
- Stocker le gold layer sur S3
- Commencer la validation de la datas
- Stocker la data validées sur postgres 
  - Créer des models
  - Stocker en continue 

## ORCHESTRATION
### TODO
- Apprendre la stucture de Airflow
- Faire les jobs pour tout le processus
- Orchestrer le système
- Stocker Airflow dans un serveur AWS



# AFTER


## APPLICATION
### TODO
- Créer le backend pour l'application 
  - SQLalchemy pour les requètes
  - Alembic pour les contrats
  - FastAPI pour les apis et les services !


## FRONTEND
### TODO
- Faire des interfaces 


# TASKS
1. Faire une documentation de l'architecture
2. Finir les bronzes 
3. Apprendre S3
4. Intégrer S3
5. Faire Silver
6. Faire Gold 
7. Postgres
8. Avoir le redis Cache


Aller chercher les datas avec des balises