## 🚀 PySpark Bike Sharing Analysis - Initial Implementation

### 📋 **Résumé des changements**

Cette PR introduit une analyse complète des données de vélos partagés de Chicago utilisant PySpark pour traiter plus de 1.1 million de trajets.

### ✨ **Fonctionnalités ajoutées**

#### **🔍 Analyses de données (6 analyses complètes)**
- ⏱️ **Durée moyenne des trajets par jour** - Identification des patterns temporels
- 📈 **Nombre de trajets quotidiens** - Analyse de la fréquentation
- 🚉 **Stations les plus populaires par mois** - Insights géographiques  
- 🔝 **Top 3 stations par jour (2 dernières semaines)** - Tendances récentes
- 👥 **Analyse par genre** - Comparaison durée trajets Hommes vs Femmes
- 👶👴 **Analyse par âge** - Top 10 âges trajets longs/courts

#### **🛠️ Infrastructure technique**
- 🐳 **Containerisation Docker** avec Spark 3.0.1
- 📊 **Harmonisation intelligente des schémas** (formats 2019 vs 2020)
- 💾 **Export automatique des rapports** en CSV
- 🧪 **Suite de tests unitaires** avec pytest
- ⚡ **Optimisations Spark** (cache, mémoire, partitioning)

### 📊 **Données traitées**
- **2019 Q4** : 704,054 trajets (12 colonnes)
- **2020 Q1** : 426,887 trajets (13 colonnes)  
- **Total** : **1,130,941 trajets analysés**

### 🎯 **Insights clés découverts**
- **Station dominante** : Canal St & Adams St (6,564+ trajets/mois)
- **Saisonnalité** : -70% d'activité entre octobre et novembre
- **Durée moyenne** : ~500 secondes avec variations météo
- **Patterns** : Pics d'usage jours ouvrables vs weekends

### 🗂️ **Fichiers ajoutés**
```
├── main.py                     # Script principal d'analyse PySpark
├── test_main.py               # Tests unitaires complets  
├── docker-compose.yml         # Orchestration des services
├── Dockerfile                 # Image personnalisée Spark
├── requirements.txt           # Dépendances Python
├── setup.py                   # Configuration package
├── README.md                  # Documentation complète
├── LICENSE                    # Licence MIT
├── .gitignore                # Exclusions Git
├── data/                     # Fichiers sources (ZIP)
└── reports/                  # Rapports CSV générés
```

### 🚀 **Comment tester**

1. **Build l'image Docker :**
   ```bash
   docker build --tag=pyspark-bike-analysis .
   ```

2. **Lancer l'analyse complète :**
   ```bash
   docker-compose up run
   ```

3. **Exécuter les tests :**
   ```bash
   docker-compose up test
   ```

### ✅ **Tests**
- [x] Chargement des données ZIP
- [x] Harmonisation des schémas
- [x] 6 analyses fonctionnelles  
- [x] Export CSV automatique
- [x] Tests unitaires passent
- [x] Docker build successful

### 📈 **Performance**
- Traitement de 1.1M+ lignes en ~2-3 minutes
- Utilisation mémoire optimisée (2GB driver/executor)
- Cache intelligent pour performances

### 🔄 **Prochaines étapes potentielles**
- [ ] Ajout d'analyses géospatiales avec les coordonnées lat/lng
- [ ] Dashboard interactif avec visualisations
- [ ] Pipeline automatisé avec Apache Airflow
- [ ] Déploiement sur cluster Spark

---

**Type de changement** : ✨ Nouvelle fonctionnalité  
**Impact** : 🔥 Major - Nouveau projet complet  
**Documentation** : ✅ À jour
