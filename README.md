# 🌟 Projet Spark Scala Sbt sous IntelliJ 🌟

## 👤 Auteur : Mr MEJRI Salam

---

## 📚 Contexte Projet

Le service de vente de l’entreprise LatDior Data cherche un Data Engineer pour mettre en place une application Spark/Scala/SBT pour la gestion des contrats. L’application doit être capable de traiter les types de fichiers suivants:

- 📁 JSON
- 📁 CVS
- 📁 Parquet
- 📁 ORC
- 📁 XML

Pour la version 1, vous allez intégrer uniquement le traitement des fichiers CSV et JSON.

---

## 🔧 Contraintes Techniques

❖ Vous serez obligé de réaliser les développements avec:
- 🛠️ IntelliJ IDEA
- 🛠️ Sbt
- 🛠️ Scala 2.12.15
- 🛠️ Java 1.8
- 🛠️ Spark

❖ Le respect du découpage du code est impératif. Sans le respect du découpage défini, vos développements ne seront pas acceptés.

❖ Un passage de connaissance sera organisé dès votre arrivée.

---

## 📂 Découpage du code

### `sbt.build`
📄 [Configuration sbt.build](https://github.com/mslouma88/SparkProjet/blob/main/build.sbt)

### 📁 Type CSV
📄 [Fichier CSV](https://github.com/mslouma88/SparkProjet/blob/main/src/main/resources/Configuration/reader_csv.json)

### 📁 Type JSON
📄 [Fichier JSON](https://github.com/mslouma88/SparkProjet/blob/main/src/main/resources/Configuration/reader_json.json)

### 📝 Présentation des données

- Exemple de CVS avec séparateur #
- Exemple de JSON

### 🛠️ Présentation de l’existant

- Prepare Appli Args
  📄 [Args.scala](https://github.com/mslouma88/SparkProjet/blob/main/src/main/scala/sda/args/Args.scala)

- Parser FileConf
  📄 [ConfigurationParser.scala](https://github.com/mslouma88/SparkProjet/blob/main/src/main/scala/sda/parser/ConfigurationParser.scala)
  📄 [FileReaderUsingIOSource.scala](https://github.com/mslouma88/SparkProjet/blob/main/src/main/scala/sda/parser/FileReaderUsingIOSource.scala)

- Objet CsvReader
  📄 [Reader.scala](https://github.com/mslouma88/SparkProjet/blob/main/src/main/scala/sda/reader/Reader.scala)
  📄 [CsvReader.scala](https://github.com/mslouma88/SparkProjet/blob/main/src/main/scala/sda/reader/CsvReader.scala)

- JsonReader
  - Coder la classe JsonReader de la même façon que la classe CsvReader mais avec ses propres attributs.
  - La classe héritera le trait Reader.
  - Elle doit contenir une fonction `read` qui permet de lire un fichier JSON et retourne un DataFrame en se basant sur les attributs de la classe JsonReader provenant du fichier de conf `reader_json.json`.

- Objet ServiceVente
  📄 [ServiceVente.scala](https://github.com/mslouma88/SparkProjet/blob/main/src/main/scala/sda/traitement/ServiceVente.scala)

  - `def calculTTC(): DataFrame`
    - Calcule le TTC => HTT+TVA*HTT, le TTC doit être arrondi à 2 chiffres après la virgule.
    - Supprime la colonne TVA, HTT.

  - `Extract_Date_End_Contrat_Ville`
    - Créer une nouvelle colonne `Date_End_contrat` et `Ville` en utilisant la méthode `select from_json` et `regexp_extract` pour extraire `YYYY-MM-DD`.
    - Supprime la colonne `metaData`.

  - `Contrat_Status`
    - Créer une nouvelle colonne `Contrat_Status` avec "Expired" si le contrat a expiré et sinon "Actif".

### 🗃️ Main Configuration

- Type JSON
- Type CSV

### 🖥️ MainBatch

📄 [MainBatch.scala](https://github.com/mslouma88/SparkProjet/blob/main/src/main/scala/sda/main/MainBatch.scala)

---

Ce projet vise à automatiser la gestion des contrats pour l’entreprise LatDior Data en utilisant les technologies Spark, Scala et SBT, développées sous IntelliJ IDEA.
