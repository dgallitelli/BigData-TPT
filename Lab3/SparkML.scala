// Databricks notebook source
// MAGIC %md
// MAGIC # D&K907a - IOT Big Data Processing - Lab 3 : Spark Machine Learning
// MAGIC ### A.Y. 2017/18 - Davide Gallitelli

// COMMAND ----------

import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.util.MLUtils

val data = MLUtils.loadLibSVMFile(sc,"/databricks-datasets/samples/data/mllib/sample_libsvm_data.txt")

// COMMAND ----------

// Split the data into training and test
val splits = data.randomSplit(Array(0.7,0.3), seed=16)
val (trainingData, testData) = (splits(0),splits(1))

// Train a DecisionTree model
val numClasses = 2
val categoricalFeaturesInfo = Map[Int,Int]()
val impurity = "gini"
val maxDepth = 5
val maxBins = 32

// COMMAND ----------

val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo, impurity, maxDepth, maxBins)

// Evaluate model on test instances and compute test error
val labelsAndPreds = testData.map{ point =>
  val prediction = model.predict(point.features)
  (point.label, prediction)
}
val testErr = labelsAndPreds.filter(r=>r._1!=r._2).count.toDouble/testData.count()
println("Test Error = "+testErr)
println("Learnt classification tree model:\n"+model.toDebugString)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Question 1 - What is the error of the classifier with this dataset?
// MAGIC 
// MAGIC The error of the classifier with this dataset is 0.07, which means approximately 7% .

// COMMAND ----------

// MAGIC %md
// MAGIC ### Question 2 - Improve the error of the classifier (tuning parameters or using Random Forest)

// COMMAND ----------

// MAGIC %md
// MAGIC The first test is to tune the parameters of the standard Decision Tree model.
// MAGIC 
// MAGIC The previous results were quite good, obtaining a 7% error . Still, this number coud be improved by parameter tuning. 
// MAGIC Reducing the **maxBins** parameter is also a good approach to improve accuracy, which represents the number of bins used when discretizing continuous features. This allows the variables to be split in broader categories. Moreover, a different algorithm for impurity computation has been used, "entropy".
// MAGIC 
// MAGIC These choices yielded a classification error of 0.034, ~3.4% .

// COMMAND ----------

// Improving with parameter tuning
val impurity2 = "entropy"
val maxDepth2 = 5
val maxBins2 = 5
val model2 = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo, impurity2, maxDepth2, maxBins2)
val labelsAndPreds2 = testData.map{ point =>
  val prediction = model2.predict(point.features)
  (point.label, prediction)
}
val testErr2 = labelsAndPreds2.filter(r=>r._1!=r._2).count.toDouble/testData.count()
println("Test Error = "+testErr2)
println("Learnt classification tree model:\n"+model2.toDebugString)

// COMMAND ----------

// MAGIC %md
// MAGIC One of the best method for classification is the **random forest** algorithm. It is an ensemble method of multiple, here *numTrees*, decision trees. This classification method proves to be extremely effective, yielding a 0.0 error on the dataset. Further tries should be done on different datasets to get the actual dataset, possibly with cross-validation.

// COMMAND ----------

// Improving with Random Forest
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel

val numTrees = 15
val featureSubsetStrategy = "auto"
val rf_model = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
  numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

val rf_labelAndPreds = testData.map { point =>
  val prediction = rf_model.predict(point.features)
  (point.label, prediction)
}
val rf_testErr = rf_labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count()
println("Test Error = " + rf_testErr)
println("Learned classification forest model:\n" + rf_model.toDebugString)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Question 3 - Use cross-validation for the evaluation

// COMMAND ----------

// MAGIC %md
// MAGIC The *sparkML* library provides useful routines fot the k-fold cross-validation evaluation.
// MAGIC 
// MAGIC It allows the definition of a **pipeline** for the estimator, a series of stages with multiple classifiers (in this case, only the sparkML version of the Random Classifier) has been used. Then, the performances of the Random Forest algorithm are analyzed via an evaluator, here based on the *accuracy* metric. 
// MAGIC 
// MAGIC With a **k=15**, a real improvement in the accuracy can be noticed, with an error rate of just 0.9% .

// COMMAND ----------

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.tuning.{ParamGridBuilder, CrossValidator}
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator


val rf = new RandomForestClassifier()
  .setLabelCol("label")
  .setFeaturesCol("features")
  .setNumTrees(30)
val pipeline = new Pipeline().setStages(Array(rf)) 
val paramGrid = new ParamGridBuilder().build() // No parameter search
val evaluator = new MulticlassClassificationEvaluator()
  .setLabelCol("label")
  .setPredictionCol("prediction")
  // "f1" (default), "weightedPrecision", "weightedRecall", "accuracy"
  .setMetricName("accuracy") 
val cv = new CrossValidator()
  // ml.Pipeline with ml.classification.RandomForestClassifier
  .setEstimator(pipeline)
  // ml.evaluation.MulticlassClassificationEvaluator
  .setEvaluator(evaluator) 
  .setEstimatorParamMaps(paramGrid)
  .setNumFolds(15)
val trainDF = sqlContext.createDataFrame(trainingData)
val testDF = sqlContext.createDataFrame(testData)
val model = cv.fit(MLUtils.convertVectorColumnsToML(trainDF))
val prediction = model.transform(MLUtils.convertVectorColumnsToML(testDF))

val myAvgMetrics = 1 - (model.avgMetrics)(0)
println(myAvgMetrics)

// COMMAND ----------


