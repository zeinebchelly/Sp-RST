package rst

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import scala.util.Try
import scala.io.Source 
import scala.util.Sorting.quickSort
import java.io.FileWriter
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

/**
 * @authors Beck Gaël & Chelly Dagdia Zaineb
 */
object Main {
  def main(args: Array[String]): Unit =
  {
	@transient val sc = new SparkContext(new SparkConf)

	/* Requiered Paramaters
	 * Datasetes should be as follow :
	 * 	Only real
	 * 	Only Categorial
	 * 	Both with real as first columns and categorial next columns
	 */
	val rawdata = sc.textFile(args(0)).cache // args(0) = path to file of any type
	val sep = Try(args(1)).getOrElse(",") // separator type
	val nbFeatures = rawdata.first.split(sep).size - 1 // Number of features supposing that the last column is the label
	//val savingPath = args(2)
	val sizeFeatCluster = Try(args(2).toInt).getOrElse(8) 		// Size of clusters of features
	val nbBlocClusteringFeats = Try(args(3).toInt).getOrElse(2) 	// nb bucket in LSH for clusterise features, roughly 1000 feat per bucket
	val nbBucketDiscretization = Try(args(4).toInt).getOrElse(10) // Defined in how many bucket we discretize real data
	

	val featuresReduction =  new DimReduction(sc)

	val fw = new FileWriter("F"+nbBlocClusteringFeats.toString+" B"+nbBucketDiscretization.toString, true)
	fw.write("Welcome to QFS result file\nParameters are\nNumber Bucket Discretization : " + nbBucketDiscretization + "\nsizeFeatCluster : " + sizeFeatCluster + "\nNumber Bloc Clustering Feats : " + nbBlocClusteringFeats )

  


	val realCatLabel = rawdata.map(_.split(sep)).map( rawVector => (rawVector.take(nbFeatures).map(_.toDouble), rawVector.last)).cache
	val transposedRDD = Fcts.rddTranspose(realCatLabel.map(_._1))
	val columnsOfFeats = Fcts.rapidClusteringofFeature(transposedRDD, nbBlocClusteringFeats, sizeFeatCluster)
	val nbColumn = columnsOfFeats.size

	val realData = realCatLabel.map(_._1).cache
	val labelData = realCatLabel.map(_._2).zipWithIndex.cache

	val nbRealFeatures = realData.first.size
	val discretizedRealRDD = Fcts.discretize(sc, realData, nbBucketDiscretization)
	val labelAsDouble = Fcts.labelToDouble(sc, labelData)

	val readyForDimReducRDD = discretizedRealRDD.zip(labelAsDouble).map{ case(realValues, (label, id)) => (id, realValues, label) }
	
	val seqFeats = (0 until nbFeatures).toArray
	rawdata.unpersist(true)
	realCatLabel.unpersist(true)

	val dividByFeatsRDD = Fcts.divideBySelectedFeatures(readyForDimReducRDD, columnsOfFeats)
	dividByFeatsRDD.cache

	val t0 = System.nanoTime
	val reduceFeats = featuresReduction.roughSetPerFeatsD2(dividByFeatsRDD, nbColumn, columnsOfFeats)
	val t1 = System.nanoTime
	val duration = (t1 - t0) / 1000000000D

	val sortedByFeatReadyToSave = reduceFeats.sorted.mkString(",")

	fw.write("\nDuration of RST exclusively : " + duration + " s\nReduced Features (start to index 0) :\n" + sortedByFeatReadyToSave)
	fw.close

	sc.stop
	}
}