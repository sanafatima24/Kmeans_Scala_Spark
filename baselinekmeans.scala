// import org.apache.spark.sql.SparkSession
// import org.apache.spark.mllib.linalg.Vectors
// import org.apache.spark.mllib.feature.StandardScaler
// import org.apache.spark.mllib.clustering.KMeans
// import org.apache.spark.rdd.RDD

// // Initialize Spark Session
// val spark = SparkSession.builder()
//   .appName("KMeansBaseline")
//   .getOrCreate()

// val sc = spark.sparkContext

// // Load the dataset into an RDD
// val data = sc.textFile("path_to_wine_quality_dataset.csv")

// // Assuming the first row is a header
// val header = data.first()
// val dataRdd = data.filter(row => row != header)
//   .map { line =>
//     val cols = line.split(",").map(_.trim.toDouble)
//     (cols.init, cols.last)  // Separate features and labels
//   }

// // Data Preparation
// val featuresRdd: RDD[org.apache.spark.mllib.linalg.Vector] = dataRdd.map { case (features, _) =>
//   Vectors.dense(features)
// }

// val scaler = new StandardScaler(withMean = true, withStd = true).fit(featuresRdd)
// val normalizedFeaturesRdd = featuresRdd.map(scaler.transform)

// // K-Means Baseline Implementation
// val k = 3
// val maxIterations = 20

// // Randomly initialize centroids
// val initialCentroids = normalizedFeaturesRdd.takeSample(withReplacement = false, num = k, seed = 42)

// // Function to compute closest centroid
// def closestCentroid(point: org.apache.spark.mllib.linalg.Vector, centroids: Array[org.apache.spark.mllib.linalg.Vector]): Int = {
//   val distances = centroids.map(centroid => Vectors.sqdist(point, centroid))
//   distances.indexOf(distances.min)
// }

// // K-Means Algorithm
// def kmeansRdd(featuresRdd: RDD[org.apache.spark.mllib.linalg.Vector], k: Int, maxIterations: Int): Array[org.apache.spark.mllib.linalg.Vector] = {
//   var centroids = featuresRdd.takeSample(withReplacement = false, num = k, seed = 42)
  
//   for (_ <- 1 to maxIterations) {
//     // Assign points to the nearest centroid
//     val clusters = featuresRdd.map { point =>
//       (closestCentroid(point, centroids), (point, 1))
//     }
    
//     // Recalculate centroids
//     val newCentroids = clusters
//       .reduceByKey { case ((point1, count1), (point2, count2)) =>
//         (Vectors.dense((point1.toArray, point2.toArray).zipped.map(_ + _)), count1 + count2)
//       }
//       .mapValues { case (sumPoints, count) =>
//         Vectors.dense(sumPoints.toArray.map(_ / count))
//       }
//       .collect()
//       .sortBy(_._1)
//       .map(_._2)
    
//     centroids = newCentroids
//   }
  
//   centroids
// }

// // Measure performance
// val startTime = System.nanoTime()

// // Run the k-means algorithm
// val finalCentroids = kmeansRdd(normalizedFeaturesRdd, k, maxIterations)

// val endTime = System.nanoTime()
// val duration = (endTime - startTime) / 1e9d
// println(s"Execution time: $duration seconds")

// // Print final centroids
// println("Final centroids:")
// finalCentroids.foreach(println)


// ---------------------------------------------------------------------------------------------------------------------------


// import org.apache.spark.sql.SparkSession
// import org.apache.spark.mllib.linalg.Vectors
// import org.apache.spark.mllib.feature.StandardScaler
// import org.apache.spark.rdd.RDD

// object BaselineKMeans {
//   def main(args: Array[String]): Unit = {
//     val spark = SparkSession.builder()
//       .appName("Baseline KMeans")
//       .master("local[*]") // or your specific Spark master
//       .getOrCreate()

//     val sc = spark.sparkContext

//     val data = sc.textFile("path_to_wine_quality_dataset.csv")
//     val header = data.first()
//     val dataRdd = data.filter(row => row != header)
    
//     val featuresRdd: RDD[org.apache.spark.mllib.linalg.Vector] = dataRdd.map { line =>
//       val parts = line.split(";").map(_.toDouble)
//       Vectors.dense(parts)
//     }
    
//     val scaler = new StandardScaler(withMean = true, withStd = true).fit(featuresRdd)
//     val normalizedFeaturesRdd = featuresRdd.map(scaler.transform)

//     val k = 3
//     val maxIterations = 20

//     val initialCentroids = normalizedFeaturesRdd.takeSample(withReplacement = false, num = k, seed = 42)

//     def closestCentroid(point: org.apache.spark.mllib.linalg.Vector, centroids: Array[org.apache.spark.mllib.linalg.Vector]): Int = {     
//       centroids.zipWithIndex.minBy { case (centroid, _) =>
//         Vectors.sqdist(point, centroid)
//       }._2
//     }

//     // def kmeansRdd(featuresRdd: RDD[org.apache.spark.mllib.linalg.Vector], k: Int, maxIterations: Int): Array[org.apache.spark.mllib.linalg.Vector] = {
//     //   var centroids = initialCentroids
//     //   for (i <- 1 to maxIterations) {
//     //     val closest = featuresRdd.map(point => (closestCentroid(point, centroids), point))
//     //     val newCentroids = closest
//     //       .groupByKey()
//     //       .mapValues(points => Vectors.dense(points.reduce(_ + _).toArray.map(_ / points.size)))
//     //       .collect()
//     //       .sortBy(_._1)
//     //       .map(_._2)

//     //     if (newCentroids.sameElements(centroids)) return centroids
//     //     centroids = newCentroids
//     //   }
//     //   centroids
//     // }

//     def kmeansRdd(featuresRdd: RDD[org.apache.spark.mllib.linalg.Vector], k: Int, maxIterations: Int): Array[org.apache.spark.mllib.linalg.Vector] = {
//       var centroids = initialCentroids
//       for (i <- 1 to maxIterations) {
//         val closest = featuresRdd.map(point => (closestCentroid(point, centroids), point))
//         val newCentroids = closest
//           .groupByKey()
//           .mapValues { points =>
//             val summedVector = points.reduce((v1, v2) => Vectors.dense(
//               v1.toArray.zip(v2.toArray).map { case (a, b) => a + b }
//             ))
//             Vectors.dense(summedVector.toArray.map(_ / points.size))
//           }
//           .collect()
//           .sortBy(_._1)
//           .map(_._2)

//         if (newCentroids.sameElements(centroids)) return centroids
//         centroids = newCentroids
//       }
//       centroids
//     }


//     val startTime = System.nanoTime()
//     val finalCentroids = kmeansRdd(normalizedFeaturesRdd, k, maxIterations)
//     val endTime = System.nanoTime()

//     val duration = (endTime - startTime) / 1e9d
//     println(s"Execution time: $duration seconds")

//     println("Final centroids:")
//     finalCentroids.foreach(println)

//     spark.stop()
//   }
// }



// -----------------------------------------------------------------------------------------------------------------



// import org.apache.spark.sql.SparkSession
// import org.apache.spark.mllib.linalg.Vectors
// import org.apache.spark.mllib.feature.StandardScaler
// import org.apache.spark.rdd.RDD
// import org.apache.spark.SparkConf
// import org.apache.spark.SparkContext

// object BaselineKMeans {
//   def main(args: Array[String]): Unit = {
//     // Configure Spark with memory settings
//     val conf = new SparkConf()
//       .setAppName("BaselineKMeans")
//       .set("spark.driver.memory", "512M")  // Set driver memory

//     // Initialize SparkContext and SparkSession with the configuration
//     val sc = new SparkContext(conf)
//     val spark = SparkSession.builder()
//       .appName("Baseline KMeans")
//       .master("local[*]") // or your specific Spark master
//       .getOrCreate()

//     // Your existing code
//     val data = sc.textFile("path_to_wine_quality_dataset.csv")
//     val header = data.first()
//     val dataRdd = data.filter(row => row != header)
    
//     val featuresRdd: RDD[org.apache.spark.mllib.linalg.Vector] = dataRdd.map { line =>
//       val parts = line.split(";").map(_.toDouble)
//       Vectors.dense(parts)
//     }
    
//     val scaler = new StandardScaler(withMean = true, withStd = true).fit(featuresRdd)
//     val normalizedFeaturesRdd = featuresRdd.map(scaler.transform)

//     val k = 3
//     val maxIterations = 20

//     val initialCentroids = normalizedFeaturesRdd.takeSample(withReplacement = false, num = k, seed = 42)

//     def closestCentroid(point: org.apache.spark.mllib.linalg.Vector, centroids: Array[org.apache.spark.mllib.linalg.Vector]): Int = {     
//       centroids.zipWithIndex.minBy { case (centroid, _) =>
//         Vectors.sqdist(point, centroid)
//       }._2
//     }

//     def kmeansRdd(featuresRdd: RDD[org.apache.spark.mllib.linalg.Vector], k: Int, maxIterations: Int): Array[org.apache.spark.mllib.linalg.Vector] = {
//       var centroids = initialCentroids
//       for (i <- 1 to maxIterations) {
//         val closest = featuresRdd.map(point => (closestCentroid(point, centroids), point))
//         val newCentroids = closest
//           .groupByKey()
//           .mapValues { points =>
//             val summedVector = points.reduce((v1, v2) => Vectors.dense(
//               v1.toArray.zip(v2.toArray).map { case (a, b) => a + b }
//             ))
//             Vectors.dense(summedVector.toArray.map(_ / points.size))
//           }
//           .collect()
//           .sortBy(_._1)
//           .map(_._2)

//         if (newCentroids.sameElements(centroids)) return centroids
//         centroids = newCentroids
//       }
//       centroids
//     }

//     val startTime = System.nanoTime()
//     val finalCentroids = kmeansRdd(normalizedFeaturesRdd, k, maxIterations)
//     val endTime = System.nanoTime()

//     val duration = (endTime - startTime) / 1e9d
//     println(s"Execution time: $duration seconds")

//     println("Final centroids:")
//     finalCentroids.foreach(println)

//     spark.stop()
//   }
// }




// --------------------------------------------------------------------------------------------------------------------




import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object BaselineKMeans {
  def main(args: Array[String]): Unit = {
    // Configure Spark with memory settings and set master URL
    val conf = new SparkConf()
      .setAppName("BaselineKMeans")
      .setMaster("local[*]")  // Set the master URL to local[*]
      .set("spark.driver.memory", "512M")  // Set driver memory
      .set("spark.dynamicAllocation.enabled", "true")
      .set("spark.shuffle.service.enabled", "true")

    // Initialize SparkContext and SparkSession with the configuration
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    // Your existing code
    val data = sc.textFile("winequality-red.csv")
    val header = data.first()
    val dataRdd = data.filter(row => row != header)
    
    val featuresRdd: RDD[org.apache.spark.mllib.linalg.Vector] = dataRdd.map { line =>
      val parts = line.split(";").map(_.toDouble)
      Vectors.dense(parts)
    }
    
    val scaler = new StandardScaler(withMean = true, withStd = true).fit(featuresRdd)
    val normalizedFeaturesRdd = featuresRdd.map(scaler.transform)

    val k = 3
    val maxIterations = 20

    val initialCentroids = normalizedFeaturesRdd.takeSample(withReplacement = false, num = k, seed = 42)

    def closestCentroid(point: org.apache.spark.mllib.linalg.Vector, centroids: Array[org.apache.spark.mllib.linalg.Vector]): Int = {     
      centroids.zipWithIndex.minBy { case (centroid, _) =>
        Vectors.sqdist(point, centroid)
      }._2
    }

    def kmeansRdd(featuresRdd: RDD[org.apache.spark.mllib.linalg.Vector], k: Int, maxIterations: Int): Array[org.apache.spark.mllib.linalg.Vector] = {
      var centroids = initialCentroids
      for (i <- 1 to maxIterations) {
        val closest = featuresRdd.map(point => (closestCentroid(point, centroids), point))
        val newCentroids = closest
          .groupByKey()
          .mapValues { points =>
            val summedVector = points.reduce((v1, v2) => Vectors.dense(
              v1.toArray.zip(v2.toArray).map { case (a, b) => a + b }
            ))
            Vectors.dense(summedVector.toArray.map(_ / points.size))
          }
          .collect()
          .sortBy(_._1)
          .map(_._2)

        if (newCentroids.sameElements(centroids)) return centroids
        centroids = newCentroids
      }
      centroids
    }

    val startTime = System.nanoTime()
    val finalCentroids = kmeansRdd(normalizedFeaturesRdd, k, maxIterations)
    val endTime = System.nanoTime()

    val duration = (endTime - startTime) / 1e9d
    println(s"Execution time: $duration seconds")

    println("Final centroids:")
    finalCentroids.foreach(println)

    spark.stop()
  }
}
