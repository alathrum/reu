import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.linalg._
import org.apache.spark.rdd._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import java.io._

object Detector {
  def main(args: Array[String]) {
    val file = "reu/kddcup/kddcup.data_10" //fix filename

    val conf = new SparkConf().setAppName("Detector")
    val sc = new SparkContext(conf)

    val csvfile = s"reu//output//csv//allData.csv"  //fix file name
    val csv = new PrintWriter(new File(csvfile)) 
    csv.println("k,t,Total_Count,Abnormal_Count,Normal_Count,False_Positive_Count,True_Positive_Count,False_Negative_Count,True_Negative_Count,Accuracy_Rate,True_Positive_Rate,False_Positive_Rate,True_Negative_Rate,False_Negative_Rate")
    csv.flush()

    val allRaw = sc.textFile(file)
    val parse = catAndLab(allRaw)
    val allData = allRaw.map(line => (line, parse(line)._2))

    anomalyDetector(allData, 150, 100, csv)
/*    anomalyDetector(allData, 100, 100, csv)
    anomalyDetector(allData, 100, 150, csv)
    anomalyDetector(allData, 125, 50, csv)
    anomalyDetector(allData, 125, 100, csv)
    anomalyDetector(allData, 125, 150, csv)
    anomalyDetector(allData, 150, 50, csv)
    anomalyDetector(allData, 150, 100, csv)
    anomalyDetector(allData, 150, 150, csv)
    anomalyDetector(allData, 175, 50, csv)
    anomalyDetector(allData, 175, 100, csv)
    anomalyDetector(allData, 175, 150, csv)
    anomalyDetector(allData, 200, 50, csv)
    anomalyDetector(allData, 200, 100, csv)
    anomalyDetector(allData, 200, 150, csv)

*/
   
    
  }// end of main

  def anomalyDetector(allData: RDD[(String, Vector)], k: Int, t: Int, csv: PrintWriter) = {

    val allVectors = allData.values
    val norm = normalization(allVectors)

    val trainData = allData.sample(false, .1)
    val testData = allData.subtract(trainData)
    val trainVectors = trainData.values

    val normData = trainVectors.map(datum => norm(datum))

    normData.cache()

    val kmeans = new KMeans()
    kmeans.setK(k)
    kmeans.setRuns(10)
    kmeans.setEpsilon(1.0e-4)
    val model = kmeans.run(normData)

    normData.unpersist()

    val distances = normData.map(datum => distToCenter(datum, model))
    val threshold = distances.top(t).last


    val abnormal = testData.filter { case (original, datum) =>
      val normalized = norm(datum)
      distToCenter(normalized, model) > threshold
    }.keys
    
    val temp = testData.keys

    val normal = temp.subtract(abnormal)

    findStats(abnormal, normal,k,t, csv)

}


  def distToCenter(datum: Vector, model: KMeansModel) = {
    val cluster = model.predict(datum)
    val centroid = model.clusterCenters(cluster)
    distance(centroid, datum)
  }//end of distToCenter

  def distance(x: Vector, y: Vector) =
    math.sqrt(x.toArray.zip(y.toArray).map(z => z._1 - z._2).map(d => d * d).sum)


  def catAndLab(rawData: RDD[String]): (String => (String, Vector)) = {
    val splitData = rawData.map(_.split(','))
    val protocols = splitData.map(_(1)).distinct().collect().zipWithIndex.toMap
    val services = splitData.map(_(2)).distinct().collect().zipWithIndex.toMap
    val states = splitData.map(_(3)).distinct().collect().zipWithIndex.toMap

    (line: String) => {
      val buffer = line.split(',').toBuffer
      val protocol = buffer.remove(1)
      val service = buffer.remove(1)
      val tcp = buffer.remove(1)
      val label = buffer.remove(buffer.length - 1)
      val vector = buffer.map(_.toDouble)

      val newProtocol = new Array[Double](protocols.size)
      val newService = new Array[Double](services.size)
      val newTcp = new Array[Double](states.size)

      newProtocol(protocols(protocol)) = 1.0
      newService(services(service)) = 1.0
      newTcp(states(tcp)) = 1.0

      vector.insertAll(1, newTcp)
      vector.insertAll(1, newService)
      vector.insertAll(1, newProtocol)

      (label, Vectors.dense(vector.toArray))
    }
  }//end of catAndLab

  def normalization(data: RDD[Vector]): (Vector => Vector) = {
    val arrayData = data.map(_.toArray)
    val numCols = arrayData.first().length
    val n = arrayData.count()
    val sum = arrayData.reduce(
      (a,b ) => a.zip(b).map(t => t._1 + t._2))
    val sumSquares = arrayData.aggregate(
      new Array[Double](numCols)
    )(
      (a, b) => a.zip(b).map(t => t._1 + t._2 * t._2),
      (a, b) => a.zip(b).map(t => t._1 + t._2)
    )
    val stdev = sumSquares.zip(sum).map {
      case(sumSq, sums) => math.sqrt(n * sumSq - sums * sums) / n
    }

    val mean = sum.map(_ / n)
    (datum: Vector) => {
      val normArray = (datum.toArray, mean, stdev).zipped.map(
        (value, means, stdevs) =>
          if(stdevs <= 0)(value - means) else(value - means) / stdevs
      )
      Vectors.dense(normArray)
    }
  }//end of normalization

  def findStats(abnormal: RDD[String],normal: RDD[String],k: Int, t: Int, csv: PrintWriter) = {

    val file = s"reu//output//anomalies//$k-$t.txt" //fix file name
    val output = new PrintWriter(new File(file))
    output.println(s"K = $k  Threshold = $t")


    //passing data that was malicious
    val badNormal = normal.filter(line => !line.contains("normal"))

    //denied data that was non malicious
    val goodAbnormal = abnormal.filter(line => line.contains("normal"))
 
    val abnormalCount = abnormal.count() 
    val normalCount = normal.count() 
    val badNormalCount = badNormal.count() 
    val goodAbnormalCount = goodAbnormal.count() 
    val count = abnormalCount + normalCount


    val tNeg = normalCount - badNormalCount
    val fNeg = badNormalCount
    val tPos = abnormalCount - goodAbnormalCount
    val fPos = goodAbnormalCount

    val accuracy: Double = ((tPos + tNeg * 1.0) / count * 1.0) * 100
    val tpr: Double = (tPos * 1.0 / (tPos + fNeg * 1.0)) * 100
    val tnr: Double = (tNeg * 1.0 / (tNeg + fPos * 1.0)) * 100
    val fnr: Double = (fNeg * 1.0 / (tPos + fNeg * 1.0)) * 100
    val fpr: Double = (fPos * 1.0 / (tNeg + fPos * 1.0)) * 100

    
    output.println(f"\nTotal Vectors                                :   $count\n")
    output.println(f"Vectors found to be abnormal                 :   $abnormalCount")
    output.println(f"Vectors found to be normal                   :   $normalCount\n")

    output.println(f"Vectors found to be abnormal that were normal:   $fPos")
    output.println(f"Vectors found to be normal that were abnormal:   $fNeg\n")

    output.println(f"Vectors correctly labeled as abnormal        :   $tPos")
    output.println(f"Vectors correctly labeled as normal          :   $tNeg\n")

    output.println(f"Accuracy Rate                                :  $accuracy%8.4f%%\n")
    output.println(f"True Positive Rate                           :  $tpr%8.4f%%")
    output.println(f"True Negative Rate                           :  $tnr%8.4f%%\n")

    output.println(f"False Positive Rate                          :  $fpr%8.4f%%")
    output.println(f"False Negative Rate                          :  $fnr%8.4f%%")


    csv.println(s"$k,$t,$count,$abnormalCount,$normalCount,$fPos,$tPos,$fNeg,$tNeg,$accuracy,$tpr,$fpr,$tnr,$fnr")


    output.flush()
    output.close()
    csv.flush()

  }


}// end of Test
