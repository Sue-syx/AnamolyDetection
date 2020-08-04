package model.common.utils

import model.common.{CustomizedFile, RegistryLookup}
import spray.json._
import model.common.utils.MyJsonProtocol._
import model.data.{AbstractData, ReadDataFile}
import model.pipelines.AbstractEstimator
import model.pipelines.unsupervised.examples.iforest.{IsolationForest, IsolationForestParams}
import model.pipelines.unsupervised.examples.{KNNBasedDetection, KNNBasedDetectionParams, LOF, LOFParams, Mahalanobis, MahalanobisParams, StandardScaler, StandardScalerParams}

/**
  * Created by yizhouyan on 9/7/19.
  */
object ClassNameMapping {
    def mapDataTypeToClass(lookup: RegistryLookup): AbstractData = {
        val jsonAst = lookup.params.mkString.parseJson
        lookup.name match{
            case "ReadDataFile" => new ReadDataFile(jsonAst.convertTo[CustomizedFile])
        }
    }

    def mapDataRangeList(path: String)
    : (Map[String, List[Int]], List[Double], Map[String, List[(Int, Int)]]) = {

        var initDataRangeList: Map[String, List[Int]] = Map()
        var pruneDataIndexRange: Map[String, List[(Int, Int)]] = Map()

        var tmp_if_krange: List[Double] = List.tabulate(5)(n => 0.5 + 0.1 * n)
        var if_krange: List[Double] = tmp_if_krange ++ tmp_if_krange ++ tmp_if_krange ++
          tmp_if_krange ++ tmp_if_krange ++ tmp_if_krange

        val PageBlock = "PageBlock".r
        val Pima = "Pima".r
        val SpamBase = "SpamBase".r

        val pimaTest = "pimaTest".r
        if((pimaTest findAllIn path).mkString != "" ){
            // 1.LOF
            val tmp_lof_krange: List[Int] = List.tabulate(2)(n => 10 + 10 * n)
            val lof_krange: List[Int] = tmp_lof_krange ++ tmp_lof_krange
            // 2.KNN
            val knn_krange: List[Int] = lof_krange
            // 3.IF
            tmp_if_krange = List.tabulate(2)(n => 0.5 + 0.1 * n)
            if_krange = tmp_if_krange ++ tmp_if_krange
            // 4.Mahalanobis
            val mahalanobis_N_range: List[Int] = List.tabulate(2)(n => 10 + 10 * n)
            // 5. N_Range
            val N_range: List[Int] = (mahalanobis_N_range ++ mahalanobis_N_range).sorted.reverse

            val init_index_range: List[(Int, Int)] = List((0, 4), (4, 8), (8, 12), (12, 14))
            val init_coef_index_range: List[(Int, Int)] = List((0, 2), (2, 4), (4, 6), (6, 7))
            val update_index_range: List[(Int, Int)] = List((0, 4), (4, 8), (8, 12), (12, 14))
            val update_coef_index_range: List[(Int, Int)] = List((0, 2), (2, 4), (4, 6), (6, 7))

            initDataRangeList += ("LOF" -> lof_krange)
            initDataRangeList += ("KNN" -> knn_krange)
            initDataRangeList += ("Mahalanobis" -> mahalanobis_N_range)
            initDataRangeList += ("N_range" -> N_range)

            pruneDataIndexRange += ("init_index_range" -> init_index_range)
            pruneDataIndexRange += ("init_coef_index_range" -> init_coef_index_range)
            pruneDataIndexRange += ("update_index_range" -> update_index_range)
            pruneDataIndexRange += ("update_coef_index_range" -> update_coef_index_range)
        }






        if((PageBlock findAllIn path).mkString != "" ){
            // 1.LOF
            val tmp_lof_krange: List[Int] = List.tabulate(10)(n => 10 + 10 * n)
            val lof_krange: List[Int] = tmp_lof_krange ++ tmp_lof_krange ++ tmp_lof_krange ++
              tmp_lof_krange ++ tmp_lof_krange ++ tmp_lof_krange
            // 2.KNN
            val knn_krange: List[Int] = lof_krange
            // 3.IF
            tmp_if_krange = List.tabulate(5)(n => 0.5 + 0.1 * n)
            if_krange = tmp_if_krange ++ tmp_if_krange ++ tmp_if_krange ++
              tmp_if_krange ++ tmp_if_krange ++ tmp_if_krange
            // 4.Mahalanobis
            val mahalanobis_N_range: List[Int] = List.tabulate(6)(n => 300 + 100 * n)
            val tmp_mahalanobis_N_range: List[Int] = mahalanobis_N_range ++ mahalanobis_N_range ++
              mahalanobis_N_range ++ mahalanobis_N_range ++ mahalanobis_N_range ++ mahalanobis_N_range ++
              mahalanobis_N_range ++ mahalanobis_N_range ++ mahalanobis_N_range ++ mahalanobis_N_range
            val N_range: List[Int] = tmp_mahalanobis_N_range.sorted.reverse

            val init_index_range: List[(Int, Int)] = List((0, 60), (60, 120), (120, 150), (150, 156))
            val init_coef_index_range: List[(Int, Int)] = List((0, 10), (10, 20), (20, 25), (25, 26))
            val update_index_range: List[(Int, Int)] = List((0, 60), (60, 120), (120, 150), (150, 156))
            val update_coef_index_range: List[(Int, Int)] = List((0, 10), (10, 20), (20, 25), (25, 26))

            initDataRangeList += ("LOF" -> lof_krange)
            initDataRangeList += ("KNN" -> knn_krange)
            initDataRangeList += ("Mahalanobis" -> mahalanobis_N_range)
            initDataRangeList += ("N_range" -> N_range)

            pruneDataIndexRange += ("init_index_range" -> init_index_range)
            pruneDataIndexRange += ("init_coef_index_range" -> init_coef_index_range)
            pruneDataIndexRange += ("update_index_range" -> update_index_range)
            pruneDataIndexRange += ("update_coef_index_range" -> update_coef_index_range)

        }

        if((Pima findAllIn path).mkString != "" ){
            // 1.LOF
            val tmp_lof_krange: List[Int] = List.tabulate(20)(n => 10 + 10 * n)
            val lof_krange: List[Int] = tmp_lof_krange ++ tmp_lof_krange ++ tmp_lof_krange ++
              tmp_lof_krange ++ tmp_lof_krange ++ tmp_lof_krange
            // 2.KNN
            val knn_krange: List[Int] = lof_krange
            // 3.IF
            tmp_if_krange = List.tabulate(5)(n => 0.5 + 0.1 * n)
            if_krange = tmp_if_krange ++ tmp_if_krange ++ tmp_if_krange ++
              tmp_if_krange ++ tmp_if_krange ++ tmp_if_krange
            // 4.Mahalanobis
            val mahalanobis_N_range: List[Int] = List.tabulate(6)(n => 220 + 10 * n)
            var tmp_mahalanobis_N_range: List[Int] = mahalanobis_N_range ++ mahalanobis_N_range ++
              mahalanobis_N_range ++ mahalanobis_N_range ++ mahalanobis_N_range ++ mahalanobis_N_range ++
              mahalanobis_N_range ++ mahalanobis_N_range ++ mahalanobis_N_range ++ mahalanobis_N_range
            tmp_mahalanobis_N_range = tmp_mahalanobis_N_range ++ tmp_mahalanobis_N_range
            val N_range: List[Int] = tmp_mahalanobis_N_range.sorted.reverse

            val init_index_range: List[(Int, Int)] = List((0, 120), (120, 240), (240, 270), (270, 276))
            val init_coef_index_range: List[(Int, Int)] = List((0, 20), (20, 40), (40, 45), (45, 46))
            val update_index_range: List[(Int, Int)] = List((0, 60), (60, 120), (120, 150), (150, 156))
            val update_coef_index_range: List[(Int, Int)] = List((0, 10), (10, 20), (20, 25), (25, 26))

            initDataRangeList += ("LOF" -> lof_krange)
            initDataRangeList += ("KNN" -> knn_krange)
            initDataRangeList += ("Mahalanobis" -> mahalanobis_N_range)
            initDataRangeList += ("N_range" -> N_range)

            pruneDataIndexRange += ("init_index_range" -> init_index_range)
            pruneDataIndexRange += ("init_coef_index_range" -> init_coef_index_range)
            pruneDataIndexRange += ("update_index_range" -> update_index_range)
            pruneDataIndexRange += ("update_coef_index_range" -> update_coef_index_range)
        }

        if((SpamBase findAllIn path).mkString != "" ){
            // 1.LOF
            val tmp_lof_krange: List[Int] = List.tabulate(10)(n => 10 + 10 * n)
            val lof_krange: List[Int] = tmp_lof_krange ++ tmp_lof_krange ++ tmp_lof_krange ++
              tmp_lof_krange ++ tmp_lof_krange ++ tmp_lof_krange
            // 2.KNN
            val knn_krange: List[Int] = lof_krange
            // 3.IF
            tmp_if_krange = List.tabulate(5)(n => 0.5 + 0.1 * n)
            if_krange = tmp_if_krange ++ tmp_if_krange ++ tmp_if_krange ++
              tmp_if_krange ++ tmp_if_krange ++ tmp_if_krange
            // 4.Mahalanobis
            val mahalanobis_N_range: List[Int] = List.tabulate(6)(n => 1400 + 100 * n)
            val N_size: Int = 6
            val tmp_mahalanobis_N_range: List[Int] = mahalanobis_N_range ++ mahalanobis_N_range ++
              mahalanobis_N_range ++ mahalanobis_N_range ++ mahalanobis_N_range ++ mahalanobis_N_range ++
              mahalanobis_N_range ++ mahalanobis_N_range ++ mahalanobis_N_range ++ mahalanobis_N_range
            val N_range: List[Int] = tmp_mahalanobis_N_range.sorted.reverse


            val init_index_range: List[(Int, Int)] = List((0, 60), (60, 120), (120, 150), (150, 156))
            val init_coef_index_range: List[(Int, Int)] = List((0, 10), (10, 20), (20, 25), (25, 26))
            val update_index_range: List[(Int, Int)] = List((0, 60), (60, 120), (120, 150), (150, 156))
            val update_coef_index_range: List[(Int, Int)] = List((0, 10), (10, 20), (20, 25), (25, 26))

            initDataRangeList += ("LOF" -> lof_krange)
            initDataRangeList += ("KNN" -> knn_krange)
            initDataRangeList += ("Mahalanobis" -> mahalanobis_N_range)
            initDataRangeList += ("N_range" -> N_range)

            pruneDataIndexRange += ("init_index_range" -> init_index_range)
            pruneDataIndexRange += ("init_coef_index_range" -> init_coef_index_range)
            pruneDataIndexRange += ("update_index_range" -> update_index_range)
            pruneDataIndexRange += ("update_coef_index_range" -> update_coef_index_range)

        }

        (initDataRangeList, if_krange, pruneDataIndexRange)
    }

    def mapClassNameToClass(lookup: RegistryLookup, stageNum: Int = -1): AbstractEstimator = {
        val jsonAst = lookup.params.mkString.parseJson
        lookup.name match{
            case "StandardScaler" => new StandardScaler(jsonAst.convertTo[StandardScalerParams], stageNum)
            case "IsolationForest" => new IsolationForest(jsonAst.convertTo[IsolationForestParams], stageNum)
            case "KNNBasedDetection" => new KNNBasedDetection(jsonAst.convertTo[KNNBasedDetectionParams], stageNum)
            case "LOF" => new LOF(jsonAst.convertTo[LOFParams], stageNum)
            case "Mahalanobis" => new Mahalanobis(jsonAst.convertTo[MahalanobisParams], stageNum)
        }
    }
}
