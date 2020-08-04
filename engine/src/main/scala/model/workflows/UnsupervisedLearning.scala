package model.workflows

import utils.Utils._
import client.{ModelStorageSyncer, NewExperimentRun, NewOrExistingProject}
import conf.InputConfigs
import model.common.utils.ConfigParser
import model.common.{ColumnTracking, SharedParams, SubspaceParams, UnsupervisedWorkflowInput, utils}
import org.apache.spark.sql.{DataFrame, SparkSession}
import spray.json._
import model.common.utils.MyJsonProtocol._
import model.data.FetchDataExample
import model.pipelines.Pipelines
import model.pipelines.unsupervised.examples.{LOF, LOFParams, StandardScaler, StandardScalerParams}
import org.apache.commons.configuration.{CompositeConfiguration, PropertiesConfiguration}
import org.apache.spark.sql.functions.col
import model.pipelines.autoOD.AutoODFunction
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{MinMaxScaler, VectorAssembler}
import org.apache.spark.sql.types.DoubleType

import scala.io.Source
/**
 * Created by yizhouyan on 9/5/19.
 */
object UnsupervisedLearning{

    def main(args: Array[String]): Unit = {
        implicit val spark: SparkSession = initializeSparkContext("UnsupervisedLearning")
        val configs: utils.ConfigParser = new ConfigParser(args)
        val unsupervisedWorkflowInput: UnsupervisedWorkflowInput = parseJson(configs.jsonFile)
        val config: CompositeConfiguration = new CompositeConfiguration()
        config.addConfiguration(new PropertiesConfiguration(configs.confFile))

        // create Model Storage Syncer
        ModelStorageSyncer.setSyncer(new ModelStorageSyncer(
            projectConfig = NewOrExistingProject(
                config.getString(InputConfigs.projectNameConf, "Demo"),
                config.getString(InputConfigs.userNameConf, "yizhouyan"),
                config.getString(InputConfigs.projectDescConf,
                    "Project to hold all models from the demo")
            ),
            experimentRunConfig = new NewExperimentRun
        ))

        val saveToDB: Boolean = config.getBoolean(InputConfigs.saveToDBConf, false)
        val finalOutputPath: String = unsupervisedWorkflowInput.finalOutputPath match {
            case Some(x) => x
            case None => getRandomFilePath(InputConfigs.outputPathPrefixConf, "final_output")
        }
        val runExplanations: Boolean = unsupervisedWorkflowInput.runExplanations
        implicit val sharedParams:SharedParams = SharedParams(saveToDB,
            runExplanations, finalOutputPath, new ColumnTracking)

        // read data from training

        /* .data._1: DataFrame
         * .data._2: (initDataRangeList, IFkrange, pruneDataIndexRange)
         */
        val WorkflowInput_df = FetchDataExample.fetch(unsupervisedWorkflowInput.data)
        val data = WorkflowInput_df._1
        sharedParams.numPartitions = unsupervisedWorkflowInput.numPartitions match{
            case Some(x) => x
            case None =>{
                math.ceil(data.count()/5000.0).toInt
            }
        }
//        // execute pipeline stages
//        Pipelines.transform(
//            data,
//            unsupervisedWorkflowInput.pipelines
//        )

        // execute autoOD
        if (unsupervisedWorkflowInput.autoOD)
          AutoODFunction.autoOD(WorkflowInput_df)

        spark.stop()
    }

    private def parseJson(jsonPath: String): UnsupervisedWorkflowInput = {
        val source: String = Source.fromFile(jsonPath).getLines.mkString
        val jsonAst = source.parseJson // or JsonParser(source)
        jsonAst.convertTo[UnsupervisedWorkflowInput]
    }

//
//    def main(args: Array[String]): Unit = {
//      implicit val spark: SparkSession = initializeSparkContext("LOF_test")
//      implicit val sharedParams: SharedParams = SharedParams(false, false,
//        "../../results/lofTest", new ColumnTracking)
//
//      val path = "./data/Pima_withoutdupl_norm_35.csv"
//      val tmpdf: DataFrame = spark.read.format("csv").option("header", "true").load(path)
//
//      val columnsCast = col(tmpdf.columns.head) +: tmpdf.columns.tail.map(name => col(name).cast("double"))
//      val df = tmpdf.select(columnsCast :_*)
//
//      val tmp_lof_krange: List[Int] = List.tabulate(10)(n => 10 + 10 * n)
//      val lof_krange: List[Int] = tmp_lof_krange ++ tmp_lof_krange ++ tmp_lof_krange ++
//        tmp_lof_krange ++ tmp_lof_krange ++ tmp_lof_krange
//
//      val featurelist = List("att1", "att2", "att3", "att4", "att5", "att6", "att7", "att8")
//      val featurelist_scaled = List("att1_scaled", "att2_scaled", "att3_scaled", "att4_scaled",
//        "att5_scaled", "att6_scaled", "att7_scaled", "att8_scaled")
//
//      // Standard
//      val standardScalerParam = StandardScalerParams(Some(featurelist), None)
//      val StandardScalerModel = new StandardScaler(standardScalerParam, -1)
//      val data = StandardScalerModel.transform(df, -1, None)
//
//      data.printSchema()
//
//      // LOF
//      val sub = SubspaceParams(10, 15, 2, useFullSpace = true, 12)
////        val lofParams = LOFParams(tmp_lof_krange, "lof_result", false, Some(sub), Some(featurelist))
//      val lofParams = LOFParams(List(10), "lof_result", useSubspace = false, None, Some(featurelist_scaled))
//      val LOF_estimator = new LOF(lofParams, -1)
//      val lof_res = LOF_estimator.transform(data, -1)
//
//      lof_res.printSchema()
//      lof_res.select("lof_result_subspace_0_k_10").show(5)
//
//      spark.stop()
//    }

//    def main(args: Array[String]): Unit = {
//        implicit val spark: SparkSession = initializeSparkContext("LOF_test")
//
//        val path = "./data/pimaTest_norm35.csv"
//        val tmpdf: DataFrame = spark.read.format("csv").option("header", "true").load(path)
//        val columnsCast = col("id") +: tmpdf.columns.tail.map(name => col(name).cast("double"))
//        val X = tmpdf.select(columnsCast: _*)
////        SVM(X, X)
//        import model.pipelines.tools.ToolsModel.SVM
//        val predictionSVM = SVM(X, X)
//        predictionSVM.select("score","prediction", "probability").show(5)
//    }
}
