/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.breastcancerprediction;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.feature.VectorIndexer;
import org.apache.spark.ml.feature.VectorIndexerModel;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 *
 * @author Sayed
 */
public class breastcancer {

    public static class cancerobservation {

        private String ClumpThickness;
        private String UniformityofCellSize;
        private String UniformityofCellShape;
        private String MarginalAdhesion;
        private String SingleEpithelialCellSize;
        private String BareNuclei;
        private String BlandChromatin;
        private String NormalNucleoli;
        private String Mitoses;
        private String classlabel;

        public String getClumpThickness() {
            return ClumpThickness;
        }

        public void setClumpThickness(String ClumpThickness) {
            this.ClumpThickness = ClumpThickness;
        }

        public String getUniformityofCellSize() {
            return UniformityofCellSize;
        }

        public void setUniformityofCellSize(String UniformityofCellSize) {
            this.UniformityofCellSize = UniformityofCellSize;
        }

        public String getUniformityofCellShape() {
            return UniformityofCellShape;
        }

        public void setUniformityofCellShape(String UniformityofCellShape) {
            this.UniformityofCellShape = UniformityofCellShape;
        }

        public String getMarginalAdhesion() {
            return MarginalAdhesion;
        }

        public void setMarginalAdhesion(String MarginalAdhesion) {
            this.MarginalAdhesion = MarginalAdhesion;
        }

        public String getSingleEpithelialCellSize() {
            return SingleEpithelialCellSize;
        }

        public void setSingleEpithelialCellSize(String SingleEpithelialCellSize) {
            this.SingleEpithelialCellSize = SingleEpithelialCellSize;
        }

        public String getBareNuclei() {
            return BareNuclei;
        }

        public void setBareNuclei(String BareNuclei) {
            this.BareNuclei = BareNuclei;
        }

        public String getBlandChromatin() {
            return BlandChromatin;
        }

        public void setBlandChromatin(String BlandChromatin) {
            this.BlandChromatin = BlandChromatin;
        }

        public String getNormalNucleoli() {
            return NormalNucleoli;
        }

        public void setNormalNucleoli(String NormalNucleoli) {
            this.NormalNucleoli = NormalNucleoli;
        }

        public String getMitoses() {
            return Mitoses;
        }

        public void setMitoses(String Mitoses) {
            this.Mitoses = Mitoses;
        }

        public String getClasslabel() {
            return classlabel;
        }

        public void setClasslabel(String classlabel) {
            this.classlabel = classlabel;
        }

    }

    public static void main(String[] args) throws AnalysisException {
        SparkSession session = SparkSession.builder().appName("breast prediction").master("local[*]").config("spark.sql.warehouse.dir", "file:///E://").getOrCreate();
       
        Dataset<Row> data = session.read()
                .option("header","true")  .csv("C://Users//Sayed//Documents//NetBeansProjects//breast-cancer-wisconsin-data-raw.csv");
        data.show();
        JavaRDD<cancerobservation> data1 = data.toJavaRDD().map(new Function<Row, cancerobservation>() {
            @Override
            public cancerobservation call(Row t1) throws Exception {
                // throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
                cancerobservation cr = new cancerobservation();
                cr.setClumpThickness(t1.getString(1));
                cr.setUniformityofCellShape(t1.getString(2));
                cr.setUniformityofCellSize(t1.getString(3));
                cr.setMarginalAdhesion(t1.getString(4));
                cr.setSingleEpithelialCellSize(t1.getString(5));
                cr.setBareNuclei(t1.getString(6));
                cr.setBlandChromatin(t1.getString(7));
                cr.setNormalNucleoli(t1.getString(8));
                cr.setMitoses(t1.getString(9));
                cr.setClasslabel(t1.getString(10));
                return cr;
            }
        });
        Dataset<Row> data2 = session.createDataFrame(data1, cancerobservation.class);
        data2.show();
        
        //data2.write().csv("F:\\withco");
// string indexer
        StringIndexerModel indexer1 = new StringIndexer().setInputCol("clumpThickness").
                setOutputCol("oclumpThickness").fit(data2);
        StringIndexerModel indexer2 = new StringIndexer().setInputCol("uniformityofCellShape").
                setOutputCol("oUniformityofCellShape").fit(data2);
        StringIndexerModel indexer3 = new StringIndexer().setInputCol("uniformityofCellSize").
                setOutputCol("oUniformityofCellSize").fit(data2);
        StringIndexerModel indexer4 = new StringIndexer().setInputCol("marginalAdhesion").
                setOutputCol("oMarginalAdhesion").fit(data2);
        StringIndexerModel indexer5 = new StringIndexer().setInputCol("singleEpithelialCellSize").
                setOutputCol("oSingleEpithelialCellSize").fit(data2);
        StringIndexerModel indexer6 = new StringIndexer().setInputCol("bareNuclei").
                setOutputCol("oBareNuclei").fit(data2);
        StringIndexerModel indexer7 = new StringIndexer().setInputCol("blandChromatin").
                setOutputCol("oBlandChromatin").fit(data2);
        StringIndexerModel indexer8 = new StringIndexer().setInputCol("normalNucleoli").
                setOutputCol("oNormalNucleoli").fit(data2);
        StringIndexerModel indexer9 = new StringIndexer().setInputCol("mitoses").
                setOutputCol("oMitoses").fit(data2);
        StringIndexerModel indexer10 = new StringIndexer().setInputCol("classlabel").
                setOutputCol("oClasslabel").fit(data2);
        // pipline
        Pipeline pipeline1 = new Pipeline()
                .setStages(new PipelineStage[]{indexer1, indexer2, indexer3, indexer4, indexer5, indexer6, indexer7, indexer8, indexer9,indexer10});
        Dataset<Row> data3 = pipeline1.fit(data2).transform(data2);
        data3.show();
        // vector assembler
        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(new String[]{"oclumpThickness", "oUniformityofCellShape", "oUniformityofCellSize", "oMarginalAdhesion", "oSingleEpithelialCellSize",
                    "oBareNuclei", "oBlandChromatin", "oNormalNucleoli", "oMitoses"})
                .setOutputCol("features");
        Dataset<Row> featuredata = assembler.transform(data3);
        
        VectorIndexerModel featureIndexer = new VectorIndexer()
                .setInputCol("features")
                .setOutputCol("indexedFeatures")
                .setMaxCategories(4) // features with > 4 distinct values are treated as continuous.
                .fit(featuredata);
        
        Dataset<Row>[] splits = featuredata.randomSplit(new double[]{0.8, 0.2});
        Dataset<Row> trainingData = splits[0];
        Dataset<Row> testData = splits[1];
        DecisionTreeClassifier dt = new DecisionTreeClassifier()
                .setLabelCol("oClasslabel")
                .setFeaturesCol("indexedFeatures");
        Pipeline pipeline2 = new Pipeline()
                .setStages(new PipelineStage[]{featureIndexer ,dt});
        PipelineModel pm = pipeline2.fit(trainingData);
        Dataset<Row> prediction = pm.transform(testData);
        prediction.show();
        prediction.select("indexedFeatures", "oclasslabel");//.write().json("F:\\testdatan");
    }

}
