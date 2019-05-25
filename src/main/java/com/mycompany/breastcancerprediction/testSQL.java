/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.breastcancerprediction;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 *
 * @author Sayed
 */
public class testSQL {

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
        Dataset<Row> data = session.read().csv("F:\\faculty 2017\\selected 4 -big data\\breast-cancer-wisconsin.data");
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
        data2.createTempView("mydata");
        Dataset<Row> data3 = session.sql("select classlabel from mydata");
        data3.show();
    }
}
