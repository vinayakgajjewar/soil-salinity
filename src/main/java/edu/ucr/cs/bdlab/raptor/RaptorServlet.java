//package org.bdlabucr;
package edu.ucr.cs.bdlab.raptor;


import java.io.IOException;

// import java.util.*; // maps
import java.util.List; // lists
import java.util.ArrayList;
import java.util.Map;

// for some reason, including this import causes the servlet to break :(
//import java.nio.file.Paths; // paths

// jackson library to read/write json files
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.ucr.cs.bdlab.beast.io.GeoJSONFeatureWriter;
import edu.ucr.cs.bdlab.beast.io.SpatialReader;
import edu.ucr.cs.bdlab.beast.JavaSpatialSparkContext;
import edu.ucr.cs.bdlab.beast.common.BeastOptions;
import edu.ucr.cs.bdlab.beast.geolite.IFeature;
import edu.ucr.cs.bdlab.beast.geolite.ITile;
import edu.ucr.cs.bdlab.beast.JavaSpatialRDDHelper;
import edu.ucr.cs.bdlab.raptor.Statistics;
import edu.ucr.cs.bdlab.raptor.HDF4Reader;
import edu.ucr.cs.bdlab.raptor.RaptorMixin;
import edu.ucr.cs.bdlab.raptor.RaptorJoinResult;
import edu.ucr.cs.bdlab.raptor.RaptorJoinFeature;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.api.java.JavaPairRDD;
// https://spark.apache.org/docs/latest/rdd-programming-guide.html
//import org.apache.spark.SparkContext;
//import org.apache.spark.SparkConf;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import scala.Tuple2;
import scala.Tuple4;
import scala.Tuple5;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

public class RaptorServlet extends HttpServlet {

    protected SparkConnector sparkconnector;

    protected JavaSpatialSparkContext jssc;



    protected DBRead dbr;

    public RaptorServlet() {
        System.out.println("----initializing servlet");

        // initialize DB reader
        dbr = DBRead.getInstance();

        // get or create spark context
        sparkconnector = SparkConnector.getInstance();

        jssc = new JavaSpatialSparkContext(sparkconnector.getSC());
    }

    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

        float minx, miny, maxx, maxy;

        // time at start of GET request
        long t1 = System.nanoTime();

        // wrap code in try-catch block to handle case where user accesses endpoint directly in browser
        try {

            minx = Float.parseFloat(request.getParameter("minx"));
            miny = Float.parseFloat(request.getParameter("miny"));
            maxx = Float.parseFloat(request.getParameter("maxx"));
            maxy = Float.parseFloat(request.getParameter("maxy"));

            System.out.println("----minx: " + Float.toString(minx));
            System.out.println("----miny: " + Float.toString(miny));
            System.out.println("----maxx: " + Float.toString(maxx));
            System.out.println("----maxy: " + Float.toString(maxy));

        } catch (java.lang.NullPointerException e) {

            // extents obj isn't given when accessing endpoint via browser
            // so fill in our own values
            minx = -130;
            miny = 32;
            maxx = -115;
            maxy = 45;
        }

        //dbr.read();

        // we set content-type as application/geo+json
        // not application/json
        response.setContentType("application/geo+json");
        response.setStatus(HttpServletResponse.SC_OK);

        // set Access-Control-Allow-Origin
        // otherwise, the front-end won't be able to make GET requests to this server
        // because of CORS policy
        response.addHeader("Access-Control-Allow-Origin", "*");

        // read an example geojson file into a list


        // use csv (slow)
        //JavaRDD<IFeature> records = jssc.readCSVPoint("data/csv/test_wildfire_visualization_4326.csv", "x", "y", '\t', true);
        //JavaRDD<IFeature> records = jssc.readCSVPoint("data/csv/wildfire_visualization_4326_reversed.csv", "x", "y", '\t', true);

        // use rtree (fast)
        //JavaRDD<IFeature> records = jssc.spatialFile("data/rtree/wildfire_index/wildfire_index/", "rtree");

        // load raster data
        JavaRDD<ITile> raster = jssc.geoTiff("data/tif/0_5_compressed/ph.tif", 0, new BeastOptions());

        JavaRDD<IFeature> records = jssc.shapefile("data/shapefile/CA_farmland.zip");
        
        // filter by map extents
        GeometryFactory geometryFactory = new GeometryFactory();
        Geometry extents = geometryFactory.toGeometry(new Envelope(minx, maxx, miny, maxy));
        // take 1000 records maximum
        List<IFeature> filteredRecords = JavaSpatialRDDHelper.rangeQuery(records, extents).take(1000);

        // read geotiff data
        //RaptorMixin.RasterReadMixinFunctions rrmf = RaptorMixin.RasterReadMixinFunctions(JavaSparkContext.toSparkContext(sparkconnector.getSC()));
        //RDD<ITile> raster = rrmf.geoTiff("data/geotiff/0_5_compressed/ph.tif", 0, new BeastOptions());
        //RaptorMixin.RasterReadMixinFunctions rrmf = new RaptorMixin.RasterReadMixinFunctions(JavaSparkContext.toSparkContext(sparkconnector.getSC()));
        //RDD<ITile> raster = rrmf.geoTiff("data/geotiff/0_5_compressed/ph.tif", 0, new BeastOptions());

        System.out.println("----done reading records");

        // run raptor join operation
        JavaRDD<RaptorJoinFeature<Float>> join = JavaSpatialRDDHelper.raptorJoin(jssc.parallelize(filteredRecords), raster, new BeastOptions());

        // aggregate results
        JavaPairRDD<String, Float> aggResults = join.mapToPair(v -> new Tuple2<>(v.feature(), v.m()))
            .reduceByKey(Float::sum)
            .mapToPair(fv -> {
                String name = fv._1().getAs("County");
                float val = fv._2();
                return new Tuple2<>(name, val);
            });

        System.out.println(aggResults.first());
        
        /*
        
        // write output
        System.out.println("Something\tSomething else\n");
        for (Map.Entry<String, Float> result : aggResults.collectAsMap().entrySet()) {
            System.out.printf("%s\t%r\n", result.getKey(), result.getValue());
        }*/

        // try writing out a record
        try (GeoJSONFeatureWriter writer = new GeoJSONFeatureWriter()) {
            //writer.initialize(new GZIPOutputStream(response.getOutputStream()), new Configuration());
            writer.initialize(response.getOutputStream(), new Configuration());
            /*for (int i = 0; i < filteredRecords.size(); i++) {
                writer.write(filteredRecords.get(i));
            }*/

            for (int i = 0; i < filteredRecords.size(); i++) {
                writer.write(filteredRecords.get(i));
            }
            //writer.write(records.get(0));
            //writer.write(records.first());
        } catch (InterruptedException e) {
            System.out.println(e);
        }

        // time at end of GET request
        long t2 = System.nanoTime();

        // print out statistics
        System.out.println("----GET request duration:");
        System.out.println((t2 - t1) * 1e-9);
        System.out.println("----records sent:");
        System.out.println(filteredRecords.size());
    }
}