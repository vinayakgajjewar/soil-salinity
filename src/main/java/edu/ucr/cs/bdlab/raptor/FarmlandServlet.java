package edu.ucr.cs.bdlab.raptor;

import edu.ucr.cs.bdlab.raptor.SingleMachineRaptorJoin;

import java.io.IOException;

import java.util.List; // lists
import java.util.Map;

import edu.ucr.cs.bdlab.beast.io.GeoJSONFeatureWriter;
import edu.ucr.cs.bdlab.beast.JavaSpatialSparkContext;
import edu.ucr.cs.bdlab.beast.common.BeastOptions;
import edu.ucr.cs.bdlab.beast.geolite.IFeature;
import edu.ucr.cs.bdlab.beast.geolite.ITile;
import edu.ucr.cs.bdlab.beast.JavaSpatialRDDHelper;
import org.apache.hadoop.conf.Configuration;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import scala.Tuple2;
import scala.Long;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

public class FarmlandServlet extends HttpServlet {

    protected SparkConnector sparkconnector;

    protected JavaSpatialSparkContext jssc;

    public FarmlandServlet() {
        System.out.println("----initializing farmland servlet");

        // get or create spark context
        sparkconnector = SparkConnector.getInstance();
        jssc = new JavaSpatialSparkContext(sparkconnector.getSC());
    }

    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

        // time at start of GET request
        long t1 = System.nanoTime();

        float minx, miny, maxx, maxy;
        String soilDepth = "";
        String layer = "";
        String agg = "";

        // wrap code in try-catch block to handle case where user accesses endpoint directly in browser
        try {

            // get extents parameters
            minx = Float.parseFloat(request.getParameter("minx"));
            miny = Float.parseFloat(request.getParameter("miny"));
            maxx = Float.parseFloat(request.getParameter("maxx"));
            maxy = Float.parseFloat(request.getParameter("maxy"));

            // get sidebar select parameters
            soilDepth = request.getParameter("soildepth");
            layer = request.getParameter("layer");
            agg = request.getParameter("agg");

            // print parameters
            System.out.println("----minx: " + minx);
            System.out.println("----miny: " + miny);
            System.out.println("----maxx: " + maxx);
            System.out.println("----maxy: " + maxy);
            System.out.println("----soildepth: " + soilDepth);
            System.out.println("----layer: " + layer);
            System.out.println("----agg: " + agg);

        } catch (java.lang.NullPointerException e) {

            // extents obj isn't given when accessing endpoint via browser
            // so fill in our own values
            minx = -130;
            miny = 32;
            maxx = -115;
            maxy = 45;
        }

        // we set content-type as application/geo+json
        // not application/json
        response.setContentType("application/geo+json");
        response.setStatus(HttpServletResponse.SC_OK);

        // set Access-Control-Allow-Origin
        // otherwise, the front-end won't be able to make GET requests to this server
        // because of CORS policy
        response.addHeader("Access-Control-Allow-Origin", "*");

        // load raster data based on selected soil depth and layer
        /*String rasterPath = "data/tif/";
        if (soilDepth.equals("0-5")) {
            rasterPath = rasterPath.concat("0_5_compressed/");
        } else if (soilDepth.equals("5-15")) {
            rasterPath = rasterPath.concat("5_15_compressed/");
        } else if (soilDepth.equals("15-30")) {
            rasterPath = rasterPath.concat("15_30_compressed/");
        }
        rasterPath = rasterPath.concat(layer + ".tif");
        System.out.println("----raster path=" + rasterPath);
        JavaRDD<ITile> raster = jssc.geoTiff(rasterPath, 0, new BeastOptions());
         */

        JavaRDD<IFeature> records = jssc.shapefile("data/shapefile/CA_farmland.zip");

        // filter by map extents
        GeometryFactory geometryFactory = new GeometryFactory();
        Geometry extents = geometryFactory.toGeometry(new Envelope(minx, maxx, miny, maxy));
        // take 1000 records maximum
        List<IFeature> filteredRecords = JavaSpatialRDDHelper.rangeQuery(records, extents).take(1000);

        System.out.println("----done reading records");

        /*
        // run raptor join operation
        JavaRDD<RaptorJoinFeature<Float>> join = JavaSpatialRDDHelper.raptorJoin(jssc.parallelize(filteredRecords), raster, new BeastOptions());

        // aggregate min results
        JavaPairRDD<String, Float> aggMinResults = join.mapToPair(v -> new Tuple2<>(v.feature(), v.m()))
            .reduceByKey(Float::min)
            .mapToPair(fv -> {
                String name = fv._1().getAs("County");
                float val = fv._2();
                return new Tuple2<>(name, val);
            });

        // aggregate max results
        JavaPairRDD<String, Float> aggMaxResults = join.mapToPair(v -> new Tuple2<>(v.feature(), v.m()))
                .reduceByKey(Float::max)
                .mapToPair(fv -> {
                    String name = fv._1().getAs("County");
                    float val = fv._2();
                    return new Tuple2<>(name, val);
                });
        
        // write output
        System.out.println("County\tMin pH\n");
        for (Map.Entry<String, Float> result : aggMinResults.collectAsMap().entrySet()) {
            System.out.printf("%s\t%f\n", result.getKey(), result.getValue());
        }

        System.out.println("County\tMax pH\n");
        for (Map.Entry<String, Float> result : aggMaxResults.collectAsMap().entrySet()) {
            System.out.printf("%s\t%f\n", result.getKey(), result.getValue());
        }
         */


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