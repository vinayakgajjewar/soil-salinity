package edu.ucr.cs.bdlab.raptor;

import edu.ucr.cs.bdlab.beast.geolite.GeometryReader;
import edu.ucr.cs.bdlab.beast.indexing.RTreeFeatureReader;
import edu.ucr.cs.bdlab.beast.io.SpatialFileRDD;
import edu.ucr.cs.bdlab.raptor.SingleMachineRaptorJoin;

import java.io.IOException;

import java.util.Enumeration;
import java.util.List; // lists
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import edu.ucr.cs.bdlab.beast.io.GeoJSONFeatureWriter;
import edu.ucr.cs.bdlab.beast.JavaSpatialSparkContext;
import edu.ucr.cs.bdlab.beast.common.BeastOptions;
import edu.ucr.cs.bdlab.beast.geolite.IFeature;
import edu.ucr.cs.bdlab.beast.geolite.ITile;
import edu.ucr.cs.bdlab.beast.JavaSpatialRDDHelper;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
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

    Configuration hadoopConf = new Configuration();

    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        // time at start of GET request
        long t1 = System.nanoTime();

        boolean gzipResponse = isGZIPAcceptable(request);

        // we set content-type as application/geo+json
        // not application/json
        response.setContentType("application/geo+json");
        response.setStatus(HttpServletResponse.SC_OK);
        if (gzipResponse)
            response.setHeader("Content-Encoding", "gzip");

        // set Access-Control-Allow-Origin
        // otherwise, the front-end won't be able to make GET requests to this server because of CORS policy
        response.addHeader("Access-Control-Allow-Origin", "*");

        // Load the Farmland features
        RTreeFeatureReader reader = new RTreeFeatureReader();
        Path indexPath = new Path("data/CA_farmland/CA_farmland.rtree");

        FileSystem fs = indexPath.getFileSystem(hadoopConf);
        long fileLength = fs.getFileStatus(indexPath).getLen();
        FileSplit inputFileSplit = new FileSplit(indexPath, 0, fileLength, null);
        BeastOptions opts = new BeastOptions();
        try {
            // get extents parameters
            double minx = Double.parseDouble(request.getParameter("minx"));
            double miny = Double.parseDouble(request.getParameter("miny"));
            double maxx = Double.parseDouble(request.getParameter("maxx"));
            double maxy = Double.parseDouble(request.getParameter("maxy"));
            opts.set(SpatialFileRDD.FilterMBR(), String.format("%f,%f,%f,%f", minx, miny, maxx, maxy));
        } catch (java.lang.NullPointerException e) {
            // MBR not passed. Use all farmlands
        }
        reader.initialize(inputFileSplit, opts);

        // try writing out a record
        int numRecords = 0;
        try (GeoJSONFeatureWriter writer = new GeoJSONFeatureWriter()) {
            writer.initialize(gzipResponse? new GZIPOutputStream(response.getOutputStream()) : response.getOutputStream(), new Configuration());
            for (IFeature feature : reader) {
                writer.write(feature);
                numRecords++;
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        reader.close();

        // time at end of GET request
        long t2 = System.nanoTime();

        // print out statistics
        System.out.printf("Farmland request wrote %d records in %f seconds\n", numRecords, (t2 - t1) * 1e-9);
    }

    public static boolean isGZIPAcceptable(HttpServletRequest request) {
        boolean serverAcceptGZIP = false;
        Enumeration serverAccepts = request.getHeaders("Accept-Encoding");
        while (serverAccepts.hasMoreElements()) {
            String serverAccept = (String) serverAccepts.nextElement();
            if (serverAccept.toLowerCase().contains("gzip"))
                serverAcceptGZIP = true;
        }
        return serverAcceptGZIP;
    }
}