package edu.ucr.cs.bdlab.raptor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.ucr.cs.bdlab.beast.common.BeastOptions;
import edu.ucr.cs.bdlab.beast.geolite.IFeature;
import edu.ucr.cs.bdlab.beast.indexing.RTreeFeatureReader;
import edu.ucr.cs.bdlab.beast.io.GeoJSONFeatureWriter;
import edu.ucr.cs.bdlab.beast.io.SpatialFileRDD;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.zip.GZIPOutputStream;

public class VectorFeaturesServlet extends HttpServlet {

    Configuration hadoopConf = new Configuration();

    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        // time at start of GET request
        long t1 = System.nanoTime();

        boolean gzipResponse = isGZIPAcceptable(request);

        // we set content-type as application/geo+json
        // not application/json
        response.setContentType("application/geo+json");

        String path = request.getRequestURI();

        // set Access-Control-Allow-Origin
        // otherwise, the front-end won't be able to make GET requests to this server because of CORS policy
        response.addHeader("Access-Control-Allow-Origin", "*");

        // Extract the ID from the path
        String datasetID = path.replaceAll(".*/vectors/(.*)\\.geojson", "$1");

        // Locate the index path
        String dataIndexPath = getIndexPathById(VectorInfoServlet.VectorIndexFile, datasetID);
        if (dataIndexPath == null) {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            PrintWriter writer = response.getWriter();
            writer.printf("{'message': 'Cannot find dataset %s'}", datasetID);
            return;
        }

        response.setStatus(HttpServletResponse.SC_OK);
        // Load the Farmland features
        RTreeFeatureReader reader = new RTreeFeatureReader();
        Path indexPath = new Path(dataIndexPath);

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

        if (gzipResponse)
            response.setHeader("Content-Encoding", "gzip");

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

    public static String getIndexPathById(File filePath, String id) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode rootNode = objectMapper.readTree(filePath);

        if (rootNode.isArray()) {
            Iterator<JsonNode> elements = rootNode.elements();
            while (elements.hasNext()) {
                JsonNode element = elements.next();
                if (element.get("id").asText().equals(id)) {
                    return element.get("index_path").asText();
                }
            }
        }
        return null; // ID not found
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