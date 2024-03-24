package edu.ucr.cs.bdlab.raptor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import edu.ucr.cs.bdlab.beast.JavaSpatialSparkContext;
import edu.ucr.cs.bdlab.beast.common.BeastOptions;
import edu.ucr.cs.bdlab.davinci.DaVinciServer;
import edu.ucr.cs.bdlab.davinci.MultilevelPyramidPlotHelper;
import edu.ucr.cs.bdlab.davinci.TileIndex;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.imageio.ImageIO;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.awt.image.BufferedImage;
import java.io.*;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Returns a PNG tile for the Farmland dataset
 */
public class VectorTileServlet extends HttpServlet {
  // spark context
  private JavaSpatialSparkContext jssc;

  private DaVinciServer daVinciServer;

  // constructor
  public VectorTileServlet() {
    // get or create spark context
    SparkConnector sparkconnector = SparkConnector.getInstance();
    jssc = new JavaSpatialSparkContext(sparkconnector.getSC());

    daVinciServer = new DaVinciServer();
    daVinciServer.setup(jssc._sc(), new BeastOptions());
  }

  protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
    // set Access-Control-Allow-Origin
    // otherwise, the front-end won't be able to make GET requests to this server because of CORS policy
    response.addHeader("Access-Control-Allow-Origin", "*");

    // Locate the image name
    String uri = request.getRequestURI();
    Pattern pattern = Pattern.compile("/vectors/([^/]+)/tile-(\\d+)-(\\d+)-(\\d+)\\.(.*)");
    Matcher matcher = pattern.matcher(uri);

    if (!matcher.find()) {
      response.setStatus(HttpServletResponse.SC_NOT_FOUND);
      return;
    }
    String datasetID = matcher.group(1);
    int z = Integer.parseInt(matcher.group(2));
    int x = Integer.parseInt(matcher.group(3));
    int y = Integer.parseInt(matcher.group(4));
    String ext = matcher.group(5);

    String vizPath = getVizPathById(VectorInfoServlet.VectorIndexFile, datasetID);
    if (vizPath == null) {
      response.setStatus(HttpServletResponse.SC_NOT_FOUND);
      return;
    }

    String redirectedPath = String.format("/dynamic/visualize.cgi/%s/tile-%d-%d-%d.%s", vizPath, z, x, y, ext);

    boolean handled = daVinciServer.getTile(uri, request, response, vizPath, z, x, y, ext);
  }

  public static String getVizPathById(File filePath, String id) throws IOException {
    ObjectMapper objectMapper = new ObjectMapper();
    JsonNode rootNode = objectMapper.readTree(filePath);

    if (rootNode.isArray()) {
      Iterator<JsonNode> elements = rootNode.elements();
      while (elements.hasNext()) {
        JsonNode element = elements.next();
        if (element.get("id").asText().equals(id)) {
          return element.get("viz_path").asText();
        }
      }
    }
    return null; // ID not found
  }
}