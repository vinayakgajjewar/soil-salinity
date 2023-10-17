package edu.ucr.cs.bdlab.raptor;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import edu.ucr.cs.bdlab.beast.JavaSpatialSparkContext;
import edu.ucr.cs.bdlab.davinci.MultilevelPyramidPlotHelper;
import edu.ucr.cs.bdlab.davinci.TileIndex;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;

import javax.imageio.ImageIO;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Returns a PNG tile for the Farmland dataset
 */
public class TileServlet extends HttpServlet {

  /** A server-side cache for copies of generated tiles to avoid regenerating the same tile for multiple users. */
  private Cache<Long, byte[]> cache;

  /** An empty image to return on failures */
  private static final byte[] EmptyImage;

  static {
    try {
      BufferedImage img = new BufferedImage(1, 1, BufferedImage.TYPE_INT_ARGB);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ImageIO.write(img, "png", baos);
      EmptyImage = baos.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException("Error creating empty image", e);
    }
  }

  // spark context
  private JavaSpatialSparkContext jssc;

  // constructor
  public TileServlet() {
    // get or create spark context
    SparkConnector sparkconnector = SparkConnector.getInstance();
    jssc = new JavaSpatialSparkContext(sparkconnector.getSC());

    cache = CacheBuilder.newBuilder()
        .maximumSize(100000) // the cache size is 100,000 tiles
        .expireAfterAccess(30, TimeUnit.DAYS)
        .build();
  }

  protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
    // set Access-Control-Allow-Origin
    // otherwise, the front-end won't be able to make GET requests to this server because of CORS policy
    response.addHeader("Access-Control-Allow-Origin", "*");

    // Locate the image name
    String path = request.getServletPath();
    if (!path.contains("tile-")) {
      response.setStatus(HttpServletResponse.SC_NOT_FOUND);
      return;
    }
    // Set the response type to png
    response.setContentType("image/png");
    response.setStatus(HttpServletResponse.SC_OK);
    String tileFileName = path.substring(path.indexOf("tile-"));
    Path vizIndexPath = new Path("data/CA_farmland/CA_farmland_viz");
    // Check if the file already exists, i.e., pre-rendered
    Path pngPath = new Path(vizIndexPath, tileFileName);
    FileSystem fs = pngPath.getFileSystem(jssc.hadoopConfiguration());
    if (fs.exists(pngPath)) {
      // Directly serve the PNG image
      InputStream in = fs.open(pngPath);
      OutputStream out = response.getOutputStream();
      IOUtils.copy(in, out);
      in.close();
      out.close();
    } else {
      // Compute and cache
      String[] tileParts = path.substring(path.indexOf("tile-")).split("[-.]");
      int z = Integer.parseInt(tileParts[1]);
      int x = Integer.parseInt(tileParts[2]);
      int y = Integer.parseInt(tileParts[3]);
      long tileID = TileIndex.encode(z, x, y);
      try {
        byte[] imageData = cache.get(tileID, () -> {
          // Create the intermediate output stream where the image will be written
          ByteArrayOutputStream interimOutput = new ByteArrayOutputStream();
          // Call the function that plots the tile from the raw data
          MultilevelPyramidPlotHelper.plotTile(fs, vizIndexPath, tileID, interimOutput, new LongWritable());
          interimOutput.close();
          return interimOutput.toByteArray();
        });
        OutputStream out = response.getOutputStream();
        out.write(imageData);
        out.close();
      } catch (ExecutionException e) {
        // Return an empty image
        OutputStream out = response.getOutputStream();
        out.write(EmptyImage);
        out.close();
      }
    }
  }
}