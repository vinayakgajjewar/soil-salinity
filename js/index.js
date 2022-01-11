// elements that make up the popup
const container = document.getElementById("popup");
const content = document.getElementById("popup-content");
const closer = document.getElementById("popup-closer");

// create overlay to anchor popup to map
const overlay = new ol.Overlay({
  element: container,
  autoPan: true,
  autoPanAnimation: {
    duration: 250,
  }
});

// add click handler to hide popup
closer.onclick = function () {
  overlay.setPosition(undefined);
  closer.blur();
  return false;
}

var style = new ol.style.Style({
  image: new ol.style.Icon({
    scale: 0.02,
    src: "data/media/new_wildfire_icon.png"
  })
});

// style object for polygons
var polygonStyle = new ol.style.Style({
  stroke: new ol.style.Stroke({
    color: "ff00ff",
    width: 1
  })
});

function makeDynamicGETRequest(map) {

  // base URL
  var baseURL = "http://localhost:8080/raptor-backend-0.1-SNAPSHOT/vectors/states.geojson";

  // extents
  minx = map.getView().calculateExtent()[0];
  miny = map.getView().calculateExtent()[1];
  maxx = map.getView().calculateExtent()[2];
  maxy = map.getView().calculateExtent()[3];

  // generate url with extents parameters
  // we use ol.proj.toLonLat() to convert from pixel coordinates to longitude/latitude
  var url = baseURL + "?minx=" + ol.proj.toLonLat([minx, miny])[0] + "&miny=" + ol.proj.toLonLat([minx, miny])[1] + "&maxx=" + ol.proj.toLonLat([maxx, maxy])[0] + "&maxy=" + ol.proj.toLonLat([maxx, maxy])[1];

  // iterate through all layers in map
  // when we find our vector layer, set its url to updated url with extents parameters
  map.getLayers().forEach(function (layer) {
    if (layer instanceof ol.layer.Vector) {
      layer.getSource().setUrl(url);
      layer.getSource().refresh();
    }
  });
}

$(document).ready(function () {

  // create map
  var map = new ol.Map({
    target: "map",
    layers: [

      // OpenStreetMap
      new ol.layer.Tile({
        source: new ol.source.OSM()
      }),

      // multilevel visualization
      new ol.layer.Tile({
        source: new ol.source.XYZ({
          url: "data/multilevel/wildfire_visualization_4326_reversed/tile-{z}-{x}-{y}.png",
          tileSize: [256, 256],
          attributions: '<a href="https://davinci.cs.ucr.edu">&copy;DaVinci</a>'
        }),
        maxZoom: 12
      }),

      // empty vector layer
      new ol.layer.Vector({
        minZoom: 12,
        source: new ol.source.Vector({
          format: new ol.format.GeoJSON({featureProjection: "EPSG:3857"})
        }),
        style: function(feature) {
          return style;
        }
      })
    ],
    overlays: [overlay],
    view: new ol.View({
      center: [-13694686.259677762, 4715193.587946976],
      zoom: 6
    })
  });

  //makeDynamicGETRequest(map);

  // add hover handler to render popup
  map.on("pointermove", function (evt) {

    var p = evt.pixel;
    var feature = map.forEachFeatureAtPixel(p, function(feature) {
      return feature;
    });
    if (feature) {

      // if we're hovering over a feature, display feature information
      let popupContent = `
        acq_date: ${feature.get("acq_date")}
        <br>
        frp: ${feature.get("frp")}
        <br>
        TEMP_ave: ${feature.get("TEMP_ave")}
        <br>
        WSPD_ave: ${feature.get("WSPD_ave")}
      `;
      content.innerHTML = popupContent;

      // set pos of overlay at click coordinate
      const coordinate = evt.coordinate;
      overlay.setPosition(coordinate);

    } else {
      //overlay.setPosition(undefined);
      //closer.blur();
    }
  });

  // on singleclick, display current feature info at bottom of map
  map.on("singleclick", function (evt) {
    var p = evt.pixel;
    console.log(evt.coordinate);
    var feature = map.forEachFeatureAtPixel(p, function(feature) {
      return feature;
    });
    if (feature) {
      // if we're clicking on a feature, display more info on the side
      document.getElementById("acq_date").innerHTML = feature.get("acq_date");
      document.getElementById("acq_time").innerHTML = feature.get("acq_time");
      document.getElementById("frp").innerHTML = feature.get("frp");
      document.getElementById("TEMP_ave").innerHTML = feature.get("TEMP_ave");
      document.getElementById("TEMP_min").innerHTML = feature.get("TEMP_min");
      document.getElementById("TEMP_max").innerHTML = feature.get("TEMP_max");
      document.getElementById("WSPD_ave").innerHTML = feature.get("WSPD_ave");
    }
  });

  // make dynamic GET request at end of map move event
  // only want to make request if zoom level is high enough
  map.on("moveend", function() {
    if (map.getView().getZoom() >= 12) {
      makeDynamicGETRequest(map);
    }
  });
});