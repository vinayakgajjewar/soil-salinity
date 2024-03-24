# FutureFarmNow API Documentation
Version: 0.2-RC1

All the following end-points are hosted under the URL `https://raptor.cs.ucr.edu/futurefarmnow-backend-<VERSION>`. The current version is shown at the top of this document. All the following endpoints should be preceded with the base URL given above.

## List vector products
Lists all the vector datasets available on the server.

| End Point   | `/vectors.json` |
|-------------|-----------------|
| HTTP Method | GET             |

*Result:*
```json
{
  "vectors": [
    {
      "id": "farmland",
      "title": "California Farmlands",
      "description": "All farmlands in California"
    }, {
      ...
    }
  ]
}
```

## Retrieve vector dataset
Returns the full vector dataset or a subset of it defined by a region in the GeoJSON file format.


| End Point   | `/vectors/<id>.geojson` |
|-------------|-------------------------|
| HTTP method | GET                     |

### Parameters

| Parameter | Required? | How to use | Description                              |
|-----------|-----------|------------|------------------------------------------|
| id        | required  | URL        | The ID of the vector dataset to retrieve | 
| minx      | optional  | ?minx=     | West edge longitude                      |
| miny      | optional  | ?miny=     | South edge latitude                      |
| maxx      | optional  | ?maxx=     | East edge longitude                      |
| maxy      | optional  | ?maxy=     | North edge latitude                      |

Note: if MBR is not specified, the whole dataset will be returned as GeoJSON.

### Examples
#### URL
<http://raptor.cs.ucr.edu/futurefarmnow-backend-0.2-RC1/vectors/farmland.geojson?minx=-120.1&miny=40&maxx=-120&maxy=40.01>

#### Response
```json
{
  "type" : "FeatureCollection",
  "features" : [ {
    "type" : "Feature",
    "properties" : {
      "OBJECTID" : 11327,
      "Crop2014" : "Alfalfa and Alfalfa Mixtures",
      "Acres" : 5.834048494999999E-14,
      "County" : "Lassen",
      "Source" : "Land IQ, LLC",
      "Modified_B" : "Zhongwu Wang",
      "Date_Data_" : "July, 2014",
      "Last_Modif" : "2017-05-07T00:00:00.000Z",
      "DWR_Standa" : "P | PASTURE",
      "GlobalID" : "{332F0E0D-503F-4D89-9B9B-9617134F904A}"
    },
    "geometry" : {
      "type" : "Polygon",
      "coordinates" : [<coordinate array>]
    }
  }]
}
```

## Retrieve vector dataset Tile
Retrieves a tile visualization for the entire Farmland dataset.

| Endpoint    | `/vectors/<id>/tile-z-x-y.png`                                 |
|-------------|-----------------------------------------------------------------|
| HTTP method | GET                                                             |
| Since       | 0.1.1                                                           |

### Examples
#### URL
<http://raptor.cs.ucr.edu/futurefarmnow-backend-0.2-RC1/vectors/farmland/tile-1-0-0.png>

#### Response
![Tile 1,0,0](images/tile-1-0-0.png)

## Get soil statistics for a single farmland

Get soil statistics for a single geometry defined by GeoJSON. The output is in JSON format.

| Endpoint     | `/soil/singlepolygon.json` |
|--------------|----------------------------|
| HTTP method  | GET/POST                   |
| POST payload | GeoJSON geometry object    |

| Parameter | Required? | How to use                                                                                                           | Description                           |
|-----------|-----------|----------------------------------------------------------------------------------------------------------------------|---------------------------------------|
| soildepth | Required  | ?soildepth=from-to                                                                                                   | Soil depth on which to run statistics |
| layer     | Required  | Accepted values: "alpha", "bd", "clay", "hb", "ksat", "lambda", "n", "om", "ph", "sand", "silt","theta_r", "theta_s" | Soil parameter to consider            |

*Note*: GET is the preferred HTTP method for this service since it does not modify the state of the server and its caching will be handled better by the client. However, since some SDKs do not support a payload for GET requests, the POST method is also supported for compatibility.

*Note*: The parameter `soildepth` is provided as a range in the format `from-to`.
Currently, the accepted values for from and to are {0, 5, 15, 30, 60, 100, 200} and `from` must be less than `to`.

### Examples
#### URL
<https://raptor.cs.ucr.edu/futurefarmnow-backend-0.2-RC1/soil/singlepolygon.json?soildepth=0-5&layer=lambda>

#### GET/POST payload
```json
{"type" : "Polygon",  "coordinates" : [ [ [ -120.11975251694177, 36.90564006418889 ], [ -120.12409234994458, 36.90565751854381 ], [ -120.12406217104261, 36.90824957916899 ], [ -120.12410082465097, 36.90918197014845 ], [ -120.12405123315573, 36.90918899854245 ], [ -120.11974725371255, 36.9091820470047 ], [ -120.11975251694177, 36.90564006418889 ] ] ] }
```

#### Example with cUrl
```shell
cat > test.geojson
{"type" : "Polygon",  "coordinates" : [ [ [ -120.11975251694177, 36.90564006418889 ], [ -120.12409234994458, 36.90565751854381 ], [ -120.12406217104261, 36.90824957916899 ], [ -120.12410082465097, 36.90918197014845 ], [ -120.12405123315573, 36.90918899854245 ], [ -120.11874725371255, 36.9091820470047 ], [ -120.11975251694177, 36.90564006418889 ] ] ]  }
^D
curl -X GET "http://raptor.cs.ucr.edu/futurefarmnow-backend-0.2-RC1/soil/singlepolygon.json?soildepth=0-5&layer=lambda" -H "Content-Type: application/geo+json" -d @test.geojson
```

#### Response
```json
{"query":{"soildepth":"0-5","layer":"alpha"},"results":{"max":-0.24714702,"min":-0.25878787,"sum":-0.24714702,"median":-48.268135,"stddev":-0.25208578,"count":195,"mean":-0.2475289}}
```

## Get soil statistics for all farmlands in a region
Get computed soil statistics for selected vector products in JSON format

| Endpoint    | `/soil/<id>.json` |
|-------------|--------------------|
| HTTP method | GET                |

| Parameter | Required? | How to use                                                                                                           | Description                          |
|-----------|-----------|----------------------------------------------------------------------------------------------------------------------|--------------------------------------|
| id        | Required  | URL                                                                                                                  | The ID of the vector dataset         |
| minx      | Optional  | ?minx=                                                                                                               | West edge longitude value            |
| miny      | Optional  | ?miny=                                                                                                               | South edge latitude value            |
| maxx      | Optional  | ?maxx=                                                                                                               | East edge longitude value            |
| maxy      | Optional  | ?maxy=                                                                                                               | North edge latitude value            |
| soildepth | Required  | from-to                                                                                                              | Soil depth to compute statistics for |
| layer     | Required  | Accepted values: "alpha", "bd", "clay", "hb", "ksat", "lambda", "n", "om", "ph", "sand", "silt","theta_r", "theta_s" | Soil parameter to consider           |

*Note*: The parameter `soildepth` is provided as a range in the format `from-to`.
Currently, the accepted values for from and to are {0, 5, 15, 30, 60, 100, 200} and `from` must be less than `to`.

### Examples
#### URL
<http://raptor.cs.ucr.edu/futurefarmnow-backend-0.2-RC1/soil/farmland.json?minx=-127.8&miny=29.8&maxx=-115.6&maxy=33.7&soildepth=0-5&layer=ph>

#### Response
```json
{
  "query":{
    "soildepth":"0-5",
    "layer":"ph",
    "mbr":{
      "minx":-127.8,
      "miny":29.8,
      "maxx":-112.6,
      "maxy":42.7
    }
  },
  "results":[{
    "objectid":"41176",
    "value":6.1090255
  },
    ...
  ]
}
```
