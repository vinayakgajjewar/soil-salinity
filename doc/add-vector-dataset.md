# Add Vector Product
To add a new vector product to the server, do the following steps.

## Prerequisites
You must have Beast CLI installed

## Index the file
Build an R-tree index on the source file. This should be a single file.
Assuming the input file `CA_farmland.zip` in Shapefile format, you can run the following command:

```shell
beast cat CA_farmland.zip iformat:shapefile CA_farmland.rtree oformat:rtree numpartitions:1
```

You can replace the input file name and format as needed. This will create an output directory `CA_farmland.rtree`
that contains a single file `part-00000.rtree`. Copy this file to the server `data/CA_farmland/index.rtree`.

`cp CA_farmland.rtree/part-00000.rtree data/CA_farmland/index.rtree`

## Visualize the data
Build a visualization index on the indexed data using the following command.

```shell
beast mplot data/CA_farmland/index.rtree iformat:rtree data/CA_farmland/plot.zip -mercator plotter:gplot levels:20
```

## Compute dataset summary
This will help when writing the dataset information.

```shell
beast summary data/CA_farmland/index.rtree iformat:rtree
```

The output will contain the following part:
```json
{
  "extent": {
    "min_coord": [
      -124.38,
      32.59
    ],
    "max_coord": [
      -114.50,
      42.00
    ]
  },
  ...
}
```

## Add dataset info
Edit the file `data/vectors.json` in the server and add an entry with the following template.

```json
{
  "id": "farmland",
  "title": "California Farmlands",
  "description": "All farmlands in California",
  "extents": [-124.38, 32.59, -114.50, 42.00],
  "index_path": "data/CA_farmland/index.rtree",
  "viz_path": "data/CA_farmland/plot.zip"
}
```