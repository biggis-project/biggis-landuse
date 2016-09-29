# biggis-landuse
Land use update detection based on Geotrellis and Spark

# Quick and dirty usage example

``` sh
# first, we compile everything and produce a fat jar
# which contains all the dependences
mvn package

# now we can run the example app
java -cp target/biggis-landuse-0.0.1-SNAPSHOT.jar \
  biggis.landuse.spark.examples.GeotiffToPyramid \
  /path/to/raster.tif \
  new_layer_name \
  /path/to/catalog-dir
```