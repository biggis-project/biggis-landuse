# biggis-landuse
Land use update detection based on Geotrellis and Spark

# Quick and dirty usage example

``` sh
# first, we compile everything and produce a fat jar
# which contains all the dependences
mvn package

# now we can run the example app
java -cp target/biggis-landuse-0.0.2-SNAPSHOT.jar \
  biggis.landuse.spark.examples.GeotiffToPyramid \
  /path/to/raster.tif \
  new_layer_name \
  /path/to/catalog-dir
```

# GettingStarted Example
###### in src/main/scala/biggis.landuse.spark.examples/GettingStarted.scala
##### based on https://github.com/geotrellis/geotrellis-landsat-tutorial
```
# download examples from geotrellis-landsat-tutorial
wget http://landsat-pds.s3.amazonaws.com/L8/107/035/LC81070352015218LGN00/LC81070352015218LGN00_B3.TIF
wget http://landsat-pds.s3.amazonaws.com/L8/107/035/LC81070352015218LGN00/LC81070352015218LGN00_B4.TIF
wget http://landsat-pds.s3.amazonaws.com/L8/107/035/LC81070352015218LGN00/LC81070352015218LGN00_B5.TIF
wget http://landsat-pds.s3.amazonaws.com/L8/107/035/LC81070352015218LGN00/LC81070352015218LGN00_BQA.TIF
wget http://landsat-pds.s3.amazonaws.com/L8/107/035/LC81070352015218LGN00/LC81070352015218LGN00_MTL.txt
# to data/geotrellis-landsat-tutorial
```
##### To Debug in IDE, please set VM options to
  -Dspark.master=local[*]
##### and program arguments to
  target/geotrellis-catalog
#