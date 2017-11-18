# biggis-landuse
Land use update detection based on Geotrellis and Spark

# Quick and dirty usage example

``` sh
# first, we compile everything and produce a fat jar
# which contains all the dependences
mvn package

# now we can run the example app
java -cp target/biggis-landuse-0.0.5-SNAPSHOT.jar \
  biggis.landuse.spark.examples.GeotiffToPyramid \
  /path/to/raster.tif \
  new_layer_name \
  /path/to/catalog-dir
```

# GettingStarted Example
Code for this example is located inside `src/main/scala/biggis.landuse.spark.examples/GettingStarted.scala`

```
# based on https://github.com/geotrellis/geotrellis-landsat-tutorial
# download examples from geotrellis-landsat-tutorial
# to data/geotrellis-landsat-tutorial
wget http://landsat-pds.s3.amazonaws.com/L8/107/035/LC81070352015218LGN00/LC81070352015218LGN00_B3.TIF
wget http://landsat-pds.s3.amazonaws.com/L8/107/035/LC81070352015218LGN00/LC81070352015218LGN00_B4.TIF
wget http://landsat-pds.s3.amazonaws.com/L8/107/035/LC81070352015218LGN00/LC81070352015218LGN00_B5.TIF
wget http://landsat-pds.s3.amazonaws.com/L8/107/035/LC81070352015218LGN00/LC81070352015218LGN00_BQA.TIF
wget http://landsat-pds.s3.amazonaws.com/L8/107/035/LC81070352015218LGN00/LC81070352015218LGN00_MTL.txt
```

# Using an IDE
We strongly recommend using an IDE for Scala development,
in particular IntelliJ IDEA which has a better support
for Scala than Eclipse

For IDE Builds please select *Maven Profile* **IDE** before running to avoid using *scope provided* (necessary for cluster builds)

[biggis-spark]: https://github.com/biggis-project/biggis-spark

Since Geotrellis uses Apache Spark for processing, we need to set the `spark.master` property first.
- For local debugging, the easiest option is to set the VM command line argument to `-Dspark.master=local[*]`.
- Other option for local debugging, that is closer to a cluster setup is to run geotrellis in a docker container as implemented in [biggis-spark]. In this case, use `-Dspark.master=spark://localhost:7077`
- Third option is to use a real cluster which can run on the same docker-based infrastructure from [biggis-spark]

Geotrellis always work with a "catalog" which is basically a directory either in local filesystem or in HDFS.
You might want to use `target/geotrellis-catalog` during development. This way, the catalog will be deleted when running `mvn clean` and won't be included into git repository.
