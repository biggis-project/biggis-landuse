### Usage:
```
docker build -t biggis/biggis-landuse:0.0.2-SNAPSHOT .
```

### Check:
```
docker images
```

### Archive (tar):
```
docker save biggis/biggis-landuse:0.0.2-SNAPSHOT -o biggis-landuse-0.0.2-SNAPSHOT.tar
```

### Start (e.g. GettingStarted Example):
```
docker run -t biggis/biggis-landuse:0.0.2-SNAPSHOT java "-Dspark.master=local[*]" -Xmx2g -cp /biggis-landuse/target/biggis-landuse-0.0.2-SNAPSHOT.jar biggis.landuse.spark.examples.GettingStarted /biggis-landuse/target/geotrellis-catalog
```
