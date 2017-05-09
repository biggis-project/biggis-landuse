### Usage (Build):
```
docker build -t biggis/biggis-landuse:0.0.2-SNAPSHOT .
```

### Archive (tar):
```
docker save biggis/biggis-landuse:0.0.2-SNAPSHOT -o biggis-landuse-0.0.2-SNAPSHOT.tar
```

#### Volume access (Experimental)
##### Create data volume:
```
docker create -v biggis-landuse_data:/data --name biggis-landuse_data biggis/biggis-landuse:0.0.2-SNAPSHOT /data
```
##### Copy data into volume:
```
docker cp ./readme.md biggis-landuse_data:/readme.md
```

### Start (e.g. GettingStarted Example):
```
docker run --volumes-from biggis-landuse_data -t biggis/biggis-landuse:0.0.2-SNAPSHOT java "-Dspark.master=local[*]" -Xmx2g -cp biggis-landuse-0.0.2-SNAPSHOT.jar biggis.landuse.spark.examples.GettingStarted geotrellis-catalog
```

### Hints: 
``` 
docker images
docker volume ls
docker ps -a
# clean up containers:
docker rm $(docker ps -q --filter "status=exited")
$(docker images -q --filter "dangling=true")
```