### Usage (Build):
```
cd ..
mvn package
cp ./target/biggis-landuse-0.0.3-SNAPSHOT.jar ./docker-integration
cd ./docker-integration
```
```
docker build -t biggis/biggis-landuse:0.0.3-SNAPSHOT .
```

### Archive (tar):
```
docker save biggis/biggis-landuse:0.0.3-SNAPSHOT -o biggis-landuse-0.0.3-SNAPSHOT.tar
```

#### Volume access (Experimental)
##### Create data volume:
```
docker create -v ${path_to_biggis-landuse_data}:/data -v $(path_to_local_fs_or_hdfs)/geotrellis-catalog/:/geotrellis-catalog --name biggis-landuse_data biggis/biggis-landuse:0.0.3-SNAPSHOT /data
```
##### Copy data into volume:
```
docker cp ./readme.md biggis-landuse_data:/data/readme.md
```

### Start (e.g. GettingStarted Example):
```
docker run --volumes-from biggis-landuse_data -p 4040:4040 -p 18080:18080 -t biggis/biggis-landuse:0.0.3-SNAPSHOT java "-Dspark.master=local[*]" -Xmx2g -cp biggis-landuse-0.0.3-SNAPSHOT.jar biggis.landuse.spark.examples.GettingStarted geotrellis-catalog
```

### Hints: 
``` 
docker images
docker volume ls
docker ps -a
# clean up containers:
docker rm $(docker ps -q --filter "status=exited")
docker rmi $(docker images -q --filter "dangling=true")
```