#!/bin/bash
java -Xmx8g -Dspark.master=local[*]  -cp target/biggis-landuse-0.0.1-SNAPSHOT.jar \
  biggis.landuse.spark.examples.$1 \
  $2 \
  $3 \
  $4 \
  $5 \
  $6 \
  $7 \
  $8 \
  $9
