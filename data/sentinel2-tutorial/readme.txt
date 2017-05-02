#! /bin/bash
# example data download, see: https://github.com/geotrellis/geotrellis-landsat-tutorial/blob/master/data/landsat/download-data.sh

#wget http://landsat-pds.s3.amazonaws.com/L8/107/035/LC81070352015218LGN00/LC81070352015218LGN00_B3.TIF
#wget http://landsat-pds.s3.amazonaws.com/L8/107/035/LC81070352015218LGN00/LC81070352015218LGN00_B4.TIF
#wget http://landsat-pds.s3.amazonaws.com/L8/107/035/LC81070352015218LGN00/LC81070352015218LGN00_B5.TIF
#wget http://landsat-pds.s3.amazonaws.com/L8/107/035/LC81070352015218LGN00/LC81070352015218LGN00_BQA.TIF
#wget http://landsat-pds.s3.amazonaws.com/L8/107/035/LC81070352015218LGN00/LC81070352015218LGN00_MTL.txt

# https://s3.amazonaws.com/geotrellis-sample-datasets/landsat/LC80140322014139LGN00.tar.bz
# tar xvfj LC80140322014139LGN00.tar.bz


# Example Sentinel2 AWS	(B02.jp2, B03.jp2, B04.jp2, B08.jp2)
#wget http://sentinel-s2-l1c.s3.amazonaws.com/tiles/10/S/DG/2015/12/7/0/B02.jp2

#(32/U/MU/2016/5/5/0, 32/U/MU/2016/6/24/1, 32/U/MU/2016/8/13/0, 32/U/MU/2016/8/23/0)

mkdir 32_UMU_2016_5_5_0
cd 32_UMU_2016_5_5_0
wget http://sentinel-s2-l1c.s3.amazonaws.com/tiles/32/U/MU/2016/5/5/0/B02.jp2
wget http://sentinel-s2-l1c.s3.amazonaws.com/tiles/32/U/MU/2016/5/5/0/B03.jp2
wget http://sentinel-s2-l1c.s3.amazonaws.com/tiles/32/U/MU/2016/5/5/0/B04.jp2
wget http://sentinel-s2-l1c.s3.amazonaws.com/tiles/32/U/MU/2016/5/5/0/B08.jp2
cd ..

mkdir 32_UMU_2016_6_24_1
cd 32_UMU_2016_6_24_1
wget http://sentinel-s2-l1c.s3.amazonaws.com/tiles/32/U/MU/2016/6/24/1/B02.jp2
wget http://sentinel-s2-l1c.s3.amazonaws.com/tiles/32/U/MU/2016/6/24/1/B03.jp2
wget http://sentinel-s2-l1c.s3.amazonaws.com/tiles/32/U/MU/2016/6/24/1/B04.jp2
wget http://sentinel-s2-l1c.s3.amazonaws.com/tiles/32/U/MU/2016/6/24/1/B08.jp2
cd ..

mkdir 32_UMU_2016_8_13_0
cd 32_UMU_2016_8_13_0
wget http://sentinel-s2-l1c.s3.amazonaws.com/tiles/32/U/MU/2016/8/13/0/B02.jp2
wget http://sentinel-s2-l1c.s3.amazonaws.com/tiles/32/U/MU/2016/8/13/0/B03.jp2
wget http://sentinel-s2-l1c.s3.amazonaws.com/tiles/32/U/MU/2016/8/13/0/B04.jp2
wget http://sentinel-s2-l1c.s3.amazonaws.com/tiles/32/U/MU/2016/8/13/0/B08.jp2
cd ..

mkdir 32_UMU_2016_8_23_0
cd 32_UMU_2016_8_23_0
wget http://sentinel-s2-l1c.s3.amazonaws.com/tiles/32/U/MU/2016/8/23/0/B02.jp2
wget http://sentinel-s2-l1c.s3.amazonaws.com/tiles/32/U/MU/2016/8/23/0/B03.jp2
wget http://sentinel-s2-l1c.s3.amazonaws.com/tiles/32/U/MU/2016/8/23/0/B04.jp2
wget http://sentinel-s2-l1c.s3.amazonaws.com/tiles/32/U/MU/2016/8/23/0/B08.jp2
cd ..

#(32/T/LT/2016/5/5/0, 32/T/LT/2016/6/24/0, 32/T/LT/2016/8/13/0, 32/T/LT/2016/8/23/0)

mkdir 32_TLT_2016_5_5_0
cd 32_TLT_2016_5_5_0
wget http://sentinel-s2-l1c.s3.amazonaws.com/tiles/32/T/LT/2016/5/5/0/B02.jp2
wget http://sentinel-s2-l1c.s3.amazonaws.com/tiles/32/T/LT/2016/5/5/0/B03.jp2
wget http://sentinel-s2-l1c.s3.amazonaws.com/tiles/32/T/LT/2016/5/5/0/B04.jp2
wget http://sentinel-s2-l1c.s3.amazonaws.com/tiles/32/T/LT/2016/5/5/0/B08.jp2
cd ..

mkdir 32_TLT_2016_6_24_0
cd 32_TLT_2016_6_24_0
wget http://sentinel-s2-l1c.s3.amazonaws.com/tiles/32/T/LT/2016/6/24/0/B02.jp2
wget http://sentinel-s2-l1c.s3.amazonaws.com/tiles/32/T/LT/2016/6/24/0/B03.jp2
wget http://sentinel-s2-l1c.s3.amazonaws.com/tiles/32/T/LT/2016/6/24/0/B04.jp2
wget http://sentinel-s2-l1c.s3.amazonaws.com/tiles/32/T/LT/2016/6/24/0/B08.jp2
cd ..

mkdir 32_TLT_2016_8_13_0
cd 32_TLT_2016_8_13_0
wget http://sentinel-s2-l1c.s3.amazonaws.com/tiles/32/T/LT/2016/8/13/0/B02.jp2
wget http://sentinel-s2-l1c.s3.amazonaws.com/tiles/32/T/LT/2016/8/13/0/B03.jp2
wget http://sentinel-s2-l1c.s3.amazonaws.com/tiles/32/T/LT/2016/8/13/0/B04.jp2
wget http://sentinel-s2-l1c.s3.amazonaws.com/tiles/32/T/LT/2016/8/13/0/B08.jp2
cd ..

mkdir 32_TLT_2016_8_23_0
cd 32_TLT_2016_8_23_0
wget http://sentinel-s2-l1c.s3.amazonaws.com/tiles/32/T/LT/2016/8/23/0/B02.jp2
wget http://sentinel-s2-l1c.s3.amazonaws.com/tiles/32/T/LT/2016/8/23/0/B03.jp2
wget http://sentinel-s2-l1c.s3.amazonaws.com/tiles/32/T/LT/2016/8/23/0/B04.jp2
wget http://sentinel-s2-l1c.s3.amazonaws.com/tiles/32/T/LT/2016/8/23/0/B08.jp2
cd ..


