Note for Professor Glenn

Anthony Jiang
ajj38
anthony.jiang@yale.edu
09/03/2019

1. The below python module is submitted to fulfill the requirements to place out of CPSC 201 and directly take CPSC 223.

2. The code cannot be directly run without downloading certain dependencies from the NOAA Global Historical Climatology
Project (GHCN) archives. First, it requires a GHCN-daily dataset, found off
ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/by_year/. Second, it will require a list of stations in the NOAA GHCN
station format, found at this link ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt Third, while it does
rely on the relatively standard python libraries pandas and numpy, it also relies upon a library called Pyeto that has
not been updated by the original creators in a while. Although technically maintained as a component of opencroplib
0.1.4 found at https://pypi.org/project/opencroplib/, I downloaded the code off the original developer's github
https://github.com/woodcrafty/PyETo.

3. In one sentence, this module estimates the overall water savings when weather-based irrigation controllers are
implemented nationwide. At its core, it models the behavior of such a weather-based irrigation controller -- a device
used to control the watering schedule of residential lawns -- by examining the weather forecast, past precipitation
patterns, and biological water demands to determine when best to water a lawn. It then simulates the implementation of
such systems at every NOAA GHCN station over the course of a year, then calculates how much water is saved across all
such stations relative to a set default lawn watering pattern. Ultimately, it estimated 48% potential savings if every
single lawn nationwide was watered using a weather-based irrigation controller

4. This module can be thought of as three sections. The first is the input handling. A key challenge here was filtering
the data for only the desired weather records, as well as processing it in a manner that was easy to handle while
simultaneously avoided the problem of pandas DataFrame.append O(n^2) quadratic copy. Indeed, the input data file is 1.3
GB and the output around 300 MB. The second section, water and calculate_et_t, are the actual algorithm. calculate_et_t
determines the water demand throughout the year for plants using the thornthwaite method. This allows the module to
further improve upon regular weather-based irrigation controllers by adjusting the weekly water target as the growing
season progresses. water in turn takes the calculated et_ts to actually model a single station using weather data.
Finally, the last component implements the model for an individual irrigation controller on a nationwide scale. The
method test_single_station_search searches for an specific input station, modelling savings in one locale, while
test_all_stations evaluates the savings nationwide.