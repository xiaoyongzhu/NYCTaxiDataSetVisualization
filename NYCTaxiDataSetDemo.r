# uncomment the code piece below to install packages
# please note that for rgeos you need to install from source
#install.packages(c("lubridate", "sp", "maptools", "stringr", "magrittr","ggplot2"))
#install.packages(c("circlize", "purrr"))
#install.packages('rgeos', type="source")

rxSetComputeContext("local")

hdfsFS <- RxHdfsFileSystem()

spark_cc <- RxSpark(persistentRun = TRUE,
                    #                    extraSparkConfig = "--conf spark.speculation=true", numExecutors = 23, executorMem = "4G", executorCores = 3 )
                    extraSparkConfig = "--conf spark.speculation=true")


rxSetComputeContext(spark_cc)

rxGetComputeContext()

data_path <- file.path("wasb:///NYCDataRServerVisualization/")

#taxi_path <- file.path(data_path, "rawdata")
taxi_path <- file.path(data_path, "yellow_tripdata_2009-01.csv")
hdfs_ls <- paste0("hadoop fs -ls ", taxi_path)
system(hdfs_ls)

taxi_xdf <- file.path(data_path, "TestXdf")

#get some sample data from local disk
nyc_sample_df <-
  read.csv("/home/sshuser/nyc-taxi-data/data/yellow_tripdata_2009-01.csv",
           nrows = 1000)

taxi_text <- RxTextData(taxi_path, fileSystem = hdfsFS)
taxi_xdf <- RxXdfData(taxi_xdf, fileSystem = hdfsFS)

rxImport(
  inData = taxi_text,
  outFile = taxi_xdf,
  overwrite = TRUE,
  reportProgress = 3
)

rxGetInfo(taxi_xdf, getVarInfo = TRUE, numRows = 5)


taxi_tip <-
  RxXdfData("wasb:///NYCData/YellowTrip/taxitipXdf", fileSystem = hdfsFS)


rxDataStep(
  inData = taxi_xdf,
  outFile = taxi_tip,
  overwrite = TRUE,
  transforms = list(tip_percent = ifelse(Fare_Amt > 0,
                                         Tip_Amt / Fare_Amt,
                                         NA))
)

rxGetInfo(taxi_tip, getVarInfo = TRUE, numRows = 5)

xforms <-
  function(data) {
    # transformation function for extracting some date and time features
    require(lubridate)
    weekday_labels <-
      c('Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat')
    cut_levels <- c(1, 5, 9, 12, 16, 18, 22)
    hour_labels <-
      c('1AM-5AM',
        '5AM-9AM',
        '9AM-12PM',
        '12PM-4PM',
        '4PM-6PM',
        '6PM-10PM',
        '10PM-1AM')
    
    #pickup_datetime <- lubridate::ymd_hms(data$tpep_pickup_datetime, tz = "UTC")
    pickup_datetime <-
      lubridate::ymd_hms(data$Trip_Pickup_DateTime, tz = "UTC")
    pickup_hour <- addNA(cut(hour(pickup_datetime), cut_levels))
    pickup_dow <-
      factor(wday(pickup_datetime),
             levels = 1:7,
             labels = weekday_labels)
    levels(pickup_hour) <- hour_labels
    # dropoff_datetime <- lubridate::ymd_hms(data$tpep_dropoff_datetime, tz = "UTC")
    dropoff_datetime <-
      lubridate::ymd_hms(data$Trip_Dropoff_DateTime, tz = "UTC")
    dropoff_hour <- addNA(cut(hour(dropoff_datetime), cut_levels))
    dropoff_dow <-
      factor(wday(dropoff_datetime),
             levels = 1:7,
             labels = weekday_labels)
    levels(dropoff_hour) <- hour_labels
    #
    data$pickup_hour <- pickup_hour
    data$pickup_dow <- pickup_dow
    data$dropoff_hour <- dropoff_hour
    data$dropoff_dow <- dropoff_dow
    data$trip_duration <-
      as.integer(lubridate::interval(pickup_datetime, dropoff_datetime))
    
    return(data)
  }

x <- head(taxi_tip)
rxSetComputeContext("local")

rxDataStep(
  inData = x,
  outFile = NULL,
  transformFunc = xforms,
  transformPackages = "lubridate"
)

rxSetComputeContext(spark_cc)

taxi_date <- RxXdfData("wasb:///NYCData/YellowTrip/TaxiDatesTranf",
                       fileSystem = hdfsFS)

rxDataStep(
  inData = taxi_tip,
  outFile = taxi_date,
  transformFunc = xforms,
  transformPackages = "lubridate",
  overwrite = TRUE
)

rxGetInfo(taxi_date, numRows = 5, getVarInfo = TRUE)

tip_dist_df <-
  rxCube(tip_percent ~ pickup_hour + pickup_dow,
         data = taxi_date,
         returnDataFrame = TRUE)

library(ggplot2)
library(magrittr)

tip_dist_df %>% ggplot(aes(x = pickup_hour, y = pickup_dow, fill = tip_percent)) +
  geom_tile() + theme_minimal() +
  scale_fill_continuous(label = scales::percent) +
  labs(
    x = "Pickup Hour",
    y = "Pickup Day of Week",
    fill = "Tip Percent",
    title = "Distribution of Tip Percents",
    subtitle = "Do Passengers Tip More in the AM?"
  )



library(rgeos)
library(sp)
library(maptools)
library(stringr)

download.file("http://www.zillow.com/static/shp/ZillowNeighborhoods-NY.zip",
              destfile = "./ZillowNeighborhoods-NY.zip")

unzip("./ZillowNeighborhoods-NY.zip", exdir = "./ZillowNeighborhoods-NY")



library(maptools)

nyc_shapefile <-
  readShapePoly('./ZillowNeighborhoods-NY/ZillowNeighborhoods-NY.shp')
mht_shapefile <-
  subset(nyc_shapefile, City == 'New York' & County == 'New York')

mht_shapefile@data$id <- as.character(mht_shapefile@data$Name)


find_nhoods <- function(data) {
  # extract pick-up lat and long and find their neighborhoods
  pickup_longitude <-
    ifelse(is.na(data$Start_Lon), 0, data$Start_Lon)
  pickup_latitude <-
    ifelse(is.na(data$Start_Lat), 0, data$Start_Lat)
  data_coords <-
    data.frame(long = pickup_longitude, lat = pickup_latitude)
  coordinates(data_coords) <- c('long', 'lat')
  nhoods <- over(data_coords, shapefile)
  # add only the pick-up neighborhood and city columns to the data
  data$pickup_nhood <- nhoods$Name
  data$pickup_borough <- nhoods$County
  # extract drop-off lat and long and find their neighborhoods
  dropoff_longitude <-
    ifelse(is.na(data$End_Lon), 0, data$End_Lon)
  dropoff_latitude <-
    ifelse(is.na(data$End_Lat), 0, data$End_Lat)
  data_coords <-
    data.frame(long = dropoff_longitude, lat = dropoff_latitude)
  coordinates(data_coords) <- c('long', 'lat')
  nhoods <- over(data_coords, shapefile)
  # add only the drop-off neighborhood and city columns to the data
  data$dropoff_nhood <- nhoods$Name
  data$dropoff_borough <- nhoods$County
  # return the data with the new columns added in
  data
}

rxSetComputeContext("local")

head(
  rxDataStep(
    nyc_sample_df,
    transformFunc = find_nhoods,
    transformPackages = c("sp", "maptools"),
    transformObjects = list(shapefile = mht_shapefile)
  )
)

rxSetComputeContext(spark_cc)

taxi_hoods <- RxXdfData("wasb:///NYCData/YellowTrip/TaxiHoodsXdf",
                        fileSystem = hdfsFS)

rxDataStep(
  taxi_date,
  taxi_hoods,
  transformFunc = find_nhoods,
  transformPackages = c("sp", "maptools", "rgeos"),
  transformObjects = list(shapefile = mht_shapefile),
  overwrite = TRUE
)

rxGetInfo(taxi_hoods, getVarInfo = TRUE, numRows = 5)

mht_xdf <- RxXdfData("wasb:///NYCData/YellowTrip/ManhattanXdf",
                     fileSystem = hdfsFS)

# Add this line to Ali's repo
mht_hoods <- RxXdfData("wasb:///NYCData/YellowTrip/MhtHoodsXdf",
                       fileSystem = hdfsFS)


# comment out to see if it works or not
# nhoods_by_borough <-
#   rxCrossTabs( ~ pickup_nhood:pickup_borough, taxi_hoods)
# nhoods_by_borough <- nhoods_by_borough$counts[[1]]
# nhoods_by_borough <- as.data.frame(nhoods_by_borough)
#
# # get the neighborhoods by borough
# lnbs <- lapply(names(nhoods_by_borough),
#                function(vv)
#                  subset(
#                    nhoods_by_borough,
#                    nhoods_by_borough[, vv] > 0,
#                    select = vv,
#                    drop = FALSE
#                  ))
# lapply(lnbs, head)
#
# manhattan_nhoods <-
#   rownames(nhoods_by_borough)[nhoods_by_borough$`New York` > 0]

refactor_columns <- function(dataList) {
  dataList$pickup_nb = factor(dataList$pickup_nhood, levels = nhoods_levels)
  dataList$dropoff_nb = factor(dataList$dropoff_nhood, levels = nhoods_levels)
  dataList
}


rxDataStep(
  taxi_hoods,
  mht_hoods,
  overwrite = TRUE,
  transformFunc = refactor_columns,
  transformObjects = list(nhoods_levels = manhattan_nhoods)
)


# try it locally entirely -------------------------------------------------

# rxDataStep(inData = mht_hoods,
#            outFile = RxXdfData("mht.xdf",
#                                fileSystem = RxNativeFileSystem()),
#            reportProgress = 3)
#
# mht_local_xdf <- RxXdfData("mht.xdf")
#
# mht_local_df <- rxDataStep(inData =  mht_local_xdf,
#                            outFile = NULL,
#                            maxRowsByCols = 10^100)

rxDataStep(
  mht_hoods,
  mht_xdf,
  rowSelection = (
    Passenger_Count > 0 &
      Trip_Distance >= 0 & Trip_Distance < 30 &
      trip_duration > 0 & trip_duration < 60 * 60 * 24 &
      str_detect(pickup_borough, 'New York') &
      str_detect(dropoff_borough, 'New York') &
      !is.na(pickup_nb) &
      !is.na(dropoff_nb) &
      Fare_Amt > 0
  ),
  transformPackages = "stringr",
  varsToDrop = c(
    'mta_tax',
    'Total_Amt',
    'pickup_borough',
    'dropoff_borough',
    'pickup_nhood',
    'dropoff_nhood'
  ),
  overwrite = TRUE
)


rxGetInfo(mht_xdf, numRows = 5, getVarInfo = TRUE)


library(dplyr)

nbs_df <-
  rxCube( ~ pickup_nb + dropoff_nb + pickup_hour,
          data = mht_xdf,
          returnDataFrame = TRUE)
nbs_df <- nbs_df %>% tbl_df %>%
  filter(Counts >= 100) %>%
  mutate(width = ntile(Counts, 5))

library(purrr)
library(circlize)

nbs <- c(
  "Lower East Side",
  "East Village",
  "Chelsea",
  "Midtown",
  "Upper East Side",
  "Upper West Side",
  "Greenwich Vilalge"
)


chord_diag <-
  . %>% select(pickup_nb, dropoff_nb, width) %>% chordDiagram()

chord_plot <- nbs_df %>%
  filter(pickup_nb %in% nbs,
         dropoff_nb %in% nbs) %>%
  split(.$pickup_hour) %>%
  map(chord_diag)
