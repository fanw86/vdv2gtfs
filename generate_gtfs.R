# Comprehensive GTFS Generator from VDV Data
# Combines stop_times and shapes generation with visualization
# Input: VDV tables in .x10 format
# Output: Complete GTFS feed and visualization files

rm(list = ls())
library(dplyr)
library(readr)
library(stringr)
library(lubridate)
library(tidyr)
library(purrr)
#library(sf)
#library(leaflet)
#library(classInt)
#library(RColorBrewer)
library(readxl)

### Shared Utility Functions ###
convert_to_decimal <- function(x) {
  x <- as.numeric(x)
  sign <- ifelse(x < 0, -1, 1)
  x <- abs(x)
  
  degrees <- floor(x / 10000000)
  minutes <- floor((x - degrees * 10000000) / 100000)
  seconds <- floor((x - degrees * 10000000 - minutes * 100000) / 1000)
  milliseconds <- x - degrees * 10000000 - minutes * 100000 - seconds * 1000
  
  decimal_degrees <- sign * (degrees + minutes / 60 + (seconds + milliseconds / 1000) / 3600)
  round(decimal_degrees, 6)
}

load_vdv_table <- function(folder, path) {
  path <- paste0(folder, "/", path)
  if (!file.exists(path)) {
    stop(paste("File not found:", path))
  }
  tryCatch({
    col_names <- read_lines(path, skip = 10, n_max = 1) %>%
      str_split(";") %>%
      unlist() %>%
      str_trim()
    
    data <- read_delim(path, delim = ";", col_names = col_names, trim_ws = TRUE, skip = 12)
    data <- data[-((nrow(data)-1):nrow(data)),]
    data %>% select(-1)
  }, error = function(e) {
    stop(paste("Error loading", path, ":", e$message))
  })
}

### Stop Times Processing Functions ###
create_trip_stop_mapping <- function(journey, route_sequence, routes) {
  journey %>%
    left_join(route_sequence,
              by = c("BASE_VERSION" = "BASE_VERSION",
                     "LINE_NO" = "LINE_NO",
                     "ROUTE_ABBR" = "ROUTE_ABBR"),
              relationship = 'many-to-many') %>%
    left_join(routes,
              by=c("BASE_VERSION" = "BASE_VERSION",
                   "LINE_NO" = "LINE_NO",
                   "ROUTE_ABBR" = "ROUTE_ABBR")
              ) %>%
    arrange(JOURNEY_NO, SEQUENCE_NO) %>%
    group_by(JOURNEY_NO) %>%
    mutate(
      TO_POINT_NO = lead(POINT_NO)
    ) %>%
    ungroup() 
}

calculate_stop_times <- function(trip_stop_mapping, travel_time) {
  trip_stop_mapping %>%
    left_join(travel_time,
              by = c("BASE_VERSION" = "BASE_VERSION",
                     "POINT_TYPE"="POINT_TYPE",
                     "POINT_NO" = "POINT_NO",
                     "TO_POINT_NO" = "TO_POINT_NO",
                     "OP_DEP_NO"="OP_DEP_NO",
                     "TIMING_GROUP_NO" = "TIMING_GROUP_NO"
              )) %>%
    group_by(JOURNEY_NO) %>%
    mutate(
      arrival_time = first(DEPARTURE_TIME),
      arrival_time = cumsum(c(0, TRAVEL_TIME[-n()])),
      arrival_time = arrival_time + DEPARTURE_TIME[1],
      departure_time = arrival_time
    ) %>%
    ungroup() %>%
    mutate(
      arrival_time = as.character(hms::hms(arrival_time)),
      departure_time = as.character(hms::hms(departure_time))
    )
  #browser()
}




### Main Execution ###
day_type = 15

  folder <- "./vdv_202505080912"
  
  # Load all required tables
  stop <- load_vdv_table(folder, "i2531280.x10")
  stop_point <- load_vdv_table(folder, "i2291280.x10")
  journey <- load_vdv_table(folder, "i7151280.x10")
  travel_time <- load_vdv_table(folder, "i2821280.x10") %>% distinct()
  route_sequence <- load_vdv_table(folder, "i2461280.x10")
  link <- load_vdv_table(folder, "i2991280.x10")
  point_on_link <- load_vdv_table(folder, "i9951280.x10")
  routes <- load_vdv_table(folder, "i2261280.x10")

  # Process stop times
  trip_stop_mapping <- create_trip_stop_mapping(journey, route_sequence,routes)
  trip_stop_mapping_rev <- trip_stop_mapping %>% filter(ROUTE_ABBR < 30)
  stop_times <- calculate_stop_times(trip_stop_mapping_rev, travel_time) 


  # Create gtfs directory if it doesn't exist
  if (!dir.exists("gtfs")) {
    dir.create("gtfs")
  }

  # Generate GTFS files
  # routes.txt
  routes %>%
    select(route_id = LINE_NO, route_short_name = LINE_ABBR) %>%
    filter(!is.na(route_id)) %>%
    distinct() %>%
    mutate(route_type = 3) %>%
    write_csv("gtfs/routes.txt")



  # stop_times.txt
  stop_times %>%
    select(trip_id = JOURNEY_NO, arrival_time, departure_time, 
           stop_id = POINT_NO, stop_sequence = SEQUENCE_NO) %>%
    write_csv("gtfs/stop_times.txt")


  # Precompute stop coords
  stop_coords <- stop %>%
    filter(POINT_TYPE %in% c(1,2,42)) %>%
    transmute(BASE_VERSION, POINT_NO, POINT_TYPE,
              lon = convert_to_decimal(POINT_LONGITUDE),
              lat = convert_to_decimal(POINT_LATITUDE))

  # Build per-route coordinate sequences by just chaining stops
  route_coords_simple <- route_sequence %>%
    arrange(BASE_VERSION, LINE_NO, ROUTE_ABBR, SEQUENCE_NO) %>%
    left_join(stop_coords, by = c("BASE_VERSION","POINT_NO","POINT_TYPE")) %>%
    filter(!is.na(lon), !is.na(lat)) %>%
    group_by(BASE_VERSION, LINE_NO, ROUTE_ABBR) %>%
    reframe(coordinates = list(pick(lon, lat))) %>%
    ungroup()

  # Dedupe by hash instead of long strings
  route_coords_simple <- route_coords_simple %>%
    mutate(coord_hash = sapply(coordinates, function(df) digest::digest(as.matrix(df))))

  unique_shapes <- route_coords_simple %>%
    distinct(coord_hash, .keep_all = TRUE) %>%
    mutate(shape_id = paste0("shape_", row_number()))

  route_to_shape <- route_coords_simple %>%
    left_join(unique_shapes %>% select(coord_hash, shape_id), by = "coord_hash") %>%
    select(BASE_VERSION, LINE_NO, ROUTE_ABBR, shape_id)

  # Flatten to shapes.txt
  shapes_txt <- unique_shapes %>%
    select(shape_id, coordinates) %>%
    tidyr::unnest(coordinates) %>%
    group_by(shape_id) %>%
    mutate(
      shape_pt_sequence = dplyr::row_number(),
      shape_dist_traveled = c(0, cumsum(geosphere::distHaversine(
        cbind(lon[-n()], lat[-n()]), cbind(lon[-1], lat[-1])
      )))
    ) %>%
    ungroup() %>%
    transmute(shape_id,
              shape_pt_lat = lat,
              shape_pt_lon = lon,
              shape_pt_sequence,
              shape_dist_traveled)

  # write shapes.txt
  
  shapes_txt %>% write_csv("gtfs/shapes.txt")

  # trips.txt with shape_id
  stop_times %>%
    select(trip_id = JOURNEY_NO, route_id = LINE_NO, ROUTE_ABBR) %>%
    distinct() %>%
    left_join(route_to_shape, by = c("route_id" = "LINE_NO", "ROUTE_ABBR" = "ROUTE_ABBR")) %>%
    mutate(service_id = 1) %>%
    select(trip_id, route_id, service_id, shape_id) %>%
    write_csv("gtfs/trips.txt")

  # stops.txt
  stop %>% 
    filter(POINT_TYPE %in% c(1, 2, 42)) %>%
    select(stop_id = POINT_NO, stop_name = STOP_DESC,
           stop_lat = POINT_LATITUDE, stop_lon = POINT_LONGITUDE) %>%
    mutate(
      stop_lat = convert_to_decimal(stop_lat),
      stop_lon = convert_to_decimal(stop_lon),
      stop_name = iconv(stop_name, to = "utf-8")
    ) %>%
    write_csv("gtfs/stops.txt")

  # agency.txt
  
  agency <- tibble::tibble(
    agency_id = "1",
    agency_name = "ITC",
    agency_url = "https://example.com",
    agency_timezone = "Asia/Dubai",
    agency_lang = "ar",
    agency_phone = NA_character_,
    agency_fare_url = NA_character_,
    agency_email = NA_character_
  )
  
  readr::write_csv(agency, "gtfs/agency.txt")
  
  
  # calendar
  
  
  
  






