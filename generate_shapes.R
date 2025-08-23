# Generate GTFS shapes.txt from VDV data
# Combines functionality from generate_stop_times.r and genrate_line_shapes.R
# Input: VDV tables in .x10 format
# Output: shapes.txt in GTFS format

rm(list = ls())
library(dplyr)
library(readr)
library(stringr)
library(sf)

convert_to_decimal <- function(x) {
  # Convert string to numeric for calculations, retaining the sign
  x <- as.numeric(x)
  
  # Extract degrees, minutes, seconds, and milliseconds
  sign <- ifelse(x < 0, -1, 1)
  x <- abs(x)
  
  degrees <- floor(x / 10000000)
  minutes <- floor((x - degrees * 10000000) / 100000)
  seconds <- floor((x - degrees * 10000000 - minutes * 100000) / 1000)
  milliseconds <- x - degrees * 10000000 - minutes * 100000 - seconds * 1000
  
  # Convert to decimal degrees
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

# Main processing
folder <- "vdv_202505080912"

# Load required VDV tables
stop <- load_vdv_table(folder, "i2531280.x10")
link <- load_vdv_table(folder, "i2991280.x10")
point_on_link <- load_vdv_table(folder, "i9951280.x10")
route_sequence <- load_vdv_table(folder, "i2461280.x10")
routes <- load_vdv_table(folder, "i2261280.x10")

# Process point_on_link to create route geometries
link_geometries <- point_on_link %>%
  left_join(stop, by = c("BASE_VERSION" = "BASE_VERSION",
                        "POINT_TO_LINK_NO" = "POINT_NO",
                        "POINT_TO_LINK_TYPE" = "POINT_TYPE")) %>%
  mutate(
    POINT_LONGITUDE = convert_to_decimal(POINT_LONGITUDE),
    POINT_LATITUDE = convert_to_decimal(POINT_LATITUDE)
  ) %>%
  filter(!is.na(POINT_LONGITUDE)) %>%
  group_by(OP_DEP_NO, POINT_TYPE, POINT_NO, TO_POINT_NO, TO_POINT_TYPE) %>%
  arrange(POINT_ON_LINK_SERIAL_NO) %>%
  summarize(
    coords = list(cbind(POINT_LONGITUDE, POINT_LATITUDE)),
    .groups = 'drop'
  ) %>%
  mutate(geometry = map(coords, st_linestring)) %>%
  st_as_sf() %>%
  select(-coords) %>%
  st_set_crs(4326) %>%
  left_join(link, by = c('OP_DEP_NO', 'POINT_TYPE', 'POINT_NO', 'TO_POINT_NO', 'TO_POINT_TYPE'))

# Process route sequence to create complete route shapes
processed_route_sequence <- route_sequence %>%
  group_by(LINE_NO, ROUTE_ABBR) %>%
  mutate(
    TO_POINT_TYPE = lead(POINT_TYPE),
    TO_POINT_NO = lead(POINT_NO)
  ) %>%
  ungroup() %>%
  left_join(routes, by = c('BASE_VERSION', "LINE_NO", "ROUTE_ABBR")) %>%
  filter(!is.na(TO_POINT_NO))

route_shapes <- processed_route_sequence %>%
  left_join(link_geometries, by = c('OP_DEP_NO', "POINT_TYPE", "POINT_NO", "TO_POINT_NO", "TO_POINT_TYPE")) %>%
  group_by(LINE_NO, ROUTE_ABBR) %>%
  arrange(SEQUENCE_NO) %>%
  summarize(geometry = st_combine(geometry), .groups = "drop") %>%
  mutate(geometry = st_line_merge(geometry))

# Generate GTFS shapes.txt
shapes_txt <- route_shapes %>%
  st_as_sf() %>%
  st_cast("POINT") %>%
  group_by(LINE_NO, ROUTE_ABBR) %>%
  mutate(
    shape_id = paste0(LINE_NO, "_", ROUTE_ABBR),
    shape_pt_sequence = row_number(),
    shape_pt_lon = st_coordinates(geometry)[,1],
    shape_pt_lat = st_coordinates(geometry)[,2]
  ) %>%
  ungroup() %>%
  select(shape_id, shape_pt_lat, shape_pt_lon, shape_pt_sequence) %>%
  st_drop_geometry()

# Write shapes.txt to GTFS directory
if (!dir.exists("gtfs")) {
  dir.create("gtfs")
}
write_csv(shapes_txt, "gtfs/shapes.txt")

message("GTFS shapes.txt generated successfully in gtfs/ directory")