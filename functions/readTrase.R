#' Read data from a Trase file in a usable form
#'
#' @param path A string as a path to the Trase file
#' @export
readTrase <- function(path) {
  month.short <- c("JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL",
      "AUG", "SEP", "OCT","NOV", "DEC")
  months <- factor(month.short, levels = month.short, ordered = TRUE)
  test.string <- readLines(path, n = 1L)
  if (stringr::str_detect(test.string, "\\t")) {
    data <- data.table::as.data.table(read.delim(path, header = FALSE))
  } else {
    data <- data.table::as.data.table(read.csv(path, header = FALSE))
  }
  if (ncol(data) == 11){
    date.col = "V8"
    time.col = "V9"
    sens.col = "V5"
    val.col = "V1"
    err.col = "V10"
  } else if (ncol(data) == 15) {
    date.col = "V11"
    time.col = "V12"
    sens.col = "V8"
    val.col = "V3"
    err.col = "V14"
  } else if (ncol(data) == 16) {
    date.col = "V12"
    time.col = "V13"
    sens.col = "V9"
    val.col = "V4"
    err.col = "V15"
  } else {
    stop(path)
  }
  data[, Datum := paste(get(date.col), get(time.col))]
  for (index in 1 : length(months)) {
    data[, Datum := stringr::str_replace(Datum,
            paste0("(?i)",months[index]),
            paste0("_", as.character(as.numeric(months[index])), "_"))]
  }
  data[, Datum := stringr::str_replace_all(Datum, "[.] |(?<=_) ", "")]
  data[, Datum := stringr::str_replace_all(Datum,
          "(?<=[0-9]{1,2}_[0-9]{1,2}_)([0-9]{2})(?= )",
          "20\\1")]
  data[, Datum := MyUtilities::as.POSIXctFixed(Datum, format = "%d_%m_%Y %H:%M:%S", tz = "UTC")]
  data[, Datum := lubridate::round_date(Datum, "15 mins")]
  data[, Instruments := paste0("Sensor_", get(sens.col))]
  data[, Values := get(val.col)]
  data[, Error := get(err.col)]
  small.table <- data[, .(Datum, Instruments, Values, Error)]
  wide.table <- dcast(small.table, Datum ~ Instruments,
      value.var = c("Values", "Error"))
  data.table::setkey(wide.table, Datum)
  col.names <- names(wide.table)
  order.by <- stringr::str_replace(col.names, "([[:alpha:]]+_?[[:alpha:]]*_?)([0-9]?)",
      "\\2_\\1")
  out.table <- wide.table[, col.names[order(order.by)], with = FALSE]
  return(out.table)
}
