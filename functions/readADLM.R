########################################################################################################################
#' Reads data from an ADLM xlsx file
#' @param path Path to the ADLM file to read
#' @export
readADLM <- function(path) {
    col_names <- path %>%
        readxl::read_excel(n_max = 1) %>%
        names()
    path %>%
        readxl::read_excel(skip = 2, col_names = col_names) %>%
        mutate(Datum = lubridate::round_date(Datum, "1 mins")) %>%
        mutate(across(!Datum, as.numeric)) %>%
        arrange(Datum)
}
