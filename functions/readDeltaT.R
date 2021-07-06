#' Read data from a DeltaT ".dat" file in a usable form
#'
#' @param deltaT_dat_path A string as a path to the deltaT file
#' @export
readDeltaT <- function(deltaT_dat_path) {
    raw_dat_data <- read.csv(deltaT_dat_path, header = TRUE, skip = 7, fileEncoding = "cp1258")
    reduced_dat_data <- raw_dat_data %>%
        slice(-1:-3) %>%
        select(-matches("^X\\.*[0-9]*$")) %>%
        select(-matches("EsUmEgDu")) %>%
        mutate_if(is.factor, as.character) %>%
        mutate_at(vars(-starts_with("Label")), as.numeric) %>%
        mutate_if(is.character, trimws)

    drop_prefix_and_suffix_pattern <- "X\\.+|\\.+$"
    old_col_names <- names(reduced_dat_data)
    dropped_pre_and_suffix <- stringr::str_replace(old_col_names,
                                                   pattern = drop_prefix_and_suffix_pattern,
                                                   replacement = "")
    data.table::setnames(reduced_dat_data, dropped_pre_and_suffix)
    data.table::setnames(reduced_dat_data, old = "Label", new = "Datum")

    file_readout_year <- as.numeric(stringr::str_match(basename(deltaT_dat_path), "(?<=_)[0-9]{4}(?=_)"))
    if (is.na(file_readout_year)) {
        stop("File names for DeltaT need to contain the retrieval year in format _XXXX_ as it is not logged in file")
    }
    date_vector <- reduced_dat_data %>%
        mutate(Datum = as.POSIXct(Datum, tz = "UTC", format = "%d/%m %H:%M:%S")) %>%
        pull(Datum)
    if (.hasYearTransition(date_vector)) {
        transition_index <- .findYearTransitionIndex(date_vector)
        first_year <- rep(file_readout_year - 1, transition_index)
        second_year <- rep(file_readout_year, length(date_vector) - transition_index)
        year_paste_vector <- c(first_year, second_year)
    } else {
        year_paste_vector <- rep(file_readout_year, length(date_vector))
    }
    added_date_data <- reduced_dat_data %>%
        mutate(Datum = paste(year_paste_vector, Datum)) %>%
        mutate(Datum = as.POSIXct(Datum, tz = "UTC", format = "%Y %d/%m %H:%M:%S")) %>%
        mutate(Datum = lubridate::round_date(Datum, "5 mins"))

    return(added_date_data)
}

.hasYearTransition <- function(POSIXct_vector) {
    time_differences_in_seconds <- unique(diff(as.numeric(POSIXct_vector)))
    has_year_transition_time_jump <- TRUE %in% (time_differences_in_seconds < -31500000)
    return(has_year_transition_time_jump)
}

.findYearTransitionIndex <- function(POSIXct_vector) {
    time_differences_in_seconds <- diff(as.numeric(POSIXct_vector))
    first_new_year_element_index <- which(time_differences_in_seconds < -31500000)
    return(first_new_year_element_index)
}
