#' Read data from a Tinytag ".txt" file in a usable form
#'
#' @param path A string as a path to the Tinytag file
#' @export
readTinyTag <- function(txt.path) {
    import <- readr::read_delim(
        file = txt.path,
        delim = "\t",
        col_names = FALSE,
        col_types = readr::cols(),
        skip = 5) %>%
        select(-1)
    if (ncol(import) == 2) {
        names(import) <- c("Datum", "RegenX")
    } else if (ncol(import) == 3) {
        import <- import %>%
            rename("RegenX" = "X4") %>%
            tidyr::unite("Datum", X2|X3, sep = " ") %>%
            mutate(Datum = lubridate::ymd_hms(Datum))
    } else {
        stop("More than 3 columns found in tinytag file!")
    }
    import %>%
        mutate(across(Datum & where(is.character), ~ lubridate::parse_date_time(
            .x,
            orders = c("dmy_HM")))) %>%
        mutate(Datum = lubridate::round_date(Datum, "5 mins")) %>%
        mutate(RegenX = stringr::str_replace(
            RegenX,
            pattern = " mm$",
            replacement = ""
        )) %>%
        mutate(RegenX = stringr::str_replace(
            RegenX,
            pattern = ",",
            replacement = "."
        )) %>%
        mutate(across(where(is.character), as.numeric)) %>%
        data.table::as.data.table()
    # Check if following pattern replacement was needed in some cases
    # tt.table[, RegenX := as.numeric(stringr::str_match(RegenX, pattern = "^[0-9]+(?:\\.[0-9]+$)?"))]
}
