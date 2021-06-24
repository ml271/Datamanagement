#' Read data from an Envilog file in a usable form
#'
#' @param path A string as a path to the Envilog file
#' @export
readEnvilog <- function(path) {
    path <- iconv(path, to = "latin1")
    data <- data.frame(tryCatch(
        read.csv(path, skip = 1, fileEncoding = "cp1258"),
        error = function(e) read.csv2(path, skip = 1, fileEncoding = "cp1258"))) %>%
        select(-No) %>%
        filter(Time != "")

    col_names <- names(data) %>%
        stringr::str_replace(
            pattern = "KK|K[^(R|L)]",
            replacement = "_X") %>%
        stringr::str_replace(
            pattern = "KR",
            replacement = "_Y") %>%
        stringr::str_replace(
            pattern = "KL|[.]L[.]",
            replacement = "_Z") %>%
        stringr::str_replace(
            pattern = "C",
            replacement = "_T_PF") %>%
        stringr::str_replace(
            pattern = "pF",
            replacement = "_MP") %>%
        stringr::str_replace(
            pattern = ".*((?<!T_)MP|T_PF).*([XYZ]).*([0-9]{2}).*",
            replacement = "\\3_\\1_\\2") %>%
        stringr::str_replace(
            pattern = "X.H_X",
            replacement = "")
    col_names[1] <- "Datum"
    names(data) <- col_names

    data %>%
        mutate(Datum = lubridate::dmy_hms(Datum)) %>%
        mutate(Datum = lubridate::round_date(Datum, "5 mins")) %>%
        mutate(across(-Datum, ~ stringr::str_replace(
            .x,
            pattern = "^\\D+$",
            replacement = ""))) %>%
        mutate(across(-Datum, ~ stringr::str_replace(
            .x,
            pattern = ",",
            replacement = "."
        ))) %>%
        mutate(across(where(is.character), as.numeric)) %>%
        data.table::as.data.table()
}
