
library(checkmate)
library(vctrs)

extract_row_as_list <- function(x, i) {
  assert_data_frame(x)
  assert_integerish(i, lower = 1, upper = nrow(x), any.missing = FALSE)
  lapply(vec_slice(x, i), function(maybe_wrapped) {
    if (is.list(maybe_wrapped) && !is.data.frame(maybe_wrapped)) {
      maybe_wrapped[[1L]]
    } else {
      maybe_wrapped
    }
  })
}
