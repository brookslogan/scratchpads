
new_epi_sized_iterator <- function(f, size) {
  assert_function(f, args = character())
  assert_int(size, lower = 0)

  attr(f, "epiprocess::size") <- size
  class(f) <- "epi_sized_iterator"
  f
}

as.list.epi_sized_iterator <- function(itr) {
  result <- vector("list", length(grp_updates$subtbl))
  for (i in seq_len(attr(itr, "epiprocess::size"))) {
    subresult <- itr()
    if (identical(subresult, as.symbol(".__exhausted__."))) {
      stop("stopped early")
    } else {
      result[[i]] <- subresult
    }
  }
  if (!identical(itr(), as.symbol(".__exhausted__."))) {
    stop("didn't stop in time")
  }
  result
}
