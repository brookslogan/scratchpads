# try itrb with native functions

new_epi_sized_iteratorf <- function(size, f) {
  assert_int(size, lower = 0)
  assert_function(f, args = "i")
  assert_false(inherits(f, "epi_sized_iteratorf"))

  itrb <- f
  attr(itrb, "epiprocess::size") <- size
  class(itrb) <- "epi_sized_iteratorf"
  itrb
}

itrf_size <- function(itrf) {
  attr(itrf, "epiprocess::size")
}

list_itrf <- function(x) {
  new_epi_sized_iteratorf(
    length(x),
    function(i) x[[i]]
  )
}

rextendr::rust_source(
  profile = "release",
  code = '
#[extendr]
fn call_rs(f:Robj, x:Robj) -> Result<Robj, Error> {
  f.call(Pairlist::from_pairs([("", x)]))
}
#[extendr]
fn map_rs(x:List, f:Robj) -> List {
  x.into_iter().map(|(_, e)| f.call(Pairlist::from_pairs([("", e)])).unwrap()).collect()
}
')

# https://gist.github.com/Thell/861d464c7c85ffd4c7285a894b7715f4
# extendr_deps = list(
#   "extendr-api" = list(git = "https://github.com/extendr/extendr")
# ),

# https://github.com/extendr/arrow-extendr/blob/0ad25c13c9d63d68e6bf7b268a928b6e9d62c49c/src/to.rs#L11
# use extendr_api::{error::Result, prelude::*};
# ^ source of Result<Robj>?


bench::mark(
  # map_rs(as.list(1:5), function(x) x^2),
  # lapply(as.list(1:5), function(x) x^2),
  map_rs(as.list(1:1000), function(x) x^2),
  lapply(as.list(1:1000), function(x) x^2),
  min_time = 3, max_iterations = 1e9
)
# ... not fantastic, plus above is already converting into list for no
# reason.  but this may not necessarily be overhead shared by actual
# iterator approach.


# TODO try native versions

# as.list.epi_sized_iteratorf <- function(itrf) {
#   result <- vector("list", attr(itrf, "epiprocess::size"))
#   for (i in seq_len(attr(itrf, "epiprocess::size"))) {
#     result[[i]] <- itrf(i)
#   }
#   result
# }

# itrf_map_itrf <- function(itrf, f) {
#   curr_state <- list(0L, NULL) # i, result
#   new_epi_sized_iteratorf(
#     itrf_size(itrf),
#     function(i) {
#       curr_i <- curr_state[[1L]]
#       if (i == curr_i) {
#         if (curr_i == 0L) {
#           stop("0 out of bounds")
#         } else {
#           curr_state[[2L]]
#         }
#       } else {
#         result <- f(itrf(i))
#         curr_state <- list(i, result) # atomic update
#         result
#       }
#     }
#   )
# }

# itrf_map_itrf0 <- function(itrf, f) {
#   new_epi_sized_iteratorf(
#     itrf_size(itrf),
#     function(i) f(itrf(i))
#   )
# }

# XXX Rcpp probably going to be faster, but license more restrictive
