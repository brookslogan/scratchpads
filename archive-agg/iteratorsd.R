

cached_exhausted <- exhausted()

new_itrd <- function(f) {
  assert_function(f, args = "i")

  itrc <- f
  class(itrc) <- "itrd"
  itrc
}

# TODO rename? input doesn't have to be list
list_itrd <- function(x) {
  new_itrd(
    function(i) if (i <= length(x)) x[[i]] else cached_exhausted
  )
}

# TODO rename? input doesn't have to be list
itrd_map_itrd <- function(itrd, f) {
  new_itrd(
    function(i) {
      inp <- itrd(i)
      if (identical(inp, cached_exhausted)) {
        cached_exhausted
      } else {
        f(inp)
      }
    }
  )
}

# TODO mixed sized/unsized index iterators with an S3 itr_for_each to
# handle looping for us... fn call overhead might be bad, but let's
# see... perhaps could also use code manip though that would likely
# mess up srcrefs...
