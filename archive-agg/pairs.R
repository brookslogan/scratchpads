
Rcpp::cppFunction("List mkpair_a(SEXP s, int i) {return List::create(s, i);}")
Rcpp::cppFunction("List mkpair_b(SEXP s, IntegerVector i) {return List::create(s, i);}")
rextendr::rust_function("fn mkpair_c(a:Robj, b:i32) -> List {list!(a, b)}")
rextendr::rust_function("fn mkpair_d(a:Robj, b:Robj) -> List {list!(a, b)}")
# TODO check if above are optimized

rextendr::rust_source(
  profile = "release",
  code = '
#[extendr]
fn mkpair_e(a:Robj, b:i32) -> List {
  list!(a, b)
}
#[extendr]
fn mkpair_f(a:Robj, b:Robj) -> List {
  list!(a, b)
}
#[extendr]
fn mkpair_g(a:Robj, b:Robj) -> Robj {
  r!(list!(a, b))
}
')

# TODO try https://gist.github.com/Thell/861d464c7c85ffd4c7285a894b7715f4

# TODO try (linked from archived rustinr):
# https://github.com/r-rust,
# https://jeroen.github.io/erum2018/#1

# TODO check on Rcpp funs

emptyenv_obj <- emptyenv()
bench::mark(
  list("elt", 5L),
  list2("elt", 5L),
  `[[<-`(`[[<-`(vector("list", 2L), 1L, "elt"), 2L, 5L),
  mkpair_a("elt", 5L),
  mkpair_b("elt", 5L),
  mkpair_c("elt", 5L),
  mkpair_d("elt", 5L),
  mkpair_e("elt", 5L),
  mkpair_f("elt", 5L),
  mkpair_g("elt", 5L),
  pairlist("elt", 5L),
  pairlist2("elt", 5L),
  `[[<-`(`[[<-`(new.env(parent = emptyenv()), "currelt", "elt"), "i", 5L),
  `[[<-`(`[[<-`(new.env(FALSE, emptyenv()), "currelt", "elt"), "i", 5L),
  `[[<-`(`[[<-`(new.env(parent = emptyenv_obj), "currelt", "elt"), "i", 5L),
  `[[<-`(`[[<-`(new.env(FALSE, emptyenv_obj), "currelt", "elt"), "i", 5L),
  check = FALSE,
  min_time = 2,
  max_iterations = 1e9
)
