


# TODO try different max delay and training window parms, then also with aux covariates

withr::with_rng_version("3.5", withr::with_seed(4120329, {
  # n <- 100L
  # n <- 200L
  # n <- 50L
  # n <- 500L
  # n <- 1000L
  # n <- 10000L
  n <- 30L
  # n <- 50L
  # n <- 10L
  # n <- 15L
  # n <- 20L
  # n <- 10000L
  # TODO replicates
  sapply(1:1000, function(sim_i) {
    trend <- plogis(5*sin(1:n/52*2*pi) - 5) %>%
      # {. / mean(.) * 200e3 / 52 * 0.001}
      {. / mean(.) * 200e3 / 52 * 0.01}
    finalized <- rpois(n, trend)
    delay_dist <- (5:1) %>% {. / sum(.)}
    # delay_dist <- c(8, .5, .5, .5, .5) %>% {. / sum(.)}
    ar_feats <- t(sapply(finalized, function(size) rmultinom(1L, size, delay_dist))) %>%
      {sapply(1:ncol(.), function(j) dplyr::lag(.[,j], j - 1L))} %>%
      {.[is.na(.)] <- 0L; .}
    lm1 <- lm(finalized ~ ar_feats)
    lm2 <- lm(finalized ~ ar_feats + 0)
    # trend2 <- trend
    trend2 <- plogis(5*cos((1:n)/52*2*pi) - 5) %>%
      # {. / mean(.) * 200e3 / 52 * 0.001}
      {. / mean(.) * 200e3 / 52 * 0.01}
    finalized2 <- rpois(n, trend2)
    ar_feats2 <- t(sapply(finalized2, function(size) rmultinom(1L, size, delay_dist))) %>%
      {sapply(1:ncol(.), function(j) dplyr::lag(.[,j], j - 1L))} %>%
      {.[is.na(.)] <- 0L; .}
    pred1 <- predict(lm1, newdata = tibble(ar_feats = ar_feats2))
    pred2 <- predict(lm2, newdata = tibble(ar_feats = ar_feats2))
    c(
      mean(abs(ar_feats2[1,] - finalized2)) / mean(abs(finalized2 - dplyr::lag(finalized2)), na.rm = TRUE),
      mean(abs(pred1 - finalized2)) / mean(abs(finalized2 - dplyr::lag(finalized2)), na.rm = TRUE),
      mean(abs(pred2 - finalized2)) / mean(abs(finalized2 - dplyr::lag(finalized2)), na.rm = TRUE),
      mean(abs(trend2 - finalized2)) / mean(abs(finalized2 - dplyr::lag(finalized2)), na.rm = TRUE)
    )
  })
})) %>%
  rowMeans()






# think about collinearity detection

N <- 100L
y <- rnorm(N)
x1 <- rep(0, N)
x2 <- rep(0, N)

lm(y ~ x1 + x2)

quantreg::rq(y ~ x1 + x2)

quantreg::rq(y ~ x1 + x2, method = "fn")

# maybe don't try as build feature by feature.  just copy over handling from epiforecast-R
