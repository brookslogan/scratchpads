
full_archive %>%
  {.$versions_end <- .$versions_end %>% `+`(6 - as.POSIXlt(.)$wday); .} %>%
  epix_slide(
    ~ .x,
    .versions = unique(.$DT$version) %>%
      `+`(6 - as.POSIXlt(.)$wday) %>%
      unique()
  ) %>%
  as_epi_archive() %>%
  revision_summary(confirmed_admissions_covid_ew)

# To determine what's semistable, do we first get a "stable proxy"
# (anchor lag, or unfiltered latest if no anchor lag?  or something
# checking for actual stability? may have the latter in
# revision_summary already, though there are issues with it on this
# data set being worked around above...), then check when real-time
# (relt 0 relv k minus relt -1 stable-proxy starts being a more
# accurate estimate of relt 0 stable-proxy - relt -1 stable-proxy than
# zero?  Or something with relt -1 relv k rather than relt -1
# stable-proxy?)

# Worry / pre-optimization: what about sparse (even datetime?) lags
# and poor performance from completion?  Then may not be able to
# follow extract_tvoffset.

# 0: 10 5 5 6 7
# 1:  6   3 5 4
# 2:  3 3 0 1 3
# 3:  1 2   1 3
# 4:  0 1   0 0
# 5:    0   1
# 6:        0

ek_names <- key_colnames(full_archive, exclude = c("time_value", "version"))
ekt_names <- c(ek_names, "time_value")

DT <- full_archive$DT[, c(key_colnames(full_archive), target), with = FALSE]

DT[DT, on = .(geo_value, time_value, version < version)]
# ^ XXX need substitute/bquote/rlang nonsense to get other_keys in here
# DT[DT, on = .(geo_value, time_value, x.version < i.version)] # data.table doesn't allow x. here
# DT[DT, on = .(geo_value, time_value, version < i.version)] # data.table doesn't allow i. here

DT[DT, on = c(ekt_names, "version < version")]
# ^ use this + stable-proxy to track error reductions?

DT[DT, on = c(ekt_names, "version > version")]
# XXX why is this the same? but idea here is to check instead
# deviations from more mature, without comparing against stable-proxy
#
# "In non-equi joins, the left side of the operator (e.g., x_int in
# x_int >= i_int) must be a column from x, while the right side (e.g.,
# i_int) must be a column from i."...

# Probably a better worry: initial data dump.  Do we need confkeys?
# (so maybe do want extract_tvoffset).  Do we need filtering/etc. to
# account for missing higher vlag data around most recent ts?
