library(here)
library(stringr)

## Copy in sql for exposure cohorts from cohort diagnostics
exposure.cohort.diag.path<-"/home/eburn/diagCovCoagExposures"
# path to the cohort diagnostics package

# copy in cohorts
# remove existing
unlink(paste0(here("Cohorts","ExposureCohorts"), "/*"))
# bring in current
sqls<-list.files(paste0(exposure.cohort.diag.path, "/inst/sql/sql_server"))
for(i in 1:length(sqls)){
  file.copy(from=paste0(exposure.cohort.diag.path, "/inst/sql/sql_server/",sqls[i]), 
            to=here("Cohorts","ExposureCohorts"), 
            overwrite = TRUE, recursive = FALSE, 
            copy.mode = TRUE)
}


outcome.cohort.diag.path<-"/home/eburn/CovCoagOutcomeDiagnostics-main/diagCovCoagOutcomes"
# path to the cohort diagnostics package

# copy in cohorts
# remove existing
unlink(paste0(here("Cohorts","OutcomeCohorts"), "/*"))
#bring in current
sqls<-list.files(paste0(outcome.cohort.diag.path, "/inst/sql/sql_server"))
# drop hosp cohorts
sqls<-sqls[str_detect(sqls,"hosp", negate = TRUE)]
# for now, work with a subset of outcomes of interest
sqls<-sqls[str_detect(sqls,paste("PE.sql", "DVT narrow.sql", "VTE narrow.sql", sep="|"))]
for(i in 1:length(sqls)){
  file.copy(from=paste0(outcome.cohort.diag.path, "/inst/sql/sql_server/",sqls[i]), 
            to=here("Cohorts","OutcomeCohorts"), 
            overwrite = TRUE, recursive = FALSE, 
            copy.mode = TRUE)
}
