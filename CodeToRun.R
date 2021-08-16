

#install.packages("renv") # if not already installed, install renv from CRAN
renv::restore() # this should prompt you to install the various packages required for the study
renv::activate()

# packages ------
library(SqlRender)
library(DatabaseConnector)
library(FeatureExtraction)
library(here)
library(lubridate)
library(stringr)
library(ggplot2)
library(DBI)
library(dbplyr)
library(dplyr)
library(tidyr)
library(kableExtra)
library(RSQLite)
library(rmarkdown)
library(tableone)
library(scales)
library(forcats)
library(epiR)
library(RPostgreSQL)
library(cmprsk)
library(mstate)
library(broom)
library(rms)
# please load the above packages 
# you should have them all available, with the required version, after
# having run renv::restore above

# set up  ------
output.folder<-here::here("output")
# the path to a folder (that exists) where the results from this analysis will be saved

oracleTempSchema<-NULL
connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "....",
                                                                server ="....",
                                                                user = "....",
                                                                password = "....",
                                                                port = "....",
                                                                pathToDriver = "....",)
db <- dbConnect("....")



# The sql dialect used (see with the OHDSI SqlRender package)
targetDialect <-"...." 

# The name of the schema that contains the OMOP CDM with patient-level data
cdm_database_schema<-"...." 
# The name of the schema that contains the vocabularies
vocabulary_database_schema<-"...." 
# The name of the schema where a results table will be created 
results_database_schema<-"...." 
# Tables to be created in your results schema for this analysis
# You can keep the above names or change them
# Note, any existing tables in your results schema with the same name will be overwritten
cohortTableExposures<-"CoagulopathyInCovid19Exposures"
cohortTableOutcomes <-"CoagulopathyInCovid19Outcomes"
cohortTableComorbidities<-"CoagulopathyInCovid19Comorbidities"
cohortTableMedications<-"CoagulopathyInCovid19Medications"

# The name/ acronym for your database (to be used in the titles of reports, etc)
db.name<-"...." 

# Is mortality captured in your database?
mortality.captured<-TRUE

# which analyses to run? (set to FALSE to not run)
# outpatient - general covid cohorts
# hospitalised - hostpiatlised covid paitents
run.outpatient<-TRUE
run.hospitalised<-FALSE

# if you have already created the cohorts, you can set these to FALSE to skip instantiating the cohorts again
create.exposure.cohorts<-TRUE
create.outcome.cohorts<-TRUE
create.profile.cohorts<-TRUE


# run the analysis ------
run.as.test<-FALSE

start<-Sys.time()
source(here("RunStudy.R"))
Sys.time()-start





