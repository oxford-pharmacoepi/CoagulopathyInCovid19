# functions ----
# printing numbers with 1 decimal place and commas 
nice.num<-function(x) {
  trimws(format(round(x,1),
                big.mark=",", nsmall = 1, digits=1, scientific=FALSE))}
# printing numbers with 2 decimal place and commas 
nice.num2<-function(x) {
  trimws(format(round(x,2),
                big.mark=",", nsmall = 2, digits=2, scientific=FALSE))}
# for counts- without decimal place
nice.num.count<-function(x) {
  trimws(format(x,
                big.mark=",", nsmall = 0, digits=0, scientific=FALSE))}


# link to db tables -----
person_db<-tbl(db, sql(paste0("SELECT * FROM ",
                              cdm_database_schema,
                              ".person")))
observation_period_db<-tbl(db, sql(paste0("SELECT * FROM ",
                                          cdm_database_schema,
                                          ".observation_period")))
visit_occurrence_db<-tbl(db, sql(paste0("SELECT * FROM ",
                                        cdm_database_schema,
                                        ".visit_occurrence")))
condition_occurrence_db<-tbl(db, sql(paste0("SELECT * FROM ",
                                            cdm_database_schema,
                                            ".condition_occurrence")))
drug_era_db<-tbl(db, sql(paste0("SELECT * FROM ",
                                cdm_database_schema,
                                ".drug_era")))
concept_db<-tbl(db, sql(paste0("SELECT * FROM ",
                                        vocabulary_database_schema,
                                        ".concept")))
concept_ancestor_db<-tbl(db, sql(paste0("SELECT * FROM ",
                                        vocabulary_database_schema,
                                        ".concept_ancestor")))

if(mortality.captured==TRUE){
death_db<-tbl(db, sql(paste0("SELECT * FROM ",
                                cdm_database_schema,
                                ".death")))
}

# instantiate study cohorts ----
source(here("Cohorts","InstantiateStudyCohorts.R"))

# get database end date -----
db.end.date<-observation_period_db %>% 
    summarise(max(observation_period_end_date, na.rm=TRUE)) %>% 
    collect() %>%  pull()

# Run analysis ----
source(here("Analysis","Analysis.R"))

# Tidy up and save ----
Survival.summary<-bind_rows(Survival.summary, .id = NULL)
Survival.summary$db<-db.name
Survival.summary<-Survival.summary %>% 
  group_by(group, strata, outcome,pop, pop.type,
           outcome.name,prior.obs.required, surv.type) %>% 
  mutate(cum.n.event=cumsum(n.event))





if (!file.exists(output.folder)){
  dir.create(output.folder, recursive = TRUE)}

save(Patient.characteristcis, 
     file = paste0(output.folder, "/Patient.characteristcis_", db.name, ".RData"))
save(Survival.summary, 
     file = paste0(output.folder, "/Survival.summary_", db.name, ".RData"))
save(Model.estimates, 
     file = paste0(output.folder, "/Model.estimates_", db.name, ".RData"))
save(Cohort.entry.plot.data, 
     file = paste0(output.folder, "/Cohort.entry.plot.data_", db.name, ".RData"))
save(Cohort.age.plot.data, 
     file = paste0(output.folder, "/Cohort.age.plot.data_", db.name, ".RData"))

# # zip results
print("Zipping results to output folder")
unlink(paste0(output.folder, "/OutputToShare_", db.name, ".zip"))
zipName <- paste0(output.folder, "/OutputToShare_", db.name, ".zip")


files<-c(paste0(output.folder, "/Patient.characteristcis_", db.name, ".RData"),
         paste0(output.folder, "/Survival.summary_", db.name, ".RData"),
         paste0(output.folder, "/Model.estimates_", db.name, ".RData"),
         paste0(output.folder, "/Cohort.entry.plot.data_", db.name, ".RData") ,
         paste0(output.folder, "/Cohort.age.plot.data_", db.name, ".RData")   )
files <- files[file.exists(files)==TRUE]
createZipFile(zipFile = zipName,
              rootFolder=output.folder,
              files = files)

print("Done!")
print("-- If all has worked, there should now be a zip folder with your results in the output folder to share")
print("-- Thank you for running the study!")
