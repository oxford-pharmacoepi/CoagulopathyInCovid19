
# instantiate exposure cohorts -----


cohort.sql<-list.files(here("Cohorts","ExposureCohorts", "sql"))
cohort.sql<-cohort.sql[cohort.sql!="CreateCohortTable.sql"]

if(run.outpatient==FALSE){
cohort.sql<-cohort.sql[str_detect(cohort.sql, "hosp")]
}

if(run.hospitalised==FALSE){
cohort.sql<-cohort.sql[str_detect(cohort.sql, "hosp", negate = TRUE)]
}

exposure.cohorts<-tibble(id=as.integer(1:length(cohort.sql)),
                        file=cohort.sql,
                        name=str_replace(cohort.sql, ".sql", ""))  

if(create.exposure.cohorts==TRUE){
print(paste0("- Getting exposure cohorts"))
  
conn <- connect(connectionDetails)
# create empty cohorts table
sql<-readSql(here("Cohorts","ExposureCohorts","sql","CreateCohortTable.sql"))
sql<-SqlRender::translate(sql, targetDialect = targetDialect)
renderTranslateExecuteSql(conn=conn, 
                            sql,
                            cohort_database_schema =  results_database_schema,
                            cohort_table = cohortTableExposures)
rm(sql)
  
for(cohort.i in 1:length(exposure.cohorts$id)){
working.id<-exposure.cohorts$id[cohort.i]
print(paste0("-- Getting: ",  exposure.cohorts$name[cohort.i],
                 " (", cohort.i, " of ", length(exposure.cohorts$name), ")"))

sql<-readSql(here("Cohorts","ExposureCohorts","sql",exposure.cohorts$file[cohort.i])) 
sql <- sub("BEGIN: Inclusion Impact Analysis - event.*END: Inclusion Impact Analysis - person", "", sql)

sql<-SqlRender::translate(sql, targetDialect = targetDialect)
renderTranslateExecuteSql(conn=conn, 
                              sql, 
                              cdm_database_schema = cdm_database_schema,
                              vocabulary_database_schema = vocabulary_database_schema,
                              target_database_schema = results_database_schema,
                              # results_database_schema = results_database_schema,
                              target_cohort_table = cohortTableExposures,
                              target_cohort_id = working.id)  
  }
disconnect(conn)


# link to table
exposure.cohorts_db<-tbl(db, sql(paste0("SELECT * FROM ",
                                        results_database_schema,".",
                                        cohortTableExposures)))


# combined exposure cohorts
study.cohorts<-exposure.cohorts_db %>% 
  collect()

# diagnosis narrow or pcr positive
diag_narrow_pcr_test_positive <- study.cohorts %>% 
  filter(cohort_definition_id %in% 
           c(exposure.cohorts %>% 
  filter(name %in% 
           c("COVID19 diagnosis narrow",
             "COVID19 PCR positive test")) %>% 
  select(id) %>% pull())) %>% 
  arrange(subject_id, cohort_start_date) %>% 
  group_by(subject_id) %>% 
  mutate(seq=1:length(subject_id)) %>% 
  filter(seq==1) %>% 
  select(-seq) %>% 
  mutate(cohort_definition_id=max(exposure.cohorts$id)+1)

conn <- connect(connectionDetails)
insertTable(connection=conn,
            tableName=paste0(results_database_schema, ".",cohortTableExposures),
            data=diag_narrow_pcr_test_positive,
            dropTableIfExists=FALSE,
            createTable = FALSE,
            progressBar=TRUE)
disconnect(conn)
rm(study.cohorts)



} else {
  print(paste0("Skipping creating exposure cohorts")) 
}

exposure.cohorts<-bind_rows(exposure.cohorts,
                            tibble(id=as.integer(max(exposure.cohorts$id)+1),
                                   file=NA,
                                   name="COVID19 diagnosis narrow or PCR positive test"))

# link to table
exposure.cohorts_db<-tbl(db, sql(paste0("SELECT * FROM ",
                                        results_database_schema,".",
                                        cohortTableExposures))) %>% 
  mutate(cohort_definition_id=as.integer(cohort_definition_id)) 


# drop any exposure cohorts with less than 5 people
# exposure.cohorts_db %>% 
#   group_by(cohort_definition_id) %>% tally()

exposure.cohorts<-exposure.cohorts %>% 
  inner_join(exposure.cohorts_db %>% 
               group_by(cohort_definition_id) %>% 
               tally() %>% 
               collect() %>% 
               filter(n>5) %>% 
               select(cohort_definition_id),
             by=c("id"="cohort_definition_id"))

# instantiate outcome cohorts -----
cohort.sql<-list.files(here("Cohorts","OutcomeCohorts"))
cohort.sql<-cohort.sql[cohort.sql!="CreateCohortTable.sql"]
outcome.cohorts<-tibble(id=as.integer(1:length(cohort.sql)),
                         file=cohort.sql,
                         name=str_replace(cohort.sql, ".sql", "")) 
if(create.outcome.cohorts==TRUE){
print(paste0("- Getting outcome cohorts"))

conn <- connect(connectionDetails)
# create empty cohorts table
sql<-readSql(here("Cohorts","ExposureCohorts","CreateCohortTable.sql"))
sql<-SqlRender::translate(sql, targetDialect = targetDialect)
renderTranslateExecuteSql(conn=conn, 
                          sql,
                          cohort_database_schema =  results_database_schema,
                          cohort_table = cohortTableOutcomes)
rm(sql)

for(cohort.i in 1:length(outcome.cohorts$id)){
  working.id<-outcome.cohorts$id[cohort.i]
  print(paste0("-- Getting: ",  outcome.cohorts$name[cohort.i],
               " (", cohort.i, " of ", length(outcome.cohorts$name), ")"))
  
  sql<-readSql(here("Cohorts","OutcomeCohorts",outcome.cohorts$file[cohort.i])) 
  sql <- sub("BEGIN: Inclusion Impact Analysis - event.*END: Inclusion Impact Analysis - person", "", sql)
  sql<-SqlRender::translate(sql, targetDialect = targetDialect)
  renderTranslateExecuteSql(conn=conn, 
                            sql, 
                            cdm_database_schema = cdm_database_schema,
                            vocabulary_database_schema = vocabulary_database_schema,
                            target_database_schema = results_database_schema,
                            # results_database_schema = results_database_schema,
                            target_cohort_table = cohortTableOutcomes,
                            target_cohort_id = working.id)  
}
disconnect(conn)
} else {
  print(paste0("Skipping creating exposure cohorts")) 
}



# link to table
outcome.cohorts_db<-tbl(db, sql(paste0("SELECT * FROM ",
                                        results_database_schema,".",
                                        cohortTableOutcomes)))%>% 
  mutate(cohort_definition_id=as.integer(cohort_definition_id)) 

# drop any outcome cohorts with fewer than 5 people in database
outcome.cohorts<-outcome.cohorts %>% 
  inner_join(outcome.cohorts_db %>% 
               group_by(cohort_definition_id) %>% 
               tally() %>% 
               collect() %>% 
               filter(n>5) %>% 
               select(cohort_definition_id),
             by=c("id"="cohort_definition_id"))  
# for full analysis
outcome.cohorts<-outcome.cohorts %>% 
  filter(name %in%
           c("death",
             "DVT narrow", "PE", "VTE narrow",
             "MI", "isc stroke", "MI isc stroke", 
             "all stroke" , "MACE"))


# instantiate comorbidity cohorts ----
# for those people that are in our exposure cohorts

cond.codes<-c("434621", 
              "4098292", 
              "4125650",  
              "317009",   
              "313217",   
              "443392", 
              "201820",
              "433736",
              "321588",
              "316866",
              "4030518",
              "255573",
              "4182210")
cond.names<-c("autoimmune_disease",
              "antiphospholipid_syndrome",
              "thrombophilia",
              "asthma",
              "atrial_fibrillation",
              "malignant_neoplastic_disease",
              "diabetes_mellitus",
              "obesity",
              "heart_disease",
              "hypertensive_disorder",
              "renal_impairment",
              "copd",
              "dementia")

if(create.profile.cohorts==TRUE){
# add the concept ids of interest to the cohortTableProfiles table in the results 
# schema in the cdm
# these will be the code of interest and all of its descendants
print(paste0("-- Getting codes for conditions"))
conn<-connect(connectionDetails)
# table with all the concept ids of interest (code and descendants)
insertTable(connection=conn,
            tableName=paste0(results_database_schema, ".",cohortTableComorbidities),
            data=data.frame(condition_id=integer(), concept_id =integer()),
            createTable = TRUE,
            progressBar=FALSE)
for(n in 1:length(cond.codes)){ # add codes for each condition
  working.code<-cond.codes[n]
  working.name<-cond.names[n]
  sql<-paste0("INSERT INTO ", results_database_schema, ".",cohortTableComorbidities, " (condition_id, concept_id) SELECT DISTINCT ", n ,", descendant_concept_id FROM ", vocabulary_database_schema, ".concept_ancestor WHERE ancestor_concept_id IN (",working.code, ");")
  suppressMessages(executeSql(conn, sql, progressBar = FALSE))
}
disconnect(conn)

#link to table
cohortTableComorbidities_db<-tbl(db, sql(paste0("SELECT * FROM ",
                                           results_database_schema,
                                           ".", cohortTableComorbidities)))
print(paste0("-- Getting conditions for study population"))

#people with at least one of the conditions
cond.persons <- condition_occurrence_db %>%
  select(person_id, condition_concept_id, condition_start_date) %>% 
  inner_join(cohortTableComorbidities_db ,
             by=c("condition_concept_id"="concept_id"))
# keep first record per condition group
cond.persons <- cond.persons %>% 
  select(person_id, condition_id, condition_start_date) %>% 
  group_by(person_id, condition_id) %>% 
  mutate(seq=row_number()) %>% 
  filter(seq==1) %>% 
  ungroup() %>% 
  select(-seq)
# keep if in at least one of the exposure cohorts
cond.persons <- cond.persons %>% 
  inner_join(exposure.cohorts_db %>% 
               select(subject_id) %>% 
               distinct() %>% 
               rename("person_id"="subject_id"))
cond.persons<-cond.persons %>% collect()

# insert into db
conn<-connect(connectionDetails)
# drop table with codeses
sql<-paste("drop table ", paste0(results_database_schema, ".",cohortTableComorbidities))
sql<-SqlRender::translate(sql, targetDialect = targetDialect)
renderTranslateExecuteSql(conn=conn,  sql)  
# add cond.persons
insertTable(connection=conn,
            tableName=paste0(results_database_schema, ".",cohortTableComorbidities),
            data=cond.persons,
            createTable = TRUE,
            progressBar=TRUE)
disconnect(conn)
} else {
  print(paste0("Skipping creating profile cohorts")) 
  
  cohortTableComorbidities_db<-tbl(db, sql(paste0("SELECT * FROM ",
                                                  results_database_schema,
                                                  ".", cohortTableComorbidities)))
  }
# we now have a table summarising comorbidities
# cohortTableComorbidities_db %>% 
#   group_by(condition_id) %>% 
#   tally()

# instantiate medication cohorts ----
# for those people that are in our exposure cohorts
drug.codes<-c("21603933", 
              "21603991",
              "21602722",
              "21600961",
              "21601853",
              "21601386",
              "21602472",
              "21603831",
              "21602471",
              "21601254")
drug.names<-c("antiinflamatory_and_antirheumatic", 
              "coxibs",
              "corticosteroids",
              "antithrombotic",
              "lipid_modifying",
              "antineoplastic_immunomodulating",
              "hormonal_contraceptives",
              "tamoxifen",
              "sex_hormones_modulators",
              "immunoglobulins")

if(create.profile.cohorts==TRUE){ 
# add the concept ids of interest to the table in the results 
# schema in the cdm
# these will be the code of interest and all of its descendants
print(paste0("-- Getting codes for medications"))
conn<-connect(connectionDetails)
# table with all the concept ids of interest (code and descendants)
insertTable(connection=conn,
            tableName=paste0(results_database_schema, ".",cohortTableMedications),
            data=data.frame(drug_id=integer(), concept_id =integer()),
            createTable = TRUE,
            progressBar=FALSE)
for(n in 1:length(drug.codes)){ # add codes for each condition
  working.code<-drug.codes[n]
  working.name<-drug.names[n]
  sql<-paste0("INSERT INTO ", results_database_schema, ".",cohortTableMedications, " (drug_id, concept_id) SELECT DISTINCT ", n ,", descendant_concept_id FROM ", vocabulary_database_schema, ".concept_ancestor WHERE ancestor_concept_id IN (",working.code, ");")
  suppressMessages(executeSql(conn, sql, progressBar = FALSE))
}
disconnect(conn)

#link to table
cohortTableMedications_db<-tbl(db, sql(paste0("SELECT * FROM ",
                                                results_database_schema,
                                                ".", cohortTableMedications)))
print(paste0("-- Getting medications for study population"))

#people with at least one of the medications with era that ended in 2019 at the latest 
working.date<-as.Date(dmy(paste0("01-01-","2019")))
drug.persons <- drug_era_db %>%
  filter(drug_era_end_date>=working.date) %>% 
  select(person_id, drug_concept_id, drug_era_start_date, drug_era_end_date) %>% 
  inner_join(cohortTableMedications_db ,
             by=c("drug_concept_id"="concept_id"))
# keep if in at least one of the exposure cohorts
drug.persons <- drug.persons %>% 
  inner_join(exposure.cohorts_db %>% 
               select(subject_id) %>% 
               distinct() %>% 
               rename("person_id"="subject_id"))
drug.persons<-drug.persons %>% collect()

# insert into db
conn<-connect(connectionDetails)
# drop table with codes
sql<-paste("drop table ", paste0(results_database_schema, ".",cohortTableMedications))
sql<-SqlRender::translate(sql, targetDialect = targetDialect)
renderTranslateExecuteSql(conn=conn,  sql)  
# add drug.persons
drug.persons$drug_era_start_date<-as.Date(drug.persons$drug_era_start_date)
drug.persons$drug_era_end_date<-as.Date(drug.persons$drug_era_end_date)
insertTable(connection=conn,
            tableName=paste0(results_database_schema, ".",cohortTableMedications),
            data=drug.persons,
            createTable = TRUE,
            progressBar=TRUE)
disconnect(conn)
} else {
  cohortTableMedications_db<-tbl(db, sql(paste0("SELECT * FROM ",
                                                results_database_schema,
                                                ".", cohortTableMedications)))
}
# # we now have a table summarising medications
# cohortTableMedications_db%>%
#   group_by(drug_id) %>%
#   tally()
