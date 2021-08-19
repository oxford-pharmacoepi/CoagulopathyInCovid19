
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

if(run.as.test==TRUE){
exposure.cohorts <-  head(exposure.cohorts, 1)
}

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
if(nrow(study.cohorts %>% 
  filter(cohort_definition_id %in% 
           c(exposure.cohorts %>% 
  filter(name %in% 
           c("COVID19 diagnosis narrow",
             "COVID19 PCR positive test")) %>% 
  select(id) %>% pull())) 
  ) > 0 ){

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
}


# diagnosis narrow or pcr positive
if(nrow(study.cohorts %>% 
  filter(cohort_definition_id %in% 
           c(exposure.cohorts %>% 
  filter(name %in% 
           c("COVID19 diagnosis narrow hosp",
             "COVID19 PCR positive test hosp")) %>% 
  select(id) %>% pull())) 
  ) > 0 ){

diag_narrow_pcr_test_positive.hosp <- study.cohorts %>% 
  filter(cohort_definition_id %in% 
           c(exposure.cohorts %>% 
  filter(name %in% 
           c("COVID19 diagnosis narrow hosp",
             "COVID19 PCR positive test hosp")) %>% 
  select(id) %>% pull())) %>% 
  arrange(subject_id, cohort_start_date) %>% 
  group_by(subject_id) %>% 
  mutate(seq=1:length(subject_id)) %>% 
  filter(seq==1) %>% 
  select(-seq) %>% 
  mutate(cohort_definition_id=max(exposure.cohorts$id)+2)

conn <- connect(connectionDetails)
insertTable(connection=conn,
            tableName=paste0(results_database_schema, ".",cohortTableExposures),
            data=diag_narrow_pcr_test_positive.hosp,
            dropTableIfExists=FALSE,
            createTable = FALSE,
            progressBar=TRUE)
disconnect(conn)
}


} else {
  print(paste0("Skipping creating exposure cohorts")) 
}

exposure.cohorts<-bind_rows(exposure.cohorts,
                            tibble(id=as.integer(max(exposure.cohorts$id)+1),
                                   file=NA,
                                   name="COVID19 diagnosis narrow or PCR positive test"))

exposure.cohorts<-bind_rows(exposure.cohorts,
                            tibble(id=as.integer(max(exposure.cohorts$id)+2),
                                   file=NA,
                                   name="COVID19 diagnosis narrow or PCR positive test hosp"))

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
# exposure.cohorts

# instantiate outcome cohorts -----
cohort.sql<-list.files(here("Cohorts","OutcomeCohorts"))
cohort.sql<-cohort.sql[cohort.sql!="CreateCohortTable.sql"]

outcome.cohorts<-tibble(id=as.integer(1:length(cohort.sql)),
                         file=cohort.sql,
                         name=str_replace(cohort.sql, ".sql", "")) 

if(run.as.test==TRUE){
outcome.cohorts <-  head(outcome.cohorts, 1)
}

if(create.outcome.cohorts==TRUE){
print(paste0("- Getting outcome cohorts"))

conn <- connect(connectionDetails)
# create empty cohorts table
sql<-readSql(here("Cohorts","ExposureCohorts","sql","CreateCohortTable.sql"))
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

if(run.as.test==TRUE){
cond.codes<-c("434621")
cond.names<-c("autoimmune_disease")
}


if(create.profile.cohorts==TRUE){
# add the concept ids of interest to the cohortTableProfiles table in the results 
# schema in the cdm
# these will be the code of interest and all of its descendants
print(paste0("-- Getting conditions"))

# template sql

conn <- connect(connectionDetails)
# create empty cohorts table
sql<-readSql(here("Cohorts","ExposureCohorts","sql","CreateCohortTable.sql"))
sql<-SqlRender::translate(sql, targetDialect = targetDialect)
renderTranslateExecuteSql(conn=conn, 
                          sql,
                          cohort_database_schema =  results_database_schema,
                          cohort_table = cohortTableComorbidities)
rm(sql)

for(cohort.i in 1:length(cond.codes)){
  
  working.id<-cond.codes[cohort.i]
  print(paste0("-- Getting: ",  cond.names[cohort.i],
               " (", cohort.i, " of ", length(cond.names), ")"))
  
  sql<-readSql(here("Cohorts","ComorbidityCohorts","sql", "Condition_template.sql")) 
  sql <- sub("BEGIN: Inclusion Impact Analysis - event.*END: Inclusion Impact Analysis - person", "", sql)
  sql<-SqlRender::translate(sql, targetDialect = targetDialect)
  renderTranslateExecuteSql(conn=conn, 
                            sql, 
                            cdm_database_schema = cdm_database_schema,
                            Condition_top_code = cond.codes[cohort.i],
                            vocabulary_database_schema = vocabulary_database_schema,
                            target_database_schema = results_database_schema,
                            target_cohort_table = cohortTableComorbidities,
                            target_cohort_id = as.integer(working.id))  
}


# smoking
working.id<-"1111"
print(paste0("-- Getting: smoking"))
  
  sql<-readSql(here("Cohorts","ComorbidityCohorts","sql", "smoking.sql")) 
  sql <- sub("BEGIN: Inclusion Impact Analysis - event.*END: Inclusion Impact Analysis - person", "", sql)
  sql<-SqlRender::translate(sql, targetDialect = targetDialect)
  renderTranslateExecuteSql(conn=conn, 
                            sql, 
                            cdm_database_schema = cdm_database_schema,
                            vocabulary_database_schema = vocabulary_database_schema,
                            target_database_schema = results_database_schema,
                            target_cohort_table = cohortTableComorbidities,
                            target_cohort_id = as.integer(working.id))  
disconnect(conn)
} else {
  print(paste0("Skipping creating comorbidity cohorts")) 
}

# add smoking to list
cond.codes<-c(cond.codes, "1111")
cond.names<-c(cond.names,"smoking") 


# link to table
cohortTableComorbidities_db<-tbl(db, sql(paste0("SELECT * FROM ",
                                           results_database_schema,
                                           ".", cohortTableComorbidities)))%>% 
  mutate(cohort_definition_id=as.integer(cohort_definition_id)) 

cohortTableComorbidities_db %>%
  group_by(cohort_definition_id) %>%
  tally()

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


if(run.as.test==TRUE){
drug.codes<-c("21603933")
drug.names<-c("antiinflamatory_and_antirheumatic")
}




if(create.profile.cohorts==TRUE){
# add the concept ids of interest to the cohortTableProfiles table in the results 
# schema in the cdm
# these will be the code of interest and all of its descendants
print(paste0("-- Getting medications"))

# template sql

conn <- connect(connectionDetails)
# create empty cohorts table
sql<-readSql(here("Cohorts","ExposureCohorts","sql","CreateCohortTable.sql"))
sql<-SqlRender::translate(sql, targetDialect = targetDialect)
renderTranslateExecuteSql(conn=conn, 
                          sql,
                          cohort_database_schema =  results_database_schema,
                          cohort_table = cohortTableMedications)
rm(sql)

for(cohort.i in 1:length(drug.codes)){
  
  working.id<-drug.codes[cohort.i] 
  print(paste0("-- Getting: ",  drug.names[cohort.i],
               " (", cohort.i, " of ", length(drug.codes), ")"))
  
  sql<-readSql(here("Cohorts","MedicationCohorts","sql", "Medication_template.sql")) 
  sql <- sub("BEGIN: Inclusion Impact Analysis - event.*END: Inclusion Impact Analysis - person", "", sql)
  sql<-SqlRender::translate(sql, targetDialect = targetDialect)
  renderTranslateExecuteSql(conn=conn, 
                            sql, 
                            cdm_database_schema = cdm_database_schema,
                            Medication_top_code = drug.codes[cohort.i],
                            vocabulary_database_schema = vocabulary_database_schema,
                            target_database_schema = results_database_schema,
                            target_cohort_table = cohortTableMedications,
                            target_cohort_id = as.integer(working.id))  
}
disconnect(conn)
} else {
  print(paste0("Skipping creating medication cohorts")) 
}

# link to table
cohortTableMedications_db<-tbl(db, sql(paste0("SELECT * FROM ",
                                           results_database_schema,
                                           ".", cohortTableMedications)))%>% 
  mutate(cohort_definition_id=as.integer(cohort_definition_id)) 

# cohortTableMedications_db %>%
#   group_by(cohort_definition_id) %>%
#   tally()
