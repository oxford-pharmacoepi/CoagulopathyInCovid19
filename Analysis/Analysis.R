
# drop any exposure cohorts with less than 5 people -----
exposure.cohorts<-exposure.cohorts %>% 
  inner_join(exposure.cohorts_db %>% 
               group_by(cohort_definition_id) %>% 
               tally() %>% 
               collect() %>% 
               filter(n>5) %>% 
               select(cohort_definition_id),
             by=c("id"="cohort_definition_id"))  

# drop any outcome cohorts with fewer than 50 people -----
outcome.cohorts<-outcome.cohorts %>% 
  inner_join(outcome.cohorts_db %>% 
               group_by(cohort_definition_id) %>% 
               tally() %>% 
               collect() %>% 
               filter(n>50) %>% 
               select(cohort_definition_id),
             by=c("id"="cohort_definition_id"))  
# for full analysis
outcome.cohorts<-outcome.cohorts %>% 
  filter(name %in%
           c("death",
             "DVT narrow", "PE", "VTE narrow",
             "MI", "isc stroke", "MI isc stroke", 
             "all stroke" , "MACE"))

# Initiate lists to store output ----
Patient.characteristcis<-list() # to collect tables of characteristics
Survival.summary<-list() # to collect cumulative incidence estimates
Model.estimates<-list() # 

# run through exposures and outcomes ----
for(i in 1:length(exposure.cohorts$id)){ 
working.study.cohort.id<-  exposure.cohorts$id[i]
working.study.cohort<-  exposure.cohorts$name[i]
print(paste0("Running anyalsis for: ", working.study.cohort, " (", i, " of ", 
             length(exposure.cohorts$id), ")"))
  
# Create Pop df ----  
Pop<-person_db %>% 
  inner_join(exposure.cohorts_db %>% 
               filter(cohort_definition_id==working.study.cohort.id) %>% 
               select(subject_id,cohort_start_date) %>% 
               rename("person_id"="subject_id")) %>% 
  select(person_id,gender_concept_id, 
         year_of_birth, month_of_birth, day_of_birth,
         cohort_start_date)  %>% 
  left_join(observation_period_db %>% 
              select("person_id",  "observation_period_start_date", "observation_period_end_date") %>% 
              distinct()) %>% 
  collect()

# nrow(Pop %>% select(person_id) %>% distinct())/nrow(Pop)

# add age -----
Pop$age<- NA
if(sum(is.na(Pop$day_of_birth))==0 & sum(is.na(Pop$month_of_birth))==0){
  # if we have day and month 
  Pop<-Pop %>%
    mutate(age=floor(as.numeric((ymd(cohort_start_date)-
                                   ymd(paste(year_of_birth,
                                             month_of_birth,
                                             day_of_birth, sep="-"))))/365.25))
} else { 
  Pop<-Pop %>% 
    mutate(age= year(cohort_start_date)-year_of_birth)
}

# age age groups ----
Pop<-Pop %>% 
  mutate(age_gr=ifelse(age<20,  "<20",
           ifelse(age>=20 &  age<=44,  "20-44",
                       ifelse(age>=45 & age<=54,  "45-54",
                              ifelse(age>=55 & age<=64,  "55-64",
                                     ifelse(age>=65 & age<=74, "65-74", 
                                            ifelse(age>=75 & age<=84, "75-84",      
                                                   ifelse(age>=85, ">=85",
                                                          NA)))))))) %>% 
  mutate(age_gr= factor(age_gr, 
                        levels = c("<20","20-44","45-54", "55-64",
                                   "65-74", "75-84",">=85"))) 
table(Pop$age_gr, useNA = "always")

# wider age groups
Pop<-Pop %>% 
  mutate(age_gr2=ifelse(age<=44,  "<=44",
                        ifelse(age>=45 & age<=64,  "45-64",    
                               ifelse(age>=65, ">=65",
                                      NA)))) %>% 
  mutate(age_gr2= factor(age_gr2, 
                         levels = c("<=44", "45-64",">=65")))
table(Pop$age_gr2, useNA = "always")

# another alternative set of age groups
Pop<-Pop %>% 
  mutate(age_gr3= ifelse(age<20,  "<20",
                         ifelse(age>=20 &  age<=29,  "20-29",
                         ifelse(age>=30 &  age<=39,  "30-39",
                         ifelse(age>=40 &  age<=49,  "40-49",
                                ifelse(age>=50 &  age<=59,  "50-59",
                                       ifelse(age>=60 & age<=69,  "60-69",
                                              ifelse(age>=70 & age<=79,  "70-79",      
                                                     ifelse(age>=80, ">=80",
                                                            NA))))))))) %>% 
  mutate(age_gr3= factor(age_gr3, 
                         levels = c("<20", "20-29","30-39","40-49", "50-59",
                                    "60-69", "70-79",">=80")))
table(Pop$age_gr3, useNA = "always")


# add gender -----
#8507 male
#8532 female
Pop<-Pop %>% 
  mutate(gender= ifelse(gender_concept_id==8507, "Male",
                        ifelse(gender_concept_id==8532, "Female", NA ))) %>% 
  mutate(gender= factor(gender, 
                        levels = c("Male", "Female")))
table(Pop$gender, useNA = "always")

# if missing age or gender, drop ----
Pop<-Pop %>% 
  filter(!is.na(age))

Pop<-Pop %>% 
  filter(!is.na(gender))



# add prior observation time -----
Pop<-Pop %>%  
  mutate(prior_obs_days=as.numeric(difftime(cohort_start_date,
                                            observation_period_start_date,
                                            units="days"))) %>% 
  mutate(prior_obs_years=prior_obs_days/365.25)

# condition history ------
# add each condition to pop
for(n in 1:length(cond.codes)){# add each to Pop
  working.code<-cond.codes[n]
  working.name<-cond.names[n]
  
working.persons.all.history <- cohortTableComorbidities_db %>% 
    filter(condition_id==n) %>% 
    collect() %>% 
    inner_join(Pop %>% select(person_id,cohort_start_date),
               by = "person_id") %>% 
    filter(cohort_start_date>condition_start_date) %>% # before index date
    select(person_id) %>% 
    mutate(working.cond.all.hist=1)
working.persons.one.year <- cohortTableComorbidities_db %>% 
  filter(condition_id==n) %>% 
  collect() %>% 
  inner_join(Pop %>% select(person_id,cohort_start_date),
             by = "person_id") %>% 
  filter(condition_start_date<(cohort_start_date) & 
        condition_start_date>=(cohort_start_date-years(1))) %>% 
  select(person_id) %>% 
  mutate(working.cond.one.year=1)
working.persons.30.days <- cohortTableComorbidities_db %>% 
  filter(condition_id==n) %>% 
  collect() %>% 
  inner_join(Pop %>% select(person_id,cohort_start_date),
             by = "person_id") %>% 
  filter(condition_start_date<(cohort_start_date) & 
           condition_start_date>=(cohort_start_date-days(30))) %>% 
  select(person_id) %>% 
  mutate(working.cond.30.days=1)
  
if(nrow(working.persons.all.history)>0){
    Pop<-Pop %>%
      left_join(working.persons.all.history,
                by = "person_id") %>% 
      rename(!!paste0(working.name, ".all.history"):="working.cond.all.hist")
  } else {
    Pop$working.cond.all.hist<-0
    Pop<-Pop %>% 
      rename(!!paste0(working.name, ".all.history"):="working.cond.all.hist")
  }

if(nrow(working.persons.one.year)>0){
  Pop<-Pop %>%
    left_join(working.persons.one.year,
              by = "person_id") %>% 
    rename(!!paste0(working.name, ".one.year"):="working.cond.one.year")
} else {
  Pop$working.cond.one.year<-0
  Pop<-Pop %>% 
    rename(!!paste0(working.name, ".one.year"):="working.cond.one.year")
}

if(nrow(working.persons.30.days)>0){
  Pop<-Pop %>%
    left_join(working.persons.30.days,
              by = "person_id") %>% 
    rename(!!paste0(working.name, ".30.days"):="working.cond.30.days")
} else {
  Pop$working.cond.30.days<-0
  Pop<-Pop %>% 
    rename(!!paste0(working.name, ".30.days"):="working.cond.30.days")
}
  
}
#to zero if absent
Pop<-Pop %>%
  mutate(across(all_of(paste0(cond.names, ".all.history")), ~ replace_na(.x, 0))) %>%
  mutate(across(all_of(paste0(cond.names, ".one.year")), ~ replace_na(.x, 0))) %>%
  mutate(across(all_of(paste0(cond.names, ".30.days")), ~ replace_na(.x, 0)))

# medication history -----
# 1) 183 days prior to four days prior index date
# 2) on index date - new user, non-user, prevalent user

# add each medication to pop
for(n in 1:length(drug.codes)){# add each to Pop
  working.code<-drug.codes[n]
  working.name<-drug.names[n]
  
  working.persons <- cohortTableMedications_db %>% 
    filter(drug_id==n) %>% 
    collect() %>% 
    inner_join(Pop %>% select(person_id,cohort_start_date),
               by = "person_id") %>% 
    filter((drug_era_start_date<=(cohort_start_date-days(4))
           & drug_era_start_date>=(cohort_start_date-days(183))) |
             (drug_era_end_date<=(cohort_start_date-days(4))
           & drug_era_end_date>=(cohort_start_date-days(183)))) %>% 
    select(person_id) %>% 
    mutate(working.drug=1) %>% 
    distinct()
  
  
working.persons.before.index<- cohortTableMedications_db %>% 
    filter(drug_id==n) %>% 
    collect() %>% 
    inner_join(Pop %>% select(person_id,cohort_start_date),
               by = "person_id") %>% 
    filter(drug_era_start_date<(cohort_start_date) &
             drug_era_end_date>=(cohort_start_date)) %>% 
    select(person_id) %>% 
    mutate(working.drug.before.index=1) %>% 
    distinct()  
  
working.persons.on.index<- cohortTableMedications_db %>% 
    filter(drug_id==n) %>% 
    collect() %>% 
    inner_join(Pop %>% select(person_id,cohort_start_date),
               by = "person_id") %>% 
    filter(drug_era_start_date<=(cohort_start_date) &
          drug_era_end_date>=(cohort_start_date)) %>% 
    select(person_id) %>% 
    mutate(working.drug.on.index=1) %>% 
    distinct()
  
working.persons.on.index<-working.persons.on.index %>% 
    left_join(working.persons.before.index)
table(working.persons.on.index$working.drug.on.index,
      working.persons.on.index$working.drug.before.index, 
      useNA = "always")
  
working.persons.on.index<-working.persons.on.index %>% 
  mutate(working.drug.on.index=ifelse(is.na(working.drug.before.index),
                                            "New user", "Prevalent user"))

if(nrow(working.persons)>0){
    Pop<-Pop %>%
      left_join(working.persons,
                by = "person_id") %>% 
      rename(!!working.name:="working.drug")
  } else {
    Pop$working.drug<-0
    Pop<-Pop %>% 
      rename(!!working.name:="working.drug")
  }
  
if(nrow(working.persons.on.index)>0){
  Pop<-Pop %>%
    left_join(working.persons.on.index %>% 
                select(-working.drug.before.index),
              by = "person_id") %>% 
    rename(!!paste0(working.name, ".on.index"):="working.drug.on.index")
} else {
  Pop$working.drug.on.index<-0
  Pop<-Pop %>% 
    rename(!!paste0(working.name, ".on.index"):="working.drug.on.index")
}


}
#to zero if absent
Pop<-Pop %>%
  mutate(across(all_of(drug.names), ~ replace_na(.x, 0))) %>%
  mutate(across(all_of(paste0(drug.names, ".on.index")), ~ replace_na(.x, "Non-user")))

# composite condition and medication measure -----
c.names<-c("autoimmune_disease",
           "antiphospholipid_syndrome",
           "thrombophilia",
           "atrial_fibrillation",
           "malignant_neoplastic_disease",
           "diabetes_mellitus",
           "obesity",
           "renal_impairment")
c.names<-paste0(c.names, ".all.history")
d.names<-c("antiinflamatory_and_antirheumatic", 
           "coxibs",
           "corticosteroids",
           "hormonal_contraceptives",
           "tamoxifen",
           "sex_hormones_modulators")

Pop$cond.comp<-rowSums(Pop %>% select(all_of(c.names))) 
Pop$drug.comp<-rowSums(Pop %>% select(all_of(d.names))) 
Pop$cond.drug.comp<-rowSums(Pop %>% select(all_of(c(c.names,  d.names)))) 

Pop<-Pop %>% 
  mutate(cond.comp=ifelse(cond.comp==0, 0, 1))%>% 
  mutate(drug.comp=ifelse(drug.comp==0, 0, 1))%>% 
  mutate(cond.drug.comp=ifelse(cond.drug.comp==0, 0, 1))


# summarise characteristics -----
get.summary.characteristics<-function(working.data, working.name){

summary.characteristics1<-data.frame(Overall=t(working.data %>% 
                           mutate(index_year=year(cohort_start_date)) %>% 
                           summarise(n=nice.num.count(length(person_id)),
                                     age=paste0(nice.num.count(median(age)),  " [",
                                                nice.num.count(quantile(age,probs=0.25)),  " to ",
                                                nice.num.count(quantile(age,probs=0.75)),   "]" ), 
                                     age.under.20=paste0(nice.num.count(sum(age_gr3=="<20")),
                                                      " (",  nice.num((sum(age_gr3=="<20")/length(person_id))*100),  "%)"),
                                     age.20_29=paste0(nice.num.count(sum(age_gr3=="20-29")),
                                                      " (",  nice.num((sum(age_gr3=="20-29")/length(person_id))*100),  "%)"),
                                     age.30_39=paste0(nice.num.count(sum(age_gr3=="30-39")),
                                                      " (",  nice.num((sum(age_gr3=="30-39")/length(person_id))*100),  "%)"), 
                                     age.40_49=paste0(nice.num.count(sum(age_gr3=="40-49")),
                                                      " (",  nice.num((sum(age_gr3=="40-49")/length(person_id))*100),  "%)"), 
                                     age.50_59=paste0(nice.num.count(sum(age_gr3=="50-59")),
                                                      " (",  nice.num((sum(age_gr3=="50-59")/length(person_id))*100),  "%)"), 
                                     age.60_69=paste0(nice.num.count(sum(age_gr3=="60-69")),
                                                      " (",  nice.num((sum(age_gr3=="60-69")/length(person_id))*100),  "%)"), 
                                     age.70_79=paste0(nice.num.count(sum(age_gr3=="70-79")),
                                                      " (",  nice.num((sum(age_gr3=="70-79")/length(person_id))*100),  "%)"), 
                                     age.80u=paste0(nice.num.count(sum(age_gr3==">=80")),
                                                    " (",  nice.num((sum(age_gr3==">=80")/length(person_id))*100),  "%)"), 
                                     sex.male=paste0(nice.num.count(sum(gender=="Male")),
                                                     " (",  nice.num((sum(gender=="Male")/length(person_id))*100),
                                                     "%)"),
                                     prior_obs_years=paste0(nice.num(median(prior_obs_years)),
                                                            " [", nice.num(quantile(prior_obs_years,probs=0.25)), 
                                                            " to ", nice.num(quantile(prior_obs_years,probs=0.75)),   "]" ))
    ))

    # and all the conds and medications
summary.characteristics2<-data.frame(Overall=t(working.data %>% 
                           summarise_at(.vars = all_of(c(paste0(cond.names, ".all.history"),
                                                         paste0(cond.names, ".one.year"),
                                                         paste0(cond.names, ".30.days"),
                                                         drug.names)), 
                                        .funs = function(x, tot){
                                          paste0(nice.num.count(sum(x)), " (", nice.num((sum(x)/tot)*100), "%)")
                                        } , tot=nrow(working.data))) )
summary.characteristics3<-data.frame(Overall=t(working.data %>% 
                           summarise_at(.vars = all_of(c(paste0(drug.names, ".on.index"))), 
                                        .funs = function(x, tot){
                                          paste0(nice.num.count(sum(x=="New user")), " (", 
                                                 nice.num((sum(x=="New user")/tot)*100), "%)")
                                        } , tot=nrow(working.data))) )
row.names(summary.characteristics3)<-paste0(row.names(summary.characteristics3), ": New user")
summary.characteristics4<-data.frame(Overall=t(working.data %>% 
                           summarise_at(.vars = all_of(c(paste0(drug.names, ".on.index"))), 
                                        .funs = function(x, tot){
                                          paste0(nice.num.count(sum(x=="Prevalent user")), " (", 
                                                 nice.num((sum(x=="Prevalent user")/tot)*100), "%)")
                                        } , tot=nrow(working.data))) )
row.names(summary.characteristics4)<-paste0(row.names(summary.characteristics4), ": Prevalent user")

summary.characteristics5<-data.frame(Overall=t(working.data %>% 
                           summarise_at(.vars = all_of(c(paste0(drug.names, ".on.index"))), 
                                        .funs = function(x, tot){
                                          paste0(nice.num.count(sum(x=="Non-user")), " (", 
                                                 nice.num((sum(x=="Non-user")/tot)*100), "%)")
                                        } , tot=nrow(working.data))) ) 
row.names(summary.characteristics5)<-paste0(row.names(summary.characteristics5), ": Non-user")
summary.characteristics3_4.5<-bind_rows(summary.characteristics3,
                    summary.characteristics4,
                    summary.characteristics5)

r.names<-sort(row.names(summary.characteristics3_4.5))
summary.characteristics3_4.5<-data.frame(Overall=summary.characteristics3_4.5[ order(row.names(summary.characteristics3_4.5)), ])
row.names(summary.characteristics3_4.5)<-r.names
summary.characteristics6<-data.frame(Overall=t(working.data %>% 
                           summarise_at(.vars = all_of(c("cond.comp", "drug.comp", 
                                                         "cond.drug.comp")), 
                                        .funs = function(x, tot){
                                          paste0(nice.num.count(sum(x)), " (", nice.num((sum(x)/tot)*100), "%)")
                                        } , tot=nrow(working.data))) )
  


summary.characteristics<-bind_rows(summary.characteristics1,
                                   summary.characteristics2,
                                   summary.characteristics3_4.5,
                                   summary.characteristics6)
    
  summary.characteristics$Overall<-as.character(summary.characteristics$Overall)
  
  rownames(summary.characteristics)<-str_to_sentence(rownames(summary.characteristics))
  rownames(summary.characteristics)<-str_replace(rownames(summary.characteristics),
                                                 "Sex.male", "Sex: Male")
  rownames(summary.characteristics)<-str_replace(rownames(summary.characteristics),
                                                 "Prior_obs_years", "Years of prior observation time")
  rownames(summary.characteristics)<-str_replace_all(rownames(summary.characteristics) , "_", " ")
  
  #obscure any counts less than 5
  summary.characteristics$Overall<-
    ifelse(str_sub(summary.characteristics$Overall, 1, 2) %in%  c("1 ","2 ", "3 ","4 "),
           "<5",summary.characteristics$Overall)
  summary.characteristics$var<-row.names(summary.characteristics)
  row.names(summary.characteristics)<-1:nrow(summary.characteristics)
  
  summary.characteristics <-summary.characteristics %>% 
    mutate(var=ifelse(var=="Cond.comp",
                      "One or more condition of interest", var )) %>% 
    mutate(var=ifelse(var=="Drug.comp",
                      "One or more medication of interest", var )) %>% 
    mutate(var=ifelse(var=="Cond.drug.comp",
                      "One or more condition/ medication of interest", var ))
  
  
  
  summary.characteristics %>% 
    select(var, Overall) %>% 
    rename(!!working.name:=Overall)
  
}

# overall
Pop.summary.characteristics<-get.summary.characteristics(Pop, "Overall")
Pop.summary.characteristics.before.Septermber<-get.summary.characteristics(
  Pop %>% filter(cohort_start_date<as.Date("2020-09-01")) ,
  "Before Septermber 2020")
Pop.summary.characteristics.from.Septermber<-get.summary.characteristics(
  Pop %>% filter(cohort_start_date>=as.Date("2020-09-01")) ,
  "From Septermber 2020")



# those with a year of prior history
Pop.summary.characteristics.with.history<-get.summary.characteristics(
  Pop %>% filter(prior_obs_years>=1),
  "Overall")
Pop.summary.characteristics.before.Septermber.with.history<-get.summary.characteristics(
  Pop %>% filter(cohort_start_date<as.Date("2020-09-01")) %>% 
    filter(prior_obs_years>=1) ,
  "Overall")
Pop.summary.characteristics.from.Septermber.with.history<-get.summary.characteristics(
  Pop %>% filter(cohort_start_date>=as.Date("2020-09-01")) %>% filter(prior_obs_years>=1) ,
  "Overall")




# results for each outcome of interest -----
for(j in 1:length(outcome.cohorts$id)){ # for each outcome of interest
  working.outcome<-outcome.cohorts$id[j]
  working.outcome.name<-outcome.cohorts$name[j]
  
  print(paste0("- Getting ", working.outcome.name,
               " (", j, " of ", length(outcome.cohorts$id), ")"))
  working.Pop<-Pop 

# drop time-varying covariates, we will get them again for the index date of the event 
  working.Pop<-working.Pop %>% 
    select(-c("age", "age_gr", "age_gr2", "age_gr3", "prior_obs_years","prior_obs_days",
           "cond.comp", "drug.comp", "cond.drug.comp")) %>% 
    select(-all_of(paste0(cond.names, ".all.history"))) %>% 
    select(-all_of(paste0(cond.names, ".one.year"))) %>% 
    select(-all_of(paste0(cond.names, ".30.days"))) %>% 
    select(-all_of(paste0(drug.names, ".on.index"))) %>% 
    select(-all_of(drug.names))

  # event of interest
  if(working.outcome.name=="imm throm"){
    # for imm throm, either this specific cohort based on diagnosis codes or HIT (which included drug eras)
    ids<-c(outcome.cohorts %>% 
             filter(name=="HIT") %>% 
             select(id) %>% pull(),
           working.outcome)
    working.outcomes<-outcome.cohorts_db %>%
      filter(cohort_definition_id %in% 
               ids) %>%
      select(subject_id, cohort_start_date) %>% 
      collect()
  } else {
    working.outcomes<-outcome.cohorts_db %>%
      filter(cohort_definition_id %in% working.outcome) %>%
      select(subject_id, cohort_start_date) %>% 
      collect()
  }
  
  # thrombocytopenia window
  if(str_detect(working.outcome.name, "(with thrombocytopenia 10 days pre to 10 days post)")){
    thrombocyt.id<-outcome.cohorts %>% 
      filter(name=="thrombocyt") %>% 
      select(id) %>% pull() 
    thromb.outcomes<-outcome.cohorts_db %>%
      filter(cohort_definition_id ==thrombocyt.id) %>% 
      select(subject_id, cohort_start_date) %>% 
      rename("thromb.date"="cohort_start_date") %>% 
      collect()
    # find any outcomes with thrombocytopenia also observed
    working.outcomes<-working.outcomes %>% 
      inner_join(thromb.outcomes)
    # find any outcomes with thrombocytopenia in the time window
    working.outcomes$dtime<-as.numeric(difftime(working.outcomes$thromb.date,
                                                working.outcomes$cohort_start_date, units="days"))
    working.outcomes<-working.outcomes %>% 
      filter(dtime>=(-10)) %>% 
      filter(dtime<=10)
    
    working.outcomes<-working.outcomes %>% 
      select(-dtime) %>% 
      select(-thromb.date)
  }
  
  if(str_detect(working.outcome.name, "(with thrombocytopenia 42 days pre to 14 days post)")){
    thrombocyt.id<-outcome.cohorts %>% 
      filter(name=="thrombocyt") %>% 
      select(id) %>% pull() 
    thromb.outcomes<-outcome.cohorts_db %>%
      filter(cohort_definition_id ==thrombocyt.id) %>% 
      select(subject_id, cohort_start_date) %>% 
      rename("thromb.date"="cohort_start_date") %>% 
      collect()
    # find any outcomes with thrombocytopenia also observed
    working.outcomes<-working.outcomes %>% 
      inner_join(thromb.outcomes)
    # find any outcomes with thrombocytopenia in the time window
    working.outcomes$dtime<-as.numeric(difftime(working.outcomes$thromb.date,
                                                working.outcomes$cohort_start_date, units="days"))
    working.outcomes<-working.outcomes %>% 
      filter(dtime>=(-42)) %>% 
      filter(dtime<=14)
    
    working.outcomes<-working.outcomes %>% 
      select(-dtime) %>% 
      select(-thromb.date)
  }
  
  # any occurrences
  if(working.outcomes %>%  
     inner_join(working.Pop %>% 
                select(person_id) %>% 
                rename("subject_id"="person_id")) %>% 
     tally() %>% 
     pull()<5){
    print(paste0("Less than five occurrences of ", working.outcome.name, " among study population"))
  } else {
    # continue only if occurrences of outcome
    
    # drop anyone with the outcome in the year prior to the index, including index date itself ------
    washout.outcomes<-working.outcomes %>% 
      inner_join(working.Pop %>% 
                   select(person_id,cohort_start_date) %>% 
                   rename("subject_id"="person_id") %>% 
                   rename("Pop_cohort_start_date"="cohort_start_date")) %>% 
      filter(cohort_start_date>= (Pop_cohort_start_date-years(1))) %>% 
      filter(cohort_start_date<= Pop_cohort_start_date) # nb including index date itself
    
    working.Pop<-working.Pop %>% 
      anti_join(washout.outcomes %>% 
                  select(subject_id) %>% 
                  distinct(),
                by=c("person_id"="subject_id"))  
    
    # history of outcome event -----
    history.outcomes<-working.outcomes %>%  
      inner_join(working.Pop %>% 
                   select(person_id,cohort_start_date) %>% 
                   rename("subject_id"="person_id") %>% 
                   rename("Pop_cohort_start_date"="cohort_start_date")) %>% 
      filter(cohort_start_date< (Pop_cohort_start_date-years(1))) 
    working.Pop<-working.Pop %>% 
      left_join(history.outcomes  %>% 
                  select(subject_id) %>% 
                  distinct() %>% 
                  mutate(history_outcome=1),
                by=c("person_id"="subject_id"))
    working.Pop<-working.Pop %>% 
      mutate(history_outcome=ifelse(is.na(history_outcome),0,1))
    
    
    # first event after index date up to  -----
      # for covid: outcomes over 90 days from index
      f_u.outcome<-working.outcomes %>%  
        inner_join(working.Pop %>% 
                     select(person_id,cohort_start_date) %>% 
                     rename("subject_id"="person_id") %>% 
                     rename("Pop_cohort_start_date"="cohort_start_date"))  %>% 
        filter(cohort_start_date> Pop_cohort_start_date) %>% 
        filter(cohort_start_date<= (Pop_cohort_start_date+days(90))) 
    

    if(nrow(f_u.outcome)>=5){ 
      f_u.outcome<-f_u.outcome %>% 
        group_by(subject_id) %>%
        arrange(cohort_start_date) %>% 
        mutate(seq=1:length(subject_id)) %>% 
        filter(seq==1) %>% 
        select(subject_id,cohort_start_date)  %>% 
        rename("f_u.outcome_date"="cohort_start_date") %>% 
        mutate(f_u.outcome=1)
      working.Pop<-working.Pop %>% 
        left_join(f_u.outcome,
                  by=c("person_id"="subject_id"))
      working.Pop<-working.Pop %>% 
        mutate(f_u.outcome=ifelse(is.na(f_u.outcome),0,1))
      # sum(working.Pop$f_u.outcome)
      
# TAR -----
# CovidCohorts: censor at first of outcome, end of observation period, 90 days
working.Pop<-working.Pop %>%
          mutate(f_u.outcome_date=if_else(f_u.outcome==1,
                                          f_u.outcome_date, 
                                          if_else(observation_period_end_date < (cohort_start_date+days(90)),
                                                  observation_period_end_date, 
                                                  (cohort_start_date+days(90)) )))

      
working.Pop<-working.Pop %>% 
        mutate(f_u.outcome.days=as.numeric(difftime(f_u.outcome_date,
                                                    cohort_start_date, 
                                                    units="days")))
# working.Pop %>%
#         group_by(f_u.outcome) %>%
#         summarise(min(f_u.outcome.days),
#                   median(f_u.outcome.days),
#                   max(f_u.outcome.days))
      
# require minimum of 1 day of time at risk ----
working.Pop<-working.Pop %>% 
        filter(f_u.outcome.days>0)
      
# add age and gender -----
working.Pop$age<- NA
if(sum(is.na(working.Pop$day_of_birth))==0 & sum(is.na(working.Pop$month_of_birth))==0){
        # if we have day and month 
        working.Pop<-working.Pop %>%
          mutate(age=floor(as.numeric((ymd(cohort_start_date)-
                                         ymd(paste(year_of_birth,
                                                   month_of_birth,
                                                   day_of_birth, sep="-"))))/365.25))
      } else { 
        working.Pop<-working.Pop %>% 
          mutate(age= year(cohort_start_date)-year_of_birth)
      }
      
working.Pop<-working.Pop %>% 
  mutate(age_gr=ifelse(age<20,  "<20",
                       ifelse(age>=20 &  age<=44,  "20-44",
                              ifelse(age>=45 & age<=54,  "45-54",
                                     ifelse(age>=55 & age<=64,  "55-64",
                                            ifelse(age>=65 & age<=74, "65-74", 
                                                   ifelse(age>=75 & age<=84, "75-84",      
                                                          ifelse(age>=85, ">=85",
                                                                 NA)))))))) %>% 
  mutate(age_gr= factor(age_gr, 
                        levels = c("<20","20-44","45-54", "55-64",
                                   "65-74", "75-84",">=85"))) 
#table(working.Pop$age_gr, useNA = "always")
      
# wider age groups
working.Pop<-working.Pop %>% 
        mutate(age_gr2=ifelse(age<=44,  "<=44",
                              ifelse(age>=45 & age<=64,  "45-64",    
                                     ifelse(age>=65, ">=65",
                                            NA)))) %>% 
        mutate(age_gr2= factor(age_gr2, 
                               levels = c("<=44", "45-64",">=65")))
#table(working.Pop$age_gr2, useNA = "always")
      
# another alternative set of age groups
working.Pop<-working.Pop %>% 
  mutate(age_gr3= ifelse(age<20,  "<20",
                               ifelse(age>=20 &  age<=29,  "20-29",
                                      ifelse(age>=30 &  age<=39,  "30-39",
                                             ifelse(age>=40 &  age<=49,  "40-49",
                                                    ifelse(age>=50 &  age<=59,  "50-59",
                                                           ifelse(age>=60 & age<=69,  "60-69",
                                                                  ifelse(age>=70 & age<=79,  "70-79",      
                                                                         ifelse(age>=80, ">=80",
                                                                                NA))))))))) %>% 
        mutate(age_gr3= factor(age_gr3, 
                               levels = c("<20", "20-29","30-39","40-49", "50-59",
                                          "60-69", "70-79",">=80")))
# table(working.Pop$age_gr3, useNA = "always")

      
# add prior observation time -----
working.Pop<-working.Pop %>%  
        mutate(prior_obs_days=as.numeric(difftime(f_u.outcome_date,
                                                  observation_period_start_date,
                                                  units="days"))) %>% 
        mutate(prior_obs_years=prior_obs_days/365.25)
      
# condition history ------
# add each condition to pop
for(n in 1:length(cond.codes)){# add each to Pop
  working.code<-cond.codes[n]
  working.name<-cond.names[n]
  
  working.persons.all.history <- cohortTableComorbidities_db %>% 
    filter(condition_id==n) %>% 
    collect() %>% 
    inner_join(working.Pop %>% select(person_id,cohort_start_date),
               by = "person_id") %>% 
    filter(cohort_start_date>condition_start_date) %>% # before index date
    select(person_id) %>% 
    mutate(working.cond.all.hist=1)
  working.persons.one.year <- cohortTableComorbidities_db %>% 
    filter(condition_id==n) %>% 
    collect() %>% 
    inner_join(working.Pop %>% select(person_id,cohort_start_date),
               by = "person_id") %>% 
    filter(condition_start_date<(cohort_start_date) & 
             condition_start_date>=(cohort_start_date-years(1))) %>% 
    select(person_id) %>% 
    mutate(working.cond.one.year=1)
  working.persons.30.days <- cohortTableComorbidities_db %>% 
    filter(condition_id==n) %>% 
    collect() %>% 
    inner_join(working.Pop %>% select(person_id,cohort_start_date),
               by = "person_id") %>% 
    filter(condition_start_date<(cohort_start_date) & 
             condition_start_date>=(cohort_start_date-days(30))) %>% 
    select(person_id) %>% 
    mutate(working.cond.30.days=1)
  
  if(nrow(working.persons.all.history)>0){
    working.Pop<-working.Pop %>%
      left_join(working.persons.all.history,
                by = "person_id") %>% 
      rename(!!paste0(working.name, ".all.history"):="working.cond.all.hist")
  } else {
    Pop$working.cond.all.hist<-0
    working.Pop<-working.Pop %>% 
      rename(!!paste0(working.name, ".all.history"):="working.cond.all.hist")
  }
  
  if(nrow(working.persons.one.year)>0){
    working.Pop<-working.Pop %>%
      left_join(working.persons.one.year,
                by = "person_id") %>% 
      rename(!!paste0(working.name, ".one.year"):="working.cond.one.year")
  } else {
    working.Pop$working.cond.one.year<-0
    working.Pop<-working.Pop %>% 
      rename(!!paste0(working.name, ".one.year"):="working.cond.one.year")
  }
  
  if(nrow(working.persons.30.days)>0){
    working.Pop<-working.Pop %>%
      left_join(working.persons.30.days,
                by = "person_id") %>% 
      rename(!!paste0(working.name, ".30.days"):="working.cond.30.days")
  } else {
    working.Pop$working.cond.30.days<-0
    working.Pop<-working.Pop %>% 
      rename(!!paste0(working.name, ".30.days"):="working.cond.30.days")
  }
  
}
#to zero if absent
working.Pop<-working.Pop %>%
  mutate(across(all_of(paste0(cond.names, ".all.history")), ~ replace_na(.x, 0))) %>%
  mutate(across(all_of(paste0(cond.names, ".one.year")), ~ replace_na(.x, 0))) %>%
  mutate(across(all_of(paste0(cond.names, ".30.days")), ~ replace_na(.x, 0)))


# medication history -----
# 1) 183 days prior to four days prior index date
# 2) on index date - new user, non-user, prevalent user

# add each medication to pop
for(n in 1:length(drug.codes)){# add each to Pop
  working.code<-drug.codes[n]
  working.name<-drug.names[n]
  
  working.persons <- cohortTableMedications_db %>% 
    filter(drug_id==n) %>% 
    collect() %>% 
    inner_join(working.Pop %>% select(person_id,cohort_start_date),
               by = "person_id") %>% 
    filter((drug_era_start_date<=(cohort_start_date-days(4))
           & drug_era_start_date>=(cohort_start_date-days(183))) |
             (drug_era_end_date<=(cohort_start_date-days(4))
           & drug_era_end_date>=(cohort_start_date-days(183)))) %>% 
    select(person_id) %>% 
    mutate(working.drug=1) %>% 
    distinct()
  
  
working.persons.before.index<- cohortTableMedications_db %>% 
    filter(drug_id==n) %>% 
    collect() %>% 
    inner_join(working.Pop %>% select(person_id,cohort_start_date),
               by = "person_id") %>% 
    filter(drug_era_start_date<(cohort_start_date) &
             drug_era_end_date>=(cohort_start_date)) %>% 
    select(person_id) %>% 
    mutate(working.drug.before.index=1) %>% 
    distinct()  
  
working.persons.on.index<- cohortTableMedications_db %>% 
    filter(drug_id==n) %>% 
    collect() %>% 
    inner_join(working.Pop %>% select(person_id,cohort_start_date),
               by = "person_id") %>% 
    filter(drug_era_start_date<=(cohort_start_date) &
          drug_era_end_date>=(cohort_start_date)) %>% 
    select(person_id) %>% 
    mutate(working.drug.on.index=1) %>% 
    distinct()
  
working.persons.on.index<-working.persons.on.index %>% 
    left_join(working.persons.before.index)
table(working.persons.on.index$working.drug.on.index,
      working.persons.on.index$working.drug.before.index, 
      useNA = "always")
  
working.persons.on.index<-working.persons.on.index %>% 
  mutate(working.drug.on.index=ifelse(is.na(working.drug.before.index),
                                            "New user", "Prevalent user"))

if(nrow(working.persons)>0){
    working.Pop<-working.Pop %>%
      left_join(working.persons,
                by = "person_id") %>% 
      rename(!!working.name:="working.drug")
  } else {
    working.Pop$working.drug<-0
    working.Pop<-working.Pop %>% 
      rename(!!working.name:="working.drug")
  }
  
if(nrow(working.persons.on.index)>0){
  working.Pop<-working.Pop %>%
    left_join(working.persons.on.index %>% 
                select(-working.drug.before.index),
              by = "person_id") %>% 
    rename(!!paste0(working.name, ".on.index"):="working.drug.on.index")
} else {
  working.Pop$working.drug.on.index<-0
  working.Pop<-working.Pop %>% 
    rename(!!paste0(working.name, ".on.index"):="working.drug.on.index")
}


}
#to zero if absent
working.Pop<-working.Pop %>%
  mutate(across(all_of(drug.names), ~ replace_na(.x, 0))) %>%
  mutate(across(all_of(paste0(drug.names, ".on.index")), ~ replace_na(.x, "Non-user")))


# composite condition and medication measure -----
      working.Pop$cond.comp<-rowSums(working.Pop %>% select(all_of(c.names))) 
      working.Pop$drug.comp<-rowSums(working.Pop %>% select(all_of(d.names))) 
      working.Pop$cond.drug.comp<-rowSums(working.Pop %>% select(all_of(c(c.names,  d.names)))) 
      
      working.Pop<-working.Pop %>% 
        mutate(cond.comp=ifelse(cond.comp==0, 0,1))%>% 
        mutate(drug.comp=ifelse(drug.comp==0, 0,1))%>% 
        mutate(cond.drug.comp=ifelse(cond.drug.comp==0, 0,1))

# working.Pop.w.outcome -----
working.Pop.w.outcome<-working.Pop %>% filter(f_u.outcome==1) 
      
# summarise characteristics -----
# overall
Pop.summary.characteristics<-left_join(Pop.summary.characteristics,
                                             get.summary.characteristics(working.Pop.w.outcome, working.outcome.name),
                                             by="var")
Pop.summary.characteristics.before.Septermber<-left_join(Pop.summary.characteristics.before.Septermber,
                                             get.summary.characteristics(working.Pop.w.outcome %>% filter(cohort_start_date<as.Date("2020-09-01")),
                                                                         working.outcome.name),
                                             by="var")      
Pop.summary.characteristics.from.Septermber<-left_join(Pop.summary.characteristics.from.Septermber,
                                                     get.summary.characteristics(working.Pop.w.outcome %>% filter(cohort_start_date>=as.Date("2020-09-01")),
                                                                                 working.outcome.name),
                                                     by="var")           
      
#with.history
# overall
Pop.summary.characteristics.with.history<-left_join(Pop.summary.characteristics.with.history,
                                                          get.summary.characteristics(working.Pop.w.outcome %>% 
                                                                                        filter(prior_obs_years>=1), working.outcome.name),
                                                          by="var")
Pop.summary.characteristics.before.Septermber.with.history<-left_join(Pop.summary.characteristics.before.Septermber.with.history,
                                                     get.summary.characteristics(working.Pop.w.outcome %>% 
                                                                                   filter(prior_obs_years>=1) %>% filter(cohort_start_date<as.Date("2020-09-01")),
                                                                                 working.outcome.name),
                                                     by="var")      
Pop.summary.characteristics.from.Septermber.with.history<-left_join(Pop.summary.characteristics.from.Septermber.with.history,
                                                   get.summary.characteristics(working.Pop.w.outcome  %>% 
                                                                                 filter(prior_obs_years>=1)%>% filter(cohort_start_date>=as.Date("2020-09-01")),
                                                                               working.outcome.name),
                                                   by="var")   

    
# cumulative incidence set up ------
if(mortality.captured==TRUE & working.outcome.name!="death"){
# get deaths
deaths<-death_db %>% 
  select(person_id,death_date) %>% 
  inner_join(exposure.cohorts_db %>% 
               select(subject_id) %>% 
               rename("person_id"="subject_id")) %>% 
  collect()
working.Pop<-working.Pop %>% 
  left_join(deaths)
# only over tar
working.Pop$death.days<-as.numeric(difftime(working.Pop$death_date,
                               working.Pop$cohort_start_date, units="days"))
# quantile(working.Pop$death.days, na.rm=TRUE)
working.Pop<-working.Pop %>% 
  mutate(death_date=if_else(!is.na(death_date) & 
                             (cohort_start_date+days(death.days) > f_u.outcome_date),
                           as.Date(NA), death_date))
working.Pop$death.days<-as.numeric(difftime(working.Pop$death_date,
                               working.Pop$cohort_start_date, units="days"))
quantile(working.Pop$death.days, na.rm=TRUE)

# death status variable
working.Pop<-working.Pop %>% 
  mutate(death_status=ifelse(!is.na(death_date), 1,0))
# prop.table(table(working.Pop$death_status, useNA = "always"))

# 
working.Pop<-working.Pop %>% 
  mutate(death_date=if_else(!is.na(death_date),death_date,f_u.outcome_date))
working.Pop$death.days<-as.numeric(difftime(working.Pop$death_date,
                               working.Pop$cohort_start_date, units="days"))
# quantile(working.Pop$death.days, na.rm=TRUE)

# competing risk variable
working.Pop <- working.Pop %>% 
               mutate(w.outcome_or_mortality.etime=ifelse(f_u.outcome==0, 
                                       death.days, f_u.outcome.days))
working.Pop <- working.Pop %>% 
                   mutate(w.outcome_or_mortality.event=ifelse(f_u.outcome==0, 
                                       2*death_status, 1))
# table(working.Pop$w.outcome_or_mortality.event)
working.Pop <- working.Pop %>%
                  mutate(w.outcome_or_mortality.event=
                        factor(w.outcome_or_mortality.event,
                               0:2, labels=c("censor", "outcome of interest","death"))) 
# table(working.Pop$w.outcome_or_mortality.event)
# table(working.Pop$w.outcome_or_mortality.event)
# prop.table(table(working.Pop$w.outcome_or_mortality.event))
# prop.table(table(is.na(working.Pop$w.outcome_or_mortality.etime)))
# 
# working.Pop %>% 
#   group_by(w.outcome_or_mortality.event) %>% 
#   summarise(min(w.outcome_or_mortality.etime),
#              median(w.outcome_or_mortality.etime),
#             max(w.outcome_or_mortality.etime))

# mstate
tmat <- matrix(NA, 3, 3)
dimnames(tmat) <- list(from = c("at risk","event of interest","death"),
                       to = c("at risk","event of interest","death"))
tmat[1, 2:3]<- 1:2
 # to make sure event of interest is considered ascoming before death 
# working.Pop %>% 
#   summarise(sum(death.days==f_u.outcome.days &
#                 f_u.outcome==1 & death_status==1))
working.Pop<-working.Pop %>%
  mutate(f_u.outcome.days=ifelse(death.days==f_u.outcome.days &
                                   f_u.outcome==1 & 
                                   death_status==1, f_u.outcome.days-0.5, f_u.outcome.days))

r<-msprep(time = c(NA,
                   "f_u.outcome.days",
                   "death.days"),
          status = c(NA,
                     "f_u.outcome",
                     "death_status"),
          id="person_id",
          data = as.data.frame(working.Pop),
          trans = tmat)
events(r)

r.1<-r %>% 
  filter(trans==1) %>% #outcome of interest
  select(person_id, time, status) %>% 
  left_join(working.Pop)
r.2<-r %>% 
  filter(trans==2) %>% #competing event (death)
  select(person_id, time, status) %>% 
  left_join(working.Pop)
}

# Cumulative incidence ------
get.Surv.summaries<-function(working.data,
                             value.prior.obs.required,
                             value.pop.type,
                             value.working.outcome,
                             value.working.outcome.name,
                             value.working.study.cohort
                             ){
  # add to working.Survival.summary
  working.Survival.summary<-list()
  working.times<-seq(0,90)
  
  #overall
  #km method
  s<-summary(survfit(Surv(f_u.outcome.days, f_u.outcome) ~ 1, 
                     data = working.data), times = working.times)
  working.Survival.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                                   "overall")]]<-data.frame(
                                     time=s$time,
                                     n.risk=s$n.risk,
                                     n.event= s$n.event,
                                     surv=s$surv,
                                     lower=s$lower,
                                     upper=s$upper,
                                     group="overall")%>% 
    mutate(strata="overall") %>% 
    mutate(outcome=value.working.outcome) %>% 
    mutate(outcome.name=value.working.outcome.name) %>% 
    mutate(pop.type=value.pop.type) %>% 
    mutate(prior.obs.required=value.prior.obs.required)  %>% 
    mutate(pop=value.working.study.cohort) %>% 
    mutate(surv.type="KM")
  
   # competing risk 
if(mortality.captured==TRUE & working.outcome.name!="death"){
s <- survfit(Surv(w.outcome_or_mortality.etime, 
                      w.outcome_or_mortality.event) ~1 , 
                 data=working.data)
s.fit.summary <- summary(s, times=working.times)
  working.Survival.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                                   "overall",";","cr")]]<-data.frame(
   time=s.fit.summary$time,
   n.risk=s.fit.summary$n.risk[,1],
   n.event= s.fit.summary$n.event[,2],
   surv=s.fit.summary$pstate[,2],
   lower=s.fit.summary$lower[,2],
   upper=s.fit.summary$upper[,2],
   group="overall")  %>% 
    mutate(strata="overall") %>% 
    mutate(outcome=value.working.outcome) %>% 
    mutate(outcome.name=value.working.outcome.name) %>% 
    mutate(pop.type=value.pop.type) %>% 
    mutate(prior.obs.required=value.prior.obs.required)  %>% 
    mutate(pop=value.working.study.cohort) %>% 
    mutate(surv.type="Competing risk")
}
  
  #gender
  #km method  
  s<-summary(survfit(Surv(f_u.outcome.days, f_u.outcome) ~ gender, 
                     data = working.data), times = working.times)
  working.Survival.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,
                                   ";","gender")]]<-data.frame(
                                     time=s$time,
                                     n.risk=s$n.risk,
                                     n.event= s$n.event,
                                     surv=s$surv,
                                     lower=s$lower,
                                     upper=s$upper,
                                     group=s$strata)%>% 
    mutate(strata="gender") %>% 
    mutate(outcome=value.working.outcome) %>% 
    mutate(outcome.name=value.working.outcome.name) %>% 
    mutate(pop.type=value.pop.type) %>% 
    mutate(prior.obs.required=value.prior.obs.required)  %>% 
    mutate(pop=value.working.study.cohort)%>% 
    mutate(surv.type="KM")
  
   # competing risk 
  if(mortality.captured==TRUE & working.outcome.name!="death"){
s <- survfit(Surv(w.outcome_or_mortality.etime, 
                      w.outcome_or_mortality.event) ~gender , 
                 data=working.data)
s.fit.summary <- summary(s, times=working.times)
working.Survival.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                                   "gender",";","cr")]]<-data.frame(
   time=s.fit.summary$time,
   n.risk=s.fit.summary$n.risk[,1],
   n.event= s.fit.summary$n.event[,2],
   surv=s.fit.summary$pstate[,2],
   lower=s.fit.summary$lower[,2],
   upper=s.fit.summary$upper[,2],
   group=s.fit.summary$strata)  %>% 
    mutate(strata="gender") %>% 
    mutate(outcome=value.working.outcome) %>% 
    mutate(outcome.name=value.working.outcome.name) %>% 
    mutate(pop.type=value.pop.type) %>% 
    mutate(prior.obs.required=value.prior.obs.required)  %>% 
    mutate(pop=value.working.study.cohort) %>% 
    mutate(surv.type="Competing risk")  
  }
  
  
  
# cond.comp  
  s<-summary(survfit(Surv(f_u.outcome.days, f_u.outcome) ~ cond.comp, 
                     data = working.data), times = working.times)
  working.Survival.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,
                                   ";","cond.comp")]]<-data.frame(
                                     time=s$time,
                                     n.risk=s$n.risk,
                                     n.event= s$n.event,
                                     surv=s$surv,
                                     lower=s$lower,
                                     upper=s$upper,
                                     group=s$strata)%>% 
    mutate(strata="cond.comp") %>% 
    mutate(outcome=value.working.outcome) %>% 
    mutate(outcome.name=value.working.outcome.name) %>% 
    mutate(pop.type=value.pop.type) %>% 
    mutate(prior.obs.required=value.prior.obs.required)  %>% 
    mutate(pop=value.working.study.cohort)%>% 
    mutate(surv.type="KM")
  if(mortality.captured==TRUE & working.outcome.name!="death"){
s <- survfit(Surv(w.outcome_or_mortality.etime, 
                      w.outcome_or_mortality.event) ~cond.comp , 
                 data=working.data)
s.fit.summary <- summary(s, times=working.times)
working.Survival.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                                   "cond.comp",";","cr")]]<-data.frame(
   time=s.fit.summary$time,
   n.risk=s.fit.summary$n.risk[,1],
   n.event= s.fit.summary$n.event[,2],
   surv=s.fit.summary$pstate[,2],
   lower=s.fit.summary$lower[,2],
   upper=s.fit.summary$upper[,2],
   group=s.fit.summary$strata)  %>% 
    mutate(strata="cond.comp") %>% 
    mutate(outcome=value.working.outcome) %>% 
    mutate(outcome.name=value.working.outcome.name) %>% 
    mutate(pop.type=value.pop.type) %>% 
    mutate(prior.obs.required=value.prior.obs.required)  %>% 
    mutate(pop=value.working.study.cohort) %>% 
    mutate(surv.type="Competing risk")    
  }
  
  
  s<-summary(survfit(Surv(f_u.outcome.days, f_u.outcome) ~ drug.comp, 
                     data = working.data), times = working.times)
  working.Survival.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,
                                   ";","drug.comp")]]<-data.frame(
                                     time=s$time,
                                     n.risk=s$n.risk,
                                     n.event= s$n.event,
                                     surv=s$surv,
                                     lower=s$lower,
                                     upper=s$upper,
                                     group=s$strata)%>% 
    mutate(strata="drug.comp") %>% 
    mutate(outcome=value.working.outcome) %>% 
    mutate(outcome.name=value.working.outcome.name) %>% 
    mutate(pop.type=value.pop.type) %>% 
    mutate(prior.obs.required=value.prior.obs.required)  %>% 
    mutate(pop=value.working.study.cohort)%>% 
    mutate(surv.type="KM")
if(mortality.captured==TRUE & working.outcome.name!="death"){
s <- survfit(Surv(w.outcome_or_mortality.etime, 
                      w.outcome_or_mortality.event) ~drug.comp , 
                 data=working.data)
s.fit.summary <- summary(s, times=working.times)
working.Survival.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                                   "drug.comp",";","cr")]]<-data.frame(
   time=s.fit.summary$time,
   n.risk=s.fit.summary$n.risk[,1],
   n.event= s.fit.summary$n.event[,2],
   surv=s.fit.summary$pstate[,2],
   lower=s.fit.summary$lower[,2],
   upper=s.fit.summary$upper[,2],
   group=s.fit.summary$strata)  %>% 
    mutate(strata="drug.comp") %>% 
    mutate(outcome=value.working.outcome) %>% 
    mutate(outcome.name=value.working.outcome.name) %>% 
    mutate(pop.type=value.pop.type) %>% 
    mutate(prior.obs.required=value.prior.obs.required)  %>% 
    mutate(pop=value.working.study.cohort) %>% 
    mutate(surv.type="Competing risk")    
  }
  
  
  s<-summary(survfit(Surv(f_u.outcome.days, f_u.outcome) ~ cond.drug.comp, 
                     data = working.data), times = working.times)
  working.Survival.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,
                                   ";","cond.drug.comp")]]<-data.frame(
                                     time=s$time,
                                     n.risk=s$n.risk,
                                     n.event= s$n.event,
                                     surv=s$surv,
                                     lower=s$lower,
                                     upper=s$upper,
                                     group=s$strata)%>% 
    mutate(strata="cond.drug.comp") %>% 
    mutate(outcome=value.working.outcome) %>% 
    mutate(outcome.name=value.working.outcome.name) %>% 
    mutate(pop.type=value.pop.type) %>% 
    mutate(prior.obs.required=value.prior.obs.required)  %>% 
    mutate(pop=value.working.study.cohort)%>% 
    mutate(surv.type="KM")
  if(mortality.captured==TRUE & working.outcome.name!="death"){
s <- survfit(Surv(w.outcome_or_mortality.etime, 
                      w.outcome_or_mortality.event) ~cond.drug.comp , 
                 data=working.data)
s.fit.summary <- summary(s, times=working.times)
working.Survival.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                                   "cond.drug.comp",";","cr")]]<-data.frame(
   time=s.fit.summary$time,
   n.risk=s.fit.summary$n.risk[,1],
   n.event= s.fit.summary$n.event[,2],
   surv=s.fit.summary$pstate[,2],
   lower=s.fit.summary$lower[,2],
   upper=s.fit.summary$upper[,2],
   group=s.fit.summary$strata)  %>% 
    mutate(strata="cond.drug.comp") %>% 
    mutate(outcome=value.working.outcome) %>% 
    mutate(outcome.name=value.working.outcome.name) %>% 
    mutate(pop.type=value.pop.type) %>% 
    mutate(prior.obs.required=value.prior.obs.required)  %>% 
    mutate(pop=value.working.study.cohort) %>% 
    mutate(surv.type="Competing risk")    
  }
  
  
  
  s<-summary(survfit(Surv(f_u.outcome.days, f_u.outcome) ~ age_gr+gender, 
                     data = working.data), times = working.times)
  working.Survival.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,
                                   ";","age_gr_gender")]]<-data.frame(
                                     time=s$time,
                                     n.risk=s$n.risk,
                                     n.event= s$n.event,
                                     surv=s$surv,
                                     lower=s$lower,
                                     upper=s$upper,
                                     group=s$strata)%>% 
    mutate(strata="age_gr_gender") %>% 
    mutate(outcome=value.working.outcome) %>% 
    mutate(outcome.name=value.working.outcome.name) %>% 
    mutate(pop.type=value.pop.type) %>% 
    mutate(prior.obs.required=value.prior.obs.required) %>% 
    mutate(pop=value.working.study.cohort)%>% 
    mutate(surv.type="KM")
  if(mortality.captured==TRUE & working.outcome.name!="death"){
s <- survfit(Surv(w.outcome_or_mortality.etime, 
                      w.outcome_or_mortality.event) ~age_gr+gender , 
                 data=working.data)
s.fit.summary <- summary(s, times=working.times)
working.Survival.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                                   "age_gr_gender",";","cr")]]<-data.frame(
   time=s.fit.summary$time,
   n.risk=s.fit.summary$n.risk[,1],
   n.event= s.fit.summary$n.event[,2],
   surv=s.fit.summary$pstate[,2],
   lower=s.fit.summary$lower[,2],
   upper=s.fit.summary$upper[,2],
   group=s.fit.summary$strata)  %>% 
    mutate(strata="age_gr_gender") %>% 
    mutate(outcome=value.working.outcome) %>% 
    mutate(outcome.name=value.working.outcome.name) %>% 
    mutate(pop.type=value.pop.type) %>% 
    mutate(prior.obs.required=value.prior.obs.required)  %>% 
    mutate(pop=value.working.study.cohort) %>% 
    mutate(surv.type="Competing risk")    
  }
  
  
  
  s<-summary(survfit(Surv(f_u.outcome.days, f_u.outcome) ~ age_gr2+gender, 
                     data = working.data), times = working.times)
  working.Survival.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,
                                   ";","age_gr2_gender")]]<-data.frame(
                                     time=s$time,
                                     n.risk=s$n.risk,
                                     n.event= s$n.event,
                                     surv=s$surv,
                                     lower=s$lower,
                                     upper=s$upper,
                                     group=s$strata)%>% 
    mutate(strata="age_gr2_gender") %>% 
    mutate(outcome=value.working.outcome) %>% 
    mutate(outcome.name=value.working.outcome.name) %>% 
    mutate(pop.type=value.pop.type) %>% 
    mutate(prior.obs.required=value.prior.obs.required)   %>% 
    mutate(pop=value.working.study.cohort)%>% 
    mutate(surv.type="KM")
  if(mortality.captured==TRUE & working.outcome.name!="death"){
s <- survfit(Surv(w.outcome_or_mortality.etime, 
                      w.outcome_or_mortality.event) ~age_gr2+gender , 
                 data=working.data)
s.fit.summary <- summary(s, times=working.times)
working.Survival.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                                   "age_gr2_gender",";","cr")]]<-data.frame(
   time=s.fit.summary$time,
   n.risk=s.fit.summary$n.risk[,1],
   n.event= s.fit.summary$n.event[,2],
   surv=s.fit.summary$pstate[,2],
   lower=s.fit.summary$lower[,2],
   upper=s.fit.summary$upper[,2],
   group=s.fit.summary$strata)  %>% 
    mutate(strata="age_gr2_gender") %>% 
    mutate(outcome=value.working.outcome) %>% 
    mutate(outcome.name=value.working.outcome.name) %>% 
    mutate(pop.type=value.pop.type) %>% 
    mutate(prior.obs.required=value.prior.obs.required)  %>% 
    mutate(pop=value.working.study.cohort) %>% 
    mutate(surv.type="Competing risk")    
  }
    
  s<-summary(survfit(Surv(f_u.outcome.days, f_u.outcome) ~ age_gr3+gender, 
                     data = working.data), times = working.times)
  working.Survival.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,
                                   ";","age_gr3_gender")]]<-data.frame(
                                     time=s$time,
                                     n.risk=s$n.risk,
                                     n.event= s$n.event,
                                     surv=s$surv,
                                     lower=s$lower,
                                     upper=s$upper,
                                     group=s$strata)%>% 
    mutate(strata="age_gr3_gender") %>% 
    mutate(outcome=value.working.outcome) %>% 
    mutate(outcome.name=value.working.outcome.name) %>% 
    mutate(pop.type=value.pop.type) %>% 
    mutate(prior.obs.required=value.prior.obs.required)   %>% 
    mutate(pop=value.working.study.cohort)%>% 
    mutate(surv.type="KM")
  if(mortality.captured==TRUE & working.outcome.name!="death"){
s <- survfit(Surv(w.outcome_or_mortality.etime, 
                      w.outcome_or_mortality.event) ~age_gr3+gender , 
                 data=working.data)
s.fit.summary <- summary(s, times=working.times)
working.Survival.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                                   "age_gr3_gender",";","cr")]]<-data.frame(
   time=s.fit.summary$time,
   n.risk=s.fit.summary$n.risk[,1],
   n.event= s.fit.summary$n.event[,2],
   surv=s.fit.summary$pstate[,2],
   lower=s.fit.summary$lower[,2],
   upper=s.fit.summary$upper[,2],
   group=s.fit.summary$strata)  %>% 
    mutate(strata="age_gr3_gender") %>% 
    mutate(outcome=value.working.outcome) %>% 
    mutate(outcome.name=value.working.outcome.name) %>% 
    mutate(pop.type=value.pop.type) %>% 
    mutate(prior.obs.required=value.prior.obs.required)  %>% 
    mutate(pop=value.working.study.cohort) %>% 
    mutate(surv.type="Competing risk")  
  }
  
  s<-summary(survfit(Surv(f_u.outcome.days, f_u.outcome) ~ age_gr, 
                     data = working.data), times = working.times)
  working.Survival.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,
                                   ";","age_gr")]]<-data.frame(
                                     time=s$time,
                                     n.risk=s$n.risk,
                                     n.event= s$n.event,
                                     surv=s$surv,
                                     lower=s$lower,
                                     upper=s$upper,
                                     group=s$strata)%>% 
    mutate(strata="age_gr") %>% 
    mutate(outcome=value.working.outcome) %>% 
    mutate(outcome.name=value.working.outcome.name) %>% 
    mutate(pop.type=value.pop.type) %>% 
    mutate(prior.obs.required=value.prior.obs.required)  %>% 
    mutate(pop=value.working.study.cohort)%>% 
    mutate(surv.type="KM")
  if(mortality.captured==TRUE & working.outcome.name!="death"){
s <- survfit(Surv(w.outcome_or_mortality.etime, 
                      w.outcome_or_mortality.event) ~age_gr , 
                 data=working.data)
s.fit.summary <- summary(s, times=working.times)
working.Survival.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                                   "age_gr",";","cr")]]<-data.frame(
   time=s.fit.summary$time,
   n.risk=s.fit.summary$n.risk[,1],
   n.event= s.fit.summary$n.event[,2],
   surv=s.fit.summary$pstate[,2],
   lower=s.fit.summary$lower[,2],
   upper=s.fit.summary$upper[,2],
   group=s.fit.summary$strata)  %>% 
    mutate(strata="age_gr") %>% 
    mutate(outcome=value.working.outcome) %>% 
    mutate(outcome.name=value.working.outcome.name) %>% 
    mutate(pop.type=value.pop.type) %>% 
    mutate(prior.obs.required=value.prior.obs.required)  %>% 
    mutate(pop=value.working.study.cohort) %>% 
    mutate(surv.type="Competing risk")  
  }
  
  s<-summary(survfit(Surv(f_u.outcome.days, f_u.outcome) ~ age_gr2, 
                     data = working.data), times = working.times)
  working.Survival.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,
                                   ";","age_gr2")]]<-data.frame(
                                     time=s$time,
                                     n.risk=s$n.risk,
                                     n.event= s$n.event,
                                     surv=s$surv,
                                     lower=s$lower,
                                     upper=s$upper,
                                     group=s$strata)%>% 
    mutate(strata="age_gr2") %>% 
    mutate(outcome=value.working.outcome) %>% 
    mutate(outcome.name=value.working.outcome.name) %>% 
    mutate(pop.type=value.pop.type) %>% 
    mutate(prior.obs.required=value.prior.obs.required)   %>% 
    mutate(pop=value.working.study.cohort)%>% 
    mutate(surv.type="KM")
  if(mortality.captured==TRUE & working.outcome.name!="death"){
  s <- survfit(Surv(w.outcome_or_mortality.etime, 
                      w.outcome_or_mortality.event) ~age_gr2 , 
                 data=working.data)
s.fit.summary <- summary(s, times=working.times)
working.Survival.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                                   "age_gr2",";","cr")]]<-data.frame(
   time=s.fit.summary$time,
   n.risk=s.fit.summary$n.risk[,1],
   n.event= s.fit.summary$n.event[,2],
   surv=s.fit.summary$pstate[,2],
   lower=s.fit.summary$lower[,2],
   upper=s.fit.summary$upper[,2],
   group=s.fit.summary$strata)  %>% 
    mutate(strata="age_gr2") %>% 
    mutate(outcome=value.working.outcome) %>% 
    mutate(outcome.name=value.working.outcome.name) %>% 
    mutate(pop.type=value.pop.type) %>% 
    mutate(prior.obs.required=value.prior.obs.required)  %>% 
    mutate(pop=value.working.study.cohort) %>% 
    mutate(surv.type="Competing risk")  
  }
  
  s<-summary(survfit(Surv(f_u.outcome.days, f_u.outcome) ~ age_gr3, 
                     data = working.data), times = working.times)
  working.Survival.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,
                                   ";","age_gr3")]]<-data.frame(
                                     time=s$time,
                                     n.risk=s$n.risk,
                                     n.event= s$n.event,
                                     surv=s$surv,
                                     lower=s$lower,
                                     upper=s$upper,
                                     group=s$strata)%>% 
    mutate(strata="age_gr3") %>% 
    mutate(outcome=value.working.outcome) %>% 
    mutate(outcome.name=value.working.outcome.name) %>% 
    mutate(pop.type=value.pop.type) %>% 
    mutate(prior.obs.required=value.prior.obs.required)   %>% 
    mutate(pop=value.working.study.cohort)%>% 
    mutate(surv.type="KM")
  if(mortality.captured==TRUE & working.outcome.name!="death"){
   s <- survfit(Surv(w.outcome_or_mortality.etime, 
                      w.outcome_or_mortality.event) ~age_gr3 , 
                 data=working.data)
s.fit.summary <- summary(s, times=working.times)
working.Survival.summary[[paste0(value.working.study.cohort,";",value.working.outcome.name,";",
                                   "age_gr3",";","cr")]]<-data.frame(
   time=s.fit.summary$time,
   n.risk=s.fit.summary$n.risk[,1],
   n.event= s.fit.summary$n.event[,2],
   surv=s.fit.summary$pstate[,2],
   lower=s.fit.summary$lower[,2],
   upper=s.fit.summary$upper[,2],
   group=s.fit.summary$strata)  %>% 
    mutate(strata="age_gr3") %>% 
    mutate(outcome=value.working.outcome) %>% 
    mutate(outcome.name=value.working.outcome.name) %>% 
    mutate(pop.type=value.pop.type) %>% 
    mutate(prior.obs.required=value.prior.obs.required)  %>% 
    mutate(pop=value.working.study.cohort) %>% 
    mutate(surv.type="Competing risk")  
  }
 
  bind_rows(working.Survival.summary,.id = NULL)
  
  
}



Survival.summary[[paste0(working.study.cohort,";",working.outcome.name,";","No.prior.obs", ";", "pop.all")]]<-
    get.Surv.summaries(working.data=working.Pop,
                       value.prior.obs.required="No",
                       value.pop.type="All",
                       value.working.outcome=working.outcome,
                       value.working.outcome.name=working.outcome.name,
                       value.working.study.cohort=working.study.cohort)
Survival.summary[[paste0(working.study.cohort,";",working.outcome.name,";","No.prior.obs", ";", "before.Septermber")]]<-
  get.Surv.summaries(working.data=working.Pop %>% filter(cohort_start_date<as.Date("2020-09-01")) ,
                     value.prior.obs.required="No",
                     value.pop.type="Before Septermber 2020",
                     value.working.outcome=working.outcome,
                     value.working.outcome.name=working.outcome.name,
                     value.working.study.cohort=working.study.cohort)
Survival.summary[[paste0(working.study.cohort,";",working.outcome.name,";","No.prior.obs", ";", "after.Septermber")]]<-
  get.Surv.summaries(working.data=working.Pop %>% filter(cohort_start_date>=as.Date("2020-09-01")) ,
                     value.prior.obs.required="No",
                     value.pop.type="From Septermber 2020",
                     value.working.outcome=working.outcome,
                     value.working.outcome.name=working.outcome.name,
                     value.working.study.cohort=working.study.cohort)


Survival.summary[[paste0(working.study.cohort,";",working.outcome.name,";","Prior.obs", ";", "pop.all")]]<-
  get.Surv.summaries(working.data=working.Pop %>% 
                       filter(prior_obs_years>=1),
                     value.prior.obs.required="Yes",
                     value.pop.type="All",
                     value.working.outcome=working.outcome,
                     value.working.outcome.name=working.outcome.name,
                     value.working.study.cohort=working.study.cohort)
Survival.summary[[paste0(working.study.cohort,";",working.outcome.name,";","Prior.obs", ";", "before.Septermber")]]<-
  get.Surv.summaries(working.data=working.Pop %>% filter(cohort_start_date<as.Date("2020-09-01")) %>% 
                       filter(prior_obs_years>=1) ,
                     value.prior.obs.required="Yes",
                     value.pop.type="Before Septermber 2020",
                     value.working.outcome=working.outcome,
                     value.working.outcome.name=working.outcome.name,
                     value.working.study.cohort=working.study.cohort)
Survival.summary[[paste0(working.study.cohort,";",working.outcome.name,";","Prior.obs", ";", "after.Septermber")]]<-
  get.Surv.summaries(working.data=working.Pop %>% filter(cohort_start_date>=as.Date("2020-09-01")) %>% 
                       filter(prior_obs_years>=1) ,
                     value.prior.obs.required="Yes",
                     value.pop.type="From Septermber 2020",
                     value.working.outcome=working.outcome,
                     value.working.outcome.name=working.outcome.name,
                     value.working.study.cohort=working.study.cohort)

# modelling  ------

#
print(" -- Fitting models")
# if(nrow(working.Pop %>% 
#         filter(f_u.outcome==1))>=100){ 
get.models<-function(working.data, working.r.1.data, working.r.2.data){

dd<<-datadist(working.data %>% select(-c("day_of_birth", "month_of_birth"))); options(datadist = "dd" )
# choose whether to fit age as linear or with rcs (3 knots)
m.age.linear<-cph(Surv(f_u.outcome.days, f_u.outcome) ~ age,
       surv=TRUE,x=TRUE,y=TRUE,
       data = working.data)
m.age.rcs.3<-cph(Surv(f_u.outcome.days, f_u.outcome) ~ rcs(age,3),
       surv=TRUE,x=TRUE,y=TRUE,
       data = working.data)
age.fit<-ifelse(AIC(m.age.linear)<=AIC(m.age.rcs.3),
       "age", "rcs(age,3)")
# # interaction between age and sex
# age.gender.interaction<-anova(cph(Surv(f_u.outcome.days, f_u.outcome) ~ age_gr3*gender,
#        surv=TRUE,x=TRUE,y=TRUE,
#        data = working.data))
# age.gender.interaction<-as.character(ifelse(tail(age.gender.interaction[,3],1)<0.005, "Yes", "No"))

# age.gender.f<-ifelse(age.gender.interaction=="Yes", paste0(age.fit, "*gender"),
#                      paste0(age.fit, "+ gender")
age.gender.f<-paste0(age.fit, "+ gender")

if(mortality.captured==TRUE & working.outcome.name!="death"){
# same again for competing risk of death
m.age.linear.comp.risk<-cph(Surv(f_u.outcome.days, f_u.outcome) ~ age,
       surv=TRUE,x=TRUE,y=TRUE,
       data = working.r.2.data)
m.age.rcs.3.comp.risk<-cph(Surv(f_u.outcome.days, f_u.outcome) ~ rcs(age,3),
       surv=TRUE,x=TRUE,y=TRUE,
       data = working.r.2.data)
age.fit.comp.risk<-ifelse(AIC(m.age.linear.comp.risk)<=AIC(m.age.rcs.3.comp.risk),
       "age", "rcs(age,3)")
# # interaction between age and sex
# age.gender.interaction.comp.risk<-anova(cph(Surv(f_u.outcome.days, f_u.outcome) ~ age_gr3*gender,
#        surv=TRUE,x=TRUE,y=TRUE,
#        data = working.r.2.data))
# age.gender.interaction.comp.risk<-as.character(ifelse(tail(age.gender.interaction.comp.risk[,3],1)<0.005, "Yes", "No"))

# age.gender.f.comp.risk<-ifelse(age.gender.interaction.comp.risk=="Yes", paste0(age.fit.comp.risk, "*gender"),
#                      paste0(age.fit.comp.risk, "+ gender"))
age.gender.f.comp.risk<- paste0(age.fit.comp.risk, "+ gender")
 }

# 1) age, stratified by gender
# without competing risk
m.age.male<-cph(as.formula(paste("Surv(f_u.outcome.days, f_u.outcome)~", age.fit)),
       surv=TRUE,x=TRUE,y=TRUE,
       data = working.data %>% filter(gender=="Male"))
m.age.female<-cph(as.formula(paste("Surv(f_u.outcome.days, f_u.outcome)~", age.fit)),
       surv=TRUE,x=TRUE,y=TRUE,
       data = working.data %>% filter(gender=="Female"))
if(mortality.captured==TRUE & working.outcome.name!="death"){
# with competing risk
m.age.male.cr.1<-cph(as.formula(paste("Surv(time, status)~", age.fit)),
       surv=TRUE,x=TRUE,y=TRUE,
       data = working.r.1.data %>% filter(gender=="Male"))
m.age.female.cr.1<-cph(as.formula(paste("Surv(time, status)~", age.fit)),
       surv=TRUE,x=TRUE,y=TRUE,
       data = working.r.1.data %>% filter(gender=="Female"))

m.age.male.cr.2<-cph(as.formula(paste("Surv(time, status)~", age.fit.comp.risk)),
       surv=TRUE,x=TRUE,y=TRUE,
       data = working.r.2.data %>% filter(gender=="Male"))
m.age.female.cr.2<-cph(as.formula(paste("Surv(time, status)~", age.fit.comp.risk)),
       surv=TRUE,x=TRUE,y=TRUE,
       data = working.r.2.data %>% filter(gender=="Female"))
}

# summarise relative hazard ratios for age
ages<-seq(20,90, 5)
ref.age<-65

working.summary.age.male<-list()
for(age.i in 1:length(ages)){
# w/o competing risk
working.summary.age.male[[paste0(age.i, ".overall")]]<-head(as.data.frame(summary(m.age.male, 
        age=c(65,ages[age.i]),
        antilog=FALSE)),1)   %>% 
  mutate(model.type="overall")
if(mortality.captured==TRUE & working.outcome.name!="death"){
# w competing risk
working.summary.age.male[[paste0(age.i, ".cr.1")]]<-head(as.data.frame(summary(m.age.male.cr.1, 
        age=c(65,ages[age.i]),
        antilog=FALSE)),1)  %>% 
  mutate(model.type="cause-specific;outcome.of.interest")
working.summary.age.male[[paste0(age.i, ".cr.2")]]<-head(as.data.frame(summary(m.age.male.cr.2, 
        age=c(65,ages[age.i]),
        antilog=FALSE)),1)  %>% 
  mutate(model.type="cause-specific;death")
}}
working.summary.age.male<-bind_rows(working.summary.age.male) %>% 
  mutate(hr=exp(Effect),
         hr.low=exp(`Lower 0.95`),
         hr.high=exp(`Upper 0.95`)) %>% 
  mutate(ref.age=Low,
         rel.age=High) %>% 
  select(hr, hr.low, hr.high, ref.age, rel.age, model.type) %>%  
  mutate(gender="Male")


working.summary.age.female<-list()
for(age.i in 1:length(ages)){
# w/o competing risk
working.summary.age.female[[paste0(age.i, ".overall")]]<-head(as.data.frame(summary(m.age.female, 
        age=c(65,ages[age.i]),
        antilog=FALSE)),1)   %>% 
  mutate(model.type="overall")
if(mortality.captured==TRUE & working.outcome.name!="death"){
# w competing risk
working.summary.age.female[[paste0(age.i, ".cr.1")]]<-head(as.data.frame(summary(m.age.female.cr.1, 
        age=c(65,ages[age.i]),
        antilog=FALSE)),1)  %>% 
  mutate(model.type="cause-specific;outcome.of.interest")
working.summary.age.female[[paste0(age.i, ".cr.2")]]<-head(as.data.frame(summary(m.age.female.cr.2, 
        age=c(65,ages[age.i]),
        antilog=FALSE)),1)  %>% 
  mutate(model.type="cause-specific;death")
}}
working.summary.age.female<-bind_rows(working.summary.age.female) %>% 
  mutate(hr=exp(Effect),
         hr.low=exp(`Lower 0.95`),
         hr.high=exp(`Upper 0.95`)) %>% 
  mutate(ref.age=Low,
         rel.age=High) %>% 
  select(hr, hr.low, hr.high, ref.age, rel.age,model.type) %>%  
  mutate(gender="Female")



working.summary.age<-bind_rows(working.summary.age.male, working.summary.age.female)
# working.summary.age %>%
#   ggplot()+
#   facet_grid(. ~ model.type)+
#   geom_line(aes(rel.age, hr, colour=gender))+
#   geom_line(aes(rel.age, hr.low, colour=gender), linetype="dashed")+
#   geom_line(aes(rel.age, hr.high, colour=gender), linetype="dashed")

# 2 gender
get.models.gender<-function(){
# unadjusted 
m.unadj<-cph(as.formula(paste("Surv(f_u.outcome.days, f_u.outcome)~","gender")),
       surv=TRUE,x=TRUE,y=TRUE,
       data = working.data)
if(mortality.captured==TRUE & working.outcome.name!="death"){
m.unadj.cr.1<-cph(as.formula(paste("Surv(time, status)~","gender")),
       surv=TRUE,x=TRUE,y=TRUE,
       data = working.r.1.data)
m.unadj.cr.2<-cph(as.formula(paste("Surv(time, status)~","gender")),
       surv=TRUE,x=TRUE,y=TRUE,
       data = r.2)}
# adjusted for age
m.adj<-cph(as.formula(paste("Surv(f_u.outcome.days, f_u.outcome)~","gender", "+",  age.fit)),
       surv=TRUE,x=TRUE,y=TRUE,
       data = working.data)
if(mortality.captured==TRUE & working.outcome.name!="death"){
m.adj.cr.1<-cph(as.formula(paste("Surv(time, status)~","gender", "+",  age.fit)),
       surv=TRUE,x=TRUE,y=TRUE,
       data = working.r.1.data)
m.adj.cr.2<-cph(as.formula(paste("Surv(time, status)~","gender", "+",  age.fit.comp.risk)),
       surv=TRUE,x=TRUE,y=TRUE,
       data = r.2)}




if(mortality.captured==TRUE & working.outcome.name!="death"){

bind_rows(
head(as.data.frame(summary(m.unadj, gender='Female', antilog=FALSE)),1) %>% 
  mutate(hr=exp(Effect),
         hr.low=exp(`Lower 0.95`),
         hr.high=exp(`Upper 0.95`)) %>% 
  select(hr, hr.low, hr.high)%>%  
  mutate(model="Unadjusted") %>% 
  mutate(model.type="overall"),
head(as.data.frame(summary(m.unadj.cr.1, gender='Female', antilog=FALSE)),1) %>% 
  mutate(hr=exp(Effect),
         hr.low=exp(`Lower 0.95`),
         hr.high=exp(`Upper 0.95`)) %>% 
  select(hr, hr.low, hr.high)%>%  
  mutate(model="Unadjusted") %>% 
  mutate(model.type="cause-specific;outcome.of.interest"),
head(as.data.frame(summary(m.unadj.cr.2, gender='Female', antilog=FALSE)),1) %>% 
  mutate(hr=exp(Effect),
         hr.low=exp(`Lower 0.95`),
         hr.high=exp(`Upper 0.95`)) %>% 
  select(hr, hr.low, hr.high)%>%  
  mutate(model="Unadjusted") %>% 
  mutate(model.type="cause-specific;death"),

tail(as.data.frame(summary(m.adj, gender='Female', antilog=FALSE)),1) %>% 
  mutate(hr=exp(Effect),
         hr.low=exp(`Lower 0.95`),
         hr.high=exp(`Upper 0.95`)) %>% 
  select(hr, hr.low, hr.high)%>%  
  mutate(model="Adjusted") %>% 
  mutate(model.type="overall"),
tail(as.data.frame(summary(m.adj.cr.1, gender='Female', antilog=FALSE)),1) %>% 
  mutate(hr=exp(Effect),
         hr.low=exp(`Lower 0.95`),
         hr.high=exp(`Upper 0.95`)) %>% 
  select(hr, hr.low, hr.high)%>% 
  mutate(model="Adjusted") %>%
  mutate(model.type="cause-specific;outcome.of.interest"),
tail(as.data.frame(summary(m.adj.cr.2, gender='Female', antilog=FALSE)),1) %>% 
  mutate(hr=exp(Effect),
         hr.low=exp(`Lower 0.95`),
         hr.high=exp(`Upper 0.95`)) %>% 
  select(hr, hr.low, hr.high)%>% 
  mutate(model="Adjusted") %>%
  mutate(model.type="cause-specific;death")
) %>% 
  mutate(var="Sex (Male:Female)")
} else {
    
bind_rows(
head(as.data.frame(summary(m.unadj, gender='Female', antilog=FALSE)),1) %>% 
  mutate(hr=exp(Effect),
         hr.low=exp(`Lower 0.95`),
         hr.high=exp(`Upper 0.95`)) %>% 
  select(hr, hr.low, hr.high)%>%  
  mutate(model="Unadjusted") %>% 
  mutate(model.type="overall"),

tail(as.data.frame(summary(m.adj, gender='Female',antilog=FALSE)),1) %>% 
  mutate(hr=exp(Effect),
         hr.low=exp(`Lower 0.95`),
         hr.high=exp(`Upper 0.95`)) %>% 
  select(hr, hr.low, hr.high)%>%  
  mutate(model="Adjusted") %>% 
  mutate(model.type="overall")
) %>% 
 mutate(var="Sex (Male:Female)")
} 


}
working.summary.gender<-get.models.gender()


# 3 other exposures of interest
# unadjusted and adjusted for age and gender (or in the case of gender, just age)
get.models.exposures<-function(var){
  if(nrow(working.data %>% 
    filter(!!as.name(var)==1))>5){
    
m.unadj<-cph(as.formula(paste("Surv(f_u.outcome.days, f_u.outcome)~",{{var}})),
       surv=TRUE,x=TRUE,y=TRUE,
       data = working.data)
if(mortality.captured==TRUE & working.outcome.name!="death"){
m.unadj.cr.1<-cph(as.formula(paste("Surv(time, status)~",{{var}})),
       surv=TRUE,x=TRUE,y=TRUE,
       data = working.r.1.data)
m.unadj.cr.2<-cph(as.formula(paste("Surv(time, status)~",{{var}})),
       surv=TRUE,x=TRUE,y=TRUE,
       data = r.2)
}
m.adj<-cph(as.formula(paste("Surv(f_u.outcome.days, f_u.outcome)~",{{var}}, "+",  age.gender.f)),
       surv=TRUE,x=TRUE,y=TRUE,
       data = working.data)
if(mortality.captured==TRUE & working.outcome.name!="death"){
m.adj.cr.1<-cph(as.formula(paste("Surv(time, status)~",{{var}}, "+",  age.gender.f)),
       surv=TRUE,x=TRUE,y=TRUE,
       data = working.r.1.data)
m.adj.cr.2<-cph(as.formula(paste("Surv(time, status)~",{{var}}, "+",  age.gender.f.comp.risk)),
       surv=TRUE,x=TRUE,y=TRUE,
       iter.max=250,
       data = working.r.2.data)
}

if(mortality.captured==TRUE & working.outcome.name!="death"){

bind_rows(
head(as.data.frame(summary(m.unadj, antilog=FALSE)),1) %>% 
  mutate(hr=exp(Effect),
         hr.low=exp(`Lower 0.95`),
         hr.high=exp(`Upper 0.95`)) %>% 
  select(hr, hr.low, hr.high)%>%  
  mutate(model="Unadjusted") %>% 
  mutate(model.type="overall"),
head(as.data.frame(summary(m.unadj.cr.1, antilog=FALSE)),1) %>% 
  mutate(hr=exp(Effect),
         hr.low=exp(`Lower 0.95`),
         hr.high=exp(`Upper 0.95`)) %>% 
  select(hr, hr.low, hr.high)%>%  
  mutate(model="Unadjusted") %>% 
  mutate(model.type="cause-specific;outcome.of.interest"),
head(as.data.frame(summary(m.unadj.cr.2, antilog=FALSE)),1) %>% 
  mutate(hr=exp(Effect),
         hr.low=exp(`Lower 0.95`),
         hr.high=exp(`Upper 0.95`)) %>% 
  select(hr, hr.low, hr.high)%>%  
  mutate(model="Unadjusted") %>% 
  mutate(model.type="cause-specific;death"),

head(as.data.frame(summary(m.adj, antilog=FALSE)),1) %>% 
  mutate(hr=exp(Effect),
         hr.low=exp(`Lower 0.95`),
         hr.high=exp(`Upper 0.95`)) %>% 
  select(hr, hr.low, hr.high)%>%  
  mutate(model="Adjusted") %>% 
  mutate(model.type="overall"),
head(as.data.frame(summary(m.adj.cr.1, antilog=FALSE)),1) %>% 
  mutate(hr=exp(Effect),
         hr.low=exp(`Lower 0.95`),
         hr.high=exp(`Upper 0.95`)) %>% 
  select(hr, hr.low, hr.high)%>% 
  mutate(model="Adjusted") %>%
  mutate(model.type="cause-specific;outcome.of.interest"),
head(as.data.frame(summary(m.adj.cr.2, antilog=FALSE)),1) %>% 
  mutate(hr=exp(Effect),
         hr.low=exp(`Lower 0.95`),
         hr.high=exp(`Upper 0.95`)) %>% 
  select(hr, hr.low, hr.high)%>% 
  mutate(model="Adjusted") %>%
  mutate(model.type="cause-specific;death")
) %>% 
  mutate(var={{var}})
} else {
    
bind_rows(
head(as.data.frame(summary(m.unadj, antilog=FALSE)),1) %>% 
  mutate(hr=exp(Effect),
         hr.low=exp(`Lower 0.95`),
         hr.high=exp(`Upper 0.95`)) %>% 
  select(hr, hr.low, hr.high)%>%  
  mutate(model="Unadjusted") %>% 
  mutate(model.type="overall"),

head(as.data.frame(summary(m.adj, antilog=FALSE)),1) %>% 
  mutate(hr=exp(Effect),
         hr.low=exp(`Lower 0.95`),
         hr.high=exp(`Upper 0.95`)) %>% 
  select(hr, hr.low, hr.high)%>%  
  mutate(model="Adjusted") %>% 
  mutate(model.type="overall")
) %>% 
  mutate(var={{var}})  
} 
} else {
   tibble()
 }  
  
  
  
  

}

working.summary.exposures<-list()

working.summary.exposures[[1]]<-get.models.exposures("asthma.all.history")
working.summary.exposures[[2]]<-get.models.exposures("diabetes_mellitus.all.history")
working.summary.exposures[[3]]<-get.models.exposures("malignant_neoplastic_disease.all.history")
working.summary.exposures[[4]]<-get.models.exposures("heart_disease.all.history")
working.summary.exposures[[5]]<-get.models.exposures("hypertensive_disorder.all.history")
working.summary.exposures[[6]]<-get.models.exposures("obesity.all.history")
working.summary.exposures[[7]]<-get.models.exposures("renal_impairment.all.history")
working.summary.exposures[[8]]<-get.models.exposures("cond.comp")

working.summary.exposures[[9]]<-get.models.exposures("antiinflamatory_and_antirheumatic")
working.summary.exposures[[10]]<-get.models.exposures("antithrombotic")
working.summary.exposures[[11]]<-get.models.exposures("corticosteroids")
working.summary.exposures[[12]]<-get.models.exposures("drug.comp") 
working.summary.exposures<- bind_rows(working.summary.exposures)


row.names(working.summary.age)<-1:nrow(working.summary.age)
row.names(working.summary.gender)<-1:nrow(working.summary.gender)
row.names(working.summary.exposures)<-1:nrow(working.summary.exposures)

bind_rows(working.summary.age,
          working.summary.gender,
          working.summary.exposures)

}


Model.estimates[[paste0(working.study.cohort,
                                   ";",working.outcome.name,";",
                                   "No.prior.obs", ";",
                                   "pop.all")]]<-get.models(working.Pop,
                                                            r.1, r.2) %>% 
  mutate(prior.obs.required="No",
         pop.type="All",
         working.outcome=working.outcome,
         working.outcome.name=working.outcome.name,
         working.study.cohort=working.study.cohort)


if(nrow(working.Pop %>% 
        filter(cohort_start_date<as.Date("2020-09-01")) %>% 
        filter(f_u.outcome==1))>=100){  
Model.estimates[[paste0(working.study.cohort,
                                   ";",working.outcome.name,";",
                                   "No.prior.obs", ";",
                                   "Before Septermber")]]<-get.models(working.Pop %>% filter(cohort_start_date<as.Date("2020-09-01")),
                                                            r.1 %>% filter(cohort_start_date<as.Date("2020-09-01")),
                                                            r.2 %>% filter(cohort_start_date<as.Date("2020-09-01"))) %>% 
  mutate(prior.obs.required="No",
         pop.type="Before Septermber 2020",
         working.outcome=working.outcome,
         working.outcome.name=working.outcome.name,
         working.study.cohort=working.study.cohort)}

if(nrow(working.Pop %>% 
        filter(cohort_start_date>=as.Date("2020-09-01")) %>% 
        filter(f_u.outcome==1))>=100){ 
Model.estimates[[paste0(working.study.cohort,
                                   ";",working.outcome.name,";",
                                   "No.prior.obs", ";",
                                   "From Septermber")]]<-get.models(working.Pop %>% filter(cohort_start_date>=as.Date("2020-09-01")),
                                                            r.1 %>% filter(cohort_start_date>=as.Date("2020-09-01")),
                                                            r.2 %>% filter(cohort_start_date>=as.Date("2020-09-01"))) %>% 
  mutate(prior.obs.required="No",
         pop.type="From Septermber 2020",
         working.outcome=working.outcome,
         working.outcome.name=working.outcome.name,
         working.study.cohort=working.study.cohort)}


if(nrow(working.Pop  %>%   filter(prior_obs_years>=1)%>% 
        filter(f_u.outcome==1))>=100){  
Model.estimates[[paste0(working.study.cohort,
                                   ";",working.outcome.name,";",
                                   "prior.obs", ";",
                                   "pop.all")]]<-get.models(working.Pop %>%   filter(prior_obs_years>=1),
                                                            r.1 %>%  filter(prior_obs_years>=1),
                                                            r.2 %>%  filter(prior_obs_years>=1)) %>% 
  mutate(prior.obs.required="Yes",
         pop.type="All",
         working.outcome=working.outcome,
         working.outcome.name=working.outcome.name,
         working.study.cohort=working.study.cohort)}

if(nrow(working.Pop %>% filter(cohort_start_date<as.Date("2020-09-01")) %>% 
        filter(prior_obs_years>=1)%>% 
        filter(f_u.outcome==1))>=100){  
Model.estimates[[paste0(working.study.cohort,
                                   ";",working.outcome.name,";",
                                   "prior.obs", ";",
                                   "Before Septermber")]]<-get.models(working.Pop %>% filter(cohort_start_date<as.Date("2020-09-01")) %>% 
                                                                   filter(prior_obs_years>=1),
                                                            r.1 %>% filter(cohort_start_date<as.Date("2020-09-01")) %>% 
                                                                   filter(prior_obs_years>=1),
                                                             r.2 %>% filter(cohort_start_date<as.Date("2020-09-01")) %>% 
                                                                   filter(prior_obs_years>=1)) %>% 
  mutate(prior.obs.required="Yes",
         pop.type="Before Septermber 2020",
         working.outcome=working.outcome,
         working.outcome.name=working.outcome.name,
         working.study.cohort=working.study.cohort)}

if(nrow(working.Pop %>% filter(cohort_start_date>=as.Date("2020-09-01")) %>% 
        filter(prior_obs_years>=1)%>% 
        filter(f_u.outcome==1))>=100){
Model.estimates[[paste0(working.study.cohort,
                                   ";",working.outcome.name,";",
                                   "prior.obs", ";",
                                   "From Septermber")]]<-get.models(working.Pop %>% filter(cohort_start_date>=as.Date("2020-09-01")) %>% 
                                                                   filter(prior_obs_years>=1),
                                                            r.1 %>% filter(cohort_start_date>=as.Date("2020-09-01")) %>% 
                                                                   filter(prior_obs_years>=1),
                                                            r.2 %>% filter(cohort_start_date>=as.Date("2020-09-01")) %>% 
                                                                   filter(prior_obs_years>=1)) %>% 
  mutate(prior.obs.required="Yes",
         pop.type="From Septermber 2020",
         working.outcome=working.outcome,
         working.outcome.name=working.outcome.name,
         working.study.cohort=working.study.cohort)}
}
    }

    }
  


pat.ch<-list()
pat.ch[[1]]<-Pop.summary.characteristics %>%
    mutate(pop.type="All") %>% 
    mutate(prior.obs.required="No") %>% 
    mutate(pop=working.study.cohort) %>% 
    mutate(age_gr2="All")
pat.ch[[2]]<-Pop.summary.characteristics.before.Septermber %>%
  mutate(pop.type="Before Septermber 2020") %>% 
  mutate(prior.obs.required="No") %>% 
  mutate(pop=working.study.cohort) %>% 
  mutate(age_gr2="All")
pat.ch[[3]]<-Pop.summary.characteristics.from.Septermber %>%
  mutate(pop.type="From Septermber 2020") %>% 
  mutate(prior.obs.required="No") %>% 
  mutate(pop=working.study.cohort) %>% 
  mutate(age_gr2="All")

pat.ch[[4]]<-Pop.summary.characteristics.with.history %>%
    mutate(pop.type="All") %>% 
    mutate(prior.obs.required="Yes") %>% 
    mutate(pop=working.study.cohort) %>% 
    mutate(age_gr2="All")
pat.ch[[5]]<-Pop.summary.characteristics.before.Septermber.with.history %>%
  mutate(pop.type="Before Septermber 2020") %>% 
  mutate(prior.obs.required="Yes") %>% 
  mutate(pop=working.study.cohort) %>% 
  mutate(age_gr2="All")
pat.ch[[6]]<-Pop.summary.characteristics.from.Septermber.with.history %>%
  mutate(pop.type="From Septermber 2020") %>% 
  mutate(prior.obs.required="Yes") %>% 
  mutate(pop=working.study.cohort) %>% 
  mutate(age_gr2="All")


Patient.characteristcis[[paste0(i)]]<-bind_rows(pat.ch)

}







