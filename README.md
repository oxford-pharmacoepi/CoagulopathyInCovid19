Coagulopathy and thromboembolic events among people with COVID-19
========================================================================================================================================================
- Results explorer: **[ShinyApp](https://livedataoxford.shinyapps.io/ThromboembolismInCovid19/)**

## Running the analysis
1) Download this entire repository (you can download as a zip folder using Code -> Download ZIP, or you can use GitHub Desktop). 
2) Open the project <i>CovCoagBackgroundIncidence.Rproj</i> in RStudio (when inside the project, you will see its name on the top-right of your RStudio session)
3) Open the <i>CodeToRun.R</i> file which should be the only file that you need to interact with
<li> Run <i>renv::activate()</i> and <i>renv::restore()</i> to bring in the required packages to be used</li> 
<li> <i>output.folder <- "...."</i>: the path to a folder (that exists) where the results from this analysis will be saved</li> 
<li> <i>connectionDetails <- createConnectionDetails(".........")</i>: These are the connection details for the 
<a href="http://ohdsi.github.io/DatabaseConnector">OHDSI DatabaseConnector</a> package.Note, this is v4.0.0 of DatabaseConnector and so you will need to have downloaded the relevant drivers (see <a href="http://ohdsi.github.io/DatabaseConnector/articles/UsingDatabaseConnector.html">here</a> for more details) and pass the <i>pathToDriver</i> argument to the <i>createConnectionDetails</i> command.</li>
<li><i>db <- dbConnect(".........")</i>: This is a connection to your database with the <a href="https://rdrr.io/cran/DBI/man/dbConnect.html">DBI</a> package. Database specific information for how to create this connection can be found <a href="https://db.rstudio.com/databases">here</a> </li>
<li><i>targetDialect <-"....."</i>: This is your sql dialect used with the OHDSI <a href="https://ohdsi.github.io/SqlRender/articles/UsingSqlRender.html">SqlRender</a> package</li> 
<li><i>cdm_database_schema <-"....."</i>: This is the name of the schema that contains the OMOP CDM with patient-level data </li> 
<li><i>vocabulary_database_schema <-"....."</i>: This is the name of the schema that contains the OMOP CDM vocabularies </li> 
<li><i>results_database_schema <-"....."</i>: This is the name of the schema where a results table will be created </li> 
<li><i>cohortTableExposures<-"CoagulopathyInCovid19Exposures"</i>: Name of the table which will contain the exposure cohorts
<li><i>cohortTableOutcomes <-"CoagulopathyInCovid19Outcomes"</i>: Name of the table which will contain the outcomes cohorts
<li><i>cohortTableComorbidities<-"CoagulopathyInCovid19Comorbidities"</i>: Name of the table which will contain the comorbidity cohorts
<li><i>cohortTableMedications<-"CoagulopathyInCovid19Medications" </i>: Name of the table which will contain the medication cohorts
<li><i>db.name <-"....."</i>: This is the short name/ acronym for your database</li>  
<li><i>mortality.captured<-TRUE</i>: TRUE if mortality is captured, FALSE otherwise
<li><i>create.exposure.cohorts<-TRUE</i>: TRUE if creating cohorts, FALSE otherwise
<li><i>create.outcome.cohorts<-TRUE</i>: TRUE if creating cohorts, FALSE otherwise
<li><i>create.profile.cohorts<-TRUE</i>: TRUE if creating cohorts, FALSE otherwise
<li>After running you should then have a zip folder with results to share in your output folder.</li> 
