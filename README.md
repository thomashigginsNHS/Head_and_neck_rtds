This is the RAP code for head and neck radiotherapy metrics. 

There is one R file that calculates two metrics: 

1. Metric 5a: Percentage of adjuvant radiotherapy treatments of head and neck mucosal cancers that start within 6 weeks of surgery. 
2. Metric 5b: Median and interquartile range (IQR) wait in days from date of last surgery to start of radiotherapy.  This is adjuvant. 

Rationale: Adjuvant radiotherapy after surgery for high risk localised head and neck mucosal cancers has been shown to improve overall survival. Delay of more than 6 weeks after completion of surgery has been shown to be associated with a decrease in overall survival. 

The R file uses supporting Oracle drivers. 

For both metrics, the data extraction involves joining cancer registry data to HES data and radiotherapy data. 