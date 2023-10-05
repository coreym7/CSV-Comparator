# CSV Data Comparison Project

**An efficient script created for comparison and analysis of large CSV files, focusing on identifying and highlighting new entries and changes in existing records.**

## Problem Statement
The goal of this project was to create a Python script to process, format, and edit a large CSV file. The primary objective was to filter out and list only new rows or rows with changes in certain columns, by comparing it against another CSV file. 

## Challenges 
The CSV file contained several columns. The first three columns were not to be included in the final output but were crucial in determining which entries should be included. Also, the file had to be compared against another CSV file, with "Employee Number" serving as the unique identifier for each entry. Certain values in the "Action" column needed to be updated based on changes in other columns, and there was a requirement to clean up email addresses by removing periods to the left of the '@' symbol.

## Strategy and Technologies Used
The Python language was chosen for this task, as it is one of the most widely used languages for data manipulation. We also leveraged the powerful Pandas library for data manipulation and analysis, and the datetime and dateutil libraries for handling dates.

## Thought Process and Implementation
The strategy was to first clean up the data, convert necessary columns to datetime format, identify new hires and terminations, and then merge the new DataFrame with the base one. After that, we identified the rows with changes in the specified columns and filtered the rows to include in the output. The output was then saved to a new CSV file.

## Results and Learnings
The initial script had some issues - the output file was longer than expected, each entry had every column listed twice, and every entry was flagged as needing an update. However, after a careful review and debugging, these issues were resolved by addressing trailing spaces, selecting required columns before merging, and correctly comparing columns and filtering rows. This project highlighted the importance of data preprocessing and demonstrated how careful selection of columns can prevent unnecessary computations and incorrect results.

## Impact
By implementing this Python script, the client was able to automate a time-consuming task, resulting in improved efficiency and accuracy in their data handling. 

## Note
Please note that the specific script has been generalized to an extend to be able to share the logic of the code. 
