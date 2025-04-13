# bulk-data-cleaner

I need a .exe program that will allow me to upload gigabytes of zipped excel files and raw data.
- needs to process the excel files quickly
-needs to allow for parallel processing up to a user set % of available of processes.
- needs to have a status window showing ongoing processes
- allow for the creation of a processing queue
- allow for admin control of queue - process, pause, resume, completed (archive)
- allow for admin control of a default output folder, with the ability to designate a new folder per file.

For all data organized excel files, and raw data we need the data to be assigned to the appropriate record.

For structured excel files...

I need to get rid of empty columns (especially those that just say #!$@-) & need to ensure the content is in the correct columns.

I need to turn 1 file into 3 different file formats broken down into CSV (<50 mb), 1 tab per csv.

1. I need the content to create a workbook with sheets separated by country, then each sheet is sorted by language, then occupation, then industry/job title. A sheet with entries that may not have been understood like... city: Ciudad de MÃ©xico... which should be Ciudad de Mexico (Mexico City)- Output zipped CSVs

2. I need the content to create a workbook with sheets separated by country, then each sheet is sorted by occupation in the ReachInbox upload format (remove all extra content). - Output zipped CSVs with the following headers:

Email
First_Name
Last_Name
Company_Name
Linkdin
Personalised_Lines


3a. I need the content to create a workbook with sheets separated by country, then each sheet is sorted by occupation in the GHL upload format (remove all extra content beyond the content listed below: first row header name (column): Email (a), First name (e), Last name (g), department (m), job title(O), job level (q), city (s), state (u), country (w), linkedin profile (aa), employer (ac), employer website (ae), phone (ag), employer facebook page (ai),employer linkedin (ak), employer founded date (as), employer zip (aw), languages spoken (be), industry (bg), focus (bi), skills (bk)
- Output zipped CSVs

OR

3b. If you connect to GHL api and upload all the contacts fields listed in 3a. you can not produce the workbook/sheets and just uploaded the cleaned data into GHL.

I would like for the program to have the ability to also process unstructured data. For unstructured raw data in google sheets, or excel files... I would like to incorporate ai maybe via api key, to determine patterns, and correlate as much data a possible with the appropriate record based on how the data is read. put names with numbers, and email addresses, and urls, and expertise.
