## STEPS

0) OPTIONAL. If we use HPC, we need to push all code to GitHub and than create a new project on HPC. Than we need to clone this project to our local machine. Here is example for srce

1) First start script data_prepare.R. This script import events and prices data and merge them. Than, if we can use HPC (srce), we need to mannually add this data to HPC cluster. Her is ecample for srce
scp /home/sn/data/strategies/pread/dataset_pread.csv padobran:/home/jmaric/pread/dataset_pread.csv

2) Generate rolling predictors on HPC or ocally usgin predictors_padobran.R script.

2.hpc) If HPC is used, download generated predictors to local machine. Example:

