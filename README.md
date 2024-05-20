
## STEPS

0) OPTIONAL. If we use HPC, we need to push all code to GitHub and than create a new project on HPC. Than we need to clone this project to our local machine. Here is example for srce

2) First start script data_prepare.R. This script import events and prices data and merge them. Than, if we can use HPC (srce), we need to mannually add this data to HPC cluster. Her is ecample for srce
scp /home/sn/data/strategies/pread/dataset_pread.csv jmaric@padobran.srce.hr:<udaljena-putanja>