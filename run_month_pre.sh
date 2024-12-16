
#!/bin/bash

#PBS -N PREAD
#PBS -l ncpus=4
#PBS -l mem=32GB
#PBS -J 1-22
#PBS -o experiments_pre_test/logs
#PBS -j oe

cd ${PBS_O_WORKDIR}
apptainer run image_estimate.sif run_job.R 0 experiments_pre_test

