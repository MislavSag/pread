#!/bin/bash

#PBS -N pread_predictions
#PBS -l ncpus=2
#PBS -l mem=100GB
#PBS -J 1-1000
#PBS -o logs
#PBS -j oe

cd ${PBS_O_WORKDIR}

apptainer run image.sif predictors_padobran.R
