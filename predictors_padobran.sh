#!/bin/bash

#PBS -N pread_predictions
#PBS -l ncpus=1
#PBS -l mem=1GB
#PBS -J 1-4321
#PBS -o logs
#PBS -j oe

cd ${PBS_O_WORKDIR}

apptainer run image_predicotrs.sif predictors_padobran.R
