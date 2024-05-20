#!/bin/bash

#PBS -N spymlpreds
#PBS -l ncpus=4
#PBS -l mem=64GB
#PBS -J 1-1000
#PBS -o logs
#PBS -j oe

cd ${PBS_O_WORKDIR}

apptainer run image.sif predictors_padobran.R
