#!/bin/bash

#PBS -N PREADPREP
#PBS -l mem=80GB

cd ${PBS_O_WORKDIR}
apptainer run image_estimate.sif estimate_padobran.R
