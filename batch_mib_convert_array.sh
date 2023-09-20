#!/usr/bin/env bash
#SBATCH --partition cs04r
#SBATCH --job-name epsic_test
#SBATCH --nodes 1
#SBATCH --tasks-per-node 1
#SBATCH --cpus-per-task 1
#SBATCH --time 05:00:00
#SBATCH --mem 100G



echo "I am running the array job with task ID $SLURM_ARRAY_TASK_ID"

module load python/epsic3.10

sleep 10

if [[ $# -eq 3 ]]; then
    python /dls_sw/e02/software/batch_mib_convert/mib2hdf_watch_convert.py $1 $2 $3 "$SLURM_ARRAY_TASK_ID"
else
    python /dls_sw/e02/software/batch_mib_convert/mib2hdf_watch_convert.py $1 $2 $3 "$SLURM_ARRAY_TASK_ID" -folder $4 
fi

