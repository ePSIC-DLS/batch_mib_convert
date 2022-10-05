#!/bin/bash
#$ -l h_rt=48:00:00
#$ -cwd
#$ -q test-high.q
#$ -l redhat_release=rhel7
#$ -l m_mem_free=40G
#$ -l cpu_model=amd_epyc

# TIMESTAMP=$(/usr/bin/date +"%Y%m%d-%H%M")
# STRACE_FILE="/exports/batch_mib_convert_testing/batch_mib_convert.${JOB_ID}.${SGE_TASK_ID}.${TIMESTAMP}.strace.txt"
# TRACE_CMD="strace -o ${STRACE_FILE} -f -v -tttT"

echo "I am task $SGE_TASK_ID"

module load python/epsic3.7
if [[ $# -eq 3 ]]; then
  python /dls_sw/e02/scripts/batch_mib_convert/mib2hdf_watch_convert.py $1 $2 $3 "$SGE_TASK_ID"
else
  python /dls_sw/e02/scripts/batch_mib_convert/mib2hdf_watch_convert.py $1 $2 $3 "$SGE_TASK_ID" -folder $4 
fi

