#!/bin/bash

## Task scheduler script
## Bijoy Joseph
## 2015.08.19
## Usage: $0 [FileNAME_with_list_of_scripts]
#*************************************************#

## NOTE: ***********************************************#
## Run bash scripts in parallel by using xargs -I
#-> To run R scripts or other scripts (perl/python/java)
#+ use the script_type function from task_scheduler.sh,
#+ group scripts, and then call the script as below:
#   for i in $(cat scheduler.conf)
#     do 
#       echo $i
#     done | xargs -I{} --max-procs 4 bash -c '
#    {
#      echo sleep 2
#      echo {}
#      {} 1> /dev/null
#    }'
## END NOTE ********************************************#

## NOTE 2: *********************************************#
# TO DO: Add in functionality for distributed computing
#+ taskset/cpuset can be used to set spu affinity, if required
#+ (has been tried, and it works)
#+ OS handles the scheduling well enough, so perhaps, no need!
## END NOTE 2*******************************************#


  [ -s "$1" ] || echo "*** File $1 does not exist . . ."
  [ -s "$1" ] || exit 1;

## Trap Signals and cleanup
  trap "rm -rf /tmp/tmp_scheduler*.tmp; exit" SIGHUP SIGINT SIGTERM

  eval $(date "+TODATE='%Y%m%d' TIMED='%s_%N'")
  LOG_FILE=task_scheduler-$TODATE.log

#########+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++##########
## (1) Find bumber of CPUs and cores
#########+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++##########
  function get_numcores() {
    num_cores=$(cat /proc/cpuinfo | grep '^processor' | wc -l)
    echo $num_cores
  }

#########+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++##########
## (2) Find the script type
##    - $1 -> Name of the script to run (full path)
#########+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++##########
  function get_filetype() {
    case `file "$1" | cut -f2 -d ':' | sed -e 's/ /\n/g' | grep 'Python\|Perl\|Bourne-Again\|bash\|POSIX\|ASCII' | head -1` in
      Python)
        echo 'Python script'
      ;;
      Perl)
        echo 'Perl script'
      ;;
      bash|Bourne-Again|POSIX)
        echo 'shell script'
      ;;
      ASCII)
        if [ "$(echo $1 | rev | cut -b1-2 | rev | tr '[A-Z]' '[a-z]')" == '.r' ]; then
          echo 'R script'
        else
          echo 'ERROR: is this really an executable script?'
        fi
      ;;
      *)
        echo "ERROR: could not determine script type! $1"
      ;;
    esac
  }

#########+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++##########
## (3) pick first job from config file and run it
#########+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++##########
  function pass_job() {
    [ -s $temp_conf_file ] && {
      file_torun=$(sed -n '1p' $temp_conf_file)
      sed -i '1d' $temp_conf_file
      [ -s "$file_torun" ] || {
        echo "  -> $file_torun does not exist! ..." | tee -a $LOG_FILE
        continue
      }
      local exec_type=$(get_filetype "$file_torun")
    }

    case "$exec_type" in
      'R script')
        printf "  -> R script: $file_torun" | tee -a $LOG_FILE
        R CMD BATCH "$file_torun" &
      ;;

      'Python script')
        printf "  -> Python script: $file_torun" | tee -a $LOG_FILE
        python "$file_torun" &
      ;;

      'Perl script')
        printf "  -> Perl script: $file_torun" | tee -a $LOG_FILE
        perl "$file_torun" &
      ;;

      'shell script')
        printf "  -> Shell script: $file_torun" | tee -a $LOG_FILE
        [ -x $file_torun ] && "$file_torun" &
        [ -x $file_torun ] || echo -e "\n  ... Error: Not an executable - $file_torun" | tee -a $LOG_FILE
      ;;

      *)
        echo "  ... Error (pass_job): Indeterminate exec_type - $file_torun" | tee -a $LOG_FILE
#        pass_job
      ;;
    esac

#     [ -n $! ] && echo $! >> $list_pids
      [ -n "$!" ] && echo "$!" | sed -e 's/ //g' >> $list_pids

#    echo -e "  -- PIDs after adding new: `cat $list_pids | sed -e ':LP N;s/\n/ /; tLP'`" | tee -a $LOG_FILE
    echo ", PID: $!" | tee -a $LOG_FILE
    unset file_torun
  }

#########+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++##########
## (4) Watch PIDs after all jobs have been passed
#########+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++##########
  function pid_watch(){
    for run_job in `cat $list_pids`
      do
        if [ ! -d /proc/$run_job/task/$run_job/ ]; then
          sed -i "/^$run_job$/d" $list_pids
          running_jobs=`cat $list_pids | sed -e '/^$/d' | wc -l`
          echo "  -----> watching PIDs: $run_job completed, $running_jobs remain(s): `cat $list_pids | sed -e ':LP N;s/\n/ /; tLP'` ..." | tee -a $LOG_FILE
          [ "$running_jobs" -eq 0 ] && {
            echo -e "\nDone!: All jobs completed ...!\n#############################" | tee -a $LOG_FILE
            exit 0
           }
        fi
      done

      if [ "$running_jobs" -gt 0 ]; then
        echo "  -----> $running_jobs running ...: `cat $list_pids | sed -e ':LP N;s/\n/ /; tLP'`"
        sleep 10 && pid_watch
      fi
  }

##*************************** SCRIPT START HERE **************************** ##
## Read in file list, determine #cpus/cores for the number of maximum concurrent jobs
#+   max_concur_jobs defaults to four
##************************************************************************** ##
  max_concur_jobs=$(get_numcores)
  [ -n $max_concur_jobs ] || max_concur_jobs=4

# make a temporary copy of config file, to delete passed jobs one by one
#+  rev and sort to get R scripts at the beginning, assuming they are time-consuming
  sed -e 's/ //g' "$1" | grep -v '^#' | cut -f1 -d ':' | rev | sort -r | rev > /tmp/tmpsched_file_list_${TIMED}.tmp && temp_conf_file="/tmp/tmpsched_file_list_${TIMED}.tmp"

  total_jobs=$(cat $temp_conf_file | grep '^/' | wc -l)

# If total jobs less than max concurrent jobs, then update max_concur_jobs
  [ $total_jobs -lt $max_concur_jobs ] && max_concur_jobs=$total_jobs 
  list_pids="/tmp/tmpsched_pids_list_${TIMED}.tmp"

  >$LOG_FILE
  >$list_pids

  echo -e "  Task scheduler\n  -> maximum concurrent jobs: $max_concur_jobs\n  -> total jobs to run: $total_jobs\n" | tee -a $LOG_FILE

## (1) run $max_concur_jobs jobs based on script type, collect its PID
#+    use "taskset -c $((run_job-1)) CMD", to set cpu affinity (will not work on virtual machines
  for run_job in $(seq 1 $max_concur_jobs)
    do
      file_torun=`sed -n '1p' $temp_conf_file`
      sed -i '1d' $temp_conf_file

      [ -s "$file_torun" ] || {
        echo "  -> $file_torun does not exist! ..." | tee -a $LOG_FILE
        continue
      }
      exec_type=$(get_filetype "$file_torun")

      case "$exec_type" in
        'R script')
          printf "  -> R script: $file_torun" | tee -a $LOG_FILE
          R CMD BATCH "$file_torun" &
        ;;

        'Python script')
          printf "  -> Python script: $file_torun" | tee -a $LOG_FILE
          python "$file_torun" &
        ;;

        'Perl script')
          printf "  -> Perl script: $file_torun" | tee -a $LOG_FILE
          perl "$file_torun" &
        ;;

        'shell script')
          printf "  -> Shell script: $file_torun" | tee -a $LOG_FILE
          [ -x $file_torun ] && "$file_torun" &
          [ -x $file_torun ] || echo -e "\n  ... Error: Not an executable - $file_torun" | tee -a $LOG_FILE
        ;;

        *)
          echo "  ... Error: Indeterminate exec_type - $file_torun" | tee -a $LOG_FILE
#          pass_job
        ;;
      esac

      [ -n "$!" ] && echo "$!" | sed -e 's/ //g' >> $list_pids

      echo ", PID: $! (job #$((total_jobs - `cat $temp_conf_file | wc -l`)))" | tee -a $LOG_FILE
    done

# # of currently running jobs
  running_jobs=`cat $list_pids | sed -e '/^$/d' | wc -l`
  echo -e "\n##########\n  -> Started $running_jobs concurrent jobs, with PIDs: `cat $list_pids | sed -e ':LP N;s/\n/ /; tLP'` ...\n##########\n"

  [ -s "$list_pids" ] || {
    echo '  --> Error: no jobs started!' | tee -a $LOG_FILE
    exit 1
  }

## (2) watch jobs based on PID, if PID is over, replace it with new job
#  while test -n "$list_pids"
  while [ `cat $temp_conf_file | sed -e '/^$/d' | wc -l` -gt 0 ]
    do
      cat $list_pids | sed -e '/^$/d' | while read run_job
        do
          if [ ! -d "/proc/$run_job/task/$run_job/" ]; then
            echo -n "  -- $run_job completed ... " | tee -a $LOG_FILE
            sed -i "/^$run_job$/d" $list_pids
            echo " PIDs after deleting $run_job: `cat $list_pids | sed -e ':LP N;s/\n/ /; tLP'`" | tee -a $LOG_FILE
            running_jobs=`cat $list_pids | sed -e '/^$/d' | wc -l`

#             if [ `cat $temp_conf_file | wc -l` -gt 0 ] && [ $running_jobs -lt $max_concur_jobs ]; then
            if [ $running_jobs -lt $max_concur_jobs ]; then
              pass_job
              echo -e "  -> jobcount=$((total_jobs - `cat $temp_conf_file | wc -l`))\n" | tee -a $LOG_FILE
            fi
          fi
        done

      [ -s $temp_conf_file ] || {
        echo -e '\n#####\n  ... done! -- no more scripts to pass ...\n#####\n' | tee -a $LOG_FILE
      }
    done

# update # of currently running jobs
  running_jobs=`cat $list_pids | sed -e '/^$/d' | wc -l`

## (3) After all jobs are passed, then watch PIDs
  if [ `cat $temp_conf_file | sed -e '/^$/d' | wc -l` -eq 0 ] && [ $running_jobs -gt 0 ]; then
    echo "  -----> # jobs still running: $running_jobs -> `cat $list_pids | sed -e ':LP N;s/\n/ /; tLP'`" | tee -a $LOG_FILE
    pid_watch
  fi

# clean up
  rm -rf /tmp/tmp_scheduler*.tmp
