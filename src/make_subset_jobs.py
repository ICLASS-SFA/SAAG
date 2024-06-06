"""
Make job-array slurm scripts to subset variables from the SAAG wrfout files
"""
import glob, os
import textwrap
import subprocess

if __name__=='__main__':

    submit_job = False

    in_dir = '/global/cfs/cdirs/m1657/gsharing/SAAG/WRF4KM_2000-2020/'
    out_dir = '/pscratch/sd/f/feng045/SAAG/hist/OLR_RAINNC/'
    base_name = 'wrfout_d01_'

    # Subset variables, separated by "," without space
    var_names = 'Times,XLAT,XLONG,OLR,RAINNC,I_RAINNC'
    # Slurm task filename for job array
    jobarray_filename = 'tasks_jobarray.txt'
    # Slurm submission script name
    slurm_filename = f'slurm_subset_wrfout_vars.sh'
    # Slurm job wallclock time
    wallclock_time = '06:00:00'
    # Code directory
    code_dir = '/global/homes/f/feng045/program/SAAG/'

    start_year = 2000
    end_year = 2022
    # end_year = 2001

    # Create a task list for job array
    jobarray_file = open(jobarray_filename, "w")
    ntasks = 0

    # Loop over years
    for year in range(start_year, end_year+1):
        # Find all input files
        in_files = sorted(glob.glob(f'{in_dir}{year}/{base_name}*'))

        # Create the list of job tasks needed by SLURM...
        task_filename = f'tasks_subset_{year}.txt'
        task_file = open(task_filename, "w")

        # Make an output directory for the year
        out_dir_year = f'{out_dir}{year}/'
        os.makedirs(out_dir_year, exist_ok=True)

        # Loop over in_files
        for file in in_files:
            # Strip the file path
            fn = os.path.basename(file)
            # Subset command
            cmd = f'ncks -O -v {var_names} {file} {out_dir_year}{fn}'
            task_file.write(f"{cmd}\n")

        task_file.close()
        print(task_filename)

        # Put job command for each year into a task list for job array
        cmd = f'bash {task_filename}'
        jobarray_file.write(f"{cmd}\n")
        ntasks += 1
    # end for file in in_files:

    jobarray_file.close()
    print(jobarray_filename)

    # import pdb; pdb.set_trace()


    # Create a SLURM submission script for the above task list...
    slurm_file = open(slurm_filename, "w")
    text = f"""\
        #!/bin/bash
        #SBATCH -A m1657
        #SBATCH -J SAAGsubset
        #SBATCH -t {wallclock_time}
        ##SBATCH -q regular
        #SBATCH -C cpu
        #SBATCH --ntasks=1
        #SBATCH --cpus-per-task=1
        #SBATCH --qos=shared
        #SBATCH --output=log_subset_saag_wrfout_%A_%a.log
        #SBATCH --mail-type=END
        #SBATCH --mail-user=zhe.feng@pnnl.gov
        #SBATCH --array=1-{ntasks}

        date
        # Activate E3SM environment
        source /global/common/software/e3sm/anaconda_envs/load_latest_e3sm_unified_pm-cpu.sh

        # cd {code_dir}

        # Takes a specified line ($SLURM_ARRAY_TASK_ID) from the task file
        LINE=$(sed -n "$SLURM_ARRAY_TASK_ID"p {jobarray_filename})
        echo $LINE
        # Run the line as a command
        $LINE

        date
    """
    slurm_file.writelines(textwrap.dedent(text))
    slurm_file.close()
    print(slurm_filename)

    # Run command
    if submit_job == True:
        cmd = f'sbatch --array=1-{ntasks} {slurm_filename}'
        print(cmd)
        subprocess.run(f'{cmd}', shell=True)