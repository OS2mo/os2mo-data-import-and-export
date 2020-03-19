Job-runner.d is a way of extracting functionality from job-runner.sh out into smaller files.
This way job-runner can be used:

 * sourced - it will not import everything in job-runner.d
 * running - it will import everything in job-runner.d 

So far, however, everything remains in job-runner and only new functionality is added here
