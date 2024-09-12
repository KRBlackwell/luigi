import luigi
import subprocess
import time
import os
import json
import logging

#nohup python luigi_sas_example.py > luigi_10sept24.txt

logging.basicConfig(level=logging.DEBUG)
EDITDATAPATH = os.environ['DATA']
LOGDATAPATH = os.environ['LOG']
CY = os.environ['CY']
INPUT_PATH = os.path.join(EDITDATAPATH,'input')
EDIT_PATH = os.path.join(EDITDATAPATH)
LOG_PATH = os.path.join(LOGDATAPATH)
TARGET_LOG_PATH = os.path.join(LOGDATAPATH,'targets')

def load_config(config_file='program_listing.json'):
    with open(config_file, 'r') as f:
        return json.load(f)

class RunSASProgram(luigi.Task):
    program_name = luigi.Parameter()
       
    def requires(self):
        config = load_config()
        dependencies = config.get(self.program_name, [])
        logging.debug(f"Task {self.program_name} requires: {dependencies}")
       
        return [RunSASProgram(program_name=dep) for dep in dependencies]

    def run(self):
        new_program_name = self.program_name.split()[0]
        print(new_program_name)
        logging.debug(f"Running SAS command for {new_program_name}")
        start_time = time.time()

        new_log_name = new_program_name + ".log"
        new_output_name = new_program_name + ".lst"

        if self.program_name.endswith(".sas"):
            prod_command = f"sas {self.program_name} -log {os.path.join(LOG_PATH, new_log_name)} -print {os.path.join(LOG_PATH, new_output_name)}"
        elif self.program_name.endswith(".sh"):
            prod_command = f"bash {self.program_name}"
        try:
            logging.debug(f"Command: {prod_command}")
            process = subprocess.run(prod_command, shell=True, capture_output=True, text=True, check=True)
            #TODO add in grep error
            # grep_error_command = "grep ERROR " + new_log_name + " | grep -v 'Add code to display error message if rewinds exceeded' | grep -v 'PTRANS ERROR.  NUMBER OF REWINDS EXCEDED.' | grep -v '** 6/2015 FIX ERROR ''"
            #add in QA checks
            logging.debug(f"Command stdout: {process.stdout}")
            logging.debug(f"Command stderr: {process.stderr}")
        except subprocess.CalledProcessError as e:
            logging.error(f"Command failed with exit code {e.returncode}")
            logging.error(f"stdout: {e.output}")
            logging.error(f"stderr: {e.stderr}")
            raise

        output_log = process.stdout
        error_log = process.stderr

        end_time = time.time()
        runtime = end_time - start_time
        runtime_format = round(runtime, 2)

        logging.debug(f"Task {new_program_name}, runtime: {runtime_format}\n\n")
    def output(self):
        #I added a special output to each SAS program because targets need to be unique
        #    if it exists, then it thinks things ran already
        #they don't recommend writing to the same dataset in any of the programs, but that's exactly what we do in current production
        #target is typically a file according to the docs, but with SAS I don't want to do that in the params for this simple test
        #     because SAS data gets created right away on disk and is locked. log files are created right away and appended to.
        luigi_log_name = "SIPP_" + self.program_name + ".ran"
        luigi_log_path = os.path.join(TARGET_LOG_PATH, luigi_log_name)
        return luigi.LocalTarget(luigi_log_path)
class SASWorkflow(luigi.WrapperTask):
    def requires(self):
        config = load_config()
        logging.debug(f"Config: {config}, Config.get(0): {config.get(0)}")
        #it will get the dependencies for tasks if you enter the final program that runs
        dependencies = config.get('compare.sas', [])
        if not dependencies:
            logging.error("*******No start_program found in the configuration.")
            return []
        tasks = [RunSASProgram(program_name=dep) for dep in dependencies]
        tasks.append(RunSASProgram(program_name='compare.sas'))
        return tasks

if __name__ == '__main__':
    luigi.run(main_task_cls=SASWorkflow, local_scheduler=True)
