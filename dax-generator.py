#!/usr/bin/python

from Pegasus.DAX3 import *

import sys
import os
from stat import *


def split_list(alist, elements_per_slice):
    results = []
    for start in range(0, len(alist), elements_per_slice):
        results.append(alist[start:start+elements_per_slice])
    return results


def concat_outputs(dax, level, input_list, input_task_map):

    output_list = []
    output_task_map = {}

    # add levels to the workflow until we only have one node
    inputs_per_task = 30
    final_level = False
    if len(input_list) < inputs_per_task:
        final_level = True
    
    count = 0
    for input_set in split_list(input_list, inputs_per_task):
        count += 1
        
        job = Job(name="concat")

        # output for this task
        if final_level:
            out_file = File("full-output.out")
            job.uses(out_file, link=Link.OUTPUT, transfer=True)
        else:
            out_file = File("concat-%d-%d.out" %(level, count))
            job.uses(out_file, link=Link.OUTPUT, transfer=False)
        job.addArguments(out_file)

        # inputs/args for this task
        for in_file in input_set:
            job.uses(in_file, link=Link.INPUT)
            job.addArguments(in_file)

        dax.addJob(job)
            
        # job dependencies
        for in_file in input_set:
            dax.depends(parent = input_task_map[in_file], child=job)

        output_list.append(out_file)
        output_task_map[out_file] = job

    if not final_level:
        concat_outputs(dax, level + 1, output_list, output_task_map)


def add_tasks(dax, base_dir):

    inputs_dir = base_dir + "/inputs"

    # keep track of outputs and tasks so we can concatenate outpus later
    output_list = []
    output_task_map = {}

    reqs = "(HAS_BLAST =?= True)"
    reqs += " && (TARGET.GLIDEIN_ResourceName =!= \"AGLT2\")"
    reqs += " && (TARGET.GLIDEIN_ResourceName =!= \"NYSGRID_CORNELL_NYS1\")"
        
    # Add executables to the DAX-level replica catalog
    concat = Executable(name="concat", arch="x86_64", installed=False)
    concat.addPFN(PFN("file://" + base_dir + "/concat", "local"))
    dax.addExecutable(concat)
    wrapper = Executable(name="blast-wrapper", arch="x86_64", installed=False)
    wrapper.addPFN(PFN("file://" + base_dir + "/blast-wrapper", "local"))
    wrapper.addProfile(Profile(Namespace.CONDOR, "requirements", reqs))
    dax.addExecutable(wrapper)
    
    for in_name in sorted(os.listdir(inputs_dir)):
    
        # add input file to the DAX-level replica catalog
        in_file = File(in_name)
        in_file.addPFN(PFN("file://" + inputs_dir + "/" + in_name, "local"))
        dax.addFile(in_file)
    
        # outputs
        out_name = in_name + ".out"
        out_file = File(out_name)
    
        # job
        job = Job(name="blast-wrapper")
        job.addArguments(in_name, out_name)
        job.uses(in_file, link=Link.INPUT)
        job.uses(out_file, link=Link.OUTPUT, transfer=False)
        
        dax.addJob(job)

        # keep track of outputs and tasks so we can concatenate outpus later
        output_list.append(out_file)
        output_task_map[out_file] = job

    concat_outputs(dax, 2, output_list, output_task_map)


def main():
    base_dir = os.getcwd()

    # top level workflow
    dax = ADAG("blast")
    
    # notifcations on state changes for the dax
    dax.invoke("all", "/usr/share/pegasus/notification/email")

    add_tasks(dax, base_dir)

    # Write the DAX
    f = open("dax.xml", "w")
    dax.writeXML(f)
    f.close()


main()

