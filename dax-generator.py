#!/usr/bin/python

from Pegasus.DAX3 import *

import sys
import os
from stat import *

##############################
## Settings

tasks_per_job = 1

##############################

base_dir = os.getcwd()
inputs_dir = base_dir + "/inputs"

# top level workflow
dax = ADAG("blast")
    
# notifcations on state changes for the dax
dax.invoke("all", "/usr/share/pegasus/notification/email")
    
# Add executables to the DAX-level replica catalog
wrapper = Executable(name="blast-wrapper", arch="x86_64", installed=False)
wrapper.addPFN(PFN("file://" + base_dir + "/blast-wrapper", "local"))
wrapper.addProfile(Profile(Namespace.PEGASUS, "clusters.size", tasks_per_job))
wrapper.addProfile(Profile(Namespace.CONDOR, "requirements", "(HAS_BLAST =?= True)"))
dax.addExecutable(wrapper)

for in_name in os.listdir(inputs_dir):

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
    job.uses(out_file, link=Link.OUTPUT)
    
    dax.addJob(job)

# Write the DAX
f = open("dax.xml", "w")
dax.writeXML(f)
f.close()


