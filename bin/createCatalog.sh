#! /bin/bash
export SCRAM_ARCH=slc3_ia32_gcc323
export POOL_AUTH_USER=CMS_VAL_GENERAL_POOL_OWNER #PLEASE CHANGE 
export POOL_AUTH_PASSWORD=val_gen_own_1031  #PLEASE CHANGE
FCregisterPFN -u relationalcatalog_oracle://cms_val_lb.cern.ch/CMS_VAL_GENERAL_POOL_OWNER -p cms_val_lb.cern.ch -t POOL_RDBMS

