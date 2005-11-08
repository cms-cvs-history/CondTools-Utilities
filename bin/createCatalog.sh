#! /bin/bash
export SCRAM_ARCH=slc3_ia32_gcc323
eval `scramv1 runtime -sh`
export POOL_AUTH_PATH=./
#export POOL_AUTH_USER=CMS_VAL_GENERAL_POOL_OWNER #PLEASE CHANGE 
#export POOL_AUTH_PASSWORD=val_gen_own_1031  #PLEASE CHANGE
FCregisterPFN -u relationalcatalog_oracle://cms_val_lb.cern.ch/CMS_VAL_GENERAL_POOL_OWNER -p oracle://cms_val_lb.cern.ch/CMS_VAL_GENERAL_POOL_OWNER -t POOL_RDBMS

FCregisterPFN -u relationalcatalog_oracle://cms_val_lb.cern.ch/CMS_VAL_GENERAL_POOL_OWNER -p oracle://cms_val_lb.cern.ch/CMS_VAL_ECAL_POOL_OWNER -t POOL_RDBMS

FCregisterPFN -u relationalcatalog_oracle://cms_val_lb.cern.ch/CMS_VAL_GENERAL_POOL_OWNER -p oracle://cms_val_lb.cern.ch/CMS_VAL_HCAL_POOL_OWNER -t POOL_RDBMS

FCregisterPFN -u relationalcatalog_oracle://cms_val_lb.cern.ch/CMS_VAL_GENERAL_POOL_OWNER -p oracle://cms_val_lb.cern.ch/CMS_VAL_CSC_POOL_OWNER -t POOL_RDBMS

FCregisterPFN -u relationalcatalog_oracle://cms_val_lb.cern.ch/CMS_VAL_GENERAL_POOL_OWNER -p oracle://cms_val_lb.cern.ch/CMS_VAL_DT_POOL_OWNER -t POOL_RDBMS

FCregisterPFN -u relationalcatalog_oracle://cms_val_lb.cern.ch/CMS_VAL_GENERAL_POOL_OWNER -p oracle://cms_val_lb.cern.ch/CMS_VAL_STRIP_POOL_OWNER -t POOL_RDBMS

FCregisterPFN -u relationalcatalog_oracle://cms_val_lb.cern.ch/CMS_VAL_GENERAL_POOL_OWNER -p oracle://cms_val_lb.cern.ch/CMS_VAL_PIXEL_POOL_OWNER -t POOL_RDBMS

FCregisterPFN -u relationalcatalog_oracle://cms_val_lb.cern.ch/CMS_VAL_GENERAL_POOL_OWNER -p oracle://cms_val_lb.cern.ch/CMS_VAL_PRESH_POOL_OWNER -t POOL_RDBMS

FCregisterPFN -u relationalcatalog_oracle://cms_val_lb.cern.ch/CMS_VAL_GENERAL_POOL_OWNER -p oracle://cms_val_lb.cern.ch/CMS_VAL_RPC_POOL_OWNER -t POOL_RDBMS