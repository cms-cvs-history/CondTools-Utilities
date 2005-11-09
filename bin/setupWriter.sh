#! /bin/sh
########################################################################
#Prerequesits:
# --you have an application which writes offline data, example see CMSSW/src/CondTools/Ecal/test/OfflinePedWriter.cpp or CMSSW/src/CondCore/ESSources/test/testWriteCalib.cpp
# --you have bootstraped a CMSSW scram project from where your writer application runs
#INSTRUCTION:
# --download this script and change the section "user defined parameters" and the section "user defined application" according to your setup 
# --run this script in the directory just outside your working CMSSW project
################setting up TNS_ADMIN######################################
THISDIR=`pwd`
TNSFILE=tnsnames.ora
rm -f ${TNSFILE}
/bin/cat >  ${TNSFILE} <<EOI
cms_val_lb.cern.ch =
   (DESCRIPTION =
     (ADDRESS = (PROTOCOL = TCP)(HOST = int2r1-v.cern.ch)(PORT = 1521))
     (LOAD_BALANCE = yes)
     (CONNECT_DATA =
       (SERVER = DEDICATED)
       (SERVICE_NAME = cms_val_lb)
     )
)
EOI
export TNS_ADMIN=${THISDIR}
echo "Using TNS_ADMIN ${TNS_ADMIN}"
##################user defined parameters#################################
export SCRAM_ARCH=slc3_ia32_gcc323
CMSSWVERSION=CMSSW_0_2_0_pre5
OWNER=ECAL
MYAPP=OfflinePedWriter  #your writer application name
##################set up catalog and connection############################
cd ${CMSSWVERSION}/src
eval `scramv1 runtime -sh`
export POOL_AUTH_USER=CMS_VAL_GENERAL_POOL_OWNER
export POOL_AUTH_PASSWORD=val_gen_own_1031
rm -f conddbcatalog.xml
echo "Publishing catalog"
FCpublish -u relationalcatalog_oracle://cms_val_lb.cern.ch/CMS_VAL_GENERAL_POOL_OWNER -d file:conddbcatalog.xml
OWNERPASS=`echo ${OWNER} | gawk '{print substr(tolower($1),0,3);}'`
export POOL_AUTH_USER=CMS_VAL_${OWNER}_POOL_OWNER
export POOL_AUTH_PASSWORD=val_${OWNERPASS}_own_1031
export POOL_CATALOG=file:${THISDIR}/conddbcatalog.xml
####################user defined application##############################
###run your writer application here, in the same script and change whatever parameters are required by your application
##########################################################################
echo "Running ${MYAPP}"
export CONNECT=oracle://cms_val_lb.cern.ch/${POOL_AUTH_USER}
export TAG=ecalfall_test      #tag
../test/${SCRAM_ARCH}/${MYAPP} ${CONNECT} 10 ${TAG}
