#! /bin/sh
export SCRAM_ARCH=slc3_ia32_gcc323
CMSSWVERSION=CMSSW_0_2_0_pre5
cd ${CMSSWVERSION}/src
OWNER=ECAL
MYAPP=testWriteCalib
eval `scramv1 runtime -sh`
export POOL_AUTH_USER=CMS_VAL_GENERAL_POOL_OWNER
export POOL_AUTH_PASSWORD=val_gen_own_1031
rm -f conddbcatalog.xml
FCpublish -u relationalcatalog_oracle://cms_val_lb.cern.ch/CMS_VAL_GENERAL_POOL_OWNER -d file:conddbcatalog.xml
OWNERPASS=`echo ${OWNER} | gawk '{print substr(tolower($1),0,3);}'`
export POOL_AUTH_USER=CMS_VAL_${OWNER}_POOL_OWNER
export POOL_AUTH_PASSWORD=val_${OWNERPASS}_own_1031
THISDIR=`pwd`
export POOL_CATALOG=file:${THISDIR}/conddbcatalog.xml

###run your writer application here, in the same script!!!!!!
../test/${SCRAM_ARCH}/${MYAPP}

###you may remove setenv  POOL_AUTH_USER, POOL_AUTH_PASSWORD from inside your application.
