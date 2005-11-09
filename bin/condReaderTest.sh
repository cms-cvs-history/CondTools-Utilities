#! /bin/bash
#Prerequesits: 
#   -CMS and LCG software are installed on the machine
#   -The data payload are already written in the database, IOV is assigned to the payload and the IOV is tagged. This script assumes your tag is ${detector}fall_test where ${detector} is your detector name in lowercase
#   -In case of oracle database, a preallocated oracle catalog is used; in case of sqlite database, the datafile and the catalog file, PoolFileCatalog.xml,generated when writing the data should ALWAYS be moved around together
#INSTRUCTION:
#   -mkdir ${workingdir}
#   -cd ${workingdir}; download this script; 
#   -Change the setup environment section according to the parameters you use for the test
#    -chmod a+x condReaderTest.sh
#    -./condReaderTest.sh
#    This script runs the full chain from boostraping CMSSW, generating the configuration file to run the test
#---------------------------------------------------------------------
# setup environment and user parameters
#vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv
THISDIR=`pwd`
CMSSWVERSION=CMSSW_0_2_0_pre5 #change to the CMSSW version for testing
export SCRAM_ARCH=slc3_ia32_gcc323
MAXEVENTS=10
FIRSTRUN=1
EVENTSINRUN=1
SERVICENAME=cms_val_lb
SERVERNAME=${SERVICENAME}.cern.ch
SERVERHOSTNAME=int2r1-v.cern.ch
GENREADER=CMS_VAL_GENERAL_POOL_READER
GENREADERPASS=val_gen_rea_1031
GENSCHEMA=CMS_VAL_GENERAL_POOL_OWNER
# ------------------------------------------------------------------------
# setup_tns ()
# write tnsnames.ora in the current directory and set variable TNS_ADMIN for running this job to this directory
# Parameters:
# Returns; 0 on success
# ------------------------------------------------------------------------
setup_tns() {
local TNSFILE=tnsnames.ora
rm -f ${TNSFILE}
/bin/cat >  ${TNSFILE} <<EOI
${SERVERNAME} =
   (DESCRIPTION =
     (ADDRESS = (PROTOCOL = TCP)(HOST = ${SERVERHOSTNAME})(PORT = 1521))
     (LOAD_BALANCE = yes)
     (CONNECT_DATA =
       (SERVER = DEDICATED)
       (SERVICE_NAME = ${SERVICENAME})
     )
)
EOI
export TNS_ADMIN=${THISDIR}
echo "[---JOB LOG---] Using TNS_ADMIN=${TNS_ADMIN}, ORACLE server ${SERVERNAME} at host ${SERVERHOSTNAME}"
return 0
}
#-------------------------------------------------------------------------
# bootstrap_cmssw ()
# bootstrap a new CMSSW project, removing old if exists
# Parameters: CMSSWVERSION($1)
# Returns; 0 on success
#-------------------------------------------------------------------------
bootstrap_cmssw () {
if [ -d $1 ]; 
    then /bin/rm -rf $1 ;
fi 
scramv1 project CMSSW $1
cd $1/src
eval `scramv1 runtime -sh`
cd ${THISDIR}
return 0
}
# ------------------------------------------------------------------------
# write_config ()
# write the job configuration file
# Parameters: OWNER($1), OBJNAME($2), MAXEVENTS($3), FIRSTRUN($4), EVENTSINRUN($5)
# Returns; 0 on success
# ------------------------------------------------------------------------
write_config() {
CONFFILE=condRead$2.cfg
local CONNECT=oracle://${SERVERNAME}/CMS_VAL_${1}_POOL_OWNER
local RCD=$2Rcd
local TAG=`echo $1| awk '{print tolower($1)}'`fall_test
/bin/cat >  ${CONFFILE} <<EOI
  process condTEST = {
	path p = { get & print }

	es_source = PoolDBESSource { VPSet toGet = {
                                   {string record = "${RCD}"
                                     string tag = "${TAG}"
                                    } }
		    		    bool loadAll = true
                                    string connect = "${CONNECT}"
			            string timetype = "runnumber" 
				   }

	module print = AsciiOutputModule { }
	
	source = EmptySource {untracked int32 maxEvents = $3 
                untracked uint32 firstRun = $4 
                untracked uint32 numberEventsInRun = $5}

	module get = EventSetupRecordDataGetter { VPSet toGet = {
	       {string record = "${RCD}"
	        vstring data = {"$2"} } 
	       }
	       untracked bool verbose = true 
	}
}
EOI
return 0
}

#main
bootstrap_cmssw ${CMSSWVERSION}
echo "[---JOB LOG---] bootstrap_cmssw status $?"
setup_tns
echo  "[---JOB LOG---] setup_tns status $?"
export POOL_AUTH_USER=${GENREADER}
export POOL_AUTH_PASSWORD=${GENREADERPASS}
rm -f ${THISDIR}/conddbcatalog.xml
echo "[---JOB LOG---] Publishing catalog"
FCpublish -u relationalcatalog_oracle://${SERVERNAME}/${GENSCHEMA} -d file:conddbcatalog.xml
export POOL_CATALOG=file:${THISDIR}/conddbcatalog.xml
#export POOL_OUTMSG_LEVEL=8
for PARAM in "ECAL EcalPedestals" "HCAL HcalPedestals"; do
  set -- $PARAM
  write_config $1 $2 ${MAXEVENTS} ${FIRSTRUN} ${EVENTSINRUN}
  echo "[---JOB LOG---] write_config $1 status $?"
  echo "[---JOB LOG---] running job for $1 $2 using ${CONFFILE}" 
  time cmsRun --parameter-set ${CONFFILE}
done
exit 0


