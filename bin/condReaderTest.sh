#! /bin/bash
#Prerequesits: 
#   -You are running on slc3_ia32_gcc323 platform
#   -You have CMS and LCG software installed on the machine
#   -Your data payload are already written in the database, IOV is assigned to the payload and the IOV is tagged
#   -Your Oracle database instance is registered in the POOL catalog
#   -In case of sqlite database, the datafile and the catalog file, PoolFileCatalog.xml,generated when writing the data should ALWAYS be moved around together
#INSTRUCTION:
#   -Change the setup environment section according to the parameters you use for the test
#     *POOL_AUTH_USER: your database account name. Can be empty for sqlite db
#     *POOL_AUTH_PASSWORD: your db password. Can be empty for sqlite db
#     *CMSSWVERSION: CMSSW project version for your test
#     *DBCONTACT: your database contact string technology_protocol://db/user
#   -Change user parameters section to setup your job
#     *OWNER: your detector name
#     *RCD: record name
#     *TAG: IOV tag name
#     *OBJNAME: the object to retrieve
#     *MAXEVENT: max number of events to be processed
#     *FIRSTRUN: start run number
#     *EVENTSINRUN: number of events in one run
#   -Run the job
#     download this script from CMSSW/CondTools/Utilities/bin
#     chmod a+x condReaderTest.sh
#     This script runs the full chain from boostraping CMSSW, generating the configuration file to run the test
#---------------------------------------------------------------------
# setup environment
#vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv
export SCRAM_ARCH=slc3_ia32_gcc323
export POOL_AUTH_USER=cms_xiezhen_dev #PLEASE CHANGE 
export POOL_AUTH_PASSWORD=xiezhen123  #PLEASE CHANGE
CMSSWVERSION=CMSSW_0_2_0_pre5 #change to the CMSSW version for testing
DBCONTACT=sqlite_file:ecalcalib.db
#---------------------------------------------------------------------
# user parameters
#vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv
OWNER=Ecal #owner of the test
RCD=EcalPedestalsRcd
TAG=EcalPedestals_2008_test
#DBCONTACT=oracle://devdb10/cms_xiezhen_dev
OBJNAME=EcalPedestals
MAXEVENTS=100
FIRSTRUN=1
EVENTSINRUN=10
#-------------------------------------------------------------------------
# Bootstrap CMSSW and write up configuration file
#-------------------------------------------------------------------------
THISDIR=`pwd`
if [ -d ${CMSSWVERSION} ]; 
    then /bin/rm -rf ${CMSSWVERSION};
fi 
scramv1 project CMSSW ${CMSSWVERSION}
cd ${CMSSWVERSION}/src
eval `scramv1 runtime -sh`
CONFFILE=condtest${OWNER}.cfg
cd ${THISDIR}
rm -f ${CONFFILE}
/bin/cat >  ${CONFFILE} <<EOI
  process condTEST = {
	path p = { get & print }

	es_source = PoolDBESSource { VPSet toGet = {
                                   {string record = "${RCD}"
                                     string tag = "${TAG}"
                                    } }
		    		    bool loadAll = true
                                    string connect = "${DBCONTACT}"
			            string timetype = "runnumber" 
				   }

	module print = AsciiOutputModule { }
	

	source = EmptySource {untracked int32 maxEvents = ${MAXEVENTS} 
                untracked uint32 firstRun = ${FIRSTRUN} 
                untracked uint32 numberEventsInRun = ${EVENTSINRUN}}

	module get = EventSetupRecordDataGetter { VPSet toGet = {
	       {string record = "${RCD}"
	        vstring data = {"${OBJNAME}"} } 
	       }
	       untracked bool verbose = true 
	}

}
EOI
#-------------------------------------------------------------------------
# run the test
#------------------------------------------------------------------------- 
echo "Running the test for conditions data reading for ${OWNER}"
cmsRun --parameter-set ${CONFFILE}

