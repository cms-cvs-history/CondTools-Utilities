//#include "CondCore/DBCommon/interface/CommandLine.h"
#include "CondCore/DBCommon/interface/ServiceLoader.h"
#include "CondCore/DBCommon/interface/TokenBuilder.h"
#include "CondCore/DBCommon/interface/DBSession.h"
#include "CondCore/DBCommon/interface/DBWriter.h"
#include "CondCore/DBCommon/interface/Exception.h"
#include "CondCore/MetaDataService/interface/MetaData.h"
#include "CondCore/IOVService/interface/IOV.h"
#include "FWCore/Framework/interface/IOVSyncValue.h"
#include "FWCore/Utilities/interface/Exception.h"
#include "FileCatalog/IFileCatalog.h"
#include "FileCatalog/URIParser.h"
#include "FileCatalog/IFCAction.h"
#include "RelationalAccess/RelationalServiceException.h"
#include "RelationalAccess/IRelationalService.h"
#include "RelationalAccess/IRelationalDomain.h"
#include "RelationalAccess/ISession.h"
#include "RelationalAccess/ITransaction.h"
#include "RelationalAccess/ISchema.h"
#include "RelationalAccess/ITable.h"
#include "RelationalAccess/ITableDataEditor.h"
#include "RelationalAccess/IQuery.h"
#include "RelationalAccess/ICursor.h"
#include "CoralBase/AttributeList.h"
#include "CoralBase/AttributeSpecification.h"
#include "CoralBase/Attribute.h"
#include "CoralBase/Exception.h"
#include "StorageSvc/DbType.h"
//#include "SealKernel/Exception.h"
#include <memory>
#include <string>
#include <iostream>
#include <cstdlib>
#include <boost/program_options.hpp>

int main(int argc, char** argv) {
  boost::program_options::options_description desc("options");
  boost::program_options::options_description visible("Usage: cms_build_iov [options] [iov_name] \n");
  visible.add_options()
    ("connect,c",boost::program_options::value<std::string>(),"connection string(required)")
    ("user,u",boost::program_options::value<std::string>(),"user name (default $CORAL_AUTH_USER)")
    ("pass,p",boost::program_options::value<std::string>(),"password (default $CORAL_AUTH_PASSWORD)")
    ("dictionary,d",boost::program_options::value<std::string>(),"dictionary(required)")
    ("table,t",boost::program_options::value<std::string>(),"payload table name(required)")
    ("object,o",boost::program_options::value<std::string>(),"payload object class name(required)")
    ("container,C",boost::program_options::value<std::string>(),"payload object container name(default same as classname)")
    ("catalog,f",boost::program_options::value<std::string>(),"file catalog contact string (default $POOL_CATALOG)")
    ("appendiov,a","append new data to an existing tag(default off), not valid for infinite IOV")
    ("infinite_iov,i","build infinite iov in run(default off)")
    ("runnumber,r","use run number type (default)")
    ("timestamp,s","use timestamp type")
    ("debug","print debug info (default off)")
    ("help,h", "help message")
    ;
  boost::program_options::options_description hidden("argument");
  hidden.add_options()
    ("iov_name", boost::program_options::value<std::string>(), "name of iov")
    ;
  desc.add(visible).add(hidden);
  boost::program_options::positional_options_description pd;
  pd.add("iov_name",1);
  boost::program_options::variables_map vm;
  try{
    boost::program_options::store(boost::program_options::command_line_parser(argc, argv).options(desc).positional(pd).run(), vm);
    boost::program_options::notify(vm);
  }catch(const boost::program_options::error& er) {
    std::cerr << er.what()<<std::endl;
    return 1;
  }
  bool debug=false;
  bool infiov=false;
  bool appendiov=false;
  unsigned long long endOfTime=0;
  //bool appendiov_check=false;
  if (vm.count("help")) {
    std::cout << visible <<std::endl;;
    return 0;
  }
  if(vm.count("debug")) {
    debug=true;
  }
  if(vm.count("infinite_iov") ){
    infiov=true;
  }
  if( vm.count("runnumber") ) {
    endOfTime=(unsigned long long)edm::IOVSyncValue::endOfTime().eventID().run();
  }else if( vm.count("timestamp") ) {
    endOfTime=edm::IOVSyncValue::endOfTime().time().value();
  }else{
    endOfTime=(unsigned long long)edm::IOVSyncValue::endOfTime().eventID().run(); //default to run number type
  }
  if(!vm.count("connect")){
    std::cerr <<"[Error] no connect[c] option given \n";
    std::cerr<<" please do "<<argv[0]<<" --help \n";
    return 1;
  }
  if(vm.count("appendiov")){
    if(infiov){
      std::cerr<<"cannot append to infinite IOV \n";
      exit(1);
    }
    appendiov=true;
  }
  std::string user("");
  std::string userenv("CORAL_AUTH_USER=");
  std::string connect=vm["connect"].as<std::string>();
  if(vm.count("user")){
    user=vm["user"].as<std::string>();
    userenv+=user;
  }else{
    if(!::getenv( "CORAL_AUTH_USER" )){ 
      std::cerr <<"[Error] no user[u] option given and $CORAL_AUTH_USER is not set";
      std::cerr<<" please do "<<argv[0]<<" --help \n";
      return 1;
    }
  }
  std::string pass("");
  std::string passenv("CORAL_AUTH_PASSWORD=");
  if(vm.count("pass")){
    pass=vm["pass"].as<std::string>();
    passenv+=pass;
  }else{
    if(!::getenv( "CORAL_AUTH_PASSWORD" )){ 
      std::cerr <<"[Error] no pass[p] option given and $CORAL_AUTH_PASSWORD is not set\n";
      std::cerr<<" please do "<<argv[0]<<" --help \n";
      return 1;
    }
  }
  if(!vm.count("dictionary")){
    std::cerr <<"[Error] no dictionary[d] option given \n";
    std::cerr<<" please do "<<argv[0]<<" --help \n";
    return 1;
  }
  std::string dictionary=vm["dictionary"].as<std::string>();
  if(!vm.count("table")){
    std::cerr <<"[Error] no table[t] option given \n";
    std::cerr<<" please do "<<argv[0]<<" --help \n";
    return 1;
  }
  std::string table=vm["table"].as<std::string>();
  if(!vm.count("object")){
    std::cerr <<"[Error] no object[o] option given \n";
    std::cerr<<" please do "<<argv[0]<<" --help \n";
    return 1;
  }
  std::string objectname=vm["object"].as<std::string>();
  std::string containername;
  if( !vm.count("container") ){
    containername=objectname;
  }else{
    containername=vm["container"].as<std::string>();
  }
  std::string catalogname;
  if( !vm.count("catalog") ){
    if(!::getenv("POOL_CATALOG")){
      catalogname="file:PoolFileCatalog.xml";
    }else{
      catalogname=std::string(::getenv("POOL_CATALOG"));
    }
  }else{
    catalogname=vm["catalog"].as<std::string>();
  }
  std::string tag;
  if( !vm.count("iov_name") ){
    std::cerr <<"[Error] No iov_name argument given \n";
    std::cerr<<" please do "<<argv[0]<<" --help \n";
    return 1;
  }else{
    tag=vm["iov_name"].as<std::string>();
  }
  if(debug) {
    std::cout<<"\t connect: "<<connect<<"\n";
    std::cout<<"\t user: "<<user<<"\n";
    std::cout<<"\t pass: "<<pass<<"\n";
    std::cout<<"\t dictionary: "<<dictionary<<"\n";
    std::cout<<"\t table: "<<table<<"\n";
    std::cout<<"\t objectname: "<<objectname<<"\n";
    std::cout<<"\t containername: "<<containername<<"\n";
    std::cout<<"\t catalog: "<<catalogname<<"\n";
    std::cout<<"\t infinite: "<<infiov<<"\n";
    std::cout<<"\t appendiov: "<<appendiov<<"\n";
    std::cout<<"\t iov_name: "<<tag<<"\n";
    std::cout<<"\t end_of_time: "<<endOfTime<<"\n";
  }
  ///end of command parsing
  cond::ServiceLoader* loader=new cond::ServiceLoader;
  try{
    if(debug) {
      loader->loadMessageService(cond::Debug);
    }else{
      loader->loadMessageService();
    }
    if( !user.empty() ){
      ::putenv(const_cast<char*>(userenv.c_str()));
    }
    if( !pass.empty() ){
      ::putenv(const_cast<char*>(passenv.c_str()));
    }
    loader->loadAuthenticationService(cond::Env);
    std::auto_ptr<pool::IFileCatalog> mycatalog(new pool::IFileCatalog);
    mycatalog->addReadCatalog(catalogname);
    pool::FClookup l;
    mycatalog->setAction(l);
    mycatalog->connect();
    mycatalog->start();
    pool::FileCatalog::FileID fid;
    std::string ftype;
    l.lookupFileByPFN(connect,fid,ftype);
    if(fid.empty()){
      std::cerr<<"Error: "<<connect<<" is not registered in the catalog "<<catalogname<<std::endl;
      exit(1);
    }else if( ftype!=pool::POOL_RDBMS_StorageType.storageName() ){
      std::cerr<<"Error: "<<connect<<" is registered in the catalog "<<catalogname<<" with the wrong storage type: "<<ftype<<std::endl;
      exit(1);
    }
    mycatalog->commit();  
    mycatalog->disconnect();

    cond::MetaData meta(connect,*loader);
    std::string myoldIOVtoken;
    if( appendiov ){
      meta.connect();
      myoldIOVtoken=meta.getToken(tag);
      meta.disconnect();
    }
    //create IOV object
    cond::IOV* myIov=new cond::IOV;
    //prepare tokenBuilder
    cond::TokenBuilder tk;
    tk.set(fid, dictionary, objectname, containername );
    
    coral::IRelationalService& relationalService=loader->loadRelationalService();
    coral::IRelationalDomain& domain = relationalService.domainForConnection(connect);
    coral::ISession* coralsession = domain.newSession( connect );
    coralsession->connect();
    coralsession->startUserSession();
    coralsession->transaction().start();
    coral::ITable& mytable=coralsession->nominalSchema().tableHandle(table);
    //std::cout<< "Querying : SELECT IOV_VALUE_ID, TIME FROM "<<tabName<<std::endl;
    std::auto_ptr< coral::IQuery > query1( mytable.newQuery() );
    query1->setRowCacheSize( 100 );
    query1->addToOutputList( "IOV_VALUE_ID" );
    if(!infiov){
      query1->addToOutputList( "TIME" );
      query1->addToOrderList( "TIME" );
    }
    query1->defineOutputType( "IOV_VALUE_ID","unsigned long long" );
    query1->defineOutputType( "TIME","unsigned long long" );
    coral::ICursor& cursor1 = query1->execute();
    //loop over iov values offsetting TIME->object by 1 place and 
    //setting the last object to have an infinite iov

    int rowNum = 0;
    std::string lastObject = "";

    while( cursor1.next() ) {
      rowNum++;
      const coral::AttributeList& row = cursor1.currentRow();
      unsigned long long myl=row["IOV_VALUE_ID"].data<unsigned long long>();
      tk.resetOID(myl);
      if(!infiov){
	if (rowNum == 1) { 
	  lastObject = tk.tokenAsString();
	  continue; 
	}
	unsigned long long mytime=row["TIME"].data<unsigned long long>();
	myIov->iov[mytime]=lastObject;
	lastObject=tk.tokenAsString();
      }else{
	unsigned long long mytime=endOfTime;
	myIov->iov[mytime]=tk.tokenAsString();
      }
    }
    
    if (!infiov) {
      unsigned long long mytime=endOfTime;
      myIov->iov[mytime]=lastObject;
    }

    coralsession->transaction().commit();    
    coralsession->disconnect();
    delete coralsession;

    //writing iov out
    cond::DBSession poolsession(connect,catalogname);
    poolsession.connect( cond::ReadWriteCreate );
    cond::DBWriter iovwriter(poolsession,"cond::IOV");
    poolsession.startUpdateTransaction();
    std::string iovtoken=iovwriter.markWrite<cond::IOV>(myIov);
    if( appendiov ){
      iovwriter.markDelete<cond::IOV>(myoldIOVtoken);
    }
    poolsession.commit();
    poolsession.disconnect();
    if(debug) std::cout<<tag<<" "<<iovtoken<<std::endl;
    
    meta.connect();
    bool result=false;
    if( !appendiov ){
      result=meta.addMapping(tag,iovtoken);
    }else{
      result=meta.replaceToken(tag,iovtoken);
    }
    if(!result){
      std::cerr<< "Error: failed to tag token "<<iovtoken<<std::endl;
      exit(1);
    }
    meta.disconnect();
  }catch(const cond::Exception& er){
    std::cerr<<"cond::Exception "<< er.what()<<std::endl;
    delete loader;
    exit(1);
  }catch(const cms::Exception& er){
    std::cerr<<"cms::Exception "<< er.what()<<std::endl;
    delete loader;
    exit(1);
  }catch(const pool::Exception& er){
    std::cerr<<"pool::Exception "<< er.what()<<std::endl;
    delete loader;
    exit(1);
  }catch(...){
    std::cerr<<"Unknown error "<<std::endl;
    delete loader;
    exit(1);
  }
  delete loader;
  return 0;
}


