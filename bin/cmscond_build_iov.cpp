//#include "CondCore/DBCommon/interface/CommandLine.h"
#include "CondCore/DBCommon/interface/RelationalStorageManager.h"
#include "CondCore/DBCommon/interface/PoolStorageManager.h"
#include "CondCore/DBCommon/interface/AuthenticationMethod.h"
#include "CondCore/DBCommon/interface/SessionConfiguration.h"
#include "CondCore/DBCommon/interface/ConnectionConfiguration.h"
#include "CondCore/DBCommon/interface/MessageLevel.h"
#include "CondCore/DBCommon/interface/DBSession.h"
#include "CondCore/DBCommon/interface/Time.h"
#include "CondCore/DBCommon/interface/Exception.h"
#include "CondCore/DBCommon/interface/TokenBuilder.h"
#include "CondCore/DBCommon/interface/DBSession.h"
#include "CondCore/IOVService/interface/IOVService.h"
#include "CondCore/IOVService/interface/IOVEditor.h"
#include "CondCore/MetaDataService/interface/MetaData.h"
#include "FWCore/Framework/interface/IOVSyncValue.h"
#include "FWCore/Utilities/interface/Exception.h"
#include "FileCatalog/IFileCatalog.h"
#include "FileCatalog/URIParser.h"
#include "FileCatalog/IFCAction.h"
#include "RelationalAccess/ISessionProxy.h"
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
    ("appendiov,a","append new data(default off). If tag exists, append new data to it; if the tag does not exist, add data to the new tag.")
    ("infinite_iov,i","build infinite iov in run(default off)")
    ("runnumber,r","use run number type (default)")
    ("timestamp,s","use timestamp type")
    ("query,q",boost::program_options::value<std::string>(),"query string")
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
  std::string user("");
  std::string pass("");
  bool debug=false;
  bool infiov=false;
  bool appendiov=false;
  cond::Time_t endOfTime=0;
  std::string query("");
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
    endOfTime=(cond::Time_t)edm::IOVSyncValue::endOfTime().eventID().run();
  }else if( vm.count("timestamp") ) {
    endOfTime=edm::IOVSyncValue::endOfTime().time().value();
  }else{
    endOfTime=(cond::Time_t)edm::IOVSyncValue::endOfTime().eventID().run(); //default to run number type
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
  std::string connect=vm["connect"].as<std::string>();
  if(vm.count("user")){
    user=vm["user"].as<std::string>();
  }
  if(vm.count("pass")){
    pass=vm["pass"].as<std::string>();
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
  std::string catalogname("file:PoolFileCatalog.xml");
  if(vm.count("catalog")){
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
  if( vm.count("query") ){
    query=vm["query"].as<std::string>();
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
    std::cout<<"\t query: "<<query<<"\n";
  }
  ///end of command parsing
  try{
    cond::DBSession* session=new cond::DBSession(true);
    session->sessionConfiguration().setAuthenticationMethod( cond::Env );
    if(debug){
      session->sessionConfiguration().setMessageLevel( cond::Debug );
    }else{
      session->sessionConfiguration().setMessageLevel( cond::Error );
    }
    session->connectionConfiguration().setConnectionRetrialTimeOut( 600 );
    session->connectionConfiguration().enableConnectionSharing();
    session->connectionConfiguration().enableReadOnlySessionOnUpdateConnections();
    std::string userenv(std::string("CORAL_AUTH_USER=")+user);
    std::string passenv(std::string("CORAL_AUTH_PASSWORD=")+pass);
    ::putenv(const_cast<char*>(userenv.c_str()));
    ::putenv(const_cast<char*>(passenv.c_str()));
    session->open();
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
    cond::RelationalStorageManager coraldb(connect,session);
    cond::MetaData meta(coraldb);
    std::string iovtoken("");
    if( appendiov ){
      coraldb.connect(cond::ReadOnly);
      coraldb.startTransaction(true);
      if( meta.hasTag(tag) ){
	iovtoken=meta.getToken(tag);
      }else{
	std::cerr<<"Warning: appending data to non-existing tag "<<tag<<std::endl;
      }
      coraldb.commit();
      coraldb.disconnect();
    }
    cond::TokenBuilder tk;
    tk.set(fid, dictionary, objectname, containername );    
    coraldb.connect(cond::ReadOnly);
    coraldb.startTransaction(true);
    coral::ITable& mytable=coraldb.sessionProxy().nominalSchema().tableHandle(table);
    //std::cout<< "Querying : SELECT IOV_VALUE_ID, TIME FROM "<<tabName<<std::endl;
    std::auto_ptr< coral::IQuery > query1( mytable.newQuery() );
    query1->setRowCacheSize( 100 );
    query1->addToOutputList( "IOV_VALUE_ID" );
    if(!query.empty()){
      coral::AttributeList emptydata;
      query1->setCondition( query, emptydata );
    }
    if(!infiov){
      query1->addToOutputList( "TIME" );
      query1->addToOrderList( "TIME" );
    }
    query1->defineOutputType( "IOV_VALUE_ID","int" );
    query1->defineOutputType( "TIME","unsigned long long" );
    coral::ICursor& cursor1 = query1->execute();
    //loop over iov values offsetting TIME->object by 1 place and 
    //setting the last object to have an infinite iov
    std::string previousPayloadToken;
    int rowNum = 0;
    std::string lastObject = "";
    std::vector< std::pair< cond::Time_t,std::string > > toPut;
    while( cursor1.next() ) {
      rowNum++;
      const coral::AttributeList& row = cursor1.currentRow();
      int myl=row["IOV_VALUE_ID"].data<int>();
      tk.resetOID(myl);
      if(!infiov){
	if (rowNum == 1) { 
	  //pass the first since Time
	  previousPayloadToken = tk.tokenAsString();
	  continue; 
	}
	cond::Time_t mytime=row["TIME"].data<cond::Time_t>();
	toPut.push_back( std::make_pair<cond::Time_t,std::string>(mytime,previousPayloadToken));
	//myIov->iov[mytime]=lastObject;
	previousPayloadToken=tk.tokenAsString();
      }else{
	cond::Time_t mytime=endOfTime;
	//myIov->iov[mytime]=tk.tokenAsString();
	toPut.push_back( std::make_pair<cond::Time_t,std::string>(mytime,tk.tokenAsString()));
      }
    }
    //
    //the very last one
    //
    if (!infiov) {
      cond::Time_t mytime=endOfTime;
      //myIov->iov[mytime]=lastObject;
      toPut.push_back( std::make_pair<cond::Time_t,std::string>(mytime,previousPayloadToken));
    }
    coraldb.commit();
    coraldb.disconnect();

    //writing iov out
    cond::PoolStorageManager pooldb(connect,catalogname,session);
    cond::IOVService iovservice(pooldb);
    cond::IOVEditor* iovEditor=iovservice.newIOVEditor(iovtoken);
    pooldb.connect();
    pooldb.startTransaction(false);
    iovEditor->bulkInsert(toPut);
    pooldb.commit();
    pooldb.disconnect();
    delete iovEditor;
    if(debug) std::cout<<tag<<" "<<iovtoken<<std::endl;
    if( !appendiov ){
      coraldb.connect( cond::ReadWriteCreate );
      coraldb.startTransaction(false);
      meta.addMapping(tag,iovtoken);
      coraldb.commit();
      coraldb.disconnect();
    }
    session->close();
    delete session;
  }catch(const cond::Exception& er){
    std::cerr<<"cond::Exception "<< er.what()<<std::endl;
    exit(1);
  }catch(const cms::Exception& er){
    std::cerr<<"cms::Exception "<< er.what()<<std::endl;
    exit(1);
  }
  return 0;
}


