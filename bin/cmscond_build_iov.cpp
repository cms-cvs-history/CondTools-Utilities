#include "CondCore/DBCommon/interface/CommandLine.h"
#include "CondCore/MetaDataService/interface/MetaData.h"
#include "CondCore/DBCommon/interface/TokenBuilder.h"
#include "CondCore/DBCommon/interface/DBWriter.h"
#include "CondCore/IOVService/interface/IOV.h"
#include "FWCore/Framework/interface/IOVSyncValue.h"
#include "FileCatalog/IFileCatalog.h"
#include "FileCatalog/URIParser.h"
#include "FileCatalog/IFCAction.h"
#include "RelationalAccess/RelationalException.h"
#include "RelationalAccess/IRelationalService.h"
#include "RelationalAccess/IRelationalDomain.h"
#include "RelationalAccess/IRelationalSession.h"
#include "RelationalAccess/IRelationalTransaction.h"
#include "RelationalAccess/IRelationalSchema.h"
#include "RelationalAccess/IRelationalTable.h"
#include "RelationalAccess/IRelationalTableDataEditor.h"
#include "RelationalAccess/IRelationalQuery.h"
#include "RelationalAccess/IRelationalCursor.h"
#include "AttributeList/AttributeList.h"
#include "StorageSvc/DbType.h"
#include "PluginManager/PluginManager.h"
#include "POOLCore/POOLContext.h"
#include "POOLCore/PoolMessageStream.h"
#include "SealKernel/Context.h"
#include "SealKernel/MessageStream.h"
#include "SealKernel/Exception.h"
#include "SealKernel/Service.h"
#include <stdexcept>
#include <memory>
//#include <vector>
#include <string>
#include <cstdlib>

void printUsage(){
  std::cout<<"usage: cms_build_iov -c -t -T -n [-f|-b|-d|-o|-m|-h]" <<std::endl; 
  std::cout<<"-c contact string (mandatory)"<<std::endl;
  std::cout<<"-t tablename (mandatory)"<<std::endl;
  std::cout<<"-T tagname (mandatory)"<<std::endl;
  std::cout<<"-n classname (payload class name, mandatory)"<<std::endl;
  std::cout<<"-m containername(optional, default to classname)"<<std::endl;
  std::cout<<"-f file catalog contact string (optional)"<<std::endl;
  std::cout<<"-b build infinit iov (default mode: if TIME not found in the object table, assume infinit iov)"<<std::endl;
  std::cout<<"-d dictionary name (mandatory)"<<std::endl;
  std::cout<<" -h print help message" <<std::endl;
}

int main(int argc, char** argv) {
  seal::PluginManager::get()->initialise();
  pool::POOLContext::loadComponent( "SEAL/Services/MessageService" );
  pool::POOLContext::loadComponent( "POOL/Services/RelationalService" );
  if(!::getenv( "POOL_OUTMSG_LEVEL" )){ //if not set, default to warning
    pool::POOLContext::setMessageVerbosityLevel(seal::Msg::Warning);
  }else{
    pool::PoolMessageStream pms("get threshold");
    pool::POOLContext::setMessageVerbosityLevel( pms.threshold() );
  }
  seal::IHandle<seal::IMessageService> mesgsvc =
    pool::POOLContext::context()->query<seal::IMessageService>("SEAL/Services/MessageService");
  if( mesgsvc ){
    //all logging go to cerr
    mesgsvc->setOutputStream( std::cerr, seal::Msg::Nil );
    mesgsvc->setOutputStream( std::cerr, seal::Msg::Verbose );
    mesgsvc->setOutputStream( std::cerr, seal::Msg::Debug );
    mesgsvc->setOutputStream( std::cerr, seal::Msg::Info );
    mesgsvc->setOutputStream( std::cerr, seal::Msg::Fatal );
    mesgsvc->setOutputStream( std::cerr, seal::Msg::Error );
    mesgsvc->setOutputStream( std::cerr, seal::Msg::Warning );
  } 
  std::string  myuri, tabName, className, contName, cat, dictName, userName, password, mytag;
  bool infiov=false;
  try{
    cond::CommandLine commands(argc,argv);
    if( commands.Exists("h") ){
      printUsage();
      exit(0);
    }
    if( commands.Exists("c") ){
      myuri=commands.GetByName("c");
    }else{
      std::cerr<<"Error: must specify db contact string(-c)"<<std::endl;
      exit(-1);
    }    
    if( commands.Exists("t") ){
      tabName=commands.GetByName("t");
    }else{
      std::cerr<<"Error: must specify table name(-t)"<<std::endl;
      exit(-1);
    }
    if( commands.Exists("T") ){
      mytag=commands.GetByName("T");
    }
    if( commands.Exists("n") ){
      className=commands.GetByName("n");
    }else{
      std::cerr<<"Error: must specify payload class name(-n)"<<std::endl;
      exit(-1);
    }
    if( commands.Exists("m") ){
      contName=commands.GetByName("m");
    }else{
      contName=className;
    }
    if( commands.Exists("f") ){
      cat=commands.GetByName("f");
    }else{
      pool::URIParser p;
      p.parse();   
      cat=p.contactstring();
    }
    if( commands.Exists("b") ){
      infiov=true;
    }
    if( commands.Exists("d") ){
      dictName=commands.GetByName("d");
    }
  }catch(std::string& strError){
    std::cerr<< "Error: command parsing error "<<strError<<std::endl;
    exit(-1);
  }
  
  if(dictName.empty()){
    std::cerr<< "Error: must specify dictionary name(-d)" <<std::endl;
    exit(-1);
  }
  //must be load after the env is set
  pool::POOLContext::loadComponent( "POOL/Services/EnvironmentAuthenticationService" );
  seal::IHandle<seal::Service> authsvc = pool::POOLContext::context()->query<seal::Service>( "POOL/Services/EnvironmentAuthenticationService" );
  if ( ! authsvc ) {
    throw std::runtime_error( "Could not retrieve the EnvironmentAuthenticationService" );
  }
  
  try{
    // catalog lookups
    std::auto_ptr<pool::IFileCatalog> mycatalog(new pool::IFileCatalog);
    mycatalog->addReadCatalog(cat);
    pool::FClookup l;
    mycatalog->setAction(l);
    mycatalog->connect();
    mycatalog->start();
    pool::FileCatalog::FileID fid;
    std::string ftype;
    l.lookupFileByPFN(myuri,fid,ftype);
    if(fid.empty()){
      std::cerr<<"Error: "<<myuri<<" is not registered in the catalog "<<cat<<std::endl;
      exit(-1);
    }else if( ftype!=pool::POOL_RDBMS_StorageType.storageName() ){
      std::cerr<<"Error: "<<myuri<<" is registered in the catalog "<<cat<<" but has the wrong storage type: "<<ftype<<std::endl;
      exit(-1);
    }
    mycatalog->commit();  
    mycatalog->disconnect();
    //create IOV object
    cond::IOV* myIov=new cond::IOV;
    //prepare tokenBuilder
    cond::TokenBuilder tk;
    tk.set(fid, dictName, className, contName );
    //prepare RAL queries
    seal::IHandle<pool::IRelationalService> serviceHandle = pool::POOLContext::context()->query<pool::IRelationalService>( "POOL/Services/RelationalService" );
    if ( ! serviceHandle ) {
      throw std::runtime_error( "Could not retrieve the relational service" );
    }
    pool::IRelationalDomain& domain = serviceHandle->domainForConnection(myuri);
    // Creating a session
    std::auto_ptr<pool::IRelationalSession> session(domain.newSession(myuri));
    if ( ! session->connect() ) {
      throw std::runtime_error( "Could not connect to the database server." );
    }
    // Start a transaction
    if ( ! session->transaction().start() ) {
      throw std::runtime_error( "Could not start a new transaction." );
    }
    pool::IRelationalTable& table=session->userSchema().tableHandle(tabName);
    //std::cout<< "Querying : SELECT IOV_VALUE_ID, TIME FROM "<<tabName<<std::endl;
    std::auto_ptr< pool::IRelationalQuery > query1( table.createQuery() );
    query1->setRowCacheSize( 10 );
    query1->addToOutputList( "IOV_VALUE_ID" );
    if(!infiov){
      query1->addToOutputList( "TIME" );
    }
    pool::IRelationalCursor& cursor1 = query1->process();
    //loop over iov values
    if( cursor1.start()) {
      while( cursor1.next() ) {
	const pool::AttributeList& row = cursor1.currentRow();
	long myl;
	row["IOV_VALUE_ID"].getValue<long>(myl);//the column name should be in DBCommon
	tk.resetOID(myl);
	if(!infiov){
	  long mytime;
	  row["TIME"].getValue<long>(mytime);//the column name should be in DBCommon
	  myIov->iov[mytime]=tk.tokenAsString();
	}else{
	  long mytime=(long)edm::IOVSyncValue::endOfTime().eventID().run();
	  myIov->iov[mytime]=tk.tokenAsString();
	}
      }
    }
    
    if ( ! session->transaction().commit() ) {
      throw std::runtime_error( "Could not commit the transaction." );
    }    
    session->disconnect();
    
    //writing iov out
    cond::DBWriter dbwriter(myuri);
    dbwriter.startTransaction();
    std::string iovtoken=dbwriter.write<cond::IOV>(myIov, "IOV");
    dbwriter.commitTransaction();
    std::cout<<mytag<<" "<<iovtoken<<std::endl;//print result to std::cout
    cond::MetaData meta(myuri);
    bool result=meta.addMapping(mytag, iovtoken);
    if(!result){
      std::cerr<< "Error: failed to tag token "<<iovtoken<<std::endl;
      exit(-1);
    }
  }catch ( pool::RelationalException& re ) {
    std::cerr << "Relational exception from " << re.flavorName() << "    " << re.what() << std::endl;
    if (re.code().isError()){
      exit(re.code().code());
    }
  }catch(const seal::Exception& er){
    std::cerr<<"Seal exception "<< er.what()<<std::endl;
    if (er.code().isError()){
      exit(er.code().code());
    }
  }catch( std::exception& e ) {
    std::cerr << "std exception "<<e.what() << std::endl;
    exit(-1);
  }catch( ... ) {
    std::cerr << "Funny error" << std::endl;
    exit(-1);
  }
}


