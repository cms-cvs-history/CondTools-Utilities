#include "CondCore/DBCommon/interface/CommandLine.h"
#include "CondCore/DBCommon/interface/TokenBuilder.h"
#include "CondCore/DBCommon/interface/DBWriter.h"
#include "CondCore/IOVService/interface/IOV.h"
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
  std::cout<<"usage: cms_build_iov -c -t -n [-f|-b|-d|-o|-s|-m|-u|-p|-h]" <<std::endl; 
  std::cout<<"-c contact string (mandatory)"<<std::endl;
  std::cout<<"-t tablename (mandatory)"<<std::endl;
  std::cout<<"-n classname (payload class name, mandatory)"<<std::endl;
  std::cout<<"-m containername(optional, default to classname)"<<std::endl;
  std::cout<<"-f file catalog contact string (optional)"<<std::endl;
  std::cout<<"-b build infinit iov (default mode: if TILLTIME not found in the object table, assume infinit iov)"<<std::endl;
  std::cout<<"-d dictionary name (optional, no need if -b or -n )"<<std::endl;
  std::cout<<"-s classid(optional)"<<std::endl;
  std::cout<<" -u username" <<std::endl;
  std::cout<<" -p password" <<std::endl;
  std::cout<<" -h print help message" <<std::endl;
}

int main(int argc, char** argv) {
  std::string dum1="POOL_AUTH_USER=cms_xiezhen_dev";
  std::string dum2="POOL_AUTH_PASSWORD=xiezhen123";
  ::putenv( const_cast<char*>(dum1.c_str()));
  ::putenv( const_cast<char*>(dum2.c_str()));
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
  std::string  myuri, tabName, className, contName, cat, dictName, classId, userName, password;
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
    if( commands.Exists("s") ){
      classId=commands.GetByName("s");
    }
    if( commands.Exists("u") ){
      userName=commands.GetByName("u");
      //std::string poolsetuser=std::string("POOL_AUTH_USER=")+userName;
      //std::cout<<"poolsetuser "<<poolsetuser<<std::endl;
      //std::cout<<"putenv "<<::putenv( const_cast<char*>(poolsetuser.c_str()) )<<std::endl;
    }
    if( commands.Exists("p") ){
      password=commands.GetByName("p");
      //std::string poolsetpass=std::string("POOL_AUTH_PASSWORD=")+password;
      //std::cout<<"poolsetpass "<<poolsetpass<<std::endl;
      //std::cout<<"putenv "<<::putenv( const_cast<char*>( poolsetpass.c_str()) )<<std::endl;
    }
  }catch(std::string& strError){
    std::cerr<< "Error: command parsing error "<<strError<<std::endl;
    exit(-1);
  }
  
  if(classId.empty()&&dictName.empty()){
    std::cerr<< "Error: must specify either classId(-s) or dictionary name(-d)" <<std::endl;
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
    std::cout<<1<<std::endl;
    l.lookupFileByPFN(myuri,fid,ftype);
    std::cout<<2<<std::endl;
    if(fid.empty()){
      std::cerr<<"Error: "<<myuri<<" is not registered in the catalog "<<cat<<std::endl;
      exit(-1);
    }else if( ftype!=pool::POOL_RDBMS_StorageType.storageName() ){
      std::cerr<<"Error: "<<myuri<<" is registered in the catalog "<<cat<<" but has the wrong storage type: "<<ftype<<std::endl;
      exit(-1);
    }
    std::cout<<3<<std::endl;
    mycatalog->commit();  
    mycatalog->disconnect();
    std::cout<<4<<std::endl;
    //create IOV object
    cond::IOV* myIov=new cond::IOV;
    //prepare tokenBuilder
    std::cout<<5<<std::endl;
    cond::TokenBuilder tk;
    tk.setDB(fid);
    if(!classId.empty()){ //classId has precedence over dict
      tk.setContainer(classId,contName);
    }else{
      tk.setContainerFromDict(dictName,className,contName);
    }
    std::cout<<6<<std::endl;
    //prepare RAL queries
    seal::IHandle<pool::IRelationalService> serviceHandle = pool::POOLContext::context()->query<pool::IRelationalService>( "POOL/Services/RelationalService" );
    if ( ! serviceHandle ) {
      throw std::runtime_error( "Could not retrieve the relational service" );
    }
    pool::IRelationalDomain& domain = serviceHandle->domainForConnection(myuri);
    std::cout<<7<<myuri<<std::endl;
    // Creating a session
    std::auto_ptr<pool::IRelationalSession> session(domain.newSession(myuri));
    std::cout<<7.7<<session.get()<<std::endl;
    std::cout<<"userName "<<userName<<std::endl;
    std::cout<<"password "<<password<<std::endl;
    //session->connectAsUser(userName, password);
    if ( ! session->connect() ) {
      std::cout<<"here"<<std::endl;
      throw std::runtime_error( "Could not connect to the database server." );
    }
    std::cout<<7.8<<std::endl;
    // Start a transaction
    if ( ! session->transaction().start() ) {
      throw std::runtime_error( "Could not start a new transaction." );
    }
    std::cout<<7.9<<std::endl;
    pool::IRelationalTable& table=session->userSchema().tableHandle(tabName);
    std::cout<< "Querying : SELECT IOV_VALUE_ID, TILLTIME FROM "<<tabName<<std::endl;
    std::auto_ptr< pool::IRelationalQuery > query1( table.createQuery() );
    std::cout<<8<<std::endl;
    query1->setRowCacheSize( 10 );
    query1->addToOutputList( "IOV_VALUE_ID" );
    query1->addToOutputList( "TILLTIME" );
    pool::IRelationalCursor& cursor1 = query1->process();
    //loop over iov values
    if( cursor1.start()) {
      while( cursor1.next() ) {
	const pool::AttributeList& row = cursor1.currentRow();
	long myl;
	row["IOV_VALUE_ID"].getValue<long>(myl);//the column name should be in DBCommon
	tk.setOID(myl);
	long mytime;
	row["TILLTIME"].getValue<long>(mytime);//the column name should be in DBCommon
	// build payload token
	std::cout<<"myl"<<myl<<std::endl;
	std::cout<<"mytime"<<mytime<<std::endl;
	myIov->iov[mytime]=tk.tokenAsString();
      }
    }
    //writing iov out
    std::cout<<"about to write"<<std::endl;
    cond::DBWriter dbwriter(myuri);
    dbwriter.openContainer("IOV"); //iov container name should be in DBCommon
    dbwriter.startTransaction();
    std::string iovtoken=dbwriter.write<cond::IOV>(myIov);
    dbwriter.commitTransaction();
    std::cout<<iovtoken<<std::endl;//print result to std::cout
    return 0;
  }catch ( pool::RelationalException& re ) {
    std::cerr << "Relational exception from " << re.flavorName() << "    " << re.what() << std::endl;
    if (re.code().isError()){
      exit(re.code().code());
    }
  }catch(const seal::Exception& er){
    std::cerr<<er.what()<<std::endl;
    if (er.code().isError()){
      exit(er.code().code());
    }
  }catch( std::exception& e ) {
    std::cerr << e.what() << std::endl;
    exit(-1);
  }catch( ... ) {
    std::cerr << "Funny error" << std::endl;
    exit(-1);
  }
}


