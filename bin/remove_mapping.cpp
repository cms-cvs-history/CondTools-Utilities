#include "CondCore/DBCommon/interface/CommandLine.h"
#include "PluginManager/PluginManager.h"
#include "POOLCore/POOLContext.h"
#include "POOLCore/PoolMessageStream.h"
#include "SealKernel/Context.h"
#include "SealKernel/Service.h"
#include "SealKernel/MessageStream.h"
#include "SealKernel/Exception.h"
#include "RelationalAccess/RelationalException.h"
#include "RelationalAccess/IRelationalService.h"
#include "RelationalAccess/IRelationalDomain.h"
#include "RelationalAccess/IRelationalSession.h"
#include "RelationalAccess/IRelationalTransaction.h"
#include "RelationalAccess/IRelationalSchema.h"
#include "RelationalAccess/IRelationalTable.h"
#include "AttributeList/AttributeList.h"
#include "RelationalAccess/IRelationalTableDataEditor.h"
#include <boost/program_options.hpp>
#include <stdexcept>
#include <string>

int main(int argc, char** argv) {
  seal::PluginManager::get()->initialise();
  pool::POOLContext::loadComponent( "SEAL/Services/MessageService" );
  pool::POOLContext::loadComponent( "POOL/Services/EnvironmentAuthenticationService" );
  pool::POOLContext::loadComponent( "POOL/Services/RelationalService" );
  if(!::getenv( "POOL_OUTMSG_LEVEL" )){ //if not set, default to warning
    pool::POOLContext::setMessageVerbosityLevel(seal::Msg::Error);
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
  boost::program_options::options_description desc("Allowed options");
  boost::program_options::options_description visible("Usage: remove_mapping contactstring mappingversion [-h]\n Options");
  visible.add_options()
    ("help,h", "help message")
    ;
  boost::program_options::options_description hidden(" argument");
  hidden.add_options()
    ("contactString", boost::program_options::value<std::string>(), "contact string to db")
    ("version", boost::program_options::value<std::string>(), "mapping version")
    ;
  desc.add(visible).add(hidden);
  boost::program_options::positional_options_description pd;
  pd.add("contactString",1);
  pd.add("version",2);
  boost::program_options::variables_map vm;
  try{
    boost::program_options::store(boost::program_options::command_line_parser(argc, argv).options(desc).positional(pd).run(), vm);
    
    boost::program_options::notify(vm);
  }catch(const boost::program_options::error& er) {
    std::cerr << er.what()<<std::endl;
    return 1;
  }
  if (vm.count("help")) {
    std::cout << visible <<std::endl;;
    return 0;
  }
  
  if(!vm.count("contactString") || !vm.count("version") ){
    std::cerr <<"[Error] no contactString or no mapping version given \n";
    std::cerr<<" please do "<<argv[0]<<" --help \n";
    return 1;
  }
  
  std::string contact=vm["contactString"].as<std::string>();
  std::string version=vm["version"].as<std::string>();
  //std::cout<<"contactString is "<<contact<<"\n";
  seal::IHandle<pool::IRelationalService> serviceHandle = pool::POOLContext::context()->query<pool::IRelationalService>( "POOL/Services/RelationalService" );
  if ( ! serviceHandle ) {
    std::cerr<<"[Error] Could not retrieve the relational service"<<std::endl;
    return 1;
  }
  pool::IRelationalDomain& domain = serviceHandle->domainForConnection( contact );
  std::auto_ptr< pool::IRelationalSession > session( domain.newSession( contact ) );
  if ( ! session->connect() ) {
    std::cerr<<"[Error] Could not connect to the database server."<<std::endl;
    return 1;
  }  
  if ( ! session->transaction().start() ) {
    std::cerr<<"[Error] Could not start a new transaction."<<std::endl;
  }
  try{
    if( session->userSchema().existsTable("POOL_OR_MAPPING_VERSIONS") ) {
      pool::IRelationalTable& table =session->userSchema().tableHandle("POOL_OR_MAPPING_VERSIONS");
      pool::IRelationalTableDataEditor& dataEditor = table.dataEditor();
      pool::AttributeListSpecification spec;
      spec.push_back( "version", pool::AttributeStaticTypeInfo<std::string>::type_name() );
      pool::AttributeList inputData( spec );
      inputData["version"].setValue<std::string>( version );
      dataEditor.deleteRows( "MAPPING_VERSION = :version",inputData ); 
    }
    if( session->userSchema().existsTable("POOL_OR_MAPPING_ELEMENTS") ) {
      pool::IRelationalTable& table =session->userSchema().tableHandle("POOL_OR_MAPPING_ELEMENTS");
      pool::IRelationalTableDataEditor& dataEditor = table.dataEditor();
      pool::AttributeListSpecification spec;
      spec.push_back( "version", pool::AttributeStaticTypeInfo<std::string>::type_name() );
      pool::AttributeList inputData( spec );
      inputData["version"].setValue<std::string>( version );
      dataEditor.deleteRows( "VERSION = :version",inputData ); 
    }
    if( session->userSchema().existsTable("POOL_OR_MAPPING_COLUMNS") ) {
      pool::IRelationalTable& table =session->userSchema().tableHandle("POOL_OR_MAPPING_COLUMNS");
      pool::IRelationalTableDataEditor& dataEditor = table.dataEditor();
      pool::AttributeListSpecification spec;
      spec.push_back( "version", pool::AttributeStaticTypeInfo<std::string>::type_name() );
      pool::AttributeList inputData( spec );
      inputData["version"].setValue<std::string>( version );
      dataEditor.deleteRows( "VERSION = :version",inputData ); 
    }
    if( session->userSchema().existsTable("POOL_RSS_CONTAINERS") ){
      pool::IRelationalTable& table =session->userSchema().tableHandle("POOL_OR_MAPPING_COLUMNS");
      pool::IRelationalTableDataEditor& dataEditor = table.dataEditor();
      pool::AttributeListSpecification spec;
      spec.push_back( "version", pool::AttributeStaticTypeInfo<std::string>::type_name() );
      pool::AttributeList inputData( spec );
      inputData["version"].setValue<std::string>( version );
      dataEditor.deleteRows( "MAPPING_VERSION = :version",inputData ); 
    }
  }catch(const pool::RelationalException& er){
    std::cerr<<"caught pool::RelationalException "<<er.what()<<std::endl;
    exit(-1);
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
  if ( ! session->transaction().commit() ) {
    throw std::runtime_error( "Could not commit the transaction." );
  }
  session->disconnect();
  return 0;
}
  

