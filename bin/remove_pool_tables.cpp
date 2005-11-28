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
  boost::program_options::options_description visible("Usage: remove_pool_tables contactstring [-h]\n Options");
  visible.add_options()
    ("help,h", "help message")
    ;
  boost::program_options::options_description hidden(" argument");
  hidden.add_options()
    ("contactString", boost::program_options::value<std::string>(), "contact string to db")
    ;
  desc.add(visible).add(hidden);
  boost::program_options::positional_options_description pd;
  pd.add("contactString",1);
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
  
  if(!vm.count("contactString")){
    std::cerr <<"[Error] no contactString given \n";
    std::cerr<<" please do "<<argv[0]<<" --help \n";
    return 1;
  }
  std::string contact=vm["contactString"].as<std::string>();
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
      session->userSchema().dropTable("POOL_OR_MAPPING_VERSIONS");
      std::cout<<"table POOL_OR_MAPPING_VERSIONS droped"<<std::endl;
    }
    if( session->userSchema().existsTable("POOL_OR_MAPPING_ELEMENTS") ) {
      session->userSchema().dropTable("POOL_OR_MAPPING_ELEMENTS");
      std::cout<<"table POOL_OR_MAPPING_ELEMENTS droped"<<std::endl;
    }
    if( session->userSchema().existsTable("POOL_OR_MAPPING_COLUMNS") ) {
      session->userSchema().dropTable("POOL_OR_MAPPING_COLUMNS");
      std::cout<<"table POOL_OR_MAPPING_COLUMNS droped"<<std::endl;
    }
    if( session->userSchema().existsTable("POOL_CONT___LINKS") ) {
      session->userSchema().dropTable("POOL_CONT___LINKS");
      std::cout<<"table POOL_CONT___LINKS droped"<<std::endl;
    }
    if( session->userSchema().existsTable("POOL_CONT___PARAMS") ) {
      session->userSchema().dropTable("POOL_CONT___PARAMS");
      std::cout<<"table POOL_CONT___PARAMS droped"<<std::endl;
    }
    if( session->userSchema().existsTable("POOL_CONT___SHAPES") ) {
      session->userSchema().dropTable("POOL_CONT___SHAPES");
      std::cout<<"table POOL_CONT___SHAPES droped"<<std::endl;
    }
    if( session->userSchema().existsTable("POOL_RSS_CONTAINERS") ) {
      session->userSchema().dropTable("POOL_RSS_CONTAINERS");
      std::cout<<"table POOL_RSS_CONTAINERS droped"<<std::endl;
    }
    if( session->userSchema().existsTable("POOL_RSS_DB") ) {
      session->userSchema().dropTable("POOL_RSS_DB");
      std::cout<<"table POOL_RSS_DB droped"<<std::endl;
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
  

