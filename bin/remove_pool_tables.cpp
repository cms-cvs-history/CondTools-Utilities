#include "PluginManager/PluginManager.h"
#include "SealKernel/ComponentLoader.h"
#include "SealKernel/Context.h"
#include "SealKernel/Service.h"
#include "SealKernel/MessageStream.h"
#include "SealKernel/Exception.h"
#include "RelationalAccess/IRelationalService.h"
#include "RelationalAccess/IRelationalDomain.h"
#include "RelationalAccess/ISession.h"
#include "RelationalAccess/ITransaction.h"
#include "RelationalAccess/ISchema.h"
#include "RelationalAccess/ITable.h"
#include "CoralBase/AttributeList.h"
#include "CoralBase/AttributeSpecification.h"
#include "CoralBase/Attribute.h"
#include "RelationalAccess/ITableDataEditor.h"
#include <boost/program_options.hpp>
#include <stdexcept>
#include <string>

int main(int argc, char** argv) {
  seal::PluginManager::get()->initialise();
  seal::Context* context = new seal::Context;
  seal::Handle<seal::ComponentLoader> loader = new seal::ComponentLoader( context );
  loader->load( "SEAL/Services/MessageService" );
  loader->load( "CORAL/Services/RelationalService" );
  std::vector< seal::Handle<seal::IMessageService> > v_msgSvc;
  context->query( v_msgSvc );
  seal::Handle<seal::IMessageService> msgSvc;
  if ( ! v_msgSvc.empty() ) {
    msgSvc = v_msgSvc.front();
    msgSvc->setOutputStream( std::cerr, seal::Msg::Nil );
    msgSvc->setOutputStream( std::cerr, seal::Msg::Verbose );
    msgSvc->setOutputStream( std::cerr, seal::Msg::Debug );
    msgSvc->setOutputStream( std::cerr, seal::Msg::Info );
    msgSvc->setOutputStream( std::cerr, seal::Msg::Fatal );
    msgSvc->setOutputStream( std::cerr, seal::Msg::Error );
    msgSvc->setOutputStream( std::cerr, seal::Msg::Warning );
  }
  if(!::getenv( "POOL_OUTMSG_LEVEL" )){ //if not set, default to warning
    msgSvc->setOutputLevel( seal::Msg::Error);
  }else{
    msgSvc->setOutputLevel( seal::Msg::Debug );
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
  seal::IHandle<coral::IRelationalService> serviceHandle = context->query<coral::IRelationalService>( "CORAL/Services/RelationalService" );
  if ( ! serviceHandle ) {
    std::cerr<<"[Error] Could not retrieve the relational service"<<std::endl;
    return 1;
  }
  coral::IRelationalDomain& domain = serviceHandle->domainForConnection( contact );
  std::auto_ptr< coral::ISession > session( domain.newSession( contact ) );
  session->connect();
  session->transaction().start();
  try{
    if( session->nominalSchema().existsTable("POOL_OR_MAPPING_VERSIONS") ) {
      session->nominalSchema().dropTable("POOL_OR_MAPPING_VERSIONS");
      std::cout<<"table POOL_OR_MAPPING_VERSIONS droped"<<std::endl;
    }
    if( session->nominalSchema().existsTable("POOL_OR_MAPPING_ELEMENTS") ) {
      session->nominalSchema().dropTable("POOL_OR_MAPPING_ELEMENTS");
      std::cout<<"table POOL_OR_MAPPING_ELEMENTS droped"<<std::endl;
    }
    if( session->nominalSchema().existsTable("POOL_OR_MAPPING_COLUMNS") ) {
      session->nominalSchema().dropTable("POOL_OR_MAPPING_COLUMNS");
      std::cout<<"table POOL_OR_MAPPING_COLUMNS droped"<<std::endl;
    }
    if( session->nominalSchema().existsTable("POOL_CONT___LINKS") ) {
      session->nominalSchema().dropTable("POOL_CONT___LINKS");
      std::cout<<"table POOL_CONT___LINKS droped"<<std::endl;
    }
    if( session->nominalSchema().existsTable("POOL_CONT___PARAMS") ) {
      session->nominalSchema().dropTable("POOL_CONT___PARAMS");
      std::cout<<"table POOL_CONT___PARAMS droped"<<std::endl;
    }
    if( session->nominalSchema().existsTable("POOL_CONT___SHAPES") ) {
      session->nominalSchema().dropTable("POOL_CONT___SHAPES");
      std::cout<<"table POOL_CONT___SHAPES droped"<<std::endl;
    }
    if( session->nominalSchema().existsTable("POOL_RSS_CONTAINERS") ) {
      session->nominalSchema().dropTable("POOL_RSS_CONTAINERS");
      std::cout<<"table POOL_RSS_CONTAINERS droped"<<std::endl;
    }
    if( session->nominalSchema().existsTable("POOL_RSS_DB") ) {
      session->nominalSchema().dropTable("POOL_RSS_DB");
      std::cout<<"table POOL_RSS_DB droped"<<std::endl;
    }
  }
  /*catch(const pool::RelationalException& er){
    std::cerr<<"caught pool::RelationalException "<<er.what()<<std::endl;
    exit(-1);
    }*/
  catch(const seal::Exception& er){
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
  session->transaction().commit();
  session->disconnect();
  delete context;
  return 0;
}
  

