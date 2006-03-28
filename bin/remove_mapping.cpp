#include "PluginManager/PluginManager.h"
#include "SealKernel/ComponentLoader.h"
#include "SealKernel/Context.h"
#include "SealKernel/Service.h"
#include "SealKernel/MessageStream.h"
//#include "SealKernel/Exception.h"
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
      coral::ITable& table =session->nominalSchema().tableHandle("POOL_OR_MAPPING_VERSIONS");
      coral::ITableDataEditor& dataEditor = table.dataEditor();
      coral::AttributeList inputData;
      inputData.extend( "version", typeid(std::string) );
      inputData["version"].data<std::string>()= version ;
      dataEditor.deleteRows( "MAPPING_VERSION = :version",inputData ); 
    }
    if( session->nominalSchema().existsTable("POOL_OR_MAPPING_ELEMENTS") ) {
      coral::ITable& table =session->nominalSchema().tableHandle("POOL_OR_MAPPING_ELEMENTS");
      coral::ITableDataEditor& dataEditor = table.dataEditor();
      coral::AttributeList inputData;
      inputData.extend( "version", typeid(std::string) );
      inputData["version"].data<std::string>()=version;
      dataEditor.deleteRows( "VERSION = :version",inputData ); 
    }
    if( session->nominalSchema().existsTable("POOL_OR_MAPPING_COLUMNS") ) {
      coral::ITable& table =session->nominalSchema().tableHandle("POOL_OR_MAPPING_COLUMNS");
      coral::ITableDataEditor& dataEditor = table.dataEditor();
      coral::AttributeList inputData;
      inputData.extend( "version", typeid(std::string) );
      inputData["version"].data<std::string>()=version;
      dataEditor.deleteRows( "VERSION = :version",inputData ); 
    }
    if( session->nominalSchema().existsTable("POOL_RSS_CONTAINERS") ){
      coral::ITable& table =session->nominalSchema().tableHandle("POOL_OR_MAPPING_COLUMNS");
      coral::ITableDataEditor& dataEditor = table.dataEditor();
      coral::AttributeList inputData;
      inputData.extend( "version", typeid(std::string) );
      inputData["version"].data<std::string>()=version;
      dataEditor.deleteRows( "MAPPING_VERSION = :version",inputData ); 
    }
  }/*catch(const coral::RelationalException& er){
    std::cerr<<"caught pool::RelationalException "<<er.what()<<std::endl;
    exit(-1);
    }
  catch(const pool::Exception& er){
    std::cerr<<er.what()<<std::endl;
    exit(-1);
    }*/catch( std::exception& e ) {
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
  

