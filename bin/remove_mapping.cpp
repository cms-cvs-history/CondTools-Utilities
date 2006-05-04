#include "CondCore/DBCommon/interface/ServiceLoader.h"
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
  boost::program_options::options_description desc("Allowed options");
  boost::program_options::options_description visible("Usage: remove_mapping contactstring mappingversion [-h]\n Options");
  visible.add_options()
    ("help,h", "help message")
    ("user,u",boost::program_options::value<std::string>(),"user name (default $CORAL_AUTH_USER)")
    ("pass,p",boost::program_options::value<std::string>(),"password (default $CORAL_AUTH_PASSWORD)")
    ("connect,c", boost::program_options::value<std::string>(), "contact string to db(required)")
    ("version,v", boost::program_options::value<std::string>(), "mapping version(required)")
    ;
  boost::program_options::options_description hidden(" argument");
  hidden.add_options()
    ;
  desc.add(visible).add(hidden);
  boost::program_options::positional_options_description pd;
  pd.add("connect",1);
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
  
  if(!vm.count("connect") || !vm.count("version") ){
    std::cerr <<"[Error] no contactString or no mapping version given \n";
    std::cerr<<" please do "<<argv[0]<<" --help \n";
    return 1;
  }
  
  std::string connect=vm["connect"].as<std::string>();
  std::string version=vm["version"].as<std::string>();
  std::string user("");
  std::string userenv("CORAL_AUTH_USER=");
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
  ///
  ///end of command parsing
  ///
  cond::ServiceLoader* loader=new cond::ServiceLoader;
  try{
    loader->loadMessageService();
    if( !user.empty() ){
      ::putenv(const_cast<char*>(userenv.c_str()));
    }
    if( !pass.empty() ){
      ::putenv(const_cast<char*>(passenv.c_str()));
    }
    loader->loadAuthenticationService(cond::Env);
    coral::IRelationalService& relationalService=loader->loadRelationalService();
    coral::IRelationalDomain& domain = relationalService.domainForConnection(connect);
    coral::ISession* session = domain.newSession( connect );
    session->connect();
    session->startUserSession();
    session->transaction().start(false);
    if( session->nominalSchema().existsTable("POOL_OR_MAPPING_VERSIONS") ) {
      coral::ITable& table =session->nominalSchema().tableHandle("POOL_OR_MAPPING_VERSIONS");
      coral::ITableDataEditor& dataEditor = table.dataEditor();
      coral::AttributeList inputData;
      inputData.extend( "version", typeid(std::string) );
      inputData["version"].data<std::string>()= version ;
      dataEditor.deleteRows( "MAPPING_VERSION =:version",inputData ); 
    }
    if( session->nominalSchema().existsTable("POOL_OR_MAPPING_ELEMENTS") ) {
      coral::ITable& table =session->nominalSchema().tableHandle("POOL_OR_MAPPING_ELEMENTS");
      coral::ITableDataEditor& dataEditor = table.dataEditor();
      coral::AttributeList inputData;
      inputData.extend( "version", typeid(std::string) );
      inputData["version"].data<std::string>()=version;
      dataEditor.deleteRows( "VERSION =:version",inputData ); 
    }
    if( session->nominalSchema().existsTable("POOL_OR_MAPPING_COLUMNS") ) {
      coral::ITable& table =session->nominalSchema().tableHandle("POOL_OR_MAPPING_COLUMNS");
      coral::ITableDataEditor& dataEditor = table.dataEditor();
      coral::AttributeList inputData;
      inputData.extend( "version", typeid(std::string) );
      inputData["version"].data<std::string>()=version;
      dataEditor.deleteRows( "VERSION =:version",inputData ); 
    }
    if( session->nominalSchema().existsTable("POOL_RSS_CONTAINERS") ){
      coral::ITable& table =session->nominalSchema().tableHandle("POOL_RSS_CONTAINERS");
      coral::ITableDataEditor& dataEditor = table.dataEditor();
      coral::AttributeList inputData;
      inputData.extend( "version", typeid(std::string) );
      inputData["version"].data<std::string>()=version;
      dataEditor.deleteRows( "MAPPING_VERSION =:version",inputData ); 
    }
    session->transaction().commit();
    session->disconnect();
    delete session;
  }catch( std::exception& e ) {
    std::cerr << e.what() << std::endl;
    exit(-1);
  }catch( ... ) {
      std::cerr << "Funny error" << std::endl;
      exit(-1);
  }
  delete loader;
  return 0;
}


