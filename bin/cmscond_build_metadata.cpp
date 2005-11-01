#include "CondCore/DBCommon/interface/CommandLine.h"
#include "CondCore/MetaDataService/interface/MetaData.h"
#include "PluginManager/PluginManager.h"
#include "POOLCore/POOLContext.h"
#include "POOLCore/PoolMessageStream.h"
#include "SealKernel/Context.h"
#include "SealKernel/MessageStream.h"
#include "SealKernel/Exception.h"

#include <stdexcept>
#include <string>

void printUsage(){
  std::cout<<"usage: cms_build_metadata -c -i -t [-u|-p|-h]" <<std::endl; 
  std::cout<<"-c contact string (mandatory)"<<std::endl;
  std::cout<<"-i input token (mandatory)"<<std::endl;
  std::cout<<"-t tag (mandatory)"<<std::endl;
  std::cout<<" -u username" <<std::endl;
  std::cout<<" -p password" <<std::endl;
  std::cout<<" -h print help message" <<std::endl;
}

int main(int argc, char** argv) {
//   std::string dum1="POOL_AUTH_USER=cms_xiezhen_dev";
//   std::string dum2="POOL_AUTH_PASSWORD=xiezhen123";
//   ::putenv( const_cast<char*>(dum1.c_str()));
//   ::putenv( const_cast<char*>(dum2.c_str()));
  seal::PluginManager::get()->initialise();
  pool::POOLContext::loadComponent( "SEAL/Services/MessageService" );
  pool::POOLContext::loadComponent( "POOL/Services/EnvironmentAuthenticationService" );
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

  std::string  myuri, mytoken, mytag, userName, password;
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
    if( commands.Exists("i") ){
      mytoken=commands.GetByName("i");
    }else{
      std::cerr<<"Error: must specify input token(-i)"<<std::endl;
      exit(-1);
    }
    if( commands.Exists("t") ){
      mytag=commands.GetByName("t");
    }else{
      std::cerr<<"Error: must specify tag(-t)"<<std::endl;
      exit(-1);
    }
    if( commands.Exists("u") ){
      userName=commands.GetByName("u");
      ::putenv( const_cast<char*>(userName.c_str()));
    }
    if( commands.Exists("p") ){
      password=commands.GetByName("p");
      ::putenv( const_cast<char*>( password.c_str() ) );
    }
  }catch(std::string& strError){
    std::cerr<< "Error: command parsing error "<<strError<<std::endl;
    exit(-1);
  }
  if( mytag.empty() ){
    std::cerr<< "Error: empty tag "<<std::endl;
    exit(-1);
  }
  try{
    cond::MetaData meta(myuri);
    bool result=meta.addMapping(mytag, mytoken);
    if(!result){
      std::cerr<< "Error: failed to tag token "<<mytoken<<std::endl;
      exit(-1);
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
  return 0;
}


