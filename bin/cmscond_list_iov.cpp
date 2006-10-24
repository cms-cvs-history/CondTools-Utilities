#include "CondCore/DBCommon/interface/ServiceLoader.h"
#include "CondCore/DBCommon/interface/DBSession.h"
#include "CondCore/DBCommon/interface/Exception.h"
#include "CondCore/MetaDataService/interface/MetaData.h"
#include "CondCore/IOVService/interface/IOVService.h"
#include "CondCore/IOVService/interface/IOVIterator.h"
#include <boost/program_options.hpp>
int main( int argc, char** argv ){
  boost::program_options::options_description desc("options");
  boost::program_options::options_description visible("Usage: cmscond_list_iov [options] \n");
  visible.add_options()
    ("connect,c",boost::program_options::value<std::string>(),"connection string(required)")
    ("user,u",boost::program_options::value<std::string>(),"user name (default \"\")")
    ("pass,p",boost::program_options::value<std::string>(),"password (default \"\")")
    ("catalog,f",boost::program_options::value<std::string>(),"file catalog contact string (default $POOL_CATALOG)")
    ("all,a","list all tags(default mode)")
    ("tag,t",boost::program_options::value<std::string>(),"list info of the specified tag")
    ("help,h", "help message")
    ;
  desc.add(visible);
  boost::program_options::variables_map vm;
  try{
    boost::program_options::store(boost::program_options::command_line_parser(argc, argv).options(desc).run(), vm);
    boost::program_options::notify(vm);
  }catch(const boost::program_options::error& er) {
    std::cerr << er.what()<<std::endl;
    return 1;
  }
  if (vm.count("help")) {
    std::cout << visible <<std::endl;;
    return 0;
  }
  std::string connect;
  std::string catalog("file:PoolFileCatalog.xml");
  std::string user("");
  std::string pass("");
  bool listAll=true;
  std::string tag;
  if(!vm.count("connect")){
    std::cerr <<"[Error] no connect[c] option given \n";
    std::cerr<<" please do "<<argv[0]<<" --help \n";
    return 1;
  }else{
    connect=vm["connect"].as<std::string>();
  }
  if(vm.count("user")){
    user=vm["user"].as<std::string>();
  }
  if(vm.count("pass")){
    pass=vm["pass"].as<std::string>();
  }
  if(vm.count("catalog")){
    catalog=vm["catalog"].as<std::string>();
  }
  if(vm.count("tag")){
    tag=vm["tag"].as<std::string>();
    listAll=false;
  }
  cond::ServiceLoader* loader=new cond::ServiceLoader;
  std::string userenv(std::string("CORAL_AUTH_USER=")+user);
  std::string passenv(std::string("CORAL_AUTH_PASSWORD=")+pass);
  ::putenv(const_cast<char*>(userenv.c_str()));
  ::putenv(const_cast<char*>(passenv.c_str()));
  loader->loadAuthenticationService(cond::Env);
  loader->loadMessageService(cond::Error);
  if( listAll ){
    try{
      cond::MetaData metadata_svc(connect, *loader);
      std::vector<std::string> alltags;
      metadata_svc.connect();
      metadata_svc.listAllTags(alltags);
      std::copy (alltags.begin(),
		 alltags.end(),
		 std::ostream_iterator<std::string>(std::cout,"\n")
		 );
      metadata_svc.disconnect();
      return 0;
    }catch(cond::Exception& er){
      std::cout<<er.what()<<std::endl;
    }catch(std::exception& er){
      std::cout<<er.what()<<std::endl;
    }catch(...){
      std::cout<<"Unknown error"<<std::endl;
    }
  }else{
    try{
      cond::MetaData metadata_svc(connect, *loader);
      std::string token;
      metadata_svc.connect();
      token=metadata_svc.getToken(tag);
      metadata_svc.disconnect();
      cond::DBSession session(connect,catalog);
      cond::IOVService iovservice(session);
      cond::IOVIterator* ioviterator=iovservice.newIOVIterator(token);
      session.connect();
      session.startReadOnlyTransaction();
      while( ioviterator->next() ){
	std::cout<<ioviterator->validity().first<<" "<<ioviterator->validity().second<<std::endl;	
      }
      session.commit();
      session.disconnect();
      delete ioviterator;
    }catch(cond::Exception& er){
      std::cout<<er.what()<<std::endl;
    }catch(std::exception& er){
      std::cout<<er.what()<<std::endl;
    }catch(...){
      std::cout<<"Unknown error"<<std::endl;
    }
  }
  delete loader;
  return 0;
}
