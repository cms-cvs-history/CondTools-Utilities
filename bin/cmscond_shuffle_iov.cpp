#include "CondCore/DBCommon/interface/RelationalStorageManager.h"
#include "CondCore/DBCommon/interface/PoolStorageManager.h"
#include "CondCore/DBCommon/interface/AuthenticationMethod.h"
#include "CondCore/DBCommon/interface/SessionConfiguration.h"
#include "CondCore/DBCommon/interface/ConnectionConfiguration.h"
#include "CondCore/DBCommon/interface/MessageLevel.h"
#include "CondCore/DBCommon/interface/DBSession.h"
#include "CondCore/DBCommon/interface/Exception.h"
#include "CondCore/MetaDataService/interface/MetaData.h"
#include "CondCore/IOVService/interface/IOVService.h"
#include "CondCore/IOVService/interface/IOVIterator.h"
#include <boost/program_options.hpp>
#include <iterator>
#include <iostream>
int main( int argc, char** argv ){
  boost::program_options::options_description desc("options");
  boost::program_options::options_description visible("Usage: cmscond_shuffle_iov [options] inputFile \n");
  visible.add_options()
    ("tag,t",boost::program_options::value<std::string>(),"tag (required)")
    ("connect,c",boost::program_options::value<std::string>(),"connection string(required)")
    ("user,u",boost::program_options::value<std::string>(),"user name (default \"\")")
    ("pass,p",boost::program_options::value<std::string>(),"password (default \"\")")
    ("catalog,f",boost::program_options::value<std::string>(),"file catalog contact string (default file:PoolFileCatalog.xml)")
    ("debug","switch on debug mode")
    ("help,h", "help message")
    ;
  boost::program_options::options_description invisible;
  invisible.add_options()
    ("inputFile",boost::program_options::value<std::string>(), "input file")
    ;
  desc.add(visible);
  desc.add(invisible);
  boost::program_options::positional_options_description posdesc;
  posdesc.add("inputFile", -1);

  boost::program_options::variables_map vm;
  try{
    boost::program_options::store(boost::program_options::command_line_parser(argc, argv).options(desc).positional(posdesc).run(), vm);
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
  std::string inputFileName;
  std::string tag("");
  bool debug=false;
  if( !vm.count("inputFile") ){
    std::cerr <<"[Error] no input file given \n";
    std::cerr<<" please do "<<argv[0]<<" --help \n";
    return 1;
  }else{
    inputFileName=vm["inputFile"].as<std::string>();
  }
  if(!vm.count("connect")){
    std::cerr <<"[Error] no connect[c] option given \n";
    std::cerr<<" please do "<<argv[0]<<" --help \n";
    return 1;
  }else{
    connect=vm["connect"].as<std::string>();
  }
  if(!vm.count("tag")){
    std::cerr <<"[Error] no tag[t] option given \n";
    std::cerr<<" please do "<<argv[0]<<" --help \n";
    return 1;
  }else{
    tag=vm["tag"].as<std::string>();
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
  if(vm.count("debug")){
    debug=true;
  }
  if(debug){
    std::cout<<"inputFile:\t"<<inputFileName<<std::endl;
    std::cout<<"connect:\t"<<connect<<'\n';
    std::cout<<"catalog:\t"<<catalog<<'\n';
    std::cout<<"tag:\t"<<tag<<'\n';
    std::cout<<"user:\t"<<user<<'\n';
    std::cout<<"pass:\t"<<pass<<'\n';
  }
  return 0;
}
