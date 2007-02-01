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
#include "CondCore/IOVService/interface/IOVEditor.h"
//#include "CondTools/Utilities/interface/CSVDataLineParser.h"
#include "CondTools/Utilities/interface/CSVHeaderLineParser.h"
#include "CondTools/Utilities/interface/CSVBlankLineParser.h"
#include <boost/program_options.hpp>
#include <boost/spirit/core.hpp>
#include <boost/spirit/utility/lists.hpp>
#include <iterator>
#include <iostream>
#include <fstream>
#include <sstream>
void parseInputFile(std::fstream& inputFile,
		    std::vector< std::pair<cond::Time_t, std::string> >& newValues){
  /*for(cond::Time_t i=1; i<100; ++i){
    newValues.push_back(std::make_pair<cond::Time_t, std::string>(i,"token"));
    }
  */
  unsigned int counter=0;
  std::vector<std::string> fieldNames;
  CSVHeaderLineParser headerParser;
  unsigned int tillidx=0; //default
  unsigned int tokenidx=1;//default
  while (! inputFile.eof() ){
    std::string line;
    std::getline (inputFile,line);
     CSVBlankLineParser blank;
    if(blank.isBlank(line)){
      continue;
    }
    if(counter==0) {
      if(!headerParser.parse(line)) {
	throw cms::Exception("unable to parse header: ")<<line;
      }
      fieldNames=headerParser.result();
      unsigned int idx=0;
      for(std::vector<std::string>::iterator it=fieldNames.begin();
	  it!=fieldNames.end(); ++it, ++idx){
	if( *it==std::string("TILL") || *it==std::string("till") ){
	  tillidx=idx;
	}
	if( *it==std::string("TOKEN") || *it==std::string("token") ){
	  tokenidx=idx;
	}
      }
    }else{
      using namespace boost::spirit;
      std::vector<std::string> result;
      boost::spirit::rule<> strlist_parser;
      strlist_parser=list_p((*anychar_p)[push_back_a(result)],',');
      parse_info<> status=boost::spirit::parse(line.c_str(),strlist_parser);
      if(!status.full) throw cms::Exception("unable to parse data: ")<<line;
      unsigned int idx=0;
      cond::Time_t till=0;
      std::string payloadToken;
      for(std::vector<std::string>::iterator it=result.begin(); 
	  it!=result.end(); ++it, ++idx){
	//std::cout<<idx<<std::endl;
	if( idx==tillidx ){
	  //std::cout<<"is till "<<*it<<std::endl;
	  std::istringstream iss(*it);
	  if((iss>>std::dec>>till).fail()){
	    throw cms::Exception("string conversion failed");
	  }
	}
	if( idx==tokenidx ){
	  //std::cout<<"is token"<<std::endl;
	  payloadToken=*it;
	}
      }
      //std::cout<<"till "<<till<<std::endl;
      //std::cout<<"token "<<payloadToken<<std::endl;
      newValues.push_back(std::make_pair<cond::Time_t, std::string>(till,payloadToken));
    }
    ++counter;
    continue;
  }
}

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
  std::fstream inputFile;
  std::string tag("");
  bool debug=false;
  std::vector< std::pair<cond::Time_t, std::string> > newValues;
  if( !vm.count("inputFile") ){
    std::cerr <<"[Error] no input file given \n";
    std::cerr<<" please do "<<argv[0]<<" --help \n";
    return 1;
  }else{
    inputFileName=vm["inputFile"].as<std::string>();
    inputFile.open(inputFileName.c_str(), std::fstream::in);
    parseInputFile(inputFile,newValues);
    inputFile.close();
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
  std::string iovtoken;
  try{
    cond::DBSession* session=new cond::DBSession(true);
    if(!debug){
      session->sessionConfiguration().setMessageLevel(cond::Error);
    }else{
      session->sessionConfiguration().setMessageLevel(cond::Debug);
    }
    session->open();
    cond::RelationalStorageManager* coraldb=new cond::RelationalStorageManager(connect);
    cond::PoolStorageManager pooldb(connect,catalog,session);
    cond::IOVService iovmanager(pooldb);
    cond::IOVEditor* editor=iovmanager.newIOVEditor("");
    pooldb.connect();
    pooldb.startTransaction(false);
    editor->bulkInsert(newValues);
    iovtoken=editor->token();
    pooldb.commit();
    pooldb.disconnect();
    cond::MetaData* metadata=new cond::MetaData(*coraldb);
    coraldb->connect(cond::ReadWriteCreate);
    coraldb->startTransaction(false);
    metadata->addMapping(tag,iovtoken);
    coraldb->commit();
    coraldb->disconnect();
    
    if(debug){
      std::cout<<"source iov token "<<iovtoken<<std::endl;
    }
    session->close();
    delete editor;
    delete metadata;
    delete session;
  }catch(const cond::Exception& er){
    std::cout<<"error "<<er.what()<<std::endl;
  }catch(const std::exception& er){
    std::cout<<"std error "<<er.what()<<std::endl;
  }
  return 0;
}
