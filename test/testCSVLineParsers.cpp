#include "CondTools/Utilities/interface/CSVHeaderLineParser.h"
#include "CondTools/Utilities/interface/CSVDataLineParser.h"
#include "CondTools/Utilities/interface/CSVBlankLineParser.h"
#include <iostream>
#include <fstream>
void dodata(CSVDataLineParser& parser, const std::string& inputline){
  bool status=parser.parse(inputline);
  if(status){
    std::vector<boost::any> result=parser.result();
    if(result.size()==0) return;
    for(std::vector<boost::any>::iterator it=result.begin(); it!=result.end(); ++it){
      if(it->type() == typeid(int)){
	try{
	  int me=boost::any_cast<int>(*it);
	  std::cout<<"int "<<me<<std::endl;
	}catch(boost::bad_any_cast& er){
	  std::cout<<er.what()<<std::endl;
	}
      }else if( it->type() == typeid(double) ){
	try{
	  double me=boost::any_cast<double>(*it);
	  std::cout<<"double "<<me<<std::endl;
	}catch(boost::bad_any_cast& er){
	  std::cout<<er.what()<<std::endl;
	}
      }else{
	try{
	  std::string me=boost::any_cast<std::string>(*it);
	  std::cout<<"string "<<me<<std::endl;
	}catch(boost::bad_any_cast& er){
	  std::cout<<er.what()<<std::endl;
	}
      }
    }
  }
}

void doheader(CSVHeaderLineParser& parser, const std::string& inputline){
  bool status=parser.parse(inputline);
  if(status){
    std::vector<std::string> result=parser.result();
    for(std::vector<std::string>::iterator it=result.begin(); it!=result.end(); ++it){
      std::cout<<*it<<std::endl;
    }
  }
}
int main(){
  std::ifstream myfile ("example.txt");
  if (myfile.is_open())
  {
    int counter=0;
    while (! myfile.eof() )
    {
      std::string line;
      std::getline (myfile,line);
      CSVBlankLineParser blank;
      if(blank.isBlank(line)){
	std::cout<<"skip "<<counter<<std::endl;
	continue;
      }
      if(counter<2){//two lines of header
	CSVHeaderLineParser headerParser;
	doheader(headerParser,line);
      }else{
	CSVDataLineParser dataParser;
	dodata(dataParser,line);
      }
      ++counter;
    }
    myfile.close();
    return 0;
  }else{
    std::cout << "Unable to open file"<<std::endl; 
    return 1;
  }
}
