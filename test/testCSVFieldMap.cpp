#include "CondTools/Utilities/interface/CSVFieldMap.h"
#include <iostream>
int main(){
  CSVFieldMap mymap;
  mymap.push_back("F1","CHAR");
  mymap.push_back("F2","INT");
  mymap.push_back("F3","FLOAT");
  for(int i=0; i<mymap.size(); ++i){
    std::cout<<i<<" "<<mymap.fieldName(i)<<std::endl;
    if(mymap.fieldType(i)==typeid(std::string)){
      std::cout<<"is string"<<std::endl;
      continue;
    }
    if(mymap.fieldType(i)==typeid(int)){
      std::cout<<"is int"<<std::endl;
      continue;
    }
    if(mymap.fieldType(i)==typeid(float)){
      std::cout<<"is float"<<std::endl;
      continue;
    }
    if(mymap.fieldType(i)==typeid(double)){
      std::cout<<"is double"<<std::endl;
      continue;
    }
  }
}
