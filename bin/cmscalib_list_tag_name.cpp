/**
 *   cmscalib_list_tag_name.cpp
 *
 *   Reads the CALIB CondDB 'METADATA' table 
 *   and lists tag names
 *   
 *   
 *   Author: Katarzyna Dziedziniewicz
 */

#include "DataFormats/EcalDetId/interface/EBDetId.h"
#include "RelationalAccess/IAuthenticationService.h"
#include "RelationalAccess/IRelationalService.h"
#include "RelationalAccess/IRelationalDomain.h"
#include "RelationalAccess/ISession.h"
#include "RelationalAccess/ITransaction.h"
#include "RelationalAccess/ISchema.h"
#include "RelationalAccess/ITable.h"
#include "RelationalAccess/IQuery.h"
#include "RelationalAccess/ICursor.h"
#include "RelationalAccess/ITableDataEditor.h"
#include "RelationalAccess/IBulkOperation.h"
#include "RelationalAccess/SchemaException.h"
#include "CoralBase/AttributeList.h"
#include "CoralBase/Attribute.h"
#include "CoralBase/AttributeSpecification.h"
#include "CoralBase/Exception.h"
#include "SealKernel/Exception.h"
#include "SealKernel/Context.h"
#include "SealKernel/ComponentLoader.h"
#include "SealKernel/IMessageService.h"
#include "PluginManager/PluginManager.h"

#include <iostream>
#include <string>
#include <vector>
#include <time.h>


class CondDBApp {
public:

  /**
   *   App constructor; Makes the database connection
   */
  CondDBApp(std::string connect, std::string user, std::string pass) :
    m_context( new seal::Context)
  {
    seal::PluginManager::get()->initialise();
    seal::Handle<seal::ComponentLoader> loader = new seal::ComponentLoader( m_context );

     loader->load( "SEAL/Services/MessageService" );
    std::vector< seal::Handle<seal::IMessageService> > v_msgSvc;
    m_context->query( v_msgSvc );
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
    if (!::getenv( "POOL_OUTMSG_LEVEL" )){ //if not set, default to warning
      msgSvc->setOutputLevel( seal::Msg::Error);
      //std::cout << "level unset" << std::endl;
    } else {
      msgSvc->setOutputLevel( seal::Msg::Error);
      //std::cout << "level set" << std::endl;
}


    loader->load( "CORAL/Services/RelationalService" );

    loader->load( "CORAL/Services/EnvironmentAuthenticationService" );

    //tu usuniete

    seal::IHandle<coral::IRelationalService> serviceHandle = m_context->query<coral::IRelationalService>( "CORAL/Services/RelationalService" );
    if ( ! serviceHandle ) {
      std::cerr << "[Error] Could not retrieve the relational service" << std::endl;
      exit(-1);
    }

    coral::IRelationalDomain& m_domain = serviceHandle->domainForConnection( connect );
    m_session = std::auto_ptr<coral::ISession>(m_domain.newSession( connect ));
    m_session->connect();
  }

  /**
   *  App destructor
   */
  ~CondDBApp() 
  {
    m_session->disconnect();
    delete m_context;
  }

  void getTokens()
  {

    m_session->startUserSession();
    m_session->transaction().start();
    coral::IQuery* cvQuery = m_session->nominalSchema().tableHandle("METADATA").newQuery();
    cvQuery->addToOutputList("NAME");
    cvQuery->defineOutputType("NAME", "string");
    cvQuery->addToOrderList("NAME");
;
     std::string tagName;
     coral::ICursor& cvCursor = cvQuery->execute();
     while (cvCursor.next()) {
      tagName= cvCursor.currentRow()["NAME"].data<std::string>();

      std::cout <<tagName << std::endl;
     }
   
     m_session->transaction().commit();
     
  
  }


private:
  seal::Context* m_context;
  std::auto_ptr<coral::ISession> m_session;
};



int main (int argc, char* argv[])
{
  std::string connect;
  std::string user;
  std::string pass;

  if (argc != 4) {
    std::cout << "Usage:" << std::endl;
    std::cout << "  " << argv[0] << " <connect string> <user> <pass>" << std::endl;
    exit(-1);
  }

  connect = argv[1];
  user = argv[2];
  pass = argv[3];

  std::string userenv("CORAL_AUTH_USER=");
  userenv += user;
  ::putenv(const_cast<char*>(userenv.c_str()));
  std::string passenv("CORAL_AUTH_PASSWORD=");
  passenv += pass;
  ::putenv(const_cast<char*>(passenv.c_str()));

  try {
    CondDBApp app(connect, user, pass);
    app.getTokens();
  } catch (coral::Exception &e) {
    std::cerr << "coral::Exception:  " << e.what() << std::endl;
//   } catch (seal::Exception &e) {
//     std::cerr << "seal::Exception:  " << e.what() << std::endl;
  } catch (std::exception &e) {
    std::cerr << "ERROR:  " << e.what() << std::endl;
  } catch (...) {
    std::cerr << "Unknown error caught" << std::endl;
  }

  //std::cout << "All Done." << std::endl;

  return 0;
}
