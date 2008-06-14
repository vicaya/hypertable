
#include <iostream>

#define HAVE_CXX_STDHEADERS 1
#include <db_cxx.h>

using namespace std;

int main(int argc, char **argv) {

  Db db(NULL, 0);               // Instantiate the Db object

  u_int32_t oFlags = DB_CREATE; // Open flags;

  try {
    // Open the database
    db.open(NULL,                // Transaction pointer 
            "my_db.db",          // Database file name 
            NULL,                // Optional logical database name
            DB_BTREE,            // Database access method
            oFlags,              // Open flags
            0);                  // File mode (using defaults)
    // DbException is not subclassed from std::exception, so
    // need to catch both of these.
  } catch(DbException &e) {
    cout << e.what() << endl;
  } catch(std::exception &e) {
    cout << e.what() << endl;
  } 
  
  return 0;
}
