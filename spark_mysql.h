# ifndef _SPARK_MYSQL_
#define _SPARK_MYSQL_

#include <mysql.h>

#define OK         0
#define ERR_ALLOC  1
#define ERR_CONN   2
#define ERR_QUERY  3
#define ERR_RESULT 4

#define ID        0
#define NAME      1
#define N_MASTERS 2
#define N_WORKERS 3
#define URL       5
#define N_NODES_P 7
#define RUNNING   8

#define HID         1
#define RESULT_NAME 2

#define MAX_QUERY_LENGTH 256

class SPARK_MYSQL {
    public:
        SPARK_MYSQL(int &retval);
        MYSQL_RES* query(const char* q, int &retval);
        void close();
    private:
        MYSQL* conn;
        const char* server = "localhost";
        const char* user   = "root";
        const char* pass   = "boincadm";
        const char* db     = "test_ec2";
};
        
void check_error(int retval, const char* msg);

#endif
