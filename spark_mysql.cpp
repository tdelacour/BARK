#include <stdlib.h>

#include "sched_msgs.h"
#include "spark_mysql.h"

SPARK_MYSQL::SPARK_MYSQL(int &retval) {
    conn = mysql_init(NULL);

    if (!conn) {
        retval = ERR_ALLOC;
    } 

    if (!mysql_real_connect(conn, server, user, pass, db, 0, NULL, 0)){
        retval = ERR_CONN;
        mysql_close(conn);
    }
}

MYSQL_RES* SPARK_MYSQL::query(const char* q, int &retval) {
    if (mysql_query(conn, q)) {
        mysql_close(conn);
        retval = ERR_QUERY;
        return NULL;
    }   

    MYSQL_RES* result = mysql_store_result(conn);
    if (!result) {
        retval = ERR_RESULT;
        return NULL; 
    }

    return result;
}

void SPARK_MYSQL::close() {
    mysql_close(conn);
}

void check_error(int retval, const char* msg) {
    switch (retval) {
        case ERR_ALLOC:
            log_messages.printf(MSG_CRITICAL, 
                "unable to allocate db data for mysql object\n");
            break;
        case ERR_CONN:
            log_messages.printf(MSG_CRITICAL,
                "unable to make connection to db test_ec2\n"); 
            break;
        case ERR_QUERY:
            log_messages.printf(MSG_CRITICAL,
                "unable to execute query: %s\n", msg);
            break;
        case ERR_RESULT:
            log_messages.printf(MSG_CRITICAL,
                "unable to get result from query: %s\n", msg);
            break;
        case OK:
            log_messages.printf(MSG_NORMAL,
                "Successfully executed: %s\n", msg);
            break;
        default:
            log_messages.printf(MSG_CRITICAL,
                "received error value: %d\n", retval);
            break;
    }
}


