#include "str_replace.h"
#include "miofile.h"
#include "parse.h"
#include "spark_mysql.h" // SPARK_MYSQL
#include "trickle_handler.h"

#define EXTERNAL_IP_ADDR 41
SPARK_MYSQL* db;

int handle_trickle_init(int, char**) {
    int retval = 0;

    db = new SPARK_MYSQL(retval);
    check_error(retval, "instantiate db");

    return retval;
}

int handle_trickle(MSG_FROM_HOST& mfh) {
    int retval, n_nodes_p, jid, error;
    char result_name[64], query[MAX_QUERY_LENGTH], ip[64];
    MIOFILE mf;
    DB_MSG_TO_HOST mth;

    mf.init_buf_read(mfh.xml);
    XML_PARSER xp(&mf); 

    while (!xp.get_tag()) {
        if (xp.parse_str("result_name", result_name, sizeof(result_name))) {
            continue;
        } 
        if (xp.parse_int("jid", jid)) {
            continue;
        }
        if (xp.parse_int("error", error)) {
            continue;
        }
    }

    // Get IP address (relevant for demo)
    sprintf(query,
        "SELECT * FROM host WHERE id=%ld",
        mfh.hostid
    );
    MYSQL_RES* result = db->query(query, retval);
    check_error(retval, query);

    MYSQL_ROW row = mysql_fetch_row(result);
    if (!row) {
        fprintf(stderr, "incorrect hid: %ld\n", mfh.hostid);
        return 1;
    }
    strncpy(ip, row[EXTERNAL_IP_ADDR], 64);

    // Insert new worker node entry in spark_node 
    sprintf(query,
        "INSERT INTO spark_node VALUES (%d, %ld, \"%s\", %d, \"%s\", %d)",
        jid,
        mfh.hostid,
        result_name,
        0,
        ip,
        error 
    );
    db->query(query, retval);
    check_error(retval, query);

    return retval;
}
