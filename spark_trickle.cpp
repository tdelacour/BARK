//#include "sched_msgs.h"
#include "str_replace.h"
#include "miofile.h"
#include "parse.h"
#include "spark_mysql.h" // SPARK_MYSQL
#include "trickle_handler.h"

SPARK_MYSQL* db;

int handle_trickle_init(int, char**) {
    int retval = 0;

    db = new SPARK_MYSQL(retval);
    check_error(retval, "instantiate db");

    return retval;
}

int handle_trickle(MSG_FROM_HOST& mfh) {
    int retval;
    char result_name[64], url[64], jid[16], query[MAX_QUERY_LENGTH];
    MIOFILE mf;

    mf.init_buf_read(mfh.xml);
    XML_PARSER xp(&mf); 

    while (!xp.get_tag()) {
        if (xp.parse_str("result_name", result_name, sizeof(result_name))) {
            continue;
        }
        if (xp.parse_str("url", url, sizeof(url))) {
            continue;
        }
        if (xp.parse_str("jid", jid, sizeof(jid))) {
            continue;
        }
    }

    // Set url in spark_job to eventually be handled by wg
    sprintf(query,
        "UPDATE spark_job SET url=\"%s\" WHERE ID=%s",
        url,
        jid
    );
    db->query(query, retval);
    check_error(retval, query);

    // Insert new master node entry in spark_node 
    sprintf(query,
        "INSERT INTO spark_node VALUES (%s, %ld, \"%s\", %d)",
        jid,
        mfh.hostid,
        result_name,
        1
    );
    db->query(query, retval);
    check_error(retval, query);

    return retval;
}
