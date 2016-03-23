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
    int retval, n_nodes_p;
    char result_name[64], jid[16], query[MAX_QUERY_LENGTH];
    MIOFILE mf;
    DB_MSG_TO_HOST mth;

    mf.init_buf_read(mfh.xml);
    XML_PARSER xp(&mf); 

    while (!xp.get_tag()) {
        if (xp.parse_str("result_name", result_name, sizeof(result_name))) {
            continue;
        } 
        if (xp.parse_str("jid", jid, sizeof(jid))) {
            continue;
        }
    }

    // Insert new worker node entry in spark_node 
    sprintf(query,
        "INSERT INTO spark_node VALUES (%s, %ld, \"%s\", %d)",
        jid,
        mfh.hostid,
        result_name,
        0
    );
    db->query(query, retval);
    check_error(retval, query);

    // Retrieve total number of workers expected
    sprintf(query,
        "SELECT * FROM spark_job WHERE ID=%s",
        jid
    );
    MYSQL_RES* result = db->query(query, retval);
    check_error(retval, query);

    MYSQL_ROW row = mysql_fetch_row(result);
    if (!row) {
        fprintf(stderr, "incorrect jid: %s\n", jid);
        return 1;
    }
    n_nodes_p = atoi(row[N_NODES_P]);

    // Look up all workers recorded in the database
    sprintf(query,
        "SELECT * FROM spark_node WHERE jid=%s AND master=0",
        jid
    );
    result = db->query(query, retval);
    check_error(retval, query);

    // TODO zookeeper means multiple masters
    // If number of workers is expected number of workers, signal master
    if ((int)mysql_num_rows(result) == n_nodes_p) {
        sprintf(query,
            "SELECT * FROM spark_node WHERE jid=%s AND master=1",
            jid
        );
        result = db->query(query, retval);
        check_error(retval, query);

        // NOTE: as long as not using zookeeper, we can assume one single master
        row = mysql_fetch_row(result);

        // Create trickle-down
        mth.clear();
        mth.create_time = time(0);
        mth.hostid = atoi(row[HID]);
        sprintf(mth.xml,
            "<trickle_down>\n"
            "   <result_name>%s</result_name>\n"
            "   <continue>1</continue>\n"
            "</trickle_down>\n",
            row[RESULT_NAME]
        );
        retval = mth.insert();
    }


    return retval;
}
