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
    int retval, shutdown, running, jid;
    char query[MAX_QUERY_LENGTH];
    MIOFILE mf;
    DB_MSG_TO_HOST mth;

    mf.init_buf_read(mfh.xml);
    XML_PARSER xp(&mf); 

    while (!xp.get_tag()) {
        if (xp.parse_int("jid", jid)) {
            continue;
        }
        if (xp.parse_int("shutdown", shutdown)) {
            continue;
        }
        if (xp.parse_int("running", running)) {
            continue;
        }
    }

    if (shutdown) {
        sprintf(query,
            "SELECT * FROM spark_node WHERE jid=%d AND master=0",
            jid
        );
        MYSQL_RES* result = db->query(query, retval);
        check_error(retval, query);

        MYSQL_ROW row;
        while ((row = mysql_fetch_row(result))) {
            mth.clear();
            mth.create_time = time(0);
            mth.hostid = atoi(row[HID]);
            sprintf(mth.xml,
                "<trickle_down>\n"
                "   <result_name>%s</result_name>\n"
                "   <shutdown>1</shutdown>\n"
                "</trickle_down>",
                row[RESULT_NAME]
            );            
            retval = mth.insert();
        }

        // Purge DB
        sprintf(query,
            "DELETE FROM spark_node WHERE jid=%d",
            jid
        );
        db->query(query, retval);
        check_error(retval, query);

        sprintf(query,
            "DELETE FROM spark_timeout WHERE jid=%d",
            jid
        );
        db->query(query, retval);
        check_error(retval, query);
    }

    if (running) {
        sprintf(query, 
            "UPDATE spark_job SET running=1 WHERE ID=%d",
            jid
        );
        db->query(query, retval);
        check_error(retval, query);
    }

    return retval;
}
