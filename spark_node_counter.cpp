#include "backend_lib.h"
#include "boinc_db.h"
#include "filesys.h"
#include "miofile.h"
#include "spark_mysql.h"
#include "trickle_handler.h"
#include "sched_util.h"
#include "sched_msgs.h"

#define CURR_TIME time(NULL)

void check_clusters() {
    char query[MAX_QUERY_LENGTH];
    int retval, n_nodes_p, jid;
    MIOFILE mf;
    DB_MSG_TO_HOST mth;

    SPARK_MYSQL* db = new SPARK_MYSQL(retval);
    check_error(retval, "instantiate db");

    sprintf(query,
        "SELECT * FROM spark_job where execute_sent=%d",
        0
    );
    MYSQL_RES* result = db->query(query, retval);
    check_error(retval, query); 

    MYSQL_ROW row;
    while ((row = mysql_fetch_row(result))) {
        n_nodes_p = atoi(row[N_NODES_P]);
        jid       = atoi(row[ID]);
        sprintf(query,
            "SELECT * FROM spark_timeout WHERE jid=%d AND timeout<%ld",
            jid,
            CURR_TIME 
        );
        MYSQL_RES* result2 = db->query(query, retval);
        check_error(retval, query);

        MYSQL_ROW row2;
        int timedout = (int)mysql_num_rows(result2);
        while ((row2 = mysql_fetch_row(result2))) {
            sprintf(query,
                "INSERT INTO spark_node VALUES (%s, %d, \"%s\", %d, \"%s\", %d)", 
                row2[0],
                (-1),
                "error",
                0,
                "-1.-1.-1.-1",
                1
            );
            db->query(query, retval);
            check_error(retval, query);
        }

        sprintf(query,
            "SELECT * FROM spark_node WHERE jid=%d AND master=0",
            jid
        );
        result2 = db->query(query, retval);
        check_error(retval, query);
    
        // TODO zookeeper means multiple masters
        // If number of workers is expected number of workers, signal master
        if ((int)mysql_num_rows(result2) + timedout >= n_nodes_p) {
            sprintf(query,
                "SELECT * FROM spark_node WHERE jid=%d AND master=1",
                jid
            );
            result2 = db->query(query, retval);
            check_error(retval, query);
    
            // NOTE: as long as not using zookeeper, we can assume one single master
            row2 = mysql_fetch_row(result2);
    
            // Create trickle-down
            mth.clear();
            mth.create_time = time(0);
            mth.hostid = atoi(row2[HID]);
            sprintf(mth.xml,
                "<trickle_down>\n"
                "   <result_name>%s</result_name>\n"
                "   <continue>1</continue>\n"
                "</trickle_down>\n",
                row2[RESULT_NAME]
            );
            retval = mth.insert();

            sprintf(query,
                "UPDATE spark_job SET execute_sent=1 WHERE ID=%d",
                jid
            );
            db->query(query, retval);
            check_error(retval, query);
        }
    }
}

int main() {
    int retval;

    retval = config.parse_file();
    if (retval) {
        log_messages.printf(MSG_CRITICAL,
            "Can't parse config.xml: %s\n", boincerror(retval)
        );
        exit(1);
    }

    retval = boinc_db.open(
        config.db_name, config.db_host, config.db_user, config.db_passwd
    );
    if (retval) {
        log_messages.printf(MSG_CRITICAL,
            "boinc_db.open failed: %s\n", boincerror(retval)
        );
        exit(1);
    }

    while (1) {
        check_clusters();
        daemon_sleep(5);
    }
}
