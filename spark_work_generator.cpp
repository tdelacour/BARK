#include "backend_lib.h"
#include "boinc_db.h"
#include "filesys.h"
#include "parse.h"
#include "spark_mysql.h" // SPARK_MYSQL
#include "str_replace.h"
#include "svn_version.h"

#include "sched_util.h"

#define REPLICATION_FACTOR 1
#define CURR_TIME time(NULL)

const char* app_name   = "test_app";
const char* master_in  = "test_app_in_master";
const char* worker_in  = "test_app_in_worker";
const char* master_out = "test_app_out_master";
const char* worker_out = "test_app_out_worker";
const char* job_dir    = "/home/boincadm/projects/test_ec2/spark";
const char* spark_tar  = "spark-1.6.0-bin-hadoop2.6.tgz";

char* in_template_master;
char* in_template_worker;
DB_APP app;
int start_time;

// create one new job for a worker node
int make_worker_job(char* url, int seq, char* jid) {
    DB_WORKUNIT wu;
    char command[2 * MAXPATHLEN], name[256], path[MAXPATHLEN], command_line[64];
    const char* infiles[1];
    int retval;

    sprintf(name, "%s_%ld_%d", "spark_job", CURR_TIME, seq);
    retval = config.download_path(spark_tar, path);
    if (retval) {
        log_messages.printf(
            MSG_CRITICAL
          , "Unable to retrieve download path, returned %d\n"
          , retval
        );
        return retval;
    }

    sprintf(command, "cp %s/%s %s", job_dir, spark_tar, path);
    system(command);

    wu.clear();
    wu.appid = app.id;
    safe_strcpy(wu.name, name);
    wu.rsc_fpops_est = 1e12;
    wu.rsc_fpops_bound = 1e14;
    wu.rsc_memory_bound = 5e8;
    wu.rsc_disk_bound = 1e9;
    wu.delay_bound = 86400;
    wu.min_quorum = REPLICATION_FACTOR;
    wu.target_nresults = REPLICATION_FACTOR;
    wu.max_error_results = REPLICATION_FACTOR*4;
    wu.max_total_results = REPLICATION_FACTOR*8;
    wu.max_success_results = REPLICATION_FACTOR*4;
    infiles[0] = spark_tar;

    // Register the job with BOINC
    sprintf(command_line, "-worker --url %s --jid %s", url, jid);
    sprintf(path, "templates/%s", worker_out);
    return create_work(
        wu,
        in_template_worker,
        path,
        config.project_path(path),
        infiles,
        1,
        config,
        command_line
    );
}

// create one new job for a master node
int make_master_job(char* name, char* n_workers, char* jid) {
    DB_WORKUNIT wu;
    char path[MAXPATHLEN], command[2 * MAXPATHLEN], command_line[64];
    const char* infiles[2];
    int retval;

    retval = config.download_path(name, path);
    if (retval){ 
        log_messages.printf( 
            MSG_CRITICAL
          , "Unable to retrieve download path, returned %d\n"
          , retval
        );
        return retval;
    }
    
    sprintf(command, "cp %s/%s %s", job_dir, name, path);
    system(command);
    config.download_path(spark_tar, path);
    sprintf(command, "cp %s/%s %s", job_dir, spark_tar, path);
    system(command);

    // Fill in the job parameters
    wu.clear();
    wu.appid = app.id;
    safe_strcpy(wu.name, name);
    wu.rsc_fpops_est = 1e12;
    wu.rsc_fpops_bound = 1e14;
    wu.rsc_memory_bound = 5e8;
    wu.rsc_disk_bound = 1e9;
    wu.delay_bound = 86400;
    wu.min_quorum = REPLICATION_FACTOR;
    wu.target_nresults = REPLICATION_FACTOR;
    wu.max_error_results = REPLICATION_FACTOR*4;
    wu.max_total_results = REPLICATION_FACTOR*8;
    wu.max_success_results = REPLICATION_FACTOR*4;
    infiles[0] = name;
    infiles[1] = spark_tar;

    // Register the job with BOINC
    sprintf(command_line, "-master --n_workers %s --jid %s", n_workers, jid);
    sprintf(path, "templates/%s", master_out);
    return create_work(
        wu,
        in_template_master,
        path,
        config.project_path(path),
        infiles,
        2,
        config,
        command_line
    );
}

void scan_for_jobs() {
    int i, retval;
    char query[MAX_QUERY_LENGTH];

    SPARK_MYSQL* db = new SPARK_MYSQL(retval);
    check_error(retval, "instantiate db");

    // MASTER NODE JOBS
    sprintf(query, "SELECT * FROM spark_job WHERE handled=false");
    MYSQL_RES* result = db->query(query, retval);
    check_error(retval, query);

    MYSQL_ROW row;
    while ((row = mysql_fetch_row(result))) {
        // TODO with zookeeper, this will be in a loop bounded w/ n_master
        retval = make_master_job(row[NAME], row[N_WORKERS], row[ID]); 
        if (retval) {
            log_messages.printf(MSG_CRITICAL, 
                "unable to create job for %s\n", row[NAME]);
        } else {
            sprintf(query, 
                "UPDATE spark_job SET handled=true WHERE file=\"%s\"",
                row[NAME]
            );
            db->query(query, retval);
            check_error(retval, query);
        }
    }

    // WORKER NODE JOBS
    sprintf(query, "SELECT * FROM spark_job WHERE url IS NOT NULL AND n_nodes > 0");
    result = db->query(query, retval);
    check_error(retval, query);

    while ((row = mysql_fetch_row(result))) {
        int n_nodes = atoi(row[N_WORKERS]);
        int n_masters = atoi(row[N_MASTERS]);
        
        for (i = 0; i < n_nodes - n_masters; ++i) {
            retval = make_worker_job(row[URL], i, row[ID]);
            if (retval) {
                log_messages.printf(MSG_CRITICAL,
                    "unable to create worker job for %s\n", row[NAME]);
            } else {
                n_nodes--;
                sprintf(query,
                    "UPDATE spark_job SET n_nodes=%d WHERE ID=%s",
                    n_nodes - n_masters,
                    row[ID]
                );
                db->query(query, retval);
                check_error(retval, query);
            }
        }
    }

    db->close();
}

int main(int argc, char** argv) {
    int i, retval;
    char buf[256];

    for (i = 1; i < argc; ++i) {
        if (is_arg(argv[i], "d")) {
            if (!argv[++i]) {
                log_messages.printf(MSG_CRITICAL, "%s requires an argument\n\n", argv[--i]);
                exit(1);
            }
            int dl = atoi(argv[i]);
            log_messages.set_debug_level(dl);
            if (dl == 4) {
                g_print_queries = true;
            }
        }
    }

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
        log_messages.printf(MSG_CRITICAL, "can't open db\n");
        exit(1);
    }

    snprintf(buf, sizeof(buf), "where name='%s'", app_name);
    if (app.lookup(buf)) {
        log_messages.printf(MSG_CRITICAL, "can't find app %s\n", app_name);
        exit(1);
    }

    snprintf(buf, sizeof(buf), "templates/%s", master_in);
    if (read_file_malloc(config.project_path(buf), in_template_master)) {
        log_messages.printf(MSG_CRITICAL, "can't read input template %s\n", buf);
        exit(1);
    }
    
    snprintf(buf, sizeof(buf), "templates/%s", worker_in);
    if (read_file_malloc(config.project_path(buf), in_template_worker)) {
        log_messages.printf(MSG_CRITICAL, "can't read input template %s\n", buf);
        exit(1);
    }

    start_time = time(0);

    log_messages.printf(MSG_NORMAL, "Starting\n");
    
    while (1) {
        scan_for_jobs();
        daemon_sleep(10);
    }
    return 0;
}
