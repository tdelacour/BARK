#include <mysql.h> 
#include <stdio.h>  // fprintf, stderr
#include <stdlib.h> // system
#include <string.h> // strcmp, strcpy
#include <time.h>   // time

#define MAX_PATH_LENGTH 64
#define MAX_QUERY_LENGTH 256

const char* job_dir = "/home/boincadm/projects/test_ec2/spark";
int start_time;

int update_db(int n_masters, int n_nodes) {
    MYSQL* conn = mysql_init(NULL);
    char query[MAX_QUERY_LENGTH];

    const char* server = "localhost";
    const char* user   = "root";
    const char* pass   = "boincadm";
    const char* db     = "test_ec2";

    if (!conn) {
        fprintf(stderr, "Unable to allocate mysql data\n");
        return 1;
    }

    if (!mysql_real_connect(conn, server, user, pass, db, 0, NULL, 0)){
        fprintf(stderr, "Unable to connect to mysql database\n");
        mysql_close(conn);
        return 1;
    }

    sprintf(
        query
      , "CREATE TABLE IF NOT EXISTS %s (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
      , "spark_job"
      , "ID int NOT NULL AUTO_INCREMENT"
      , "file varchar(255) NOT NULL"
      , "n_masters int"
      , "n_nodes int"
      , "handled bool"
      , "url varchar(255)"
      , "result text"
      , "n_nodes_p int"
      , "PRIMARY KEY (ID)"
    );
    if (mysql_query(conn, query)) {
        fprintf(stderr, "Unable to create spark_job table\n");
        mysql_close(conn);
        return 1;
    } 

    sprintf(
        query
      , "INSERT INTO %s (file, n_masters, n_nodes, handled, n_nodes_p) VALUES (\"%s%d.py\", %d, %d, %s, %d)"
      , "spark_job"
      , "spark_job_"
      , start_time
      , n_masters
      , n_nodes
      , "false"
      , n_nodes - n_masters
    );
    if (mysql_query(conn, query)) {
        fprintf(stderr, "Unable to insert spark_job into db\n");
        mysql_close(conn);
        return 1;
    }
    
    return 0;
}

int copy_to_dir(char* path) {
    char command[2 * MAX_PATH_LENGTH];
     
    start_time = time(0);
    sprintf(
        command
      , "cp %s %s/spark_job_%d.py"
      , path
      , job_dir
      , start_time
    );
    return system(command);
}

int main(int argc, char **argv) {
    char path[MAX_PATH_LENGTH];
    int i, retval, n_masters = 1, n_nodes = 2;

    strcpy(path, "default.py");
    for (i = 0; i < argc; ++i) {
        if (strcmp(argv[i], "--job") == 0) {
            if (argc <= i + 1) {
                fprintf(stderr, "Need an argument job!\n");
                return 1;
            }
            strcpy(path, argv[++i]);
        } else if (strcmp(argv[i], "--n_masters") == 0) {
            if (argc <= i + 1) {
                fprintf(stderr, "Need an argument job!\n");
                return 1;
            }
            n_masters = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--n_nodes") == 0) {
            if (argc <= i + 1) {
                fprintf(stderr, "Need an argument job!\n");
                return 1;
            }
            n_nodes = atoi(argv[++i]);

            if (n_nodes < n_masters) {
                fprintf(stderr, "Can't have more masters than nodes\n");
                return 1;
            }
        }
    }

    retval = copy_to_dir(path);
    if (retval) {
        fprintf(stderr, "Unable to copy job to app directory\n");
        return retval;
    }

    retval = update_db(n_masters, n_nodes);
    if (retval) {
        fprintf(stderr, "Unable to update database with new job\n");
        return retval;
    }

    return 0;
}
