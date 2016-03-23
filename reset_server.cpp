#include <mysql.h>
#include <stdlib.h>
#include <stdio.h>

int main(int argc, char **argv) {
    MYSQL *conn = mysql_init(NULL);

    const char *server = "localhost";
    const char *user   = "root";
    const char *pass   = "boincadm";
    const char *db     = "test_ec2";

    // Purge database tables
    if (!conn) {
        fprintf(stderr, "Oh no! Could not allocate mysql data.\n");
        exit(1);
    }

    if (!mysql_real_connect(conn, server, user, pass, db, 0, NULL, 0)) {
        fprintf(stderr, "Oh no! Could not create mysql connection.\n");
        mysql_close(conn);
        exit(1);
    }
    
    if (mysql_query(conn, "DELETE FROM app_version")) {
        fprintf(stderr, "Oh no! Could not clear table 'app_version'.\n");
        mysql_close(conn);
        exit(1);
    }

    if (mysql_query(conn, "DELETE FROM workunit")) {
        fprintf(stderr, "Oh no! Could not clear table 'workunit'.\n");
        mysql_close(conn);
        exit(1);
    }

    if (mysql_query(conn, "DELETE FROM result")) {
        fprintf(stderr, "Oh no! Could not clear table 'result'.\n");
        mysql_close(conn);
        exit(1);
    }

    if (mysql_query(conn, "DELETE FROM msg_to_host")) {
        fprintf(stderr, "Oh no! Could not clear table 'msg_to_host'.\n");
        mysql_close(conn);
        exit(1);
    }

    if (mysql_query(conn, "DELETE FROM msg_from_host")) {
        fprintf(stderr, "Oh no! Could not clear table 'msg_from_host'.\n");
        mysql_close(conn);
        exit(1);
    }

    if (mysql_query(conn, "DELETE FROM host")) {
        fprintf(stderr, "Oh no! Could not clear table 'host'.\n");
        mysql_close(conn);
        exit(1);
    }

    if (mysql_query(conn, "DELETE FROM spark_job")) {
        fprintf(stderr, "Oh no! Could not clear table 'spark_job'.\n");
        mysql_close(conn);
        exit(1);
    }

    if (mysql_query(conn, "DELETE FROM spark_node")) {
        fprintf(stderr, "Oh no! Could not clear table 'spark_node'.\n");
        mysql_close(conn);
        exit(1);
    }

    mysql_close(conn);

    // Run restart script
    system("sudo sh update_app.sh");

    exit(0);
}
