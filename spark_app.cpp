#include <ifaddrs.h> /* getifaddrs, associated structs */
#include <netdb.h>   /* NI_MAXHOST, etc */
#include <stdio.h>   /* fgetc, fopen, fputc, fprintf */
#include <stdlib.h>  /* atoi */
#include <string.h>  /* strcpy */
#include <unistd.h>

#include "boinc_api.h"  
#include "util.h"
#include "diagnostics.h"

#define INPUT "job.py"
#define SPARK_TGZ "spark.tgz"
#define OUTPUT "output.txt"
#define POLL_IVAL 10.0 // In seconds
#define TIMEOUT   30   // In seconds

// Find AF_INET address of this computer (NOT localhost)
//
int get_afinet_host(char* host) {
    struct ifaddrs *ifaddr, *ifa;
    int family, retval;

    if (getifaddrs(&ifaddr)) {
        fprintf(stderr, "Unable to get address info\n");
        return 1;
    }

    for (ifa = ifaddr; ifa != NULL; ifa = ifa->ifa_next) {
        if (ifaddr->ifa_addr == NULL) {
            continue;
        }

        family = ifa->ifa_addr->sa_family;

        if (family == AF_INET) {
            retval = getnameinfo(ifa->ifa_addr,
                        sizeof(struct sockaddr_in),
                        host, NI_MAXHOST, NULL, 0, NI_NUMERICHOST);

            if (retval) {
                fprintf(stderr, "failed to getnameinfo\n");
                return 1;
            }

            // MAJOR POTENTIAL FOR BUG 
            if (strcmp(host, "127.0.0.1") == 0) {
                continue;
            } else {
                return 0;
            }
        }
    }
    return 1;
}

// General (whitespace-delineated) tokenizer for a file
// 
int get_next_token(FILE* file, char* token) {
    int i = 0, c;
    const char* whitespace = " \t\n";

    c = fgetc(file);
    while (strchr(whitespace, c) && c != EOF) {
        c = fgetc(file);
    }

    while (!strchr(whitespace, c) && c != EOF) {
        token[i] = c;
        i++;
        c = fgetc(file);
    }
    token[i] = '\0';

    if (c == EOF) {
        return c;
    }

    if (ferror(file)) {
        fprintf(stderr, "Error reading output\n");
        return 1;
    }

    return 0;
}

// Parse spark start-master.sh output
//
int parse_out(FILE* outfile, char* logpath) {
    char token[128];
    int retval;

    retval = get_next_token(outfile, token);
    while (retval != EOF) {
        if (strcmp(token, "logging") == 0) {
            retval = get_next_token(outfile, token);
            if (retval == EOF) {
                fprintf(stderr, "error in output file\n");
                return 1;
            }

            if (strcmp(token, "to") == 0) {
                retval = get_next_token(outfile, token);
                if (retval == EOF) {
                    fprintf(stderr, "could not get logfile\n");
                    return 1;
                }

                strcpy(logpath, token);
                return 0;
            }
        }

        retval = get_next_token(outfile, token);
    }

    return 1;
}

// Parse spark start-master.sh log
//
int parse_log(FILE* logfile, char* url) {
    char token[512], ip[64], port[5];
    int retval;

    retval = get_next_token(logfile, token);
    while (retval != EOF) {
        if (strcmp(token, "--ip") == 0) {
            get_next_token(logfile, ip);
            retval = get_next_token(logfile, token);
        }

        if (strcmp(token, "--port") == 0) {
            get_next_token(logfile, port);
            sprintf(url, "spark://%s:%s", ip, port);
            return 0;
        }

        retval = get_next_token(logfile, token);
    }

    return 1;
}

int parse_log_worker_count(FILE* logfile, char** workers, int &count) {
    char token[512];
    int retval, i = 0;

    retval = get_next_token(logfile, token);
    while (retval != EOF) {
        if (strcmp(token, "Registering") == 0) {
            retval = get_next_token(logfile, token);
            if (retval != EOF && strcmp(token, "worker") == 0) {
                retval = get_next_token(logfile, token);
                if (retval != EOF) {
                    strcpy(workers[i], token);
                    i++; 
                }
            }   
        }
        retval = get_next_token(logfile, token);
    }

    count = i;
    return 0;
}

// Runs master-node start script and sets spark master url
//
int start_master(char* url, char* logpath){
    char wd[128], command[64], outpath[32], host[NI_MAXHOST];
    FILE* outfile;
    FILE* logfile;
    FILE* spark_env;
    
    // Set up spark environment variables
    if (get_afinet_host(host)) {
        fprintf(stderr, "unable to retrieve an inet addr for this node\n");
        return 1;
    }
    spark_env = boinc_fopen("./spark/conf/spark-env.sh", "w");
    if (!spark_env) {
        fprintf(stderr, "unable to open spark environment configuration\n");
        return 1;
    } 
    fprintf(spark_env, "#!/usr/bin/env bash\nSPARK_MASTER_IP=%s\nSPARK_EXECUTOR_MEMORY=512M\nSPARK_WORKER_MEMORY=512M", host);
    fclose(spark_env);

    sprintf(outpath, "start-master-out");
    getcwd(wd, sizeof(wd));
    chdir("./spark");

    sprintf(command, "./sbin/stop-master.sh");
    system(command);

    sprintf(command, "./sbin/start-master.sh > %s", outpath);
    system(command);

    sprintf(command, "mv %s %s", outpath, wd);
    system(command);

    chdir(wd);
    outfile = boinc_fopen(outpath, "r");
    if (!outfile) {
        fprintf(stderr, "unable to open output file\n");
        return 1;
    }

    if (parse_out(outfile, logpath)) {
        fprintf(stderr, "could not parse output file\n");
        fclose(outfile);
        return 1;
    }
    fclose(outfile);

    boinc_sleep(5); // Wait for master node to startup

    logfile = boinc_fopen(logpath, "r");
    if (!logfile) {
        fprintf(stderr, "unable to open standalone log file\n");
        return 1;
    }

    if (parse_log(logfile, url)) {
        fprintf(stderr, "could not parse log file\n");
        fclose(logfile);
        return 1;
    }
    fclose(logfile);

    return 0;
}

// Attach worker node to given url
//
int start_worker(char* url) {
    char wd[128], command[64];

    getcwd(wd, sizeof(wd));
    chdir("./spark");

    sprintf(command, "./sbin/stop-slave.sh");
    system(command);

    sprintf(command, "./sbin/start-slave.sh %s", url);
    system(command);

    chdir(wd);

    return 0;
}

// Time waster, useful for testing / incremental dev.
//
double do_some_computing(int foo) {
    double x = 3.14159*foo;
    int i;
    for (i=0; i<50000000; i++) {
        x += 5.12313123;
        x *= 0.5398394834;
    }
    return x;
}

// BUGGY
//
int retrieve_trickle_reply(char* reply) {
    int retval;
    char buf[256];

    // Poll periodically until trickle recieved or timeout
    retval = boinc_receive_trickle_down(reply, sizeof(reply));
    while (!retval) {
        boinc_sleep(POLL_IVAL);
        retval = boinc_receive_trickle_down(reply, sizeof(reply));      
    } 

    fprintf(stderr, "%s reply is: %s\n",
        boinc_msg_prefix(buf, sizeof(buf)), reply 
    ); 

    return 0;
}

int wait_on_workers(int n_workers, char** workers, char* logpath) {
    FILE* logfile;
    int retval, i, count = 0;

    // Poll for max 10 minutes
    for (i = 0; i < 100; ++i) {
        logfile = boinc_fopen(logpath, "r");
        if (!logfile) {
            fprintf(stderr, "Unable to open logfile to read worker info\n");
            return 0; // Conservatively estimate 0 workers connected
        }
    
        retval = parse_log_worker_count(logfile, workers, count);
        if (retval) {
            fprintf(stderr, "Unable to parse logfile to read worker info\n");
            return 0;
        }   

        if (count == n_workers) {
            return count;
        } 
        fclose(logfile);

        boinc_sleep(10);
    }

    return count;
}

int execute_job(char* url) {
    char wd[128], command[300], output_path[MAXPATHLEN];
    FILE* outfile;
    FILE* job_result;
    int retval, c;

    // Discover physical output path and open file for writing
    retval = boinc_resolve_filename(OUTPUT, output_path, MAXPATHLEN);
    if (retval) {
        fprintf(stderr, "resolve output filename returned %d\n", retval);
        return retval;
    }
    outfile = boinc_fopen(output_path, "w");
    if (!outfile) {
        fprintf(stderr, "Unable to open output file\n");
        return 1;
    }

    getcwd(wd, sizeof(wd));
    chdir("./spark");

    sprintf(command, "./bin/spark-submit --verbose --master %s ../%s %d > out_job", url, INPUT, 100);
    system(command);

    job_result = boinc_fopen("out_job", "r");
    if (!job_result) {
        fprintf(stderr, "Unable to open output file from spark submit\n");
        return 1;
    }

    c = fgetc(job_result);
    while (c != EOF) {
        fputc(c, outfile);
        c = fgetc(job_result);
    }
    fclose(outfile);

    chdir(wd);

    return 0;
}

void stop_worker() {
    char wd[128], command[64];

    getcwd(wd, sizeof(wd));
    chdir("./spark");

    sprintf(command, "./sbin/stop-slave.sh");
    system(command);

    chdir(wd);
    return;
}

void stop_master() {
    char wd[128], command[64];

    getcwd(wd, sizeof(wd));
    chdir("./spark");

    sprintf(command, "./sbin/stop-master.sh");
    system(command);    

    chdir(wd);
    return;
}

int master_main(int argc, char** argv, int jid) {
    char buf[256], command[300], input_path[MAXPATHLEN], 
         url[64], logpath[128], reply[256];
    char** workers;
    int i, retval, n_workers = -1;

    // Process command line
    for (i = 0; i < argc; ++i) {
        if (strcmp(argv[i], "--n_workers") == 0) {
            if (i + 1 >= argc) {
                fprintf(stderr, "need argument to nworkers\n");
                return 1;
            }
            n_workers = atoi(argv[i + 1]);
        }
        i++;
    }

    // If n_workers was not set, or set to negative number
    if (n_workers < 0) {
        fprintf(stderr, "incorrect number of workers specified\n");
        return 1;
    } 

    // Allocate space to store worker names
    workers = (char**)malloc(n_workers * sizeof(char*));
    for (i = 0; i < n_workers; ++i) {
        workers[i] = (char*)malloc(64 * sizeof(char)); // bc what hostname is more than 64 chars?
    }
    
    // Discover physical input path
    retval = boinc_resolve_filename(SPARK_TGZ, input_path, MAXPATHLEN);
    if (retval) {
        fprintf(stderr, "resolve input filename returned %d\n", retval);
        return retval;
    }

    // Inflate local spark directory
    sprintf(command, "tar zxvf %s", input_path);
    retval = system(command);
    if (retval) {
        fprintf(stderr, "%s command returned error: %d\n", command, retval);
        return retval;
    }

    // Kick off master and get URL when running
    retval = start_master(url, logpath); 
    if (retval) {
        fprintf(stderr, "failed to start master node: %d\n", retval);
        return retval;
    }

    // Report URL back to BOINC server
    sprintf(buf, "<url>%s</url>\n<jid>%d</jid>", url, jid);
    retval = boinc_send_trickle_up(
        const_cast<char*>("master_url"),
        buf
    );
    if (retval) {
        fprintf(stderr, "%s boinc_send_trickle_up returned %d\n",
            boinc_msg_prefix(buf, sizeof(buf)), retval
        );
        return retval;
    }    

    retval = start_worker(url);
    if (retval) {
        fprintf(stderr, "unable to start worker on this node\n");
        return retval;
    }
    
    // TODO figure out how to get actual trickle message, then 
    // actually check message and confirm contents
    retval = retrieve_trickle_reply(reply);
    if (retval) {
        fprintf(stderr, "not able to retrieve trickle reply\n");
        return retval;
    } 

    retval = wait_on_workers(n_workers, workers, logpath);
    fprintf(stderr, "Running job on %d out of %d workers expected\n", 
        retval, n_workers);
    for (i = 0; i < retval; ++i) {
        fprintf(stderr, "%s\n", workers[i]);
    }

    retval = execute_job(url);
    if (retval) {
        fprintf(stderr, "error executing spark job\n");
        return retval;
    }    

    sprintf(buf, "<shutdown>1</shutdown>\n<jid>%d</jid>", jid);
    retval = boinc_send_trickle_up(
        const_cast<char*>("spark_shutdown"),
        buf
    );
    if (retval) {
        fprintf(stderr, "%s boinc_send_trickle_up returned %d\n",
            boinc_msg_prefix(buf, sizeof(buf)), retval
        );
        return retval;
    }  

    // Give some time to send trickle up
    boinc_sleep(10);

    stop_master();
    stop_worker();

    return 0;
}

int worker_main(int argc, char** argv, int jid) {
    char reply[256], buf[256], url[64], command[300], input_path[MAXPATHLEN];
    int i, retval;
    
    for (i = 0; i < argc; ++i) {
        if (strcmp(argv[i], "--url") == 0) {
            if (i + 1 >= argc) {
                fprintf(stderr, "need argument to nworkers\n");
                return 1;
            }
            strcpy(url, argv[i + 1]);
        }
        i++;
    }

    // Discover physical input path
    retval = boinc_resolve_filename(SPARK_TGZ, input_path, MAXPATHLEN);
    if (retval) {
        fprintf(stderr, "resolve input filename returned %d\n", retval);
        return retval;
    }

    // Inflate local spark directory
    sprintf(command, "tar zxvf %s", input_path);
    retval = system(command);
    if (retval) {
        fprintf(stderr, "%s command returned error: %d\n", command, retval);
        return retval;
    }

    retval = start_worker(url); 
    if (retval) {
        fprintf(stderr, "failed to start master node: %d\n", retval);
        return retval;
    }

    // Checkpoint with BOINC server 
    sprintf(buf, "<jid>%d</jid>", jid);
    retval = boinc_send_trickle_up(
        const_cast<char*>("worker_up"),
        buf
    );
    if (retval) {
        fprintf(stderr, "%s boinc_send_trickle_up returned %d\n",
            boinc_msg_prefix(buf, sizeof(buf)), retval
        );
        return retval;
    } 

    // TODO figure out how to get actual trickle message, then 
    // actually check message and confirm contents
    retval = retrieve_trickle_reply(reply);
    if (retval) {
        fprintf(stderr, "not able to retrieve trickle reply\n");
        return retval;
    } 

    stop_worker();

    return 0;
}

int main(int argc, char** argv) {
    int i, retval, jid = -1;
    bool is_master = false;
    char buf[256];

    // Allocate BOINC
    retval = boinc_init();
    if (retval) {
        fprintf(stderr, "%s boinc_init returned %d\n",
            boinc_msg_prefix(buf, sizeof(buf)), retval
        );
        return(retval);
    }

    fprintf(stderr, "%s app started.\n",
        boinc_msg_prefix(buf, sizeof(buf))
    );

    // Decide which type of node this will be 
    for (i = 0; i < argc; ++i) {
        if (strcmp(argv[i], "-master") == 0) {
            is_master = true;
        } else if (strcmp(argv[i], "-worker") == 0) {
            is_master = false;
        } else if (strcmp(argv[i], "--jid") == 0) {
            if (i + 1 >= argc) {
                fprintf(stderr, "argument to jid needed!\n");
                exit(1);
            }
            jid = atoi(argv[i + 1]);
        }
    }

    if (jid < 0) {
        fprintf(stderr, "invalid or non-existent jid given to node\n");
        exit(1);
    }

    if (is_master) {
        retval = master_main(argc, argv, jid);
    } else {
        retval = worker_main(argc, argv, jid);
    }

    if (retval) {
        fprintf(stderr, "error executing main function\n");
        exit(retval);
    }

    // Tear down BOINC
    boinc_finish(0);
}
