#include <vector>
#include <string>
#include <fstream>

#include "assimilate_handler.h"
#include "validate_util.h"
#include "spark_mysql.h"

using std::vector;
using std::string;
using std::ifstream;

SPARK_MYSQL* db;

int assimilate_handler_init(int argc, char** argv) {
    int retval = 0;

    db = new SPARK_MYSQL(retval);
    check_error(retval, "instantiate db");

    return retval;
}

void assimilate_handler_usage() {
    // placeholder
}

int assimilate_handler(
    WORKUNIT& wu, vector<RESULT>& /*results*/, RESULT& canonical_result
) {
    char query[MAX_QUERY_LENGTH];
    string outpath, result, line;
    int retval;

    if (wu.canonical_resultid) {
        retval = get_output_file_path(canonical_result, outpath);
        if (retval) {
            fprintf(stderr, "unable to retrieve output file path\n");
            return 1; 
        }

        fprintf(stderr, "outpath is: %s\n", outpath.c_str());

        ifstream outfile (outpath.c_str());
        if (!outfile.is_open()) {
            fprintf(stderr, "unable to open output file for reading\n");
            return 1;  
        }

        while (getline (outfile, line)) {
            result.append(line);
            result.append("\n");
        }

        sprintf(query,
            "UPDATE spark_job SET result=\"%s\" WHERE file=\"%s\"",
            result.c_str(),
            wu.name
        );
        db->query(query, retval);
        check_error(retval, query);

        return 0;
    }

    return 0;
}
