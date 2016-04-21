var express    = require('express')
  , mysql      = require('mysql')
  , bodyParser = require('body-parser')
  , fs         = require('fs')
  , shortid    = require('shortid');
var connection = mysql.createConnection({
    host     : 'localhost',
    user     : 'root',
    password : 'boincadm',
    database : 'test_ec2'
});
var app = express();

connection.connect(function(err){
    if(!err) {
        console.log("Database is connected...");
    } else {
        console.log("Failed to connect database...");
    }
});

app.set('view engine', 'ejs');
app.set('views', './views');
app.use('/public', express.static(__dirname + '/public'));
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({
    extended: true
}));

function array_diff(arr1, arr2) {
    var output = [];

    for(var i = 0; i < arr1.length; ++i) {
        var contained = false;
        for(var j = 0; j < arr2.length; ++j) {
            if(arr2[j].hid == arr1[i].hid) {
                contained = true;
                break;
            }
        }

        if(!contained) {
            output.push(arr1[i]);
        }
    }

    return output;
}

app.get('/', function(req, res) {
    connection.query('SELECT * from spark_job', function(err, rows, fields) {
        //connection.end();

        var jids = [];
        if(!err) {
            for(var i = 0; i < rows.length; i++) {
                jids.push(rows[i].ID);
            } 
            res.render('index', {jids: jids});
        } else {
            console.log('Unable to perform query...');
        }
    });
});

app.post('/new_job', function(req, res){
    var data = String(req.body.file_data);// the file data as a string, will have to create a file from this here
    var file_location = '/home/boincadm/projects/test_ec2/spark/';
    var file_name = 'spark_job_' + shortid.generate() + '.py';
    var full_path = file_location + file_name;
    if (data.substring(data.length - 7, data.length) === ',Submit') {
        data = data.substring(0, data.length - 7);
    }
    fs.writeFile(full_path, data, function (err) {
                    if (err) throw err;
            });
    var workers = req.body.workers; // the number of workers
    if(!workers || workers.length < 1){
        workers = 1;
    }
   
   
   var insert = {file:file_name, n_masters: 1, n_nodes: workers, handled: 0, n_nodes_p: workers - 1, running: 0, execute_sent: 0};
   connection.query('INSERT INTO spark_job SET ?',insert, function(err, result){
       if (err) throw err;
      //  connection.end();
       console.log(result);
       res.redirect('/job_view?job_selector=' + result.insertId);
    });
    //res.render('job_view', {});

})

app.get('/job_view', function(req, res) {
    connection.query('SELECT * from host LIMIT 10', function(err, rows, fields) {
        var hosts = [];
        if(!err) {
            for(var i = 0; i < rows.length; i++) {
                hosts.push({hid: rows[i].id, ip: rows[i].external_ip_addr});
            }
        } else {
            console.log('Unable to perform host query...');
            process.exit(1);
        }

        connection.query('SELECT * from spark_node WHERE jid = ?'
                        , [req.query.job_selector]
                        , function(err, rows, fields) {
            var masters = [];
            var workers = [];
            if(!err) {
                connection.query('SELECT * from spark_job WHERE ID = ?'
                                , [req.query.job_selector]
                                , function(err, rows2, fields) {
                    var mcolor = '#ffff66';
                    var wcolor = '#ffff66';
                    if (!err) {
                        if (rows2.length > 0) {
                            if (rows2[0].running == 1) {
                                mcolor = '#55ee55';
                                wcolor = '#55ee55';
                            }         
                        }
                    }

                   for(var i = 0; i < rows.length; i++) {
                       if (rows[i].master == '1') {
                           masters.push({color: mcolor, hid: rows[i].hid, ip: rows[i].ip});
                       } else {
                           if (rows[i].error == 1) {
                               wcolor = '#ff4d4d';
                           }
                           workers.push({color: wcolor, hid: rows[i].hid, ip: rows[i].ip}); 
                       } 
                   } 
                   hosts = array_diff(hosts, masters);
                   hosts = array_diff(hosts, workers);

                   res.render('job_view', { hosts: hosts, masters: masters, workers: workers, job_selector:req.query.job_selector});
                });
            } else {
                console.log('Unable to perform spark_node query...');
                process.exit(1);
            }
        });
    });
});

app.listen(8081, function () {
    console.log("example app listening on port 8081");
});
