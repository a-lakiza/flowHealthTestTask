const cassandra = require('cassandra-driver');
const csv = require("csvtojson");
const async = require('async');
const headers = require('./headers.json');
const argv = require('yargs').argv
const executeConcurrent = cassandra.concurrent.executeConcurrent;

const csvFilePath = argv.file
const client = new cassandra.Client({ contactPoints: ['127.0.0.1'], localDataCenter: 'datacenter1' });
const tableHeaders = headers.headers.map(header => {
    const newHeader = header + ' text';
    return newHeader
})

csv({
    headers: headers.headers
})
    .fromFile(csvFilePath)
    .then((jsonObj) => {
        const queries = []
        for (i in jsonObj) {
            if (jsonObj[i].Practice_Name !== '') {
                queries.push(
                    [...Object.values(jsonObj[i])]
                )
            }
        }

        async.series([

            function connect(next) {
                client.connect(next);
            },

            function createKeyspace(next) {
                const query = "CREATE KEYSPACE IF NOT EXISTS flowHealthTestTask WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3' }";
                client.execute(query, next);
            },

            function dropTable(next) {
                const query = "DROP TABLE IF EXISTS flowHealthTestTask.practicesGrid";
                client.execute(query, next);
            },

            function createTable(next) {
                const query = "CREATE TABLE IF NOT EXISTS flowHealthTestTask.practicesGrid (" + [...tableHeaders] + ", PRIMARY KEY(practice_name))";
                client.execute(query, next);
            },

            async function insert(next) {
                const query = 'INSERT INTO flowHealthTestTask.practicesGrid (' + [...headers.headers] + ') VALUES (?, ?, ?, ?, ?,?, ?, ?, ?, ?,?, ?, ?, ?, ?,?, ?, ?, ?, ?,?, ?, ?, ?, ?,?, ?, ?, ?, ?,?, ?, ?, ?, ?)';
                try {
                    await executeConcurrent(client, query, queries);
                }
                finally {
                    console.log(`Finished executing ${queries.length} queries.`);
                }
            },
        ],
            function (err) {
                if (err) {
                    console.error('There was an error', err.message, err.stack);
                }
                console.log('Shutting down');
                client.shutdown(() => {
                    if (err) {
                        throw err;
                    }
                });
            });
    }).catch(reason => {
        console.log(reason);
    })

