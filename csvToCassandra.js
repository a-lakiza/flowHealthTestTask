const cassandra = require('cassandra-driver');
const csv = require("csvtojson");
const async = require('async');
const argv = require('yargs').argv
const headers = require('./headers.json');
const executeConcurrent = cassandra.concurrent.executeConcurrent;

const csvFilePath = argv.file
const client = new cassandra.Client({ contactPoints: ['127.0.0.1'], localDataCenter: 'datacenter1' });
const tableHeaders = headers.headers.map(header => {
    const newHeader = header + ' text';
    return newHeader
})
const tableName = csvFilePath.split('/').pop().split('.')[0].toLowerCase().replace(/\s|-/g, '_');;
console.log(tableName);

csv({
    headers: headers.headers
    })
    .fromFile(csvFilePath)
    .then((jsonObj) => {
        const queries = jsonObj.filter(item => item.Practice_Name !== '').map(item => {
            const queryItem = [...Object.values(item)]
            return queryItem
        })

        async.series([

            function connect(next) {
                client.connect(next);
            },

            function createKeyspace(next) {
                const query = "CREATE KEYSPACE IF NOT EXISTS flowHealthTestTask WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3' }";
                client.execute(query, next);
            },

            function dropTable(next) {
                const query = `DROP TABLE IF EXISTS flowHealthTestTask.${tableName}`;
                client.execute(query, next);
            },

            function createTable(next) {
                const query = `CREATE TABLE IF NOT EXISTS flowHealthTestTask.${tableName} ( ${[...tableHeaders]} , PRIMARY KEY(practice_name))`;
                client.execute(query, next);
            },

            async function insert(next) {
                const query = `INSERT INTO flowHealthTestTask.${tableName} ( ${[...headers.headers]}) VALUES (?, ?, ?, ?, ?,?, ?, ?, ?, ?,?, ?, ?, ?, ?,?, ?, ?, ?, ?,?, ?, ?, ?, ?,?, ?, ?, ?, ?,?, ?, ?, ?, ?)`;
                try {
                    await executeConcurrent(client, query, queries);
                    console.log(`Finished executing ${queries.length} queries.`);
                }
                catch (err) {
                    console.error('There was an error', err.message, err.stack);
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

