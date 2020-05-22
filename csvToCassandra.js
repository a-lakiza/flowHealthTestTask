const cassandra = require('cassandra-driver');
const elasticsearch = require('elasticsearch');
const csv = require("csvtojson");
const async = require('async');
const argv = require('yargs').argv
const headers = require('./headers.json');
const executeConcurrent = cassandra.concurrent.executeConcurrent;

const csvFilePath = argv.file
const client = new cassandra.Client({ contactPoints: ['127.0.0.1'], localDataCenter: 'datacenter1' });
const elasticClient = new elasticsearch.Client({
    host: 'localhost:9200',
    log: 'trace',
    apiVersion: '7.5', // use the same version of your Elasticsearch instance
});
const tableHeaders = headers.headers.map(header => {
    const newHeader = header + ' text';
    return newHeader
})
elasticClient.ping({
    requestTimeout: 1000
}, function (error) {
    if (error) {
        console.trace('elasticsearch cluster is down!');
    } else {
        console.log('All is well');
    }
});
const tableName = csvFilePath.split('/').pop().split('.')[0].toLowerCase().replace(/\s|-/g, '_');;
let elasticData = []
csv({
    headers: headers.headers
})
    .fromFile(csvFilePath)
    .then((jsonObj) => {
        const queries = jsonObj.filter(item => item.Practice_Name !== '').map(item => {
            const queryItem = [...Object.values(item)]
            return queryItem
        })
        elasticData = [...jsonObj]
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
                    elastic();
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

async function elastic() {
    let { body } = await elasticClient.exists({
        index: tableName,
        id: "1"
    })
    if (!body) {
        await elasticClient.index({
            index: tableName,
            id: '1',
            body: {
                mappings: {
                    properties: {
                        Practice_Name: { type: 'text' },
                        Status: { type: 'text' },
                        Platforms: { type: 'text' },
                        Practice_Address: { type: 'text' },
                        Line_2: { type: 'text' },
                        City: { type: 'text' },
                        State: { type: 'text' },
                        Zip: { type: 'text' },
                        Supply_Shipping_Address: { type: 'text' },
                        Type_of_Testing: { type: 'text' },
                        Provider: { type: 'text' },
                        Practice_Phone: { type: 'text' },
                        Practice_Fax: { type: 'text' },
                        Distributor: { type: 'text' },
                        Marketing_Group: { type: 'text' },
                        Rep: { type: 'text' },
                        Rep_Email: { type: 'text' },
                        Rep_Phone: { type: 'text' },
                        Practice_Manager: { type: 'text' },
                        PM_Email: { type: 'text' },
                        Practice_Manager_phone: { type: 'text' },
                        Dropbox_Link_to_Onboard_Paperwork: { type: 'text' },
                        Supply_Orders: { type: 'text' },
                        Result_Portal: { type: 'text' },
                        User_Name: { type: 'text' },
                        Password: { type: 'text' },
                        FHM_Portal: { type: 'text' },
                        Customized_Reqs: { type: 'text' },
                        Hospitals: { type: 'text' },
                        Critical_Results_Contact: { type: 'text' },
                        Critical_Results_Contact_Number: { type: 'text' },
                        Practice_Name_Without_Location: { type: 'text' },
                        EMR: { type: 'text' },
                        Practices_Ignite: { type: 'text' },
                        M_O_M_of_On_Boarding: { type: 'text' },
                    }
                }
            }
        }, { ignore: [400] })
    }

    body = elasticData.flatMap(doc => [{ index: { _index: tableName } }, doc])
    const { count } = await elasticClient.count({ index: tableName })
    console.log(count)
}