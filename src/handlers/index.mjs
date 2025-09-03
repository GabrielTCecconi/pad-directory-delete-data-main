import { SSMClient, GetParameterCommand } from '@aws-sdk/client-ssm';
import pg from 'pg';
const { Pool } = pg;

const region = process.env.AWS_REGION;
const ssmClient = new SSMClient({ region: region});

export const handler = async(event) => {
    const rdsHost = await getParameterStoreValue(ssmClient, 'rdsPadHost')
    const rdsUser = await getParameterStoreValue(ssmClient, 'rdsPadUser')
    const rdsPwd = await getParameterStoreValue(ssmClient, 'rdsPadPwd')
    const rdsDb = await getParameterStoreValue(ssmClient, 'rdsPadDatabase')
    const config = {
        host: rdsHost,
        user: rdsUser,     
        password: rdsPwd,
        database: rdsDb,
        port: 5432,
        max: 1,
        min: 0,
        idleTimeoutMillis: 120000,
        connectionTimeoutMillis: 10000
    }
    const pool = new Pool(config)
    const client = await pool.connect()

    const schema = 'directory'
    try {
        const resultData = await client.query(`
            select
                table_name
            from
                information_schema.tables
            where
                table_schema = $1
                and table_type = 'BASE TABLE'
        `, [schema])

        for (const row of resultData.rows) {
            const tableName = schema + '.' + row['table_name']
            await deleteFromTable(client, tableName);
        }
    }
    catch (error) {
        console.error('Error function:', error);
    }
    finally {
        client.release(true);
        pool.end();
    }
}

async function deleteFromTable(client, tableName) {
    try {
        const flagNameException = 'Índice de disponibilidade do PIX'
        const query = (tableName == 'directory.flags') ? `
            delete
            from
                ${tableName}
            where
                CAST(ext_updated_at AS DATE) < 
                (
                    select 
                        CAST(max(ext_updated_at) AS DATE) 
                    from ${tableName}
                )
                and name != '${flagNameException}'
        ` : `
            delete
            from 
                ${tableName}
            where
                CAST(ext_updated_at AS DATE) < 
                (
                    select 
                        CAST(max(ext_updated_at) AS DATE) 
                    from ${tableName}
                )
        `
        const result = await client.query(query);
        console.log(`Table "${tableName}" deleted, rows count ${result.rowCount}.`);
    } catch (error) {
        console.error(`Error deleting records "${tableName}":`, error);
    }
}

async function getParameterStoreValue(ssmClient, parameterName) {
    const getParameterCommand = new GetParameterCommand({
        Name: parameterName,
        WithDecryption: true
    });
    const response = await ssmClient.send(getParameterCommand);
    return response.Parameter.Value;
}

// CI Teste