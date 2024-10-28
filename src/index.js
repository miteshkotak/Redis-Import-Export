const Redis = require('ioredis')
const dotenv = require('dotenv')

dotenv.config()

async function exportAndImportRedisCollection(collectionName) {
    // Connect to source Redis
    const sourceRedis = new Redis({
        host: '127.0.0.1',
        port: 6380,
    });

    // Connect to destination Redis
    const destRedis = new Redis({
        host: '127.0.0.1',
        port: 6379,
    });

    try {
        // Export data from source Redis
        const keys = await sourceRedis.keys(collectionName)
        const exportData = {}

        for (const key of keys) {
            const type = await sourceRedis.type(key); // Check the type of the key
            switch (type) {
                case 'string':
                    exportData[key] = await sourceRedis.get(key);
                    break;
                case 'list':
                    exportData[key] = await sourceRedis.lrange(key, 0, -1); // Get all elements in the list
                    break;
                case 'hash':
                    exportData[key] = await sourceRedis.hgetall(key); // Get all fields and values in the hash
                    break;
                case 'set':
                    exportData[key] = await sourceRedis.smembers(key); // Get all members of the set
                    break;
                case 'zset':
                    exportData[key] = await sourceRedis.zrange(key, 0, -1, 'WITHSCORES'); // Get all members with scores
                    break;
                default:
                    console.warn(`Skipping key ${key} of type ${type}`);
            }
        }

        console.log(`Exported ${Object.keys(exportData).length} keys`)

        // Import data to destination Redis
        for (const [key, value] of Object.entries(exportData)) {
            const type = await sourceRedis.type(key); // Check the type again for import
            switch (type) {
                case 'string':
                    await destRedis.set(key, value);
                    break;
                case 'list':
                    await destRedis.rpush(key, ...value); // Push all elements to the list
                    break;
                case 'hash':
                    await destRedis.hmset(key, value); // Set all fields and values in the hash
                    break;
                case 'set':
                    await destRedis.sadd(key, ...value); // Add all members to the set
                    break;
                case 'zset':
                    await destRedis.zadd(key, ...value); // Add all members with scores to the sorted set
                    break;
                default:
                    console.warn(`Skipping import for key ${key} of type ${type}`);
            }
        }

        console.log(`Imported ${Object.keys(exportData).length} keys`)
    } catch (error) {
        console.error('Error during export/import:', error)
    } finally {
        // Close Redis connections
        await sourceRedis.quit()
        await destRedis.quit()
    }
}

const collectionName = process.argv[2] // Get the collection name from command line arguments

exportAndImportRedisCollection(collectionName)