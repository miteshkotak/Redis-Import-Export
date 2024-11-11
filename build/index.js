"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.RedisIO = void 0;
const ioredis_1 = __importDefault(require("ioredis"));
class RedisIO {
    constructor(sourceConfig, destConfig) {
        this.sourceRedis = new ioredis_1.default(sourceConfig);
        this.destRedis = new ioredis_1.default(destConfig);
    }
    async listCollections() {
        try {
            const keys = await this.sourceRedis.keys('*'); // Retrieve all keys
            console.log(`Found ${keys.length} collections:`, keys);
            return keys; // Return the list of keys
        }
        catch (error) {
            console.error('Error listing collections:', error);
            return []; // Return an empty array on error
        }
    }
    async exportAndImportCollection(collectionName) {
        try {
            const exportData = await this.exportData(collectionName);
            if (!exportData) {
                throw new Error('Export failed - no data returned');
            }
            console.log(`Exported ${Object.keys(exportData).length} keys`);
            await this.importData(exportData);
            console.log(`Imported ${Object.keys(exportData).length} keys`);
        }
        catch (error) {
            console.error('Error during export/import:', error);
        }
        finally {
            await this.closeConnections();
        }
    }
    async exportData(collectionName) {
        const keys = await this.sourceRedis.keys(collectionName);
        const exportData = {};
        try {
            for (const key of keys) {
                const type = await this.sourceRedis.type(key);
                switch (type) {
                    case 'string':
                        exportData[key] = await this.sourceRedis.get(key);
                        break;
                    case 'list':
                        exportData[key] = await this.sourceRedis.lrange(key, 0, -1);
                        break;
                    case 'hash':
                        exportData[key] = await this.sourceRedis.hgetall(key);
                        break;
                    case 'set':
                        exportData[key] = await this.sourceRedis.smembers(key);
                        break;
                    case 'zset':
                        exportData[key] = await this.sourceRedis.zrange(key, 0, -1, 'WITHSCORES');
                        break;
                    default:
                        console.warn(`Skipping key ${key} of type ${type}`);
                }
            }
            return exportData;
        }
        catch (error) {
            return console.error('Error during Exporting collection data:', error);
        }
        finally {
            await this.closeConnections();
        }
    }
    async importData(importData) {
        try {
            for (const [key, value] of Object.entries(importData)) {
                const type = await this.sourceRedis.type(key);
                switch (type) {
                    case 'string':
                        await this.destRedis.set(key, value);
                        break;
                    case 'list':
                        await this.destRedis.rpush(key, ...value);
                        break;
                    case 'hash':
                        await this.destRedis.hmset(key, value);
                        break;
                    case 'set':
                        await this.destRedis.sadd(key, ...value);
                        break;
                    case 'zset':
                        await this.destRedis.zadd(key, ...value);
                        break;
                    default:
                        await this.destRedis.set(key, JSON.stringify(value));
                }
            }
        }
        catch (error) {
            return console.error('Error during Importing Data:', error);
        }
        finally {
            await this.closeConnections();
        }
    }
    async closeConnections() {
        await this.sourceRedis.quit();
        await this.destRedis.quit();
    }
}
exports.RedisIO = RedisIO;
//Example on how to use
// const collectionName = 'sample_jobQueue:*' // Get collection name from request body
// const sourceConfig = { host: 'localhost', port: 6380 }
// // const destConfig = { host: 'localhost', port: 6379 }
// const redisExporter = new RedisIO(sourceConfig)
// // redisExporter.listCollections()
// // redisExporter.exportAndImportCollection(collectionName)
// // const importData = JSON.parse(fs.readFileSync('redis_data.json', 'utf-8')); // Read data from JSON file // Get collection name from request body
// async function mainv1 (collectionName: any) {
//     try {
//         const data = await redisExporter.exportData(collectionName)
//         console.log('success:', data)
//     } catch (error) {
//         console.log('error')
//     }
// }
// mainv1('sample_session:*')
