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
    async importData(importData, expiryDays) {
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
                // Set expiry if provided
                if (expiryDays) {
                    await this.destRedis.expire(key, expiryDays * 24 * 60 * 60); // Convert days to seconds
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
// // // const destConfig = { host: 'localhost', port: 6379 }
// const redisExporter = new RedisIO(sourceConfig)
// // // redisExporter.listCollections()
// // // redisExporter.exportAndImportCollection(collectionName)
// // // const importData = JSON.parse(fs.readFileSync('redis_data.json', 'utf-8')); // Read data from JSON file // Get collection name from request body
// let exportData : any
// // let importData =  {
// //     'sample_session:901234567': {
// //       user_id: '901',
// //       username: 'olivia_lee',
// //       email: 'olivia@example.com',
// //       last_activity: '2023-01-09 18:30:00'
// //     },
// //     'sample_session:678901234': {
// //       user_id: '678',
// //       username: 'chris_black',
// //       email: 'chris@example.com',
// //       last_activity: '2023-01-06 14:45:00'
// //     },
// //     'sample_session:334455667': {
// //       user_id: '334',
// //       username: 'ethan_white',
// //       email: 'ethan@example.com',
// //       last_activity: '2023-01-12 22:15:00'
// //     },
// //     'sample_session:778899001': {
// //       user_id: '778',
// //       username: 'logan_anderson',
// //       email: 'logan@example.com',
// //       last_activity: '2023-01-14 00:45:00'
// //     },
// //     'sample_session:990011223': {
// //       user_id: '990',
// //       username: 'mia_thompson',
// //       email: 'mia@example.com',
// //       last_activity: '2023-01-15 02:00:00'
// //     },
// //     'sample_session:456789012': {
// //       user_id: '456',
// //       username: 'bob_jones',
// //       email: 'bob@example.com',
// //       last_activity: '2023-01-04 12:15:00'
// //     },
// //     'sample_session:112233445': {
// //       user_id: '112',
// //       username: 'mia_evans',
// //       email: 'mia@example.com',
// //       last_activity: '2023-01-11 21:00:00'
// //     },
// //     'sample_session:789012345': {
// //       user_id: '789',
// //       username: 'sophia_taylor',
// //       email: 'sophia@example.com',
// //       last_activity: '2023-01-07 16:00:00'
// //     },
// //     'sample_session:012345678': {
// //       user_id: '012',
// //       username: 'noah_hall',
// //       email: 'noah@example.com',
// //       last_activity: '2023-01-10 19:45:00'
// //     },
// //     'sample_session:890123456': {
// //       user_id: '890',
// //       username: 'david_wilson',
// //       email: 'david@example.com',
// //       last_activity: '2023-01-08 17:15:00'
// //     },
// //     'sample_session:556677889': {
// //       user_id: '556',
// //       username: 'ava_martin',
// //       email: 'ava@example.com',
// //       last_activity: '2023-01-13 23:30:00'
// //     },
// //     'sample_session:567890123': {
// //       user_id: '567',
// //       username: 'emily_brown',
// //       email: 'emily@example.com',
// //       last_activity: '2023-01-05 13:30:00'
// //     }
// //   }
// async function mainExport (collectionName: any) {
//     try {
//         exportData = await redisExporter.exportData(collectionName)
//         console.log('success:', exportData)
//     } catch (error) {
//         console.log('error')
//     }
// }
// // async function mainImport (importData: any) {
// //     try {
// //         const data = await redisExporter.importData(importData)
// //         console.log('success:', data)
// //     } catch (error) {
// //         console.log('error')
// //     }
// // }
// //mainExport('sample_jobQueue:*')
// mainImport(importData)
