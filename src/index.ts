import Redis from 'ioredis'
import fs from 'fs'

export interface ExportData {
    [key: string]: any 
}

export interface ImportData {
    [key: string]: any 
}

export class RedisIO {
    sourceRedis: any
    destRedis: any
    constructor(sourceConfig?: any, destConfig?: any) {
        this.sourceRedis = new Redis(sourceConfig)
        this.destRedis  = new Redis(destConfig)
    }

    async listCollections() {
        try {
            const keys = await this.sourceRedis.keys('*'); // Retrieve all keys
            console.log(`Found ${keys.length} collections:`, keys);
            return keys; // Return the list of keys
        } catch (error) {
            console.error('Error listing collections:', error);
            return []; // Return an empty array on error
        }
    }

    async exportAndImportCollection(collectionName: String) {
        try {
            const exportData = await this.exportData(collectionName)
            if (!exportData) {
                throw new Error('Export failed - no data returned')
            }
            console.log(`Exported ${Object.keys(exportData).length} keys`)
            await this.importData(exportData)
            console.log(`Imported ${Object.keys(exportData).length} keys`)
        } catch (error) {
            console.error('Error during export/import:', error)
        } finally {
            await this.closeConnections()
        }
    }

    async exportData(collectionName: String) {
        const keys = await this.sourceRedis.keys(collectionName)
        const exportData: ExportData = {}
        try {
            for (const key of keys) {
                const type = await this.sourceRedis.type(key)
                switch (type) {
                    case 'string':
                        exportData[key] = await this.sourceRedis.get(key)
                        break
                    case 'list':
                        exportData[key] = await this.sourceRedis.lrange(key, 0, -1)
                        break
                    case 'hash':
                        exportData[key] = await this.sourceRedis.hgetall(key)
                        break
                    case 'set':
                        exportData[key] = await this.sourceRedis.smembers(key)
                        break
                    case 'zset':
                        exportData[key] = await this.sourceRedis.zrange(key, 0, -1, 'WITHSCORES')
                        break
                    default:
                        console.warn(`Skipping key ${key} of type ${type}`)
                }
            }
            await this.saveToFile('dump.rdb', exportData)
            return exportData
        } catch (error) {
            return  console.error('Error during Exporting collection data:', error)
        }finally {
            await this.closeConnections()
        }
    }

      // New method to save data to a file
      async saveToFile(filename: string, data: ExportData) {
        await fs.writeFile(filename, JSON.stringify(data))
    }
    async importData(importData: ImportData, expiryDays?: number) {
        try {
            for (const [key, value] of Object.entries(importData)) {
                const type = await this.sourceRedis.type(key)
                switch (type) {
                    case 'string':
                        await this.destRedis.set(key, value)
                        break
                    case 'list':
                        await this.destRedis.rpush(key, ...(value as string[]))
                        break
                    case 'hash':
                        await this.destRedis.hmset(key, value)
                        break
                    case 'set':
                        await this.destRedis.sadd(key, ...(value as string[]))
                        break
                    case 'zset':
                        await this.destRedis.zadd(key, ...(value as string[]))
                        break  
                    default:
                        await this.destRedis.set(key, JSON.stringify(value))
                }
                     // Set expiry if provided
                     if (expiryDays) {
                        await this.destRedis.expire(key, expiryDays * 24 * 60 * 60); // Convert days to seconds
                    }
            }
            
        } catch (error) {
            return  console.error('Error during Importing Data:', error)
        }finally {
            await this.closeConnections()
        }
      
    }

    async closeConnections() {
        await this.sourceRedis.quit()
        await this.destRedis.quit()
    }
}

//Example on how to use

// const collectionName = 'sample_jobQueue:*' // Get collection name from request body
// const sourceConfig = { host: 'localhost', port: 6379 }
// // // const destConfig = { host: 'localhost', port: 6379 }

// const redisExporter = new RedisIO(sourceConfig)
// redisExporter.listCollections()
// // // redisExporter.exportAndImportCollection(collectionName)
// // // const importData = JSON.parse(fs.readFileSync('redis_data.json', 'utf-8')); // Read data from JSON file // Get collection name from request body
// let exportData : any


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
// mainExport('sample_jobQueue:*')
// mainImport(importData)