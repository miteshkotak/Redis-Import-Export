import Redis from 'ioredis'

export interface ExportData {
    [key: string]: any 
}


export class RedisExporter {
    sourceRedis: any
    destRedis: any
    constructor(sourceConfig: any, destConfig: any) {
        this.sourceRedis = new Redis(sourceConfig)
        this.destRedis  = new Redis(destConfig)
    }

    async exportAndImportCollection(collectionName: String) {
        try {
            const exportData = await this.exportData(collectionName)
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
        return exportData;
    }

    async importData(exportData: ExportData) {
        for (const [key, value] of Object.entries(exportData)) {
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
                    console.warn(`Skipping import for key ${key} of type ${type}`)
            }
        }
    }

    async closeConnections() {
        await this.sourceRedis.quit()
        await this.destRedis.quit()
    }
}
