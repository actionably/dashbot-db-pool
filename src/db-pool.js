'use strict'

const q = require('q')
const mysql = require('mysql')
const md5 = require('js-md5')
const NodeCache = require('node-cache')

class ConnectionWrapper {
  constructor(rawConnection) {
    this.rawConnection = rawConnection
    this.queryWithPromise = q.nbind(this.rawConnection.query, this.rawConnection)
  }

  async query(sql, args) {
    if (process.env.MYSQL_PRINT_SQL === 'true') {
      console.log(mysql.format(sql, args))
    }
    try {
      const results = await this.queryWithPromise(sql, args)
      return results[0]
    } catch (err) {
      err.failedSql = mysql.format(sql, args)
      console.error(err, JSON.stringify(err))
      throw err
    }
  }

  async queryOne(sql, args) {
    const items = await this.query(sql, args)
    if (items.length) {
      return items[0]
    } else {
      return null
    }
  }

  async queryOneNum(sql, args) {
    const obj = await this.queryOne(sql, args)
    if (obj && obj.num) {
      return obj.num
    } else {
      return null
    }
  }

  async insert(sql, args) {
    const results = await this.query(sql, args)
    if (results.insertId) {
      return results.insertId
    } else {
      return null
    }
  }

  release() {
    this.rawConnection.release()
  }
}

/*
 * wrapper around the pool class that simplifies the response and adds a few handy helpers.
 */
class PoolWrapper {
  constructor() {
    this.cache = new NodeCache({stdTTL: 30, checkperiod: 120})
  }

  connect() {
    if (this.rawPool) {
      return
    }
    let defaultDbUrl = process.env.MYSQL_URL
    if (!defaultDbUrl) {
      throw new Error(`Missing environment variable 'MYSQL_URL'`)
    }

    if (defaultDbUrl.indexOf('multipleStatements=true') === -1) {
      if (defaultDbUrl.indexOf('?') === -1) {
        defaultDbUrl += '?'
      } else {
        defaultDbUrl += '&'
      }
      defaultDbUrl += 'multipleStatements=true'
    }

    if (defaultDbUrl.indexOf('charset=utf8mb4') === -1) {
      defaultDbUrl += '&charset=utf8mb4'
    }

    this.rawPool = mysql.createPool(defaultDbUrl)
    this.queryWithPromise = q.nbind(this.rawPool.query, this.rawPool)
    this.endWithPromise = q.nbind(this.rawPool.end, this.rawPool)
    this.getConnectionWithPromise = q.nbind(this.rawPool.getConnection, this.rawPool)
    console.log('connecting to mysql ' + defaultDbUrl)
  }

  async end() {
    if (!this.rawPool) {
      return
    }
    await this.endWithPromise()
    this.rawPool = null
  }

  async getConnection() {
    this.connect()
    const rawConnection = await this.getConnectionWithPromise()
    return new ConnectionWrapper(rawConnection)
  }

  format(sql, args) {
    return mysql.format(sql, args)
  }

  async query(sql, args, opts) {
    if (opts && opts.useCache) {
      return this.queryCache(sql, args, opts.ttl)
    }
    this.connect()
    if (process.env.MYSQL_PRINT_SQL === 'true') {
      console.log(mysql.format(sql, args))
    }
    try {
      const results = await this.queryWithPromise(sql, args)
      return results[0]
    } catch (err) {
      err.failedSql = mysql.format(sql, args)
      console.error(err, JSON.stringify(err))
      throw err
    }
  }

  async queryCache(sql, args, ttl) {
    const formattedSql = mysql.format(sql, args)
    const hash = md5(formattedSql)
    let data = this.cache.get(hash)
    if (data) {
      if (process.env.MYSQL_PRINT_SQL === 'true') {
        console.log('CACHE HIT: ' + formattedSql)
      }
      return data
    }
    data = await this.query(sql, args)
    this.cache.set(hash, data, ttl)
    return data
  }

  async insert(sql, args) {
    const results = await this.query(sql, args)
    if (results.insertId) {
      return results.insertId
    } else {
      return null
    }
  }

  async queryOne(sql, args, opts) {
    const items = await this.query(sql, args, opts)
    if (items.length) {
      return items[0]
    } else {
      return null
    }
  }

  async queryOneNum(sql, args, opts) {
    const obj = await this.queryOne(sql, args, opts)
    if (obj && obj.num) {
      return obj.num
    } else {
      return null
    }
  }
}

module.exports = new PoolWrapper()
