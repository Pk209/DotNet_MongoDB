using MongoDB.Driver;
using MongoDB.Bson;
using System;
using System.Collections.Generic;
using MongoDB.Bson.Serialization;
using System.Linq;
using System.Threading.Tasks;
using System.Threading;

namespace MongoDBTest
{

    public class MongoDBService : IDisposable
    {
        //protected readonly ILog _log = LogManager.GetLogger("");

        protected static IMongoClient _client;
        protected static IMongoDatabase _database;
        protected const int MAX_LIMIT = 1000;


        #region Constructors
        public MongoDBService(string connectionString, string dbName)
        {
            try
            {
                _client = new MongoClient(connectionString);
                _database = _client.GetDatabase(dbName);
            }
            catch (Exception ex)
            {
                //_log.Error(ex);
            }
        }
        #endregion

        #region Select
        /// <summary>
        /// Returns a list of the given BsonDocument filter
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="collectionName"></param>
        /// <param name="filter"></param>
        /// <param name="limit"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task<List<T>> SelectAsync<T>(string collectionName, FilterDefinition<BsonDocument> filter, int limit = MAX_LIMIT, CancellationToken cancellationToken = default)
        {
            var returnList = new List<T>();
            try
            {
                var limit_list = limit > MAX_LIMIT ? MAX_LIMIT : limit;
                var collection = _database.GetCollection<BsonDocument>(collectionName);

                var result = await collection.Find(filter).Limit(limit).ToListAsync(cancellationToken);

                foreach (var item in result)
                {
                    returnList.Add(BsonSerializer.Deserialize<T>(item));
                }
            }
            catch (Exception ex)
            {
                //_log.Error(ex);
            }
            return returnList;
        }

        /// <summary>
        /// Returns a list of the given key-value string
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="collectionName"></param>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <param name="limit"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task<List<T>> SelectAsync<T>(string collectionName, string key, string value, int limit = MAX_LIMIT, CancellationToken cancellationToken = default)
        {
            var returnList = new List<T>();
            try
            {
                limit = limit > MAX_LIMIT ? MAX_LIMIT : limit;
                var collection = _database.GetCollection<BsonDocument>(collectionName);
                var filter = Builders<BsonDocument>.Filter.Eq(key, value);

                var result = await collection.Find(filter).Limit(limit).ToListAsync(cancellationToken);

                foreach (var item in result)
                {
                    returnList.Add(BsonSerializer.Deserialize<T>(item));
                }
            }
            catch (Exception ex)
            {
                //_log.Error(ex);
            }
            return returnList;
        }

        /// <summary>
        /// Returns a list of the given key-String and value-type(TValue)
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="collectionName"></param>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <param name="limit"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task<List<T>> SelectAsync<T, TValue>(string collectionName, string key, TValue value, int limit = MAX_LIMIT, CancellationToken cancellationToken = default)
        {
            var returnList = new List<T>();
            try
            {
                limit = limit > MAX_LIMIT ? MAX_LIMIT : limit;
                var collection = _database.GetCollection<BsonDocument>(collectionName);
                var filter = Builders<BsonDocument>.Filter.Eq(key, value);


                var result = await collection.Find(filter).Limit(limit).ToListAsync(cancellationToken);

                foreach (var item in result)
                {
                    returnList.Add(BsonSerializer.Deserialize<T>(item));
                }
            }
            catch (Exception ex)
            {
                //_log.Error(ex);
            }
            return returnList;
        }

        /// <summary>
        /// Select a single record of the given BsonDocument filter
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="collectionName"></param>
        /// <param name="filter"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task<T> SelectOneAsync<T>(string collectionName, FilterDefinition<BsonDocument> filter, CancellationToken cancellationToken = default)
        {
            try
            {
                var collection = _database.GetCollection<BsonDocument>(collectionName);
                var result = await collection.Find(filter).FirstAsync(cancellationToken);

                return BsonSerializer.Deserialize<T>(result);
            }
            catch (Exception ex)
            {
                //_log.Error(ex);
            }
            return default;
        }

        /// <summary>
        /// Select a single record of the given key-value string
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="collectionName"></param>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task<T> SelectOneAsync<T>(string collectionName, string key, string value, CancellationToken cancellationToken = default)
        {
            try
            {
                var collection = _database.GetCollection<BsonDocument>(collectionName);
                var filter = Builders<BsonDocument>.Filter.Eq(key, value);
                var result = await collection.Find(filter).FirstAsync(cancellationToken);

                return BsonSerializer.Deserialize<T>(result);
            }
            catch (Exception ex)
            {
                //_log.Error(ex);
            }
            return default;
        }


        /// <summary>
        /// Select a single record of the given key-String and value- type(TValue)
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="collectionName"></param>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task<T> SelectOneAsync<T, TValue>(string collectionName, string key, TValue value, CancellationToken cancellationToken = default)
        {
            try
            {
                var collection = _database.GetCollection<BsonDocument>(collectionName);
                var filter = Builders<BsonDocument>.Filter.Eq(key, value);
                var result = await collection.Find(filter).FirstAsync(cancellationToken);

                return BsonSerializer.Deserialize<T>(result);
            }
            catch (Exception ex)
            {
                //_log.Error(ex);
            }
            return default;
        }
        #endregion

        #region Insert
        /// <summary>
        /// InsertOneAsync a BsonDocument 
        /// </summary>
        /// <param name="collectionName"></param>
        /// <param name="doc"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task<bool> InsertOneAsync(string collectionName, BsonDocument doc, CancellationToken cancellationToken = default)
        {
            try
            {
                var collection = _database.GetCollection<BsonDocument>(collectionName);
                await collection.InsertOneAsync(doc, null, cancellationToken);
                return true;
            }
            catch (Exception ex)
            {
                //_log.Error(ex);
            }
            return false;
        }

        /// <summary>
        /// Insert an object of any type
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="collectionName"></param>
        /// <param name="doc"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task<bool> InsertOneAsync<T>(string collectionName, T doc, CancellationToken cancellationToken = default)
        {
            try
            {
                var collection = _database.GetCollection<BsonDocument>(collectionName);
                await collection.InsertOneAsync(doc.ToBsonDocument(), null, cancellationToken);
                return true;
            }
            catch (Exception ex)
            {
                //_log.Error(ex);
            }
            return false;
        }

        /// <summary>
        /// Insert multiple objects into a collection
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="collectionName"></param>
        /// <param name="documents"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task<bool> InsertManyAsync<T>(string collectionName, IEnumerable<T> documents, CancellationToken cancellationToken = default)
        {
            try
            {
                List<BsonDocument> docs = new List<BsonDocument>();
                for (int i = 0; i < documents.Count(); i++)
                {
                    docs[i] = documents.ElementAt(i).ToBsonDocument();
                }
                var collection = _database.GetCollection<BsonDocument>(collectionName);
                await collection.InsertManyAsync(docs, null, cancellationToken);
                return true;
            }
            catch (Exception ex)
            {
                //_log.Error(ex);
            }
            return false;
        }
        #endregion

        #region Update & Upsert
        /// <summary>
        /// Update an object
        /// </summary>
        /// <param name="collectionName"></param>
        /// <param name="filter"></param>
        /// <param name="update"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task<bool> UpdateOneAsync(string collectionName, FilterDefinition<BsonDocument> filter, UpdateDefinition<BsonDocument> update, bool isUpsert = false, CancellationToken cancellationToken = default)
        {
            try
            {
                var collection = _database.GetCollection<BsonDocument>(collectionName);
                await collection.UpdateOneAsync(filter, update, new UpdateOptions { IsUpsert = isUpsert }, cancellationToken);
                return true;
            }
            catch (Exception ex)
            {
                //_log.Error(ex);
            }
            return false;
        }

        /// <summary>
        /// Update an object
        /// </summary>
        /// <param name="collectionName"></param>
        /// <param name="key">The field name to identify the object to be updated</param>
        /// <param name="value">The value to the identifier field</param>
        /// <param name="update"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task<bool> UpdateOneAsync<T>(string collectionName, string key, T value, UpdateDefinition<BsonDocument> update, bool isUpsert = false, CancellationToken cancellationToken = default)
        {
            try
            {
                var filter = Builders<BsonDocument>.Filter.Eq(key, value);
                var collection = _database.GetCollection<BsonDocument>(collectionName);
                await collection.UpdateOneAsync(filter, update, new UpdateOptions { IsUpsert = isUpsert }, cancellationToken);
                return true;
            }
            catch (Exception ex)
            {
                //_log.Error(ex);
            }
            return false;
        }

        /// <summary>
        /// Update an object
        /// </summary>
        /// <param name="collectionName"></param>
        /// <param name="key">The field name to identify the object to be updated</param>
        /// <param name="value">The value to the identifier field</param>
        /// <param name="keyUpdate"></param>
        /// <param name="valueUpdate"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task<bool> UpdateOneAsync<T, TValue>(string collectionName, string key, T value, string keyUpdate, TValue valueUpdate, bool isUpsert = false, CancellationToken cancellationToken = default)
        {
            try
            {
                var filter = Builders<BsonDocument>.Filter.Eq(key, value);
                var update = Builders<BsonDocument>.Update.Set(keyUpdate, valueUpdate);
                var collection = _database.GetCollection<BsonDocument>(collectionName);
                await collection.UpdateOneAsync(filter, update, new UpdateOptions { IsUpsert = isUpsert }, cancellationToken);
                return true;
            }
            catch (Exception ex)
            {
                //_log.Error(ex);
            }
            return false;
        }

        /// <summary>
        /// Update to multiple objects
        /// </summary>
        /// <param name="collectionName"></param>
        /// <param name="key">The field name to identify the object to be updated</param>
        /// <param name="value">The value to the identifier field</param>
        /// <param name="keyUpdate"></param>
        /// <param name="valueUpdate"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task<bool> UpdateManyAsync<T, TValue>(string collectionName, string key, T value, string keyUpdate, TValue valueUpdate, bool isUpsert = false, CancellationToken cancellationToken = default)
        {
            try
            {
                var filter = Builders<BsonDocument>.Filter.Eq(key, value);
                var update = Builders<BsonDocument>.Update.Set(keyUpdate, valueUpdate);
                var collection = _database.GetCollection<BsonDocument>(collectionName);
                await collection.UpdateManyAsync(filter, update, new UpdateOptions { IsUpsert = isUpsert }, cancellationToken);
                return true;
            }
            catch (Exception ex)
            {
                //_log.Error(ex);
            }
            return false;
        }

        /// <summary>
        /// Update an Array inside an object
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="collectionName"></param>
        /// <param name="arrayField"></param>
        /// <param name="list"></param>
        /// <param name="filter"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task<bool> UpdateArrayAsync<T>(string collectionName, string arrayField, List<T> list, FilterDefinition<BsonDocument> filter, CancellationToken cancellationToken = default)
        {
            try
            {
                var collection = _database.GetCollection<BsonDocument>(collectionName);
                var update = Builders<BsonDocument>.Update.PushEach(arrayField, list);
                await collection.FindOneAndUpdateAsync<BsonDocument>(filter, update, null, cancellationToken);
                return true;
            }
            catch (Exception ex)
            {
                //_log.Error(ex);
            }
            return false;
        }
        #endregion

        #region Delete
        /// <summary>
        /// Remove an object from  a collection
        /// </summary>
        /// <param name="collectionName"></param>
        /// <param name="filter"></param>
        /// <returns></returns>
        public async Task<bool> DeleteOneAsync(string collectionName, FilterDefinition<BsonDocument> filter, CancellationToken cancellationToken = default)
        {
            try
            {
                var collection = _database.GetCollection<BsonDocument>(collectionName);
                await collection.DeleteOneAsync(filter);
                return true;
            }
            catch (Exception ex)
            {
                //_log.Error(ex);
            }
            return false;
        }

        /// <summary>
        /// Remove an object from a collection
        /// </summary>
        /// <param name="collectionName"></param>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public async Task<bool> DeleteOneAsync<T>(string collectionName, string key, T value, CancellationToken cancellationToken = default)
        {
            try
            {
                var collection = _database.GetCollection<BsonDocument>(collectionName);
                var filter = Builders<BsonDocument>.Filter.Eq(key, value);
                await collection.DeleteOneAsync(filter, cancellationToken);

                return true;
            }
            catch (Exception ex)
            {
                //_log.Error(ex);
            }
            return false;
        }
        #endregion

        public void Dispose()
        {
            GC.SuppressFinalize(this);
        }
    }
}
