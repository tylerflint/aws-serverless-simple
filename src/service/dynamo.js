import crypto                   from 'crypto';
import get                      from 'lodash.get';
import chunk                    from 'lodash.chunk';
import isEmpty                  from 'lodash.isempty';
import isEqual                  from 'lodash.isequal'
import { DatabaseError }        from '../errors';
import { fromCursor, toCursor } from '../util';

// With dynamoDB, there are times that we need to insert documents into 
// a root context. To avoid hot-key situations, we can generate a partition
// value that we can reliably generate to determine the partition containing
// a particular document.
// 
// args:
//  key: string
//  partitions: int
export var partition = function(key, partitions) {
  const hash = key => crypto.createHash('sha1').update(key).digest('hex');

  const digest = function(key) {
    const index = hash(`${key}`);
    
    return index.split('').map(char => char.charCodeAt(0));
  };

  const hashValueHash = (a, b, c, d) => ((a << 24) | (b << 16) | (c << 8) | d) >>> 0;

  const hashValue = function(key) {
    const x = digest(key);

    return hashValueHash(x[3], x[2], x[1], x[0]);
  };
  
  return hashValue(key) % partitions;
};

// This is mostly a wrapper around the DocumentClient adapter, with some
// helper functionality to reduce the tedium
export class DynamoDB {
  
  constructor({ table, client, transactLimit=25 }) {
    this.table = table;
    this.client = client;
    this.transactLimit = transactLimit;
  }
    
  // Instantiate a collection wrapper with an instance of ourself injected
  collection(keys) {
    return new DynamoCollection({ dynamo: this, keys });
  }

  // Wrapper to simplify get operations and also bubble a db scoped error
  async get(key, mapFn) {
    // assemble the query
    const params = {
      TableName: this.table,
      Key: key
    };
    
    // run the query
    const result = await this.client.get(params).promise().catch(function(error) {
      throw new DatabaseError(error.message);
    });
    
    // fetch the item
    let item = get(result, 'Item');
    
    // if we have a mapFn, map it
    if (mapFn != null) {
      item = mapFn(item);
    }
      
    // return the result
    return item;
  }
    
  // Wrapper to simplify put operations and also bubble a db scoped error
  async put(data) {
    // assemble the query
    const params = {
      TableName: this.table,
      Item: data
    };
    
    // run the query
    const result = await this.client.put(params).promise().catch(function(error) {
      throw new DatabaseError(error.message);
    });
    
    // return the attributes
    return get(result, 'Attributes');
  }

  async putBatch(items) {
    // first, we need to make sure we're not over the transact limit
    // 
    // if we are, we just need to break it up into chunks
    if (items.length > this.transactLimit) {
      // break into chunks of our transactLimit
      const chunks = chunk(items, this.transactLimit);
      
      // return the collective transactWrite
      return Promise.all(chunks.map((part) => this.putBatch(part)));
    }
    
    // assemble the query
    items = items.map(item => ({
      PutRequest: {
        Item: item
      }
    }));
        
    // assemble the query
    const params =
      {RequestItems: {}};
    
    // add the items to the table
    params.RequestItems[this.table] = items;
    
    // run the query
    await this.client.batchWrite(params).promise().catch(function(error) {
      throw new DatabaseError(error.message);
    });
      
    // return true
    return true;
  }

  // Wrapper to simplify update operations and also bubble a db scoped error
  async update(opts) {
    // assemble the query
    const params = {
      TableName: this.table,
      ReturnValues: 'ALL_NEW',
      ...opts
    };
    
    // run the query
    const result = await this.client.update(params).promise().catch(function(error) {
      throw new DatabaseError(error.message);
    });
    
    // return the attributes
    return get(result, 'Attributes');
  }

  // Wrapper to simplify query operations and also bubble a db scoped error
  async query(pk, sk, { limit, cursor, filters, reverse } = {}, mapFn) {
    let params;
    if (sk) {
      params = {
        TableName: this.table,
        KeyConditionExpression: '#pk = :pk and begins_with(#sk, :sk)',
        ExpressionAttributeNames: {
          '#pk': 'pk',
          '#sk': 'sk'
        },
        ExpressionAttributeValues: {
          ':pk': pk,
          ':sk': sk
        }
      };
    } else {
      params = {
        TableName: this.table,
        KeyConditionExpression: '#pk = :pk',
        ExpressionAttributeNames: {
          '#pk': 'pk'
        },
        ExpressionAttributeValues: {
          ':pk': pk
        }
      };
    }
    
    // set the limit if provided
    if (limit) {
      params.Limit = limit;
    }
    
    // set a cursor if provided
    if (cursor) {
      params.ExclusiveStartKey = fromCursor(cursor);
    }
    
    // apply filters
    if (filters) {
      const expressions = filters.map(function(filter) {
        // extract the pieces
        const [ field, criteria, ...rest ] = filter.split(' ');
        // in case the rest was split, re-join into a single value
        let value = rest.join(' ');
        // also, in case the value is actually a number let's cast it
        const asNumber = Number(value);
        if (asNumber) {
          value = asNumber;
        }
        // add the attribute name
        params.ExpressionAttributeNames[`#${field}`] = field;
        // add the attribute value
        params.ExpressionAttributeValues[`:${field}`] = (function() {
          if (value === 'true') { 
            return true;
          }
          if (value === 'false') {
            return false;
          }
          return value;
        })();
        // return the expression
        return `#${field} ${criteria} :${field}`;
      });
      
      // join the expressions together
      params.FilterExpression = expressions.join(' and ');
    }
    
    // if reverse, don't scan index forward
    if (reverse) {
      params.ScanIndexForward = false;
    }
    
    // fetch the data
    const data = await this.client.query(params).promise().catch(function(error) {
      throw new DatabaseError(error.message);
    });
    
    // at a minimum, let's extract the items
    const result =
      {items: data.Items};
    
    // if there is more data available let's provide a cursor
    if (data.LastEvaluatedKey) {
      result.cursor = toCursor(data.LastEvaluatedKey);
    }
    
    // if a map fn was provided, let's apply it
    if (mapFn != null) {
      result.items = result.items.map(mapFn);
    }
    
    return result;
  }

  // Wrapper to simplify transactWrite operations and also bubble a db scoped error
  async transactWrite(ops, opts = {}) {
    // first, we need to make sure we're not over the transact limit
    // 
    // if we are, we just need to break it up into chunks
    if (ops.length > this.transactLimit) {
      // break into chunks of our transactLimit
      const chunks = chunk(ops, this.transactLimit);
      
      // return the collective transactWrite
      return Promise.all(chunks.map((part) => this.transactWrite(part, opts)));
    }
    
    // assemble the base query structure
    const params =
      {TransactItems: []};
    
    // iterate through the ops and assemble the params
    ops.forEach(op => {
      // handle put
      if (op.Put) {
        const putOp = op.Put;
        
        // inject the tablename if it's not present
        if (!putOp.TableName) {
          putOp["TableName"] = this.table;
        }
        
        // add the putOp to the transactItems
        params.TransactItems.push({
          Put: putOp});
      }
      
      // handle delete
      if (op.Delete) {
        const deleteOp = op.Delete;
        
        // inject the tablename if it's not present
        if (!deleteOp.TableName) {
          deleteOp["TableName"] = this.table;
        }
        
        // add the deleteOp to the transactItems
        params.TransactItems.push({
          Delete: deleteOp});
      }
      
      // handle update
      if (op.Update) {
        const updateOp = op.Update;
        
        // inject the tablename if it's not present
        if (!updateOp.TableName) {
          updateOp["TableName"] = this.table;
        }
        
        // add the updateOp to the transactItems
        return params.TransactItems.push({
          Update: updateOp});
      }
    });
    
    // transact-write queries can fail if concurrent processes are
    // attempting to modify the same object, and one of the processes
    // modifies the object before the second process finishes. We
    // can retry if that ends up being the case
    let success = false;
    let attempts = 0;
    let res = undefined;
    
    while (!success) {
      attempts += 1;
      
      // if attempts hits 20 something is very wrong
      if (attempts > 20) {
        throw new DatabaseError('Max attempts retrying TransactionConflict');
      }
      
      // run the query
      res = await this.client.transactWrite(params).promise()
        // if no error, we got the write
        .then(() => success = true).catch(async function(error) {
          if (error.message.match(/TransactionConflict/)) {
            // wait for a random time between 1 and 20 ms
            const wait = Math.floor(Math.random() * (20 + 1));
            // sleep
            return await (new Promise( resolve => setTimeout(resolve, wait)));
          } else {
            console.log(JSON.stringify(params, null, 2));
            throw new DatabaseError(error.message);
          }
      });
    }
    
    // return res if we get here
    return res;
  }

  delete(key, opts = {}) {
    const params = {
      TableName: this.table,
      Key: key
    };
    
    return this.client.delete(params).promise().catch(function(error) {
      throw new DatabaseError(error.message);
    });
  }
      
  // truncate the table (DANGEROUS)
  async truncate() {
    // the data comes back in pages, so we need to keep
    // a marker to let us know that we're not at the end yet
    let end = false;
    
    while (!end) {
      const params =
        {TableName: this.table};
      
      // fetch the page of data
      const data = await this.client.scan(params).promise().catch(function(error) {
        throw new DatabaseError(error.message);
      });
      
      // set the state if we have more pages
      if (!data.LastEvaluatedKey) {
        end = true;
      }
      
      // iterate through the items and delete them
      for (let { pk, sk } of data.Items) {
        await this.delete({ pk, sk });
      }
    }
    
    return true;
  }

  // export the table for local seeds
  async export() {
    // the full list of items to export
    const items = [];
    
    // the data comes back in pages, so we need to keep
    // a marker to let us know that we're not at the end yet
    let end = false;
    
    while (!end) {
      const params =
        {TableName: this.table};
      
      // fetch the page of data
      const data = await this.client.scan(params).promise().catch(function(error) {
        throw new DatabaseError(error.message);
      });
      
      // set the state if we have more pages
      if (!data.LastEvaluatedKey) {
        end = true;
      }
      
      // iterate through the items and delete them
      data.Items.forEach(item => items.push(item));
    }
    
    // export the results as prettified json
    return JSON.stringify(items, null, 2);
  }

  // import the table from local seeds
  async import(data) {
    // import the data as a batch
    await this.putBatch(JSON.parse(data));
    
    // report
    return true;
  }

  // assemble update opts from an object
  updateExpression(object) {
    // strip pk/sk if provided
    delete object.pk;
    delete object.sk;
      
    // assemble the updateOps
    let updateExpression = '';
    const attributeNames = {};
    const attributeValues = {};
      
    for (let key in object) {
      const val = object[key];
      if (updateExpression === '') {
        updateExpression = `set #${key} = :${key}`;
      } else {
        updateExpression += `, #${key} = :${key}`;
      }
        
      attributeNames[`#${key}`] = key;
      attributeValues[`:${key}`] = val;
    }
    
    return {
      UpdateExpression: updateExpression,
      ExpressionAttributeNames: attributeNames,
      ExpressionAttributeValues: attributeValues
    };
  }
}

// Dynamodb is a very free-form database with high-performance
// and infinite scalability, at the cost of needing to understand
// every possible access pattern and create a copy of the data
// to be stored under a specific key for every access pattern.
// 
// The 'collection' abstraction takes this very tedius and error-prone
// process and makes a very simple pattern for storing objects in collections
export class DynamoCollection {

  constructor({ dynamo, keys }) {
    // dynamo is our connection to the db
    this.dynamo = dynamo;

    // if access patterns aren't set, this doesn't work
    if (keys === undefined)
      throw new DatabaseError('Undefined schema');

    // prepare the access patterns by starting with an empty object
    this.keys = [];

    // now let's iterate through the provided access patterns and
    // validate configuration and set sensible defaults
    for (const pattern of keys) {
      // let's extract the pattern
      let { pk, sk, fields, when, ...extra } = pattern;

      // first, there shouldn't be any extra config here
      if (!isEmpty(extra))
        throw new DatabaseError(`Unknown access pattern config: ${JSON.stringify(extra)}`);

      // primary key is required
      if (pk === undefined)
        throw new DatabaseError(`Missing required access pattern config 'pk'`);

      // sort key is required
      if (sk === undefined)
        throw new DatabaseError(`Missing required access pattern config 'sk'`);

      // when needs to be a function
      if (when !== undefined && !(when instanceof Function))
        throw new DatabaseError("Access pattern config 'when' must be a function");

      // if we made it through, then let's go ahead and add the access pattern
      this.keys.push({ pk, sk, fields, when });
    }
  }

  // Add an item to the collection
  add(data, opts={}) {
    // extract the opts
    const { extraOps } = opts;

    // create a list of ops to transact
    let ops = [];

    // run through the access patterns and assemble the ops
    for (const pattern of this.keys) {
      // extract the pattern config
      const { pk, sk, fields, when } = pattern;

      // if when is provided, let's make sure we should add this
      if (when !== undefined && !when(data))
        continue;

      // assemble the item
      let item = { 
        pk: this.#key(pk, data), 
        sk: this.#key(sk, data),
      };

      // if fields is defined, we need to extract the data
      if (fields !== undefined) {
        for (const field of fields) {
          item[field] = data[field];
        }
      } else {
        item = { ...data, ...item };
      }

      // add the op
      ops.push({
        Put: {
          Item: item
        }
      });
    }

    // if extraOps are set, add those
    if (extraOps !== undefined)
      ops = [...ops, ...extraOps];

    // console.log(JSON.stringify(ops, null, 2));

    // return early if there are no ops
    if (ops.length == 0)
      return;

    // run the ops
    return this.dynamo.transactWrite(ops);
  }

  // Remove an item from the collection
  remove(data, opts={}) {
    // extract the opts
    const { extraOps } = opts;

    // create a list of ops to transact
    let ops = [];

    // run through the access patterns and assemble the ops
    for (const pattern of this.keys) {
      // extract the pattern config
      const { pk, sk, when } = pattern;

      // if when is provided, no need to remove a key that doesn't exist
      if (when !== undefined && !when(data))
        continue;

      // assemble the key
      let key = {
        pk: this.#key(pk, data),
        sk: this.#key(sk, data),
      };

      // add the op
      ops.push({
        Delete: {
          Key: key
        }
      });
    }

    // if extraOps are set, add those
    if (extraOps !== undefined)
      ops = [...ops, ...extraOps];

    // console.log(JSON.stringify(ops, null, 2));

    // return early if there are no ops
    if (ops.length == 0)
      return;

    // transact the ops
    return this.dynamo.transactWrite(ops);
  }

  // Update an item in the collection
  update(data, changes, opts={}) {
    // extract the opts
    const { extraOps } = opts;

    // remove the changes that aren't changes at all
    for (const key in changes) {
      if (isEqual(data[key], changes[key]))
        delete changes[key];
    }

    // create a list of ops to transact
    let ops = [];

    // run through the access patterns and assemble the ops
    for (const pattern of this.keys) {
      // extract the pattern config
      const { pk, sk, fields, when } = pattern;

      // we need to work with a new item and a previous item
      let prev = {};
      let curr = {};
      let currChanges = {};

      // if fields are defined, pull the requested data
      if (fields !== undefined) {
        for (const field of fields) {
          prev[field] = data[field];

          if (changes[field] !== undefined) {
            curr[field] = changes[field];
            currChanges[field] = changes[field];
          } else {
            curr[field] = data[field];
          }
        }
      } else {
        prev = data;
        curr = { ...prev, ...changes };
        currChanges = changes;
      }

      // if when was true but now false, we need a delete op
      if (when !== undefined && (when(data) && !when({ ...data, ...changes }))) {
        // add the op
        ops.push({
          Delete: {
            Key: {
              pk: this.#key(pk, data),
              sk: this.#key(sk, data)
            }
          }
        });

        // nothing more to do
        continue;
      }

      // if when is provided and false, nothing to do
      if (when !== undefined && !when({ ...data, ...changes }))
        continue;

      // assemble the previous key
      const prevKey = {
        pk: this.#key(pk, data),
        sk: this.#key(sk, data),
      }
      
      // assemble the current key
      const currKey = {
        pk: this.#key(pk, { ...data, ...changes }),
        sk: this.#key(sk, { ...data, ...changes }),
      }
      
      // has the key changed?
      if (prevKey.pk != currKey.pk || prevKey.sk != currKey.sk) {
        // need a delete op for the previous item
        ops.push({
          Delete: {
            Key: prevKey
          }
        });

        // and a create op for the current
        ops.push({
          Put: {
            Item: { ...curr, ...currKey }
          }
        });
      } else if (!isEmpty(currChanges)) {
        // assemble the updateExpression
        const updateFields = this.dynamo.updateExpression(currChanges);

        // assemble an op for updating
        ops.push({
          Update: {
            Key: currKey,
            ...updateFields,
          }
        });
      }
    }

    // if extraOps are set, add those
    if (extraOps !== undefined)
      ops = [...ops, ...extraOps];

    // console.log(JSON.stringify(ops, null, 2));

    // return early if there are no ops
    if (ops.length == 0)
      return;

    // transact the ops
    return this.dynamo.transactWrite(ops);
  }

  #key(pattern, data) {
    // create a key
    let key = pattern;

    // grab the tokens from the pattern
    for (const token of pattern.matchAll(/{(\w+)}/g)) {
      // extract the match {field}
      const match = token[0];

      // extract the field
      const field = token[1];

      // grab the value from the data
      const value = data[field];

      // replace the match with the value in the key
      key = key.replace(new RegExp(match, 'g'), value);
    }

    // return the key
    return key;
  }
}
