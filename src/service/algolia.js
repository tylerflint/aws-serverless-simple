import algoliasearch from 'algoliasearch/lite';

export class Algolia {
  
  constructor({ isTest, env, apiKey, appId }) {
    this.isTest = isTest;
    this.env = env;
    this.apiKey = apiKey;
    this.appId = appId;
  }
    
  getClient() {
    return this.client || (this.client = algoliasearch(this.appId, this.apiKey));
  }
    
  getIndex(collection) {
    if (!this.indexes) { this.indexes = {}; }
    return this.indexes[collection] || (this.indexes[collection] = this.getClient().initIndex(`${this.env}-${collection}`));
  }
    
  configureIndex(collection, opts={}) {
    if (this.isTest) {
      return;
    }
    
    // extract the opts
    const { attributes, filters=[] } = opts;
    
    // init the index
    const index = this.getIndex(collection);
    
    // configure settings
    return index.setSettings({
      searchableAttributes: attributes,
      attributesForFaceting: filters.map(f => `filterOnly(${f})`)
    });
  }
    
  addObjects(collection, objects) {
    if (this.isTest) {
      return;
    }
      
    // prepare the objects
    objects = objects.map(({ id, ...object }) => ({
      objectID: id,
      ...object
    }));
    
    // init the index
    const index = this.getIndex(collection);
      
    // save the objects
    return index.saveObjects(objects);
  }
    
  addObject(collection, { id, ...object }) {
    if (this.isTest) {
      return;
    }
      
    // init the index
    const index = this.getIndex(collection);
    
    // save the object
    return index.saveObject({ objectID: id, ...object });
  }
    
  removeObject(collection, id) {
    if (this.isTest) {
      return;
    }
      
    // init the index
    const index = this.getIndex(collection);
    
    // delete the object
    return index.deleteObject(id);
  }
    
  search(collection, query, filters={}, mapFn) {
    if (this.isTest) {
      return;
    }
    
    // init the index
    const index = this.getIndex(collection);
    
    // prepare the facet filters
    const facetFilters = [];
    
    for (let attr in filters) {
      facetFilters.push(`${attr}:${filters[attr]}`);
    }
    
    // search
    return index.search(query, { facetFilters })
      .then(function({ hits }) {
        // replace objectID with ID
        let items = hits.map(({ objectID, ...object }) => ({
          id: objectID,
          ...object
        }));
          
        // if a map fn was provided, let's apply it
        if (mapFn != null) {
          items = items.map(mapFn);
        }
          
        return { items };});
  }
}
        
    
  
