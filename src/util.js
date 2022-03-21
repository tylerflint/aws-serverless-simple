import _isEqual from 'lodash.isequal';
import crypto   from 'crypto';

// Common helper utilities

// provide a timestamp in unix epoch format
export function unixNow() {
  return new Date().getTime();
} 

// Encode a string
export function base64Encode(data) {
  return Buffer.from(data).toString('base64');
} 

// Decode a string
export function base64Decode(data) {
  return Buffer.from(data, 'base64').toString('ascii');
} 

// Encode an object into a cursor
export function toCursor(data) {
  return base64Encode(JSON.stringify(data));
} 

// Decode a cursor into an object
export function fromCursor(data) {
  return JSON.parse(base64Decode(data));
}
  
// generate a secure, random token
export function randomToken() {
  return crypto.randomBytes(256).toString('base64');
}
  
// generate a random, six-digit code
export function randomCode() {
  return Math.floor(100000 + (Math.random() * 900000));
}
  
// Returns an object with { added, removed, changed }
export function collectionDifferences(a=[], b=[], key='key') {
  // detect the added
  const added = [];
  
  // detect the removed
  const removed = [];
  
  // detect the changed
  const changed = [];
  
  // iterate through a to find the removed and changed
  a.forEach(function(x) {
    // find the element
    const y = b.find(e => e[key] === x[key]);
    
    // add to remove if it doesn't exist
    if (!y) {
      removed.push(x);
      return;
    }
      
    // check to see if it's different
    if (!_isEqual(x, y)) {
      // create a change record
      const change = { 
        from: x,
        to  : y
      };
      
      // add back the key
      change[key] = x[key];
      
      // add to the list of changes
      return changed.push(change);
    }
  });
  
  // now let's iterate through b to find what was added
  b.forEach(function(y) {
    // find the element
    const x = a.find(e => e[key] === y[key]);
    
    // add to added if it doesn't exist
    if (!x) {
      return added.push(y);
    }
  });
  
  // return the delta
  return { added, removed, changed };
};
