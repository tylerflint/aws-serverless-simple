import axios            from 'axios';
import { base64Encode } from '../util';

export class Ably {
  
  constructor({ isTest, apiKey }) {
    this.isTest = isTest;
    this.apiKey = apiKey;
  }
    
  publish({ channel, event }) {
    // we don't need to publish events in a test env
    if (this.isTest) { return; }
    
    // publish to the channel
    return axios({
      method : 'post',
      url    : `https://rest.ably.io/channels/${channel}/messages`,
      data   : event,
      headers: {
        'Authorization': `Basic ${base64Encode(this.apiKey)}`
      }
    });
  }
}
