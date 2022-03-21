import _uniq          from 'lodash.uniq';
import { QueueError } from '../errors';

//######### Worker ##########

export class Worker {
  
  constructor({ topicArn, queueArn, snsClient, sfsClient }) {
    this.topicArn = topicArn;
    this.queueArn = queueArn;
    this.snsClient = snsClient;
    this.sfsClient = sfsClient;
    this.cancelArn = this.queueArn.replace('stateMachine', 'execution');
  }
    
  async enqueue(topic, data, context, opts={}) {
    // extract the context
    let msg;
    const { isWorker } = context;
    
    // extract the opts
    let { attempts, delay, errors, retry, retryStrategy } = opts;
    
    // set some sensible defaults
    if (!attempts) { attempts = 0; }
    if (!delay) { delay = 0; }
    if (!retry) { retry = false; }
    if (!retryStrategy) { retryStrategy = 'stepped'; }
    if (!errors) { errors = []; }
    
    // if this is a retry but retry if false, send straight to the morgue
    if ((attempts > 0) && !retry) {
      return addCorpse({ topic, data, attempts, errors, retry }, context);
    }

    // assemble the job
    let job = { topic, data, attempts, errors, retry, retryStrategy };
    
    // if we're already in the worker context and no delay is set, run it now
    if (isWorker && !delay) {
      return this.work(job, context);
    }
    
    // serialize the job for submission
    job = JSON.stringify(job);
    
    // otherwise, do we have a delay? If so let's drop it in a wait state function
    if (delay && (delay > 0)) {
      msg = { delay, job };
        
      const params = { 
        stateMachineArn: this.queueArn,
        input: JSON.stringify(msg) 
      };
        
      // start the execution and extract the arn
      const { executionArn } = await this.sfsClient.startExecution(params).promise().catch(function(error) {
        throw new QueueError(error.message);
      });
      
      // extract and return the id from the executionArn
      return executionArn.split(':').pop();
    }
    
    // no delay is set so let's drop this in sns straight away
    msg = {
      Message : job,
      TopicArn: this.topicArn
    };
    
    // publish the message and extract the message id
    const { MessageId: messageId } = await this.snsClient.publish(msg).promise().catch(function(error) {
      throw new QueueError(error.message);
    });
      
    // return the message ID
    return messageId;
  }

  dequeue(context) {
    return event => {
      // was this worker triggered directly with a single job?
      if (event.job != null) {
        return this.work(JSON.parse(event.job), context);
      }
      
      // or triggered with one or more sns records
      return Promise.allSettled(event.Records.map(record => {
        return this.work(JSON.parse(record.Sns.Message), context);
      })
      );
    };
  }
  
  cancel(id) {
    return this.sfsClient.stopExecution({ executionArn: `${this.cancelArn}:${id}` }).promise().catch(function(error) {
      throw new QueueError(error.message);
    });
  }
  
  status(id) {
    return this.sfsClient.describeExecution({ executionArn: `${this.cancelArn}:${id}` }).promise().catch(function(error) {
      throw new QueueError(error.message);
    });
  }
      
  async work(job, context) {
    // extract the workers from the context
    const { workers } = context;
    
    // print something useful to the log
    console.log('Job:')
    console.log(`  Topic : ${job.topic}`)
    console.log(`  Data  : ${JSON.stringify(job.data)}`)
    
    // find the worker for this job
    const handler = workers[job.topic];
    
    // fire the handler, if one exists
    if (handler) {
      try {
        await handler(job.data, context);
      } catch (error) {
        // show something meaningful in the logs
        console.error('Job failed :(')
        console.error(`  Topic  : ${job.topic}`)
        console.error(`  Data   : ${JSON.stringify(job.data)}`)
        console.error(`  Retry  : ${job.retry ? 'yes' : 'no'}`)
        console.error(`  ${error.stack}`)
        
        // re-enqueue and try again
        if (job.retry) {
          try {
            await this.retry(job, error.stack, context);
          } catch (error1) {
            error = error1;
            console.error('Failed to re-enqueue :(')
            console.error(`  Topic  : ${job.topic}`)
            console.error(`  Data   : ${JSON.stringify(job.data)}`)
            console.error(`  ${error.stack}`)
          }
        }
      }
    }
    
    // return true since we already handled any errors or failures
    return true;
  }

  retry({ topic, data, attempts, errors, retry, retryStrategy }, err, context) {
    // set attempts to 0 if not set
    if (!attempts) {
      attempts = 0;
    }
      
    // increment the attempts
    attempts += 1;
        
    // if no errors, let's default to an empty list
    if (!errors) {
      errors = [];
    }
      
    // add the error
    errors.push(err);
    
    // we only want unique errors, not the same error over and over
    errors = _uniq(errors);
    
    // set the delay
    const delay = delayDuration(attempts, retryStrategy);
    
    // if the delay is 0, we've hit our max attempts for this strategy
    if (delay === 0) {
      return addCorpse({ topic, data, attempts, errors }, context);
    }
    
    // enqueue
    return this.enqueue(topic, data, context, { attempts, delay, errors, retry, retryStrategy });
  }
}

//######### Helpers ##########

function delayDuration(attempts, strategy) {
  if (!strategy) { strategy = 'stepped'; }
  
  if (strategy === 'equal') {
    return equalBackoffDelay(attempts);
  }
    
  if (strategy === 'stepped') {
    return steppedBackoffDelay(attempts);
  }
    
  if (strategy === 'steppedQuick') {
    return steppedQuickBackoffDelay(attempts);
  }
   
  if (strategy === 'steppedLong') {
    return steppedLongBackoffDelay(attempts);
  }

  if (strategy === 'exponential') {
    return exponentialBackoffDelay(attempts);
  }
  
  // if it's a strategy we don't recognize, emulate max attempts reached
  return 0;
};

function equalBackoffDelay(attempts) {
  if (attempts === 1) {
    return 15;
  }
    
  if (attempts === 2) {
    return 30;
  }
    
  if (attempts === 3) {
    return 45;
  }
    
  if (attempts === 4) {
    return 60;
  }
  
  // max attempts reached
  return 0;
};

function steppedBackoffDelay(attempts) {
  // try 3 times quickly
  if (attempts < 4) {
    return 1;
  }
  
  // try a few more times with a slight delay
  if (attempts < 15) {
    return 5;
  }
    
  // try for a while with a 15 second interval
  if (attempts < 61) {
    return 15;
  }
    
  // keep trying, space it out a bit
  if (attempts < 121) {
    return 30;
  }
    
  // max attempts reached
  return 0;
};

function steppedQuickBackoffDelay(attempts) {
  if (attempts < 4) {
     return 1;
   }
     
  if (attempts < 11) {
    return 3;
  }
    
  if (attempts < 21) {
    return 10;
  }
    
  // max attempts reached
  return 0;
};

function steppedLongBackoffDelay(attempts) {
  // try 3 times quickly
  if (attempts < 4) {
    return 1;
  }

  // try a few times with a day delay
  if (attempts < 10) {
    return 86400;
  }

  // try a few more times with a 3 day delay
  if (attempts < 15) {
    return 259200;
  }

  // try for a couple of weeks
  if (attempts < 20) {
    return 604800;
  }

  // we've hit the limit after nearly 3 months
  return 0;
};

function exponentialBackoffDelay(attempts) {};


// ########## The Morgue ##########

function allCorpses(filter, { db }) {};

function getCorpse(id, { db }) {};

function addCorpse({ topic, data, attempts, errors, retry }, { db }) {
  console.error(`Job sent to the morgue :(`)
  console.error(`Topic    : ${topic}`)
  console.error(`Data     : ${JSON.stringify(data)}`)
  console.error(`Attempts : ${attempts}`)
}

function removeCorpse(id, { db }) {};
