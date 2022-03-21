// a helper for models to publish events in a way that the UI can consume
export var publishChange = function(entity, context) {
  // extract the context
  const { isWorker, worker, pubsub, silencePublish } = context;
  
  // if push has been silenced (usually for batch or large operations) let's exit
  if (silencePublish) {
    return;
  }
  
  // let's only publish within a worker (revisit)
  if (!isWorker) {
    return worker.enqueue('publishChange', entity, context);
  }

  // extract the information
  const { path, action, id } = entity;
  
  // create an event for the entity
  const entityEvent = { 
    channel: path.replace(/\//g, '_'),
    event: {
      name: 'change',
      data: { action, id }
    }
  };
  
  // if the entity doesn't have a parent collection let's just
  // publish the entity event
  if (path.split('/').length < 3) {
    return pubsub.publish(entityEvent);
  }
  
  // create an event for the collection
  const collectionEvent = { 
    channel: path.split('/').slice(0, -1).join('_'),
    event: {
      name: 'change',
      data: {
        id,
        action: (() => { switch (action) {
                  case 'create': return 'add';
                  case 'update': return 'update';
                  case 'delete': return 'remove';
        } })()
      }
    }
  };
  
  // publish
  return Promise.all([
    pubsub.publish(entityEvent)
  ,
    pubsub.publish(collectionEvent)
  ]);
};

// a helper to run an operation which will silence all the publish events
export var withSilentPublish = async function(context, op) {
  // silence the publish
  context.silencePublish = true;
  
  // run the op
  await op();
  
  // unsilence the publish
  return context.silencePublish = false;
};
