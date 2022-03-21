// We want to be able to catch errors at the source, and then wrap them
// with some context so that the edge handlers (graphql, API, etc) can
// provide some vague, external context so the users can be generally
// informed of what went wrong, while our internal systems can have
// specific information.

// Generic error for dynamodb issues
export class DatabaseError extends Error {
  constructor(message) {
    super(`DatabaseError: ${message}`);
    this.name = 'DatabaseError';
  }
}

// Generic error for SNS issues
export class QueueError extends Error {
  constructor(message) {
    super(`QueueError: ${message}`);
    this.name = 'QueueError';
  }
}

// Generic error for user input validation
export class ValidationError extends Error {
  constructor(message) {
    super(`ValidationError: ${message}`);
    this.name = 'ValidationError';
  }
}

// Generic error for user providing invalid input operation
export class InputError extends Error {
  constructor(message) {
    super(`InputError: ${message}`);
    this.name = 'InputError';
  }
}
  
// Generic error for when auth is missing
export class AuthError extends Error {
  constructor(message) {
    super(`AuthError: ${message}`);
    this.name = 'AuthError';
  }
}
  
// Generic error for unauthorized access
export class AccessError extends Error {
  constructor(message) {
    super(`AccessError: ${message}`);
    this.name = 'AccessError';
  }
}

// Generic error for reporting issues with 3rd-party services
// this will usually only be used with axios responses
export class ServiceError extends Error {
  constructor({ response, request, message }) {
    // server responded with non 2xx status code
    if (response) {
      // extract the response data
      const { status, data } = response;
      super(`ServiceError: server responded with ${status} - ${data}`);
    
    // request never made it to the server
    } else if (request) {
      super(`ServiceError: request failed - ${request}`);
    
    } else {
      // something happened setting up the request
      super(`ServiceError: ${message}`);
    }
      
    this.name = "ServiceError";
  }
}
