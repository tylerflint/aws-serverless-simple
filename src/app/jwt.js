import { sign, verify } from 'jsonwebtoken';
import { AuthError }    from '../errors';

const JWT_EXPIRATION_TIME = '999 days';
const {
  JWT_SECRET
} = process.env;

export var encode = (payload, expires=JWT_EXPIRATION_TIME) => sign(payload, JWT_SECRET, { expiresIn: expires });

export var decode = function(token) {
  try {
    return verify(token, JWT_SECRET);
  } catch (_error) {
    throw new AuthError('Invalid token');
  }
};

export var renew = (token, expires=JWT_EXPIRATION_TIME) => encode(decode(token));
