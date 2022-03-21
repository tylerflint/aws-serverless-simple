import _differenceWith from 'lodash.differencewith';
import { unixNow }     from '../util';

export const migrate = async function(data, context) {
  // extract the context
  const { migrations, dynamo } = context;
  
  // data is a placeholder for the time being. At some point if we need
  // to provide configuration or customization it could be provided
  
  // grab the previous migrations that have already run
  const { items: previousMigrations } = await allMigrations(dynamo);
  
  // filter the list of migrations that have already run
  const pendingMigrations = _differenceWith(migrations, previousMigrations, (migration, prevMigration) => migration.name === prevMigration.name);
  
  if (pendingMigrations.length === 0) {
    console.log("No pending migrations to run - database up-to-date");
    return;
  }
  
  for (let { name, description, run } of pendingMigrations) {
    // report
    console.log(`Starting migration: ${description}`);
    
    // run
    try {
      await run(context);
    } catch (error) {
      // show something meaningful in the logs
      console.error('Migration failed :(')
      console.error(`  Description : ${description}`)
      console.error(`  ${error.stack}`)
      
      // don't continue
      return;
    }
    
    // persist migration to the database
    await createMigration(name, dynamo);
  }
    
  // report
  return console.log("All migrations complete - database up-to-date");
};
  
var allMigrations = dynamo => dynamo.query("root#migrations", 'name-');
  
var createMigration = function(name, dynamo) {
  const createdAt = `${unixNow()}`;
  return dynamo.put({
    pk: "root#migrations",
    sk: `name-${name}-${createdAt}`,
    name,
    createdAt
  });
};
